// scripts/fix_company_name_format.ts
//
// companies_new コレクション上で、
// nameフィールドに「（株）」が含まれている場合、「株式会社」に修正するスクリプトです。
//
// 例:
//   （株）ABC → 株式会社ABC
//   ABC（株） → ABC株式会社
//
// 実行例:
//   DRY_RUN=1 npx ts-node scripts/fix_company_name_format.ts   // 更新せず候補だけログ
//   npx ts-node scripts/fix_company_name_format.ts             // 実際に更新
//
// 再開オプション:
//   START_FROM_DOC_ID="docId123" npx ts-node scripts/fix_company_name_format.ts  // 特定のドキュメントIDから再開
//
// Firestore 認証:
//   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/serviceAccountKey.json"

import admin from "firebase-admin";
import * as fs from "fs";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// 1 回のクエリで読む件数
const PAGE_SIZE = 1000;
// 1 バッチで更新する件数（Firestore の上限 500 未満にする）
const BATCH_UPDATE_SIZE = 400;

// DRY_RUN=1 のときは更新せずログだけ出す
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";

// 再開オプション
const START_FROM_DOC_ID = process.env.START_FROM_DOC_ID;

function initFirebaseAdmin() {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    console.error(
      "❌ エラー: 環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されていません"
    );
    process.exit(1);
  }

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(
      `❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`
    );
    process.exit(1);
  }

  const serviceAccount = JSON.parse(
    fs.readFileSync(serviceAccountPath, "utf8")
  );

  const projectId =
    serviceAccount.project_id ||
    process.env.GCLOUD_PROJECT ||
    process.env.GCP_PROJECT ||
    PROJECT_ID;

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });
  console.log(`✅ Firebase Admin initialized (Project ID: ${projectId})`);

  return admin.firestore();
}

// 「（株）」を「株式会社」に変換（前株・後株を判定）
function normalizeCompanyNameFormat(name: string | null | undefined): string | null {
  if (!name) return null;
  const trimmed = String(name).trim();
  if (!trimmed) return null;

  // 既に「株式会社」が含まれている場合はそのまま
  if (trimmed.includes("株式会社")) {
    return trimmed;
  }

  // 「（株）」を検出
  if (trimmed.includes("（株）")) {
    // 前株: 「（株）○○」→ 「株式会社○○」
    if (trimmed.startsWith("（株）")) {
      return "株式会社" + trimmed.substring(3);
    }
    // 後株: 「○○（株）」→ 「○○株式会社」
    if (trimmed.endsWith("（株）")) {
      return trimmed.substring(0, trimmed.length - 3) + "株式会社";
    }
    // 中間にある場合も後株として処理
    const index = trimmed.indexOf("（株）");
    if (index > 0) {
      return trimmed.substring(0, index) + "株式会社" + trimmed.substring(index + 3);
    }
  }

  return trimmed;
}

async function main() {
  const db = initFirebaseAdmin();

  const colRef = db.collection(COLLECTION_NAME);

  let lastDoc: FirebaseFirestore.QueryDocumentSnapshot | null = null;

  // 再開オプション: 特定のドキュメントIDから開始
  if (START_FROM_DOC_ID) {
    try {
      const startDoc = await colRef.doc(START_FROM_DOC_ID).get();
      if (startDoc.exists) {
        lastDoc = startDoc as FirebaseFirestore.QueryDocumentSnapshot;
        console.log(`🔄 Resuming from document ID: ${START_FROM_DOC_ID}`);
      } else {
        console.warn(`⚠️  Warning: Document ID "${START_FROM_DOC_ID}" not found. Starting from beginning.`);
      }
    } catch (error) {
      console.error(`❌ Error loading start document: ${error}`);
      process.exit(1);
    }
  }

  let scanned = 0;
  let candidates = 0;
  let updated = 0;

  let batch = db.batch();
  let batchCount = 0;

  // チェックポイントファイル（再開用）
  const checkpointFile = "fix_company_name_format_checkpoint.txt";
  // 候補リストファイル
  const candidatesFile = "fix_company_name_format_candidates.txt";

  // 候補リストファイルを初期化
  if (DRY_RUN && fs.existsSync(candidatesFile)) {
    fs.unlinkSync(candidatesFile);
  }

  console.log(
    `🔍 Scan start: collection="${COLLECTION_NAME}", pageSize=${PAGE_SIZE}, batchUpdateSize=${BATCH_UPDATE_SIZE}, DRY_RUN=${DRY_RUN}`
  );

  // チェックポイントから再開（START_FROM_DOC_IDが指定されていない場合）
  if (!START_FROM_DOC_ID && fs.existsSync(checkpointFile)) {
    try {
      const checkpointData = fs.readFileSync(checkpointFile, "utf8").trim();
      const checkpointDocId = checkpointData.split("\n")[0];
      if (checkpointDocId) {
        const checkpointDoc = await colRef.doc(checkpointDocId).get();
        if (checkpointDoc.exists) {
          lastDoc = checkpointDoc as FirebaseFirestore.QueryDocumentSnapshot;
          const checkpointScanned = checkpointData.split("\n")[1] ? parseInt(checkpointData.split("\n")[1], 10) : 0;
          scanned = checkpointScanned;
          console.log(`🔄 Resuming from checkpoint: docId=${checkpointDocId}, scanned=${scanned}`);
        }
      }
    } catch (error) {
      console.warn(`⚠️  Warning: Could not load checkpoint: ${error}`);
    }
  }

  while (true) {
    let query = colRef.orderBy(admin.firestore.FieldPath.documentId()).limit(
      PAGE_SIZE
    );
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snap = await query.get();
    if (snap.empty) {
      break;
    }

    for (const doc of snap.docs) {
      scanned += 1;

      const data = doc.data();
      const name = (data as any).name;

      // nameフィールドが存在し、「（株）」が含まれている場合
      if (name && typeof name === "string" && name.includes("（株）")) {
        const normalizedName = normalizeCompanyNameFormat(name);

        // 正規化後の名前が元と異なる場合、更新対象
        if (normalizedName && normalizedName !== name) {
          candidates += 1;

          if (DRY_RUN) {
            const candidateLine = `${doc.id}\t"${name}"\t"${normalizedName}"\n`;
            fs.appendFileSync(candidatesFile, candidateLine, "utf8");
            if (candidates <= 100 || candidates % 1000 === 0) {
              console.log(
                `🔧 [candidate] docId=${doc.id} "${name}" → "${normalizedName}"`
              );
            }
          } else {
            batch.update(doc.ref, { name: normalizedName });
            batchCount += 1;

            if (batchCount >= BATCH_UPDATE_SIZE) {
              await batch.commit();
              updated += batchCount;
              console.log(
                `💾 Committed update batch: ${batchCount} docs (total updated: ${updated}, scanned: ${scanned})`
              );
              batch = db.batch();
              batchCount = 0;
            }
          }
        }
      }

      if (scanned % 10000 === 0) {
        console.log(
          `📦 scanning... scanned=${scanned}, candidates=${candidates}, updated=${updated}`
        );
        // チェックポイントを保存（10000件ごと）
        if (!DRY_RUN && lastDoc) {
          try {
            fs.writeFileSync(
              checkpointFile,
              `${lastDoc.id}\n${scanned}`,
              "utf8"
            );
          } catch (error) {
            // チェックポイント保存エラーは無視
          }
        }
      }
    }

    lastDoc = snap.docs[snap.docs.length - 1];
  }

  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    updated += batchCount;
    console.log(
      `💾 Committed final update batch: ${batchCount} docs (total updated: ${updated})`
    );
  }

  // チェックポイントファイルを削除（処理完了時）
  if (fs.existsSync(checkpointFile)) {
    try {
      fs.unlinkSync(checkpointFile);
      console.log(`🗑️  Checkpoint file removed`);
    } catch (error) {
      // チェックポイント削除エラーは無視
    }
  }

  console.log("✅ Update finished");
  console.log(`  🔍 scanned docs : ${scanned}`);
  console.log(`  🔧 candidates   : ${candidates}`);
  console.log(`  ✅ updated      : ${updated} (DRY_RUN=${DRY_RUN})`);

  if (DRY_RUN && candidates > 0) {
    console.log(`\n📄 候補リストをファイルに保存しました: ${candidatesFile}`);
    console.log(`   合計 ${candidates} 件の修正候補が記録されています`);
  }
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
