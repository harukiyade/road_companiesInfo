// scripts/delete_companies_with_urls.ts
//
// companies_new コレクションのうち、以下の条件を満たすドキュメントを削除します：
//   - companyUrl と contactFormUrl の両方のみがあるドキュメント（それ以外に有効なフィールドがない）
//   - companyUrl のみがあるドキュメント（それ以外に有効なフィールドがない）
//   - contactFormUrl のみがあるドキュメント（それ以外に有効なフィールドがない）
// つまり、companyUrl または contactFormUrl のいずれかまたは両方があり、
// かつそれ以外に有効なフィールド（null/空文字列/空配列/空オブジェクト以外）がないドキュメントを削除します。
//
// 実行例:
//   DRY_RUN=1 npx ts-node scripts/delete_companies_with_urls.ts   // 削除せず候補だけログ
//   npx ts-node scripts/delete_companies_with_urls.ts             // 実際に削除
//
// 再開オプション:
//   START_FROM_DOC_ID="docId123" npx ts-node scripts/delete_companies_with_urls.ts  // 特定のドキュメントIDから再開
//
// Firestore 認証:
//   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/serviceAccountKey.json"

import admin from "firebase-admin";
import * as fs from "fs";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// 1 回のクエリで読む件数
const PAGE_SIZE = 1000;
// 1 バッチで削除する件数（Firestore の上限 500 未満にする）
const BATCH_DELETE_SIZE = 400;

// DRY_RUN=1 のときは削除せずログだけ出す
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

function normalizeString(v: unknown): string {
  if (v === null || v === undefined) return "";
  const str = String(v).trim();
  return str;
}

function hasValue(v: unknown): boolean {
  if (v === null || v === undefined) return false;
  if (typeof v === "string") {
    const normalized = v.trim();
    return normalized !== "" && normalized !== "null" && normalized !== "undefined";
  }
  if (typeof v === "number") return true;
  if (typeof v === "boolean") return true;
  if (Array.isArray(v)) return v.length > 0;
  if (typeof v === "object") {
    // オブジェクトが空でないかチェック
    return Object.keys(v).length > 0;
  }
  return true;
}

// companyUrlとcontactFormUrl以外に有効なフィールドがあるかチェック
function hasOtherValidFields(data: any): boolean {
  for (const key in data) {
    // companyUrlとcontactFormUrlはスキップ
    if (key === "companyUrl" || key === "contactFormUrl") {
      continue;
    }
    // その他のフィールドに有効な値があるかチェック
    if (hasValue(data[key])) {
      return true;
    }
  }
  return false;
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
  let deleted = 0;

  let batch = db.batch();
  let batchCount = 0;

  // チェックポイントファイル（再開用）
  const checkpointFile = "delete_urls_checkpoint.txt";
  // 候補リストファイル
  const candidatesFile = "delete_urls_candidates.txt";
  
  // 候補リストファイルを初期化
  if (DRY_RUN && fs.existsSync(candidatesFile)) {
    fs.unlinkSync(candidatesFile);
  }

  console.log(
    `🔍 Scan start: collection="${COLLECTION_NAME}", pageSize=${PAGE_SIZE}, batchDeleteSize=${BATCH_DELETE_SIZE}, DRY_RUN=${DRY_RUN}`
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
      const companyUrl = (data as any).companyUrl;
      const contactFormUrl = (data as any).contactFormUrl;

      const hasCompanyUrl = hasValue(companyUrl);
      const hasContactFormUrl = hasValue(contactFormUrl);

      // 削除条件：
      // 1. companyUrl または contactFormUrl のいずれかまたは両方がある
      // 2. かつ、それ以外に有効なフィールドがない
      // これにより、以下のパターンが削除対象になる：
      // - companyUrl 且つ contactFormUrl のみ
      // - companyUrl のみ
      // - contactFormUrl のみ
      if ((hasCompanyUrl || hasContactFormUrl) && !hasOtherValidFields(data)) {
        candidates += 1;

        let urlInfo = "";
        if (hasCompanyUrl && hasContactFormUrl) {
          urlInfo = "both companyUrl and contactFormUrl";
        } else if (hasCompanyUrl) {
          urlInfo = "companyUrl only";
        } else {
          urlInfo = "contactFormUrl only";
        }

        if (DRY_RUN) {
          const candidateLine = `${doc.id}\t${urlInfo}\n`;
          fs.appendFileSync(candidatesFile, candidateLine, "utf8");
          if (candidates <= 100 || candidates % 1000 === 0) {
            console.log(
              `🗑️ [candidate] docId=${doc.id} (${urlInfo})`
            );
          }
        } else {
          batch.delete(doc.ref);
          batchCount += 1;

          if (batchCount >= BATCH_DELETE_SIZE) {
            await batch.commit();
            deleted += batchCount;
            console.log(
              `💾 Committed delete batch: ${batchCount} docs (total deleted: ${deleted}, scanned: ${scanned})`
            );
            batch = db.batch();
            batchCount = 0;
          }
        }
      }

      if (scanned % 10000 === 0) {
        console.log(
          `📦 scanning... scanned=${scanned}, candidates=${candidates}, deleted=${deleted}`
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
    deleted += batchCount;
    console.log(
      `💾 Committed final delete batch: ${batchCount} docs (total deleted: ${deleted})`
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

  console.log("✅ Cleanup finished");
  console.log(`  🔍 scanned docs : ${scanned}`);
  console.log(`  🗑️ candidates   : ${candidates}`);
  console.log(`  ❌ deleted      : ${deleted} (DRY_RUN=${DRY_RUN})`);
  
  if (DRY_RUN && candidates > 0) {
    console.log(`\n📄 候補リストをファイルに保存しました: ${candidatesFile}`);
    console.log(`   合計 ${candidates} 件の削除候補が記録されています`);
  }
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
