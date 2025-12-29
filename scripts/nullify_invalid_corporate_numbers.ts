// scripts/nullify_invalid_corporate_numbers.ts
//
// companies_new コレクション上で、
// 法人番号フィールドに13桁の数値が入っていないものは、
// その法人番号フィールドのみを削除（nullに設定）するスクリプトです。
//
// 実行例:
//   DRY_RUN=1 npx ts-node scripts/nullify_invalid_corporate_numbers.ts   // 更新せず候補だけログ
//   npx ts-node scripts/nullify_invalid_corporate_numbers.ts             // 実際に更新
//
// 再開オプション:
//   START_FROM_DOC_ID="docId123" npx ts-node scripts/nullify_invalid_corporate_numbers.ts  // 特定のドキュメントIDから再開
//   SKIP_SCANNED=2110000 npx ts-node scripts/nullify_invalid_corporate_numbers.ts            // スキャン件数をスキップ（非効率）
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
const SKIP_SCANNED = process.env.SKIP_SCANNED ? parseInt(process.env.SKIP_SCANNED, 10) : 0;

// 法人番号パターン（13桁の数値）
const CORPORATE_NUMBER_PATTERN = /^\d{13}$/;

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

// 文字列正規化
function norm(s: string | null | undefined): string {
  if (!s) return "";
  return s.toString().trim();
}

// 法人番号を検証（13桁の数値のみ有効）
function validateCorporateNumber(value: string | null | undefined): string | null {
  const v = norm(value);
  if (!v) return null;
  
  // 科学記数法（9.18E+12など）は壊れているので無視
  if (/^\d+\.\d+E\+\d+$/i.test(v) || /^\d+\.\d+E-\d+$/i.test(v) || /E/i.test(v)) {
    return null;
  }
  
  // 通常の数値文字列を処理（13桁の数値のみ）
  const digits = v.replace(/\D/g, "");
  if (digits.length === 13 && CORPORATE_NUMBER_PATTERN.test(digits)) {
    return digits;
  }
  
  return null;
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
  const checkpointFile = "nullify_corporate_number_checkpoint.txt";

  console.log(
    `🔍 Scan start: collection="${COLLECTION_NAME}", pageSize=${PAGE_SIZE}, batchUpdateSize=${BATCH_UPDATE_SIZE}, DRY_RUN=${DRY_RUN}`
  );
  if (SKIP_SCANNED > 0) {
    console.log(`⏭️  Will skip first ${SKIP_SCANNED} scanned documents`);
  }
  
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
      
      // スキップオプション: 指定された件数までスキップ
      if (SKIP_SCANNED > 0 && scanned <= SKIP_SCANNED) {
        if (scanned % 10000 === 0) {
          console.log(`⏭️  Skipping... scanned=${scanned}/${SKIP_SCANNED}`);
        }
        lastDoc = doc as FirebaseFirestore.QueryDocumentSnapshot;
        continue;
      }

      const data = doc.data();
      const corporateNumber = (data as any).corporateNumber;

      // corporateNumberフィールドが存在し、13桁の数値でない場合
      if (corporateNumber !== null && corporateNumber !== undefined) {
        const validated = validateCorporateNumber(corporateNumber);
        
        // 検証に失敗した場合（nullが返された場合）、nullに設定
        if (validated === null) {
          candidates += 1;

          if (DRY_RUN) {
            console.log(
              `🔧 [candidate] docId=${doc.id} (corporateNumber="${corporateNumber}" is invalid, will be set to null)`
            );
          } else {
            batch.update(doc.ref, { corporateNumber: null });
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
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
