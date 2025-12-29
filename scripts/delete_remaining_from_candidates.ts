// scripts/delete_remaining_from_candidates.ts
//
// delete_urls_candidates.txt からドキュメントIDを読み込み、
// まだ存在するドキュメントを削除するスクリプトです。
//
// 実行例:
//   GOOGLE_APPLICATION_CREDENTIALS="./albert-ma-firebase-adminsdk-iat1k-a64039899f.json" \
//   npx ts-node scripts/delete_remaining_from_candidates.ts

import admin from "firebase-admin";
import * as fs from "fs";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";
const CANDIDATES_FILE = "delete_urls_candidates.txt";

// 1 バッチで削除する件数（Firestore の上限 500 未満にする）
const BATCH_DELETE_SIZE = 400;

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

async function main() {
  if (!fs.existsSync(CANDIDATES_FILE)) {
    console.error(`❌ エラー: 候補リストファイルが見つかりません: ${CANDIDATES_FILE}`);
    process.exit(1);
  }

  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  // 候補リストを読み込む
  const candidatesContent = fs.readFileSync(CANDIDATES_FILE, "utf8");
  const lines = candidatesContent.trim().split("\n");
  const docIds = lines.map(line => line.split("\t")[0]).filter(id => id);

  console.log(`📄 候補リストから ${docIds.length} 件のドキュメントIDを読み込みました`);

  let checked = 0;
  let existing = 0;
  let deleted = 0;
  let batch = db.batch();
  let batchCount = 0;

  for (const docId of docIds) {
    checked += 1;

    try {
      const docRef = colRef.doc(docId);
      const doc = await docRef.get();

      if (doc.exists) {
        existing += 1;
        batch.delete(docRef);
        batchCount += 1;

        if (batchCount >= BATCH_DELETE_SIZE) {
          await batch.commit();
          deleted += batchCount;
          console.log(
            `💾 Committed delete batch: ${batchCount} docs (total deleted: ${deleted}, checked: ${checked}/${docIds.length})`
          );
          batch = db.batch();
          batchCount = 0;
        }
      }

      if (checked % 1000 === 0) {
        console.log(
          `📦 checking... checked=${checked}/${docIds.length}, existing=${existing}, deleted=${deleted}`
        );
      }
    } catch (error) {
      console.error(`❌ Error checking docId ${docId}: ${error}`);
    }
  }

  // 最後のバッチをコミット
  if (batchCount > 0) {
    await batch.commit();
    deleted += batchCount;
    console.log(
      `💾 Committed final delete batch: ${batchCount} docs (total deleted: ${deleted})`
    );
  }

  console.log("✅ Cleanup finished");
  console.log(`  🔍 checked docs  : ${checked}`);
  console.log(`  📦 existing docs  : ${existing}`);
  console.log(`  ❌ deleted       : ${deleted}`);
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
