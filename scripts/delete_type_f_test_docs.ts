/*
  タイプF（124.csv）のテストドキュメントを削除するスクリプト
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";

const COLLECTION_NAME = "companies_new";
const DOC_IDS_TO_DELETE = [
  "1764963262070000004", // 株式会社プロスパー
  "1764963262115000005", // クォーク株式会社
  "1764963262171000007", // 東京化学塗料株式会社
  "1764963262227000009", // 富士企画株式会社
];

// Firebase初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: serviceAccount.project_id,
  });

  console.log(`✅ Firebase 初期化完了`);
}

const db = admin.firestore();
const companiesCol = db.collection(COLLECTION_NAME);

async function main() {
  console.log(`\n🗑️  タイプFのテストドキュメントを削除中...\n`);
  
  for (const docId of DOC_IDS_TO_DELETE) {
    const docRef = companiesCol.doc(docId);
    const docSnapshot = await docRef.get();
    
    if (docSnapshot.exists) {
      const data = docSnapshot.data();
      const companyName = data?.name || '(名前不明)';
      await docRef.delete();
      console.log(`✅ 削除完了: ${docId} (${companyName})`);
    } else {
      console.log(`⚠️  ドキュメントが見つかりません: ${docId}`);
    }
  }
  
  console.log(`\n✅ 削除処理完了`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

