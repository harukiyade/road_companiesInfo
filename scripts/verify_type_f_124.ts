/*
  タイプF（124.csv）のテストドキュメントを確認するスクリプト
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";

const COLLECTION_NAME = "companies_new";
const DOC_IDS = [
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
  console.log(`\n🔍 タイプFのテストドキュメントを確認中...\n`);
  
  for (const docId of DOC_IDS) {
    const docRef = companiesCol.doc(docId);
    const docSnapshot = await docRef.get();
    
    if (docSnapshot.exists) {
      const data = docSnapshot.data();
      console.log(`✅ ${docId}`);
      console.log(`   会社名: ${data?.name || '(空)'}`);
      console.log(`   都道府県: ${data?.prefecture || '(空)'}`);
      console.log(`   代表者名: ${data?.representativeName || '(空)'}`);
      console.log(`   郵便番号: ${data?.postalCode || '(空)'}`);
      console.log(`   住所: ${data?.address || '(空)'}`);
      console.log(`   業種1: ${data?.industryLarge || '(空)'}`);
      console.log(`   業種2: ${data?.industryMiddle || '(空)'}`);
      console.log(`   業種3: ${data?.industrySmall || '(空)'}`);
      console.log(`   業種4: ${data?.industryDetail || '(空)'}`);
      console.log(`   業種カテゴリ: ${JSON.stringify(data?.industryCategories || [])}`);
      console.log(`   csvType: ${data?.csvType || '(空)'}`);
      console.log();
    } else {
      console.log(`❌ ${docId}: ドキュメントが見つかりません`);
      console.log();
    }
  }
  
  console.log(`✅ 確認完了`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

