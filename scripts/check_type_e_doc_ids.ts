/*
  タイプE（116.csv）の3つのドキュメントのIDを確認するスクリプト
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const COMPANY_NAMES = [
  "株式会社ワールド・アメニティー",
  "株式会社ジックス",
  "株式会社興和アークビルド"
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

const db: Firestore = admin.firestore();
const companiesCol = db.collection(COLLECTION_NAME);

async function main() {
  console.log("🔍 対象企業のドキュメントIDを検索中...\n");

  for (const companyName of COMPANY_NAMES) {
    console.log(`📋 検索中: ${companyName}`);
    
    const snapshot = await companiesCol
      .where("name", "==", companyName)
      .limit(5)
      .get();

    if (snapshot.empty) {
      console.log(`  ⚠️  見つかりませんでした\n`);
    } else {
      for (const doc of snapshot.docs) {
        const data = doc.data();
        console.log(`  ✅ docId: ${doc.id}`);
        console.log(`     会社名: ${data.name}`);
        console.log(`     法人番号: ${data.corporateNumber || '(null)'}`);
        console.log(`     住所: ${data.address || '(null)'}`);
        console.log();
      }
    }
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

