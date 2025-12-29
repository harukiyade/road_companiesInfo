/* 
  「丹羽興業株式会社」のドキュメントを確認するスクリプト
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

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
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

async function main() {
  const companyName = "丹羽興業株式会社";
  const corporateNumber = "9180000000000";
  
  console.log(`🔍 「${companyName}」のドキュメントを検索中...\n`);
  
  // 企業名で検索
  const snapByName = await companiesCol
    .where("name", "==", companyName)
    .limit(10)
    .get();
  
  console.log(`📊 企業名で検索: ${snapByName.size} 件\n`);
  
  if (snapByName.size === 0) {
    console.log(`⚠️  ドキュメントが見つかりませんでした`);
    console.log(`\n💡 インポートを再実行してください:`);
    console.log(`   export GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json`);
    console.log(`   npx ts-node scripts/import_companies_from_csv.ts csv/116.csv`);
    return;
  }
  
  for (const doc of snapByName.docs) {
    const data = doc.data();
    console.log(`${"=".repeat(60)}`);
    console.log(`📄 ドキュメントID: ${doc.id}`);
    console.log(`${"=".repeat(60)}`);
    console.log(`企業名: ${data.name || '(なし)'}`);
    console.log(`法人番号: ${data.corporateNumber || '(なし)'}`);
    console.log(`住所: ${data.address || '(なし)'}`);
    console.log(`説明: ${data.companyDescription || '(なし)'}`);
    console.log(`概要: ${data.overview ? (data.overview.length > 100 ? data.overview.substring(0, 100) + '...' : data.overview) : '(なし)'}`);
    console.log(`取引先: ${data.clients || '(なし)'}`);
    console.log(`仕入れ先: ${Array.isArray(data.suppliers) ? data.suppliers.join(', ') : (data.suppliers || '(なし)')}`);
    console.log(`取引先銀行: ${Array.isArray(data.banks) ? data.banks.join(', ') : (data.banks || '(なし)')}`);
    console.log(`取締役: ${data.executives || '(なし)'}`);
    console.log(`株主: ${data.shareholders || '(なし)'}`);
    console.log(`資本金: ${data.capitalStock ? data.capitalStock.toLocaleString() + '円' : '(なし)'}`);
    console.log(`直近売上: ${data.revenue ? data.revenue.toLocaleString() + '円' : '(なし)'}`);
    console.log(`直近利益: ${data.latestProfit ? data.latestProfit.toLocaleString() + '円' : '(なし)'}`);
    console.log(`URL: ${data.companyUrl || '(なし)'}`);
    console.log(`\n`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
