/* 
  テストインポート結果を確認するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/check_test_import.ts
*/

import admin from "firebase-admin";
import { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      "./serviceAccountKey.json",
    ];

    for (const defaultPath of defaultPaths) {
      const resolvedPath = require("path").resolve(defaultPath);
      if (require("fs").existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        break;
      }
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ エラー: サービスアカウントキーファイルのパスが指定されていません");
    process.exit(1);
  }

  const serviceAccount = require("fs").readFileSync(serviceAccountPath, "utf8");
  const projectId = JSON.parse(serviceAccount).project_id;

  admin.initializeApp({
    credential: admin.credential.cert(JSON.parse(serviceAccount)),
    projectId,
  });
}

const db: Firestore = admin.firestore();
const companiesCol = db.collection(COLLECTION_NAME);

async function main() {
  const testCompanies = [
    "丹羽興業株式会社",
    "株式会社やぶやグループ",
    "藤吉工業株式会社",
    "株式会社テクニカルスタッフ",
    "株式会社ジュニアー",
  ];

  console.log("📄 テストインポート結果を確認します\n");

  for (const companyName of testCompanies) {
    const snap = await companiesCol
      .where("name", "==", companyName)
      .limit(1)
      .get();

    if (!snap.empty) {
      const doc = snap.docs[0];
      const data = doc.data();
      
      console.log(`\n${"=".repeat(80)}`);
      console.log(`${companyName}`);
      console.log(`docId: ${doc.id}`);
      console.log(`\n業種:`);
      console.log(`  industryLarge: ${data.industryLarge || ""}`);
      console.log(`  industryMiddle: ${data.industryMiddle || ""}`);
      console.log(`  industrySmall: ${data.industrySmall || ""}`);
      console.log(`  industryDetail: ${data.industryDetail || ""}`);
      console.log(`  industry: ${data.industry || ""}`);
      console.log(`  industries: ${JSON.stringify(data.industries || [])}`);
      console.log(`\n住所:`);
      console.log(`  postalCode: ${data.postalCode || ""}`);
      console.log(`  address: ${data.address || ""}`);
      console.log(`  headquartersAddress: ${data.headquartersAddress || ""}`);
      console.log(`\n財務:`);
      console.log(`  capitalStock: ${data.capitalStock || ""}`);
      console.log(`  latestRevenue: ${data.latestRevenue || ""}`);
      console.log(`  latestProfit: ${data.latestProfit || ""}`);
      console.log(`  financials: ${JSON.stringify(data.financials || {})}`);
    } else {
      console.log(`\n${"=".repeat(80)}`);
      console.log(`${companyName} → 見つかりませんでした`);
    }
  }

  console.log(`\n${"=".repeat(80)}`);
  console.log("✅ 確認完了");
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});
