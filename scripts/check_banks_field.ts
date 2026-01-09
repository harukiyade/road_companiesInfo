/* 
  取引先銀行フィールドを確認するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/check_banks_field.ts
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

  console.log("📄 取引先銀行フィールドを確認します\n");

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
      console.log(`\n取引先銀行:`);
      console.log(`  banks: ${JSON.stringify(data.banks || [])}`);
      console.log(`  mainBanks: ${data.mainBanks || ""}`);
      console.log(`  bank: ${data.bank || ""}`);
      console.log(`  salesNotes: ${data.salesNotes || ""}`);
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
