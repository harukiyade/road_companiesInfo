/**
 * 特定の企業のFirestoreデータを確認するスクリプト
 */

import * as admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

// 確認したい法人番号またはドキュメントID
const TARGET_CORPORATE_NUMBER = "2180001031124"; // 株式会社タイルメント
const TARGET_DOC_ID = "175816"; // 企業ID

// Firebase初期化
function initializeFirebase() {
  const projectRoot = process.cwd();
  const defaultPaths = [
    "./serviceAccountKey.json",
    "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
    path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
  ];

  let serviceAccountPath: string | null = null;
  for (const p of defaultPaths) {
    if (fs.existsSync(p)) {
      serviceAccountPath = p;
      break;
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ エラー: サービスアカウントキーが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  
  if (!admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  }
  
  return admin.firestore();
}

async function checkCompanyData() {
  const db = initializeFirebase();
  
  // 両方のコレクションを確認
  const collections = ["companies_new", "companies"];
  
  for (const colName of collections) {
    console.log(`\n🔍 ${colName} コレクションを検索中...`);
    const col = db.collection(colName);
    
    // ドキュメントID「175816」で検索
    const byDocId = await col.doc(TARGET_DOC_ID).get();
    if (byDocId.exists) {
      console.log(`✅ [${colName}] ドキュメントID: ${byDocId.id}\n`);
      printData(byDocId.data() as Record<string, any>);
      return;
    }
    
    // ドキュメントIDとして検索
    const byId = await col.doc(TARGET_CORPORATE_NUMBER).get();
    if (byId.exists) {
      console.log(`✅ [${colName}] ドキュメントID: ${byId.id}\n`);
      printData(byId.data() as Record<string, any>);
      return;
    }
    
    // corporateNumberフィールドで検索
    const snap = await col.where("corporateNumber", "==", TARGET_CORPORATE_NUMBER).limit(1).get();
    if (!snap.empty) {
      const doc = snap.docs[0];
      console.log(`✅ [${colName}] ドキュメントID: ${doc.id}\n`);
      printData(doc.data() as Record<string, any>);
      return;
    }
  }
  
  console.log("\n❌ データが見つかりませんでした");
}

function printData(data: Record<string, any>) {
  // 重要なフィールドを先に表示
  const priorityFields = [
    "name",
    "corporateNumber",
    "representativeName",
    "address",
    "postalCode",
    "phoneNumber",
    "fax",
    "email",
    "companyUrl",
    "prefecture",
    "established",
    "capitalStock",
    "employeeCount",
    "listing",
    "fiscalMonth",
    "revenue",
    "executives",
    "shareholders",
    "overview",
    "companyDescription",
    "businessDescriptions",
    "industryLarge",
    "industryMiddle",
    "industrySmall",
    "industryDetail",
    "suppliers",
    "clients",
    "officeCount",
    "factoryCount",
    "storeCount",
    "representativeBirthDate",
    "representativeHomeAddress",
    "metaDescription",
    "metaKeywords",
    "salesNotes",
    "tags",
  ];
  
  console.log("========================================");
  console.log("📊 企業データ");
  console.log("========================================\n");
  
  for (const field of priorityFields) {
    const value = data[field];
    if (value !== null && value !== undefined && value !== "" && 
        !(Array.isArray(value) && value.length === 0)) {
      const displayValue = typeof value === "string" && value.length > 100
        ? value.substring(0, 100) + "..."
        : JSON.stringify(value);
      console.log(`${field}: ${displayValue}`);
    }
  }
  
  console.log("\n========================================");
  console.log("📋 全フィールド（未設定含む）");
  console.log("========================================\n");
  
  for (const [field, value] of Object.entries(data)) {
    const displayValue = typeof value === "string" && value.length > 80
      ? value.substring(0, 80) + "..."
      : JSON.stringify(value);
    console.log(`${field}: ${displayValue}`);
  }
}

checkCompanyData().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

