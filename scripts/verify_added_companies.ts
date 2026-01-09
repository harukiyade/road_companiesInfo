/* 
  追加した各タイプの企業データをcompanies_newコレクションから取得して確認するスクリプト
*/

import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// 追加されたドキュメントIDとタイプのマッピング
const ADDED_COMPANIES: Record<string, string> = {
  "A": "wnPspUkcfFcb3Qz7zjuB",
  "B": "8QYZZEMVp2THCO9wNpEY",
  "C": "o5DoyvVwxfnI227rg52Y",
  "D": "hCbGuFYwMzyZlwCrfj1T",
  "E": "mFu0zOpOk63POUirjGIs",
  "F": "KmgKFCRYgBHAO4aBEnyu",
  "G": "yAdIfuyx3OmCkqGWjOIs",
  "H": "GGlcAaYbxBJYfRvK1HhN",
  "I": "YJ8wLD9dIbkqXSR5VMxm",
  "J": "QtAp1FMaDaFZYEMLPcuj",
};

// Firebase初期化
if (!admin.apps.length) {
  const serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || 
    path.join(__dirname, "../albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
  
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccountPath)
  });
}

const db: Firestore = admin.firestore();

/**
 * 重要なフィールドのみを抽出
 */
function extractKeyFields(data: any): Record<string, any> {
  return {
    name: data.name || "(なし)",
    corporateNumber: data.corporateNumber || "(なし)",
    phoneNumber: data.phoneNumber || "(なし)",
    postalCode: data.postalCode || "(なし)",
    address: data.address || "(なし)",
    prefecture: data.prefecture || "(なし)",
    representativeName: data.representativeName || "(なし)",
    established: data.established || "(なし)",
    companyUrl: data.companyUrl || "(なし)",
    industry: data.industry || "(なし)",
    industryLarge: data.industryLarge || "(なし)",
    csvType: data.csvType || "(なし)",
    csvSource: data.csvSource || "(なし)",
  };
}

/**
 * タイプ別の特徴的なフィールドを取得
 */
function getTypeSpecificFields(type: string, data: any): Record<string, any> {
  switch (type) {
    case "A":
    case "B":
      return {
        businessDescriptions: data.businessDescriptions || "(なし)",
        shareholders: data.shareholders || "(なし)",
        executives: data.executives || "(なし)",
      };
    case "C":
      return {
        foundingYear: data.foundingYear || "(なし)",
      };
    case "D":
    case "E":
      return {
        clients: data.clients || "(なし)",
        suppliers: data.suppliers || "(なし)",
        revenue: data.revenue || "(なし)",
      };
    case "F":
      return {
        companyDescription: data.companyDescription || "(なし)",
        overview: data.overview || "(なし)",
      };
    case "G":
      return {
        banks: data.banks || [],
        nameEn: data.nameEn || "(なし)",
      };
    case "H":
      return {
        executivesData: data.executivesData || [],
        bankCorporateNumber: data.bankCorporateNumber || "(なし)",
      };
    case "I":
      return {
        financialsData: data.financialsData || [],
      };
    case "J":
      return {
        departmentsData: data.departmentsData || [],
      };
    default:
      return {};
  }
}

async function main() {
  console.log("=".repeat(80));
  console.log("追加した企業データの確認");
  console.log("=".repeat(80));
  console.log();

  for (const [type, docId] of Object.entries(ADDED_COMPANIES)) {
    console.log(`\n${"=".repeat(80)}`);
    console.log(`タイプ${type}: ドキュメントID = ${docId}`);
    console.log("=".repeat(80));

    try {
      const docRef = db.collection(COLLECTION_NAME).doc(docId);
      const docSnap = await docRef.get();

      if (!docSnap.exists) {
        console.log("❌ ドキュメントが見つかりません");
        continue;
      }

      const data = docSnap.data();
      if (!data) {
        console.log("❌ データが空です");
        continue;
      }

      console.log("\n【基本情報】");
      const keyFields = extractKeyFields(data);
      for (const [key, value] of Object.entries(keyFields)) {
        console.log(`  ${key}: ${value}`);
      }

      console.log(`\n【タイプ${type}の特徴的なフィールド】`);
      const typeSpecific = getTypeSpecificFields(type, data);
      for (const [key, value] of Object.entries(typeSpecific)) {
        if (Array.isArray(value)) {
          console.log(`  ${key}: [${value.length}件]`);
          if (value.length > 0) {
            console.log(`    ${JSON.stringify(value[0])}`);
          }
        } else {
          const displayValue = typeof value === 'string' && value.length > 100 
            ? value.substring(0, 100) + "..." 
            : value;
          console.log(`  ${key}: ${displayValue}`);
        }
      }

      // フィールド数を確認
      const fieldCount = Object.keys(data).length;
      console.log(`\n【統計】`);
      console.log(`  総フィールド数: ${fieldCount}`);
      console.log(`  null以外のフィールド数: ${Object.values(data).filter(v => v !== null).length}`);

      console.log("\n✓ データ取得成功");
    } catch (error: any) {
      console.error(`❌ エラー: ${error.message}`);
    }
  }

  console.log("\n" + "=".repeat(80));
  console.log("確認完了");
  console.log("=".repeat(80));
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

