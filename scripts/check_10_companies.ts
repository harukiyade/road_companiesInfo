/* 
  追加した10社がcompanies_newコレクションに正常に保存されているか確認
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

async function main() {
  console.log("================================================================================");
  console.log("companies_newコレクション 追加データ確認");
  console.log("================================================================================");
  console.log();

  const results: any[] = [];
  let successCount = 0;
  let failCount = 0;

  for (const [type, docId] of Object.entries(ADDED_COMPANIES)) {
    try {
      const docRef = db.collection(COLLECTION_NAME).doc(docId);
      const docSnap = await docRef.get();

      if (!docSnap.exists) {
        console.log(`❌ タイプ${type}: ドキュメントが見つかりません (ID: ${docId})`);
        failCount++;
        results.push({
          type,
          status: "❌ 見つかりません",
          name: "-",
          corporateNumber: "-",
          csvSource: "-",
        });
        continue;
      }

      const data = docSnap.data();
      if (!data) {
        console.log(`❌ タイプ${type}: データが空です (ID: ${docId})`);
        failCount++;
        results.push({
          type,
          status: "❌ データ空",
          name: "-",
          corporateNumber: "-",
          csvSource: "-",
        });
        continue;
      }

      successCount++;
      results.push({
        type,
        status: "✅ 正常",
        name: data.name || "(名前なし)",
        corporateNumber: data.corporateNumber || "(なし)",
        csvSource: data.csvSource || "(不明)",
        fieldCount: Object.keys(data).length,
        nonNullCount: Object.values(data).filter(v => v !== null && v !== undefined).length,
      });

    } catch (error: any) {
      console.log(`❌ タイプ${type}: エラー - ${error.message} (ID: ${docId})`);
      failCount++;
      results.push({
        type,
        status: "❌ エラー",
        name: "-",
        corporateNumber: "-",
        csvSource: "-",
      });
    }
  }

  console.log("\n================================================================================");
  console.log("確認結果一覧");
  console.log("================================================================================\n");

  // テーブルヘッダー
  console.log("タイプ | ステータス | 企業名                          | 法人番号          | CSVソース");
  console.log("-------|------------|--------------------------------|------------------|-------------");

  // 各行を表示
  for (const result of results) {
    const name = result.name.substring(0, 30).padEnd(30, " ");
    const corpNum = String(result.corporateNumber).substring(0, 16).padEnd(16, " ");
    const csvSource = result.csvSource.substring(0, 15);
    
    console.log(`  ${result.type}    | ${result.status}   | ${name} | ${corpNum} | ${csvSource}`);
  }

  console.log("\n================================================================================");
  console.log("統計情報");
  console.log("================================================================================\n");

  console.log(`総件数: ${results.length}件`);
  console.log(`✅ 正常: ${successCount}件`);
  console.log(`❌ 異常: ${failCount}件`);
  
  if (successCount > 0) {
    console.log("\n【正常に保存されている企業の詳細】\n");
    
    for (const result of results.filter(r => r.status === "✅ 正常")) {
      console.log(`タイプ${result.type}: ${result.name}`);
      console.log(`  - 法人番号: ${result.corporateNumber}`);
      console.log(`  - データソース: ${result.csvSource}`);
      console.log(`  - 総フィールド数: ${result.fieldCount}`);
      console.log(`  - 値が入っているフィールド数: ${result.nonNullCount}`);
      console.log(`  - データ充実度: ${((result.nonNullCount / result.fieldCount) * 100).toFixed(1)}%`);
      console.log();
    }
  }

  console.log("================================================================================");
  
  if (successCount === results.length) {
    console.log("🎉 全ての企業データが正常にcompanies_newコレクションに保存されています！");
  } else {
    console.log(`⚠️  ${failCount}件の企業データに問題があります。`);
  }
  
  console.log("================================================================================");
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

