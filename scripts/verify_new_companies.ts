/* 
  新しく追加した10社がcompanies_newコレクションに正常に保存されているか確認
*/

import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// 追加されたドキュメントIDとタイプのマッピング
const NEW_COMPANIES: Record<string, string> = {
  "A": "YapwvpPy6P5Ag3HgQSbb",
  "B": "nWaQxIQIZEQUK9Bk49V4",
  "C": "RDUjjfEINYCGkEtxdu6y",
  "D": "J6NpQJNdKQKximg7Ddb8",
  "E": "T26SGgSC2iN9Y7wTB059",
  "F": "Da1bklitrNuy1PRFWaLS",
  "G": "Aoh1ZtNAMbpCpV1GudQV",
  "H": "wjfEcA3qkWgDlVmAkAmx",
  "I": "rfODM79w8VPGnadmd8yy",
  "J": "FVCBXMICk0bzVEkZzxZv",
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
  console.log("companies_newコレクション 新規追加データの確認");
  console.log("================================================================================");
  console.log();

  const results: any[] = [];
  let successCount = 0;
  let failCount = 0;

  for (const [type, docId] of Object.entries(NEW_COMPANIES)) {
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
        });
        continue;
      }

      successCount++;
      
      // フィールド数をカウント
      const totalFields = Object.keys(data).length;
      const nonNullFields = Object.values(data).filter(v => {
        if (v === null || v === undefined) return false;
        if (Array.isArray(v) && v.length === 0) return false;
        return true;
      }).length;
      
      results.push({
        type,
        docId,
        status: "✅ 正常",
        name: data.name || "(名前なし)",
        corporateNumber: data.corporateNumber || "(なし)",
        address: data.address || "(なし)",
        phoneNumber: data.phoneNumber || "(なし)",
        totalFields,
        nonNullFields,
        fillRate: ((nonNullFields / totalFields) * 100).toFixed(1),
      });

    } catch (error: any) {
      console.log(`❌ タイプ${type}: エラー - ${error.message} (ID: ${docId})`);
      failCount++;
      results.push({
        type,
        status: "❌ エラー",
        name: "-",
        corporateNumber: "-",
      });
    }
  }

  console.log("================================================================================");
  console.log("確認結果一覧");
  console.log("================================================================================\n");

  // テーブルヘッダー
  console.log("タイプ | ステータス | 企業名");
  console.log("-------|------------|--------------------------------");

  // 各行を表示
  for (const result of results) {
    const name = String(result.name).substring(0, 30).padEnd(30, " ");
    console.log(`  ${result.type}    | ${result.status}   | ${name}`);
  }

  console.log("\n================================================================================");
  console.log("詳細情報");
  console.log("================================================================================\n");

  for (const result of results.filter(r => r.status === "✅ 正常")) {
    console.log(`【タイプ${result.type}】 ${result.name}`);
    console.log(`  ドキュメントID: ${result.docId}`);
    console.log(`  法人番号: ${result.corporateNumber}`);
    console.log(`  住所: ${result.address}`);
    console.log(`  電話番号: ${result.phoneNumber}`);
    console.log(`  総フィールド数: ${result.totalFields}`);
    console.log(`  値が入っているフィールド数: ${result.nonNullFields}`);
    console.log(`  データ充実度: ${result.fillRate}%`);
    console.log();
  }

  console.log("================================================================================");
  console.log("統計サマリー");
  console.log("================================================================================\n");

  console.log(`総件数: ${results.length}件`);
  console.log(`✅ 正常: ${successCount}件`);
  console.log(`❌ 異常: ${failCount}件`);
  
  if (successCount === results.length) {
    console.log("\n🎉 全ての企業データが正常にcompanies_newコレクションに保存されています！");
    console.log("\n【追加されたドキュメントID一覧】\n");
    for (const [type, docId] of Object.entries(NEW_COMPANIES)) {
      console.log(`タイプ${type}: ${docId}`);
    }
  } else {
    console.log(`\n⚠️  ${failCount}件の企業データに問題があります。`);
  }
  
  console.log("\n================================================================================");
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

