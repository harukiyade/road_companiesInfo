/* 
  116.csvからインポートされたドキュメントの状態を確認するスクリプト
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import { parse } from "csv-parse/sync";
import type { Firestore, CollectionReference } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_FILE = "csv/116.csv";

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
  console.log(`📄 CSVファイルを読み込み中: ${CSV_FILE}\n`);
  
  // CSVファイルを読み込み
  const filePath = path.resolve(CSV_FILE);
  if (!fs.existsSync(filePath)) {
    console.error(`❌ ファイルが見つかりません: ${CSV_FILE}`);
    process.exit(1);
  }
  
  const buf = fs.readFileSync(filePath);
  const records: Array<Array<string>> = parse(buf, {
    columns: false,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    skip_records_with_error: true,
  });
  
  if (records.length < 2) {
    console.error(`❌ エラー: CSVファイルにデータ行がありません`);
    process.exit(1);
  }
  
  const headers = records[0];
  const nameIndex = headers.findIndex(h => h === "会社名" || h === "企業名" || h === "name");
  
  if (nameIndex === -1) {
    console.error(`❌ エラー: 会社名カラムが見つかりません`);
    process.exit(1);
  }
  
  // 最初の10件と最後の10件、および丹羽興業株式会社を確認
  const checkRows = [1, 2, 3, 4, 5, records.length - 5, records.length - 4, records.length - 3, records.length - 2, records.length - 1];
  const niwaIndex = records.findIndex((row, idx) => idx > 0 && row[nameIndex]?.trim() === "丹羽興業株式会社");
  if (niwaIndex > 0) {
    checkRows.push(niwaIndex);
  }
  
  const uniqueRows = Array.from(new Set(checkRows)).sort((a, b) => a - b);
  
  console.log(`📊 確認対象: ${uniqueRows.length} 件\n`);
  
  let foundCount = 0;
  let notFoundCount = 0;
  
  for (const rowIndex of uniqueRows) {
    if (rowIndex < 1 || rowIndex >= records.length) continue;
    
    const row = records[rowIndex];
    const companyName = row[nameIndex]?.trim();
    
    if (!companyName || companyName === "") continue;
    
    const snapshot = await companiesCol
      .where("name", "==", companyName)
      .limit(5)
      .get();
    
    if (snapshot.empty) {
      console.log(`❌ 見つからない: ${companyName} (CSV行: ${rowIndex + 1})`);
      notFoundCount++;
    } else {
      for (const doc of snapshot.docs) {
        const data = doc.data();
        console.log(`✅ 見つかった: ${companyName}`);
        console.log(`   ドキュメントID: ${doc.id}`);
        console.log(`   法人番号: ${data.corporateNumber || '(なし)'}`);
        console.log(`   住所: ${data.address ? (data.address.length > 50 ? data.address.substring(0, 50) + '...' : data.address) : '(なし)'}`);
        console.log(`   説明: ${data.companyDescription ? (data.companyDescription.length > 50 ? data.companyDescription.substring(0, 50) + '...' : data.companyDescription) : '(なし)'}`);
        console.log(``);
        foundCount++;
        break; // 最初の1件のみ表示
      }
    }
  }
  
  console.log(`${"=".repeat(60)}`);
  console.log(`📊 確認結果サマリー`);
  console.log(`${"=".repeat(60)}`);
  console.log(`✅ 見つかった: ${foundCount} 件`);
  console.log(`❌ 見つからない: ${notFoundCount} 件`);
  console.log(`\n✅ 確認完了`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
