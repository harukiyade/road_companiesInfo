/* 
  116.csvに含まれる企業をcompanies_newコレクションから削除するスクリプト
  nameフィールドで検索して削除
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import { parse } from "csv-parse/sync";
import type { Firestore, CollectionReference } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_FILE = "csv/116.csv";
const DRY_RUN = process.argv.includes("--dry-run");

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
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  
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
    console.error(`❌ エラー: CSVファイルにデータ行がありません（総行数: ${records.length}）`);
    process.exit(1);
  }
  
  const headers = records[0];
  const nameIndex = headers.findIndex(h => h === "会社名" || h === "企業名" || h === "name");
  
  if (nameIndex === -1) {
    console.error(`❌ エラー: 会社名カラムが見つかりません`);
    process.exit(1);
  }
  
  // 企業名を抽出
  const companyNames = new Set<string>();
  for (let i = 1; i < records.length; i++) {
    const companyName = records[i][nameIndex]?.trim();
    if (companyName && companyName !== "") {
      companyNames.add(companyName);
    }
  }
  
  console.log(`📄 CSVファイル: ${CSV_FILE}`);
  console.log(`📊 抽出された企業名: ${companyNames.size} 件\n`);
  
  // 各企業名で検索して削除
  const deletedIds: string[] = [];
  const notFoundNames: string[] = [];
  
  for (const companyName of companyNames) {
    const snapshot = await companiesCol
      .where("name", "==", companyName)
      .limit(100)
      .get();
    
    if (snapshot.empty) {
      notFoundNames.push(companyName);
      continue;
    }
    
    for (const doc of snapshot.docs) {
      deletedIds.push(doc.id);
      if (DRY_RUN) {
        console.log(`  🔍 (DRY_RUN) 削除予定: ${doc.id} (${companyName})`);
      } else {
        await doc.ref.delete();
        console.log(`  ✅ 削除完了: ${doc.id} (${companyName})`);
      }
    }
  }
  
  console.log(`\n${"=".repeat(60)}`);
  console.log(`📊 削除結果サマリー`);
  console.log(`${"=".repeat(60)}`);
  console.log(`削除対象: ${deletedIds.length} 件`);
  console.log(`見つからなかった企業名: ${notFoundNames.length} 件`);
  
  if (notFoundNames.length > 0 && notFoundNames.length <= 10) {
    console.log(`\n見つからなかった企業名:`);
    notFoundNames.forEach(name => console.log(`  - ${name}`));
  } else if (notFoundNames.length > 10) {
    console.log(`\n見つからなかった企業名（最初の10件）:`);
    notFoundNames.slice(0, 10).forEach(name => console.log(`  - ${name}`));
  }
  
  console.log(`\n✅ 処理完了`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に削除するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
