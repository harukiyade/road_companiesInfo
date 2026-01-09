/*
  タイプAの重複チェックスクリプト
  
  企業名+住所などで同じ企業を特定して重複を検出します。
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/check_duplicates_type_a.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";

// タイプAのCSVファイル一覧
const TYPE_A_FILES = [
  "csv/7.csv", "csv/8.csv", "csv/9.csv", "csv/10.csv", "csv/11.csv",
  "csv/12.csv", "csv/13.csv", "csv/14.csv", "csv/15.csv", "csv/16.csv",
  "csv/17.csv", "csv/18.csv", "csv/19.csv", "csv/20.csv", "csv/21.csv",
  "csv/22.csv", "csv/25.csv", "csv/26.csv", "csv/27.csv", "csv/28.csv",
  "csv/29.csv", "csv/30.csv", "csv/31.csv", "csv/32.csv", "csv/33.csv",
  "csv/34.csv", "csv/35.csv", "csv/39.csv", "csv/52.csv", "csv/54.csv",
  "csv/55.csv", "csv/56.csv", "csv/57.csv", "csv/58.csv", "csv/59.csv",
  "csv/60.csv", "csv/61.csv", "csv/62.csv", "csv/63.csv", "csv/64.csv",
  "csv/65.csv", "csv/66.csv", "csv/67.csv", "csv/68.csv", "csv/69.csv",
  "csv/70.csv", "csv/71.csv", "csv/72.csv", "csv/73.csv", "csv/74.csv",
  "csv/75.csv", "csv/76.csv", "csv/77.csv", "csv/101.csv", "csv/104.csv"
];

// Firebase初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
      console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${defaultPath}`);
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId = serviceAccount.project_id;

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });

  console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
}

const db: Firestore = admin.firestore();

function trim(v?: string | null): string | null {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function normalizeStr(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "").replace(/株式会社|有限会社|合同会社|合名会社/g, "");
}

function normalizeAddress(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "");
}

interface CompanyRecord {
  source: string;
  rowIndex: number;
  name: string;
  address: string | null;
  prefecture: string | null;
  postalCode: string | null;
  phoneNumber: string | null;
  corporateNumber: string | null;
  docId: string | null;
}

async function main() {
  console.log("🔍 タイプAの重複チェックを開始します\n");

  const allRecords: CompanyRecord[] = [];

  // CSV から全レコードを読み込み
  for (const file of TYPE_A_FILES) {
    const filePath = path.resolve(file);
    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️  ファイルが見つかりません: ${file}`);
      continue;
    }

    const buf = fs.readFileSync(filePath);
    try {
      const records = parse(buf, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_records_with_error: true,
      });

      console.log(`📄 ${path.basename(file)}: ${records.length} 行`);

      records.forEach((row: any, idx: number) => {
        const name = trim(row["会社名"]) ?? trim(row["企業名"]) ?? trim(row["name"]);
        if (!name) return;

        const address = trim(row["会社住所"]) ?? trim(row["住所"]) ?? trim(row["address"]);
        const prefecture = trim(row["都道府県"]) ?? trim(row["prefecture"]);
        const postalCode = trim(row["会社郵便番号"]) ?? trim(row["郵便番号"]) ?? trim(row["postalCode"]);
        const phoneNumber = trim(row["電話番号"]) ?? trim(row["phone"]) ?? trim(row["phoneNumber"]);
        const corporateNumber = trim(row["法人番号"]) ?? trim(row["corporateNumber"]);

        allRecords.push({
          source: path.basename(file),
          rowIndex: idx + 1,
          name,
          address,
          prefecture,
          postalCode,
          phoneNumber,
          corporateNumber,
          docId: null,
        });
      });
    } catch (err: any) {
      console.warn(`  ⚠️ ${path.basename(file)}: CSVパースエラー - ${err.message}`);
    }
  }

  console.log(`\n📊 総レコード数: ${allRecords.length}`);

  // Firestore から既存データを取得してドキュメントIDをマッピング
  console.log("\n🔎 Firestoreのデータを取得中...");
  const companiesSnap = await db.collection(COLLECTION_NAME).get();
  const companiesMap = new Map<string, any>();
  
  companiesSnap.docs.forEach(doc => {
    const data = doc.data();
    companiesMap.set(doc.id, data);
  });

  console.log(`✅ Firestore: ${companiesMap.size} 件のドキュメントを取得`);

  // 各レコードに対してFirestoreのドキュメントIDを探す
  for (const record of allRecords) {
    // 法人番号で検索
    if (record.corporateNumber) {
      const found = Array.from(companiesMap.entries()).find(([id, data]) => 
        data.corporateNumber === record.corporateNumber
      );
      if (found) {
        record.docId = found[0];
        continue;
      }
    }

    // 企業名+住所で検索
    const normName = normalizeStr(record.name);
    const normAddr = normalizeAddress(record.address);

    const found = Array.from(companiesMap.entries()).find(([id, data]) => {
      const docName = normalizeStr(data.name);
      const docAddr = normalizeAddress(data.address);
      
      if (docName !== normName) return false;
      if (normAddr && docAddr && normAddr === docAddr) return true;
      if (record.postalCode && data.postalCode && record.postalCode === data.postalCode) return true;
      
      return false;
    });

    if (found) {
      record.docId = found[0];
    }
  }

  // 重複検出: 同じdocIdを持つレコードをグループ化
  const duplicateGroups = new Map<string, CompanyRecord[]>();
  
  for (const record of allRecords) {
    if (!record.docId) continue; // Firestoreに存在しないレコードは除外
    
    if (!duplicateGroups.has(record.docId)) {
      duplicateGroups.set(record.docId, []);
    }
    duplicateGroups.get(record.docId)!.push(record);
  }

  // 2件以上のレコードを持つグループのみを抽出（重複）
  const actualDuplicates = Array.from(duplicateGroups.entries())
    .filter(([_, records]) => records.length > 1);

  console.log(`\n📊 重複検出結果:`);
  console.log(`  - 重複グループ数: ${actualDuplicates.length}`);
  console.log(`  - 重複レコード総数: ${actualDuplicates.reduce((sum, [_, records]) => sum + records.length, 0)}`);

  if (actualDuplicates.length === 0) {
    console.log("\n✅ タイプAに重複はありません！");
    return;
  }

  // 重複の詳細を表示
  console.log(`\n⚠️  重複が見つかりました:\n`);
  
  actualDuplicates.forEach(([docId, records], index) => {
    console.log(`【重複グループ ${index + 1}】 Firestore docId: ${docId}`);
    console.log(`  企業名: ${records[0].name}`);
    console.log(`  住所: ${records[0].address || "(なし)"}`);
    console.log(`  重複数: ${records.length} 件`);
    console.log(`  出現箇所:`);
    
    records.forEach(record => {
      console.log(`    - ${record.source} (行 ${record.rowIndex})`);
    });
    
    console.log("");
  });

  // 重複サマリーをファイルに出力
  const outputPath = path.resolve("TYPE_A_DUPLICATES_REPORT.txt");
  let reportContent = `タイプA 重複レポート\n`;
  reportContent += `生成日時: ${new Date().toISOString()}\n`;
  reportContent += `\n総レコード数: ${allRecords.length}\n`;
  reportContent += `重複グループ数: ${actualDuplicates.length}\n`;
  reportContent += `重複レコード総数: ${actualDuplicates.reduce((sum, [_, records]) => sum + records.length, 0)}\n`;
  reportContent += `\n${"=".repeat(80)}\n\n`;

  actualDuplicates.forEach(([docId, records], index) => {
    reportContent += `【重複グループ ${index + 1}】\n`;
    reportContent += `  Firestore docId: ${docId}\n`;
    reportContent += `  企業名: ${records[0].name}\n`;
    reportContent += `  住所: ${records[0].address || "(なし)"}\n`;
    reportContent += `  法人番号: ${records[0].corporateNumber || "(なし)"}\n`;
    reportContent += `  重複数: ${records.length} 件\n`;
    reportContent += `  出現箇所:\n`;
    
    records.forEach(record => {
      reportContent += `    - ${record.source} (行 ${record.rowIndex})\n`;
    });
    
    reportContent += `\n`;
  });

  fs.writeFileSync(outputPath, reportContent, "utf8");
  console.log(`📄 重複レポートを出力しました: ${outputPath}`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

