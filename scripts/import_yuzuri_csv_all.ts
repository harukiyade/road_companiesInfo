/**
 * csv/yuzuri配下のCSVファイルを全件インポート（テストで使用したファイルを除く）
 * 
 * 使い方:
 *   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
 *   npx ts-node scripts/import_yuzuri_csv_all.ts
 */

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// テストで使用したファイル（除外）
const TESTED_FILES = new Set(["1.csv", "2.csv", "10.csv", "17.csv"]);

// Firebase 初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
    ];
    for (const p of defaultPaths) {
      const resolved = path.resolve(p);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolved}`);
        break;
      }
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ サービスアカウント JSON のパスを指定してください");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId =
    serviceAccount.project_id ||
    process.env.GCLOUD_PROJECT ||
    process.env.GCP_PROJECT;

  if (!projectId) {
    console.error("❌ Project ID が取得できませんでした");
    process.exit(1);
  }

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });
  console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// ヘルパー関数
function trim(value: string | null | undefined): string {
  if (!value) return "";
  return String(value).trim();
}

function isEmpty(value: string | null | undefined): boolean {
  return !value || trim(value) === "";
}

// ドキュメントIDを数字のみの文字列に統一する
function generateNumericDocId(
  corporateNumber: string | null,
  rowIndex: number
): string {
  // corporateNumberが存在し、数字のみの場合 → そのまま使用
  if (corporateNumber && /^[0-9]+$/.test(corporateNumber.trim())) {
    return corporateNumber.trim();
  }
  
  // それ以外の場合 → Date.now() + 行番号から数字のみの一意IDを生成
  const timestamp = Date.now();
  const paddedIndex = String(rowIndex).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 売上規模を100万倍して数値に変換
function parseRevenue(value: string | null | undefined): number | null {
  if (isEmpty(value)) return null;
  
  const cleaned = trim(value).replace(/,/g, "");
  
  // "非公開"、"非公表"などの文字列はnull
  if (isNaN(Number(cleaned))) {
    return null;
  }
  
  const num = parseFloat(cleaned);
  if (isNaN(num)) return null;
  
  // 百万円単位なので100万倍
  return num * 1000000;
}

// 既存ドキュメントを検索（name, prefecture, representativeNameで）
async function findExistingCompany(
  name: string,
  prefecture: string,
  representativeName: string
): Promise<{ ref: any; id: string } | null> {
  if (isEmpty(name)) return null;

  // nameで検索
  const nameQuery = await companiesCol
    .where("name", "==", trim(name))
    .limit(10)
    .get();

  if (nameQuery.empty) return null;

  // prefectureとrepresentativeNameで絞り込み
  for (const doc of nameQuery.docs) {
    const data = doc.data();
    const docPrefecture = trim(data.prefecture || "");
    const docRepresentativeName = trim(data.representativeName || "");

    const csvPrefecture = trim(prefecture);
    const csvRepresentativeName = trim(representativeName);

    // prefectureとrepresentativeNameが一致するか確認
    if (
      (!csvPrefecture || !docPrefecture || docPrefecture === csvPrefecture) &&
      (!csvRepresentativeName || !docRepresentativeName || docRepresentativeName === csvRepresentativeName)
    ) {
      return { ref: doc.ref, id: doc.id };
    }
  }

  return null;
}

// グループ1の処理（25カラム）
async function processGroup1(csvPath: string, globalRowIndex: number, skipFirstRows: number = 0): Promise<{ docIds: string[]; nextRowIndex: number }> {
  const fileName = path.basename(csvPath);
  console.log(`\n📄 グループ1処理: ${fileName}${skipFirstRows > 0 ? ` (最初の${skipFirstRows}件をスキップ)` : ""}`);
  
  const buf = fs.readFileSync(csvPath);
  const records = parse(buf, {
    columns: false,
    skip_empty_lines: true,
    relax_quotes: true,
  });

  if (records.length < 3) {
    console.log("  ⚠️ データ行が不足しています");
    return { docIds: [], nextRowIndex: globalRowIndex };
  }

  const docIds: string[] = [];
  let currentBatch: WriteBatch = db.batch();
  let batchCount = 0;
  let processedCount = 0;
  let currentRowIndex = globalRowIndex;

  // ヘッダー行をスキップ（1-2行目）+ テストでインポート済みの行をスキップ
  const startRow = 2 + skipFirstRows;
  for (let i = startRow; i < records.length; i++) {
    const row = records[i];
    if (row.length < 7) continue;

    const name = trim(row[0]);
    const prefecture = trim(row[1]);
    const representativeName = trim(row[2]);
    const industryLarge = trim(row[3]);
    const kubun = trim(row[4]); // 区分
    const revenueStr = trim(row[5]);
    const overview = trim(row[6]);

    if (isEmpty(name)) continue;

    // 既存ドキュメントを検索
    const existing = await findExistingCompany(name, prefecture, representativeName);
    
    const updateData: Record<string, any> = {
      name: name,
    };

    if (!isEmpty(prefecture)) updateData.prefecture = prefecture;
    if (!isEmpty(representativeName)) updateData.representativeName = representativeName;
    if (!isEmpty(industryLarge)) updateData.industryLarge = industryLarge;
    if (!isEmpty(overview)) updateData.overview = overview;

    // 売上規模
    const revenue = parseRevenue(revenueStr);
    if (revenue !== null) updateData.revenue = revenue;

    // 区分の処理
    if (!isEmpty(kubun)) {
      if (kubun === "非上場") {
        updateData.listing = "非上場";
      } else {
        updateData.marketSegment = kubun;
        updateData.listing = "上場";
      }
    }

    if (existing) {
      currentBatch.update(existing.ref, updateData);
      docIds.push(existing.id);
      if (processedCount < 5 || processedCount % 100 === 0) {
        console.log(`  ✅ 更新: ${name} (${existing.id})`);
      }
    } else {
      // 数値IDを生成
      const docId = generateNumericDocId(null, currentRowIndex);
      const newRef = companiesCol.doc(docId);
      currentBatch.set(newRef, updateData);
      docIds.push(docId);
      if (processedCount < 5 || processedCount % 100 === 0) {
        console.log(`  ➕ 追加: ${name} (${docId})`);
      }
      currentRowIndex++;
    }

    batchCount++;
    processedCount++;

    if (batchCount >= 400) {
      await currentBatch.commit();
      currentBatch = db.batch();
      batchCount = 0;
    }
  }

  if (batchCount > 0) {
    await currentBatch.commit();
  }

  console.log(`  ✅ ${fileName}: ${processedCount} 件処理完了`);
  return { docIds, nextRowIndex: currentRowIndex };
}

// グループ2の処理（25カラム + 担当者コメント）
async function processGroup2(csvPath: string, globalRowIndex: number, skipFirstRows: number = 0): Promise<{ docIds: string[]; nextRowIndex: number }> {
  const fileName = path.basename(csvPath);
  console.log(`\n📄 グループ2処理: ${fileName}${skipFirstRows > 0 ? ` (最初の${skipFirstRows}件をスキップ)` : ""}`);
  
  const buf = fs.readFileSync(csvPath);
  const records = parse(buf, {
    columns: false,
    skip_empty_lines: true,
    relax_quotes: true,
  });

  if (records.length < 3) {
    console.log("  ⚠️ データ行が不足しています");
    return { docIds: [], nextRowIndex: globalRowIndex };
  }

  const docIds: string[] = [];
  let currentBatch: WriteBatch = db.batch();
  let batchCount = 0;
  let processedCount = 0;
  let currentRowIndex = globalRowIndex;

  // ヘッダー行をスキップ（1-2行目）+ テストでインポート済みの行をスキップ
  const startRow = 2 + skipFirstRows;
  for (let i = startRow; i < records.length; i++) {
    const row = records[i];
    if (row.length < 8) continue;

    const name = trim(row[0]);
    const prefecture = trim(row[1]);
    const representativeName = trim(row[2]);
    const industryLarge = trim(row[3]);
    const kubun = trim(row[4]);
    const revenueStr = trim(row[5]);
    const overview = trim(row[6]);
    const needs = trim(row[7]); // 担当者コメント

    if (isEmpty(name)) continue;

    const existing = await findExistingCompany(name, prefecture, representativeName);
    
    const updateData: Record<string, any> = {
      name: name,
    };

    if (!isEmpty(prefecture)) updateData.prefecture = prefecture;
    if (!isEmpty(representativeName)) updateData.representativeName = representativeName;
    if (!isEmpty(industryLarge)) updateData.industryLarge = industryLarge;
    if (!isEmpty(overview)) updateData.overview = overview;
    if (!isEmpty(needs)) updateData.needs = needs;

    const revenue = parseRevenue(revenueStr);
    if (revenue !== null) updateData.revenue = revenue;

    if (!isEmpty(kubun)) {
      if (kubun === "非上場") {
        updateData.listing = "非上場";
      } else {
        updateData.marketSegment = kubun;
        updateData.listing = "上場";
      }
    }

    if (existing) {
      currentBatch.update(existing.ref, updateData);
      docIds.push(existing.id);
      if (processedCount < 5 || processedCount % 100 === 0) {
        console.log(`  ✅ 更新: ${name} (${existing.id})`);
      }
    } else {
      const docId = generateNumericDocId(null, currentRowIndex);
      const newRef = companiesCol.doc(docId);
      currentBatch.set(newRef, updateData);
      docIds.push(docId);
      if (processedCount < 5 || processedCount % 100 === 0) {
        console.log(`  ➕ 追加: ${name} (${docId})`);
      }
      currentRowIndex++;
    }

    batchCount++;
    processedCount++;

    if (batchCount >= 400) {
      await currentBatch.commit();
      currentBatch = db.batch();
      batchCount = 0;
    }
  }

  if (batchCount > 0) {
    await currentBatch.commit();
  }

  console.log(`  ✅ ${fileName}: ${processedCount} 件処理完了`);
  return { docIds, nextRowIndex: currentRowIndex };
}

// グループ3の処理（26カラム）
async function processGroup3(csvPath: string, globalRowIndex: number, skipFirstRows: number = 0): Promise<{ docIds: string[]; nextRowIndex: number }> {
  // 1.csvのみテストで最初の5件をインポート済み
  const fileName = path.basename(csvPath);
  const skipRows = fileName === "1.csv" ? 5 : 0;
  return processGroup1(csvPath, globalRowIndex, skipRows);
}

// グループ4の処理（26カラム + コメント）
async function processGroup4(csvPath: string, globalRowIndex: number, skipFirstRows: number = 0): Promise<{ docIds: string[]; nextRowIndex: number }> {
  const fileName = path.basename(csvPath);
  console.log(`\n📄 グループ4処理: ${fileName}${skipFirstRows > 0 ? ` (最初の${skipFirstRows}件をスキップ)` : ""}`);
  
  const buf = fs.readFileSync(csvPath);
  const records = parse(buf, {
    columns: false,
    skip_empty_lines: true,
    relax_quotes: true,
  });

  if (records.length < 3) {
    console.log("  ⚠️ データ行が不足しています");
    return { docIds: [], nextRowIndex: globalRowIndex };
  }

  const docIds: string[] = [];
  let currentBatch: WriteBatch = db.batch();
  let batchCount = 0;
  let processedCount = 0;
  let currentRowIndex = globalRowIndex;

  // ヘッダー行をスキップ（1-2行目）+ テストでインポート済みの行をスキップ
  const startRow = 2 + skipFirstRows;
  for (let i = startRow; i < records.length; i++) {
    const row = records[i];
    if (row.length < 8) continue;

    const name = trim(row[0]);
    const prefecture = trim(row[1]);
    const representativeName = trim(row[2]);
    const industryLarge = trim(row[3]);
    const kubun = trim(row[4]);
    const revenueStr = trim(row[5]);
    const overview = trim(row[6]);
    const needs = trim(row[7]); // コメント

    if (isEmpty(name)) continue;

    const existing = await findExistingCompany(name, prefecture, representativeName);
    
    const updateData: Record<string, any> = {
      name: name,
    };

    if (!isEmpty(prefecture)) updateData.prefecture = prefecture;
    if (!isEmpty(representativeName)) updateData.representativeName = representativeName;
    if (!isEmpty(industryLarge)) updateData.industryLarge = industryLarge;
    if (!isEmpty(overview)) updateData.overview = overview;
    if (!isEmpty(needs)) updateData.needs = needs;

    const revenue = parseRevenue(revenueStr);
    if (revenue !== null) updateData.revenue = revenue;

    if (!isEmpty(kubun)) {
      if (kubun === "非上場") {
        updateData.listing = "非上場";
      } else {
        updateData.marketSegment = kubun;
        updateData.listing = "上場";
      }
    }

    if (existing) {
      currentBatch.update(existing.ref, updateData);
      docIds.push(existing.id);
      if (processedCount < 5 || processedCount % 100 === 0) {
        console.log(`  ✅ 更新: ${name} (${existing.id})`);
      }
    } else {
      const docId = generateNumericDocId(null, currentRowIndex);
      const newRef = companiesCol.doc(docId);
      currentBatch.set(newRef, updateData);
      docIds.push(docId);
      if (processedCount < 5 || processedCount % 100 === 0) {
        console.log(`  ➕ 追加: ${name} (${docId})`);
      }
      currentRowIndex++;
    }

    batchCount++;
    processedCount++;

    if (batchCount >= 400) {
      await currentBatch.commit();
      currentBatch = db.batch();
      batchCount = 0;
    }
  }

  if (batchCount > 0) {
    await currentBatch.commit();
  }

  console.log(`  ✅ ${fileName}: ${processedCount} 件処理完了`);
  return { docIds, nextRowIndex: currentRowIndex };
}

async function main() {
  const yuzuriDir = path.join(process.cwd(), "csv", "yuzuri");
  
  if (!fs.existsSync(yuzuriDir)) {
    console.error(`❌ ディレクトリが見つかりません: ${yuzuriDir}`);
    process.exit(1);
  }

  console.log("🚀 csv/yuzuri配下のCSVファイルを全件インポート開始\n");
  console.log(`ℹ️  テストで使用したファイル（${Array.from(TESTED_FILES).join(", ")}）は、最初の5件をスキップして処理します\n`);

  // グループ定義
  const groups = [
    { 
      name: "グループ1", 
      files: ["2.csv"], 
      processor: processGroup1,
      skipFirstRows: 5 // テストで最初の5件をインポート済み
    },
    { 
      name: "グループ2", 
      files: ["10.csv"], 
      processor: processGroup2,
      skipFirstRows: 5 // テストで最初の5件をインポート済み
    },
    { 
      name: "グループ3", 
      files: ["1.csv", "3.csv", "4.csv", "5.csv", "6.csv", "7.csv", "8.csv", "9.csv", "11.csv", "12.csv", "13.csv", "14.csv", "15.csv", "16.csv", "18.csv", "19.csv", "20.csv", "21.csv"], 
      processor: processGroup3,
      skipFirstRows: 0 // 1.csvのみテストで最初の5件をインポート済み
    },
    { 
      name: "グループ4", 
      files: ["17.csv"], 
      processor: processGroup4,
      skipFirstRows: 5 // テストで最初の5件をインポート済み
    },
  ];

  const allDocIds: string[] = [];
  let globalRowIndex = 0;

  for (const group of groups) {
    console.log(`\n${"=".repeat(80)}`);
    console.log(`${group.name} 処理開始`);
    console.log("=".repeat(80));

    for (const file of group.files) {
      const csvPath = path.join(yuzuriDir, file);
      if (!fs.existsSync(csvPath)) {
        console.log(`⚠️ ファイルが見つかりません: ${file}`);
        continue;
      }

      try {
        const skipRows = (group as any).skipFirstRows || 0;
        const result = await group.processor(csvPath, globalRowIndex, skipRows);
        allDocIds.push(...result.docIds);
        globalRowIndex = result.nextRowIndex;
      } catch (error: any) {
        console.error(`  ❌ ${file}: エラー - ${error.message}`);
      }
    }
  }

  console.log("\n" + "=".repeat(80));
  console.log("✅ 処理完了");
  console.log("=".repeat(80));
  console.log(`\n📋 処理したドキュメント数: ${allDocIds.length} 件`);
  console.log(`   最初の10件のID:`);
  for (let i = 0; i < Math.min(10, allDocIds.length); i++) {
    console.log(`     - ${allDocIds[i]}`);
  }
  if (allDocIds.length > 10) {
    console.log(`     ... 他 ${allDocIds.length - 10} 件`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

