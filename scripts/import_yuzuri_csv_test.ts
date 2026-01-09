/**
 * csv/yuzuri配下のCSVファイルをテストインポート（各グループ5件のみ）
 * 
 * 使い方:
 *   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
 *   npx ts-node scripts/import_yuzuri_csv_test.ts
 */

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const TEST_LIMIT = 5; // 各グループ5件のみ

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
): Promise<DocumentReference | null> {
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
      return doc.ref;
    }
  }

  return null;
}

// グループ1の処理
async function processGroup1(csvPath: string): Promise<string[]> {
  console.log(`\n📄 グループ1処理: ${path.basename(csvPath)}`);
  
  const buf = fs.readFileSync(csvPath);
  const records = parse(buf, {
    columns: false,
    skip_empty_lines: true,
    relax_quotes: true,
  });

  if (records.length < 3) {
    console.log("  ⚠️ データ行が不足しています");
    return [];
  }

  const docIds: string[] = [];
  let currentBatch: WriteBatch = db.batch();
  let batchCount = 0;
  let processedCount = 0;

  // ヘッダー行をスキップ（1-2行目）
  for (let i = 2; i < records.length && processedCount < TEST_LIMIT; i++) {
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
    const existingRef = await findExistingCompany(name, prefecture, representativeName);
    
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

    if (existingRef) {
      currentBatch.update(existingRef, updateData);
      docIds.push(existingRef.id);
      console.log(`  ✅ 更新: ${name} (${existingRef.id})`);
    } else {
      // 数値IDを生成
      const docId = generateNumericDocId(null, i);
      const newRef = companiesCol.doc(docId);
      currentBatch.set(newRef, updateData);
      docIds.push(docId);
      console.log(`  ➕ 追加: ${name} (${docId})`);
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

  return docIds;
}

// グループ2の処理（担当者コメント→needs）
async function processGroup2(csvPath: string): Promise<string[]> {
  console.log(`\n📄 グループ2処理: ${path.basename(csvPath)}`);
  
  const buf = fs.readFileSync(csvPath);
  const records = parse(buf, {
    columns: false,
    skip_empty_lines: true,
    relax_quotes: true,
  });

  if (records.length < 3) {
    console.log("  ⚠️ データ行が不足しています");
    return [];
  }

  const docIds: string[] = [];
  let currentBatch: WriteBatch = db.batch();
  let batchCount = 0;
  let processedCount = 0;

  for (let i = 2; i < records.length && processedCount < TEST_LIMIT; i++) {
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

    const existingRef = await findExistingCompany(name, prefecture, representativeName);
    
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

    if (existingRef) {
      currentBatch.update(existingRef, updateData);
      docIds.push(existingRef.id);
      console.log(`  ✅ 更新: ${name} (${existingRef.id})`);
    } else {
      // 数値IDを生成
      const docId = generateNumericDocId(null, i);
      const newRef = companiesCol.doc(docId);
      currentBatch.set(newRef, updateData);
      docIds.push(docId);
      console.log(`  ➕ 追加: ${name} (${docId})`);
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

  return docIds;
}

// グループ3の処理（グループ1と同じ）
async function processGroup3(csvPath: string): Promise<string[]> {
  return processGroup1(csvPath);
}

// グループ4の処理（コメント→needs）
async function processGroup4(csvPath: string): Promise<string[]> {
  console.log(`\n📄 グループ4処理: ${path.basename(csvPath)}`);
  
  const buf = fs.readFileSync(csvPath);
  const records = parse(buf, {
    columns: false,
    skip_empty_lines: true,
    relax_quotes: true,
  });

  if (records.length < 3) {
    console.log("  ⚠️ データ行が不足しています");
    return [];
  }

  const docIds: string[] = [];
  let currentBatch: WriteBatch = db.batch();
  let batchCount = 0;
  let processedCount = 0;

  for (let i = 2; i < records.length && processedCount < TEST_LIMIT; i++) {
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

    const existingRef = await findExistingCompany(name, prefecture, representativeName);
    
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

    if (existingRef) {
      currentBatch.update(existingRef, updateData);
      docIds.push(existingRef.id);
      console.log(`  ✅ 更新: ${name} (${existingRef.id})`);
    } else {
      // 数値IDを生成
      const docId = generateNumericDocId(null, i);
      const newRef = companiesCol.doc(docId);
      currentBatch.set(newRef, updateData);
      docIds.push(docId);
      console.log(`  ➕ 追加: ${name} (${docId})`);
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

  return docIds;
}

async function main() {
  const yuzuriDir = path.join(process.cwd(), "csv", "yuzuri");
  
  if (!fs.existsSync(yuzuriDir)) {
    console.error(`❌ ディレクトリが見つかりません: ${yuzuriDir}`);
    process.exit(1);
  }

  console.log("🚀 csv/yuzuri配下のCSVファイルをテストインポート開始\n");

  // グループ定義
  const groups = [
    { name: "グループ1", files: ["2.csv"], processor: processGroup1 },
    { name: "グループ2", files: ["10.csv"], processor: processGroup2 },
    { name: "グループ3", files: ["1.csv"], processor: processGroup3 },
    { name: "グループ4", files: ["17.csv"], processor: processGroup4 },
  ];

  const allDocIds: string[] = [];

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
        const docIds = await group.processor(csvPath);
        allDocIds.push(...docIds);
        console.log(`  ✅ ${file}: ${docIds.length} 件処理完了`);
      } catch (error: any) {
        console.error(`  ❌ ${file}: エラー - ${error.message}`);
      }
    }
  }

  console.log("\n" + "=".repeat(80));
  console.log("✅ 処理完了");
  console.log("=".repeat(80));
  console.log(`\n📋 処理したドキュメントID一覧 (合計 ${allDocIds.length} 件):`);
  for (const docId of allDocIds) {
    console.log(`  - ${docId}`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

