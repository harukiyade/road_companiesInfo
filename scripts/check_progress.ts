/* 
  重複削除スクリプトの進捗を確認するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/check_progress.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_DIR = "./csv/add_20251224";

// ==============================
// Firebase 初期化
// ==============================
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

    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        break;
      }
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ エラー: サービスアカウントキーファイルのパスが指定されていません");
    process.exit(1);
  }

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(`❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
    process.exit(1);
  }

  try {
    const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
    const projectId = serviceAccount.project_id || process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT;

    if (!projectId) {
      console.error("❌ エラー: Project ID を検出できませんでした");
      process.exit(1);
    }

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: projectId,
    });

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})\n`);
  } catch (err: any) {
    console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
    console.error(`   詳細: ${err.message}`);
    process.exit(1);
  }
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// ==============================
// ヘルパー関数
// ==============================

function isEmptyValue(v: any): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  return false;
}

function normalizeString(s: string | null | undefined): string {
  if (isEmptyValue(s)) return "";
  return String(s).trim().replace(/\s+/g, "");
}

function normalizeCorporateNumber(value: string | null | undefined): string | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim().replace(/\D/g, "");
  if (cleaned.length === 13) {
    return cleaned;
  }
  return null;
}

// CSVファイルを読み込む
function loadCsvFiles(csvDir: string): Array<Record<string, string>> {
  const allRecords: Array<Record<string, string>> = [];
  
  if (!fs.existsSync(csvDir)) {
    console.error(`❌ エラー: CSVディレクトリが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  const files = fs.readdirSync(csvDir).filter(f => f.endsWith(".csv"));

  for (const file of files) {
    const filePath = path.join(csvDir, file);
    try {
      const csvContent = fs.readFileSync(filePath, "utf8");
      const records = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
      }) as Record<string, string>[];
      
      allRecords.push(...records);
    } catch (err: any) {
      console.error(`  ❌ ${file}: 読み込みエラー - ${err.message}`);
    }
  }

  return allRecords;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  console.log("📊 重複削除スクリプトの進捗確認\n");

  // CSVファイルを読み込む
  console.log("📄 CSVファイルを読み込み中...");
  const csvRecords = loadCsvFiles(CSV_DIR);
  console.log(`✅ CSV総レコード数: ${csvRecords.length.toLocaleString()} 件\n`);

  // CSV内で2個以上出現する企業情報を集計
  const csvCompanyCounts = new Map<string, number>();
  
  for (const record of csvRecords) {
    const name = normalizeString(record["会社名"]);
    const address = normalizeString(record["住所"]);
    const corporateNumber = normalizeCorporateNumber(record["法人番号"]);
    
    if (!name || !address) continue;
    
    const searchKey = corporateNumber 
      ? `${name}|${address}|${corporateNumber}`
      : `${name}|${address}`;
    
    csvCompanyCounts.set(searchKey, (csvCompanyCounts.get(searchKey) || 0) + 1);
  }

  const duplicateKeys = new Set<string>();
  for (const [key, count] of csvCompanyCounts.entries()) {
    if (count >= 2) {
      duplicateKeys.add(key);
    }
  }

  console.log(`📊 CSV内で2個以上出現する企業情報: ${duplicateKeys.size.toLocaleString()} 件\n`);

  // Firestoreから該当する企業をカウント
  console.log("🔍 Firestoreコレクションをスキャン中...\n");

  let scannedCount = 0;
  let duplicateCount = 0;
  const sampleDuplicates: Array<{docId: string, name: string, csvCount: number}> = [];

  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
  const PAGE_SIZE = 1000;

  while (true) {
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(PAGE_SIZE);
    
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snapshot = await query.get();
    if (snapshot.empty) {
      break;
    }

    for (const doc of snapshot.docs) {
      scannedCount++;
      const data = doc.data();
      
      const name = normalizeString(data.name);
      const address = normalizeString(data.address || data.headquartersAddress);
      const corporateNumber = normalizeCorporateNumber(data.corporateNumber);
      
      if (!name || !address) continue;

      const searchKey = corporateNumber 
        ? `${name}|${address}|${corporateNumber}`
        : `${name}|${address}`;

      if (duplicateKeys.has(searchKey)) {
        duplicateCount++;
        const csvCount = csvCompanyCounts.get(searchKey) || 0;
        
        // サンプルを保存（最初の10件）
        if (sampleDuplicates.length < 10) {
          sampleDuplicates.push({
            docId: doc.id,
            name: name,
            csvCount: csvCount,
          });
        }
      }

      if (scannedCount % 10000 === 0) {
        console.log(`  📦 スキャン中: ${scannedCount.toLocaleString()} 件処理済み... (重複: ${duplicateCount.toLocaleString()} 件)`);
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }

  console.log("\n✅ スキャン完了\n");

  console.log("📊 結果サマリー:");
  console.log(`  📄 CSV総レコード数: ${csvRecords.length.toLocaleString()} 件`);
  console.log(`  🔍 CSV内で重複する企業情報: ${duplicateKeys.size.toLocaleString()} 件`);
  console.log(`  📦 Firestoreスキャン件数: ${scannedCount.toLocaleString()} 件`);
  console.log(`  🗑️  削除対象（重複検出）: ${duplicateCount.toLocaleString()} 件`);
  
  if (duplicateCount > 0) {
    console.log(`\n📋 サンプル（最初の10件）:`);
    sampleDuplicates.forEach((item, index) => {
      console.log(`  ${index + 1}. docId=${item.docId}, name="${item.name}", CSV出現回数=${item.csvCount}`);
    });
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
