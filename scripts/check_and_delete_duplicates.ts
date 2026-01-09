/* 
  CSV内の企業情報と重複する既存データを削除するスクリプト
  
  重複条件:
  - 企業名 + 住所 + 法人番号（あれば）で検索
  - CSV内に存在する企業情報と一致するcompanies_newのドキュメントを全て削除
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/check_and_delete_duplicates.ts
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
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";
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

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
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

// 法人番号を正規化
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
  console.log(`📄 CSVファイルを読み込み中: ${files.length} ファイル\n`);

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
      
      console.log(`  ✅ ${file}: ${records.length} 件`);
      allRecords.push(...records);
    } catch (err: any) {
      console.error(`  ❌ ${file}: 読み込みエラー - ${err.message}`);
    }
  }

  console.log(`\n📊 合計: ${allRecords.length} 件のレコード\n`);
  return allRecords;
}

// 重複を検出して削除
async function checkAndDeleteDuplicates(csvRecords: Array<Record<string, string>>) {
  console.log("🔍 重複チェックを開始します...\n");

  // CSVレコードから企業情報を抽出（CSV内に存在する企業情報のセット）
  const csvCompanyKeys = new Set<string>(); // CSV内に存在する企業情報の検索キー
  
  for (const record of csvRecords) {
    const name = normalizeString(record["会社名"]);
    const address = normalizeString(record["住所"]);
    const corporateNumber = normalizeCorporateNumber(record["法人番号"]);
    
    if (!name || !address) continue; // 企業名と住所が必須
    
    // 検索キーを生成（企業名|住所|法人番号（あれば））
    const searchKey = corporateNumber 
      ? `${name}|${address}|${corporateNumber}`
      : `${name}|${address}`;
    
    csvCompanyKeys.add(searchKey);
  }

  console.log(`📊 CSV内に存在する企業情報（ユニーク）: ${csvCompanyKeys.size.toLocaleString()} 件\n`);

  if (csvCompanyKeys.size === 0) {
    console.log("⚠️  CSVファイルに有効な企業情報がありません。");
    return;
  }

  // Firestoreから企業を検索
  let scannedCount = 0;
  let duplicateCount = 0;
  let deletedCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 400;

  console.log("📊 Firestoreコレクションをスキャン中...\n");

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

      // 検索キーを生成（既存データ用）
      const searchKey = corporateNumber 
        ? `${name}|${address}|${corporateNumber}`
        : `${name}|${address}`;

      // CSV内に存在する企業情報と一致する場合、削除対象
      if (csvCompanyKeys.has(searchKey)) {
        duplicateCount++;
        console.log(`🗑️  [${duplicateCount}] 重複検出: docId=${doc.id}, name="${name}"`);
        
        if (!DRY_RUN) {
          batch.delete(doc.ref);
          batchCount++;
          deletedCount++;

          if (batchCount >= BATCH_LIMIT) {
            await batch.commit();
            console.log(`  💾 バッチコミット: ${batchCount} 件削除`);
            const newBatch = db.batch();
            batch = newBatch;
            batchCount = 0;
          }
        }
      }

      if (scannedCount % 10000 === 0) {
        console.log(`  📦 スキャン中: ${scannedCount} 件処理済み...`);
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }

  // 残りのバッチをコミット
  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    console.log(`  💾 最後のバッチコミット: ${batchCount} 件削除`);
  }

  console.log("\n✅ 重複チェック完了");
  console.log(`  📄 CSV内の企業情報（ユニーク）: ${csvCompanyKeys.size.toLocaleString()} 件`);
  console.log(`  📊 Firestoreスキャン件数: ${scannedCount.toLocaleString()} 件`);
  console.log(`  🔍 重複検出（削除対象）: ${duplicateCount.toLocaleString()} 件`);
  if (DRY_RUN) {
    console.log(`  ⚠️  DRY_RUN モードのため削除しませんでした`);
  } else {
    console.log(`  🗑️  削除件数: ${deletedCount.toLocaleString()} 件`);
  }
}

// ==============================
// メイン処理
// ==============================

async function main() {
  if (DRY_RUN) {
    console.log("🔍 DRY_RUN モード: Firestore から削除しません\n");
  } else {
    console.log("⚠️  本番モード: Firestore から重複データを削除します\n");
  }

  // CSVファイルを読み込む
  const csvRecords = loadCsvFiles(CSV_DIR);

  if (csvRecords.length === 0) {
    console.log("⚠️  CSVファイルにデータがありません");
    return;
  }

  // 重複チェック＆削除
  await checkAndDeleteDuplicates(csvRecords);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
