/* 
  法人番号がnullのドキュメントに対して、CSVから法人番号をインポートするスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    CSV_FILE=out/gBizINFO/companies_export.csv \
    npx tsx scripts/import_corporate_numbers_for_null.ts
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference, DocumentReference } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse";
import { Readable } from "stream";

const COLLECTION_NAME = "companies_new";

// パフォーマンス設定（環境変数で上書き可能）
const CHUNK_SIZE = process.env.CHUNK_SIZE ? parseInt(process.env.CHUNK_SIZE) : 500;
const CONCURRENT_CHUNKS = process.env.CONCURRENT_CHUNKS ? parseInt(process.env.CONCURRENT_CHUNKS) : 5;
const CONCURRENT_QUERIES = process.env.CONCURRENT_QUERIES ? parseInt(process.env.CONCURRENT_QUERIES) : 40;
const MAX_BUFFER_SIZE = process.env.MAX_BUFFER_SIZE ? parseInt(process.env.MAX_BUFFER_SIZE) : 200;

// ==============================
// Firebase初期化
// ==============================

let db: Firestore;

function initAdmin() {
  if (admin.apps.length > 0) {
    db = admin.firestore();
    return;
  }

  const serviceAccountPath =
    process.env.GOOGLE_APPLICATION_CREDENTIALS ||
    path.join(__dirname, "../serviceAccountKey.json");

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(`❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });

  db = admin.firestore();
}

// ==============================
// ログ関数
// ==============================

function log(message: string) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
}

// ==============================
// CSVファイルパス取得
// ==============================

function getCsvFilePath(): string {
  // 1. コマンドライン引数
  const args = process.argv.slice(2);
  if (args.length > 0 && !args[0].startsWith("--")) {
    return args[0];
  }

  // 2. 環境変数
  if (process.env.CSV_FILE) {
    return process.env.CSV_FILE;
  }

  // 3. デフォルトパス
  const defaultPath = path.join(__dirname, "../out/gBizINFO/companies_export.csv");
  return defaultPath;
}

// ==============================
// ユーティリティ関数
// ==============================

function isNullish(value: any): boolean {
  return value === null || value === undefined || value === "";
}

// ==============================
// CSVから法人番号マップを構築
// ==============================

interface CsvRecord {
  name: string;
  corporateNumber: string;
  address: string;
}

async function buildCorporateNumberMap(csvFilePath: string): Promise<Map<string, string>> {
  log("📖 CSVファイルを読み込み中...");
  
  const csvMap = new Map<string, string>(); // key: name+addressの正規化文字列, value: corporateNumber
  const nameMap = new Map<string, string>(); // key: nameの正規化文字列, value: corporateNumber (フォールバック用)
  
  return new Promise((resolve, reject) => {
    const fileStream = fs.createReadStream(csvFilePath, { encoding: "utf8" });
    let header: string[] = [];
    let rowCount = 0;
    
    const parser = parse({
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
      relax_quotes: true,
      skip_records_with_error: true,
    });
    
    parser.on("readable", function () {
      let record: Record<string, string>;
      while ((record = parser.read()) !== null) {
        if (header.length === 0) {
          header = Object.keys(record);
          continue;
        }
        
        rowCount++;
        if (rowCount % 100000 === 0) {
          log(`  📊 CSV読み込み中: ${rowCount.toLocaleString()} 行`);
        }
        
        const name = record["name"]?.trim();
        const corporateNumber = record["corporateNumber"]?.trim();
        const address = record["address"]?.trim();
        
        if (!corporateNumber || !/^\d{13}$/.test(corporateNumber)) {
          continue; // 有効な法人番号でない場合はスキップ
        }
        
        // 社名と住所の組み合わせでマップを作成
        if (name) {
          const normalizedName = name.replace(/\s+/g, "").toLowerCase();
          const normalizedAddress = address ? address.replace(/\s+/g, "").substring(0, 20).toLowerCase() : "";
          const key = `${normalizedName}|${normalizedAddress}`;
          csvMap.set(key, corporateNumber);
          
          // 社名のみのマップも作成（フォールバック用）
          if (!nameMap.has(normalizedName)) {
            nameMap.set(normalizedName, corporateNumber);
          }
        }
      }
    });
    
    parser.on("error", (err) => {
      reject(err);
    });
    
    parser.on("end", () => {
      log(`✅ CSV読み込み完了: ${rowCount.toLocaleString()} 行、${csvMap.size.toLocaleString()} 件の法人番号マップを作成`);
      // nameMapもcsvMapに統合（フォールバック用）
      nameMap.forEach((corpNum, name) => {
        if (!csvMap.has(`${name}|`)) {
          csvMap.set(`${name}|`, corpNum);
        }
      });
      resolve(csvMap);
    });
    
    fileStream.pipe(parser);
  });
}

// ==============================
// 法人番号がnullのドキュメントを取得
// ==============================

interface CompanyDoc {
  ref: DocumentReference;
  data: Record<string, any>;
}

async function getNullCorporateNumberDocs(limit: number = 10000): Promise<CompanyDoc[]> {
  log("🔍 法人番号がnullのドキュメントを検索中...");
  
  const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);
  const docs: CompanyDoc[] = [];
  const PAGE_SIZE = 1000;
  let lastDoc: any = null;
  let totalProcessed = 0;
  
  while (docs.length < limit) {
    let batchQuery = companiesCol.orderBy("__name__").limit(PAGE_SIZE);
    if (lastDoc) {
      batchQuery = batchQuery.startAfter(lastDoc);
    }
    
    const batchSnapshot = await batchQuery.get();
    
    if (batchSnapshot.empty) break;
    
    for (const doc of batchSnapshot.docs) {
      const data = doc.data();
      const corporateNumber = data.corporateNumber;
      const name = data.name;
      const address = data.address;
      
      // 法人番号がnull/undefined/空 かつ (nameまたはaddressがある)
      const isMissingCorpNum = corporateNumber === null || 
          corporateNumber === undefined || 
          corporateNumber === "" ||
          !("corporateNumber" in data);
      
      const hasNameOrAddress = (name && name.trim() !== "") || (address && address.trim() !== "");
      
      if (isMissingCorpNum && hasNameOrAddress) {
        docs.push({
          ref: doc.ref,
          data: data,
        });
      }
      
      totalProcessed++;
      if (totalProcessed % 10000 === 0) {
        log(`  📊 処理中: ${totalProcessed.toLocaleString()} 件、見つかった: ${docs.length.toLocaleString()} 社`);
      }
      
      if (docs.length >= limit) break;
    }
    
    if (docs.length >= limit) break;
    
    lastDoc = batchSnapshot.docs[batchSnapshot.docs.length - 1];
    
    if (batchSnapshot.size < PAGE_SIZE) break;
  }
  
  log(`✅ 検索完了: ${docs.length.toLocaleString()} 社の法人番号がnullのドキュメントが見つかりました`);
  return docs;
}

// ==============================
// CSVマップから法人番号を検索
// ==============================

function findCorporateNumberFromMap(
  doc: CompanyDoc,
  csvMap: Map<string, string>
): string | null {
  const name = doc.data.name?.trim();
  const address = doc.data.address?.trim();
  
  if (!name) return null;
  
  // 1. 社名+住所の組み合わせで検索
  const normalizedName = name.replace(/\s+/g, "").toLowerCase();
  const normalizedAddress = address ? address.replace(/\s+/g, "").substring(0, 20).toLowerCase() : "";
  const key = `${normalizedName}|${normalizedAddress}`;
  
  if (csvMap.has(key)) {
    return csvMap.get(key)!;
  }
  
  // 2. 社名のみで検索（フォールバック）
  if (csvMap.has(`${normalizedName}|`)) {
    return csvMap.get(`${normalizedName}|`)!;
  }
  
  return null;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  initAdmin();
  
  const csvFilePath = getCsvFilePath();
  if (!fs.existsSync(csvFilePath)) {
    console.error(`❌ エラー: CSVファイルが見つかりません: ${csvFilePath}`);
    console.error(`CSVファイルのパスを指定してください:`);
    console.error(`  1. コマンドライン引数: npx tsx scripts/import_corporate_numbers_for_null.ts <CSVファイルパス>`);
    console.error(`  2. 環境変数: CSV_FILE=<CSVファイルパス> npx tsx scripts/import_corporate_numbers_for_null.ts`);
    process.exit(1);
  }
  
  // 1. CSVから法人番号マップを構築
  const csvMap = await buildCorporateNumberMap(csvFilePath);
  
  // 2. 法人番号がnullのドキュメントを取得
  const nullDocs = await getNullCorporateNumberDocs(50000); // 最大50000件まで
  
  if (nullDocs.length === 0) {
    log("✅ 法人番号がnullのドキュメントは見つかりませんでした");
    return;
  }
  
  // 3. CSVマップから法人番号を検索して更新
  log(`\n🔄 法人番号の更新を開始...`);
  
  const BATCH_SIZE = 500;
  let updatedCount = 0;
  let notFoundCount = 0;
  const updatedDocIds: Array<{ docId: string; corporateNumber: string; name: string }> = [];
  
  for (let i = 0; i < nullDocs.length; i += BATCH_SIZE) {
    const batch = nullDocs.slice(i, i + BATCH_SIZE);
    const batchWrite = db.batch();
    let batchUpdatedCount = 0;
    
    for (const doc of batch) {
      const corporateNumber = findCorporateNumberFromMap(doc, csvMap);
      
      if (corporateNumber) {
        batchWrite.update(doc.ref, {
          corporateNumber: corporateNumber,
          updatedAt: admin.firestore.Timestamp.now(),
        });
        batchUpdatedCount++;
        
        // 更新したドキュメントIDを記録（最初の100件まで）
        if (updatedDocIds.length < 100) {
          updatedDocIds.push({
            docId: doc.ref.id,
            corporateNumber: corporateNumber,
            name: doc.data.name || "(社名なし)",
          });
        }
      } else {
        notFoundCount++;
      }
    }
    
    if (batchUpdatedCount > 0) {
      await batchWrite.commit();
      updatedCount += batchUpdatedCount;
      log(`  📊 更新中: ${Math.min(i + BATCH_SIZE, nullDocs.length).toLocaleString()} / ${nullDocs.length.toLocaleString()} 件、更新済み: ${updatedCount.toLocaleString()} 社`);
    }
  }
  
  log(`\n✅ 処理完了:`);
  log(`   - 処理対象: ${nullDocs.length.toLocaleString()} 社`);
  log(`   - 更新成功: ${updatedCount.toLocaleString()} 社`);
  log(`   - CSVに存在しない: ${notFoundCount.toLocaleString()} 社`);
  
  // 更新したドキュメントIDを表示（最初の50件）
  if (updatedDocIds.length > 0) {
    log(`\n📋 更新したドキュメントID（最初の${Math.min(50, updatedDocIds.length)}件）:`);
    updatedDocIds.slice(0, 50).forEach((item, index) => {
      log(`   ${index + 1}. docId: ${item.docId}, 法人番号: ${item.corporateNumber}, 社名: ${item.name.substring(0, 30)}`);
    });
    if (updatedDocIds.length > 50) {
      log(`   ... 他 ${updatedDocIds.length - 50} 件`);
    }
  }
}

// 実行
main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
