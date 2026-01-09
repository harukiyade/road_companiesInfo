/* 
  法人番号がnullのドキュメントに対して、CSVから法人番号とその他のフィールドを更新するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    CSV_FILE=out/gBizINFO/companies_export.csv \
    npx tsx scripts/update_null_corporate_numbers_from_csv.ts
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference, DocumentReference, WriteBatch } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse";
import { createReadStream } from "fs";

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
let companiesCol: CollectionReference;

function initAdmin() {
  if (admin.apps.length > 0) {
    db = admin.firestore();
    companiesCol = db.collection(COLLECTION_NAME);
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
  companiesCol = db.collection(COLLECTION_NAME);
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
  // 環境変数から取得
  if (process.env.CSV_FILE) {
    return path.resolve(process.env.CSV_FILE);
  }
  
  // 引数から取得
  const csvArg = process.argv.find(arg => 
    !arg.startsWith("--") && arg.endsWith(".csv")
  );
  if (csvArg) {
    return path.resolve(csvArg);
  }
  
  // デフォルトパス
  return path.join(__dirname, "../out/gBizINFO/companies_export.csv");
}

const CSV_FILE = getCsvFilePath();

// ==============================
// ユーティリティ関数
// ==============================

function isNullish(value: any): boolean {
  return value === null || value === undefined || value === "";
}

function isEmpty(value: string | null | undefined): boolean {
  return !value || value.trim() === "";
}

function trim(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = value.trim();
  return trimmed === "" ? null : trimmed;
}

// CSVの値を適切な型に変換
function convertValue(value: string, fieldName: string): any {
  if (isEmpty(value)) return null;
  
  // 数値フィールド
  const numericFields = [
    "capitalStock",
    "revenue",
    "revenueFromStatements",
    "employeeCount",
    "employeeNumber",
    "foundingYear",
    "fiscalMonth",
    "factoryCount",
    "officeCount",
    "storeCount",
    "procurementCount",
    "workplaceRowCount",
    "updateCount",
    "changeCount",
  ];
  
  if (numericFields.includes(fieldName)) {
    const num = parseFloat(value.replace(/[^\d.-]/g, ""));
    return isNaN(num) ? null : num;
  }
  
  // 配列フィールド（JSON文字列として保存されている）
  const arrayFields = [
    "industries",
    "businessItems",
    "banks",
    "tags",
    "urls",
    "executives",
  ];
  
  if (arrayFields.includes(fieldName)) {
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : null;
    } catch {
      return null;
    }
  }
  
  // 日付フィールド
  const dateFields = [
    "createdAt",
    "updatedAt",
    "extendedFieldsScrapedAt",
    "representativeBirthDate",
    "procurementLatestDate",
    "adExpiration",
  ];
  
  if (dateFields.includes(fieldName)) {
    if (isEmpty(value)) return null;
    try {
      const date = new Date(value);
      return isNaN(date.getTime()) ? null : admin.firestore.Timestamp.fromDate(date);
    } catch {
      return null;
    }
  }
  
  // 文字列フィールド
  return trim(value) || null;
}

// ==============================
// CSVからマップを構築
// ==============================

interface CsvRecord {
  name: string;
  corporateNumber: string;
  address: string;
  // 軽量化のため、record全体ではなく必要なフィールドだけを保持
  fields: Map<string, any>; // fieldName -> value
}

async function buildCsvMap(csvFilePath: string): Promise<{ csvMap: Map<string, CsvRecord[]>; header: string[] }> {
  log("📖 CSVファイルを読み込み中...");
  
  const csvMap = new Map<string, CsvRecord[]>(); // key: 正規化された社名, value: CSVレコードの配列
  let header: string[] = [];
  
  return new Promise((resolve, reject) => {
    const fileStream = createReadStream(csvFilePath, { encoding: "utf8" });
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
        }
        
        rowCount++;
        if (rowCount % 100000 === 0) {
          log(`  📊 CSV読み込み中: ${rowCount.toLocaleString()} 行`);
        }
        
        const name = record["name"]?.trim();
        const corporateNumber = record["corporateNumber"]?.trim();
        const address = record["address"]?.trim();
        
        // 法人番号が有効で、社名または住所がある場合のみ処理
        if (!corporateNumber || !/^\d{13}$/.test(corporateNumber)) {
          continue;
        }
        
        if (name) {
          // 社名を正規化（空白を削除、小文字化）
          const normalizedName = name.replace(/\s+/g, "").toLowerCase();
          if (!csvMap.has(normalizedName)) {
            csvMap.set(normalizedName, []);
          }
          
          // 軽量化: 必要なフィールドだけを保持
          const fields = new Map<string, any>();
          for (const [key, value] of Object.entries(record)) {
            if (value && value.trim() !== "") {
              fields.set(key, value);
            }
          }
          
          csvMap.get(normalizedName)!.push({
            name,
            corporateNumber,
            address: address || "",
            fields,
          });
        }
      }
    });
    
    parser.on("error", (err) => {
      reject(err);
    });
    
    parser.on("end", () => {
      log(`✅ CSV読み込み完了: ${rowCount.toLocaleString()} 行、${csvMap.size.toLocaleString()} 件の社名マップを作成`);
      resolve({ csvMap, header });
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

async function getNullCorporateNumberDocs(limit: number = 50000): Promise<CompanyDoc[]> {
  log("🔍 法人番号がnullのドキュメントを検索中...");
  
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
// CSVマップからマッチング
// ==============================

function findMatchingCsvRecord(
  doc: CompanyDoc,
  csvMap: Map<string, CsvRecord[]>
): CsvRecord | null {
  const docName = doc.data.name?.trim();
  const docAddress = doc.data.address?.trim();
  
  if (!docName) return null;
  
  // 社名を正規化
  const normalizedName = docName.replace(/\s+/g, "").toLowerCase();
  
  // CSVマップから候補を取得
  const candidates = csvMap.get(normalizedName);
  if (!candidates || candidates.length === 0) {
    return null;
  }
  
  // 複数の候補がある場合、住所でマッチング
  if (candidates.length === 1) {
    return candidates[0];
  }
  
  // 住所が一致するものを優先
  if (docAddress) {
    const normalizedDocAddress = docAddress.replace(/\s+/g, "").toLowerCase();
    
    for (const candidate of candidates) {
      const candidateAddress = candidate.address?.replace(/\s+/g, "").toLowerCase() || "";
      
      // 住所の最初の部分（都道府県+市区町村）が一致するか確認
      const docAddrStart = normalizedDocAddress.substring(0, Math.min(20, normalizedDocAddress.length));
      const candidateAddrStart = candidateAddress.substring(0, Math.min(20, candidateAddress.length));
      
      if (docAddrStart === candidateAddrStart || 
          candidateAddress.includes(docAddrStart) || 
          normalizedDocAddress.includes(candidateAddrStart)) {
        return candidate;
      }
    }
  }
  
  // 住所が一致しない場合は最初の候補を返す
  return candidates[0];
}

// ==============================
// メイン処理
// ==============================

async function main() {
  initAdmin();
  
  if (!fs.existsSync(CSV_FILE)) {
    log(`❌ エラー: CSVファイルが見つかりません: ${CSV_FILE}`);
    process.exit(1);
  }
  
  // 1. CSVからマップを構築
  const { csvMap, header } = await buildCsvMap(CSV_FILE);
  
  // 2. 法人番号がnullのドキュメントを取得
  const nullDocs = await getNullCorporateNumberDocs(50000);
  
  if (nullDocs.length === 0) {
    log("✅ 法人番号がnullのドキュメントは見つかりませんでした");
    return;
  }
  
  // 3. CSVマップからマッチングして更新
  log(`\n🔄 法人番号とその他のフィールドの更新を開始...`);
  
  const BATCH_SIZE = 500;
  let updatedCount = 0;
  let notFoundCount = 0;
  const updatedDocIds: Array<{ docId: string; corporateNumber: string; name: string }> = [];
  
  for (let i = 0; i < nullDocs.length; i += BATCH_SIZE) {
    const batch = nullDocs.slice(i, i + BATCH_SIZE);
    const batchWrite = db.batch();
    let batchUpdatedCount = 0;
    
    for (const doc of batch) {
      const csvRecord = findMatchingCsvRecord(doc, csvMap);
      
      if (csvRecord) {
        const existingData = doc.data;
        const updateData: Record<string, any> = {};
        let hasUpdate = false;
        
        // CSVの各フィールドをチェック
        for (const fieldName of header) {
          const csvRawValue = csvRecord.fields.get(fieldName);
          const csvValue = csvRawValue ? convertValue(csvRawValue, fieldName) : null;
          const existingValue = existingData[fieldName];
          
          // 既存の値がnull/undefined/空文字列の場合のみ更新
          if (isNullish(existingValue) && !isNullish(csvValue)) {
            updateData[fieldName] = csvValue;
            hasUpdate = true;
          }
        }
        
        // 法人番号は必ず更新（既存がnullの場合）
        if (isNullish(existingData.corporateNumber)) {
          updateData.corporateNumber = csvRecord.corporateNumber;
          hasUpdate = true;
        }
        
        if (hasUpdate) {
          updateData.updatedAt = admin.firestore.Timestamp.now();
          batchWrite.update(doc.ref, updateData);
          batchUpdatedCount++;
          
          // 更新したドキュメントIDを記録（最初の100件まで）
          if (updatedDocIds.length < 100) {
            updatedDocIds.push({
              docId: doc.ref.id,
              corporateNumber: csvRecord.corporateNumber,
              name: doc.data.name || "(社名なし)",
            });
          }
        } else {
          notFoundCount++;
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
