/* 
  gBizINFO CSV統合結果を companies_new に反映するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/update_companies_from_gbizinfo_csv.ts [CSVファイルのパス]
    
  オプション:
    --dry-run: 実際には更新せず、更新予定の内容を表示
    --limit=N: 処理する行数を制限（テスト用）
    
  環境変数:
    CSV_FILE: CSVファイルのパス（引数より優先）
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";
import { createReadStream } from "fs";

const COLLECTION_NAME = "companies_new";

// CSVファイルのパスを取得（環境変数 > 引数 > デフォルト）
function getCsvFilePath(): string {
  // 環境変数から取得
  if (process.env.CSV_FILE) {
    return path.resolve(process.env.CSV_FILE);
  }
  
  // 引数から取得（--dry-runや--limit=以外の最初の引数）
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
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  
  // デフォルトのパスを試す
  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
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
        console.log(`ℹ️  サービスアカウントキーを使用: ${resolvedPath}`);
        break;
      }
    }
  }
  
  if (!serviceAccountPath) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }
  
  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

const DRY_RUN = process.argv.includes("--dry-run");
const LIMIT = process.argv.find((arg: string) => arg.startsWith("--limit="))
  ? parseInt(process.argv.find((arg: string) => arg.startsWith("--limit="))!.split("=")[1])
  : null;

// ==============================
// ユーティリティ関数
// ==============================

function log(message: string) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
}

function trim(value: string | undefined | null): string {
  if (!value || typeof value !== "string") return "";
  return value.trim();
}

function isEmpty(value: string | null | undefined): boolean {
  return !value || trim(value) === "";
}

// null/undefined/空文字列をnullとして扱う
function isNullish(value: any): boolean {
  return value === null || value === undefined || value === "";
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
// ドキュメント検索関数
// ==============================

interface CompanyDoc {
  ref: DocumentReference;
  data: Record<string, any>;
}

  // バッチでドキュメントを検索（高速化のため）
// 法人番号で検索できない場合は、社名や住所で検索するフォールバックを追加
async function findCompaniesBatch(
  records: Array<{ record: Record<string, string>; rowNumber: number; corporateNumber: string }>
): Promise<Map<string, CompanyDoc>> {
  const result = new Map<string, CompanyDoc>();
  
  if (records.length === 0) return result;
  
  // 1. 法人番号でdocId直接参照を並列実行（高速化のポイント）
  const corporateNumbers = records.map(r => r.corporateNumber);
  const directRefs = corporateNumbers
    .filter(corpNum => corpNum && corpNum.trim() !== "")
    .map(corpNum => companiesCol.doc(corpNum.trim()));
  
  // 並列でget()を実行（最大500件まで、Firestoreの制限）
  const BATCH_GET_SIZE = 500;
  for (let i = 0; i < directRefs.length; i += BATCH_GET_SIZE) {
    const batchRefs = directRefs.slice(i, i + BATCH_GET_SIZE);
    const batchRecords = records.slice(i, i + BATCH_GET_SIZE);
    
    const directSnaps = await Promise.allSettled(
      batchRefs.map(ref => ref.get())
    );
    
    directSnaps.forEach((settled, index) => {
      const record = batchRecords[index];
      if (!record) return;
      
      if (settled.status === "fulfilled" && settled.value.exists) {
        result.set(record.corporateNumber, {
          ref: batchRefs[index],
          data: settled.value.data() || {},
        });
      }
    });
  }
  
  // 2. 見つからなかったものはwhereクエリで法人番号検索（並列実行数を制限）
  const notFoundRecords = records.filter(
    r => r.corporateNumber && r.corporateNumber.trim() !== "" && !result.has(r.corporateNumber.trim())
  );
  
  if (notFoundRecords.length > 0) {
    const CONCURRENT_QUERIES = process.env.CONCURRENT_QUERIES ? parseInt(process.env.CONCURRENT_QUERIES) : 40;
    for (let i = 0; i < notFoundRecords.length; i += CONCURRENT_QUERIES) {
      const batch = notFoundRecords.slice(i, i + CONCURRENT_QUERIES);
      const queryResults = await Promise.allSettled(
        batch.map(record => {
          const normalizedCorpNum = record.corporateNumber.trim();
          return companiesCol
            .where("corporateNumber", "==", normalizedCorpNum)
            .limit(1)
            .get();
        })
      );
      
      queryResults.forEach((settled, batchIndex) => {
        const record = batch[batchIndex];
        if (!record) return;
        
        if (settled.status === "fulfilled" && !settled.value.empty) {
          const doc = settled.value.docs[0];
          result.set(record.corporateNumber, {
            ref: doc.ref,
            data: doc.data() || {},
          });
        }
      });
    }
  }
  
  // 3. まだ見つからなかったものは、社名で検索（法人番号がnullのドキュメントを探す）
  const stillNotFoundRecords = records.filter(
    r => r.corporateNumber && r.corporateNumber.trim() !== "" && !result.has(r.corporateNumber.trim())
  );
  
  if (stillNotFoundRecords.length > 0) {
    const CONCURRENT_QUERIES = process.env.CONCURRENT_QUERIES ? parseInt(process.env.CONCURRENT_QUERIES) : 40;
    for (let i = 0; i < stillNotFoundRecords.length; i += CONCURRENT_QUERIES) {
      const batch = stillNotFoundRecords.slice(i, i + CONCURRENT_QUERIES);
      const queryResults = await Promise.allSettled(
        batch.map(record => {
          const name = record.record["name"]?.trim();
          if (!name) {
            return Promise.resolve({ empty: true, docs: [] } as any);
          }
          
          // 社名で検索（法人番号がnullのドキュメントも含む）
          return companiesCol
            .where("name", "==", name)
            .limit(10) // 同名企業がある可能性があるため、複数取得
            .get();
        })
      );
      
      queryResults.forEach((settled, batchIndex) => {
        const record = batch[batchIndex];
        if (!record) return;
        
        if (settled.status === "fulfilled" && !settled.value.empty) {
          const docs = settled.value.docs;
          const csvName = record.record["name"]?.trim();
          const csvAddress = record.record["address"]?.trim();
          
          // 最も一致するドキュメントを選択
          // 優先順位: 1) 法人番号がnull + 住所が一致 2) 法人番号がnull 3) 住所が一致
          let bestMatch = null;
          let bestScore = 0;
          
          for (const doc of docs) {
            const docData = doc.data();
            const docCorpNum = docData.corporateNumber;
            const docAddress = docData.address?.trim();
            const docName = docData.name?.trim();
            
            // 社名が完全一致することを確認
            if (docName !== csvName) continue;
            
            let score = 0;
            
            // 法人番号がnullの場合、スコア+10
            if (isNullish(docCorpNum)) {
              score += 10;
            }
            
            // 住所が一致する場合、スコア+5
            if (csvAddress && docAddress) {
              // 住所の最初の部分（都道府県+市区町村）が一致するか確認
              const csvAddrStart = csvAddress.substring(0, Math.min(20, csvAddress.length));
              const docAddrStart = docAddress.substring(0, Math.min(20, docAddress.length));
              if (csvAddrStart === docAddrStart || 
                  docAddress.includes(csvAddrStart) || 
                  csvAddress.includes(docAddrStart)) {
                score += 5;
              }
            }
            
            // より高いスコアのドキュメントを選択
            if (score > bestScore) {
              bestScore = score;
              bestMatch = doc;
            }
          }
          
          // 最適なマッチが見つかった場合のみ追加（スコアが5以上）
          if (bestMatch && bestScore >= 5) {
            result.set(record.corporateNumber, {
              ref: bestMatch.ref,
              data: bestMatch.data() || {},
            });
          }
        }
      });
    }
  }
  
  return result;
}

// ==============================
// チャンク処理関数
// ==============================

// CSVからcompanies_newコレクション用のデータを構築
function buildCompanyDataFromCsv(
  record: Record<string, string>,
  header: string[]
): Record<string, any> | null {
  // 最低限、nameまたはcorporateNumberが必要
  const name = record["name"]?.trim();
  const corporateNumber = record["corporateNumber"]?.trim();
  
  if (!name && !corporateNumber) {
    return null; // 必須情報がない場合はスキップ
  }
  
  // companies_newコレクションの基本テンプレート（配列フィールドは空配列）
  const data: Record<string, any> = {
    industries: [],
    businessItems: [],
    tags: [],
    urls: [],
    banks: [],
    suppliers: [],
    clients: [],
    subsidiaries: [],
    shareholders: [],
    badges: [],
  };
  
  // CSVの各フィールドをマッピング
  for (const fieldName of header) {
    const csvValue = convertValue(record[fieldName], fieldName);
    
    // nullでない値のみ設定
    if (!isNullish(csvValue)) {
      data[fieldName] = csvValue;
    } else {
      // nullの場合は、配列フィールドは空配列、それ以外はnull
      if (fieldName === "industries" || fieldName === "businessItems" || 
          fieldName === "tags" || fieldName === "urls" || fieldName === "banks" ||
          fieldName === "suppliers" || fieldName === "clients" || 
          fieldName === "subsidiaries" || fieldName === "shareholders" ||
          fieldName === "badges") {
        data[fieldName] = [];
      } else {
        data[fieldName] = null;
      }
    }
  }
  
  // タイムスタンプを設定
  const now = admin.firestore.Timestamp.now();
  data.createdAt = now;
  data.updatedAt = now;
  data.updateDate = now.toDate().toISOString().split("T")[0];
  data.updateCount = 0;
  data.changeCount = 0;
  
  return data;
}

// チャンクを処理する関数（高速化のため）
async function processChunk(
  records: Array<{ record: Record<string, string>; rowNumber: number }>,
  header: string[],
  stats: {
    processedCount: number;
    updatedCount: number;
    notFoundCount: number;
    skippedCount: number;
    foundButNoUpdateCount: number;
    csvEmptyCount: number;
    existingHasValueCount: number;
    createdCount: number;
  }
): Promise<{
  updates: Array<{ docRef: DocumentReference; updateData: Record<string, any> }>;
  creates: Array<{ docRef: DocumentReference; createData: Record<string, any> }>;
}> {
  // 法人番号でフィルタリング
  const validRecords = records
    .map(({ record, rowNumber }) => {
      const corporateNumber = record["corporateNumber"]?.trim();
      return corporateNumber ? { record, rowNumber, corporateNumber } : null;
    })
    .filter((item): item is { record: Record<string, string>; rowNumber: number; corporateNumber: string } => item !== null);
  
  if (validRecords.length === 0) {
    stats.skippedCount += records.length;
    return { updates: [], creates: [] };
  }
  
  // バッチでドキュメントを検索（高速化のポイント）
  // 法人番号で検索できない場合は、社名や住所で検索するフォールバックを追加
  const docMap = await findCompaniesBatch(validRecords);
  
  // 更新データと新規作成データを構築
  const updates: Array<{ docRef: DocumentReference; updateData: Record<string, any> }> = [];
  const creates: Array<{ docRef: DocumentReference; createData: Record<string, any> }> = [];
  
  for (const { record, rowNumber, corporateNumber } of validRecords) {
    const companyDoc = docMap.get(corporateNumber);
    
    if (!companyDoc) {
      // 見つからない場合は新規作成
      const createData = buildCompanyDataFromCsv(record, header);
      if (createData) {
        // docIdは法人番号を使用（13桁の数値であることを確認）
        const docId = /^\d{13}$/.test(corporateNumber.trim()) 
          ? corporateNumber.trim() 
          : companiesCol.doc().id; // 法人番号が無効な場合は自動生成
        const docRef = companiesCol.doc(docId);
        creates.push({ docRef, createData });
        stats.createdCount++;
        stats.processedCount++;
      } else {
        stats.notFoundCount++;
      }
      continue;
    }
    
    const existingData = companyDoc.data;
    
    // 更新データを構築（nullの値のみCSVから設定）
    const updateData: Record<string, any> = {};
    let hasUpdate = false;
    
    for (const fieldName of header) {
      const csvValue = convertValue(record[fieldName], fieldName);
      const existingValue = existingData[fieldName];
      
      // corporateNumberの特別処理
      if (fieldName === "corporateNumber") {
        // 既存の値がnull/undefined/空文字列の場合のみ更新
        // ただし、CSVの値が有効な13桁の法人番号であることを確認
        if (isNullish(existingValue) && !isNullish(csvValue)) {
          const corpNum = String(csvValue).trim();
          // 13桁の数値であることを確認
          if (/^\d{13}$/.test(corpNum)) {
            updateData[fieldName] = corpNum;
            hasUpdate = true;
          }
        }
        continue;
      }
      
      // 既存の値がnull/undefined/空文字列の場合のみ更新
      if (isNullish(existingValue) && !isNullish(csvValue)) {
        updateData[fieldName] = csvValue;
        hasUpdate = true;
      }
    }
    
    // 更新されなかった場合の理由を記録（最初の100件のみ）
    if (!hasUpdate && stats.foundButNoUpdateCount < 100) {
      let csvEmpty = true;
      let existingHasValue = false;
      
      for (const fieldName of header) {
        const csvValue = convertValue(record[fieldName], fieldName);
        const existingValue = existingData[fieldName];
        
        if (!isNullish(csvValue)) {
          csvEmpty = false;
        }
        if (!isNullish(existingValue)) {
          existingHasValue = true;
        }
      }
      
      if (csvEmpty) {
        stats.csvEmptyCount++;
      }
      if (existingHasValue) {
        stats.existingHasValueCount++;
      }
      stats.foundButNoUpdateCount++;
    }
    
    if (hasUpdate) {
      updates.push({ docRef: companyDoc.ref, updateData });
      stats.updatedCount++;
    }
    
    stats.processedCount++;
  }
  
  stats.skippedCount += records.length - validRecords.length;
  
  return { updates, creates };
}

// ==============================
// CSV読み込みと更新処理
// ==============================

async function processCsv(): Promise<void> {
  if (!fs.existsSync(CSV_FILE)) {
    log(`❌ エラー: CSVファイルが見つかりません: ${CSV_FILE}`);
    log(``);
    log(`📝 CSVファイルを指定する方法:`);
    log(`   1. 引数として指定: npx tsx scripts/update_companies_from_gbizinfo_csv.ts /path/to/file.csv`);
    log(`   2. 環境変数で指定: CSV_FILE=/path/to/file.csv npx tsx scripts/update_companies_from_gbizinfo_csv.ts`);
    log(`   3. デフォルトパスに配置: ${path.join(__dirname, "../out/gBizINFO/companies_export.csv")}`);
    process.exit(1);
  }
  
  log(`📖 CSVファイル読み込み開始: ${path.basename(CSV_FILE)}`);
  
  let header: string[] = [];
  let rowCount = 0;
  let processedCount = 0;
  let updatedCount = 0;
  let notFoundCount = 0;
  let skippedCount = 0;
  
  const BATCH_SIZE = 500; // Firestoreバッチサイズ（変更不可：Firestoreの制限）
  // 環境変数で調整可能、デフォルト値をメモリ効率的な設定に変更
  const CHUNK_SIZE = process.env.CHUNK_SIZE ? parseInt(process.env.CHUNK_SIZE) : 5000; // チャンクサイズ（メモリ効率重視）
  const CONCURRENT_CHUNKS = process.env.CONCURRENT_CHUNKS ? parseInt(process.env.CONCURRENT_CHUNKS) : 20; // 並列処理するチャンク数（メモリ効率重視）
  const MAX_BUFFER_SIZE = CHUNK_SIZE * CONCURRENT_CHUNKS * 1.2; // 最大バッファサイズ（メモリ保護、1.2倍に調整）
  
  // バッチ管理（スレッドセーフ）
  let batch: WriteBatch | null = null;
  let batchCount = 0;
  let batchLock = false;
  
  // チャンクバッファ
  const chunkBuffer: Array<{ record: Record<string, string>; rowNumber: number }> = [];
  let isPaused = false;
  const activeChunks = new Set<Promise<void>>();
  
  const stats = {
    processedCount: 0,
    updatedCount: 0,
    createdCount: 0, // 新規作成件数
    notFoundCount: 0,
    skippedCount: 0,
    foundButNoUpdateCount: 0, // 見つかったが更新されなかった件数
    csvEmptyCount: 0, // CSVの値が空だった件数
    existingHasValueCount: 0, // 既存の値がnullでなかった件数
  };
  
  // バッチコミット関数（スレッドセーフ）
  async function commitBatch(): Promise<void> {
    // ロックを取得
    while (batchLock) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    
    if (!batch || batchCount === 0) return;
    
    batchLock = true;
    try {
      const currentBatch = batch;
      const currentBatchCount = batchCount;
      batch = null;
      batchCount = 0;
      
      if (!DRY_RUN && currentBatch) {
        await currentBatch.commit();
      }
      
      // 進捗ログのみ出力
      log(`  📝 進行中: ${stats.processedCount.toLocaleString()} 社処理、${updatedCount.toLocaleString()} 社更新、${stats.createdCount.toLocaleString()} 社新規作成`);
    } finally {
      batchLock = false;
    }
  }
  
  // バッチに更新と新規作成を追加（スレッドセーフ）
  async function addToBatch(
    updates: Array<{ docRef: DocumentReference; updateData: Record<string, any> }>,
    creates: Array<{ docRef: DocumentReference; createData: Record<string, any> }>
  ) {
    // 更新を追加
    for (const { docRef, updateData } of updates) {
      // バッチロックを取得
      while (batchLock) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      
      if (!batch) {
        batch = db.batch();
      }
      
      if (!DRY_RUN) {
        batch.update(docRef, updateData);
        batchCount++;
      }
      
      updatedCount++;
      
      // バッチコミット
      if (batchCount >= BATCH_SIZE) {
        await commitBatch();
      }
    }
    
    // 新規作成を追加
    for (const { docRef, createData } of creates) {
      // バッチロックを取得
      while (batchLock) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      
      if (!batch) {
        batch = db.batch();
      }
      
      if (!DRY_RUN) {
        batch.set(docRef, createData);
        batchCount++;
      }
      
      // バッチコミット
      if (batchCount >= BATCH_SIZE) {
        await commitBatch();
      }
    }
  }
  
  // チャンクを処理する関数（並列実行可能）
  async function processChunkBuffer(): Promise<void> {
    if (chunkBuffer.length === 0) return;
    
    const chunk = chunkBuffer.splice(0, CHUNK_SIZE);
    const { updates, creates } = await processChunk(chunk, header, stats);
    
    // バッチに追加（スレッドセーフ）
    await addToBatch(updates, creates);
  }
  
  // 複数のチャンクを並列処理（残りのチャンクを処理する場合に使用）
  async function processChunksParallel() {
    // 並列処理できるチャンク数を制限
    while (chunkBuffer.length >= CHUNK_SIZE && activeChunks.size < CONCURRENT_CHUNKS) {
      const chunkPromise = processChunkBuffer().catch((error: any) => {
        log(`❌ チャンク処理エラー: ${error.message}`);
      }).finally(() => {
        activeChunks.delete(chunkPromise);
      });
      
      activeChunks.add(chunkPromise);
    }
    
    // 開始したチャンク処理が完了するまで待機
    if (activeChunks.size > 0) {
      await Promise.race(Array.from(activeChunks));
    }
  }
  
  return new Promise(async (resolve, reject) => {
    const parser = parse({
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      bom: true,
      skip_records_with_error: true, // エラー行をスキップして続行
    });
    
    // 処理完了関数
    async function finalizeProcessing() {
      // すべての並列処理が完了するまで待機
      while (activeChunks.size > 0) {
        await Promise.race(Array.from(activeChunks));
      }
      
      // 残りのチャンクを処理（並列処理）
      while (chunkBuffer.length >= CHUNK_SIZE) {
        await processChunksParallel();
      }
      
      // 残りの小さなチャンクも処理
      if (chunkBuffer.length > 0) {
        await processChunkBuffer();
      }
      
      // すべての並列処理が完了するまで待機
      while (activeChunks.size > 0) {
        await Promise.race(Array.from(activeChunks));
      }
      
      // 残りのバッチをコミット
      if (batch && batchCount > 0) {
        if (!DRY_RUN) {
          await batch.commit();
        }
      }
      
      // 統計を更新
      processedCount = stats.processedCount;
      notFoundCount = stats.notFoundCount;
      skippedCount = stats.skippedCount;
      
      log("\n" + "=".repeat(60));
      log("処理完了");
      log("=".repeat(60));
      log(`📊 総行数: ${rowCount.toLocaleString()}`);
      log(`📝 処理済み: ${processedCount.toLocaleString()} 社`);
      log(`✅ 更新: ${updatedCount.toLocaleString()} 社`);
      log(`🆕 新規作成: ${stats.createdCount.toLocaleString()} 社`);
      log(`⚠️  見つからない: ${notFoundCount.toLocaleString()} 社`);
      log(`⏭️  スキップ: ${skippedCount.toLocaleString()} 行（法人番号なし）`);
      log(`\n🔍 更新されなかった理由（最初の100件の分析）:`);
      log(`   - 見つかったが更新なし: ${stats.foundButNoUpdateCount} 社`);
      log(`   - CSVの値が空: ${stats.csvEmptyCount} 社`);
      log(`   - 既存の値がnullでない: ${stats.existingHasValueCount} 社`);
      
      resolve();
    }
    
    createReadStream(CSV_FILE, { encoding: "utf8" })
      .pipe(parser)
      .on("headers", (headers: string[]) => {
        header = headers;
        log(`📋 ヘッダー: ${headers.length} カラム`);
        if (DRY_RUN) {
          log("🔍 DRY_RUN モード: 実際には更新しません");
        }
        log("📊 データ処理を開始します...");
      })
      .on("data", async (record: Record<string, string>) => {
        if (isPaused) return;
        rowCount++;
        
        if (LIMIT && rowCount > LIMIT) {
          if (!isPaused) {
            isPaused = true;
            parser.pause();
            log(`  ⏸️  制限に達したため読み込みを停止: ${LIMIT} 行`);
            log(`  🔄 残りのデータを処理中...`);
            
            // 残りのチャンクバッファを処理
            finalizeProcessing().catch((error: any) => {
              log(`❌ チャンク処理エラー: ${error.message}`);
              reject(error);
            });
          }
          return;
        }
        
        // チャンクバッファに追加
        chunkBuffer.push({ record, rowNumber: rowCount });
        
        // メモリ保護: バッファサイズが上限に達したら、処理を待機（より積極的に）
        if (chunkBuffer.length >= MAX_BUFFER_SIZE) {
          parser.pause();
          // 並列処理が完了するまで待機（複数回待機してメモリを解放）
          while (activeChunks.size > 0 && chunkBuffer.length >= MAX_BUFFER_SIZE * 0.8) {
            await Promise.race(Array.from(activeChunks));
            // ガベージコレクションを促すために少し待機
            await new Promise(resolve => setTimeout(resolve, 10));
          }
          if (!isPaused) {
            parser.resume();
          }
        }
        
        // 早期メモリ保護: バッファサイズが上限の80%に達したら、処理を待機
        if (chunkBuffer.length >= MAX_BUFFER_SIZE * 0.8 && activeChunks.size >= CONCURRENT_CHUNKS) {
          parser.pause();
          // 1つ以上のチャンクが完了するまで待機
          while (activeChunks.size >= CONCURRENT_CHUNKS && chunkBuffer.length >= MAX_BUFFER_SIZE * 0.8) {
            await Promise.race(Array.from(activeChunks));
          }
          if (!isPaused) {
            parser.resume();
          }
        }
        
        // チャンクサイズに達したら並列処理開始
        if (chunkBuffer.length >= CHUNK_SIZE) {
          // 並列処理数をチェック
          if (activeChunks.size >= CONCURRENT_CHUNKS) {
            // 並列処理数が上限に達したら、1つ完了するまで待機
            parser.pause();
            await Promise.race(Array.from(activeChunks));
            if (!isPaused) {
              parser.resume();
            }
          }
          
          // 並列処理を開始（非ブロッキング）
          const chunkPromise = processChunkBuffer().catch((error: any) => {
            log(`❌ チャンク処理エラー: ${error.message}`);
            // エラーは記録するが、処理は続行
          }).finally(() => {
            activeChunks.delete(chunkPromise);
          });
          
          activeChunks.add(chunkPromise);
        }
        
        // 進捗ログ（100000行ごと）
        if (rowCount % 100000 === 0) {
          log(`  📊 読み込み中: ${rowCount.toLocaleString()} 行`);
        }
      })
      .on("end", async () => {
        await finalizeProcessing();
      })
      .on("error", (error: any) => {
        log(`❌ エラー: ${error.message}`);
        reject(error);
      });
  });
}

// ==============================
// メイン処理
// ==============================

async function main() {
  log("🚀 gBizINFO CSV統合結果の反映開始");
  log(`📁 使用するCSVファイル: ${CSV_FILE}`);
  
  try {
    await processCsv();
    log("\n✅ 処理完了");
  } catch (error: any) {
    log(`❌ エラー: ${error.message}`);
    console.error(error);
    process.exit(1);
  }
}

// 実行
main();

