/* 
  gBizINFO CSV統合結果を companies_new に反映するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/update_companies_from_gbizinfo_csv.ts
    
  オプション:
    --dry-run: 実際には更新せず、更新予定の内容を表示
    --limit=N: 処理する行数を制限（テスト用）
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
const CSV_FILE = path.join(__dirname, "../out/gBizINFO/companies_export.csv");

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
async function findCompaniesBatch(
  corporateNumbers: string[]
): Promise<Map<string, CompanyDoc>> {
  const result = new Map<string, CompanyDoc>();
  
  if (corporateNumbers.length === 0) return result;
  
  // 1. docIdで直接参照を並列実行（高速化のポイント）
  const directRefs = corporateNumbers
    .filter(corpNum => corpNum && corpNum.trim() !== "")
    .map(corpNum => companiesCol.doc(corpNum.trim()));
  
  // 並列でget()を実行（最大500件まで）
  const BATCH_GET_SIZE = 500;
  for (let i = 0; i < directRefs.length; i += BATCH_GET_SIZE) {
    const batchRefs = directRefs.slice(i, i + BATCH_GET_SIZE);
    const batchCorpNums = corporateNumbers.slice(i, i + BATCH_GET_SIZE);
    
    const directSnaps = await Promise.allSettled(
      batchRefs.map(ref => ref.get())
    );
    
    directSnaps.forEach((settled, index) => {
      const corpNum = batchCorpNums[index]?.trim();
      if (!corpNum) return;
      
      if (settled.status === "fulfilled" && settled.value.exists) {
        result.set(corpNum, {
          ref: batchRefs[index],
          data: settled.value.data() || {},
        });
      }
    });
  }
  
  // 2. 見つからなかったものはwhereクエリで検索（並列実行数を制限）
  const notFoundCorpNums = corporateNumbers.filter(
    corpNum => corpNum && corpNum.trim() !== "" && !result.has(corpNum.trim())
  );
  
  if (notFoundCorpNums.length > 0) {
    // Firestoreの同時クエリ制限を考慮して並列実行数を制限
    const CONCURRENT_QUERIES = 10;
    for (let i = 0; i < notFoundCorpNums.length; i += CONCURRENT_QUERIES) {
      const batch = notFoundCorpNums.slice(i, i + CONCURRENT_QUERIES);
      const queryResults = await Promise.allSettled(
        batch.map(corpNum => {
          const normalizedCorpNum = corpNum.trim();
          return companiesCol
            .where("corporateNumber", "==", normalizedCorpNum)
            .limit(1)
            .get();
        })
      );
      
      queryResults.forEach((settled, batchIndex) => {
        const corpNum = batch[batchIndex]?.trim();
        if (!corpNum) return;
        
        if (settled.status === "fulfilled" && !settled.value.empty) {
          const doc = settled.value.docs[0];
          result.set(corpNum, {
            ref: doc.ref,
            data: doc.data() || {},
          });
        }
      });
    }
  }
  
  return result;
}

// ==============================
// チャンク処理関数
// ==============================

// チャンクを処理する関数（高速化のため）
async function processChunk(
  records: Array<{ record: Record<string, string>; rowNumber: number }>,
  header: string[],
  stats: {
    processedCount: number;
    updatedCount: number;
    notFoundCount: number;
    skippedCount: number;
  }
): Promise<Array<{ docRef: DocumentReference; updateData: Record<string, any> }>> {
  // 法人番号でフィルタリング
  const validRecords = records
    .map(({ record, rowNumber }) => {
      const corporateNumber = record["corporateNumber"]?.trim();
      return corporateNumber ? { record, rowNumber, corporateNumber } : null;
    })
    .filter((item): item is { record: Record<string, string>; rowNumber: number; corporateNumber: string } => item !== null);
  
  if (validRecords.length === 0) {
    stats.skippedCount += records.length;
    return [];
  }
  
  // バッチでドキュメントを検索（高速化のポイント）
  const corporateNumbers = validRecords.map(r => r.corporateNumber);
  const docMap = await findCompaniesBatch(corporateNumbers);
  
  // 更新データを構築
  const updates: Array<{ docRef: DocumentReference; updateData: Record<string, any> }> = [];
  
  for (const { record, rowNumber, corporateNumber } of validRecords) {
    const companyDoc = docMap.get(corporateNumber);
    
    if (!companyDoc) {
      stats.notFoundCount++;
      continue;
    }
    
    const existingData = companyDoc.data;
    
    // 更新データを構築（nullの値のみCSVから設定）
    const updateData: Record<string, any> = {};
    let hasUpdate = false;
    
    for (const fieldName of header) {
      // corporateNumberは更新しない
      if (fieldName === "corporateNumber") continue;
      
      const csvValue = convertValue(record[fieldName], fieldName);
      const existingValue = existingData[fieldName];
      
      // 既存の値がnull/undefined/空文字列の場合のみ更新
      if (isNullish(existingValue) && !isNullish(csvValue)) {
        updateData[fieldName] = csvValue;
        hasUpdate = true;
      }
    }
    
    if (hasUpdate) {
      updates.push({ docRef: companyDoc.ref, updateData });
      stats.updatedCount++;
    }
    
    stats.processedCount++;
  }
  
  stats.skippedCount += records.length - validRecords.length;
  
  return updates;
}

// ==============================
// CSV読み込みと更新処理
// ==============================

async function processCsv(): Promise<void> {
  if (!fs.existsSync(CSV_FILE)) {
    log(`❌ エラー: CSVファイルが見つかりません: ${CSV_FILE}`);
    process.exit(1);
  }
  
  log(`📖 CSVファイル読み込み開始: ${path.basename(CSV_FILE)}`);
  
  let header: string[] = [];
  let rowCount = 0;
  let processedCount = 0;
  let updatedCount = 0;
  let notFoundCount = 0;
  let skippedCount = 0;
  
  const BATCH_SIZE = 500; // Firestoreバッチサイズ
  const CHUNK_SIZE = 2000; // チャンクサイズ（高速化のため）
  const CONCURRENT_CHUNKS = 5; // 並列処理するチャンク数（高速化のため）
  
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
    notFoundCount: 0,
    skippedCount: 0,
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
      
      log(`  📝 進行中: ${stats.processedCount.toLocaleString()} 社処理、${updatedCount.toLocaleString()} 社更新`);
    } finally {
      batchLock = false;
    }
  }
  
  // バッチに更新を追加（スレッドセーフ）
  async function addToBatch(updates: Array<{ docRef: DocumentReference; updateData: Record<string, any> }>) {
    for (const { docRef, updateData } of updates) {
      // バッチロックを取得
      while (batchLock) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      
      if (!batch) {
        batch = db.batch();
      }
      
      if (DRY_RUN) {
        if (updatedCount < 10) {
          log(`  🔍 DRY RUN - 更新予定: docId=${docRef.id}, フィールド数=${Object.keys(updateData).length}`);
          log(`    更新フィールド: ${Object.keys(updateData).join(", ")}`);
        }
      } else {
        batch.update(docRef, updateData);
        batchCount++;
      }
      
      updatedCount++;
      
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
    const updates = await processChunk(chunk, header, stats);
    
    // バッチに追加（スレッドセーフ）
    await addToBatch(updates);
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
      log(`⚠️  見つからない: ${notFoundCount.toLocaleString()} 社`);
      log(`⏭️  スキップ: ${skippedCount.toLocaleString()} 行（法人番号なし）`);
      
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
        log(`⚡ 高速化モード: チャンクサイズ ${CHUNK_SIZE}, バッチサイズ ${BATCH_SIZE}, 並列チャンク数 ${CONCURRENT_CHUNKS}`);
        log("📊 データ処理を開始します...");
      })
      .on("data", async (record: Record<string, string>) => {
        if (isPaused) return;
        rowCount++;
        
        // 最初の数行でログ出力
        if (rowCount <= 5) {
          log(`  📄 行 ${rowCount} を読み込み中... (corporateNumber: ${record["corporateNumber"]?.substring(0, 13) || "なし"})`);
        }
        
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
        
        if (rowCount % 10000 === 0) {
          log(`  📊 読み込み中: ${rowCount.toLocaleString()} 行、バッファ: ${chunkBuffer.length} 件`);
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

