/* 
  タイプIのcompanies_newコレクションの財務情報フィールドを実値に更新するスクリプト
  
  対象フィールド:
    - capitalStock (資本金)
    - fiscalMonth (直近決算年月)
    - revenue (直近売上)
    - latestProfit (直近利益)
  
  千円単位の値を1000倍して実値に変換します。
  
  ヘッダーと内容がずれていることがあるので、行単位で判断します。
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/fix_type_i_financials.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const BATCH_LIMIT = 500; // Firestoreのバッチ制限
const TYPE_I_FILE = "csv/132.csv";

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
      path.join(projectRoot, "config", "serviceAccountKey.json"),
      path.join(projectRoot, ".config", "serviceAccountKey.json"),
    ];

    for (const pth of defaultPaths) {
      const resolved = path.resolve(pth);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolved}`);
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
    const projectId =
      serviceAccount.project_id ||
      process.env.GCLOUD_PROJECT ||
      process.env.GCP_PROJECT;

    if (!projectId) {
      console.error("❌ エラー: Project ID を検出できませんでした");
      process.exit(1);
    }

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId,
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

// タイプIのCSVを行配列として読み込む（列インデックスベース）
function loadTypeICSVByIndex(csvFilePath: string): Array<Array<string>> {
  const buf = fs.readFileSync(csvFilePath);
  try {
    const records: Array<Array<string>> = parse(buf, {
      columns: false,  // ヘッダーを無視して配列として読み込む
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      skip_records_with_error: true,
    });
    console.log(`  📄 ${path.basename(csvFilePath)}: ${records.length} 行（タイプI: 列順序ベース）`);
    return records;
  } catch (err: any) {
    console.warn(`  ⚠️ ${path.basename(csvFilePath)}: CSVパースエラー - ${err.code || err.message}`);
    return [];
  }
}

// ヘッダー行から列インデックスを取得
function parseTypeIHeader(headerRow: Array<string>): Record<string, number> {
  const headerMap: Record<string, number> = {};
  
  for (let i = 0; i < headerRow.length; i++) {
    const header = headerRow[i]?.trim() || "";
    headerMap[header] = i;
  }
  
  return headerMap;
}

// 数値を1000倍する（千円単位から実値に変換）
function convertToActualValue(value: any): number | null {
  if (value === null || value === undefined) return null;
  
  // 数値の場合
  if (typeof value === "number") {
    // 0の場合はnullを返す
    if (value === 0) return null;
    // 既に大きな値（1億以上）の場合は変換済みと判断
    if (value >= 100000000) {
      return value; // 既に実値の可能性が高い
    }
    // 千円単位の値を実値に変換
    return value * 1000;
  }
  
  // 文字列の場合
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed || trimmed === "0" || trimmed === "非上場") return null;
    
    // カンマや空白を除去
    const cleaned = trimmed.replace(/[,\s]/g, "");
    if (!cleaned) return null;
    
    // 数値でない場合はnullを返す
    if (!/^\d+(\.\d+)?$/.test(cleaned)) return null;
    
    const num = Number(cleaned);
    if (!Number.isFinite(num) || num === 0) return null;
    
    // 既に大きな値（1億以上）の場合は変換済みと判断
    if (num >= 100000000) {
      return num; // 既に実値の可能性が高い
    }
    // 千円単位の値を実値に変換
    return num * 1000;
  }
  
  return null;
}

// 行から財務情報を抽出（行単位で判断）
function extractFinancialDataFromRow(
  row: Array<string>,
  headerMap: Record<string, number>
): {
  corporateNumber: string | null;
  capitalStock: number | null;
  fiscalMonth: string | null;
  revenue: number | null;
  latestProfit: number | null;
} {
  // 法人番号を取得（複数の可能性のある列名をチェック）
  const corporateNumberCols = ["法人番号", "corporateNumber"];
  let corporateNumber: string | null = null;
  for (const colName of corporateNumberCols) {
    const idx = headerMap[colName];
    if (idx !== undefined && row[idx]?.trim()) {
      corporateNumber = row[idx].trim();
      break;
    }
  }
  
  // 資本金を取得
  const capitalCols = ["資本金"];
  let capitalStock: number | null = null;
  for (const colName of capitalCols) {
    const idx = headerMap[colName];
    if (idx !== undefined && row[idx]?.trim()) {
      const value = convertToActualValue(row[idx]);
      if (value !== null) {
        capitalStock = value;
        break;
      }
    }
  }
  
  // 決算月1を取得（直近決算年月）
  const fiscalMonthCols = ["決算月1"];
  let fiscalMonth: string | null = null;
  for (const colName of fiscalMonthCols) {
    const idx = headerMap[colName];
    if (idx !== undefined && row[idx]?.trim()) {
      fiscalMonth = row[idx].trim();
      break;
    }
  }
  
  // 売上1を取得（直近売上）
  const revenueCols = ["売上1"];
  let revenue: number | null = null;
  for (const colName of revenueCols) {
    const idx = headerMap[colName];
    if (idx !== undefined && row[idx]?.trim()) {
      const value = convertToActualValue(row[idx]);
      if (value !== null) {
        revenue = value;
        break;
      }
    }
  }
  
  // 利益1を取得（直近利益）
  const profitCols = ["利益1"];
  let latestProfit: number | null = null;
  for (const colName of profitCols) {
    const idx = headerMap[colName];
    if (idx !== undefined && row[idx]?.trim()) {
      const value = convertToActualValue(row[idx]);
      if (value !== null) {
        latestProfit = value;
        break;
      }
    }
  }
  
  return {
    corporateNumber,
    capitalStock,
    fiscalMonth,
    revenue,
    latestProfit,
  };
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード: Firestore は書き換えません\n" : "⚠️  本番モード: Firestore を書き換えます\n");

  // タイプIのCSVファイルを読み込み
  console.log("📖 タイプIのCSVファイルを読み込み中...");
  const filePath = path.resolve(TYPE_I_FILE);
  if (!fs.existsSync(filePath)) {
    console.error(`❌ ファイルが見つかりません: ${filePath}`);
    process.exit(1);
  }
  
  const records = loadTypeICSVByIndex(filePath);
  if (records.length === 0) {
    console.error("❌ CSVファイルにデータがありません");
    process.exit(1);
  }
  
  // ヘッダー行を取得
  const headerRow = records[0];
  const headerMap = parseTypeIHeader(headerRow);
  
  console.log("📊 ヘッダーマップ:");
  Object.entries(headerMap).forEach(([name, idx]) => {
    if (["法人番号", "資本金", "決算月1", "売上1", "利益1"].includes(name)) {
      console.log(`  ${name}: インデックス ${idx}`);
    }
  });
  
  // データ行を処理
  const dataRows = records.slice(1);
  console.log(`\n📊 データ行数: ${dataRows.length} 行\n`);
  
  // 法人番号と財務情報のマップを作成
  const financialDataMap = new Map<string, {
    capitalStock: number | null;
    fiscalMonth: string | null;
    revenue: number | null;
    latestProfit: number | null;
  }>();
  
  let processedCount = 0;
  let skippedNoCorporateNumber = 0;
  let debugCount = 0;
  
  for (const row of dataRows) {
    const financialData = extractFinancialDataFromRow(row, headerMap);
    
    if (!financialData.corporateNumber) {
      skippedNoCorporateNumber++;
      continue;
    }
    
    // 13桁の法人番号のみを対象とする
    if (!/^\d{13}$/.test(financialData.corporateNumber)) {
      skippedNoCorporateNumber++;
      continue;
    }
    
    // 財務情報が1つでもあれば保存
    if (financialData.capitalStock !== null || 
        financialData.fiscalMonth !== null || 
        financialData.revenue !== null || 
        financialData.latestProfit !== null) {
      financialDataMap.set(financialData.corporateNumber, {
        capitalStock: financialData.capitalStock,
        fiscalMonth: financialData.fiscalMonth,
        revenue: financialData.revenue,
        latestProfit: financialData.latestProfit,
      });
      
      // デバッグ: 最初の5件の変換結果を表示
      if (debugCount < 5 && (financialData.capitalStock !== null || financialData.revenue !== null || financialData.latestProfit !== null)) {
        const capitalIdx = headerMap["資本金"];
        const revenueIdx = headerMap["売上1"];
        const profitIdx = headerMap["利益1"];
        console.log(`  🔍 デバッグ [${financialData.corporateNumber}]:`);
        if (financialData.capitalStock !== null) {
          const rawValue = capitalIdx !== undefined ? row[capitalIdx] : "";
          console.log(`    資本金: CSV値="${rawValue}" → 変換後=${financialData.capitalStock}`);
        }
        if (financialData.revenue !== null) {
          const rawValue = revenueIdx !== undefined ? row[revenueIdx] : "";
          console.log(`    売上: CSV値="${rawValue}" → 変換後=${financialData.revenue}`);
        }
        if (financialData.latestProfit !== null) {
          const rawValue = profitIdx !== undefined ? row[profitIdx] : "";
          console.log(`    利益: CSV値="${rawValue}" → 変換後=${financialData.latestProfit}`);
        }
        debugCount++;
      }
    }
    
    processedCount++;
    
    if (processedCount % 100 === 0) {
      console.log(`  進捗: ${processedCount} 行処理済み`);
    }
  }
  
  console.log(`\n📊 CSV処理結果:`);
  console.log(`   ✅ 処理済み: ${processedCount} 行`);
  console.log(`   ⏭️  法人番号なし: ${skippedNoCorporateNumber} 行`);
  console.log(`   💾 財務情報あり: ${financialDataMap.size} 件\n`);
  
  // Firestoreのドキュメントを更新
  console.log("🔄 Firestoreの財務数値を実値に変換中...");
  
  let updatedCount = 0;
  let skippedCount = 0;
  let batchCount = 0;
  let batch: WriteBatch = db.batch();
  
  for (const [corporateNumber, financialData] of financialDataMap.entries()) {
    try {
      // 法人番号でドキュメントを検索
      const docRef = companiesCol.doc(corporateNumber);
      const doc = await docRef.get();
      
      let targetRef: DocumentReference | null = null;
      let currentData: any = null;
      
      if (doc.exists) {
        targetRef = docRef;
        currentData = doc.data();
      } else {
        // 法人番号がdocIdでない場合、corporateNumberフィールドで検索
        const snap = await companiesCol
          .where("corporateNumber", "==", corporateNumber)
          .limit(1)
          .get();
        
        if (!snap.empty) {
          targetRef = snap.docs[0].ref;
          currentData = snap.docs[0].data();
        }
      }
      
      if (!targetRef || !currentData) {
        skippedCount++;
        continue;
      }
      
      const updateData: Record<string, any> = {};
      let hasUpdate = false;
      
      // 資本金を更新（必ずCSVの値を1000倍した値で更新）
      if (financialData.capitalStock !== null) {
        updateData.capitalStock = financialData.capitalStock;
        hasUpdate = true;
      }
      
      // 直近決算年月を更新
      if (financialData.fiscalMonth !== null) {
        const currentFiscalMonth = currentData.fiscalMonth;
        const newFiscalMonth = financialData.fiscalMonth;
        
        if (currentFiscalMonth !== newFiscalMonth) {
          updateData.fiscalMonth = newFiscalMonth;
          hasUpdate = true;
        }
      }
      
      // 直近売上を更新（必ずCSVの値を1000倍した値で更新）
      if (financialData.revenue !== null) {
        updateData.revenue = financialData.revenue;
        hasUpdate = true;
      }
      
      // 直近利益を更新（必ずCSVの値を1000倍した値で更新）
      if (financialData.latestProfit !== null) {
        updateData.latestProfit = financialData.latestProfit;
        hasUpdate = true;
      }
      
      if (hasUpdate) {
        batch.update(targetRef, updateData);
        updatedCount++;
        batchCount++;
        
        if (batchCount >= BATCH_LIMIT) {
          if (!DRY_RUN) {
            await batch.commit();
          }
          console.log(`  💾 バッチコミット (${batchCount} 件) ...`);
          batch = db.batch();
          batchCount = 0;
        }
      } else {
        skippedCount++;
      }
    } catch (error: any) {
      console.error(`  ❌ エラー (corporateNumber: ${corporateNumber}): ${error.message}`);
      skippedCount++;
    }
    
    if ((updatedCount + skippedCount) % 100 === 0) {
      console.log(`  進捗: 更新 ${updatedCount} 件、スキップ ${skippedCount} 件`);
    }
  }
  
  // 残りのバッチをコミット
  if (batchCount > 0) {
    if (!DRY_RUN) {
      await batch.commit();
    }
    console.log(`  💾 最後のバッチコミット (${batchCount} 件) ...`);
  }
  
  console.log(`\n✅ 処理完了`);
  console.log(`   📊 タイプI企業総数: ${financialDataMap.size} 件`);
  console.log(`   ✅ 更新件数: ${updatedCount} 件`);
  console.log(`   ⏭️  スキップ件数: ${skippedCount} 件`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

