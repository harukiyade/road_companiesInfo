/* 
  タイプF・Iのcompanies_newコレクションの情報を更新するスクリプト
  
  タイプF (124.csv, 125.csv, 126.csv):
    - 説明(companyDescription)
    - 概要(overview)
    - 仕入れ先(suppliers)
    - 取引先(clients)
    - 取引先銀行(banks)
    - その他の基本情報
  
  タイプI (132.csv):
    - 決算月1-5 (fiscalMonth1-5)
    - 売上1-5 (revenue1-5)
    - 利益1-5 (profit1-5)
    - 説明(companyDescription)
    - 概要(overview)
    - 仕入れ先(suppliers)
    - 取引先(clients)
    - 取引先銀行(banks)
    - その他の基本情報
  
  AIで分析して、どの情報が入るかを判断して、DBを更新します。
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/update_type_f_i_companies.ts [--dry-run]
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

// タイプFのファイル
const TYPE_F_FILES = ["csv/124.csv", "csv/125.csv", "csv/126.csv"];

// タイプIのファイル
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

// 文字列のトリム処理
function trim(value: string | null | undefined): string {
  if (!value) return "";
  return String(value).trim();
}

// 数値パース（カンマや空白を除去）
function parseNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/[,\s]/g, "");
  if (!cleaned) return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

// 財務数値のパース（千円単位を実値に変換）
function parseFinancialNumeric(
  value: string | null | undefined,
  csvType: string,
  filePath: string,
  field: string
): number | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/[,\s]/g, "");
  if (!cleaned || cleaned === "0" || cleaned === "非上場") return null;
  
  const num = Number(cleaned);
  if (!Number.isFinite(num) || num === 0) return null;
  
  // タイプF・Iの財務情報は千円単位なので1000倍
  return num * 1000;
}

// タイプFのCSVを行配列として読み込む
function loadTypeFCSVByIndex(csvFilePath: string): Array<Array<string>> {
  const buf = fs.readFileSync(csvFilePath);
  try {
    const records: Array<Array<string>> = parse(buf, {
      columns: false,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      skip_records_with_error: true,
    });
    console.log(`  📄 ${path.basename(csvFilePath)}: ${records.length} 行（タイプF）`);
    return records;
  } catch (err: any) {
    console.warn(`  ⚠️ ${path.basename(csvFilePath)}: CSVパースエラー - ${err.code || err.message}`);
    return [];
  }
}

// タイプIのCSVを行配列として読み込む
function loadTypeICSVByIndex(csvFilePath: string): Array<Array<string>> {
  const buf = fs.readFileSync(csvFilePath);
  try {
    const records: Array<Array<string>> = parse(buf, {
      columns: false,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      skip_records_with_error: true,
    });
    console.log(`  📄 ${path.basename(csvFilePath)}: ${records.length} 行（タイプI）`);
    return records;
  } catch (err: any) {
    console.warn(`  ⚠️ ${path.basename(csvFilePath)}: CSVパースエラー - ${err.code || err.message}`);
    return [];
  }
}

// タイプFの行データをマッピング
function mapTypeFRowByIndex(row: Array<string>, filePath: string = ""): Record<string, any> {
  const mapped: Record<string, any> = {};
  let colIndex = 0;
  
  // 0. 会社名
  if (row[colIndex]) mapped.name = trim(row[colIndex]);
  colIndex++;
  
  // 1. 都道府県
  if (row[colIndex]) mapped.prefecture = trim(row[colIndex]);
  colIndex++;
  
  // 2. 代表者名
  if (row[colIndex]) mapped.representativeName = trim(row[colIndex]);
  colIndex++;
  
  // 3-8. 取引種別・SBフラグ・NDA・AD・ステータス・備考（無視）
  colIndex += 6;
  
  // 9. URL
  if (row[colIndex]) mapped.companyUrl = trim(row[colIndex]);
  colIndex++;
  
  // 10. 業種1
  if (row[colIndex]) mapped.industryLarge = trim(row[colIndex]);
  colIndex++;
  
  // 11. 業種2
  if (row[colIndex]) mapped.industryMiddle = trim(row[colIndex]);
  colIndex++;
  
  // 12. 業種3
  if (row[colIndex]) mapped.industrySmall = trim(row[colIndex]);
  colIndex++;
  
  // 13. 郵便番号（業種4-7の可能性もあるが、簡略化）
  if (row[colIndex]) {
    const postalCode = trim(row[colIndex]);
    if (postalCode && /^\d{3}-?\d{4}$/.test(postalCode.replace(/-/g, ""))) {
      mapped.postalCode = postalCode.replace(/(\d{3})(\d{4})/, "$1-$2");
    }
  }
  colIndex++;
  
  // 14. 住所
  if (row[colIndex]) mapped.address = trim(row[colIndex]);
  colIndex++;
  
  // 15. 設立
  if (row[colIndex]) mapped.established = trim(row[colIndex]);
  colIndex++;
  
  // 16. 電話番号(窓口)
  if (row[colIndex]) mapped.phoneNumber = trim(row[colIndex]);
  colIndex++;
  
  // 17. 代表者郵便番号
  if (row[colIndex]) {
    const postalCode = trim(row[colIndex]);
    if (postalCode && /^\d{3}-?\d{4}$/.test(postalCode.replace(/-/g, ""))) {
      mapped.representativePostalCode = postalCode.replace(/(\d{3})(\d{4})/, "$1-$2");
    }
  }
  colIndex++;
  
  // 18. 代表者住所
  if (row[colIndex]) mapped.representativeHomeAddress = trim(row[colIndex]);
  colIndex++;
  
  // 19. 代表者誕生日
  if (row[colIndex]) mapped.representativeBirthDate = trim(row[colIndex]);
  colIndex++;
  
  // 20. 資本金
  if (row[colIndex]) {
    const capital = parseFinancialNumeric(row[colIndex], "type_f", filePath, "capitalStock");
    if (capital !== null) mapped.capitalStock = capital;
  }
  colIndex++;
  
  // 21. 上場
  if (row[colIndex]) mapped.listing = trim(row[colIndex]);
  colIndex++;
  
  // 22. 直近決算年月
  if (row[colIndex]) mapped.fiscalMonth = trim(row[colIndex]);
  colIndex++;
  
  // 23. 直近売上
  if (row[colIndex]) {
    const revenue = parseFinancialNumeric(row[colIndex], "type_f", filePath, "revenue");
    if (revenue !== null) mapped.revenue = revenue;
  }
  colIndex++;
  
  // 24. 直近利益
  if (row[colIndex]) {
    const profit = parseFinancialNumeric(row[colIndex], "type_f", filePath, "latestProfit");
    if (profit !== null) mapped.latestProfit = profit;
  }
  colIndex++;
  
  // 25. 説明
  if (row[colIndex]) mapped.companyDescription = trim(row[colIndex]);
  colIndex++;
  
  // 26. 概要
  if (row[colIndex]) mapped.overview = trim(row[colIndex]);
  colIndex++;
  
  // 27. 仕入れ先
  if (row[colIndex]) mapped.suppliers = trim(row[colIndex]);
  colIndex++;
  
  // 28. 取引先
  if (row[colIndex]) mapped.clients = trim(row[colIndex]);
  colIndex++;
  
  // 29. 取引先銀行
  if (row[colIndex]) mapped.banks = trim(row[colIndex]);
  colIndex++;
  
  // 30. 取締役
  if (row[colIndex]) mapped.executives = trim(row[colIndex]);
  colIndex++;
  
  // 31. 株主
  if (row[colIndex]) mapped.shareholders = trim(row[colIndex]);
  colIndex++;
  
  // 32. 社員数
  if (row[colIndex]) {
    const employeeCount = parseNumeric(row[colIndex]);
    if (employeeCount !== null) mapped.employeeCount = employeeCount;
  }
  colIndex++;
  
  // 33. オフィス数
  if (row[colIndex]) {
    const officeCount = parseNumeric(row[colIndex]);
    if (officeCount !== null) mapped.officeCount = officeCount;
  }
  colIndex++;
  
  // 34. 工場数
  if (row[colIndex]) {
    const factoryCount = parseNumeric(row[colIndex]);
    if (factoryCount !== null) mapped.factoryCount = factoryCount;
  }
  colIndex++;
  
  // 35. 店舗数
  if (row[colIndex]) {
    const storeCount = parseNumeric(row[colIndex]);
    if (storeCount !== null) mapped.storeCount = storeCount;
  }
  colIndex++;
  
  return mapped;
}

// タイプIの行データをマッピング
function mapTypeIRowByIndex(row: Array<string>, filePath: string = ""): Record<string, any> {
  const mapped: Record<string, any> = {};
  let colIndex = 0;
  
  // 0. 会社名
  if (row[colIndex]) mapped.name = trim(row[colIndex]);
  colIndex++;
  
  // 1. 都道府県
  if (row[colIndex]) mapped.prefecture = trim(row[colIndex]);
  colIndex++;
  
  // 2. 代表者名
  if (row[colIndex]) mapped.representativeName = trim(row[colIndex]);
  colIndex++;
  
  // 3. 法人番号
  if (row[colIndex]) mapped.corporateNumber = trim(row[colIndex]);
  colIndex++;
  
  // 4-7. 種別・状態・NDA締結・AD締結（無視）
  colIndex += 4;
  
  // 8. URL
  if (row[colIndex]) mapped.companyUrl = trim(row[colIndex]);
  colIndex++;
  
  // 9. 担当者（無視）
  colIndex++;
  
  // 10. 業種1
  if (row[colIndex]) mapped.industryLarge = trim(row[colIndex]);
  colIndex++;
  
  // 11. 業種2
  if (row[colIndex]) mapped.industryMiddle = trim(row[colIndex]);
  colIndex++;
  
  // 12. 業種3
  if (row[colIndex]) mapped.industrySmall = trim(row[colIndex]);
  colIndex++;
  
  // 13. 住所
  if (row[colIndex]) mapped.address = trim(row[colIndex]);
  colIndex++;
  
  // 14. 設立
  if (row[colIndex]) mapped.established = trim(row[colIndex]);
  colIndex++;
  
  // 15. 電話番号(窓口)
  if (row[colIndex]) mapped.phoneNumber = trim(row[colIndex]);
  colIndex++;
  
  // 16. 郵便番号
  if (row[colIndex]) {
    const postalCode = trim(row[colIndex]);
    if (postalCode && /^\d{3}-?\d{4}$/.test(postalCode.replace(/-/g, ""))) {
      mapped.postalCode = postalCode.replace(/(\d{3})(\d{4})/, "$1-$2");
    }
  }
  colIndex++;
  
  // 17. 代表者誕生日
  if (row[colIndex]) mapped.representativeBirthDate = trim(row[colIndex]);
  colIndex++;
  
  // 18. 資本金
  if (row[colIndex]) {
    const capital = parseFinancialNumeric(row[colIndex], "type_i", filePath, "capitalStock");
    if (capital !== null) mapped.capitalStock = capital;
  }
  colIndex++;
  
  // 19. 上場
  if (row[colIndex]) mapped.listing = trim(row[colIndex]);
  colIndex++;
  
  // 20-34. 決算月1-5, 売上1-5, 利益1-5
  for (let i = 1; i <= 5; i++) {
    // 決算月
    if (row[colIndex]) mapped[`fiscalMonth${i}`] = trim(row[colIndex]);
    colIndex++;
    
    // 売上
    if (row[colIndex]) {
      const revenue = parseFinancialNumeric(row[colIndex], "type_i", filePath, `revenue${i}`);
      if (revenue !== null) mapped[`revenue${i}`] = revenue;
    }
    colIndex++;
    
    // 利益
    if (row[colIndex]) {
      const profit = parseFinancialNumeric(row[colIndex], "type_i", filePath, `profit${i}`);
      if (profit !== null) mapped[`profit${i}`] = profit;
    }
    colIndex++;
  }
  
  // 35. 説明
  if (row[colIndex]) mapped.companyDescription = trim(row[colIndex]);
  colIndex++;
  
  // 36. 概要
  if (row[colIndex]) mapped.overview = trim(row[colIndex]);
  colIndex++;
  
  // 37. 仕入れ先
  if (row[colIndex]) mapped.suppliers = trim(row[colIndex]);
  colIndex++;
  
  // 38. 取引先
  if (row[colIndex]) mapped.clients = trim(row[colIndex]);
  colIndex++;
  
  // 39. 取引先銀行
  if (row[colIndex]) mapped.banks = trim(row[colIndex]);
  colIndex++;
  
  // 40. 取締役
  if (row[colIndex]) mapped.executives = trim(row[colIndex]);
  colIndex++;
  
  // 41. 株主
  if (row[colIndex]) mapped.shareholders = trim(row[colIndex]);
  colIndex++;
  
  // 42. 社員数
  if (row[colIndex]) {
    const employeeCount = parseNumeric(row[colIndex]);
    if (employeeCount !== null) mapped.employeeCount = employeeCount;
  }
  colIndex++;
  
  // 43. オフィス数
  if (row[colIndex]) {
    const officeCount = parseNumeric(row[colIndex]);
    if (officeCount !== null) mapped.officeCount = officeCount;
  }
  colIndex++;
  
  // 44. 工場数
  if (row[colIndex]) {
    const factoryCount = parseNumeric(row[colIndex]);
    if (factoryCount !== null) mapped.factoryCount = factoryCount;
  }
  colIndex++;
  
  // 45. 店舗数
  if (row[colIndex]) {
    const storeCount = parseNumeric(row[colIndex]);
    if (storeCount !== null) mapped.storeCount = storeCount;
  }
  colIndex++;
  
  return mapped;
}

// 企業を検索（法人番号、企業名、住所で検索）
async function findCompany(
  corporateNumber: string | null | undefined,
  companyName: string | null | undefined,
  address: string | null | undefined
): Promise<DocumentReference | null> {
  // 1. 法人番号で検索（docIdとして）
  if (corporateNumber && /^\d{13}$/.test(corporateNumber)) {
    const docRef = companiesCol.doc(corporateNumber);
    const doc = await docRef.get();
    if (doc.exists) {
      return docRef;
    }
    
    // corporateNumberフィールドで検索
    const snap = await companiesCol
      .where("corporateNumber", "==", corporateNumber)
      .limit(1)
      .get();
    
    if (!snap.empty) {
      return snap.docs[0].ref;
    }
  }
  
  // 2. 企業名と住所で検索
  if (companyName && address) {
    const nameNorm = companyName.trim().toLowerCase();
    const addressNorm = address.trim().toLowerCase();
    
    const snap = await companiesCol
      .where("name", "==", companyName)
      .limit(10)
      .get();
    
    for (const doc of snap.docs) {
      const data = doc.data();
      const docAddress = data.address || data.headquartersAddress || "";
      if (docAddress.toLowerCase().includes(addressNorm) || addressNorm.includes(docAddress.toLowerCase())) {
        return doc.ref;
      }
    }
  }
  
  // 3. 企業名のみで検索
  if (companyName) {
    const snap = await companiesCol
      .where("name", "==", companyName)
      .limit(1)
      .get();
    
    if (!snap.empty) {
      return snap.docs[0].ref;
    }
  }
  
  return null;
}

// タイプFの処理
async function processTypeF() {
  console.log("\n📊 タイプFの処理を開始...\n");
  
  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalSkipped = 0;
  
  for (const filePath of TYPE_F_FILES) {
    const resolvedPath = path.resolve(filePath);
    if (!fs.existsSync(resolvedPath)) {
      console.warn(`  ⚠️ ファイルが見つかりません: ${filePath}`);
      continue;
    }
    
    console.log(`\n📄 ${path.basename(filePath)} を処理中...`);
    
    const records = loadTypeFCSVByIndex(resolvedPath);
    if (records.length === 0) {
      console.warn(`  ⚠️ データがありません`);
      continue;
    }
    
    // ヘッダー行をスキップ
    const dataRows = records.slice(1);
    console.log(`  📊 データ行数: ${dataRows.length} 行`);
    
    let batch: WriteBatch = db.batch();
    let batchCount = 0;
    let fileProcessed = 0;
    let fileUpdated = 0;
    let fileSkipped = 0;
    
    for (const row of dataRows) {
      const mapped = mapTypeFRowByIndex(row, resolvedPath);
      
      // 企業を検索
      const corporateNumber = mapped.corporateNumber || null;
      const companyName = mapped.name || null;
      const address = mapped.address || null;
      
      const docRef = await findCompany(corporateNumber, companyName, address);
      
      if (!docRef) {
        fileSkipped++;
        totalSkipped++;
        continue;
      }
      
      // 更新データを準備（nullや空文字列は除外）
      const updateData: Record<string, any> = {};
      
      for (const [key, value] of Object.entries(mapped)) {
        if (value !== null && value !== undefined && value !== "") {
          updateData[key] = value;
        }
      }
      
      if (Object.keys(updateData).length > 0) {
        batch.update(docRef, updateData);
        batchCount++;
        fileUpdated++;
        totalUpdated++;
        
        if (batchCount >= BATCH_LIMIT) {
          if (!DRY_RUN) {
            await batch.commit();
          }
          console.log(`  💾 バッチコミット (${batchCount} 件)`);
          batch = db.batch();
          batchCount = 0;
        }
      } else {
        fileSkipped++;
        totalSkipped++;
      }
      
      fileProcessed++;
      totalProcessed++;
      
      if (fileProcessed % 100 === 0) {
        console.log(`  進捗: ${fileProcessed} 行処理済み (更新: ${fileUpdated}, スキップ: ${fileSkipped})`);
      }
    }
    
    // 残りのバッチをコミット
    if (batchCount > 0) {
      if (!DRY_RUN) {
        await batch.commit();
      }
      console.log(`  💾 最後のバッチコミット (${batchCount} 件)`);
    }
    
    console.log(`  ✅ ${path.basename(filePath)}: 処理済み ${fileProcessed} 行, 更新 ${fileUpdated} 件, スキップ ${fileSkipped} 件`);
  }
  
  console.log(`\n📊 タイプF処理結果:`);
  console.log(`   ✅ 処理済み: ${totalProcessed} 行`);
  console.log(`   ✅ 更新: ${totalUpdated} 件`);
  console.log(`   ⏭️  スキップ: ${totalSkipped} 件`);
}

// タイプIの処理
async function processTypeI() {
  console.log("\n📊 タイプIの処理を開始...\n");
  
  const resolvedPath = path.resolve(TYPE_I_FILE);
  if (!fs.existsSync(resolvedPath)) {
    console.error(`❌ ファイルが見つかりません: ${TYPE_I_FILE}`);
    return;
  }
  
  console.log(`📄 ${path.basename(TYPE_I_FILE)} を処理中...`);
  
  const records = loadTypeICSVByIndex(resolvedPath);
  if (records.length === 0) {
    console.error(`❌ データがありません`);
    return;
  }
  
  // ヘッダー行をスキップ
  const dataRows = records.slice(1);
  console.log(`  📊 データ行数: ${dataRows.length} 行`);
  
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  let processed = 0;
  let updated = 0;
  let skipped = 0;
  
  for (const row of dataRows) {
    const mapped = mapTypeIRowByIndex(row, resolvedPath);
    
    // 企業を検索
    const corporateNumber = mapped.corporateNumber || null;
    const companyName = mapped.name || null;
    const address = mapped.address || null;
    
    const docRef = await findCompany(corporateNumber, companyName, address);
    
    if (!docRef) {
      skipped++;
      continue;
    }
    
    // 更新データを準備（nullや空文字列は除外）
    const updateData: Record<string, any> = {};
    
    for (const [key, value] of Object.entries(mapped)) {
      if (value !== null && value !== undefined && value !== "") {
        updateData[key] = value;
      }
    }
    
    if (Object.keys(updateData).length > 0) {
      batch.update(docRef, updateData);
      batchCount++;
      updated++;
      
      if (batchCount >= BATCH_LIMIT) {
        if (!DRY_RUN) {
          await batch.commit();
        }
        console.log(`  💾 バッチコミット (${batchCount} 件)`);
        batch = db.batch();
        batchCount = 0;
      }
    } else {
      skipped++;
    }
    
    processed++;
    
    if (processed % 100 === 0) {
      console.log(`  進捗: ${processed} 行処理済み (更新: ${updated}, スキップ: ${skipped})`);
    }
  }
  
  // 残りのバッチをコミット
  if (batchCount > 0) {
    if (!DRY_RUN) {
      await batch.commit();
    }
    console.log(`  💾 最後のバッチコミット (${batchCount} 件)`);
  }
  
  console.log(`\n📊 タイプI処理結果:`);
  console.log(`   ✅ 処理済み: ${processed} 行`);
  console.log(`   ✅ 更新: ${updated} 件`);
  console.log(`   ⏭️  スキップ: ${skipped} 件`);
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード: Firestore は書き換えません\n" : "⚠️  本番モード: Firestore を書き換えます\n");
  
  await processTypeF();
  await processTypeI();
  
  console.log("\n✅ 処理完了");
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

