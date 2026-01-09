/* 
  csv/add_20251224配下の全CSVファイルをインポートするスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/import_all_csv.ts
  
  DRY_RUNモード（実際には書き込まない）:
    DRY_RUN=1 npx ts-node scripts/import_all_csv.ts
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
const BATCH_SIZE = 400; // Firestoreのバッチ制限（500未満）

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
// ヘルパー関数（import_test_5_records.tsからコピー）
// ==============================

function isEmptyValue(v: any): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  return false;
}

function isNumericString(value: string): boolean {
  return /^\d+$/.test(value);
}

// ドキュメントIDを数値の文字列で新規生成（法人番号は使わない）
function generateNewNumericDocId(index: number): string {
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 企業IDを数値で生成（companyIdフィールド用）
function generateNewCompanyId(index: number): number {
  const timestamp = Date.now();
  return timestamp * 1000000 + index;
}

function convertFromThousandYen(value: string | null | undefined): number | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim().replace(/[,，]/g, "");
  const num = Number(cleaned);
  if (isNaN(num)) return null;
  return Math.round(num * 1000);
}

function toNumber(value: string | null | undefined): number | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim().replace(/[,，]/g, "");
  const num = Number(cleaned);
  if (isNaN(num)) return null;
  return Math.round(num);
}

function normalizeCorporateNumber(value: string | null | undefined): string | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim().replace(/\D/g, "");
  if (cleaned.length === 13 && isNumericString(cleaned)) {
    return cleaned;
  }
  return null;
}

function toArray(value: string | null | undefined): string[] | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim();
  if (!cleaned) return null;
  return cleaned.split(/[，,]/).map(s => s.trim()).filter(s => s.length > 0);
}

function normalizeListing(value: string | null | undefined): string | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim();
  if (cleaned === "非上場" || cleaned === "上場") {
    return cleaned === "上場" ? "上場" : "非上場";
  }
  return cleaned || null;
}

function normalizeDate(value: string | null | undefined): string | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim();
  
  if (/^\d{4}-\d{2}-\d{2}$/.test(cleaned)) {
    return cleaned;
  }
  
  const match = cleaned.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/);
  if (match) {
    const year = match[1];
    const month = String(parseInt(match[2])).padStart(2, "0");
    const day = String(parseInt(match[3])).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }
  
  const match2 = cleaned.match(/(\d{4})年(\d{1,2})月1日/);
  if (match2) {
    const year = match2[1];
    const month = String(parseInt(match2[2])).padStart(2, "0");
    return `${year}-${month}-01`;
  }
  
  return cleaned || null;
}

// ==============================
// companies_new コレクション全フィールドテンプレート
// ==============================
const COMPANY_TEMPLATE: Record<string, any> = {
  companyId: null, name: null, nameEn: null, kana: null, corporateNumber: null,
  corporationType: null, nikkeiCode: null, badges: null, tags: null,
  createdAt: null, updatedAt: null, updateDate: null, updateCount: null,
  changeCount: null, qualificationGrade: null,
  prefecture: null, address: null, headquartersAddress: null, postalCode: null,
  location: null, departmentLocation: null,
  phoneNumber: null, contactPhoneNumber: null, fax: null, email: null,
  companyUrl: null, contactFormUrl: null,
  representativeName: null, representativeKana: null, representativeTitle: null,
  representativeBirthDate: null, representativePhone: null,
  representativePostalCode: null, representativeHomeAddress: null,
  representativeRegisteredAddress: null, representativeAlmaMater: null, executives: null,
  executiveName1: null, executiveName2: null, executiveName3: null, executiveName4: null,
  executiveName5: null, executiveName6: null, executiveName7: null, executiveName8: null,
  executiveName9: null, executiveName10: null,
  executivePosition1: null, executivePosition2: null, executivePosition3: null,
  executivePosition4: null, executivePosition5: null, executivePosition6: null,
  executivePosition7: null, executivePosition8: null, executivePosition9: null,
  executivePosition10: null,
  industry: null, industryLarge: null, industryMiddle: null, industrySmall: null,
  industryDetail: null, industries: [], industryCategories: null,
  businessDescriptions: null, businessItems: null, businessSummary: null,
  specialties: null, demandProducts: null, specialNote: null,
  capitalStock: null, revenue: null, latestRevenue: null, latestProfit: null,
  revenueFromStatements: null, operatingIncome: null, totalAssets: null,
  totalLiabilities: null, netAssets: null, issuedShares: null, financials: null,
  listing: null, marketSegment: null, latestFiscalYearMonth: null,
  fiscalMonth: null, fiscalMonth1: null, fiscalMonth2: null, fiscalMonth3: null,
  fiscalMonth4: null, fiscalMonth5: null,
  revenue1: null, revenue2: null, revenue3: null, revenue4: null, revenue5: null,
  profit1: null, profit2: null, profit3: null, profit4: null, profit5: null,
  employeeCount: null, employeeNumber: null, factoryCount: null,
  officeCount: null, storeCount: null, averageAge: null,
  averageYearsOfService: null, averageOvertimeHours: null,
  averagePaidLeave: null, femaleExecutiveRatio: null,
  established: null, dateOfEstablishment: null, founding: null,
  foundingYear: null, acquisition: null,
  clients: null, suppliers: null, subsidiaries: null, affiliations: null,
  shareholders: null, banks: null, bankCorporateNumber: null,
  departmentName1: null, departmentName2: null, departmentName3: null,
  departmentName4: null, departmentName5: null, departmentName6: null,
  departmentName7: null,
  departmentAddress1: null, departmentAddress2: null, departmentAddress3: null,
  departmentAddress4: null, departmentAddress5: null, departmentAddress6: null,
  departmentAddress7: null,
  departmentPhone1: null, departmentPhone2: null, departmentPhone3: null,
  departmentPhone4: null, departmentPhone5: null, departmentPhone6: null,
  departmentPhone7: null,
  overview: null, companyDescription: null, salesNotes: null,
  urls: [], profileUrl: null, externalDetailUrl: null, facebook: null,
  linkedin: null, wantedly: null, youtrust: null, metaKeywords: null,
};

function mapCsvRowToCompanyData(row: Record<string, string>, index: number): Record<string, any> {
  const data: Record<string, any> = JSON.parse(JSON.stringify(COMPANY_TEMPLATE));

  if (!isEmptyValue(row["会社名"])) data.name = String(row["会社名"]).trim();
  if (!isEmptyValue(row["都道府県"])) data.prefecture = String(row["都道府県"]).trim();
  if (!isEmptyValue(row["代表者名"])) data.representativeName = String(row["代表者名"]).trim();

  const corporateNumber = normalizeCorporateNumber(row["法人番号"]);
  if (corporateNumber) data.corporateNumber = corporateNumber;

  if (!isEmptyValue(row["URL"])) data.companyUrl = String(row["URL"]).trim();

  if (!isEmptyValue(row["業種1"])) {
    data.industryLarge = String(row["業種1"]).trim();
    data.industry = String(row["業種1"]).trim();
  }
  if (!isEmptyValue(row["業種2"])) data.industryMiddle = String(row["業種2"]).trim();
  if (!isEmptyValue(row["業種3"])) data.industrySmall = String(row["業種3"]).trim();

  if (!isEmptyValue(row["郵便番号"])) {
    const postalCode = String(row["郵便番号"]).trim().replace(/\D/g, "");
    if (postalCode.length === 7) {
      data.postalCode = postalCode.replace(/(\d{3})(\d{4})/, "$1-$2");
    }
  }

  if (!isEmptyValue(row["住所"])) {
    data.address = String(row["住所"]).trim();
    data.headquartersAddress = String(row["住所"]).trim();
  }

  const established = normalizeDate(row["設立"]);
  if (established) {
    data.established = established;
    data.dateOfEstablishment = established;
  }

  if (!isEmptyValue(row["電話番号(窓口)"])) {
    data.phoneNumber = String(row["電話番号(窓口)"]).trim();
    data.contactPhoneNumber = String(row["電話番号(窓口)"]).trim();
  }

  if (!isEmptyValue(row["代表者郵便番号"])) {
    const repPostalCode = String(row["代表者郵便番号"]).trim().replace(/\D/g, "");
    if (repPostalCode.length === 7) {
      data.representativePostalCode = repPostalCode.replace(/(\d{3})(\d{4})/, "$1-$2");
    }
  }

  if (!isEmptyValue(row["代表者住所"])) {
    data.representativeHomeAddress = String(row["代表者住所"]).trim();
  }

  const repBirthDate = normalizeDate(row["代表者誕生日"]);
  if (repBirthDate) data.representativeBirthDate = repBirthDate;

  const capitalStock = convertFromThousandYen(row["資本金"]);
  if (capitalStock !== null) data.capitalStock = capitalStock;

  const listing = normalizeListing(row["上場"]);
  if (listing) data.listing = listing;

  const latestFiscalYearMonth = normalizeDate(row["直近決算年月"]);
  if (latestFiscalYearMonth) data.latestFiscalYearMonth = latestFiscalYearMonth;

  const latestRevenue = convertFromThousandYen(row["直近売上"]);
  if (latestRevenue !== null) data.latestRevenue = latestRevenue;

  const latestProfit = convertFromThousandYen(row["直近利益"]);
  if (latestProfit !== null) data.latestProfit = latestProfit;

  if (!isEmptyValue(row["説明"])) data.companyDescription = String(row["説明"]).trim();
  if (!isEmptyValue(row["概要"])) data.overview = String(row["概要"]).trim();

  const suppliers = toArray(row["仕入れ先"]);
  data.suppliers = suppliers && suppliers.length > 0 ? suppliers : [];

  const clients = toArray(row["取引先"]);
  data.clients = clients && clients.length > 0 ? clients : [];

  const banks = toArray(row["取引先銀行"]);
  data.banks = banks && banks.length > 0 ? banks : [];

  const executives = toArray(row["取締役"]);
  data.executives = executives && executives.length > 0 ? executives : [];

  const shareholders = toArray(row["株主"]);
  data.shareholders = shareholders && shareholders.length > 0 ? shareholders : [];

  const employeeCount = toNumber(row["社員数"]);
  if (employeeCount !== null) data.employeeCount = employeeCount;

  const officeCount = toNumber(row["オフィス数"]);
  if (officeCount !== null) data.officeCount = officeCount;

  const factoryCount = toNumber(row["工場数"]);
  if (factoryCount !== null) data.factoryCount = factoryCount;

  const storeCount = toNumber(row["店舗数"]);
  if (storeCount !== null) data.storeCount = storeCount;

  const industries: string[] = [];
  if (!isEmptyValue(row["業種1"])) industries.push(String(row["業種1"]).trim());
  if (!isEmptyValue(row["業種2"])) industries.push(String(row["業種2"]).trim());
  if (!isEmptyValue(row["業種3"])) industries.push(String(row["業種3"]).trim());
  data.industries = industries.length > 0 ? industries : [];

  const now = admin.firestore.Timestamp.now();
  data.createdAt = now;
  data.updatedAt = now;
  data.updateDate = now.toDate().toISOString().split("T")[0];

  if (!data.urls) data.urls = [];
  if (!data.clients) data.clients = [];
  if (!data.banks) data.banks = [];
  if (!data.executives) data.executives = [];

  return data;
}

// ==============================
// メイン処理
// ==============================

function normalizeStringForDuplicate(s: string | null | undefined): string {
  if (!s) return "";
  return String(s).trim().replace(/\s+/g, "");
}

function normalizeCorporateNumberForDuplicate(value: string | null | undefined): string | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/\D/g, "");
  if (cleaned.length === 13) return cleaned;
  return null;
}

async function checkDuplicate(
  name: string | null,
  address: string | null,
  corporateNumber: string | null
): Promise<boolean> {
  if (!companiesCol || !name || !address) return false;

  const normalizedName = normalizeStringForDuplicate(name);
  const normalizedAddress = normalizeStringForDuplicate(address);
  const normalizedCorpNum = normalizeCorporateNumberForDuplicate(corporateNumber);

  if (!normalizedName || !normalizedAddress) return false;

  if (normalizedCorpNum) {
    const snapByCorp = await companiesCol
      .where("corporateNumber", "==", normalizedCorpNum)
      .limit(1)
      .get();
    if (!snapByCorp.empty) return true;
  }

  const snapByName = await companiesCol
    .where("name", "==", name.trim())
    .limit(100)
    .get();

  for (const doc of snapByName.docs) {
    const data = doc.data();
    const docAddress = normalizeStringForDuplicate(data.address || data.headquartersAddress);
    if (docAddress === normalizedAddress) return true;
  }

  return false;
}

async function main() {
  if (DRY_RUN) {
    console.log("🔍 DRY_RUN モード: Firestore には書き込みません\n");
  }

  console.log(`📄 CSVディレクトリ: ${CSV_DIR}\n`);

  if (!fs.existsSync(CSV_DIR)) {
    console.error(`❌ エラー: CSVディレクトリが見つかりません: ${CSV_DIR}`);
    process.exit(1);
  }

  const files = fs.readdirSync(CSV_DIR).filter(f => f.endsWith(".csv")).sort();
  console.log(`📊 CSVファイル数: ${files.length} ファイル\n`);

  let allRecords: Array<Record<string, string>> = [];

  for (const file of files) {
    const filePath = path.join(CSV_DIR, file);
    try {
      const csvContent = fs.readFileSync(filePath, "utf8");
      const records = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
      }) as Record<string, string>[];
      
      console.log(`  ✅ ${file}: ${records.length.toLocaleString()} 件`);
      allRecords.push(...records);
    } catch (err: any) {
      console.error(`  ❌ ${file}: 読み込みエラー - ${err.message}`);
    }
  }

  console.log(`\n📊 合計: ${allRecords.length.toLocaleString()} 件のレコード\n`);

  if (allRecords.length === 0) {
    console.log("⚠️  インポートするデータがありません");
    return;
  }

  let importedCount = 0;
  let skippedCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const baseTimestamp = Date.now();

  console.log("🔄 インポート処理を開始します...\n");

  for (let i = 0; i < allRecords.length; i++) {
    const row = allRecords[i];
    const companyData = mapCsvRowToCompanyData(row, i + 1);

    // 重複チェック
    if (!DRY_RUN) {
      const isDuplicate = await checkDuplicate(
        companyData.name,
        companyData.address || companyData.headquartersAddress,
        companyData.corporateNumber
      );

      if (isDuplicate) {
        skippedCount++;
        if (skippedCount <= 10 || skippedCount % 1000 === 0) {
          console.log(`⏭️  [${i + 1}/${allRecords.length}] スキップ（重複）: ${companyData.name || "(未設定)"}`);
        }
        continue;
      }
    }

    // ドキュメントIDを生成（ユニークなID）
    const docId = `${baseTimestamp}${String(i + 1).padStart(10, "0")}`;
    const companyId = baseTimestamp * 10000000 + (i + 1);
    companyData.companyId = companyId;

    if (!DRY_RUN) {
      const docRef = companiesCol.doc(docId);
      batch.set(docRef, companyData, { merge: false });
      batchCount++;
      importedCount++;

      if (batchCount >= BATCH_SIZE) {
        await batch.commit();
        console.log(`💾 バッチコミット: ${batchCount} 件 (累計: ${importedCount.toLocaleString()} 件インポート, ${skippedCount.toLocaleString()} 件スキップ)`);
        batch = db.batch();
        batchCount = 0;
      }
    } else {
      importedCount++;
      if (importedCount <= 5 || importedCount % 1000 === 0) {
        console.log(`✅ [${i + 1}/${allRecords.length}] ${companyData.name || "(未設定)"}`);
      }
    }

    if ((i + 1) % 1000 === 0) {
      console.log(`  📦 処理中: ${(i + 1).toLocaleString()}/${allRecords.length.toLocaleString()} 件 (インポート: ${importedCount.toLocaleString()}, スキップ: ${skippedCount.toLocaleString()})`);
    }
  }

  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    console.log(`💾 最後のバッチコミット: ${batchCount} 件`);
  }

  console.log("\n✅ インポート完了！");
  console.log(`  📊 総レコード数: ${allRecords.length.toLocaleString()} 件`);
  console.log(`  ✅ インポート: ${importedCount.toLocaleString()} 件`);
  console.log(`  ⏭️  スキップ: ${skippedCount.toLocaleString()} 件`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
