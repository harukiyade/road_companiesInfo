/* 
  タイプH・I・JのCSVファイルを読み込んで、既存ドキュメントを更新するスクリプト
  
  企業名・住所・代表者名（なくても良い）・法人番号（なくても良い）で企業を特定し、更新します。
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type {
  Firestore,
  CollectionReference,
  DocumentReference,
  DocumentData,
} from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// タイプ別のCSVファイル
const TYPE_CSV_FILES: Record<string, string> = {
  H: "csv/130.csv",
  I: "csv/132.csv",
  J: "csv/133.csv",
};

// Firebase初期化
if (!admin.apps.length) {
  const serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || 
    path.join(__dirname, "../albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
  
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccountPath)
  });
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// キャッシュ
const cacheByCorporateNumber = new Map<string, DocumentReference | null>();
const cacheByNameAndAddress = new Map<string, DocumentReference | null>();
const cacheByName = new Map<string, DocumentReference | null>();

/**
 * 既存のフィールド構造に準拠した空のテンプレート
 */
function getEmptyTemplate(): Record<string, any> {
  return {
    acquisition: null,
    adExpiration: null,
    address: null,
    affiliations: null,
    averageAge: null,
    averageOvertimeHours: null,
    averagePaidLeave: null,
    averageYearsOfService: null,
    badges: [],
    bankCorporateNumber: null,
    banks: [],
    businessDescriptions: null,
    businessItems: [],
    businessSummary: null,
    capitalStock: null,
    changeCount: null,
    clients: null,
    companyDescription: null,
    companyUrl: null,
    contactFormUrl: null,
    contactPhoneNumber: null,
    corporateNumber: null,
    corporationType: null,
    createdAt: null,
    dateOfEstablishment: null,
    demandProducts: null,
    departmentName1: null,
    departmentAddress1: null,
    departmentPhone1: null,
    departmentName2: null,
    departmentAddress2: null,
    departmentPhone2: null,
    departmentName3: null,
    departmentAddress3: null,
    departmentPhone3: null,
    departmentName4: null,
    departmentAddress4: null,
    departmentPhone4: null,
    departmentName5: null,
    departmentAddress5: null,
    departmentPhone5: null,
    departmentName6: null,
    departmentAddress6: null,
    departmentPhone6: null,
    departmentName7: null,
    departmentAddress7: null,
    departmentPhone7: null,
    departmentLocation: null,
    email: null,
    employeeCount: null,
    employeeNumber: null,
    established: null,
    executives: null,
    executiveName1: null,
    executivePosition1: null,
    executiveName2: null,
    executivePosition2: null,
    executiveName3: null,
    executivePosition3: null,
    executiveName4: null,
    executivePosition4: null,
    executiveName5: null,
    executivePosition5: null,
    executiveName6: null,
    executivePosition6: null,
    executiveName7: null,
    executivePosition7: null,
    executiveName8: null,
    executivePosition8: null,
    executiveName9: null,
    executivePosition9: null,
    executiveName10: null,
    executivePosition10: null,
    externalDetailUrl: null,
    facebook: null,
    factoryCount: null,
    fax: null,
    femaleExecutiveRatio: null,
    financials: null,
    fiscalMonth: null,
    fiscalMonth1: null,
    fiscalMonth2: null,
    fiscalMonth3: null,
    fiscalMonth4: null,
    fiscalMonth5: null,
    founding: null,
    foundingYear: null,
    headquartersAddress: null,
    industries: [],
    industry: null,
    industryCategories: null,
    industryDetail: null,
    industryLarge: null,
    industryMiddle: null,
    industrySmall: null,
    issuedShares: null,
    kana: null,
    latestFiscalYearMonth: null,
    latestProfit: null,
    latestRevenue: null,
    linkedin: null,
    listing: null,
    location: null,
    marketSegment: null,
    metaDescription: null,
    metaKeywords: null,
    name: null,
    nameEn: null,
    nikkeiCode: null,
    numberOfActivity: null,
    officeCount: null,
    operatingIncome: null,
    overview: null,
    phoneNumber: null,
    postalCode: null,
    prefecture: null,
    profileUrl: null,
    profit1: null,
    profit2: null,
    profit3: null,
    profit4: null,
    profit5: null,
    qualificationGrade: null,
    registrant: null,
    representativeAlmaMater: null,
    representativeBirthDate: null,
    representativeHomeAddress: null,
    representativeKana: null,
    representativeName: null,
    representativePhone: null,
    representativePostalCode: null,
    representativeRegisteredAddress: null,
    representativeTitle: null,
    revenue: null,
    revenueFromStatements: null,
    revenue1: null,
    revenue2: null,
    revenue3: null,
    revenue4: null,
    revenue5: null,
    salesNotes: null,
    shareholders: null,
    specialNote: null,
    specialties: null,
    storeCount: null,
    subsidiaries: [],
    suppliers: [],
    tags: [],
    totalAssets: null,
    totalLiabilities: null,
    tradingStatus: null,
    transportation: null,
    updateCount: null,
    updateDate: null,
    updatedAt: null,
    urls: [],
    wantedly: null,
    youtrust: null,
  };
}

function normalizeCompanyName(name: string): string {
  return name.trim().replace(/\s+/g, "");
}

function normalizeAddress(addr: string): string {
  return addr.trim().replace(/\s+/g, "");
}

// Firestore 上で既存ドキュメント検索
// 優先順位: 1. 法人番号 → 2. 企業名 + 住所 → 3. 企業名のみ
async function findExistingCompanyDoc(
  corporateNumber: string | null,
  companyName: string | null,
  headquartersAddress: string | null,
  address: string | null
): Promise<{
  ref: DocumentReference<DocumentData>;
  matchedBy: "corporateNumber" | "nameAndHeadquartersAddress" | "nameAndAddress" | "companyName";
} | null> {
  // 1. 法人番号で検索（最優先）
  if (corporateNumber && corporateNumber.trim()) {
    const normalizedCorpNum = corporateNumber.trim();

    // キャッシュ確認
    const cachedByCorp = cacheByCorporateNumber.get(normalizedCorpNum);
    if (cachedByCorp !== undefined) {
      if (cachedByCorp) {
        return { ref: cachedByCorp, matchedBy: "corporateNumber" };
      }
      return null;
    }

    // まずは docId=法人番号 で直接参照（新スキーマではこれが最速）
    const directRef = companiesCol.doc(normalizedCorpNum);
    const directSnap = await directRef.get();
    if (directSnap.exists) {
      cacheByCorporateNumber.set(normalizedCorpNum, directRef);
      return { ref: directRef, matchedBy: "corporateNumber" };
    }

    // 念のため、corporateNumber フィールドでの検索もフォールバックとして試す
    const snapByCorp = await companiesCol
      .where("corporateNumber", "==", normalizedCorpNum)
      .limit(1)
      .get();
    if (!snapByCorp.empty) {
      const ref = snapByCorp.docs[0].ref;
      cacheByCorporateNumber.set(normalizedCorpNum, ref);
      return { ref, matchedBy: "corporateNumber" };
    }

    // 見つからなかった結果もキャッシュ
    cacheByCorporateNumber.set(normalizedCorpNum, null);
  }

  // 2. 企業名 + 住所（headquartersAddress / address）の組み合わせ
  if (companyName && companyName.trim() && (headquartersAddress || address)) {
    const nameTrimmed = companyName.trim();
    const nameNorm = normalizeCompanyName(nameTrimmed);

    const candidates: { field: "headquartersAddress" | "address"; value: string }[] = [];
    if (headquartersAddress && headquartersAddress.trim()) {
      candidates.push({ field: "headquartersAddress", value: headquartersAddress.trim() });
    }
    if (address && address.trim()) {
      // headquartersAddress と同じ文字列なら重複させない
      if (!headquartersAddress || headquartersAddress.trim() !== address.trim()) {
        candidates.push({ field: "address", value: address.trim() });
      }
    }

    for (const cand of candidates) {
      const addrNorm = normalizeAddress(cand.value);
      const cacheKey = `${nameNorm}|${cand.field}:${addrNorm}`;
      const cached = cacheByNameAndAddress.get(cacheKey);
      if (cached !== undefined) {
        if (cached) {
          const matchedBy =
            cand.field === "headquartersAddress"
              ? "nameAndHeadquartersAddress"
              : "nameAndAddress";
          return { ref: cached, matchedBy };
        }
        // 見つからなかったキャッシュ → 次の候補へ
        continue;
      }

      const snap = await companiesCol
        .where("name", "==", nameTrimmed)
        .where(cand.field, "==", cand.value)
        .limit(1)
        .get();

      if (!snap.empty) {
        const ref = snap.docs[0].ref;
        cacheByNameAndAddress.set(cacheKey, ref);
        const matchedBy =
          cand.field === "headquartersAddress"
            ? "nameAndHeadquartersAddress"
            : "nameAndAddress";
        return { ref, matchedBy };
      }

      cacheByNameAndAddress.set(cacheKey, null);
    }
  }

  // 3. 企業名のみで検索（法人番号・住所が使えない場合のフォールバック）
  if (companyName && companyName.trim()) {
    const normalizedName = normalizeCompanyName(companyName);

    // キャッシュ確認
    const cachedByName = cacheByName.get(normalizedName);
    if (cachedByName !== undefined) {
      if (cachedByName) {
        return { ref: cachedByName, matchedBy: "companyName" };
      }
      return null;
    }

    // 新しいスキーマ: name フィールドで検索
    let snapByName = await companiesCol
      .where("name", "==", companyName.trim())
      .limit(1)
      .get();
    if (!snapByName.empty) {
      const ref = snapByName.docs[0].ref;
      cacheByName.set(normalizedName, ref);
      return { ref, matchedBy: "companyName" };
    }

    // 旧スキーマ: companyName フィールドでの検索もフォールバックで試す
    snapByName = await companiesCol
      .where("companyName", "==", companyName.trim())
      .limit(1)
      .get();
    if (!snapByName.empty) {
      const ref = snapByName.docs[0].ref;
      cacheByName.set(normalizedName, ref);
      return { ref, matchedBy: "companyName" };
    }

    // 見つからなかった結果もキャッシュ
    cacheByName.set(normalizedName, null);
  }

  return null;
}

// ドキュメントIDを生成
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

/**
 * タイプH（130.csv）のデータをマッピング
 */
function mapTypeH(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["name"] || null;
  data.corporateNumber = row["corporateNumber"] || null;
  data.representativeName = row["representativeName"] || null;
  
  // 財務情報
  if (row["revenue"] !== undefined && row["revenue"] !== null && String(row["revenue"]).trim() !== "") {
    const revenueStr = String(row["revenue"]).replace(/,/g, "").trim();
    const revenueValue = parseFloat(revenueStr);
    if (!isNaN(revenueValue)) {
      data.revenue = revenueValue;
    }
  }
  if (row["capitalStock"] !== undefined && row["capitalStock"] !== null && String(row["capitalStock"]).trim() !== "") {
    const capitalStr = String(row["capitalStock"]).replace(/,/g, "").trim();
    const capitalValue = parseFloat(capitalStr);
    if (!isNaN(capitalValue)) {
      data.capitalStock = capitalValue;
    }
  }
  
  data.listing = row["listing"] || null;
  data.address = row["address"] || null;
  data.headquartersAddress = row["address"] || null;
  
  if (row["employeeCount"] !== undefined && row["employeeCount"] !== null && String(row["employeeCount"]).trim() !== "") {
    const employeeStr = String(row["employeeCount"]).replace(/,/g, "").trim();
    const employeeValue = parseInt(employeeStr);
    if (!isNaN(employeeValue)) {
      data.employeeCount = employeeValue;
    }
  }
  
  data.established = row["established"] || null;
  data.fiscalMonth = row["fiscalMonth"] || null;
  data.industryLarge = row["industryLarge"] || null;
  data.industryMiddle = row["industryMiddle"] || null;
  data.industrySmall = row["industrySmall"] || null;
  data.industryDetail = row["industryDetail"] || null;
  data.industry = row["industryLarge"] || null; // industryLargeをindustryにも設定
  data.phoneNumber = row["phoneNumber"] || null;
  data.companyUrl = row["companyUrl"] || null;
  
  // 部署情報（1〜7）
  for (let i = 1; i <= 7; i++) {
    if (row[`departmentName${i}`]) {
      (data as any)[`departmentName${i}`] = row[`departmentName${i}`] || null;
    }
    if (row[`departmentAddress${i}`]) {
      (data as any)[`departmentAddress${i}`] = row[`departmentAddress${i}`] || null;
    }
    if (row[`departmentPhone${i}`]) {
      (data as any)[`departmentPhone${i}`] = row[`departmentPhone${i}`] || null;
    }
  }
  
  // 役員情報（1〜10）
  for (let i = 1; i <= 10; i++) {
    if (row[`executiveName${i}`]) {
      (data as any)[`executiveName${i}`] = row[`executiveName${i}`] || null;
    }
    if (row[`executivePosition${i}`]) {
      (data as any)[`executivePosition${i}`] = row[`executivePosition${i}`] || null;
    }
  }
  
  // 役員情報を文字列としても保存
  const executivesArr: string[] = [];
  for (let i = 1; i <= 10; i++) {
    const name = row[`executiveName${i}`];
    const position = row[`executivePosition${i}`];
    if (name || position) {
      executivesArr.push(`${position || ""}${name || ""}`.trim());
    }
  }
  if (executivesArr.length > 0) {
    data.executives = executivesArr.join("，");
  }
  
  return data;
}

/**
 * タイプI（132.csv）のデータをマッピング
 */
function mapTypeI(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.prefecture = row["都道府県"] || null;
  data.representativeName = row["代表者名"] || null;
  data.corporateNumber = row["法人番号"] || null;
  // URLが空の場合は「説明」フィールドから取得
  data.companyUrl = row["URL"] || row["説明"] || null;
  data.industry = row["業種1"] || null;
  data.postalCode = row["郵便番号"] || null;
  data.address = row["住所"] || null;
  data.headquartersAddress = row["住所"] || null;
  data.established = row["設立"] || null;
  data.phoneNumber = row["電話番号(窓口)"] || null;
  data.representativeRegisteredAddress = row["代表者郵便番号"] || null;
  data.representativeHomeAddress = row["代表者住所"] || null;
  data.representativeBirthDate = row["代表者誕生日"] || null;
  // 説明フィールドがURLでない場合は、companyDescriptionとして保存
  if (row["説明"] && !row["説明"].startsWith("http")) {
    data.companyDescription = row["説明"] || null;
  }
  // 概要フィールド
  data.overview = row["概要"] || null;
  // 取締役情報
  if (row["取締役"]) {
    data.executives = row["取締役"] || null;
  }
  // 上場情報
  data.listing = row["上場"] || null;
  // 資本金（0も有効な値として扱う）
  if (row["資本金"] !== undefined && row["資本金"] !== null && String(row["資本金"]).trim() !== "") {
    const capitalStr = String(row["資本金"]).replace(/,/g, "").trim();
    const capitalValue = parseFloat(capitalStr);
    if (!isNaN(capitalValue)) {
      data.capitalStock = capitalValue;
    }
  }
  // 従業員数（0も有効な値として扱う）
  if (row["社員数"] !== undefined && row["社員数"] !== null && String(row["社員数"]).trim() !== "") {
    const employeeStr = String(row["社員数"]).replace(/,/g, "").trim();
    const employeeValue = parseInt(employeeStr);
    if (!isNaN(employeeValue)) {
      data.employeeCount = employeeValue;
    }
  }
  // オフィス数、工場数、店舗数（0も有効な値として扱う）
  if (row["オフィス数"] !== undefined && row["オフィス数"] !== null && String(row["オフィス数"]).trim() !== "") {
    const officeStr = String(row["オフィス数"]).replace(/,/g, "").trim();
    const officeValue = parseInt(officeStr);
    if (!isNaN(officeValue)) {
      data.officeCount = officeValue;
    }
  }
  if (row["工場数"] !== undefined && row["工場数"] !== null && String(row["工場数"]).trim() !== "") {
    const factoryStr = String(row["工場数"]).replace(/,/g, "").trim();
    const factoryValue = parseInt(factoryStr);
    if (!isNaN(factoryValue)) {
      data.factoryCount = factoryValue;
    }
  }
  if (row["店舗数"] !== undefined && row["店舗数"] !== null && String(row["店舗数"]).trim() !== "") {
    const storeStr = String(row["店舗数"]).replace(/,/g, "").trim();
    const storeValue = parseInt(storeStr);
    if (!isNaN(storeValue)) {
      data.storeCount = storeValue;
    }
  }
  
  // 最新の決算情報を取得
  if (row["決算月1"]) data.fiscalMonth = row["決算月1"];
  if (row["売上1"]) data.revenue = parseFloat(String(row["売上1"]).replace(/,/g, ""));
  if (row["利益1"]) data.financials = row["利益1"];
  
  return data;
}

/**
 * タイプJ（133.csv）のデータをマッピング
 */
function mapTypeJ(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  // 基本情報
  data.name = row["会社名"] || null;
  data.prefecture = row["都道府県"] || null;
  data.representativeName = row["代表者名"] || null;
  data.corporateNumber = row["法人番号"] || null;
  data.companyUrl = row["URL"] || null;
  data.postalCode = row["郵便番号"] || null;
  data.address = row["住所"] || null;
  data.headquartersAddress = row["住所"] || null;
  data.established = row["設立"] || null;
  data.phoneNumber = row["電話番号(窓口)"] || null;
  
  // 業種1〜3
  data.industry = row["業種1"] || null;
  // industryLarge・industryMiddle・industrySmallに業種1〜3を順に対応
  data.industryLarge = row["業種1"] || null;
  data.industryMiddle = row["業種2"] || null;
  data.industrySmall = row["業種3"] || null;
  if (row["業種1"] || row["業種2"] || row["業種3"]) {
    data.industries = [
      row["業種1"] || null,
      row["業種2"] || null,
      row["業種3"] || null,
    ].filter(v => v !== null && v !== "");
  }
  
  // 代表者情報
  data.representativePostalCode = row["代表者郵便番号"] || null;
  data.representativeRegisteredAddress = row["代表者郵便番号"] || null;
  data.representativeHomeAddress = row["代表者住所"] || null;
  data.representativeBirthDate = row["代表者誕生日"] || null;
  
  // 財務情報
  // 資本金（0も有効な値として扱う）
  if (row["資本金"] !== undefined && row["資本金"] !== null && String(row["資本金"]).trim() !== "") {
    const capitalStr = String(row["資本金"]).replace(/,/g, "").trim();
    const capitalValue = parseFloat(capitalStr);
    if (!isNaN(capitalValue)) {
      data.capitalStock = capitalValue;
    }
  }
  
  // 上場区分
  data.listing = row["上場"] || null;
  
  // 直近決算情報
  if (row["直近決算年月"]) {
    data.fiscalMonth = row["直近決算年月"] || null;
  }
  // 直近売上
  if (row["直近売上"] !== undefined && row["直近売上"] !== null && String(row["直近売上"]).trim() !== "") {
    const revenueStr = String(row["直近売上"]).replace(/,/g, "").trim();
    const revenueValue = parseFloat(revenueStr);
    if (!isNaN(revenueValue)) {
      data.revenue = revenueValue;
    }
  }
  // 直近利益
  if (row["直近利益"] !== undefined && row["直近利益"] !== null && String(row["直近利益"]).trim() !== "") {
    const profitStr = String(row["直近利益"]).replace(/,/g, "").trim();
    const profitValue = parseFloat(profitStr);
    if (!isNaN(profitValue)) {
      data.financials = profitValue;
    }
  }
  
  // 説明・概要
  data.companyDescription = row["説明"] || null;
  data.overview = row["概要"] || null;
  
  // 仕入れ先（suppliers配列）
  if (row["仕入れ先"]) {
    const suppliersStr = String(row["仕入れ先"]);
    const suppliersArr = suppliersStr.split(/[、,，]/).map(s => s.trim()).filter(s => s);
    data.suppliers = suppliersArr;
  }
  
  // 取引先（clients）
  data.clients = row["取引先"] || null;
  
  // 取引先銀行（banks配列）
  if (row["取引先銀行"]) {
    const banksStr = String(row["取引先銀行"]);
    // 全角・半角カンマ、全角・半角読点で分割
    const banksArr = banksStr.split(/[、,，,]/).map(s => s.trim()).filter(s => s);
    data.banks = banksArr;
  }
  
  // 取締役
  data.executives = row["取締役"] || null;
  
  // 株主（shareholders）
  data.shareholders = row["株主"] || null;
  
  // 従業員数（0も有効な値として扱う）
  if (row["社員数"] !== undefined && row["社員数"] !== null && String(row["社員数"]).trim() !== "") {
    const employeeStr = String(row["社員数"]).replace(/,/g, "").trim();
    const employeeValue = parseInt(employeeStr);
    if (!isNaN(employeeValue)) {
      data.employeeCount = employeeValue;
    }
  }
  
  // オフィス数、工場数、店舗数（0も有効な値として扱う）
  if (row["オフィス数"] !== undefined && row["オフィス数"] !== null && String(row["オフィス数"]).trim() !== "") {
    const officeStr = String(row["オフィス数"]).replace(/,/g, "").trim();
    const officeValue = parseInt(officeStr);
    if (!isNaN(officeValue)) {
      data.officeCount = officeValue;
    }
  }
  if (row["工場数"] !== undefined && row["工場数"] !== null && String(row["工場数"]).trim() !== "") {
    const factoryStr = String(row["工場数"]).replace(/,/g, "").trim();
    const factoryValue = parseInt(factoryStr);
    if (!isNaN(factoryValue)) {
      data.factoryCount = factoryValue;
    }
  }
  if (row["店舗数"] !== undefined && row["店舗数"] !== null && String(row["店舗数"]).trim() !== "") {
    const storeStr = String(row["店舗数"]).replace(/,/g, "").trim();
    const storeValue = parseInt(storeStr);
    if (!isNaN(storeValue)) {
      data.storeCount = storeValue;
    }
  }
  
  return data;
}

// タイプに応じたマッピング関数を選択
function mapCompanyData(row: Record<string, any>, type: string): Record<string, any> {
  switch (type) {
    case "H": return mapTypeH(row);
    case "I": return mapTypeI(row);
    case "J": return mapTypeJ(row);
    default: throw new Error(`Unknown type: ${type}`);
  }
}

async function main() {
  console.log("================================================================================");
  console.log("タイプH・I・JのCSVファイルを読み込んで既存ドキュメントを更新");
  console.log("================================================================================");
  console.log();
  
  if (DRY_RUN) {
    console.log("🔍 DRY_RUN モード: Firestore は書き換えません\n");
  } else {
    console.log("⚠️  本番モード: Firestore を書き換えます\n");
  }

  const BATCH_LIMIT = 200;
  let globalRowIndex = 0;
  let totalUpdated = 0;
  let totalCreated = 0;
  let totalSkipped = 0;

  for (const [type, csvPath] of Object.entries(TYPE_CSV_FILES)) {
    console.log(`\n【タイプ${type}】${csvPath} を処理中...`);
    
    if (!fs.existsSync(csvPath)) {
      console.error(`  ❌ ファイルが見つかりません: ${csvPath}`);
      continue;
    }

    const csvContent = fs.readFileSync(csvPath, "utf-8");
    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
      trim: true,
    }) as Record<string, any>[];

    console.log(`  📊 総行数: ${records.length}`);

    let batch = db.batch();
    let batchCount = 0;
    let typeUpdated = 0;
    let typeCreated = 0;
    let typeSkipped = 0;

    for (let i = 0; i < records.length; i++) {
      const row = records[i];
      globalRowIndex++;

      // データをマッピング
      const mapped = mapCompanyData(row, type);

      // 企業名・住所・代表者名・法人番号を取得
      const companyName = mapped.name || null;
      const corporateNumber = mapped.corporateNumber || null;
      const headquartersAddress = mapped.headquartersAddress || mapped.address || null;
      const address = mapped.address || null;
      const representativeName = mapped.representativeName || null;

      // キーが無い場合はスキップ
      if (!companyName && !corporateNumber) {
        typeSkipped++;
        if (typeSkipped <= 5) {
          console.warn(`  ⚠️  行 ${i + 2}: 企業名・法人番号が無いためスキップ`);
        }
        continue;
      }

      // 既存ドキュメントを検索
      const existing = await findExistingCompanyDoc(
        corporateNumber,
        companyName,
        headquartersAddress,
        address
      );

      let targetRef: DocumentReference;
      let writeData: Record<string, any>;

      if (existing) {
        // 既存ドキュメントを更新
        targetRef = existing.ref;
        // 既存データとマージ（CSVのデータで上書き）
        writeData = {
          ...getEmptyTemplate(),
          ...mapped,
        };
        typeUpdated++;
        totalUpdated++;

        if (typeUpdated <= 5) {
          console.log(`  🔄 更新: ${companyName} (${existing.matchedBy}, docId: ${targetRef.id})`);
        }
      } else {
        // 新規作成
        const docId = generateNumericDocId(corporateNumber, globalRowIndex);
        targetRef = companiesCol.doc(docId);
        writeData = {
          ...getEmptyTemplate(),
          ...mapped,
        };
        typeCreated++;
        totalCreated++;

        if (typeCreated <= 5) {
          const keyInfo = corporateNumber 
            ? `法人番号: ${corporateNumber}` 
            : `企業名: ${companyName}`;
          console.log(`  ✨ 新規作成: ${companyName} (${keyInfo}, docId: ${docId})`);
        }
      }

      if (!DRY_RUN) {
        batch.set(targetRef, writeData, { merge: true });
        batchCount++;

        if (batchCount >= BATCH_LIMIT) {
          await batch.commit();
          batch = db.batch();
          batchCount = 0;
        }
      }
    }

    // 最後のバッチをコミット
    if (!DRY_RUN && batchCount > 0) {
      await batch.commit();
    }

    totalSkipped += typeSkipped;
    console.log(`  ✅ タイプ${type}完了: 更新 ${typeUpdated}件, 新規作成 ${typeCreated}件, スキップ ${typeSkipped}件`);
  }

  console.log("\n================================================================================");
  console.log("完了");
  console.log("================================================================================");
  console.log(`  🔄 更新: ${totalUpdated} 件`);
  console.log(`  ✨ 新規作成: ${totalCreated} 件`);
  console.log(`  ⏭️  スキップ: ${totalSkipped} 件`);

  if (DRY_RUN) {
    console.log("\n💡 実際に更新を実行するには、--dry-run フラグを外して実行してください");
  }
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

