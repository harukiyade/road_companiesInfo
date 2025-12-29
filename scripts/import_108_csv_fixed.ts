/* 
  108.csvをFirestore `companies_new`にインポートするスクリプト（修正版）
  参考: import_csv_by_groups.tsの処理方法
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/import_108_csv_fixed.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_FILE = path.join(process.cwd(), "csv", "108.csv");

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

    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolvedPath}`);
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

// companies_newコレクションの全159フィールドテンプレート
const COMPANY_TEMPLATE: Record<string, any> = {
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
  departmentLocation: null,
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
  qualificationGrade: null,
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
  profit1: null,
  profit2: null,
  profit3: null,
  profit4: null,
  profit5: null,
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
  netAssets: null,
  transportation: null,
  tradingStatus: null,
  updateCount: null,
  updateDate: null,
  updatedAt: null,
  urls: [],
  wantedly: null,
  youtrust: null,
};

// 郵便番号パターン（3桁-4桁）
const POSTAL_CODE_PATTERN = /^\d{3}-\d{4}$/;

// 無視するフィールド
const IGNORE_FIELDS = new Set([
  "ID",
  "取引種別",
  "SBフラグ",
  "NDA",
  "AD",
  "ステータス",
  "備考",
  "法人番号", // 崩れているため無視
]);

// 文字列を正規化
function norm(value: string | null | undefined): string {
  if (!value) return "";
  return String(value).trim();
}

// 空かどうかをチェック
function isEmpty(value: string | null | undefined): boolean {
  return !value || norm(value) === "";
}

// 郵便番号を正規化
function normalizePostalCode(value: string | null | undefined): string | null {
  if (!value) return null;
  const cleaned = norm(value);
  if (POSTAL_CODE_PATTERN.test(cleaned)) {
    return cleaned;
  }
  return null;
}

// 数値をパース（カンマ除去）
function parseNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  const cleaned = value.toString().replace(/[^\d.-]/g, "");
  if (!cleaned) return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

// 日付をパース
function parseDate(value: string | null | undefined): string | null {
  if (!value) return null;
  const cleaned = value.trim();
  if (!cleaned) return null;
  return cleaned;
}

// 代表者名から生年月日を抽出
function extractBirthDate(value: string): string | null {
  if (!value) return null;
  
  const patterns = [
    /(19\d{2}|20\d{2})[\/年-](\d{1,2})[\/月-](\d{1,2})/g,
    /(19\d{2}|20\d{2})\/(\d{1,2})\/(\d{1,2})/g,
  ];

  for (const pattern of patterns) {
    const match = value.match(pattern);
    if (match) {
      return match[0];
    }
  }
  return null;
}

// 代表者名をクリーンアップ
function cleanRepresentativeName(value: string): string | null {
  if (!value) return null;
  
  let trimmed = value.trim();
  
  // 生年月日を除去
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})[\/年-]\d{1,2}[\/月-]\d{1,2}/g, "").trim();
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})\/\d{1,2}\/\d{1,2}/g, "").trim();
  
  // 役職名を除去
  const titles = [
    "代表取締役", "代表取締役社長", "代表取締役会長", "代表取締役専務",
    "代表取締役常務", "代表取締役副社長", "取締役社長", "取締役会長",
    "社長", "会長", "専務", "常務", "副社長", "代表", "代表者", "CEO", "ceo",
    "取締役", "監査役", "（取）", "（監）", "（会）", "（常）", "（専）", "（代長）", "（代会）", "（相）", "（副長）"
  ];
  
  for (const title of titles) {
    if (trimmed.startsWith(title)) {
      trimmed = trimmed.substring(title.length).trim();
      trimmed = trimmed.replace(/^[\s・、,，]/g, "").trim();
      break;
    }
    const titlePattern = new RegExp(`^${title}[\\s・、,，]`, "i");
    if (titlePattern.test(trimmed)) {
      trimmed = trimmed.replace(titlePattern, "").trim();
      break;
    }
  }
  
  // カッコ内の情報を除去
  trimmed = trimmed.replace(/[（(].*?[）)]/g, "").trim();
  
  // 数字や記号のみの場合はnull
  if (/^[\d\s\-・、,，.。]+$/.test(trimmed)) {
    return null;
  }
  
  return trimmed || null;
}

// 行データをcompanies_newのフィールドにマッピング
function mapRowToCompanyFields(
  row: string[],
  headers: string[]
): Record<string, any> {
  const result: Record<string, any> = { ...COMPANY_TEMPLATE };

  // ヘッダーマップを作成
  const headerMap: Record<string, number> = {};
  headers.forEach((h, i) => {
    if (h && !IGNORE_FIELDS.has(h)) {
      headerMap[h] = i;
    }
  });

  // 会社名
  const nameIdx = headerMap["会社名"];
  if (nameIdx !== undefined) {
    const name = norm(row[nameIdx]);
    if (!isEmpty(name)) result.name = name;
  }

  // 都道府県
  const prefIdx = headerMap["都道府県"];
  if (prefIdx !== undefined) {
    const pref = norm(row[prefIdx]);
    if (!isEmpty(pref)) result.prefecture = pref;
  }

  // 代表者名
  const repIdx = headerMap["代表者名"];
  if (repIdx !== undefined) {
    const repValue = norm(row[repIdx]);
    if (!isEmpty(repValue)) {
      const birthDate = extractBirthDate(repValue);
      const repName = cleanRepresentativeName(repValue);
      if (repName) result.representativeName = repName;
      if (birthDate) result.representativeBirthDate = birthDate;
    }
  }

  // 法人番号は無視（崩れているため）

  // URL
  const urlIdx = headerMap["URL"];
  if (urlIdx !== undefined) {
    const url = norm(row[urlIdx]);
    if (!isEmpty(url) && (url.startsWith("http://") || url.startsWith("https://"))) {
      result.companyUrl = url;
      result.urls = [url];
    }
  }

  // 業種フィールドの処理
  // ヘッダーから業種1〜3を直接取得
  const industry1Idx = headerMap["業種1"];
  const industry2Idx = headerMap["業種2"];
  const industry3Idx = headerMap["業種3"];
  const industry4Idx = headerMap["業種4"];

  if (industry1Idx !== undefined) {
    const ind1 = norm(row[industry1Idx]);
    if (!isEmpty(ind1)) result.industryLarge = ind1;
  }
  if (industry2Idx !== undefined) {
    const ind2 = norm(row[industry2Idx]);
    if (!isEmpty(ind2)) result.industryMiddle = ind2;
  }
  if (industry3Idx !== undefined) {
    const ind3 = norm(row[industry3Idx]);
    if (!isEmpty(ind3)) result.industrySmall = ind3;
  }
  if (industry4Idx !== undefined) {
    const ind4 = norm(row[industry4Idx]);
    if (!isEmpty(ind4)) result.industryDetail = ind4;
  }

  // industry：業種4以降（ヘッダーにない業種を列順で検知）
  // 最後の業種（業種4、または業種3、業種2）の次の列から順に確認し、郵便番号が見つかるまで業種として扱う
  const otherIndustries: string[] = [];
  let lastIndustryColIdx = -1;
  
  // 最後の業種列のインデックスを決定（業種4→業種3→業種2の順で確認）
  if (industry4Idx !== undefined) {
    const ind4 = norm(row[industry4Idx]);
    if (!isEmpty(ind4)) {
      lastIndustryColIdx = industry4Idx;
    }
  }
  if (lastIndustryColIdx < 0 && industry3Idx !== undefined) {
    const ind3 = norm(row[industry3Idx]);
    if (!isEmpty(ind3)) {
      lastIndustryColIdx = industry3Idx;
    }
  }
  if (lastIndustryColIdx < 0 && industry2Idx !== undefined) {
    const ind2 = norm(row[industry2Idx]);
    if (!isEmpty(ind2)) {
      lastIndustryColIdx = industry2Idx;
    }
  }

  let postalFoundAt = -1; // 郵便番号が見つかった列のインデックス
  const headerPostalIdx = headers.findIndex((h) => h === "郵便番号");

  if (lastIndustryColIdx >= 0) {
    // 最後の業種の次の列から順に確認
    for (let i = lastIndustryColIdx + 1; i < row.length; i++) {
      const value = norm(row[i]);
      if (isEmpty(value)) continue;

      // 郵便番号パターンを検知したら終了
      const postal = normalizePostalCode(value);
      if (postal) {
        postalFoundAt = i;
        result.postalCode = postal;
        break;
      }

      // 郵便番号でない場合は業種として扱う
      otherIndustries.push(value);
    }
  } else if (headerPostalIdx >= 0) {
    // 業種が見つからない場合、ヘッダーの郵便番号列から直接取得
    const postalValue = norm(row[headerPostalIdx]);
    const postal = normalizePostalCode(postalValue);
    if (postal) {
      postalFoundAt = headerPostalIdx;
      result.postalCode = postal;
    }
  }

  if (otherIndustries.length > 0) {
    result.industry = otherIndustries.join("、");
  }

  // 列ずれのオフセットを計算
  let columnOffset = 0;
  if (postalFoundAt >= 0 && headerPostalIdx >= 0) {
    columnOffset = postalFoundAt - headerPostalIdx;
  }

  // industries配列には業種1〜3（または業種4）と業種4以降を全て含める
  const industries: string[] = [];
  if (result.industryLarge) industries.push(result.industryLarge);
  if (result.industryMiddle) industries.push(result.industryMiddle);
  if (result.industrySmall) industries.push(result.industrySmall);
  if (result.industryDetail) industries.push(result.industryDetail);
  industries.push(...otherIndustries);
  result.industries = industries;
  
  // industryフィールドには業種4以降を「、」で結合して格納
  // 業種4以降がない場合はindustryLargeを設定
  if (otherIndustries.length > 0) {
    result.industry = otherIndustries.join("、");
  } else if (result.industryDetail) {
    result.industry = result.industryDetail;
  } else {
    result.industry = result.industryLarge || null;
  }

  // ヘッダーインデックスにオフセットを適用する関数
  // 郵便番号以降のフィールドにのみオフセットを適用
  const getIndex = (headerName: string): number | undefined => {
    const baseIdx = headerMap[headerName];
    if (baseIdx === undefined) return undefined;
    
    // 郵便番号以降のフィールドにのみオフセットを適用
    if (columnOffset !== 0 && headerPostalIdx >= 0 && baseIdx >= headerPostalIdx) {
      return baseIdx + columnOffset;
    }
    return baseIdx;
  };

  // 住所
  const addressIdx = getIndex("住所");
  if (addressIdx !== undefined && addressIdx < row.length) {
    const address = norm(row[addressIdx]);
    if (!isEmpty(address)) {
      result.address = address;
      result.headquartersAddress = address;
    }
  }

  // 設立
  const establishedIdx = getIndex("設立");
  if (establishedIdx !== undefined && establishedIdx < row.length) {
    const established = norm(row[establishedIdx]);
    if (!isEmpty(established)) {
      result.established = established;
      result.dateOfEstablishment = established;
      // 年を抽出
      const yearMatch = established.match(/(\d{4})年/);
      if (yearMatch) {
        const year = parseInt(yearMatch[1]);
        if (year >= 1900 && year <= 2100) {
          result.foundingYear = year;
          result.founding = year.toString();
        }
      }
    }
  }

  // 電話番号
  const phoneIdx = getIndex("電話番号(窓口)");
  if (phoneIdx !== undefined && phoneIdx < row.length) {
    const phone = norm(row[phoneIdx]);
    if (!isEmpty(phone)) {
      result.phoneNumber = phone;
    }
  }

  // 代表者郵便番号
  const repPostalIdx = getIndex("代表者郵便番号");
  if (repPostalIdx !== undefined && repPostalIdx < row.length) {
    const repPostal = normalizePostalCode(row[repPostalIdx]);
    if (repPostal) {
      result.representativePostalCode = repPostal;
    }
  }

  // 代表者住所
  const repAddressIdx = getIndex("代表者住所");
  if (repAddressIdx !== undefined && repAddressIdx < row.length) {
    const repAddress = norm(row[repAddressIdx]);
    if (!isEmpty(repAddress)) {
      result.representativeRegisteredAddress = repAddress;
    }
  }

  // 代表者誕生日（既に代表者名から抽出済み）

  // 資本金（1000倍する）
  const capitalIdx = getIndex("資本金");
  if (capitalIdx !== undefined && capitalIdx < row.length) {
    const capital = parseNumeric(row[capitalIdx]);
    if (capital !== null) {
      result.capitalStock = capital * 1000;
    }
  }

  // 上場
  const listingIdx = getIndex("上場");
  if (listingIdx !== undefined && listingIdx < row.length) {
    const listing = norm(row[listingIdx]);
    if (!isEmpty(listing)) {
      result.listing = listing === "上場" || listing.includes("上場") ? "上場" : "非上場";
    }
  }

  // 直近決算年月
  const fiscalIdx = getIndex("直近決算年月");
  if (fiscalIdx !== undefined && fiscalIdx < row.length) {
    const fiscal = norm(row[fiscalIdx]);
    if (!isEmpty(fiscal)) {
      result.latestFiscalYearMonth = fiscal;
      // 年と月を抽出
      const yearMatch = fiscal.match(/(\d{4})年/);
      if (yearMatch) {
        const year = parseInt(yearMatch[1]);
        if (year >= 1900 && year <= 2100) {
          result.foundingYear = year;
        }
      }
      const monthMatch = fiscal.match(/(\d{1,2})月/);
      if (monthMatch) {
        const month = parseInt(monthMatch[1]);
        if (month >= 1 && month <= 12) {
          result.fiscalMonth = month;
        }
      }
    }
  }

  // 直近売上（1000倍する）
  const revenueIdx = getIndex("直近売上");
  if (revenueIdx !== undefined && revenueIdx < row.length) {
    const revenue = parseNumeric(row[revenueIdx]);
    if (revenue !== null) {
      result.latestRevenue = revenue * 1000;
    }
  }

  // 直近利益（1000倍する）
  const profitIdx = getIndex("直近利益");
  if (profitIdx !== undefined && profitIdx < row.length) {
    const profit = parseNumeric(row[profitIdx]);
    if (profit !== null) {
      result.latestProfit = profit * 1000;
    }
  }

  // 説明
  const descIdx = getIndex("説明");
  if (descIdx !== undefined && descIdx < row.length) {
    const desc = norm(row[descIdx]);
    if (!isEmpty(desc)) {
      result.companyDescription = desc;
    }
  }

  // 概要
  const overviewIdx = getIndex("概要");
  if (overviewIdx !== undefined && overviewIdx < row.length) {
    const overview = norm(row[overviewIdx]);
    if (!isEmpty(overview)) {
      result.overview = overview;
    }
  }

  // 仕入れ先
  const supplierIdx = getIndex("仕入れ先");
  if (supplierIdx !== undefined && supplierIdx < row.length) {
    const suppliers = norm(row[supplierIdx]);
    if (!isEmpty(suppliers)) {
      const supplierList = suppliers
        .split(/[，,、]/)
        .map((s) => s.trim())
        .filter((s) => s);
      result.suppliers = supplierList;
    }
  }

  // 取引先（文字列として格納）
  const clientIdx = getIndex("取引先");
  if (clientIdx !== undefined && clientIdx < row.length) {
    const clients = norm(row[clientIdx]);
    if (!isEmpty(clients)) {
      result.clients = clients;
    }
  }

  // 取引先銀行
  const bankIdx = getIndex("取引先銀行");
  if (bankIdx !== undefined && bankIdx < row.length) {
    const banks = norm(row[bankIdx]);
    if (!isEmpty(banks)) {
      const bankList = banks
        .split(/[，,、]/)
        .map((b) => b.trim())
        .filter((b) => b);
      // banksフィールドに配列として格納
      result.banks = bankList;
    }
  }

  // 取締役（文字列として格納）
  const execIdx = getIndex("取締役");
  if (execIdx !== undefined && execIdx < row.length) {
    const executives = norm(row[execIdx]);
    if (!isEmpty(executives)) {
      result.executives = executives;
    }
  }

  // 株主（文字列として格納）
  const shareholderIdx = getIndex("株主");
  if (shareholderIdx !== undefined && shareholderIdx < row.length) {
    const shareholders = norm(row[shareholderIdx]);
    if (!isEmpty(shareholders)) {
      result.shareholders = shareholders;
    }
  }

  // 社員数
  const employeeIdx = getIndex("社員数");
  if (employeeIdx !== undefined && employeeIdx < row.length) {
    const employees = parseNumeric(row[employeeIdx]);
    if (employees !== null) {
      result.employeeCount = employees;
    }
  }

  // オフィス数
  const officeIdx = getIndex("オフィス数");
  if (officeIdx !== undefined && officeIdx < row.length) {
    const offices = parseNumeric(row[officeIdx]);
    if (offices !== null) {
      result.officeCount = offices;
    }
  }

  // 工場数
  const factoryIdx = getIndex("工場数");
  if (factoryIdx !== undefined && factoryIdx < row.length) {
    const factories = parseNumeric(row[factoryIdx]);
    if (factories !== null) {
      result.factoryCount = factories;
    }
  }

  // 店舗数
  const storeIdx = getIndex("店舗数");
  if (storeIdx !== undefined && storeIdx < row.length) {
    const stores = parseNumeric(row[storeIdx]);
    if (stores !== null) {
      result.storeCount = stores;
    }
  }

  // タイムスタンプ
  result.updatedAt = admin.firestore.FieldValue.serverTimestamp();
  result.createdAt = admin.firestore.FieldValue.serverTimestamp();

  return result;
}

// ドキュメントIDを数値で生成
function generateNumericDocId(rowIndex: number): string {
  const timestamp = Date.now();
  const paddedIndex = String(rowIndex).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 既存ドキュメントを検索（企業名 + 住所）
async function findExistingCompanyDoc(
  companyName: string | null,
  address: string | null
): Promise<DocumentReference<DocumentData> | null> {
  if (!companyName || !companyName.trim()) return null;

  // 企業名で検索
  const snapByName = await companiesCol
    .where("name", "==", companyName.trim())
    .limit(1)
    .get();

  if (snapByName.empty) return null;

  // 住所も確認
  if (address && address.trim()) {
    const doc = snapByName.docs[0];
    const data = doc.data();
    const docAddress = data.address || data.headquartersAddress || "";
    if (docAddress.trim() !== address.trim()) {
      return null; // 住所が一致しない
    }
  }

  return snapByName.docs[0].ref;
}

// メイン処理
async function main() {
  console.log("📄 108.csvをインポートします\n");

  if (!fs.existsSync(CSV_FILE)) {
    console.error(`❌ エラー: ${CSV_FILE} が見つかりません`);
    process.exit(1);
  }

  console.log(`📂 CSVファイル: ${CSV_FILE}\n`);

  const content = fs.readFileSync(CSV_FILE, "utf8");
  const records: string[][] = parse(content, {
    columns: false,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_records_with_error: true,
  });

  if (records.length < 2) {
    console.log("❌ CSVに有効なレコードがありません");
    return;
  }

  const headers = records[0];
  const dataRows = records.slice(1);

  console.log(`📋 ヘッダー数: ${headers.length}`);
  console.log(`📋 処理するレコード数: ${dataRows.length}\n`);

  let createdCount = 0;
  let updatedCount = 0;
  let skippedCount = 0;

  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 400;

  for (let i = 0; i < dataRows.length; i++) {
    const row = dataRows[i];
    const rowNumber = i + 2; // ヘッダー行を考慮

    // 企業名がない場合はスキップ
    const nameIdx = headers.indexOf("会社名");
    if (nameIdx < 0 || !row[nameIdx]?.trim()) {
      skippedCount++;
      console.log(`⚠️  [行${rowNumber}] 企業名がないためスキップ`);
      continue;
    }

    const mapped = mapRowToCompanyFields(row, headers);
    const companyName = mapped.name;
    const address = mapped.address || mapped.headquartersAddress;

    console.log(`\n[行${rowNumber}] ${companyName}`);
    console.log(`  業種: ${mapped.industryLarge || ""} / ${mapped.industryMiddle || ""} / ${mapped.industrySmall || ""}`);
    if (mapped.industryDetail) console.log(`  業種4: ${mapped.industryDetail}`);
    if (mapped.industry) console.log(`  業種4以降: ${mapped.industry}`);
    console.log(`  郵便番号: ${mapped.postalCode || ""}`);
    console.log(`  住所: ${address || ""}`);
    console.log(`  資本金: ${mapped.capitalStock || ""}`);
    console.log(`  売上: ${mapped.latestRevenue || ""}`);
    console.log(`  利益: ${mapped.latestProfit || ""}`);

    // 既存ドキュメントを検索
    const existingRef = await findExistingCompanyDoc(companyName, address);

    let targetRef: DocumentReference<DocumentData>;
    if (existingRef) {
      targetRef = existingRef;
      updatedCount++;
      console.log(`  🔄 更新: ${companyName}`);
    } else {
      const docId = generateNumericDocId(i);
      targetRef = companiesCol.doc(docId);
      createdCount++;
      console.log(`  ✨ 新規作成: ${companyName} (docId: ${docId})`);
    }

    // 既存ドキュメントの場合は完全に置き換える（merge: false）
    if (existingRef) {
      batch.set(targetRef, mapped, { merge: false });
    } else {
      batch.set(targetRef, mapped, { merge: true });
    }
    batchCount++;

    if (batchCount >= BATCH_LIMIT) {
      await batch.commit();
      console.log(`  ✅ バッチコミット: ${BATCH_LIMIT}件`);
      batch = db.batch();
      batchCount = 0;
    }
  }

  // 残りのバッチをコミット
  if (batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 最終バッチコミット: ${batchCount}件`);
  }

  console.log("\n" + "=".repeat(80));
  console.log("✅ インポート完了");
  console.log(`   新規作成: ${createdCount}件`);
  console.log(`   更新: ${updatedCount}件`);
  console.log(`   スキップ: ${skippedCount}件`);
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});
