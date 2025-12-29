/* 
  CSVグループ別インポートスクリプト
  
  各グループの代表ファイルから5社分をテストとしてインポート
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/import_csv_by_groups.ts [--dry-run]
*/

import "dotenv/config";
import fs from "fs";
import path from "path";
import Papa from "papaparse";
import admin from "firebase-admin";

// Firebase初期化
function initAdmin() {
  if (admin.apps.length) return;
  try {
    admin.initializeApp({
      credential: admin.credential.applicationDefault(),
    });
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", (error as Error).message);
    throw error;
  }
}

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const TEST_MODE = process.argv.includes("--test") || process.env.TEST_MODE === 'true';
const TEST_LIMIT = process.env.TEST_LIMIT ? parseInt(process.env.TEST_LIMIT) : 5;

// グループ定義（代表ファイル）
const GROUP_FILES = {
  group1: "csv/111.csv",  // グループ1の代表
  group2: "csv/118.csv",  // グループ2の代表
  group3: "csv/38.csv",   // グループ3の代表
  group4: "csv/107.csv",  // グループ4の代表
  group5: "csv/110.csv",  // グループ5の代表
  group6: "csv/119.csv",  // グループ6の代表
  group7: "csv/122.csv",  // グループ7の代表
};

// グループ2の全ファイル
const GROUP2_FILES = [
  "csv/118.csv",
  "csv/120.csv",
  "csv/121.csv",
  "csv/123.csv",
  "csv/124.csv",
  "csv/125.csv",
];

// グループ1の全ファイルリスト
const GROUP1_FILES = [
  "csv/111.csv",
  "csv/112.csv",
  "csv/113.csv",
  "csv/114.csv",
  "csv/115.csv",
  "csv/116.csv",
  "csv/117.csv",
];

// グループ3の全ファイルリスト
const GROUP3_FILES = [
  "csv/38.csv",
];

// グループ4の全ファイルリスト
const GROUP4_FILES = [
  "csv/107.csv",
  "csv/108.csv",
  "csv/109.csv",
];

// グループ5の全ファイルリスト
const GROUP5_FILES = [
  "csv/110.csv",
];

// グループ6の全ファイルリスト
const GROUP6_FILES = [
  "csv/119.csv",
];

// グループ7の全ファイルリスト
const GROUP7_FILES = [
  "csv/122.csv",
];

// 無視するフィールド
const IGNORE_FIELDS = new Set([
  "ID",
  "取引種別",
  "SBフラグ",
  "NDA",
  "AD",
  "ステータス",
  "備考",
]);

// 郵便番号パターン（3桁-4桁）
const POSTAL_CODE_PATTERN = /^\d{3}-?\d{4}$/;

// 法人番号パターン（13桁の数値）
const CORPORATE_NUMBER_PATTERN = /^\d{13}$/;

// 文字列正規化
function norm(s: string | null | undefined): string {
  if (!s) return "";
  return s.toString().trim();
}

// 空欄チェック
function isEmpty(s: string | null | undefined): boolean {
  const v = norm(s);
  return !v || v === "-" || v === "ー" || v === "―" || v === "n/a";
}

// 郵便番号を検証・正規化
function normalizePostalCode(value: string | null | undefined): string | null {
  const v = norm(value);
  if (!v) return null;
  
  // ハイフンなしの7桁数字を検証
  const digits = v.replace(/\D/g, "");
  if (digits.length === 7) {
    return `${digits.slice(0, 3)}-${digits.slice(3)}`;
  }
  
  // ハイフンありの形式を検証
  if (POSTAL_CODE_PATTERN.test(v)) {
    return v.includes("-") ? v : `${v.slice(0, 3)}-${v.slice(3)}`;
  }
  
  return null;
}

// 法人番号を検証
function validateCorporateNumber(value: string | null | undefined): string | null {
  const v = norm(value);
  if (!v) return null;
  
  // 科学記数法（9.18E+12など）は壊れているので無視
  if (/^\d+\.\d+E\+\d+$/i.test(v) || /^\d+\.\d+E-\d+$/i.test(v) || /E/i.test(v)) {
    return null;
  }
  
  // 通常の数値文字列を処理（13桁の数値のみ）
  const digits = v.replace(/\D/g, "");
  if (digits.length === 13 && CORPORATE_NUMBER_PATTERN.test(digits)) {
    return digits;
  }
  
  return null;
}

// 数値変換（カンマ、円記号などを除去）
function parseNumber(value: string | null | undefined): number | null {
  const v = norm(value);
  if (!v) return null;
  
  // カンマ、円記号、単位を除去
  const cleaned = v.replace(/[,，円¥¥人|名]/g, "");
  
  // 億、万、千の単位を処理
  const unitMatch = cleaned.match(/^([\d.]+)\s*(億|万|千)?/);
  if (unitMatch) {
    const num = parseFloat(unitMatch[1]);
    if (isNaN(num)) return null;
    
    const unit = unitMatch[2];
    if (unit === "億") return Math.round(num * 100_000_000);
    if (unit === "万") return Math.round(num * 10_000);
    if (unit === "千") return Math.round(num * 1_000);
    return Math.round(num);
  }
  
  const num = parseFloat(cleaned.replace(/[^\d.]/g, ""));
  return isNaN(num) ? null : Math.round(num);
}

// 年を抽出（設立年など）
function extractYear(value: string | null | undefined): number | null {
  const v = norm(value);
  if (!v) return null;
  
  const match = v.match(/(\d{4})年/);
  if (match) {
    const year = parseInt(match[1]);
    if (year >= 1800 && year <= 2100) return year;
  }
  
  return null;
}

// 都道府県を抽出
const PREF_LIST = [
  "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
  "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
  "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
  "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
  "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
  "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
  "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
];

function extractPrefecture(addr: string | null | undefined): string | null {
  const v = norm(addr);
  if (!v) return null;
  
  for (const pref of PREF_LIST) {
    if (v.includes(pref)) return pref;
  }
  
  return null;
}

// 代表者名から生年月日を抽出
function extractBirthDate(value: string | null | undefined): string | null {
  const v = norm(value);
  if (!v) return null;
  
  // 生年月日パターン（1900-2100年の範囲）
  const patterns = [
    /(19\d{2}|20\d{2})[\/年-](\d{1,2})[\/月-](\d{1,2})/g,
    /(19\d{2}|20\d{2})\/(\d{1,2})\/(\d{1,2})/g,
  ];
  
  for (const pattern of patterns) {
    const match = v.match(pattern);
    if (match) {
      const dateStr = match[0];
      const parts = dateStr.split(/[\/年-]/);
      if (parts.length >= 3) {
        const year = parseInt(parts[0]);
        const month = parseInt(parts[1]);
        const day = parseInt(parts[2]);
        
        if (year >= 1900 && year <= 2100 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
          return dateStr;
        }
      }
    }
  }
  
  return null;
}

// 代表者名から役職を除去
function cleanRepresentativeName(value: string | null | undefined): string | null {
  const v = norm(value);
  if (!v) return null;
  
  // 生年月日を除去
  let cleaned = v.replace(/(19\d{2}|20\d{2})[\/年-](\d{1,2})[\/月-](\d{1,2})/g, "").trim();
  
  // 役職名を除去
  const titles = [
    "代表取締役", "代表取締役社長", "代表取締役会長", "代表取締役専務",
    "代表取締役常務", "代表取締役副社長", "取締役社長", "取締役会長",
    "社長", "会長", "専務", "常務", "副社長", "代表", "代表者", "CEO", "ceo"
  ];
  
  for (const title of titles) {
    if (cleaned.startsWith(title)) {
      cleaned = cleaned.substring(title.length).trim();
      cleaned = cleaned.replace(/^[\s・、,，]/g, "").trim();
      break;
    }
    const titlePattern = new RegExp(`^${title}[\\s・、,，]`, "i");
    if (titlePattern.test(cleaned)) {
      cleaned = cleaned.replace(titlePattern, "").trim();
      break;
    }
  }
  
  // カッコ内の情報を除去
  cleaned = cleaned.replace(/[（(].*?[）)]/g, "").trim();
  
  // 数字や記号のみの場合はnull
  if (/^[\d\s\-・、,，.。]+$/.test(cleaned)) {
    return null;
  }
  
  return cleaned || null;
}

// 業種フィールドの処理（郵便番号検知ロジック含む）
function processIndustryFields(
  row: string[],
  headers: string[],
  groupNumber: number
): {
  industries: string[];
  postalCode: string | null;
  address: string | null;
  addressStartIndex: number;
} {
  const industries: string[] = [];
  let postalCode: string | null = null;
  let address: string | null = null;
  let addressStartIndex = -1;
  
  // グループごとの開始インデックス
  let industryStartIndex = -1;
  let expectedPostalAfterIndex = -1;
  
  if (groupNumber === 1) {
    // グループ1: 業種3以降、業種4→郵便番号（例外的に業種5→郵便番号、業種5→業種6→郵便番号）
    const idx1 = headers.findIndex(h => h === "業種1");
    const idx3 = headers.findIndex(h => h === "業種3");
    if (idx3 >= 0) industryStartIndex = idx3;
    else if (idx1 >= 0) industryStartIndex = idx1 + 2;
  } else if (groupNumber === 2 || groupNumber === 6) {
    // グループ2,6: 業種2以降、業種3→郵便番号（例外的に業種4→郵便番号、業種5→業種6→郵便番号）
    const idx2 = headers.findIndex(h => h === "業種2");
    const idx3 = headers.findIndex(h => h === "業種3");
    if (idx3 >= 0) industryStartIndex = idx3;
    else if (idx2 >= 0) industryStartIndex = idx2 + 1;
  } else if (groupNumber === 3 || groupNumber === 7) {
    // グループ3,7: 業種2以降、業種3→郵便番号（例外的に業種4→郵便番号、業種4→業種5→郵便番号）
    const idx2 = headers.findIndex(h => h === "業種2");
    const idx3 = headers.findIndex(h => h === "業種3");
    if (idx3 >= 0) industryStartIndex = idx3;
    else if (idx2 >= 0) industryStartIndex = idx2 + 1;
  } else if (groupNumber === 4 || groupNumber === 5) {
    // グループ4,5: 業種3以降、業種（細）→郵便番号（例外的に業種（細）→業種5→郵便番号、業種（細）→業種5→業種6→郵便番号）
    const idx3 = headers.findIndex(h => h === "業種3");
    const idxDetail = headers.findIndex(h => h === "業種（細）");
    if (idxDetail >= 0) industryStartIndex = idxDetail;
    else if (idx3 >= 0) industryStartIndex = idx3 + 1;
  }
  
  if (industryStartIndex < 0) {
    // フォールバック: 業種で始まる列を探す
    for (let i = 0; i < headers.length; i++) {
      if (headers[i]?.includes("業種")) {
        industryStartIndex = i;
        break;
      }
    }
  }
  
  if (industryStartIndex < 0) return { industries, postalCode, address, addressStartIndex: -1 };
  
  // 業種フィールドを収集し、郵便番号を検知
  // 注意: 郵便番号以降の列は各々ヘッダーに基づいて処理されるため、
  // ここでは業種の収集と郵便番号の検知のみを行う
  let foundPostal = false;
  
  for (let i = industryStartIndex; i < row.length; i++) {
    const value = norm(row[i]);
    if (isEmpty(value)) continue;
    
    // 郵便番号パターンを検知
    if (!foundPostal && normalizePostalCode(value)) {
      postalCode = normalizePostalCode(value);
      foundPostal = true;
      addressStartIndex = i + 1;
      // 郵便番号が見つかったら、業種の収集は終了
      // 以降の列はヘッダーに基づいて処理される
      break;
    }
    
    // 郵便番号が見つかる前は業種として扱う
    // 郵便番号パターンでないことを確認
    if (!POSTAL_CODE_PATTERN.test(value)) {
      industries.push(value);
    }
  }
  
  // addressはここでは設定しない（ヘッダーに基づいて後で処理される）
  return { industries, postalCode, address: null, addressStartIndex };
}

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
  tradingStatus: null,
  transportation: null,
  updateCount: null,
  updateDate: null,
  updatedAt: null,
  urls: [],
  wantedly: null,
  youtrust: null,
};

// CSV行をcompanies_new形式に変換
function convertRowToCompany(
  row: string[],
  headers: string[],
  groupNumber: number,
  csvFilePath?: string
): Record<string, any> | null {
  // テンプレートをコピーして全フィールドを初期化
  const company: Record<string, any> = JSON.parse(JSON.stringify(COMPANY_TEMPLATE));
  
  // ファイル名を取得（130.csvと131.csvの判定用）
  const fileName = csvFilePath ? path.basename(csvFilePath) : "";
  const is130or131 = fileName.includes("130") || fileName.includes("131");
  
  // 基本マッピング
  const headerMap: Record<string, string> = {};
  headers.forEach((h, i) => {
    if (h && !IGNORE_FIELDS.has(h)) {
      headerMap[h] = i.toString();
    }
  });
  
  // 会社名（日本語ヘッダーまたは英語ヘッダー）
  let nameIdx = headerMap["会社名"];
  if (nameIdx === undefined) {
    // 英語ヘッダーの場合
    const nameColIdx = headers.findIndex(h => norm(h) === "name");
    if (nameColIdx >= 0) nameIdx = nameColIdx.toString();
  }
  if (nameIdx !== undefined) {
    const name = norm(row[parseInt(nameIdx)]);
    if (!isEmpty(name)) company.name = name;
  }
  
  // 都道府県
  const prefIdx = headerMap["都道府県"];
  if (prefIdx !== undefined) {
    const pref = norm(row[parseInt(prefIdx)]);
    if (!isEmpty(pref)) company.prefecture = pref;
  }
  
  // 代表者名（日本語ヘッダーまたは英語ヘッダー）
  let repIdx = headerMap["代表者名"];
  if (repIdx === undefined) {
    // 英語ヘッダーの場合
    const repColIdx = headers.findIndex(h => norm(h) === "representativename");
    if (repColIdx >= 0) repIdx = repColIdx.toString();
  }
  if (repIdx !== undefined) {
    const repValue = norm(row[parseInt(repIdx)]);
    if (!isEmpty(repValue)) {
      const birthDate = extractBirthDate(repValue);
      const repName = cleanRepresentativeName(repValue);
      if (repName) company.representativeName = repName;
      if (birthDate) company.representativeBirthDate = birthDate;
    }
  }
  
  // 法人番号（13桁の数値のみ）
  // グループ4と5では法人番号を無視
  if (groupNumber !== 4 && groupNumber !== 5) {
    let corpNumIdx = headerMap["法人番号"];
    if (corpNumIdx === undefined) {
      // 英語ヘッダーの場合
      const corpNumColIdx = headers.findIndex(h => norm(h) === "corporatenumber");
      if (corpNumColIdx >= 0) corpNumIdx = corpNumColIdx.toString();
    }
    if (corpNumIdx !== undefined) {
      const corpNum = validateCorporateNumber(row[parseInt(corpNumIdx)]);
      if (corpNum) company.corporateNumber = corpNum;
    }
  }
  
  // URL（日本語ヘッダーまたは英語ヘッダー）
  let urlIdx = headerMap["URL"];
  if (urlIdx === undefined) {
    // 英語ヘッダーの場合
    const urlColIdx = headers.findIndex(h => norm(h) === "companyurl" || norm(h) === "url");
    if (urlColIdx >= 0) urlIdx = urlColIdx.toString();
  }
  if (urlIdx !== undefined) {
    const url = norm(row[parseInt(urlIdx)]);
    if (!isEmpty(url) && (url.startsWith("http://") || url.startsWith("https://"))) {
      company.companyUrl = url;
    }
  }
  
  // 業種フィールドの処理（郵便番号検知含む）
  const industryData = processIndustryFields(row, headers, groupNumber);
  
  // グループ1の場合、列ずれを考慮するためのオフセット
  let columnOffset = 0;
  
  // グループ1の業種マッピング
  if (groupNumber === 1) {
    // ヘッダーから業種1〜4を直接取得（日本語ヘッダーまたは英語ヘッダー）
    let industry1Idx = headerMap["業種1"];
    let industry2Idx = headerMap["業種2"];
    let industry3Idx = headerMap["業種3"];
    let industry4Idx = headerMap["業種4"];
    
    // 英語ヘッダーの場合（130.csvなど）
    if (industry1Idx === undefined) {
      const ind1ColIdx = headers.findIndex(h => norm(h) === "industrylarge");
      if (ind1ColIdx >= 0) industry1Idx = ind1ColIdx.toString();
    }
    if (industry2Idx === undefined) {
      const ind2ColIdx = headers.findIndex(h => norm(h) === "industrymiddle");
      if (ind2ColIdx >= 0) industry2Idx = ind2ColIdx.toString();
    }
    if (industry3Idx === undefined) {
      const ind3ColIdx = headers.findIndex(h => norm(h) === "industrysmall");
      if (ind3ColIdx >= 0) industry3Idx = ind3ColIdx.toString();
    }
    if (industry4Idx === undefined) {
      const ind4ColIdx = headers.findIndex(h => norm(h) === "industrydetail");
      if (ind4ColIdx >= 0) industry4Idx = ind4ColIdx.toString();
    }
    
    if (industry1Idx !== undefined) {
      const ind1 = norm(row[parseInt(industry1Idx)]);
      if (!isEmpty(ind1)) company.industryLarge = ind1;
    }
    if (industry2Idx !== undefined) {
      const ind2 = norm(row[parseInt(industry2Idx)]);
      if (!isEmpty(ind2)) company.industryMiddle = ind2;
    }
    if (industry3Idx !== undefined) {
      const ind3 = norm(row[parseInt(industry3Idx)]);
      if (!isEmpty(ind3)) company.industrySmall = ind3;
    }
    if (industry4Idx !== undefined) {
      const ind4 = norm(row[parseInt(industry4Idx)]);
      if (!isEmpty(ind4)) company.industryDetail = ind4;
    }
    
    // industry：業種5以降（ヘッダーにない業種を列順で検知）
    // 最後の業種（業種4、または業種3、業種2）の次の列から順に確認し、郵便番号が見つかるまで業種として扱う
    const otherIndustries: string[] = [];
    // 最後の業種列のインデックスを決定（業種4→業種3→業種2の順で確認）
    let lastIndustryColIdx = -1;
    if (industry4Idx !== undefined) {
      const ind4 = norm(row[parseInt(industry4Idx)]);
      if (!isEmpty(ind4)) {
        lastIndustryColIdx = parseInt(industry4Idx);
      }
    }
    if (lastIndustryColIdx < 0 && industry3Idx !== undefined) {
      const ind3 = norm(row[parseInt(industry3Idx)]);
      if (!isEmpty(ind3)) {
        lastIndustryColIdx = parseInt(industry3Idx);
      }
    }
    if (lastIndustryColIdx < 0 && industry2Idx !== undefined) {
      const ind2 = norm(row[parseInt(industry2Idx)]);
      if (!isEmpty(ind2)) {
        lastIndustryColIdx = parseInt(industry2Idx);
      }
    }
    
    let postalFoundAt = -1; // 郵便番号が見つかった列のインデックス（グループ1用）
    const headerPostalIdx = headers.findIndex(h => h === "郵便番号");
    
    if (lastIndustryColIdx >= 0) {
      // 最後の業種の次の列から順に確認
      for (let i = lastIndustryColIdx + 1; i < row.length; i++) {
        const value = norm(row[i]);
        if (isEmpty(value)) continue;
        
        // 郵便番号パターンを検知したら終了
        if (normalizePostalCode(value)) {
          postalFoundAt = i;
          // 郵便番号が見つかったら、その列の値を郵便番号として設定
          const postal = normalizePostalCode(value);
          if (postal && !company.postalCode) {
            company.postalCode = postal;
          }
          break;
        }
        
        // 郵便番号でない場合は業種として扱う
        if (!POSTAL_CODE_PATTERN.test(value)) {
          otherIndustries.push(value);
        }
      }
    } else if (headerPostalIdx >= 0) {
      // 業種が見つからない場合、ヘッダーの郵便番号列から直接取得
      const postalValue = norm(row[headerPostalIdx]);
      const postal = normalizePostalCode(postalValue);
      if (postal) {
        postalFoundAt = headerPostalIdx;
        company.postalCode = postal;
      }
    }
    
    if (otherIndustries.length > 0) {
      company.industry = otherIndustries.join("、");
    }
    
    // グループ1の場合、郵便番号以降の列をヘッダー順で処理するためのオフセットを計算
    // ヘッダーの「郵便番号」列のインデックスと、実際の郵便番号が見つかった位置の差
    if (postalFoundAt >= 0 && headerPostalIdx >= 0) {
      columnOffset = postalFoundAt - headerPostalIdx;
    }
    
    // industries配列には業種1〜4と業種5以降を全て含める
    const industries: string[] = [];
    if (company.industryLarge) industries.push(company.industryLarge);
    if (company.industryMiddle) industries.push(company.industryMiddle);
    if (company.industrySmall) industries.push(company.industrySmall);
    if (company.industryDetail) industries.push(company.industryDetail);
    if (otherIndustries.length > 0) industries.push(...otherIndustries);
    if (industries.length > 0) company.industries = industries;
  } else if (groupNumber === 2 || groupNumber === 6) {
    // グループ2,6の業種マッピング（グループ1と同様の詳細処理）
    // ヘッダーから業種1〜3を直接取得（業種4がある場合も確認）
    const industry1Idx = headerMap["業種1"];
    const industry2Idx = headerMap["業種2"];
    const industry3Idx = headerMap["業種3"];
    const industry4Idx = headerMap["業種4"];
    
    if (industry1Idx !== undefined) {
      const ind1 = norm(row[parseInt(industry1Idx)]);
      if (!isEmpty(ind1)) company.industryLarge = ind1;
    }
    if (industry2Idx !== undefined) {
      const ind2 = norm(row[parseInt(industry2Idx)]);
      if (!isEmpty(ind2)) company.industryMiddle = ind2;
    }
    if (industry3Idx !== undefined) {
      const ind3 = norm(row[parseInt(industry3Idx)]);
      if (!isEmpty(ind3)) company.industrySmall = ind3;
    }
    if (industry4Idx !== undefined) {
      const ind4 = norm(row[parseInt(industry4Idx)]);
      if (!isEmpty(ind4)) company.industryDetail = ind4;
    }
    
    // industry：業種4以降（ヘッダーにない業種を列順で検知）
    // 最後の業種（業種4、または業種3、業種2）の次の列から順に確認し、郵便番号が見つかるまで業種として扱う
    const otherIndustries: string[] = [];
    // 最後の業種列のインデックスを決定（業種4→業種3→業種2の順で確認）
    let lastIndustryColIdx = -1;
    if (industry4Idx !== undefined) {
      const ind4 = norm(row[parseInt(industry4Idx)]);
      if (!isEmpty(ind4)) {
        lastIndustryColIdx = parseInt(industry4Idx);
      }
    }
    if (lastIndustryColIdx < 0 && industry3Idx !== undefined) {
      const ind3 = norm(row[parseInt(industry3Idx)]);
      if (!isEmpty(ind3)) {
        lastIndustryColIdx = parseInt(industry3Idx);
      }
    }
    if (lastIndustryColIdx < 0 && industry2Idx !== undefined) {
      const ind2 = norm(row[parseInt(industry2Idx)]);
      if (!isEmpty(ind2)) {
        lastIndustryColIdx = parseInt(industry2Idx);
      }
    }
    
    let postalFoundAt = -1; // 郵便番号が見つかった列のインデックス（グループ2用）
    const headerPostalIdx = headers.findIndex(h => h === "郵便番号");
    
    if (lastIndustryColIdx >= 0) {
      // 最後の業種の次の列から順に確認
      for (let i = lastIndustryColIdx + 1; i < row.length; i++) {
        const value = norm(row[i]);
        if (isEmpty(value)) continue;
        
        // 郵便番号パターンを検知したら終了
        if (normalizePostalCode(value)) {
          postalFoundAt = i;
          // 郵便番号が見つかったら、その列の値を郵便番号として設定
          const postal = normalizePostalCode(value);
          if (postal && !company.postalCode) {
            company.postalCode = postal;
          }
          break;
        }
        
        // 郵便番号でない場合は業種として扱う
        if (!POSTAL_CODE_PATTERN.test(value)) {
          otherIndustries.push(value);
        }
      }
    } else if (headerPostalIdx >= 0) {
      // 業種が見つからない場合、ヘッダーの郵便番号列から直接取得
      const postalValue = norm(row[headerPostalIdx]);
      const postal = normalizePostalCode(postalValue);
      if (postal) {
        postalFoundAt = headerPostalIdx;
        company.postalCode = postal;
      }
    }
    
    if (otherIndustries.length > 0) {
      company.industry = otherIndustries.join("、");
    }
    
    // グループ2の場合、郵便番号以降の列をヘッダー順で処理するためのオフセットを計算
    // ヘッダーの「郵便番号」列のインデックスと、実際の郵便番号が見つかった位置の差
    if (postalFoundAt >= 0 && headerPostalIdx >= 0) {
      columnOffset = postalFoundAt - headerPostalIdx;
    }
    
    // industries配列には業種1〜3（または業種4）と業種4以降を全て含める
    const industries: string[] = [];
    if (company.industryLarge) industries.push(company.industryLarge);
    if (company.industryMiddle) industries.push(company.industryMiddle);
    if (company.industrySmall) industries.push(company.industrySmall);
    if (company.industryDetail) industries.push(company.industryDetail);
    if (otherIndustries.length > 0) industries.push(...otherIndustries);
    if (industries.length > 0) company.industries = industries;
  } else if (groupNumber === 3 || groupNumber === 7) {
    // グループ3,7の業種マッピング（グループ1,2,6と同様の詳細処理）
    // ヘッダーから業種1〜3を直接取得（業種4がある場合も確認）
    const industry1Idx = headerMap["業種1"];
    const industry2Idx = headerMap["業種2"];
    const industry3Idx = headerMap["業種3"];
    const industry4Idx = headerMap["業種4"];
    
    if (industry1Idx !== undefined) {
      const ind1 = norm(row[parseInt(industry1Idx)]);
      if (!isEmpty(ind1)) company.industryLarge = ind1;
    }
    if (industry2Idx !== undefined) {
      const ind2 = norm(row[parseInt(industry2Idx)]);
      if (!isEmpty(ind2)) company.industryMiddle = ind2;
    }
    if (industry3Idx !== undefined) {
      const ind3 = norm(row[parseInt(industry3Idx)]);
      if (!isEmpty(ind3)) company.industrySmall = ind3;
    }
    if (industry4Idx !== undefined) {
      const ind4 = norm(row[parseInt(industry4Idx)]);
      if (!isEmpty(ind4)) company.industryDetail = ind4;
    }
    
    // industry：業種4以降（ヘッダーにない業種を列順で検知）
    // 最後の業種（業種4、または業種3、業種2）の次の列から順に確認し、郵便番号が見つかるまで業種として扱う
    const otherIndustries: string[] = [];
    // 最後の業種列のインデックスを決定（業種4→業種3→業種2の順で確認）
    let lastIndustryColIdx = -1;
    if (industry4Idx !== undefined) {
      const ind4 = norm(row[parseInt(industry4Idx)]);
      if (!isEmpty(ind4)) {
        lastIndustryColIdx = parseInt(industry4Idx);
      }
    }
    if (lastIndustryColIdx < 0 && industry3Idx !== undefined) {
      const ind3 = norm(row[parseInt(industry3Idx)]);
      if (!isEmpty(ind3)) {
        lastIndustryColIdx = parseInt(industry3Idx);
      }
    }
    if (lastIndustryColIdx < 0 && industry2Idx !== undefined) {
      const ind2 = norm(row[parseInt(industry2Idx)]);
      if (!isEmpty(ind2)) {
        lastIndustryColIdx = parseInt(industry2Idx);
      }
    }
    
    let postalFoundAt = -1; // 郵便番号が見つかった列のインデックス（グループ3用）
    const headerPostalIdx = headers.findIndex(h => h === "郵便番号");
    
    if (lastIndustryColIdx >= 0) {
      // 最後の業種の次の列から順に確認
      for (let i = lastIndustryColIdx + 1; i < row.length; i++) {
        const value = norm(row[i]);
        if (isEmpty(value)) continue;
        
        // 郵便番号パターンを検知したら終了
        if (normalizePostalCode(value)) {
          postalFoundAt = i;
          // 郵便番号が見つかったら、その列の値を郵便番号として設定
          const postal = normalizePostalCode(value);
          if (postal && !company.postalCode) {
            company.postalCode = postal;
          }
          break;
        }
        
        // 郵便番号でない場合は業種として扱う
        if (!POSTAL_CODE_PATTERN.test(value)) {
          otherIndustries.push(value);
        }
      }
    } else if (headerPostalIdx >= 0) {
      // 業種が見つからない場合、ヘッダーの郵便番号列から直接取得
      const postalValue = norm(row[headerPostalIdx]);
      const postal = normalizePostalCode(postalValue);
      if (postal) {
        postalFoundAt = headerPostalIdx;
        company.postalCode = postal;
      }
    }
    
    if (otherIndustries.length > 0) {
      company.industry = otherIndustries.join("、");
    }
    
    // グループ3の場合、郵便番号以降の列をヘッダー順で処理するためのオフセットを計算
    // ヘッダーの「郵便番号」列のインデックスと、実際の郵便番号が見つかった位置の差
    if (postalFoundAt >= 0 && headerPostalIdx >= 0) {
      columnOffset = postalFoundAt - headerPostalIdx;
    }
    
    // industries配列には業種1〜3（または業種4）と業種4以降を全て含める
    const industries: string[] = [];
    if (company.industryLarge) industries.push(company.industryLarge);
    if (company.industryMiddle) industries.push(company.industryMiddle);
    if (company.industrySmall) industries.push(company.industrySmall);
    if (company.industryDetail) industries.push(company.industryDetail);
    if (otherIndustries.length > 0) industries.push(...otherIndustries);
    if (industries.length > 0) company.industries = industries;
  } else if (groupNumber === 4 || groupNumber === 5) {
    // グループ4,5の業種マッピング
    // ヘッダーから業種1〜（細）を直接取得
    const industry1Idx = headerMap["業種1"];
    const industry2Idx = headerMap["業種2"];
    const industry3Idx = headerMap["業種3"];
    const industryDetailIdx = headerMap["業種（細）"];
    
    if (industry1Idx !== undefined) {
      const ind1 = norm(row[parseInt(industry1Idx)]);
      if (!isEmpty(ind1)) company.industryLarge = ind1;
    }
    if (industry2Idx !== undefined) {
      const ind2 = norm(row[parseInt(industry2Idx)]);
      if (!isEmpty(ind2)) company.industryMiddle = ind2;
    }
    if (industry3Idx !== undefined) {
      const ind3 = norm(row[parseInt(industry3Idx)]);
      if (!isEmpty(ind3)) company.industrySmall = ind3;
    }
    if (industryDetailIdx !== undefined) {
      const indDetail = norm(row[parseInt(industryDetailIdx)]);
      if (!isEmpty(indDetail)) company.industryDetail = indDetail;
    }
    
    // industry：業種5以降（ヘッダーにない業種を列順で検知）
    // 最後の業種（業種（細）→業種3→業種2の順で確認）の次の列から順に確認し、郵便番号が見つかるまで業種として扱う
    const otherIndustries: string[] = [];
    // 最後の業種列のインデックスを決定（業種（細）→業種3→業種2の順で確認）
    let lastIndustryColIdx = -1;
    if (industryDetailIdx !== undefined) {
      const indDetail = norm(row[parseInt(industryDetailIdx)]);
      if (!isEmpty(indDetail)) {
        lastIndustryColIdx = parseInt(industryDetailIdx);
      }
    }
    if (lastIndustryColIdx < 0 && industry3Idx !== undefined) {
      const ind3 = norm(row[parseInt(industry3Idx)]);
      if (!isEmpty(ind3)) {
        lastIndustryColIdx = parseInt(industry3Idx);
      }
    }
    if (lastIndustryColIdx < 0 && industry2Idx !== undefined) {
      const ind2 = norm(row[parseInt(industry2Idx)]);
      if (!isEmpty(ind2)) {
        lastIndustryColIdx = parseInt(industry2Idx);
      }
    }
    
    let postalFoundAt = -1; // 郵便番号が見つかった列のインデックス（グループ4用）
    const headerPostalIdx = headers.findIndex(h => h === "郵便番号");
    
    if (lastIndustryColIdx >= 0) {
      // 最後の業種の次の列から順に確認
      for (let i = lastIndustryColIdx + 1; i < row.length; i++) {
        const value = norm(row[i]);
        if (isEmpty(value)) continue;
        
        // 郵便番号パターンを検知したら終了
        if (normalizePostalCode(value)) {
          postalFoundAt = i;
          // 郵便番号が見つかったら、その列の値を郵便番号として設定
          const postal = normalizePostalCode(value);
          if (postal && !company.postalCode) {
            company.postalCode = postal;
          }
          break;
        }
        
        // 郵便番号でない場合は業種として扱う
        if (!POSTAL_CODE_PATTERN.test(value)) {
          otherIndustries.push(value);
        }
      }
    } else if (headerPostalIdx >= 0) {
      // 業種が見つからない場合、ヘッダーの郵便番号列から直接取得
      const postalValue = norm(row[headerPostalIdx]);
      const postal = normalizePostalCode(postalValue);
      if (postal) {
        postalFoundAt = headerPostalIdx;
        company.postalCode = postal;
      }
    }
    
    if (otherIndustries.length > 0) {
      company.industry = otherIndustries.join("、");
    }
    
    // グループ4,5の場合、郵便番号以降の列をヘッダー順で処理するためのオフセットを計算
    // ヘッダーの「郵便番号」列のインデックスと、実際の郵便番号が見つかった位置の差
    if (postalFoundAt >= 0 && headerPostalIdx >= 0) {
      columnOffset = postalFoundAt - headerPostalIdx;
    }
    
    // industries配列には業種1〜（細）と業種5以降を全て含める
    const industries: string[] = [];
    if (company.industryLarge) industries.push(company.industryLarge);
    if (company.industryMiddle) industries.push(company.industryMiddle);
    if (company.industrySmall) industries.push(company.industrySmall);
    if (company.industryDetail) industries.push(company.industryDetail);
    if (otherIndustries.length > 0) industries.push(...otherIndustries);
    if (industries.length > 0) company.industries = industries;
  } else {
    // 他のグループは従来通り
    if (industryData.industries.length > 0) {
      company.industries = industryData.industries;
      if (industryData.industries[0]) company.industryLarge = industryData.industries[0];
      if (industryData.industries[1]) company.industryMiddle = industryData.industries[1];
      if (industryData.industries[2]) company.industrySmall = industryData.industries[2];
      if (industryData.industries.length > 3) {
        company.industryDetail = industryData.industries.slice(3).join("、");
      }
    }
  }
  
  // グループ1,2,3,4,5,6,7の場合、列ずれを考慮してヘッダーに基づいて処理
  // columnOffsetが設定されている場合、ヘッダーインデックスにオフセットを加算
  const getColumnIndex = (headerName: string): number | undefined => {
    const headerIdx = headerMap[headerName];
    if (headerIdx === undefined) return undefined;
    const baseIdx = parseInt(headerIdx);
    if ((groupNumber === 1 || groupNumber === 2 || groupNumber === 3 || groupNumber === 4 || groupNumber === 5 || groupNumber === 6 || groupNumber === 7) && columnOffset !== 0) {
      // 郵便番号以降のフィールドはオフセットを適用
      const headerPostalIdx = headers.findIndex(h => h === "郵便番号");
      if (headerPostalIdx >= 0 && baseIdx >= headerPostalIdx) {
        return baseIdx + columnOffset;
      }
    }
    return baseIdx;
  };
  
  // 郵便番号（グループ1で既に設定されている場合はそのまま使用）
  if (!company.postalCode) {
    let postalIdx = getColumnIndex("郵便番号");
    if (postalIdx === undefined) {
      // 英語ヘッダーの場合
      const postalColIdx = headers.findIndex(h => norm(h) === "postalcode" || norm(h) === "postal");
      if (postalColIdx >= 0) postalIdx = postalColIdx;
    }
    if (postalIdx !== undefined) {
      const postalValue = norm(row[postalIdx]);
      const postal = normalizePostalCode(postalValue);
      if (postal) {
        company.postalCode = postal;
      } else if (industryData.postalCode) {
        company.postalCode = industryData.postalCode;
      }
    } else if (industryData.postalCode) {
      company.postalCode = industryData.postalCode;
    }
  }
  
  // 住所（ヘッダーに基づいて取得、オフセット適用）
  let addrIdx = getColumnIndex("住所");
  if (addrIdx === undefined) {
    // 英語ヘッダーの場合
    const addrColIdx = headers.findIndex(h => norm(h) === "address");
    if (addrColIdx >= 0) addrIdx = addrColIdx;
  }
  if (addrIdx !== undefined) {
    const addrValue = norm(row[addrIdx]);
    if (!isEmpty(addrValue)) {
      const postalInAddr = normalizePostalCode(addrValue);
      if (!postalInAddr) {
        company.address = addrValue;
        if (!company.prefecture) {
          const pref = extractPrefecture(addrValue);
          if (pref) company.prefecture = pref;
        }
      }
    }
  }
  
  // 設立（オフセット適用）
  let establishedIdx = getColumnIndex("設立");
  if (establishedIdx === undefined) {
    // 英語ヘッダーの場合
    const establishedColIdx = headers.findIndex(h => norm(h) === "established");
    if (establishedColIdx >= 0) establishedIdx = establishedColIdx;
  }
  if (establishedIdx !== undefined) {
    const established = norm(row[establishedIdx]);
    if (!isEmpty(established)) {
      company.established = established;
      const year = extractYear(established);
      if (year) company.foundingYear = year;
    }
  }
  
  // 電話番号(窓口)（オフセット適用）
  let phoneIdx = getColumnIndex("電話番号(窓口)");
  if (phoneIdx === undefined) {
    // 英語ヘッダーの場合
    const phoneColIdx = headers.findIndex(h => norm(h) === "phonenumber" || norm(h) === "phone");
    if (phoneColIdx >= 0) phoneIdx = phoneColIdx;
  }
  if (phoneIdx !== undefined) {
    const phone = norm(row[phoneIdx]);
    if (!isEmpty(phone)) company.phoneNumber = phone;
  }
  
  // 代表者郵便番号（オフセット適用）
  const repPostalIdx = getColumnIndex("代表者郵便番号");
  if (repPostalIdx !== undefined) {
    const repPostal = normalizePostalCode(row[repPostalIdx]);
    if (repPostal) company.representativePostalCode = repPostal;
  }
  
  // 代表者住所（オフセット適用）
  const repAddrIdx = getColumnIndex("代表者住所");
  if (repAddrIdx !== undefined) {
    const repAddr = norm(row[repAddrIdx]);
    if (!isEmpty(repAddr)) company.representativeHomeAddress = repAddr;
  }
  
  // 代表者誕生日（代表者名から抽出済みの場合はスキップ、オフセット適用）
  if (!company.representativeBirthDate) {
    const repBirthIdx = getColumnIndex("代表者誕生日");
    if (repBirthIdx !== undefined) {
      const repBirth = norm(row[repBirthIdx]);
      if (!isEmpty(repBirth)) company.representativeBirthDate = repBirth;
    }
  }
  
  // 資本金（グループ1,2,3,4,5,6,7は1000倍、オフセット適用、130.csvと131.csvは対象外）
  let capitalIdx = getColumnIndex("資本金");
  if (capitalIdx === undefined) {
    // 英語ヘッダーの場合
    const capitalColIdx = headers.findIndex(h => norm(h) === "capitalstock");
    if (capitalColIdx >= 0) capitalIdx = capitalColIdx;
  }
  if (capitalIdx !== undefined) {
    const capital = parseNumber(row[capitalIdx]);
    if (capital !== null) {
      // 130.csvと131.csvは1000倍しない（ファイル名で判定）
      const fileName = path.basename(csvFilePath || "");
      const is130or131 = fileName.includes("130") || fileName.includes("131");
      if (is130or131) {
        company.capitalStock = capital;
      } else {
        company.capitalStock = (groupNumber === 1 || groupNumber === 2 || groupNumber === 3 || groupNumber === 4 || groupNumber === 5 || groupNumber === 6 || groupNumber === 7) ? capital * 1000 : capital;
      }
    }
  }
  
  // 上場（オフセット適用）
  let listingIdx = getColumnIndex("上場");
  if (listingIdx === undefined) {
    // 英語ヘッダーの場合
    const listingColIdx = headers.findIndex(h => norm(h) === "listing");
    if (listingColIdx >= 0) listingIdx = listingColIdx;
  }
  if (listingIdx !== undefined) {
    const listing = norm(row[listingIdx]);
    if (!isEmpty(listing)) company.listing = listing;
  }
  
  // 直近決算年月（オフセット適用、グループ4では無視）
  if (groupNumber !== 4) {
    let fiscalIdx = getColumnIndex("直近決算年月");
    if (fiscalIdx === undefined) {
      // 英語ヘッダーの場合
      const fiscalColIdx = headers.findIndex(h => norm(h) === "fiscalmonth");
      if (fiscalColIdx >= 0) fiscalIdx = fiscalColIdx;
    }
    if (fiscalIdx !== undefined) {
      const fiscal = norm(row[fiscalIdx]);
      if (!isEmpty(fiscal)) {
        company.latestFiscalYearMonth = fiscal;
        const year = extractYear(fiscal);
        if (year) {
          const monthMatch = fiscal.match(/(\d{1,2})月/);
          if (monthMatch) {
            const month = parseInt(monthMatch[1]);
            if (month >= 1 && month <= 12) {
              company.fiscalMonth = month;
            }
          }
        }
      }
    }
  }
  
  // 直近売上（グループ1,2,3,4,5,6,7は1000倍、オフセット適用、130.csvと131.csvは対象外）
  let revenueIdx = getColumnIndex("直近売上");
  if (revenueIdx === undefined) {
    // 英語ヘッダーの場合
    const revenueColIdx = headers.findIndex(h => norm(h) === "revenue");
    if (revenueColIdx >= 0) revenueIdx = revenueColIdx;
  }
  if (revenueIdx !== undefined) {
    const revenue = parseNumber(row[revenueIdx]);
    if (revenue !== null) {
      // 130.csvと131.csvは1000倍しない（ファイル名で判定）
      const fileName = path.basename(csvFilePath || "");
      const is130or131 = fileName.includes("130") || fileName.includes("131");
      if (is130or131) {
        company.latestRevenue = revenue;
      } else {
        company.latestRevenue = (groupNumber === 1 || groupNumber === 2 || groupNumber === 3 || groupNumber === 4 || groupNumber === 5 || groupNumber === 6 || groupNumber === 7) ? revenue * 1000 : revenue;
      }
    }
  }
  
  // 直近利益（グループ1,2,3,4,5,6,7は1000倍、オフセット適用、130.csvと131.csvは対象外）
  let profitIdx = getColumnIndex("直近利益");
  if (profitIdx === undefined) {
    // 英語ヘッダーの場合
    const profitColIdx = headers.findIndex(h => norm(h) === "profit");
    if (profitColIdx >= 0) profitIdx = profitColIdx;
  }
  if (profitIdx !== undefined) {
    const profit = parseNumber(row[profitIdx]);
    if (profit !== null) {
      // 130.csvと131.csvは1000倍しない（ファイル名で判定）
      const fileName = path.basename(csvFilePath || "");
      const is130or131 = fileName.includes("130") || fileName.includes("131");
      if (is130or131) {
        company.latestProfit = profit;
      } else {
        company.latestProfit = (groupNumber === 1 || groupNumber === 2 || groupNumber === 3 || groupNumber === 4 || groupNumber === 5 || groupNumber === 6 || groupNumber === 7) ? profit * 1000 : profit;
      }
    }
  }
  
  // 説明（オフセット適用）
  const descIdx = getColumnIndex("説明");
  if (descIdx !== undefined) {
    const desc = norm(row[descIdx]);
    if (!isEmpty(desc)) company.companyDescription = desc;
  }
  
  // 概要（オフセット適用）
  const overviewIdx = getColumnIndex("概要");
  if (overviewIdx !== undefined) {
    const overview = norm(row[overviewIdx]);
    if (!isEmpty(overview)) company.overview = overview;
  }
  
  // 仕入れ先（オフセット適用）
  const supplierIdx = getColumnIndex("仕入れ先");
  if (supplierIdx !== undefined) {
    const supplier = norm(row[supplierIdx]);
    if (!isEmpty(supplier)) {
      company.suppliers = supplier.split(/[，,]/).map(s => norm(s)).filter(s => s);
    }
  }
  
  // 取引先（オフセット適用）
  const clientIdx = getColumnIndex("取引先");
  if (clientIdx !== undefined) {
    const client = norm(row[clientIdx]);
    if (!isEmpty(client)) company.clients = client;
  }
  
  // 取引先銀行（オフセット適用）
  const bankIdx = getColumnIndex("取引先銀行");
  if (bankIdx !== undefined) {
    const bank = norm(row[bankIdx]);
    if (!isEmpty(bank)) {
      company.banks = bank.split(/[，,]/).map(s => norm(s)).filter(s => s);
    }
  }
  
  // 取締役（オフセット適用）
  const execIdx = getColumnIndex("取締役");
  if (execIdx !== undefined) {
    const exec = norm(row[execIdx]);
    if (!isEmpty(exec)) company.executives = exec;
  }
  
  // 株主（オフセット適用）
  const shareholderIdx = getColumnIndex("株主");
  if (shareholderIdx !== undefined) {
    const shareholder = norm(row[shareholderIdx]);
    if (!isEmpty(shareholder)) company.shareholders = shareholder;
  }
  
  // 社員数（オフセット適用）
  let empIdx: number | undefined = getColumnIndex("社員数");
  if (empIdx === undefined) {
    // 英語ヘッダーの場合
    const empColIdx = headers.findIndex(h => norm(h) === "employeecount" || norm(h) === "employee");
    if (empColIdx >= 0) empIdx = empColIdx;
  }
  if (empIdx !== undefined) {
    const emp = parseNumber(row[empIdx]);
    if (emp !== null) company.employeeCount = emp;
  }
  
  // オフィス数（オフセット適用）
  const officeIdx = getColumnIndex("オフィス数");
  if (officeIdx !== undefined) {
    const office = parseNumber(row[officeIdx]);
    if (office !== null) company.officeCount = office;
  }
  
  // 工場数（オフセット適用）
  const factoryIdx = getColumnIndex("工場数");
  if (factoryIdx !== undefined) {
    const factory = parseNumber(row[factoryIdx]);
    if (factory !== null) company.factoryCount = factory;
  }
  
  // 店舗数（オフセット適用）
  const storeIdx = getColumnIndex("店舗数");
  if (storeIdx !== undefined) {
    const store = parseNumber(row[storeIdx]);
    if (store !== null) company.storeCount = store;
  }
  
  // 必須フィールドチェック
  if (!company.name) return null;
  
  // タイムスタンプ
  company.updatedAt = admin.firestore.FieldValue.serverTimestamp();
  company.createdAt = admin.firestore.FieldValue.serverTimestamp();
  
  return company;
}

// 数値IDを生成（法人番号がある場合はそれを使用）
function generateNumericId(corporateNumber: string | null | undefined): string {
  // 法人番号が存在し、13桁の数値の場合 → そのまま使用
  if (corporateNumber && /^\d{13}$/.test(corporateNumber.trim())) {
    return corporateNumber.trim();
  }
  
  // それ以外の場合 → タイムスタンプ + ランダム
  const timestamp = Date.now();
  const random = Math.floor(Math.random() * 10000);
  return `${timestamp}${random.toString().padStart(4, "0")}`;
}

// 既存ドキュメントをチェック（法人番号で検索）
async function findExistingCompany(
  companiesCol: admin.firestore.CollectionReference,
  corporateNumber: string | null | undefined,
  companyName: string | null | undefined
): Promise<admin.firestore.DocumentReference | null> {
  // 1. 法人番号で検索（最優先）
  if (corporateNumber && /^\d{13}$/.test(corporateNumber.trim())) {
    const normalizedCorpNum = corporateNumber.trim();
    
    // docId=法人番号で直接参照
    const directRef = companiesCol.doc(normalizedCorpNum);
    const directSnap = await directRef.get();
    if (directSnap.exists) {
      return directRef;
    }
    
    // corporateNumberフィールドで検索
    const snapByCorp = await companiesCol
      .where("corporateNumber", "==", normalizedCorpNum)
      .limit(1)
      .get();
    if (!snapByCorp.empty) {
      return snapByCorp.docs[0].ref;
    }
  }
  
  // 2. 企業名で検索（法人番号がない場合）
  if (companyName && companyName.trim()) {
    const snapByName = await companiesCol
      .where("name", "==", companyName.trim())
      .limit(1)
      .get();
    if (!snapByName.empty) {
      return snapByName.docs[0].ref;
    }
  }
  
  return null;
}

// CSVファイルを読み込む
function readCsvFile(filePath: string): { headers: string[]; rows: string[][] } {
  const buf = fs.readFileSync(filePath);
  const text = buf.toString("utf8");
  
  const parsed = Papa.parse<string[]>(text, {
    skipEmptyLines: true,
    header: false,
  });
  
  if (parsed.errors?.length) {
    console.warn(`[CSV] パース警告: ${parsed.errors.slice(0, 3).map(e => e.message).join(", ")}`);
  }
  
  const rows = parsed.data as string[][];
  if (rows.length === 0) {
    return { headers: [], rows: [] };
  }
  
  const headers = rows[0].map(h => norm(h));
  const dataRows = rows.slice(1);
  
  return { headers, rows: dataRows };
}

// CSVファイルからグループ番号を判定
function detectGroupNumber(csvFilePath: string): number {
  const fileName = path.basename(csvFilePath);
  
  // グループ1: 111-117.csv
  if (GROUP1_FILES.some(f => f.includes(fileName))) return 1;
  
  // グループ2: 118, 120-125.csv
  if (GROUP2_FILES.some(f => f.includes(fileName))) return 2;
  
  // グループ3: 38.csv
  if (GROUP3_FILES.some(f => f.includes(fileName))) return 3;
  
  // グループ4: 107-109.csv
  if (GROUP4_FILES.some(f => f.includes(fileName))) return 4;
  
  // グループ5: 110.csv
  if (GROUP5_FILES.some(f => f.includes(fileName))) return 5;
  
  // グループ6: 119.csv
  if (GROUP6_FILES.some(f => f.includes(fileName))) return 6;
  
  // グループ7: 122.csv
  if (GROUP7_FILES.some(f => f.includes(fileName))) return 7;
  
  // デフォルト: グループ1のルールを適用
  return 1;
}

// メイン処理
async function main() {
  initAdmin();
  const db = admin.firestore();
  const companiesCol = db.collection(COLLECTION_NAME);
  
  // コマンドライン引数からCSVファイルを取得
  const csvFileArg = process.argv[2];
  
  let targetFiles: string[] = [];
  
  if (csvFileArg) {
    // 指定されたCSVファイルを処理
    const fullPath = path.resolve(process.cwd(), csvFileArg);
    if (!fs.existsSync(fullPath)) {
      console.error(`❌ エラー: CSVファイルが見つかりません: ${fullPath}`);
      process.exit(1);
    }
    targetFiles = [fullPath];
  } else {
    // デフォルト: グループ1の残りのファイルを処理（112.csv〜117.csv）
    const remainingGroup1Files = GROUP1_FILES.slice(1); // 111.csvを除く
    targetFiles = remainingGroup1Files.map(f => path.resolve(process.cwd(), f));
  }
  
  const results: Record<string, { created: number; skipped: number; errors: number }> = {};
  const createdDocIds: string[] = [];
  let totalCreated = 0;
  let totalSkipped = 0;
  let totalErrors = 0;
  
  for (const filePath of targetFiles) {
    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️  ファイルが見つかりません: ${filePath}`);
      continue;
    }
    
    const groupNumber = detectGroupNumber(filePath);
    const groupName = `group${groupNumber}`;
    const fileName = path.basename(filePath);
    
    console.log(`\n📁 処理中: ${groupName} (${fileName})`);
    console.log(`   モード: ${TEST_MODE ? `テスト（${TEST_LIMIT}件のみ）` : '全量'}`);
    
    const { headers, rows } = readCsvFile(filePath);
    if (headers.length === 0 || rows.length === 0) {
      console.warn(`  ⚠️  データがありません`);
      continue;
    }
    
    let created = 0;
    let skipped = 0;
    let errors = 0;
    
    // テストモードの場合は件数制限
    const targetRows = TEST_MODE ? rows.slice(0, TEST_LIMIT) : rows;
    
    if (TEST_MODE && rows.length > TEST_LIMIT) {
      console.log(`   ⚠️  テストモード: ${rows.length}件中${TEST_LIMIT}件のみ処理します`);
    }
    
    for (let i = 0; i < targetRows.length; i++) {
      const row = targetRows[i];
      // 実際のCSV行番号を計算（ヘッダー行を除くため+2）
      const actualRowNumber = i + 2;
      try {
        const company = convertRowToCompany(row, headers, groupNumber, filePath);
        
        if (!company) {
          console.log(`  ⏭️  行 ${actualRowNumber}: スキップ（必須フィールド不足）`);
          skipped++;
          continue;
        }
        
        // 既存ドキュメントをチェック
        const existingRef = await findExistingCompany(
          companiesCol,
          company.corporateNumber,
          company.name
        );
        
        if (existingRef) {
          // 既存ドキュメントが見つかった場合はスキップ
          console.log(`  ⏭️  行 ${actualRowNumber}: ${company.name} (既存のためスキップ)`);
          skipped++;
          continue;
        }
        
        if (DRY_RUN) {
          console.log(`  🔍 行 ${actualRowNumber}: ${company.name || "(名前なし)"}`);
          console.log(`     フィールド数: ${Object.keys(company).length}`);
          if (company.address) {
            console.log(`     address: ${company.address.substring(0, 100)}${company.address.length > 100 ? "..." : ""}`);
          }
          created++;
        } else {
          const docId = generateNumericId(company.corporateNumber);
          await companiesCol.doc(docId).set(company);
          createdDocIds.push(docId);
          console.log(`  ✅ 行 ${actualRowNumber}: ${company.name} (ID: ${docId})`);
          if (company.address && company.address.length > 100) {
            console.log(`     ⚠️  addressが長すぎます: ${company.address.length}文字`);
          }
          created++;
        }
      } catch (error) {
        console.error(`  ❌ 行 ${actualRowNumber}: エラー - ${(error as Error).message}`);
        errors++;
      }
    }
    
    totalCreated += created;
    totalSkipped += skipped;
    totalErrors += errors;
    
    results[fileName] = { created, skipped, errors };
  }
  
  // 結果サマリー
  console.log("\n" + "=".repeat(60));
  console.log("📊 処理結果サマリー");
  console.log("=".repeat(60));
  
  for (const [groupName, result] of Object.entries(results)) {
    console.log(`${groupName}:`);
    console.log(`  作成: ${result.created}, スキップ: ${result.skipped}, エラー: ${result.errors}`);
  }
  
  if (!DRY_RUN && createdDocIds.length > 0) {
    const outputFile = path.resolve(
      process.cwd(),
      `created_test_companies_${Date.now()}.txt`
    );
    fs.writeFileSync(outputFile, createdDocIds.join("\n"), "utf8");
    console.log(`\n📝 作成されたドキュメントID: ${outputFile}`);
  }
  
  console.log("\n✅ 処理完了");
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});

