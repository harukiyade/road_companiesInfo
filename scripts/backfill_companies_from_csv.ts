/* 
  CSV と companies_new コレクションを突合して、
  CSV 側の内容が Firestore に正しく入るようにバックフィルするスクリプト（推測版）

  ・法人番号 (corporateNumber / 法人番号) をキーに既存ドキュメントを特定
  ・法人番号が無い／一致しない場合は「企業名＋都道府県＋住所＋郵便番号＋電話番号＋URL など」で該当企業をスコアリングして特定
  ・name は CSV を優先して「常に上書き」
  ・その他の項目は「Firestore が null/空 の場合のみ CSV で補完」
  ・CSV ヘッダー名 + 値のパターンを見て、companies_new のフィールドを「それっぽく」推測してマッピング

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/backfill_companies_from_csv.ts [--dry-run] [csvファイル or ディレクトリ...]

    例:
      # ./csv 配下の *.csv を DRY RUN（書き込みなし）で確認
      npx ts-node scripts/backfill_companies_from_csv.ts --dry-run

      # ./csv 配下の *.csv を実際に更新
      npx ts-node scripts/backfill_companies_from_csv.ts

      # 特定ファイルだけ対象にする
      npx ts-node scripts/backfill_companies_from_csv.ts ./csv/135.csv ./csv/136.csv
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type {
  Firestore,
  CollectionReference,
  DocumentReference,
} from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";
import axios from "axios";
import * as cheerio from "cheerio";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// ==============================
// companies_new のフィールド一覧（テンプレート）
// ==============================
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
  dateOfEstablishment: null,
  demandProducts: null,
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
  profit1: null,
  profit2: null,
  profit3: null,
  profit4: null,
  profit5: null,
  linkedin: null,
  listing: null,
  location: null,
  marketSegment: null,
  netAssets: null,
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

const NUMERIC_FIELDS = new Set<string>([
  "capitalStock",
  "employeeCount",
  "employeeNumber",
  "numberOfActivity",
  "revenue",
  "revenueFromStatements",
  "revenue1",
  "revenue2",
  "revenue3",
  "revenue4",
  "revenue5",
  "latestRevenue",
  "latestProfit",
  "profit1",
  "profit2",
  "profit3",
  "profit4",
  "profit5",
  "issuedShares",
  "totalAssets",
  "totalLiabilities",
  "netAssets",
  "operatingIncome",
  "factoryCount",
  "officeCount",
  "storeCount",
  "changeCount",
  "updateCount",
]);

// Firestoreの1MB制限を考慮した、各フィールドの最大文字数制限
// UTF-8エンコーディングで1文字約3バイトとして計算（安全マージン込み）
const FIELD_MAX_LENGTHS: Record<string, number> = {
  shareholders: 100000,      // 約300KB
  executives: 100000,         // 約300KB
  overview: 200000,           // 約600KB
  companyDescription: 200000,  // 約600KB
  businessDescriptions: 50000, // 約150KB
  address: 5000,
  representativeHomeAddress: 5000,
  name: 500,
  representativeName: 200,
};

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

// ==============================
// ヘルパー
// ==============================
type CsvRow = Record<string, string>;

function trim(v?: string | null): string | null {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

// 「（株）」を「株式会社」に変換（前株・後株を判定）
function normalizeCompanyNameFormat(name: string | null | undefined): string | null {
  if (!name) return null;
  const trimmed = String(name).trim();
  if (!trimmed) return null;

  // 「（株）」を検出
  if (trimmed.includes("（株）")) {
    // 前株: 「（株）○○」→ 「株式会社○○」
    if (trimmed.startsWith("（株）")) {
      return "株式会社" + trimmed.substring(3);
    }
    // 後株: 「○○（株）」→ 「○○株式会社」
    if (trimmed.endsWith("（株）")) {
      return trimmed.substring(0, trimmed.length - 3) + "株式会社";
    }
    // 中間にある場合も後株として処理
    const index = trimmed.indexOf("（株）");
    if (index > 0) {
      return trimmed.substring(0, index) + "株式会社" + trimmed.substring(index + 3);
    }
  }

  return trimmed;
}

// JSONから企業名を抽出
function extractCompanyNameFromJson(jsonStr: string | null | undefined): string | null {
  if (!jsonStr) return null;
  
  try {
    // 文字列がJSON形式かチェック
    let parsed: any;
    if (typeof jsonStr === "string") {
      parsed = JSON.parse(jsonStr);
    } else {
      parsed = jsonStr;
    }

    // 企業サマリから企業名を抽出
    if (parsed?.企業サマリ?.kv?.会社名) {
      return normalizeCompanyNameFormat(parsed.企業サマリ.kv.会社名);
    }
    if (parsed?.企業サマリ?.kv?.name) {
      return normalizeCompanyNameFormat(parsed.企業サマリ.kv.name);
    }
    if (parsed?.会社名) {
      return normalizeCompanyNameFormat(parsed.会社名);
    }
    if (parsed?.name) {
      return normalizeCompanyNameFormat(parsed.name);
    }
  } catch (e) {
    // JSONパースエラーは無視
  }

  return null;
}

// JSONからフィールド情報を抽出してマッピング
function extractFieldsFromJson(jsonStr: string | null | undefined, filePath: string = ""): Record<string, any> {
  const result: Record<string, any> = {};
  if (!jsonStr) return result;

  try {
    let parsed: any;
    if (typeof jsonStr === "string") {
      parsed = JSON.parse(jsonStr);
    } else {
      parsed = jsonStr;
    }

    const kv = parsed?.企業サマリ?.kv;
    if (!kv) return result;

    // 各フィールドをマッピング
    if (kv.会社名) {
      result.name = normalizeCompanyNameFormat(kv.会社名);
    }
    if (kv.英文名) {
      result.nameEn = trim(kv.英文名);
    }
    if (kv.法人番号) {
      const validated = validateCorporateNumber(kv.法人番号);
      if (validated) result.corporateNumber = validated;
    }
    if (kv.本社住所) {
      result.address = trim(kv.本社住所);
    }
    if (kv.業種) {
      result.industry = trim(kv.業種);
    }
    if (kv.資本金) {
      // タイプGのJSONは実値の可能性が高いため、単位変換は適用しない
      const num = parseFinancialNumeric(kv.資本金, "type_g", filePath, "capitalStock");
      if (num !== null) result.capitalStock = num;
    }
    if (kv.売上高 || kv["売上高（単独）"]) {
      // タイプGのJSONは実値の可能性が高いため、単位変換は適用しない
      const num = parseFinancialNumeric(kv.売上高 || kv["売上高（単独）"], "type_g", filePath, "revenue");
      if (num !== null) result.revenue = num;
    }
    if (kv.従業員数) {
      const num = parseNumeric(kv.従業員数);
      if (num !== null) result.employeeCount = num;
    }
    if (kv.設立年月日) {
      result.established = trim(kv.設立年月日);
    }
    if (kv.決算月) {
      result.fiscalMonth = trim(kv.決算月);
    }
    if (kv.代表者名) {
      processRepresentativeName(kv.代表者名, result);
    }
    if (kv.事業内容) {
      result.businessDescriptions = trim(kv.事業内容);
    }
    if (kv.URL) {
      result.companyUrl = trim(kv.URL);
    }
    if (kv.所属団体) {
      result.affiliations = trim(kv.所属団体);
    }
  } catch (e) {
    // JSONパースエラーは無視
  }

  return result;
}

// フィールド内から企業名を抽出（「日経バリューサーチ」以外の値から）
function extractCompanyNameFromFields(data: Record<string, any>): string | null {
  // 優先順位: overview > companyDescription > businessDescriptions > address
  const fields = ["overview", "companyDescription", "businessDescriptions", "address", "representativeName"];
  
  for (const field of fields) {
    const value = data[field];
    if (!value || typeof value !== "string") continue;
    
    // 「日経バリューサーチ」を含む場合はスキップ
    if (value.includes("日経バリューサーチ")) continue;
    
    // 企業名っぽい文字列を抽出（「株式会社」を含む、または短い文字列）
    const lines = value.split(/\n|。/);
    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed.length > 2 && trimmed.length < 50) {
        if (trimmed.includes("株式会社") || trimmed.includes("（株）") || trimmed.includes("有限会社")) {
          return normalizeCompanyNameFormat(trimmed);
        }
      }
    }
  }

  return null;
}

// 企業HPから企業名を取得（Webスクレイピング）
async function extractCompanyNameFromUrl(url: string | null | undefined): Promise<string | null> {
  if (!url) return null;
  
  try {
    // URLを正規化
    let normalizedUrl = url.trim();
    if (!normalizedUrl.startsWith("http://") && !normalizedUrl.startsWith("https://")) {
      normalizedUrl = "https://" + normalizedUrl;
    }

    const urlObj = new URL(normalizedUrl);
    
    // タイムアウトを5秒に設定
    const response = await axios.get(normalizedUrl, {
      timeout: 5000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      },
      maxRedirects: 5,
      validateStatus: (status) => status < 500 // 500未満のステータスコードを許可
    });

    if (response.status !== 200) {
      // ステータスコードが200でない場合は、ドメイン名から推測
      return extractCompanyNameFromDomain(urlObj.hostname);
    }

    const $ = cheerio.load(response.data);
    
    // 1. <title>タグから企業名を抽出
    const title = $("title").text().trim();
    if (title) {
      // 「株式会社」を含む場合は抽出
      if (title.includes("株式会社") || title.includes("（株）")) {
        // より厳密なパターンマッチング（短い文字列を優先）
        const patterns = [
          /([^|｜\-–—\s]{2,30}(?:株式会社|（株）)[^|｜\-–—\s]{0,20})/,  // 短いパターン
          /([^|｜\-–—\s]+(?:株式会社|（株）)[^|｜\-–—\s]*)/  // 長いパターン
        ];
        
        for (const pattern of patterns) {
          const match = title.match(pattern);
          if (match && match[1] && match[1].length <= 50) {
            const extracted = normalizeCompanyNameFormat(match[1]);
            if (extracted && extracted.length <= 50) {
              return extracted;
            }
          }
        }
      }
    }

    // 2. <h1>タグから企業名を抽出
    const h1 = $("h1").first().text().trim();
    if (h1 && (h1.includes("株式会社") || h1.includes("（株）"))) {
      return normalizeCompanyNameFormat(h1);
    }

    // 3. meta property="og:site_name" から企業名を抽出
    const ogSiteName = $('meta[property="og:site_name"]').attr("content");
    if (ogSiteName && (ogSiteName.includes("株式会社") || ogSiteName.includes("（株）"))) {
      return normalizeCompanyNameFormat(ogSiteName);
    }

    // 4. meta name="description" の前後から企業名を抽出
    const description = $('meta[name="description"]').attr("content");
    if (description) {
      const descMatch = description.match(/([^。\s]+(?:株式会社|（株）)[^。\s]*)/);
      if (descMatch) {
        return normalizeCompanyNameFormat(descMatch[1]);
      }
    }

    // 5. ページ内のテキストから「株式会社」を含む最初の文字列を抽出（短いものを優先）
    const bodyText = $("body").text();
    // より厳密なパターン（50文字以内）
    const companyMatch = bodyText.match(/([^。\n\s]{2,30}(?:株式会社|（株）)[^。\n\s]{0,20})/);
    if (companyMatch && companyMatch[1] && companyMatch[1].length <= 50) {
      const extracted = normalizeCompanyNameFormat(companyMatch[1]);
      if (extracted && extracted.length <= 50) {
        return extracted;
      }
    }

    // 6. 上記で見つからない場合は、ドメイン名から推測
    return extractCompanyNameFromDomain(urlObj.hostname);
  } catch (e: any) {
    // エラーが発生した場合は、ドメイン名から推測
    try {
      const urlObj = new URL(url.startsWith("http") ? url : `https://${url}`);
      return extractCompanyNameFromDomain(urlObj.hostname);
    } catch {
      return null;
    }
  }
}

// ドメイン名から企業名を推測
function extractCompanyNameFromDomain(hostname: string): string | null {
  if (!hostname) return null;
  
  // ドメイン名から企業名を推測（例: example.co.jp → example）
  const parts = hostname.split(".");
  if (parts.length > 0) {
    let mainPart = parts[0];
    if (mainPart === "www" && parts.length > 1) {
      mainPart = parts[1];
    }
    
    if (mainPart && mainPart.length > 2) {
      // 簡易的な企業名として返す
      return mainPart;
    }
  }
  
  return null;
}

// フィールドの最大長を制限する関数（Firestoreの1MB制限対策）
function truncateFieldValue(field: string, value: string): string {
  const maxLength = FIELD_MAX_LENGTHS[field];
  if (maxLength && value.length > maxLength) {
    const truncated = value.substring(0, maxLength);
    console.warn(
      `⚠️  [${field}] フィールドが長すぎるため切り詰めました: ${value.length}文字 → ${truncated.length}文字`
    );
    return truncated;
  }
  return value;
}

function parseNumeric(v: string): number | null {
  const cleaned = v.replace(/[^\d.-]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

// CSVタイプが百万単位かどうかを判定する関数
function isCsvTypeInMillions(csvType: string | null | undefined, filePath: string): boolean {
  // タイプC（105.csv）には財務情報が含まれていないため、百万単位変換は不要
  // タイプEとFは別プロジェクトで実行中のため、ここでは変換しない
  // タイプJは千円単位として処理（別関数で判定）
  // その他のタイプは実値として処理
  return false;
}

// タイプJ（133.csv, 134.csv, 135.csv, 136.csv）は千円単位
function isCsvTypeInThousands(csvType: string | null | undefined, filePath: string): boolean {
  if (isTypeJCSV(filePath)) {
    return true;
  }
  // csvTypeが設定されている場合も判定（タイプJのみ）
  if (csvType === "type_j") {
    return true;
  }
  return false;
}

// 財務数値を実値に変換する関数
// 百万単位の場合は1000000を掛ける、千円単位の場合は1000を掛ける
function parseFinancialNumeric(
  v: string,
  csvType: string | null | undefined,
  filePath: string,
  fieldName: string
): number | null {
  const num = parseNumeric(v);
  if (num === null) return null;
  
  // 財務数値フィールドのみ単位変換を適用
  const financialFields = [
    "capitalStock",
    "revenue",
    "revenueFromStatements",
    "revenue1",
    "revenue2",
    "revenue3",
    "revenue4",
    "revenue5",
    "latestRevenue",
    "latestProfit",
    "profit1",
    "profit2",
    "profit3",
    "profit4",
    "profit5",
    "totalAssets",
    "totalLiabilities",
    "netAssets",
    "operatingIncome"
  ];
  
  if (financialFields.includes(fieldName)) {
    // 百万単位のCSVタイプの場合は1000000を掛けて実値に変換
    if (isCsvTypeInMillions(csvType, filePath)) {
      return num * 1000000;
    }
    // 千円単位のCSVタイプ（タイプJ）の場合は1000を掛けて実値に変換
    if (isCsvTypeInThousands(csvType, filePath)) {
      return num * 1000;
    }
  }
  
  return num;
}

function isCsvFile(p: string): boolean {
  return p.toLowerCase().endsWith(".csv");
}

function normalizeStr(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "");
}

// 数字だけ抜き出す（郵便番号・電話番号など）
function digitsOnly(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).replace(/\D/g, "");
}

// 値が空かどうかを判定
function isEmptyValue(v: any): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  if (Array.isArray(v) && v.length === 0) return true;
  return false;
}

// tagsから不要なヘッダー内容を削除する関数
function cleanTags(tags: any): any[] {
  if (!Array.isArray(tags)) {
    return [];
  }
  
  // 削除対象のヘッダー名
  const headersToRemove = ["取引種別", "SBフラグ", "NDA", "AD", "ステータス"];
  
  return tags.filter((tag: any) => {
    if (typeof tag !== "string") {
      return true; // 文字列以外は保持
    }
    
    const tagLower = tag.toLowerCase();
    // ヘッダー名が含まれているか、またはヘッダー名と完全一致する場合は削除
    for (const header of headersToRemove) {
      if (tag === header || tagLower === header.toLowerCase() || tag.includes(header)) {
        return false;
      }
    }
    
    return true;
  });
}

// URL からホスト名だけ取り出して比較しやすくする
function normalizeUrlHost(v: string | null | undefined): string {
  if (!v) return "";
  let s = String(v).trim();
  if (!s) return "";
  if (!/^https?:\/\//i.test(s)) {
    s = "https://" + s;
  }
  try {
    const u = new URL(s);
    return u.hostname.toLowerCase();
  } catch {
    return "";
  }
}

// ドキュメントIDを数字のみの文字列に統一する（既存の import_companies_from_csv.ts と同じ形式）
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

function collectCsvFiles(): string[] {
  const args = process.argv.slice(2).filter((a) => a !== "--dry-run");

  if (args.length === 0) {
    const defaultDir = path.resolve("./csv");
    if (!fs.existsSync(defaultDir)) {
      console.error(
        '❌ エラー: "./csv" ディレクトリが存在しません。引数で CSV ファイル or ディレクトリを指定してください。'
      );
      process.exit(1);
    }
    const files = fs
      .readdirSync(defaultDir)
      .filter((f) => isCsvFile(f))
      .map((f) => path.join(defaultDir, f));

    if (files.length === 0) {
      console.error("❌ エラー: ./csv 配下に CSV ファイルが見つかりませんでした");
      process.exit(1);
    }

    console.log(`📂 ./csv から CSV ファイルを検出: ${files.length} 件`);
    return files;
  }

  const result: string[] = [];
  for (const arg of args) {
    const resolved = path.resolve(arg);
    if (!fs.existsSync(resolved)) {
      console.warn(`⚠️ 指定パスが存在しません: ${resolved} (スキップ)`);
      continue;
    }
    const stat = fs.statSync(resolved);
    if (stat.isDirectory()) {
      const files = fs
        .readdirSync(resolved)
        .filter((f) => isCsvFile(f))
        .map((f) => path.join(resolved, f));
      result.push(...files);
    } else if (stat.isFile() && isCsvFile(resolved)) {
      result.push(resolved);
    }
  }

  if (result.length === 0) {
    console.error("❌ エラー: 有効な CSV ファイルが見つかりませんでした");
    process.exit(1);
  }

  console.log(`📂 指定されたパスから CSV ファイルを検出: ${result.length} 件`);
  return result;
}

// タイプEのCSVファイルを識別
function isTypeECSV(filePath: string): boolean {
  // タイプE形式のファイルを判定
  // ヘッダーが「会社名,都道府県,代表者名...」で始まり、業種1,業種2,業種3...の順序を持つ
  const typeEFiles = [
    "csv/107.csv", "csv/108.csv", "csv/109.csv", "csv/110.csv",
    "csv/111.csv", "csv/112.csv", "csv/113.csv", "csv/114.csv",
    "csv/115.csv", "csv/116.csv", "csv/117.csv", "csv/118.csv",
    "csv/122.csv", "csv/24.csv",
    "csv/40.csv", "csv/41.csv", "csv/42.csv", "csv/48.csv", "csv/50.csv"
    // 注意: 133.csv, 134.csv, 135.csv, 136.csvはタイプJとして別途処理
  ];
  return typeEFiles.some(f => filePath.endsWith(f));
}

// タイプCのCSVファイルを識別
function isTypeCCSV(filePath: string): boolean {
  const typeCFiles = ["csv/105.csv"];
  return typeCFiles.some(f => filePath.endsWith(f));
}

// タイプFのCSVファイルを識別
function isTypeFCSV(filePath: string): boolean {
  const typeFFiles = ["csv/124.csv", "csv/125.csv", "csv/126.csv"];
  return typeFFiles.some(f => filePath.endsWith(f));
}

// タイプGのCSVファイルを識別
function isTypeGCSV(filePath: string): boolean {
  const typeGFiles = ["csv/127.csv", "csv/128.csv"];
  return typeGFiles.some(f => filePath.endsWith(f));
}

// タイプJのCSVファイルを識別
function isTypeJCSV(filePath: string): boolean {
  const typeJFiles = ["csv/133.csv", "csv/134.csv", "csv/135.csv", "csv/136.csv"];
  return typeJFiles.some(f => filePath.endsWith(f));
}

// 値がJSON形式かどうかを判定
function isJsonValue(value: any): boolean {
  if (value === null || value === undefined) return false;
  
  // 文字列の場合、JSON形式かどうかを判定
  if (typeof value === "string") {
    const trimmed = value.trim();
    // JSON形式の文字列（{...} または [...] で始まる）
    if ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
        (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
      try {
        JSON.parse(trimmed);
        return true;
      } catch {
        return false;
      }
    }
    return false;
  }
  
  // オブジェクトまたは配列の場合
  if (typeof value === "object") {
    return Array.isArray(value) || (value.constructor === Object);
  }
  
  return false;
}

// 値が数値かどうかを判定（郵便番号判定用）
function isNumericValue(value: string | null | undefined): boolean {
  if (!value) return false;
  const trimmed = String(value).trim();
  if (!trimmed) return false;
  
  // 数値のみ（カンマやハイフンを含む可能性がある）
  const cleaned = trimmed.replace(/[,\-\s]/g, "");
  return /^\d+$/.test(cleaned) && cleaned.length > 0;
}

// 郵便番号を検証（7桁の数値でない場合はnull）
// 注: ユーザーは「13桁の数値でないときはnull」と言っていますが、実際の郵便番号は7桁です
// ここでは実際の郵便番号形式（XXX-XXXX）を検証します
function validatePostalCode(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  
  // 郵便番号形式（XXX-XXXX）を検証
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 7) {
    // 7桁の数字の場合、XXX-XXXX形式に変換
    return digits.replace(/(\d{3})(\d{4})/, "$1-$2");
  }
  
  // 7桁でない場合はnull
  return null;
}

// タイプEのCSVを行配列として読み込む（列インデックスベース）
function loadTypeECSVByIndex(csvFilePath: string): Array<Array<string>> {
  const buf = fs.readFileSync(csvFilePath);
  try {
    const records: Array<Array<string>> = parse(buf, {
      columns: false,  // ヘッダーを無視して配列として読み込む
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      skip_records_with_error: true,
    });
    console.log(`  📄 ${path.basename(csvFilePath)}: ${records.length} 行（タイプE: 列順序ベース）`);
    return records;
  } catch (err: any) {
    console.warn(`  ⚠️ ${path.basename(csvFilePath)}: CSVパースエラー - ${err.code || err.message}`);
    return [];
  }
}

// ヘッダー行からURL列のインデックスを検出
function findUrlColumnIndex(headerRow: Array<string>): number | null {
  for (let i = 0; i < headerRow.length; i++) {
    const cellValue = headerRow[i];
    if (!cellValue) continue;
    const trimmed = trim(cellValue);
    if (!trimmed) continue;
    const header = trimmed.toLowerCase();
    if (header === "url" || header === "企業url" || header === "会社url" || header === "hp" || header === "hpurl") {
      return i;
    }
  }
  return null;
}

// 代表者名から個人名のみを抽出する関数
function extractPersonNameFromRepresentative(representativeName: string | null | undefined): string | null {
  if (!representativeName) return null;
  
  let trimmed = trim(representativeName);
  if (!trimmed) return null;
  
  // 役職名を除去（代表取締役、社長、代表など）
  const titles = [
    "代表取締役",
    "代表取締役社長",
    "代表取締役会長",
    "代表取締役専務",
    "代表取締役常務",
    "代表取締役副社長",
    "取締役社長",
    "取締役会長",
    "社長",
    "会長",
    "専務",
    "常務",
    "副社長",
    "代表",
    "代表者",
    "CEO",
    "ceo"
  ];
  
  // 役職名で始まる場合は除去
  for (const title of titles) {
    if (trimmed.startsWith(title)) {
      trimmed = trimmed.substring(title.length).trim();
      // スペースや記号を除去
      trimmed = trimmed.replace(/^[\s・、,，]/g, "").trim();
      break;
    }
    // 役職名が含まれている場合（前後にスペースがある）
    const titlePattern = new RegExp(`^${title}[\\s・、,，]`, "i");
    if (titlePattern.test(trimmed)) {
      trimmed = trimmed.replace(titlePattern, "").trim();
      break;
    }
  }
  
  // カッコ内の情報を除去（例: 山田太郎（代表取締役））
  trimmed = trimmed.replace(/[（(].*?[）)]/g, "").trim();
  
  // 生年月日パターンを除去（例: 1965/12/27、1965-12-27など）
  // 1900-2100年の範囲の生年月日パターンのみを除去
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})[\/年-]\d{1,2}[\/月-]\d{1,2}/g, "").trim();
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})\/\d{1,2}\/\d{1,2}/g, "").trim();
  
  // 数字や記号のみの場合はnull
  if (/^[\d\s\-・、,，.。]+$/.test(trimmed)) {
    return null;
  }
  
  // 空でない場合は返す
  return trimmed || null;
}

// 代表者名から生年月日を抽出
function extractBirthDateFromRepresentativeName(representativeName: string | null | undefined): string | null {
  if (!representativeName || typeof representativeName !== "string") return null;
  
  const trimmed = representativeName.trim();
  if (!trimmed) return null;
  
  // 生年月日パターン（1900-2100年の範囲）
  const birthdatePatterns = [
    /(19\d{2}|20\d{2})[\/年-](\d{1,2})[\/月-](\d{1,2})/g,  // 1977/1/1, 1977-1-1, 1977年1月1日
    /(19\d{2}|20\d{2})\/(\d{1,2})\/(\d{1,2})/g,            // 1977/1/1
  ];
  
  for (const pattern of birthdatePatterns) {
    const match = trimmed.match(pattern);
    if (match) {
      const dateStr = match[0];
      const parts = dateStr.split(/[\/年-]/);
      if (parts.length >= 3) {
        const year = parseInt(parts[0]);
        const month = parseInt(parts[1]);
        const day = parseInt(parts[2]);
        
        // 有効な生年月日かチェック
        if (year >= 1900 && year <= 2100 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
          return dateStr;
        }
      }
    }
  }
  
  return null;
}

// 代表者名を処理して、個人名と生年月日を分離
function processRepresentativeName(representativeName: string | null | undefined, mapped: Record<string, any>): void {
  if (!representativeName || typeof representativeName !== "string") return;
  
  const trimmed = representativeName.trim();
  if (!trimmed) return;
  
  // 生年月日を抽出
  const birthDate = extractBirthDateFromRepresentativeName(trimmed);
  if (birthDate && !mapped.representativeBirthDate) {
    mapped.representativeBirthDate = birthDate;
  }
  
  // 個人名（氏名）のみを抽出
  const personName = extractPersonNameFromRepresentative(trimmed);
  if (personName) {
    mapped.representativeName = personName;
  } else {
    // 個人名として抽出できなかった場合、生年月日を除去した値を使用
    let cleaned = trimmed;
    if (birthDate) {
      cleaned = cleaned.replace(birthDate, "").trim();
      cleaned = cleaned.replace(/^[\s・、,，\-]/g, "").replace(/[\s・、,，\-]$/g, "").trim();
    }
    if (cleaned && cleaned.length > 0) {
      mapped.representativeName = cleaned;
    }
  }
}

// タイプEの行データを列インデックスに基づいてマッピング
function mapTypeERowByIndex(row: Array<string>, urlColumnIndex: number | null = null, filePath: string = ""): Record<string, any> {
  const mapped: Record<string, any> = {};
  let colIndex = 0;
  
  // 1. 会社名
  if (row[colIndex]) mapped.name = trim(row[colIndex]);
  colIndex++;
  
  // 2. 都道府県
  if (row[colIndex]) mapped.prefecture = trim(row[colIndex]);
  colIndex++;
  
  // 3. 代表者名（個人名のみを抽出）
  if (row[colIndex]) {
    const personName = extractPersonNameFromRepresentative(row[colIndex]);
    if (personName) {
      mapped.representativeName = personName;
    }
  }
  colIndex++;
  
  // 4. 法人番号（存在する場合）
  // URL列の位置でない場合のみ、法人番号として処理を試みる
  if (urlColumnIndex === null || colIndex !== urlColumnIndex) {
    if (row[colIndex]) {
      const validated = validateCorporateNumber(row[colIndex]);
      if (validated) {
        mapped.corporateNumber = validated;
      }
    }
    colIndex++;
  }
  
  // 5. URL（ヘッダーから検出した位置を使用）
  if (urlColumnIndex !== null) {
    // ヘッダーから検出したURL列の位置から直接取得
    if (row[urlColumnIndex]) {
      const urlValue = trim(row[urlColumnIndex]);
      if (urlValue) {
        mapped.companyUrl = urlValue;
      }
    }
    // colIndexをURL列の次の位置に調整
    if (colIndex <= urlColumnIndex) {
      colIndex = urlColumnIndex + 1;
    }
  } else {
    // ヘッダーから検出できなかった場合、従来の位置（colIndex）を使用
    if (row[colIndex]) {
      const urlValue = trim(row[colIndex]);
      if (urlValue) {
        mapped.companyUrl = urlValue;
      }
    }
    colIndex++;
  }
  
  // 6. 業種1
  if (row[colIndex]) mapped.industryLarge = trim(row[colIndex]);
  colIndex++;
  
  // 7. 業種2
  if (row[colIndex]) mapped.industryMiddle = trim(row[colIndex]);
  colIndex++;
  
  // 8. 業種3
  if (row[colIndex]) mapped.industrySmall = trim(row[colIndex]);
  colIndex++;
  
  // 9. 業種4（空の場合は空のまま）
  if (row[colIndex]) {
    const industry4Value = trim(row[colIndex]);
    if (industry4Value) {
      mapped.industryDetail = industry4Value;
    }
  }
  colIndex++;
  
  // 10-11. 業種5・業種6の処理（動的判定）
  // industryCategoriesを初期化
  mapped.industryCategories = [];
  
  // 業種5の位置をチェック（colIndex=9が郵便番号の位置）
  // 業種は文字列、郵便番号は3桁-4桁の数値形式なので、その形式で判断
  const industry5Value = row[colIndex] ? trim(row[colIndex]) : null;
  
  if (industry5Value) {
    // 郵便番号の形式かどうかをチェック（3桁-4桁の数値形式）
    const postalCode = validatePostalCode(industry5Value);
    if (postalCode) {
      // 郵便番号の形式 = 業種5と6はない、これは郵便番号
      mapped.postalCode = postalCode;
      colIndex++;
    } else {
      // 郵便番号の形式でない = 業種5（文字列）
      mapped.industryCategories.push(industry5Value);
      colIndex++;
      
      // 業種6の位置をチェック
      const industry6Value = row[colIndex] ? trim(row[colIndex]) : null;
      if (industry6Value) {
        const postalCode6 = validatePostalCode(industry6Value);
        if (postalCode6) {
          // 業種6の位置に郵便番号が来た（業種6はない）
          mapped.postalCode = postalCode6;
          colIndex++;
        } else {
          // 業種6がある（非数値）
          mapped.industryCategories.push(industry6Value);
          colIndex++;
          
          // 次の位置が郵便番号
          if (row[colIndex]) {
            const postalCodeNext = validatePostalCode(row[colIndex]);
            if (postalCodeNext) {
              mapped.postalCode = postalCodeNext;
            }
          }
          colIndex++;
        }
      } else {
        // 業種6がない場合、次の位置が郵便番号
        if (row[colIndex]) {
          const postalCodeNext = validatePostalCode(row[colIndex]);
          if (postalCodeNext) {
            mapped.postalCode = postalCodeNext;
          }
        }
        colIndex++;
      }
    }
  } else {
    // 業種5がない場合、この位置が郵便番号
    if (row[colIndex]) {
      const postalCode = validatePostalCode(row[colIndex]);
      if (postalCode) {
        mapped.postalCode = postalCode;
      }
    }
    colIndex++;
  }
  
  // 12. 住所（郵便番号の次）
  if (!mapped.postalCode && row[colIndex]) {
    // 郵便番号がまだ設定されていない場合、この位置が郵便番号の可能性
    const postalCode = validatePostalCode(row[colIndex]);
    if (postalCode) {
      mapped.postalCode = postalCode;
      colIndex++;
      // 次の位置が住所
      if (row[colIndex]) mapped.address = trim(row[colIndex]);
      colIndex++;
    } else {
      // 郵便番号でない場合は住所
      if (row[colIndex]) mapped.address = trim(row[colIndex]);
      colIndex++;
    }
  } else {
    // 郵便番号は既に設定済み
    if (row[colIndex]) mapped.address = trim(row[colIndex]);
    colIndex++;
  }
  
  // 13. 設立
  if (row[colIndex]) mapped.established = trim(row[colIndex]);
  colIndex++;
  
  // 14. 電話番号(窓口)
  if (row[colIndex]) mapped.phoneNumber = trim(row[colIndex]);
  colIndex++;
  
  // 15. 代表者郵便番号
  if (row[colIndex]) {
    const postalCode = validatePostalCode(row[colIndex]);
    if (postalCode) {
      mapped.representativePostalCode = postalCode;
    }
  }
  colIndex++;
  
  // 16. 代表者住所
  if (row[colIndex]) mapped.representativeHomeAddress = trim(row[colIndex]);
  colIndex++;
  
  // 17. 代表者誕生日
  if (row[colIndex]) mapped.representativeBirthDate = trim(row[colIndex]);
  colIndex++;
  
  // 18. 資本金
  if (row[colIndex]) {
    const num = parseFinancialNumeric(row[colIndex], "type_e", filePath, "capitalStock");
    if (num !== null) mapped.capitalStock = num;
  }
  colIndex++;
  
  // 19. 上場
  if (row[colIndex]) mapped.listing = trim(row[colIndex]);
  colIndex++;
  
  // 20. 直近決算年月
  if (row[colIndex]) {
    mapped.latestFiscalYearMonth = trim(row[colIndex]);
  }
  colIndex++;
  
  // 21. 直近売上
  if (row[colIndex]) {
    const num = parseFinancialNumeric(row[colIndex], "type_e", filePath, "revenue");
    if (num !== null) mapped.revenue = num;
  }
  colIndex++;
  
  // 22. 直近利益
  if (row[colIndex]) {
    const num = parseFinancialNumeric(row[colIndex], "type_e", filePath, "latestProfit");
    if (num !== null) mapped.latestProfit = num;
  }
  colIndex++;
  
  // 23. 説明
  if (row[colIndex]) mapped.companyDescription = trim(row[colIndex]);
  colIndex++;
  
  // 24. 概要
  if (row[colIndex]) mapped.overview = trim(row[colIndex]);
  colIndex++;
  
  // 25. 仕入れ先
  if (row[colIndex]) {
    const suppliersValue = trim(row[colIndex]);
    if (suppliersValue) {
      // 配列として保存（カンマ区切りの場合は分割）
      mapped.suppliers = suppliersValue.split(/[，,]/).map(s => s.trim()).filter(s => s);
    }
  }
  colIndex++;
  
  // 26. 取引先
  if (row[colIndex]) mapped.clients = trim(row[colIndex]);
  colIndex++;
  
  // 27. 取引先銀行
  if (row[colIndex]) {
    const banksValue = trim(row[colIndex]);
    if (banksValue) {
      // 配列として保存（カンマ区切りの場合は分割）
      mapped.banks = banksValue.split(/[，,]/).map(s => s.trim()).filter(s => s);
    }
  }
  colIndex++;
  
  // 28. 取締役
  if (row[colIndex]) mapped.executives = trim(row[colIndex]);
  colIndex++;
  
  // 29. 株主
  if (row[colIndex]) mapped.shareholders = trim(row[colIndex]);
  colIndex++;
  
  // 30. 社員数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.employeeCount = num;
  }
  colIndex++;
  
  // 31. オフィス数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.officeCount = num;
  }
  colIndex++;
  
  // 32. 工場数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.factoryCount = num;
  }
  colIndex++;
  
  // 33. 店舗数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.storeCount = num;
  }
  colIndex++;
  
  return mapped;
}

// タイプCのCSVを行配列として読み込む（列インデックスベース）
function loadTypeCCSVByIndex(csvFilePath: string): Array<Array<string>> {
  const buf = fs.readFileSync(csvFilePath);
  try {
    const records: Array<Array<string>> = parse(buf, {
      columns: false,  // ヘッダーを無視して配列として読み込む
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      skip_records_with_error: true,
    });
    console.log(`  📄 ${path.basename(csvFilePath)}: ${records.length} 行（タイプC: 列順序ベース）`);
    return records;
  } catch (err: any) {
    console.warn(`  ⚠️ ${path.basename(csvFilePath)}: CSVパースエラー - ${err.code || err.message}`);
    return [];
  }
}

// タイプFのCSVを行配列として読み込む（列インデックスベース）
function loadTypeFCSVByIndex(csvFilePath: string): Array<Array<string>> {
  const buf = fs.readFileSync(csvFilePath);
  try {
    const records: Array<Array<string>> = parse(buf, {
      columns: false,  // ヘッダーを無視して配列として読み込む
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      skip_records_with_error: true,
    });
    console.log(`  📄 ${path.basename(csvFilePath)}: ${records.length} 行（タイプF: 列順序ベース）`);
    return records;
  } catch (err: any) {
    console.warn(`  ⚠️ ${path.basename(csvFilePath)}: CSVパースエラー - ${err.code || err.message}`);
    return [];
  }
}

// タイプCの行データを列インデックスに基づいてマッピング
// 構造: 会社名,電話番号,郵便番号,住所,URL,代表者,郵便番号,住所,創業,,設立,株式保有率,役員,概要,業種（大）,業種（細）,業種（中）,業種（小）,業種（細）
// インデックス9（空白）を「取引先」として処理
function mapTypeCRowByIndex(row: Array<string>, filePath: string = ""): Record<string, any> {
  const mapped: Record<string, any> = {};
  let colIndex = 0;
  
  // 0. 会社名
  if (row[colIndex]) mapped.name = trim(row[colIndex]);
  colIndex++;
  
  // 1. 電話番号
  if (row[colIndex]) mapped.contactPhoneNumber = trim(row[colIndex]);
  colIndex++;
  
  // 2. 郵便番号
  if (row[colIndex]) {
    const postalCode = validatePostalCode(row[colIndex]);
    if (postalCode) mapped.postalCode = postalCode;
  }
  colIndex++;
  
  // 3. 住所
  if (row[colIndex]) mapped.address = trim(row[colIndex]);
  colIndex++;
  
  // 4. URL
  if (row[colIndex]) mapped.companyUrl = trim(row[colIndex]);
  colIndex++;
  
  // 5. 代表者
  if (row[colIndex]) {
    processRepresentativeName(row[colIndex], mapped);
  }
  colIndex++;
  
  // 6. 郵便番号（代表者）
  if (row[colIndex]) {
    const postalCode = validatePostalCode(row[colIndex]);
    if (postalCode) mapped.representativePostalCode = postalCode;
  }
  colIndex++;
  
  // 7. 住所（代表者）
  if (row[colIndex]) mapped.representativeHomeAddress = trim(row[colIndex]);
  colIndex++;
  
  // 8. 創業
  if (row[colIndex]) mapped.founding = trim(row[colIndex]);
  colIndex++;
  
  // 9. （空白）→ 取引先として処理
  if (row[colIndex]) mapped.clients = trim(row[colIndex]);
  colIndex++;
  
  // 10. 設立
  if (row[colIndex]) mapped.established = trim(row[colIndex]);
  colIndex++;
  
  // 11. 株式保有率
  if (row[colIndex]) mapped.shareholders = trim(row[colIndex]);
  colIndex++;
  
  // 12. 役員
  if (row[colIndex]) mapped.executives = trim(row[colIndex]);
  colIndex++;
  
  // 13. 概要
  if (row[colIndex]) mapped.companyDescription = trim(row[colIndex]);
  colIndex++;
  
  // 14以降: 業種（大）、業種（細）、業種（中）、業種（小）、業種（細）
  // 業種の順序が不規則なので、値があるものを順に処理
  const industryFields: Array<keyof typeof mapped> = ['industryLarge', 'industryDetail', 'industryMiddle', 'industrySmall'];
  let industryIndex = 0;
  
  while (colIndex < row.length && industryIndex < industryFields.length) {
    if (row[colIndex]) {
      const field = industryFields[industryIndex];
      mapped[field] = trim(row[colIndex]);
      industryIndex++;
    }
    colIndex++;
  }
  
  // 残りの業種フィールドがあれば、industryCategoriesに追加
  const industryCategories: string[] = [];
  while (colIndex < row.length) {
    if (row[colIndex]) {
      const value = trim(row[colIndex]);
      if (value) industryCategories.push(value);
    }
    colIndex++;
  }
  if (industryCategories.length > 0) {
    mapped.industryCategories = industryCategories;
  }
  
  return mapped;
}

// タイプFの行データを列インデックスに基づいてマッピング
// 構造: 会社名(0),都道府県(1),代表者名(2),取引種別(3),SBフラグ(4),NDA(5),AD(6),ステータス(7),備考(8),URL(9),業種1(10),業種2(11),業種3(12),郵便番号(13),住所(14),設立(15),電話番号(窓口)(16),代表者郵便番号(17),代表者住所(18),代表者誕生日(19),資本金(20),上場(21),直近決算年月(22),直近売上(23),直近利益(24),説明(25),概要(26),仕入れ先(27),取引先(28),取引先銀行(29),取締役(30),株主(31),社員数(32),オフィス数(33),工場数(34),店舗数(35)
// 注意: 業種4〜7はヘッダーにないが、業種3の後に続く列を確認する必要がある
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
  if (row[colIndex]) {
    processRepresentativeName(row[colIndex], mapped);
  }
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
  
  // 13以降: 業種4〜7の処理（動的判定）
  // 業種4の位置をチェック
  const industry4Value = row[colIndex] ? trim(row[colIndex]) : null;
  
  if (industry4Value && isNumericValue(industry4Value)) {
    // 業種4の位置に数値が来た = 業種4〜7はない、これは郵便番号
    const postalCode = validatePostalCode(industry4Value);
    if (postalCode) mapped.postalCode = postalCode;
    colIndex++;
  } else {
    // 業種4がある（非数値）
    if (industry4Value) {
      mapped.industryDetail = industry4Value;
    }
    colIndex++;
    
    // 業種5の位置をチェック
    const industry5Value = row[colIndex] ? trim(row[colIndex]) : null;
    if (industry5Value && isNumericValue(industry5Value)) {
      // 業種5の位置に数値が来た = 業種5〜7はない、これは郵便番号
      const postalCode = validatePostalCode(industry5Value);
      if (postalCode) mapped.postalCode = postalCode;
      colIndex++;
    } else {
      // 業種5がある（非数値）
      if (industry5Value) {
        // industryCategoriesを初期化
        if (!mapped.industryCategories) mapped.industryCategories = [];
        mapped.industryCategories.push(industry5Value);
      }
      colIndex++;
      
      // 業種6の位置をチェック
      const industry6Value = row[colIndex] ? trim(row[colIndex]) : null;
      if (industry6Value && isNumericValue(industry6Value)) {
        // 業種6の位置に郵便番号が来た（業種6〜7はない）
        const postalCode = validatePostalCode(industry6Value);
        if (postalCode) mapped.postalCode = postalCode;
        colIndex++;
      } else {
        // 業種6がある（非数値）
        if (industry6Value) {
          if (!mapped.industryCategories) mapped.industryCategories = [];
          mapped.industryCategories.push(industry6Value);
        }
        colIndex++;
        
        // 業種7の位置をチェック
        const industry7Value = row[colIndex] ? trim(row[colIndex]) : null;
        if (industry7Value && isNumericValue(industry7Value)) {
          // 業種7の位置に郵便番号が来た（業種7はない）
          const postalCode = validatePostalCode(industry7Value);
          if (postalCode) mapped.postalCode = postalCode;
          colIndex++;
        } else {
          // 業種7がある（非数値）
          if (industry7Value) {
            if (!mapped.industryCategories) mapped.industryCategories = [];
            mapped.industryCategories.push(industry7Value);
          }
          colIndex++;
          
          // 次の位置が郵便番号
          if (row[colIndex]) {
            const postalCode = validatePostalCode(row[colIndex]);
            if (postalCode) mapped.postalCode = postalCode;
            colIndex++;
          } else {
            colIndex++;
          }
        }
      }
    }
  }
  
  // 郵便番号がまだ設定されていない場合、現在の位置を確認
  if (!mapped.postalCode && row[colIndex]) {
    const postalCode = validatePostalCode(row[colIndex]);
    if (postalCode) {
      mapped.postalCode = postalCode;
      colIndex++;
    } else {
      colIndex++;
    }
  } else if (!mapped.postalCode) {
    colIndex++;
  }
  
  // 住所
  if (row[colIndex]) mapped.address = trim(row[colIndex]);
  colIndex++;
  
  // 設立
  if (row[colIndex]) mapped.established = trim(row[colIndex]);
  colIndex++;
  
  // 電話番号(窓口)
  if (row[colIndex]) mapped.contactPhoneNumber = trim(row[colIndex]);
  colIndex++;
  
  // 代表者郵便番号
  if (row[colIndex]) {
    const postalCode = validatePostalCode(row[colIndex]);
    if (postalCode) mapped.representativePostalCode = postalCode;
  }
  colIndex++;
  
  // 代表者住所
  if (row[colIndex]) mapped.representativeHomeAddress = trim(row[colIndex]);
  colIndex++;
  
  // 代表者誕生日
  if (row[colIndex]) mapped.representativeBirthDate = trim(row[colIndex]);
  colIndex++;
  
  // 資本金
  if (row[colIndex]) {
    const capital = parseFinancialNumeric(row[colIndex], "type_f", filePath, "capitalStock");
    if (capital !== null) mapped.capitalStock = capital;
  }
  colIndex++;
  
  // 上場
  if (row[colIndex]) mapped.listing = trim(row[colIndex]);
  colIndex++;
  
  // 直近決算年月
  if (row[colIndex]) mapped.fiscalMonth = trim(row[colIndex]);
  colIndex++;
  
  // 直近売上
  if (row[colIndex]) {
    const revenue = parseFinancialNumeric(row[colIndex], "type_f", filePath, "revenue");
    if (revenue !== null) mapped.revenue = revenue;
  }
  colIndex++;
  
  // 直近利益
  if (row[colIndex]) {
    const profit = parseFinancialNumeric(row[colIndex], "type_f", filePath, "latestProfit");
    if (profit !== null) mapped.latestProfit = profit;
  }
  colIndex++;
  
  // 説明
  if (row[colIndex]) mapped.companyDescription = trim(row[colIndex]);
  colIndex++;
  
  // 概要
  if (row[colIndex]) mapped.overview = trim(row[colIndex]);
  colIndex++;
  
  // 仕入れ先
  if (row[colIndex]) mapped.suppliers = trim(row[colIndex]);
  colIndex++;
  
  // 取引先
  if (row[colIndex]) mapped.clients = trim(row[colIndex]);
  colIndex++;
  
  // 取引先銀行
  if (row[colIndex]) mapped.banks = trim(row[colIndex]);
  colIndex++;
  
  // 取締役
  if (row[colIndex]) mapped.executives = trim(row[colIndex]);
  colIndex++;
  
  // 株主
  if (row[colIndex]) mapped.shareholders = trim(row[colIndex]);
  colIndex++;
  
  // 社員数
  if (row[colIndex]) {
    const employeeCount = parseNumeric(row[colIndex]);
    if (employeeCount !== null) mapped.employeeCount = employeeCount;
  }
  colIndex++;
  
  // オフィス数
  if (row[colIndex]) {
    const officeCount = parseNumeric(row[colIndex]);
    if (officeCount !== null) mapped.officeCount = officeCount;
  }
  colIndex++;
  
  // 工場数
  if (row[colIndex]) {
    const factoryCount = parseNumeric(row[colIndex]);
    if (factoryCount !== null) mapped.factoryCount = factoryCount;
  }
  colIndex++;
  
  // 店舗数
  if (row[colIndex]) {
    const storeCount = parseNumeric(row[colIndex]);
    if (storeCount !== null) mapped.storeCount = storeCount;
  }
  colIndex++;
  
  return mapped;
}

function loadCsvRows(csvFilePath: string): CsvRow[] {
  const buf = fs.readFileSync(csvFilePath);
  try {
    const records: CsvRow[] = parse(buf, {
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,           // 不正なクォートを許容
      relax_column_count: true,     // カラム数の不一致を許容
      skip_records_with_error: true, // エラー行をスキップ
    });
    console.log(`  📄 ${path.basename(csvFilePath)}: ${records.length} 行`);
    return records;
  } catch (err: any) {
    console.warn(`  ⚠️ ${path.basename(csvFilePath)}: CSVパースエラー - ${err.code || err.message}`);
    console.warn(`     このファイルはスキップします`);
    return [];
  }
}

// ==============================
// 「値」を見てフィールドを推測するロジック
// ==============================

const PREF_NAMES = [
  "北海道",
  "青森県",
  "岩手県",
  "宮城県",
  "秋田県",
  "山形県",
  "福島県",
  "茨城県",
  "栃木県",
  "群馬県",
  "埼玉県",
  "千葉県",
  "東京都",
  "神奈川県",
  "新潟県",
  "富山県",
  "石川県",
  "福井県",
  "山梨県",
  "長野県",
  "岐阜県",
  "静岡県",
  "愛知県",
  "三重県",
  "滋賀県",
  "京都府",
  "大阪府",
  "兵庫県",
  "奈良県",
  "和歌山県",
  "鳥取県",
  "島根県",
  "岡山県",
  "広島県",
  "山口県",
  "徳島県",
  "香川県",
  "愛媛県",
  "高知県",
  "福岡県",
  "佐賀県",
  "長崎県",
  "熊本県",
  "大分県",
  "宮崎県",
  "鹿児島県",
  "沖縄県",
];

// 住所文字列から都道府県っぽい部分を抜き出す
function extractPrefectureFromAddress(addr: string | null | undefined): string | null {
  if (!addr) return null;
  const s = String(addr).trim();
  if (!s) return null;
  for (const p of PREF_NAMES) {
    if (s.startsWith(p)) return p;
  }
  return null;
}

function ratio(values: string[], predicate: (v: string) => boolean): number {
  const nonEmpty = values.filter((v) => v.trim() !== "");
  if (nonEmpty.length === 0) return 0;
  const ok = nonEmpty.filter(predicate).length;
  return ok / nonEmpty.length;
}

function looksLikeCorporateNumber(v: string): boolean {
  return /^\d{13}$/.test(v.replace(/\D/g, ""));
}

function looksLikePostalCode(v: string): boolean {
  return /^\d{3}-?\d{4}$/.test(v.trim());
}

function looksLikePhone(v: string): boolean {
  const s = v.trim();
  if (!/^0\d/.test(s)) return false;
  const digits = s.replace(/\D/g, "");
  return digits.length === 9 || digits.length === 10 || digits.length === 11;
}

function looksLikeEmail(v: string): boolean {
  const s = v.trim();
  if (!s) return false;
  return /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(s);
}

function looksLikeUrl(v: string): boolean {
  const s = v.trim();
  if (!s) return false;
  if (/^https?:\/\//i.test(s)) return true;
  if (s.includes(".") && (s.endsWith(".jp") || s.endsWith(".com") || s.endsWith(".co.jp"))) return true;
  return false;
}

function looksLikeCompanyName(v: string): boolean {
  const s = v.trim();
  if (!s) return false;
  if (s.includes("株式会社") || s.includes("有限会社") || s.includes("合名会社") || s.includes("合同会社")) return true;
  // 漢字 + 会社っぽい
  if (/[一-龥]/.test(s) && s.length >= 2 && s.length <= 30) return true;
  return false;
}

function looksLikePersonName(v: string): boolean {
  const s = v.trim();
  if (!s) return false;
  if (s.length < 2 || s.length > 20) return false;
  if (s.includes("@") || looksLikeUrl(s)) return false;
  // 漢字 or カナが多い
  if (/[一-龥ぁ-んァ-ン]/.test(s)) return true;
  return false;
}

function looksLikePrefecture(v: string): boolean {
  const s = v.trim();
  if (!s) return false;
  return PREF_NAMES.some((p) => s.startsWith(p) || s === p);
}

function looksLikeAddress(v: string): boolean {
  const s = v.trim();
  if (!s) return false;
  if (looksLikePrefecture(s)) return true;
  if (/[一-龥]/.test(s) && /[丁目番地号\-]/.test(s)) return true;
  return false;
}

function looksLikeDescription(v: string): boolean {
  const s = v.trim();
  if (s.length < 15) return false;
  // 日本語の文章っぽい
  return /[。\.、，]/.test(s);
}

// 既存の HEADER_TO_FIELD も「ヒント」として残しておく
const HEADER_HINT: Record<string, string> = {
  // 企業名
  "企業名": "name",
  "会社名": "name",
  "商号": "name",
  name: "name",

  // 法人番号
  "法人番号": "corporateNumber",
  corporateNumber: "corporateNumber",
  corporate_number: "corporateNumber",

  // 住所
  "住所": "address",
  "所在地": "address",
  "本社所在地": "address",
  "本社住所": "address",
  "会社住所": "address",
  address: "address",

  // 郵便番号
  "郵便番号": "postalCode",
  "会社郵便番号": "postalCode",
  postalCode: "postalCode",

  // 電話番号
  "電話番号": "phoneNumber",
  "代表電話": "phoneNumber",
  phone: "phoneNumber",
  tel: "phoneNumber",
  phoneNumber: "phoneNumber",

  // 窓口電話番号
  "窓口電話番号": "contactPhoneNumber",
  "窓口電話": "contactPhoneNumber",
  "問い合わせ電話番号": "contactPhoneNumber",
  "問い合わせ電話": "contactPhoneNumber",
  contactPhoneNumber: "contactPhoneNumber",

  // FAX
  "FAX": "fax",
  "FAX番号": "fax",
  fax: "fax",

  // URL
  "URL": "companyUrl",
  "会社URL": "companyUrl",
  "企業URL": "companyUrl",
  "企業ホームページURL": "companyUrl",
  "HP": "companyUrl",
  "HP_URL": "companyUrl",
  hpUrl: "companyUrl",
  url: "companyUrl",
  companyUrl: "companyUrl",
  "問い合わせフォームURL": "contactFormUrl",
  "お問い合わせURL": "contactFormUrl",
  contactFormUrl: "contactFormUrl",

  // 代表者関連
  "代表者名": "representativeName",
  "代表者": "representativeName",
  representative: "representativeName",
  representativeName: "representativeName",
  "代表者名カナ": "representativeKana",
  "代表者カナ": "representativeKana",
  representativeKana: "representativeKana",
  "代表者役職": "representativeTitle",
  "代表者肩書": "representativeTitle",
  representativeTitle: "representativeTitle",
  "代表者生年月日": "representativeBirthDate",
  "代表者誕生日": "representativeBirthDate",
  representativeBirthDate: "representativeBirthDate",
  "代表者出身校": "representativeAlmaMater",
  "代表者郵便番号": "representativePostalCode",
  representativePostalCode: "representativePostalCode",
  "代表者住所": "representativeHomeAddress",
  representativeHomeAddress: "representativeHomeAddress",
  "代表者電話番号": "representativePhone",
  "代表者電話": "representativePhone",
  representativePhone: "representativePhone",

  // 業種
  "業種": "industry",
  "ジャンル": "industryCategories",
  industry: "industry",
  industries: "industries",
  "業界大分類": "industryLarge",
  "業界中分類": "industryMiddle",
  "業界小分類": "industrySmall",
  "業種-大": "industryLarge",
  "業種-中": "industryMiddle",
  "業種-小": "industrySmall",
  "業種-細": "industryDetail",
  "業種（大）": "industryLarge",
  "業種（中）": "industryMiddle",
  "業種（小）": "industrySmall",
  "業種（細）": "industryDetail",
  "業種（分類１）": "industryLarge",
  "業種（分類２）": "industryMiddle",
  "業種（分類３）": "industrySmall",
  "業種1": "industryLarge",
  "業種2": "industryMiddle",
  "業種3": "industrySmall",
  industryLarge: "industryLarge",
  industryMiddle: "industryMiddle",
  industrySmall: "industrySmall",
  industryDetail: "industryDetail",

  // 事業内容・営業種目
  "営業種目": "businessDescriptions",
  "事業内容": "businessDescriptions",
  businessDescriptions: "businessDescriptions",

  // 設立
  "設立": "established",
  "設立年月日": "established",
  established: "established",

  // 株主
  "株主": "shareholders",
  "主要株主": "shareholders",
  "株式保有率": "shareholders",
  shareholders: "shareholders",

  // 取締役
  "取締役": "executives",
  "役員": "executives",
  executives: "executives",
  
  // 役員名1～10
  executiveName1: "executiveName1",
  executivePosition1: "executivePosition1",
  executiveName2: "executiveName2",
  executivePosition2: "executivePosition2",
  executiveName3: "executiveName3",
  executivePosition3: "executivePosition3",
  executiveName4: "executiveName4",
  executivePosition4: "executivePosition4",
  executiveName5: "executiveName5",
  executivePosition5: "executivePosition5",
  executiveName6: "executiveName6",
  executivePosition6: "executivePosition6",
  executiveName7: "executiveName7",
  executivePosition7: "executivePosition7",
  executiveName8: "executiveName8",
  executivePosition8: "executivePosition8",
  executiveName9: "executiveName9",
  executivePosition9: "executivePosition9",
  executiveName10: "executiveName10",
  executivePosition10: "executivePosition10",

  // 概況・概要
  "概況": "overview",
  "企業概要": "overview",
  overview: "overview",
  "会社説明": "companyDescription",
  companyDescription: "companyDescription",

  // 従業員数・売上・資本金
  "従業員数": "employeeCount",
  "社員数": "employeeCount",
  employees: "employeeCount",
  employeeCount: "employeeCount",
  "売上高": "revenue",
  sales: "revenue",
  revenue: "revenue",
  "資本金": "capitalStock",
  capital: "capitalStock",
  capitalStock: "capitalStock",

  // 直近決算情報
  "直近決算年月": "latestFiscalYearMonth",
  latestFiscalYearMonth: "latestFiscalYearMonth",
  "直近売上": "latestRevenue",
  latestRevenue: "latestRevenue",
  "直近利益": "latestProfit",
  "経常利益": "latestProfit",
  latestProfit: "latestProfit",

  // 決算月1～5
  "決算月1": "fiscalMonth1",
  "決算月2": "fiscalMonth2",
  "決算月3": "fiscalMonth3",
  "決算月4": "fiscalMonth4",
  "決算月5": "fiscalMonth5",
  fiscalMonth1: "fiscalMonth1",
  fiscalMonth2: "fiscalMonth2",
  fiscalMonth3: "fiscalMonth3",
  fiscalMonth4: "fiscalMonth4",
  fiscalMonth5: "fiscalMonth5",

  // 売上1～5
  "売上1": "revenue1",
  "売上2": "revenue2",
  "売上3": "revenue3",
  "売上4": "revenue4",
  "売上5": "revenue5",
  revenue1: "revenue1",
  revenue2: "revenue2",
  revenue3: "revenue3",
  revenue4: "revenue4",
  revenue5: "revenue5",

  // 利益1～5
  "利益1": "profit1",
  "利益2": "profit2",
  "利益3": "profit3",
  "利益4": "profit4",
  "利益5": "profit5",
  profit1: "profit1",
  profit2: "profit2",
  profit3: "profit3",
  profit4: "profit4",
  profit5: "profit5",

  // 上場
  "上場": "listing",
  "上場区分": "listing",
  listing: "listing",

  // 創業
  "創業": "founding",
  founded: "founding",
  founding: "founding",

  // 都道府県
  "都道府県": "prefecture",
  prefecture: "prefecture",

  // 取引可否
  "取引可否": "tradingStatus",
  "取引状況": "tradingStatus",
  "取引ステータス": "tradingStatus",
  tradingStatus: "tradingStatus",

  // 説明（companyDescription）
  "説明": "companyDescription",

  // 取引先
  "取引先": "clients",
  "主要取引先": "clients",
  clients: "clients",

  // 仕入れ先
  "仕入れ先": "suppliers",
  "主要仕入先": "suppliers",
  suppliers: "suppliers",

  // 子会社・関連会社
  "子会社・関連会社": "subsidiaries",
  "国内・海外の子会社": "subsidiaries",
  "関連会社": "subsidiaries",
  subsidiaries: "subsidiaries",

  // 取引先銀行
  "取引先銀行": "banks",
  "取引銀行": "banks",
  "メインバンク": "banks",
  "[募集人数][実績][主な取引銀行]": "banks",
  banks: "banks",
  
  // 51.csv固有フィールド
  "部署・拠点名": "departmentLocation",
  "得意分野": "specialties",
  "[平均年齢][平均勤続年数]": "averageAge",
  "[月平均所定外労働時間][平均有給休暇取得日数][役員及び管理的地位にある者に占める女性の割合]": "averageOvertimeHours",
  "[交通機関][加盟団体]": "transportation",

  // 会社名カナ・英語名
  "カナ": "kana",
  "会社名カナ": "kana",
  "企業名カナ": "kana",
  kana: "kana",
  "英語名": "nameEn",
  "会社名英語": "nameEn",
  nameEn: "nameEn",

  // 特記事項・メモ
  "特記事項": "specialNote",
  "メモ": "specialNote",
  "備考": "specialNote",
  "会社情報・備考": "specialNote",
  specialNote: "specialNote",
  memo: "specialNote",

  // 事業概要・事業項目
  "事業概要": "businessSummary",
  businessSummary: "businessSummary",
  "事業項目": "businessItems",
  businessItems: "businessItems",

  // 所在地（address/locationの別名）
  location: "location",

  // 設立日
  "設立日": "dateOfEstablishment",
  dateOfEstablishment: "dateOfEstablishment",

  // 従業員数（employeeNumberはemployeeCountの別名）
  employeeNumber: "employeeNumber",

  // 資格等級
  "資格等級": "qualificationGrade",
  qualificationGrade: "qualificationGrade",

  // 活動数
  "活動数": "numberOfActivity",
  numberOfActivity: "numberOfActivity",

  // 更新日
  "更新日": "updateDate",
  updateDate: "updateDate",

  // 市場区分
  "市場区分": "marketSegment",
  marketSegment: "marketSegment",

  // 所属団体
  "所属団体": "affiliations",
  affiliations: "affiliations",

  // 日経会社コード
  "日経会社コード": "nikkeiCode",
  nikkeiCode: "nikkeiCode",

  // 発行済株式数
  "発行済株式数": "issuedShares",
  issuedShares: "issuedShares",

  // 財務情報（statementsJsonから）
  totalAssets: "totalAssets",
  totalLiabilities: "totalLiabilities",
  netAssets: "netAssets",
  revenueFromStatements: "revenueFromStatements",
  operatingIncome: "operatingIncome",

  // 部署情報（7部署まで）
  departmentName1: "departmentName1",
  departmentAddress1: "departmentAddress1",
  departmentPhone1: "departmentPhone1",
  departmentName2: "departmentName2",
  departmentAddress2: "departmentAddress2",
  departmentPhone2: "departmentPhone2",
  departmentName3: "departmentName3",
  departmentAddress3: "departmentAddress3",
  departmentPhone3: "departmentPhone3",
  departmentName4: "departmentName4",
  departmentAddress4: "departmentAddress4",
  departmentPhone4: "departmentPhone4",
  departmentName5: "departmentName5",
  departmentAddress5: "departmentAddress5",
  departmentPhone5: "departmentPhone5",
  departmentName6: "departmentName6",
  departmentAddress6: "departmentAddress6",
  departmentPhone6: "departmentPhone6",
  departmentName7: "departmentName7",
  departmentAddress7: "departmentAddress7",
  departmentPhone7: "departmentPhone7",
  bankCorporateNumber: "bankCorporateNumber",

  // 130.csv/131.csv用の元フィールド（参考用に保持）
  departments: "specialNote",
  people: "overview",
  rawText: "companyDescription",
};

// ヘッダー名 + 値サンプルから、companies_new のフィールド名を推測
function inferFieldForHeader(headerRaw: string, values: string[]): string | null {
  const header = headerRaw.trim();
  const lower = header.toLowerCase();

  // 1) 既存のヒントマップを最優先
  if (HEADER_HINT[header]) return HEADER_HINT[header];
  if (HEADER_HINT[lower]) return HEADER_HINT[lower];

  // 2) ヘッダー名と COMPANY_TEMPLATE をざっくり照合
  if (header in COMPANY_TEMPLATE) return header;
  if (lower in COMPANY_TEMPLATE) return lower;

  // キーワードで簡易マッチ
  if (lower.includes("mail") || lower.includes("e-mail") || lower.includes("メール")) {
    return "email";
  }
  if (lower.includes("url") || lower.includes("hp") || lower.includes("website")) {
    return "companyUrl";
  }
  if (lower.includes("form") || lower.includes("問い合わせ")) {
    return "contactFormUrl";
  }
  if (lower.includes("pref") || lower.includes("都道府県")) {
    return "prefecture";
  }
  if (lower.includes("address") || lower.includes("住所") || lower.includes("所在地")) {
    return "address";
  }
  if (lower.includes("tel") || lower.includes("phone") || lower.includes("電話")) {
    return "phoneNumber";
  }
  if (lower.includes("fax")) {
    return "fax";
  }
  if (lower.includes("生年月日") || lower.includes("誕生日")) {
    return "representativeBirthDate";
  }
  if (lower.includes("代表者") && lower.includes("名")) {
    return "representativeName";
  }
  if (lower.includes("カナ") && lower.includes("代表")) {
    return "representativeKana";
  }
  if (lower.includes("役職") || lower.includes("肩書")) {
    return "representativeTitle";
  }
  if (lower.includes("資本金")) {
    return "capitalStock";
  }
  if (lower.includes("従業員") || lower.includes("社員数") || lower.includes("人数")) {
    return "employeeCount";
  }
  if (lower.includes("売上")) {
    return "revenue";
  }
  if (lower.includes("業種")) {
    return "industry";
  }
  if (lower.includes("概要") || lower.includes("説明")) {
    return "overview";
  }

  const sample = values.filter((v) => v && v.trim() !== "").slice(0, 50);
  if (sample.length === 0) return null;

  // 3) 値パターンから推測
  const rEmail = ratio(sample, looksLikeEmail);
  const rUrl = ratio(sample, looksLikeUrl);
  const rCorpNum = ratio(sample, looksLikeCorporateNumber);
  const rPostal = ratio(sample, looksLikePostalCode);
  const rPhone = ratio(sample, looksLikePhone);
  const rPref = ratio(sample, looksLikePrefecture);
  const rAddr = ratio(sample, looksLikeAddress);
  const rCompany = ratio(sample, looksLikeCompanyName);
  const rPerson = ratio(sample, looksLikePersonName);
  const rDesc = ratio(sample, looksLikeDescription);

  // corporateNumber
  if (rCorpNum > 0.7) return "corporateNumber";
  // email
  if (rEmail > 0.7) return "email";
  // URL
  if (rUrl > 0.7) {
    if (lower.includes("form") || lower.includes("問い合わせ")) return "contactFormUrl";
    return "companyUrl";
  }
  // phone / fax
  if (rPhone > 0.7) {
    if (lower.includes("fax")) return "fax";
    return "phoneNumber";
  }
  // postalCode
  if (rPostal > 0.7) return "postalCode";
  // prefecture
  if (rPref > 0.7) return "prefecture";
  // address
  if (rAddr > 0.7) return "address";

  // company name vs person name
  if (rCompany > 0.6 && !lower.includes("代表")) {
    // 代表じゃなければ会社名の可能性が高い
    return "name";
  }
  if (rPerson > 0.6) {
    if (lower.includes("代表")) return "representativeName";
    return "representativeName";
  }

  // 長文テキスト
  if (rDesc > 0.6) {
    if (lower.includes("概要")) return "overview";
    if (lower.includes("説明")) return "companyDescription";
    return "overview";
  }

  // それでも決まらなければ諦める
  return null;
}

// ==============================
// Firestore 検索
// ==============================

function isDummyCorporateNumber(corporateNumber: string): boolean {
  // ダミー法人番号を検出（例: 9180000000000, 8180000000000）
  if (!corporateNumber || corporateNumber.length !== 13) {
    return false;
  }
  
  // 末尾が多数の0（例: 9180000000000 → 末尾9桁が0）
  const trailingZeros = corporateNumber.match(/0+$/);
  if (trailingZeros && trailingZeros[0].length >= 9) {
    return true;
  }
  
  // 全て同じ数字（例: 1111111111111, 0000000000000）
  const uniqueDigits = new Set(corporateNumber.split(''));
  if (uniqueDigits.size === 1) {
    return true;
  }
  
  return false;
}

function validateCorporateNumber(value: string | null | undefined): string | null {
  /**
   * 法人番号のバリデーション（要件2対応）
   * - 13桁の数値のみ有効
   * - 指数表記（2.01E+12など）はnull
   * - それ以外（文字列混在、桁数不足・超過）はnull
   */
  if (!value) return null;
  
  let trimmed = String(value).trim();
  if (!trimmed) return null;
  
  // 指数表記（例: 2.01E+12）の場合はnullを返す
  if (trimmed.includes("E") || trimmed.includes("e")) {
    return null;
  }
  
  // 数字のみ抽出
  const digitsOnly = trimmed.replace(/\D/g, '');
  
  // 13桁でなければinvalid
  if (digitsOnly.length !== 13) {
    return null;
  }
  
  // 元の文字列に文字が混ざっていればinvalid
  if (trimmed !== digitsOnly && /[^\d\s-]/.test(trimmed)) {
    return null;
  }
  
  // ダミー法人番号はnull
  if (isDummyCorporateNumber(digitsOnly)) {
    return null;
  }
  
  return digitsOnly;
}

async function findCompanyDocByCorporateNumber(
  corporateNumber: string
): Promise<{ ref: DocumentReference; data: any } | null> {
  const idCandidate = corporateNumber.trim();
  
  // ダミー法人番号の場合はnullを返して名前ベース検索にフォールバック
  if (isDummyCorporateNumber(idCandidate)) {
    return null;
  }

  const byId = await companiesCol.doc(idCandidate).get();
  if (byId.exists) {
    return { ref: byId.ref, data: byId.data() };
  }

  const snap = await companiesCol
    .where("corporateNumber", "==", idCandidate)
    .limit(1)
    .get();

  if (!snap.empty) {
    const doc = snap.docs[0];
    return { ref: doc.ref, data: doc.data() };
  }

  return null;
}

// 法人番号が無い／見つからない場合のフォールバック:
// 企業名 + 各種メタ情報からスコアリングして推測
async function findCompanyDocByNameAndMeta(
  row: CsvRow
): Promise<{ ref: DocumentReference; data: any } | null> {
  const name =
    trim(row["企業名"]) ??
    trim(row["会社名"]) ??
    trim(row["name"]);

  if (!name) return null;

  const rawPref =
    trim(row["都道府県"]) ??
    trim(row["prefecture"]);

  const rawAddress =
    trim(row["会社住所"]) ??
    trim(row["住所"]) ??
    trim(row["所在地"]) ??
    trim(row["本社所在地"]) ??
    trim(row["本社住所"]) ??
    trim(row["address"]);

  // 都道府県が空なら住所から推測
  const prefecture = rawPref ?? extractPrefectureFromAddress(rawAddress ?? "");
  const normPref = normalizeStr(prefecture);
  const normAddr = normalizeStr(rawAddress);

  const rawPostal =
    trim(row["会社郵便番号"]) ??
    trim(row["郵便番号"]) ??
    trim(row["postCode"]) ??
    trim(row["postalCode"]);
  const normPostal = digitsOnly(rawPostal);

  const rawPhone =
    trim(row["電話番号"]) ??
    trim(row["代表電話"]) ??
    trim(row["phone"]) ??
    trim(row["phoneNumber"]);
  const normPhone = digitsOnly(rawPhone);

  const rawUrl =
    trim(row["URL"]) ??
    trim(row["会社URL"]) ??
    trim(row["企業URL"]) ??
    trim(row["companyUrl"]) ??
    trim(row["HP"]) ??
    trim(row["HP_URL"]);
  const normUrlHost = normalizeUrlHost(rawUrl);

  // name 完全一致で候補取得
  const snap = await companiesCol
    .where("name", "==", name)
    .limit(30)
    .get();

  if (snap.empty) {
    // 完全一致がない場合は prefix 検索も試す（例: 株式会社〇〇 / 〇〇株式会社 の違いなどを多少拾える）
    const prefixSnap = await companiesCol
      .where("name", ">=", name)
      .where("name", "<=", name + "\uf8ff")
      .limit(30)
      .get();

    if (prefixSnap.empty) {
      console.warn(
        `⚠️  名前ベースの候補が 0 件でした: name="${name}", prefecture="${prefecture ?? ""}", address="${rawAddress ?? ""}"`
      );
      return null;
    }

    const candidates = prefixSnap.docs.map((d) => ({
      ref: d.ref,
      data: d.data(),
    }));

    return pickBestCandidateFromList(
      candidates,
      name,
      prefecture,
      rawAddress,
      normPref,
      normAddr,
      normPostal,
      normPhone,
      normUrlHost
    );
  }

  const candidates = snap.docs.map((d) => ({
    ref: d.ref,
    data: d.data(),
  }));

  return pickBestCandidateFromList(
    candidates,
    name,
    prefecture,
    rawAddress,
    normPref,
    normAddr,
    normPostal,
    normPhone,
    normUrlHost
  );
}

// タイプE用: 企業名・都道府県・代表者名で企業を特定（法人番号・住所は補助的に使用）
async function findCompanyDocByNameAndMetaForTypeE(
  name: string | null | undefined,
  address: string | null | undefined,
  representativeName: string | null | undefined,
  corporateNumber: string | null | undefined,
  postalCode: string | null | undefined,
  phoneNumber: string | null | undefined,
  companyUrl: string | null | undefined,
  prefecture: string | null | undefined
): Promise<{ ref: DocumentReference; data: any; candidates?: Candidate[] } | null> {
  if (!name) return null;

  const normName = normalizeStr(name);
  const normPref = normalizeStr(prefecture);
  const normRepName = normalizeStr(representativeName);
  const normAddr = normalizeStr(address);
  const normCorpNum = corporateNumber ? digitsOnly(corporateNumber) : null;
  const normPostal = postalCode ? digitsOnly(postalCode) : null;
  const normPhone = phoneNumber ? digitsOnly(phoneNumber) : null;
  const normUrlHost = normalizeUrlHost(companyUrl);

  // ① 法人番号で検索（最優先、13桁の場合のみ）
  if (normCorpNum && normCorpNum.length === 13) {
    const validated = validateCorporateNumber(corporateNumber);
    if (validated) {
      const byCorp = await findCompanyDocByCorporateNumber(validated);
      if (byCorp) {
        return byCorp;
      }
    }
  }

  // ② 企業名・都道府県で検索（優先）
  let snap: FirebaseFirestore.QuerySnapshot;
  if (prefecture) {
    snap = await companiesCol
      .where("name", "==", name)
      .where("prefecture", "==", prefecture)
      .limit(50)
      .get();
  } else {
    // 都道府県がない場合は企業名のみで検索
    snap = await companiesCol
      .where("name", "==", name)
      .limit(50)
      .get();
  }

  let candidates: Candidate[] = [];

  if (!snap.empty) {
    candidates = snap.docs.map((d) => ({
      ref: d.ref,
      data: d.data(),
    }));
  } else {
    // 完全一致がない場合は prefix 検索も試す
    const prefixSnap = await companiesCol
      .where("name", ">=", name)
      .where("name", "<=", name + "\uf8ff")
      .limit(50)
      .get();

    if (prefixSnap.empty) {
      return null;
    }

    candidates = prefixSnap.docs.map((d) => ({
      ref: d.ref,
      data: d.data(),
    }));
  }

  const result = pickBestCandidateForTypeE(
    candidates,
    normName,
    normAddr,
    normRepName,
    normCorpNum,
    normPostal,
    normPhone,
    normUrlHost,
    normPref
  );

  if (result) {
    return { ...result, candidates };
  }

  return null;
}

// タイプF用: 会社名・都道府県・代表者名で企業を特定
async function findCompanyDocByNamePrefectureRepresentative(
  name: string | null | undefined,
  prefecture: string | null | undefined,
  representativeName: string | null | undefined
): Promise<{ ref: DocumentReference; data: any; allCandidates?: Candidate[] } | null> {
  if (!name) return null;

  const normName = normalizeStr(name);
  const normPref = normalizeStr(prefecture);
  const normRepName = normalizeStr(representativeName);

  // ① 会社名・都道府県・代表者名で検索（優先）
  let snap: FirebaseFirestore.QuerySnapshot;
  if (prefecture && representativeName) {
    // 会社名・都道府県・代表者名で検索
    const nameSnap = await companiesCol
      .where("name", "==", name)
      .where("prefecture", "==", prefecture)
      .limit(50)
      .get();
    
    // 代表者名でフィルタリング
    const candidates: Candidate[] = [];
    for (const doc of nameSnap.docs) {
      const data = doc.data();
      const docRepName = normalizeStr(data.representativeName);
      if (normRepName && docRepName && normRepName === docRepName) {
        candidates.push({ ref: doc.ref, data });
      }
    }
    
    if (candidates.length > 0) {
      return { ref: candidates[0].ref, data: candidates[0].data, allCandidates: candidates };
    }
    
    // 代表者名が一致しない場合でも、会社名・都道府県が一致していれば候補とする
    if (nameSnap.docs.length > 0) {
      const candidates2: Candidate[] = nameSnap.docs.map(d => ({ ref: d.ref, data: d.data() }));
      return { ref: candidates2[0].ref, data: candidates2[0].data, allCandidates: candidates2 };
    }
    
    snap = nameSnap;
  } else if (prefecture) {
    // 会社名・都道府県で検索
    snap = await companiesCol
      .where("name", "==", name)
      .where("prefecture", "==", prefecture)
      .limit(50)
      .get();
  } else {
    // 会社名のみで検索
    snap = await companiesCol
      .where("name", "==", name)
      .limit(50)
      .get();
  }

  let candidates: Candidate[] = [];

  if (!snap.empty) {
    candidates = snap.docs.map((d) => ({
      ref: d.ref,
      data: d.data(),
    }));
  } else {
    // 完全一致がない場合は prefix 検索も試す
    const prefixSnap = await companiesCol
      .where("name", ">=", name)
      .where("name", "<=", name + "\uf8ff")
      .limit(50)
      .get();

    if (prefixSnap.empty) {
      return null;
    }

    candidates = prefixSnap.docs.map((d) => ({
      ref: d.ref,
      data: d.data(),
    }));
  }

  // スコアリングして最適な候補を選択
  const result = pickBestCandidateForTypeF(
    candidates,
    normName,
    normPref,
    normRepName
  );

  if (result) {
    return { ...result, allCandidates: candidates };
  }

  return null;
}

// タイプF用の候補選択関数
function pickBestCandidateForTypeF(
  candidates: Candidate[],
  normName: string,
  normPref: string,
  normRepName: string
): { ref: DocumentReference; data: any; allCandidates?: Scored[] } | null {
  if (candidates.length === 0) return null;
  const scored: Scored[] = [];

  for (const c of candidates) {
    const d = c.data;
    const docPref = normalizeStr(d.prefecture);
    const docRepName = normalizeStr(d.representativeName);

    let score = 0;

    // 都道府県一致は強い（会社名・都道府県・代表者名で特定するため）
    if (normPref && docPref && normPref === docPref) {
      score += 60;
    }

    // 代表者名一致は強い（会社名・都道府県・代表者名で特定するため）
    if (normRepName && docRepName && normRepName === docRepName) {
      score += 60;
    }

    // 都道府県と代表者名の両方が一致している場合は最優先
    if (normPref && docPref && normPref === docPref && 
        normRepName && docRepName && normRepName === docRepName) {
      score += 100;
    }

    scored.push({ ref: c.ref, data: d, score });
  }

  scored.sort((a, b) => b.score - a.score);

  if (scored.length === 0) {
    return null;
  }

  const top = scored[0];
  const second = scored[1];

  // スコア閾値（会社名・都道府県・代表者名で特定するため、都道府県または代表者名が一致していれば統合）
  const HIGH_CONFIDENCE_THRESHOLD = 60; // 都道府県または代表者名が一致していれば高信頼度
  const MINIMUM_SCORE_THRESHOLD = 30; // 最低限の信頼度

  if (top.score < MINIMUM_SCORE_THRESHOLD) {
    return null;
  }

  // 高スコアの場合は統合
  if (top.score >= HIGH_CONFIDENCE_THRESHOLD) {
    if (second) {
      console.log(
        `✅ タイプF: 高スコアで統合: name="${normName}", topScore=${top.score}, secondScore=${second.score}, candidates=${scored.length}`
      );
    } else {
      console.log(
        `✅ タイプF: 高スコアで一意候補に統合: name="${normName}", topScore=${top.score}`
      );
    }
    return { ref: top.ref, data: top.data, allCandidates: scored };
  }

  // 中程度のスコア（30-59）の場合
  if (second) {
    console.warn(
      `⚠️  タイプF: 複数候補、スコア上位を採用（要注意）: name="${normName}", topScore=${top.score}, secondScore=${second.score}, candidates=${scored.length}`
    );
  } else {
    console.warn(
      `⚠️  タイプF: 一意候補を採用（スコア中程度）: name="${normName}", topScore=${top.score}`
    );
  }

  return { ref: top.ref, data: top.data, allCandidates: scored };
}

// タイプG用: 企業名で企業を特定（法人番号は補助的に使用）
async function findCompanyDocByNameForTypeG(
  name: string | null | undefined,
  corporateNumber: string | null | undefined
): Promise<{ ref: DocumentReference; data: any } | null> {
  if (!name) return null;

  // ① 企業名で検索（優先）
  const snap = await companiesCol
    .where("name", "==", name)
    .limit(10)
    .get();

  if (snap.empty) {
    // 完全一致がない場合は prefix 検索も試す
    const prefixSnap = await companiesCol
      .where("name", ">=", name)
      .where("name", "<=", name + "\uf8ff")
      .limit(10)
      .get();

    if (prefixSnap.empty) {
      return null;
    }

    // 法人番号が一致するものを優先
    if (corporateNumber) {
      const validated = validateCorporateNumber(corporateNumber);
      if (validated) {
        for (const doc of prefixSnap.docs) {
          const data = doc.data();
          const docCorpNum = data.corporateNumber ? digitsOnly(data.corporateNumber) : null;
          if (docCorpNum === validated) {
            return { ref: doc.ref, data };
          }
        }
      }
    }

    // 法人番号で一致しない場合は最初の候補を返す
    return { ref: prefixSnap.docs[0].ref, data: prefixSnap.docs[0].data() };
  }

  // 法人番号が一致するものを優先
  if (corporateNumber) {
    const validated = validateCorporateNumber(corporateNumber);
    if (validated) {
      for (const doc of snap.docs) {
        const data = doc.data();
        const docCorpNum = data.corporateNumber ? digitsOnly(data.corporateNumber) : null;
        if (docCorpNum === validated) {
          return { ref: doc.ref, data };
        }
      }
    }
  }

  // 法人番号で一致しない場合は最初の候補を返す
  return { ref: snap.docs[0].ref, data: snap.docs[0].data() };
}

type Scored = { ref: DocumentReference; data: any; score: number };

function pickBestCandidateForTypeE(
  candidates: Candidate[],
  normName: string,
  normAddr: string,
  normRepName: string,
  normCorpNum: string | null,
  normPostal: string | null,
  normPhone: string | null,
  normUrlHost: string,
  normPref: string
): { ref: DocumentReference; data: any; allCandidates?: Scored[] } | null {
  if (candidates.length === 0) return null;
  const scored: Scored[] = [];

  for (const c of candidates) {
    const d = c.data;
    const docPref = normalizeStr(d.prefecture);
    const docRepName = normalizeStr(d.representativeName);
    const docAddr = normalizeStr(d.address || d.headquartersAddress);
    const docCorpNum = d.corporateNumber ? digitsOnly(d.corporateNumber) : null;
    const docPostal = digitsOnly(d.postalCode);
    const docPhone = digitsOnly(d.phoneNumber || d.representativePhone);
    const docUrlHost = normalizeUrlHost(d.companyUrl);

    let score = 0;

    // 法人番号一致は最強（13桁の場合のみ）
    if (normCorpNum && normCorpNum.length === 13 && docCorpNum && normCorpNum === docCorpNum) {
      score += 100;
    }

    // 都道府県一致は強い（企業名・都道府県・代表者名で特定するため）
    if (normPref && docPref && normPref === docPref) {
      score += 60;
    }

    // 代表者名一致は強い（企業名・都道府県・代表者名で特定するため）
    if (normRepName && docRepName && normRepName === docRepName) {
      score += 50;
    }

    // 住所一致は中程度
    if (normAddr && docAddr) {
      if (docAddr === normAddr) {
        score += 40;
      } else if (docAddr.includes(normAddr) || normAddr.includes(docAddr)) {
        score += 30;
      } else {
        // 先頭10文字が一致
        const a = normAddr.slice(0, 10);
        const b = docAddr.slice(0, 10);
        if (a && b && a === b) {
          score += 20;
        } else {
          // 部分一致
          const commonLength = Math.min(normAddr.length, docAddr.length);
          if (commonLength >= 5) {
            let matchCount = 0;
            for (let i = 0; i < commonLength; i++) {
              if (normAddr[i] === docAddr[i]) matchCount++;
            }
            if (matchCount >= 5) {
              score += 10;
            }
          }
        }
      }
    }

    // 郵便番号一致は中程度
    if (normPostal && docPostal && normPostal === docPostal) {
      score += 30;
    }

    // 電話番号一致は中程度
    if (normPhone && docPhone && normPhone === docPhone) {
      score += 30;
    }

    // URL一致は低め
    if (normUrlHost && docUrlHost && normUrlHost === docUrlHost) {
      score += 20;
    }

    scored.push({ ref: c.ref, data: d, score });
  }

  scored.sort((a, b) => b.score - a.score);

  if (scored.length === 0) {
    return null;
  }

  const top = scored[0];
  const second = scored[1];

  // スコア閾値（企業名・都道府県・代表者名で特定するため、都道府県または代表者名が一致していれば統合）
  // 企業名は既に一致している前提なので、都道府県または代表者名が一致していれば十分
  const HIGH_CONFIDENCE_THRESHOLD = 50; // 都道府県または代表者名が一致していれば高信頼度
  const MINIMUM_SCORE_THRESHOLD = 30; // 住所や郵便番号などで一致していれば最低限の信頼度

  if (top.score < MINIMUM_SCORE_THRESHOLD) {
    return null;
  }

  // 高スコアの場合は統合
  if (top.score >= HIGH_CONFIDENCE_THRESHOLD) {
    if (second) {
      console.log(
        `✅ タイプE: 高スコアで統合: name="${normName}", topScore=${top.score}, secondScore=${second.score}, candidates=${scored.length}`
      );
    } else {
      console.log(
        `✅ タイプE: 高スコアで一意候補に統合: name="${normName}", topScore=${top.score}`
      );
    }
    return { ref: top.ref, data: top.data, allCandidates: scored };
  }

  // 中程度のスコア（30-49）の場合
  if (second) {
    console.warn(
      `⚠️  タイプE: 複数候補、スコア上位を採用（要注意）: name="${normName}", topScore=${top.score}, secondScore=${second.score}, candidates=${scored.length}`
    );
  } else {
    console.warn(
      `⚠️  タイプE: 一意候補を採用（スコア中程度）: name="${normName}", topScore=${top.score}`
    );
  }

  return { ref: top.ref, data: top.data, allCandidates: scored };
}

type Candidate = { ref: DocumentReference; data: any };

function pickBestCandidateFromList(
  candidates: Candidate[],
  name: string,
  prefecture: string | null,
  rawAddress: string | null,
  normPref: string,
  normAddr: string,
  normPostal: string,
  normPhone: string,
  normUrlHost: string
): { ref: DocumentReference; data: any } | null {
  if (candidates.length === 0) return null;

  type Scored = { ref: DocumentReference; data: any; score: number };
  const scored: Scored[] = [];

  for (const c of candidates) {
    const d = c.data;

    const docPref =
      normalizeStr(d.prefecture) ||
      normalizeStr(extractPrefectureFromAddress(d.address || d.headquartersAddress));
    const docAddr = normalizeStr(d.address || d.headquartersAddress);
    const docPostal = digitsOnly(d.postalCode);
    const docPhone = digitsOnly(
      d.phoneNumber ||
      d.representativePhone
    );
    const docUrlHost = normalizeUrlHost(d.companyUrl);

    let score = 0;

    // 郵便番号一致は強い
    if (normPostal && docPostal && normPostal === docPostal) {
      score += 40;
    }

    // URL ホスト一致も強い
    if (normUrlHost && docUrlHost && normUrlHost === docUrlHost) {
      score += 35;
    }

    // 電話番号（完全一致 or 末尾一致）
    if (normPhone && docPhone) {
      if (normPhone === docPhone) {
        score += 30;
      } else if (
        docPhone.endsWith(normPhone) ||
        normPhone.endsWith(docPhone)
      ) {
        score += 20;
      }
    }

    // 都道府県一致
    if (normPref && docPref && normPref === docPref) {
      score += 15;
    }

    // 住所の包含関係
    if (normAddr && docAddr) {
      if (docAddr.includes(normAddr) || normAddr.includes(docAddr)) {
        score += 15;
      } else {
        const a = normAddr.slice(0, 10);
        const b = docAddr.slice(0, 10);
        if (a && b && a === b) {
          score += 8;
        }
      }
    }

    scored.push({ ref: c.ref, data: d, score });
  }

  scored.sort((a, b) => b.score - a.score);

  if (scored.length === 0) {
    console.warn(
      `⚠️  名前ベースでスコアを付けましたが有力候補がありませんでした: name="${name}", prefecture="${prefecture ?? ""}", address="${rawAddress ?? ""}"`
    );
    return null;
  }

  const top = scored[0];
  const second = scored[1];

  // スコア閾値（70以上: 高信頼度で統合、50-69: 要注意、50未満: 新規作成）
  const HIGH_CONFIDENCE_THRESHOLD = 70;
  const MINIMUM_SCORE_THRESHOLD = 50;
  
  if (top.score < MINIMUM_SCORE_THRESHOLD) {
    console.warn(
      `⚠️  名前ベースでのスコアが低すぎるため新規作成します: name="${name}", prefecture="${prefecture ?? ""}", address="${rawAddress ?? ""}", topScore=${top.score}`
    );
    return null;
  }

  // 高スコアの場合は統合
  if (top.score >= HIGH_CONFIDENCE_THRESHOLD) {
    if (second) {
      console.log(
        `✅ 高スコアで統合: name="${name}", topScore=${top.score}, secondScore=${second.score}, candidates=${scored.length}`
      );
    } else {
      console.log(
        `✅ 高スコアで一意候補に統合: name="${name}", topScore=${top.score}`
      );
    }
    return { ref: top.ref, data: top.data };
  }

  // 中程度のスコア（50-69）の場合
  if (second) {
    console.warn(
      `⚠️  名前ベースで複数候補、スコア上位を採用（要注意）: name="${name}", topScore=${top.score}, secondScore=${second.score}, candidates=${scored.length}`
    );
  } else {
    console.warn(
      `⚠️  名前ベースで一意候補を採用（スコア中程度）: name="${name}", topScore=${top.score}`
    );
  }

  return { ref: top.ref, data: top.data };
}

// ==============================
// セルごとの値パターン判定（臨機応変対応）
// ==============================

function detectFieldFromValue(value: string): string | null {
  const v = value.trim();
  if (!v) return null;

  // 法人番号（13桁の数字）
  if (/^\d{13}$/.test(v.replace(/\D/g, "")) && v.replace(/\D/g, "").length === 13) {
    return "corporateNumber";
  }

  // 郵便番号（XXX-XXXX or XXXXXXX）
  if (/^\d{3}-?\d{4}$/.test(v)) {
    return "postalCode";
  }

  // 電話番号（0で始まる9〜11桁）
  if (/^0\d/.test(v)) {
    const digits = v.replace(/\D/g, "");
    if (digits.length >= 9 && digits.length <= 11) {
      return "phoneNumber";
    }
  }

  // URL
  if (/^https?:\/\//i.test(v) || /\.(co\.jp|com|jp|net|org)/.test(v)) {
    return "companyUrl";
  }

  // メールアドレス
  if (/^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(v)) {
    return "email";
  }

  // 住所（都道府県で始まる）
  for (const pref of PREF_NAMES) {
    if (v.startsWith(pref)) {
      return "address";
    }
  }

  // 都道府県のみ
  if (PREF_NAMES.includes(v)) {
    return "prefecture";
  }

  // 業種パターン（〜業で終わる）
  if (/業$/.test(v) && v.length <= 30) {
    return "industry";
  }

  // 日付パターン（YYYY年MM月DD日、YYYY/MM/DD など）
  if (/\d{4}[年\/\-]\d{1,2}[月\/\-]?\d{0,2}/.test(v)) {
    return "established";
  }

  // 金額パターン（〜円、〜万円、〜百万円など）
  if (/[0-9,]+[万百千]?円/.test(v) || /[0-9,]+百万/.test(v)) {
    return "capitalStock";
  }

  // 会社名パターン
  if (v.includes("株式会社") || v.includes("有限会社") || v.includes("合同会社")) {
    return "name";
  }

  // 人名パターン（代表者系）- 短い漢字文字列
  if (/^[一-龥ぁ-んァ-ン]{2,10}$/.test(v) && !v.includes("株") && !v.includes("業")) {
    return "representativeName";
  }

  return null;
}

// 値がマッピング先と矛盾していないかチェック
function isValueConsistentWithField(value: string, field: string): boolean {
  const v = value.trim();
  if (!v) return true; // 空は何でもOK

  switch (field) {
    case "postalCode":
    case "representativePostalCode":
      return /^\d{3}-?\d{4}$/.test(v);
    case "phoneNumber":
    case "fax":
    case "contactPhoneNumber":
    case "representativePhone":
      return /^0\d/.test(v) && v.replace(/\D/g, "").length >= 9;
    case "corporateNumber":
      return /^\d{13}$/.test(v.replace(/\D/g, ""));
    case "companyUrl":
    case "contactFormUrl":
      return /^https?:\/\//i.test(v) || v.includes(".");
    case "email":
      return v.includes("@");
    case "address":
    case "headquartersAddress":
    case "representativeHomeAddress":
      return PREF_NAMES.some((p) => v.includes(p)) || /[市区町村]/.test(v);
    case "prefecture":
      return PREF_NAMES.includes(v) || PREF_NAMES.some((p) => v.startsWith(p));
    case "industry":
    case "industryLarge":
    case "industryMiddle":
    case "industrySmall":
    case "industryDetail":
      return /業/.test(v) || /サービス|製造|建設|情報|通信|金融|不動産/.test(v);
    // 以下のフィールドは任意の文字列を許容
    case "name":
    case "representativeName":
    case "representativeBirthDate":
    case "businessDescriptions":
    case "established":
    case "shareholders":
    case "executives":
    case "overview":
    case "companyDescription":
    case "tradingStatus":
      return true;
    default:
      return true;
  }
}

// ==============================
// CSV 1 行 → 更新データ生成（推測済みマップを使う + 値パターン判定）
// ==============================

function buildUpdateFromCsvRow(
  row: CsvRow,
  headerToField: Record<string, string | null>,
  filePath: string = ""
): Record<string, any> {
  const update: Record<string, any> = {};
  const usedFields = new Set<string>(); // 重複防止
  
  // 削除対象のURL
  const DELETE_URL = "https://valuesearch.nikkei.com/vs.assets/help/views/customer-support.html";

  // Pass 1: ヘッダーマッピングに従って値を設定（値が矛盾しない場合のみ）
  for (const [headerRaw, valueRaw] of Object.entries(row)) {
    const header = headerRaw.trim();
    const mappedField = headerToField[header];

    if (!mappedField) continue;
    if (!(mappedField in COMPANY_TEMPLATE)) continue;
    if (usedFields.has(mappedField)) continue;

    const trimmed = trim(valueRaw);
    if (trimmed == null) continue;
    
    // 指定URLを含むフィールドはスキップ（削除）
    if (trimmed.includes(DELETE_URL)) {
      continue;
    }

    // 値がマッピング先と矛盾していないかチェック
    if (isValueConsistentWithField(trimmed, mappedField)) {
      // 法人番号は特別処理（要件2: 13桁の数値のみ有効）
      if (mappedField === 'corporateNumber') {
        const validated = validateCorporateNumber(trimmed);
        if (validated) {
          update[mappedField] = validated;
          usedFields.add(mappedField);
        }
      } else if (NUMERIC_FIELDS.has(mappedField)) {
        // 財務数値フィールドの場合は単位変換を適用
        const num = parseFinancialNumeric(trimmed, null, filePath, mappedField);
        if (num !== null) {
          update[mappedField] = num;
          usedFields.add(mappedField);
        }
      } else {
        // テキストフィールドは最大長を制限
        const truncated = truncateFieldValue(mappedField, trimmed);
        update[mappedField] = truncated;
        usedFields.add(mappedField);
      }
    }
  }

    // Pass 2: マッピングされなかった or 矛盾した値を、値パターンから再判定
  for (const [headerRaw, valueRaw] of Object.entries(row)) {
    const header = headerRaw.trim();
    const mappedField = headerToField[header];
    const trimmed = trim(valueRaw);
    if (trimmed == null) continue;
    
    // 指定URLを含むフィールドはスキップ（削除）
    if (trimmed.includes(DELETE_URL)) {
      continue;
    }

    // すでに正しくマッピングされた場合はスキップ
    if (mappedField && usedFields.has(mappedField) && update[mappedField] === trimmed) {
      continue;
    }

    // 値パターンから適切なフィールドを推測
    const detectedField = detectFieldFromValue(trimmed);
    if (detectedField && detectedField in COMPANY_TEMPLATE && !usedFields.has(detectedField)) {
      // 法人番号は特別処理（要件2: 13桁の数値のみ有効）
      if (detectedField === 'corporateNumber') {
        const validated = validateCorporateNumber(trimmed);
        if (validated) {
          update[detectedField] = validated;
          usedFields.add(detectedField);
        }
      } else if (NUMERIC_FIELDS.has(detectedField)) {
        // 財務数値フィールドの場合は単位変換を適用
        const num = parseFinancialNumeric(trimmed, null, filePath, detectedField);
        if (num !== null) {
          update[detectedField] = num;
          usedFields.add(detectedField);
        }
      } else {
        // テキストフィールドは最大長を制限
        const truncated = truncateFieldValue(detectedField, trimmed);
        update[detectedField] = truncated;
        usedFields.add(detectedField);
      }
    }
  }

  return update;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード: Firestore は書き換えません\n" : "⚠️  本番モード: Firestore を書き換えます\n");

  const csvFiles = collectCsvFiles();

  let totalRows = 0;
  let updatedCount = 0;
  let createdCount = 0;       // ⭐ 企業が見つからず新規作成した件数
  let notFoundCount = 0;      // corporateNumber で見つからなかった件数
  let unresolvedCount = 0;    // 名前等でも既存ドキュメントが見つからなかった件数（新規作成に切替）
  let globalRowIndex = 0;     // generateNumericDocId 用のグローバルインデックス

  for (const file of csvFiles) {
    console.log(`\n📥 CSV 読み込み開始: ${file}`);
    
    // タイプCのCSVかどうかを判定
    const isTypeC = isTypeCCSV(file);
    
    if (isTypeC) {
      // タイプC: 列順序ベースで処理
      const records = loadTypeCCSVByIndex(file);
      totalRows += records.length - 1; // ヘッダー行を除く

      if (records.length <= 1) continue; // ヘッダーのみの場合はスキップ

      console.log("🔎 タイプC: 列順序ベースでマッピング（インデックス9を「取引先」として処理）");

      // === 行ごとの更新処理（ヘッダー行をスキップ） ===
      for (let idx = 1; idx < records.length; idx++) {
        const row = records[idx];
        globalRowIndex++;
        
        // 列順序ベースでマッピング
        const mapped = mapTypeCRowByIndex(row);
        
        if (!mapped.name) {
          console.warn(`⚠️  [${path.basename(file)} row ${idx + 1}] 会社名がありません`);
          continue;
        }

        // corporateNumberが13桁でない場合はnullにする
        if (mapped.corporateNumber) {
          const validated = validateCorporateNumber(mapped.corporateNumber);
          if (!validated) {
            mapped.corporateNumber = null;
          } else {
            mapped.corporateNumber = validated;
          }
        }

        // 企業の特定: 企業名・住所・代表者名・法人番号・郵便番号・電話番号・URLなどで特定
        let found: { ref: DocumentReference; data: any; candidates?: Candidate[]; allCandidates?: any[] } | null = null;

        // ① 法人番号で検索（13桁の場合のみ）
        if (mapped.corporateNumber) {
          found = await findCompanyDocByCorporateNumber(mapped.corporateNumber);
          if (!found) {
            notFoundCount++;
            if (notFoundCount <= 10) {
              console.warn(
                `⚠️  [${path.basename(file)} row ${idx + 1}] corporateNumber="${mapped.corporateNumber}" に該当ドキュメントなし`
              );
            }
          }
        }

        // ② 法人番号がない or 見つからなかった場合 → 企業名・住所・代表者名で検索
        if (!found) {
          const byName = await findCompanyDocByNameAndMetaForTypeE(
            mapped.name,
            mapped.address,
            mapped.representativeName,
            mapped.corporateNumber,
            mapped.postalCode,
            mapped.contactPhoneNumber,
            mapped.companyUrl,
            mapped.prefecture
          );
          
          if (!byName) {
            unresolvedCount++;
            // 新規作成
            const docId = generateNumericDocId(mapped.corporateNumber, globalRowIndex);
            const newRef = db.collection(COLLECTION_NAME).doc(docId);
            
            const newData: Record<string, any> = {
              ...COMPANY_TEMPLATE,
              ...mapped,
              csvType: "type_c",
              createdAt: admin.firestore.FieldValue.serverTimestamp(),
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            };
            
            if (DRY_RUN) {
              if (createdCount < 20) {
                console.log(`📝 (DRY_RUN) 新規作成予定: docId="${docId}", name="${mapped.name}"`);
              }
            } else {
              await newRef.set(newData);
            }
            
            createdCount++;
            if (createdCount % 100 === 0) {
              console.log(`  🆕 ここまでの新規作成件数: ${createdCount} 件`);
            }

            continue;
          }
          found = byName;
        }

        // === 既存ドキュメントが見つかった場合の更新処理 ===
        const { ref, data: current, allCandidates } = found;
        
        // 複数の候補がある場合、統合処理を実行
        let documentsToMerge: Array<{ ref: DocumentReference; data: any }> = [];
        if (allCandidates && allCandidates.length > 1) {
          // スコアが高い候補を統合対象とする（スコア70以上、または上位2つが同じ企業の可能性がある場合）
          const topScore = allCandidates[0].score;
          const secondScore = allCandidates[1]?.score || 0;
          
          // 高スコアの候補を統合対象に追加
          for (const candidate of allCandidates) {
            if (candidate.score >= 70 || (candidate.score >= 50 && Math.abs(candidate.score - topScore) <= 20)) {
              documentsToMerge.push({ ref: candidate.ref, data: candidate.data });
            }
          }
          
          // 重複を除去（同じrefは1つだけ）
          const seenRefs = new Set<string>();
          documentsToMerge = documentsToMerge.filter(doc => {
            if (seenRefs.has(doc.ref.id)) {
              return false;
            }
            seenRefs.add(doc.ref.id);
            return true;
          });
          
          // 統合対象が空の場合は、最初の候補を使用
          if (documentsToMerge.length === 0 && allCandidates.length > 0) {
            documentsToMerge = [{ ref: allCandidates[0].ref, data: allCandidates[0].data }];
          }
        } else {
          documentsToMerge = [{ ref, data: current }];
        }

        // 統合先のドキュメント（スコアが最も高いもの）
        if (documentsToMerge.length === 0) {
          console.warn(`⚠️  [${path.basename(file)} row ${idx + 1}] 統合対象のドキュメントが見つかりませんでした`);
          continue;
        }
        
        const targetDoc = documentsToMerge[0];
        const targetRef = targetDoc.ref;
        const targetData = targetDoc.data;

        // 複数のドキュメントを統合して、各フィールドが多く埋まっている状態にする
        // CSVから入れたフィールド以外の値があれば統合する
        let mergedData: Record<string, any> = { ...targetData };
        
        // CSVから取り込むフィールドのセットを作成
        const csvFields = new Set(Object.keys(mapped));
        
        // 統合対象のドキュメントから情報をマージ（CSVから入れたフィールド以外の値があれば統合）
        for (let i = 1; i < documentsToMerge.length; i++) {
          const doc = documentsToMerge[i];
          for (const [field, value] of Object.entries(doc.data)) {
            // CSVから取り込むフィールドは統合しない（CSVの値で上書きするため）
            if (csvFields.has(field)) {
              continue;
            }
            
            const currentValue = mergedData[field];
            
            // 既存の値が空で、新しい値がある場合は採用
            if (isEmptyValue(currentValue) && !isEmptyValue(value)) {
              mergedData[field] = value;
            }
            // 配列の場合は統合
            else if (Array.isArray(currentValue) && Array.isArray(value)) {
              const combined = [...new Set([...currentValue, ...value])];
              mergedData[field] = combined;
            }
            // 文字列の場合は長い方を採用
            else if (typeof currentValue === "string" && typeof value === "string") {
              if (value.length > currentValue.length) {
                mergedData[field] = value;
              }
            }
          }
        }

        // CSVからマッピングされたデータを全て置き換える（今回取り込むフィールドを正とする）
        const updateData: Record<string, any> = {};
        for (const [field, csvValue] of Object.entries(mapped)) {
          // CSVに値がある場合は常にCSVを優先（置き換え）
          if (!isEmptyValue(csvValue)) {
            if (field === "corporateNumber") {
              // corporateNumberは13桁の場合のみ設定、そうでない場合はnull
              const validated = validateCorporateNumber(csvValue);
              if (validated) {
                updateData[field] = validated;
              } else {
                updateData[field] = null;
              }
            } else {
              updateData[field] = csvValue;
            }
          } else if (field === "corporateNumber" && mergedData[field]) {
            // CSVにcorporateNumberがない場合、既存値が13桁でない場合はnullにする
            const validated = validateCorporateNumber(mergedData[field]);
            if (!validated) {
              updateData[field] = null;
            }
          }
        }

        // CSVから取り込まないフィールドで、統合した値があれば保持する
        for (const [field, value] of Object.entries(mergedData)) {
          // CSVから取り込むフィールドは既にupdateDataに設定されているのでスキップ
          if (csvFields.has(field)) {
            continue;
          }
        }

        // corporateNumberが13桁でない場合はnullにする
        if (updateData.corporateNumber) {
          const validated = validateCorporateNumber(updateData.corporateNumber);
          if (!validated) {
            updateData.corporateNumber = null;
          } else {
            updateData.corporateNumber = validated;
          }
        } else if (mergedData.corporateNumber) {
          const validated = validateCorporateNumber(mergedData.corporateNumber);
          if (!validated) {
            updateData.corporateNumber = null;
          }
        }

        // tagsをクリーンアップ（既存のtagsも含めて）
        if (mergedData?.tags) {
          const cleanedTags = cleanTags(mergedData.tags);
          if (cleanedTags.length !== mergedData.tags.length || JSON.stringify(cleanedTags) !== JSON.stringify(mergedData.tags)) {
            updateData.tags = cleanedTags;
          }
        }
        if (updateData.tags) {
          updateData.tags = cleanTags(updateData.tags);
        }

        // csvTypeを設定
        updateData.csvType = "type_c";
        updateData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

        // 統合対象のドキュメントが複数ある場合、不要なドキュメントを削除
        if (documentsToMerge.length > 1 && !DRY_RUN) {
          const batch = db.batch();
          for (let i = 1; i < documentsToMerge.length; i++) {
            batch.delete(documentsToMerge[i].ref);
          }
          await batch.commit();
          console.log(
            `🔄 タイプC: ${documentsToMerge.length - 1}件の重複ドキュメントを削除しました (統合先: ${targetRef.id})`
          );
        }

        if (Object.keys(updateData).length === 0) continue;

        // 更新後のドキュメントサイズをチェック
        const finalData = { ...mergedData, ...updateData };
        const estimatedSize = JSON.stringify(finalData).length;
        const MAX_DOC_SIZE = 1000000;
        if (estimatedSize > MAX_DOC_SIZE) {
          console.warn(
            `⚠️  [${path.basename(file)} row ${idx + 1}] 更新後のドキュメントサイズが大きすぎます: ${estimatedSize} bytes, docId="${targetRef.id}"`
          );
        }

        if (DRY_RUN) {
          if (updatedCount < 20) {
            console.log(
              `📝 (DRY_RUN) docId="${targetRef.id}" 更新予定:`,
              updateData
            );
            if (documentsToMerge.length > 1) {
              console.log(
                `  🔄 統合対象: ${documentsToMerge.map(d => d.ref.id).join(", ")}`
              );
            }
          }
        } else {
          await targetRef.update(updateData);
        }

        updatedCount++;
        if (updatedCount % 500 === 0) {
          console.log(`  ✅ ここまでの更新件数: ${updatedCount} 件`);
        }
      }
      
      continue; // タイプCの処理が完了したら次のファイルへ
    }
    
    // タイプEのCSVかどうかを判定
    const isTypeE = isTypeECSV(file);
    
    if (isTypeE) {
      // タイプE: 列順序ベースで処理
      const records = loadTypeECSVByIndex(file);
      totalRows += records.length - 1; // ヘッダー行を除く

      if (records.length <= 1) continue; // ヘッダーのみの場合はスキップ

      // ヘッダー行からURL列のインデックスを検出
      const headerRow = records[0];
      const urlColumnIndex = findUrlColumnIndex(headerRow);
      
      if (urlColumnIndex !== null) {
        console.log(`🔎 タイプE: 列順序ベースでマッピング（URL列インデックス: ${urlColumnIndex}）`);
      } else {
        console.log("🔎 タイプE: 列順序ベースでマッピング（URL列が見つかりませんでした）");
      }

      // === 行ごとの更新処理（ヘッダー行をスキップ） ===
      for (let idx = 1; idx < records.length; idx++) {
        const row = records[idx];
        globalRowIndex++;
        
        // 列順序ベースでマッピング（URL列のインデックスを渡す）
        const mapped = mapTypeERowByIndex(row, urlColumnIndex, file);
        
        if (!mapped.name) {
          console.warn(`⚠️  [${path.basename(file)} row ${idx + 1}] 会社名がありません`);
          continue;
        }

        // corporateNumberが13桁でない場合はnullにする
        if (mapped.corporateNumber) {
          const validated = validateCorporateNumber(mapped.corporateNumber);
          if (!validated) {
            mapped.corporateNumber = null;
          } else {
            mapped.corporateNumber = validated;
          }
        }

        // 企業の特定: 企業名・住所・代表者名・法人番号・郵便番号・電話番号・URLなどで特定
        let found: { ref: DocumentReference; data: any; candidates?: Candidate[]; allCandidates?: any[] } | null = null;

        // ① 法人番号で検索（13桁の場合のみ）
        if (mapped.corporateNumber) {
          found = await findCompanyDocByCorporateNumber(mapped.corporateNumber);
          if (!found) {
            notFoundCount++;
            if (notFoundCount <= 10) {
              console.warn(
                `⚠️  [${path.basename(file)} row ${idx + 1}] corporateNumber="${mapped.corporateNumber}" に該当ドキュメントなし`
              );
            }
          }
        }

        // ② 法人番号がない or 見つからなかった場合 → 企業名・都道府県・代表者名で検索
        if (!found) {
          const byName = await findCompanyDocByNameAndMetaForTypeE(
            mapped.name,
            mapped.address,
            mapped.representativeName,
            mapped.corporateNumber,
            mapped.postalCode,
            mapped.phoneNumber,
            mapped.companyUrl,
            mapped.prefecture
          );
          if (!byName) {
            unresolvedCount++;
            console.warn(
              `⚠️  [${path.basename(file)} row ${idx + 1}] 既存ドキュメントが見つからなかったため新規作成します (corporateNumber="${mapped.corporateNumber ?? ""}", name="${mapped.name}")`
            );

            // ⭐ 新規作成
            const newData: Record<string, any> = {
              ...COMPANY_TEMPLATE,
              ...mapped,
              csvType: "type_e",
              createdAt: admin.firestore.FieldValue.serverTimestamp(),
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            };

            // tagsをクリーンアップ
            if (newData.tags) {
              newData.tags = cleanTags(newData.tags);
            }

            // ドキュメントサイズをチェック
            const estimatedSize = JSON.stringify(newData).length;
            const MAX_DOC_SIZE = 1000000;
            if (estimatedSize > MAX_DOC_SIZE) {
              console.warn(
                `⚠️  [${path.basename(file)} row ${idx + 1}] ドキュメントサイズが大きすぎます: ${estimatedSize} bytes`
              );
            }

            // 数値のみのドキュメントIDを生成
            const docId = generateNumericDocId(mapped.corporateNumber, globalRowIndex);
            const newRef: DocumentReference = companiesCol.doc(docId);

            if (DRY_RUN) {
              if (createdCount < 20) {
                console.log(
                  `🆕 (DRY_RUN) 新規作成予定 docId="${docId}" データ:`,
                  newData
                );
              }
            } else {
              await newRef.set(newData);
            }

            createdCount++;
            if (createdCount % 500 === 0) {
              console.log(`  🆕 ここまでの新規作成件数: ${createdCount} 件`);
            }

            continue;
          }
          found = byName;
        }

        // === 既存ドキュメントが見つかった場合の更新処理 ===
        const { ref, data: current, allCandidates } = found;
        
        // 複数の候補がある場合、統合処理を実行
        let documentsToMerge: Array<{ ref: DocumentReference; data: any }> = [];
        if (allCandidates && allCandidates.length > 1) {
          // スコアが高い候補を統合対象とする（スコア70以上、または上位2つが同じ企業の可能性がある場合）
          const topScore = allCandidates[0].score;
          const secondScore = allCandidates[1]?.score || 0;
          
          // 高スコアの候補を統合対象に追加
          for (const candidate of allCandidates) {
            if (candidate.score >= 70 || (candidate.score >= 50 && Math.abs(candidate.score - topScore) <= 20)) {
              documentsToMerge.push({ ref: candidate.ref, data: candidate.data });
            }
          }
          
          // 重複を除去（同じrefは1つだけ）
          const seenRefs = new Set<string>();
          documentsToMerge = documentsToMerge.filter(doc => {
            if (seenRefs.has(doc.ref.id)) {
              return false;
            }
            seenRefs.add(doc.ref.id);
            return true;
          });
          
          // 統合対象が空の場合は、最初の候補を使用
          if (documentsToMerge.length === 0 && allCandidates.length > 0) {
            documentsToMerge = [{ ref: allCandidates[0].ref, data: allCandidates[0].data }];
          }
        } else {
          documentsToMerge = [{ ref, data: current }];
        }

        // 統合先のドキュメント（スコアが最も高いもの）
        if (documentsToMerge.length === 0) {
          console.warn(`⚠️  [${path.basename(file)} row ${idx + 1}] 統合対象のドキュメントが見つかりませんでした`);
          continue;
        }
        
        const targetDoc = documentsToMerge[0];
        const targetRef = targetDoc.ref;
        const targetData = targetDoc.data;

        // 複数のドキュメントを統合して、各フィールドが多く埋まっている状態にする
        // CSVから入れたフィールド以外の値があれば統合する
        let mergedData: Record<string, any> = { ...targetData };
        
        // CSVから取り込むフィールドのセットを作成
        const csvFields = new Set(Object.keys(mapped));
        
        // 統合対象のドキュメントから情報をマージ（CSVから入れたフィールド以外の値があれば統合）
        for (let i = 1; i < documentsToMerge.length; i++) {
          const doc = documentsToMerge[i];
          for (const [field, value] of Object.entries(doc.data)) {
            // CSVから取り込むフィールドは統合しない（CSVの値で上書きするため）
            if (csvFields.has(field)) {
              continue;
            }
            
            const currentValue = mergedData[field];
            
            // 既存の値が空で、新しい値がある場合は採用
            if (isEmptyValue(currentValue) && !isEmptyValue(value)) {
              mergedData[field] = value;
            }
            // 配列の場合は統合
            else if (Array.isArray(currentValue) && Array.isArray(value)) {
              const combined = [...new Set([...currentValue, ...value])];
              mergedData[field] = combined;
            }
            // 文字列の場合は長い方を採用
            else if (typeof currentValue === "string" && typeof value === "string") {
              if (value.length > currentValue.length) {
                mergedData[field] = value;
              }
            }
          }
        }

        // CSVからマッピングされたデータを全て置き換える（今回取り込むフィールドを正とする）
        const updateData: Record<string, any> = {};
        for (const [field, csvValue] of Object.entries(mapped)) {
          // CSVに値がある場合は常にCSVを優先（置き換え）
          if (!isEmptyValue(csvValue)) {
            if (field === "corporateNumber") {
              // corporateNumberは13桁の場合のみ設定、そうでない場合はnull
              const validated = validateCorporateNumber(csvValue);
              if (validated) {
                updateData[field] = validated;
              } else {
                updateData[field] = null;
              }
            } else {
              updateData[field] = csvValue;
            }
          } else if (field === "corporateNumber" && mergedData[field]) {
            // CSVにcorporateNumberがない場合、既存値が13桁でない場合はnullにする
            const validated = validateCorporateNumber(mergedData[field]);
            if (!validated) {
              updateData[field] = null;
            }
          }
        }
        
        // CSVから取り込まないフィールドで、統合した値があれば保持する
        for (const [field, value] of Object.entries(mergedData)) {
          // CSVから取り込むフィールドは既にupdateDataに設定されているのでスキップ
          if (csvFields.has(field)) {
            continue;
          }
          
          // CSVから取り込まないフィールドで、統合した値があれば保持
          // （updateDataには設定しないが、mergedDataに保持されているので最終的なドキュメントに含まれる）
        }

        // corporateNumberが13桁でない場合はnullにする
        if (updateData.corporateNumber) {
          const validated = validateCorporateNumber(updateData.corporateNumber);
          if (!validated) {
            updateData.corporateNumber = null;
          } else {
            updateData.corporateNumber = validated;
          }
        } else if (mergedData.corporateNumber) {
          const validated = validateCorporateNumber(mergedData.corporateNumber);
          if (!validated) {
            updateData.corporateNumber = null;
          }
        }

        // tagsをクリーンアップ（既存のtagsも含めて）
        if (mergedData?.tags) {
          const cleanedTags = cleanTags(mergedData.tags);
          if (cleanedTags.length !== mergedData.tags.length || JSON.stringify(cleanedTags) !== JSON.stringify(mergedData.tags)) {
            updateData.tags = cleanedTags;
          }
        }
        if (updateData.tags) {
          updateData.tags = cleanTags(updateData.tags);
        }

        // csvTypeを設定
        updateData.csvType = "type_e";
        updateData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

        // 統合対象のドキュメントが複数ある場合、不要なドキュメントを削除
        if (documentsToMerge.length > 1 && !DRY_RUN) {
          const batch = db.batch();
          for (let i = 1; i < documentsToMerge.length; i++) {
            batch.delete(documentsToMerge[i].ref);
          }
          await batch.commit();
          console.log(
            `🔄 タイプE: ${documentsToMerge.length - 1}件の重複ドキュメントを削除しました (統合先: ${targetRef.id})`
          );
        }

        // updateDataが空でも、csvTypeとupdatedAtは設定されているので、必ず更新する
        // （CSVからマッピングされたデータが空でも、csvTypeとupdatedAtで更新する）
        if (Object.keys(updateData).length === 0) {
          console.warn(`⚠️  [${path.basename(file)} row ${idx + 1}] updateDataが空です (name="${mapped.name}")`);
          continue;
        }

        // 更新後のドキュメントサイズをチェック
        const finalData = { ...mergedData, ...updateData };
        const estimatedSize = JSON.stringify(finalData).length;
        const MAX_DOC_SIZE = 1000000;
        if (estimatedSize > MAX_DOC_SIZE) {
          console.warn(
            `⚠️  [${path.basename(file)} row ${idx + 1}] 更新後のドキュメントサイズが大きすぎます: ${estimatedSize} bytes, docId="${targetRef.id}"`
          );
        }

        if (DRY_RUN) {
          if (updatedCount < 20) {
            console.log(
              `📝 (DRY_RUN) docId="${targetRef.id}" 更新予定:`,
              updateData
            );
            if (documentsToMerge.length > 1) {
              console.log(
                `  🔄 統合対象: ${documentsToMerge.map(d => d.ref.id).join(", ")}`
              );
            }
          }
        } else {
          await targetRef.update(updateData);
        }

        updatedCount++;
        if (updatedCount % 500 === 0) {
          console.log(`  ✅ ここまでの更新件数: ${updatedCount} 件`);
        }
      }
      
      continue; // タイプEの処理が完了したら次のファイルへ
    }
    
    // タイプFのCSVかどうかを判定
    const isTypeF = isTypeFCSV(file);
    
    if (isTypeF) {
      // タイプF: 列順序ベースで処理（会社名・都道府県・代表者名で特定）
      const records = loadTypeFCSVByIndex(file);
      totalRows += records.length - 1; // ヘッダー行を除く

      if (records.length <= 1) continue; // ヘッダーのみの場合はスキップ

      console.log("🔎 タイプF: 列順序ベースでマッピング（会社名・都道府県・代表者名で特定）");

      // === 行ごとの更新処理（ヘッダー行をスキップ） ===
      for (let idx = 1; idx < records.length; idx++) {
        const row = records[idx];
        globalRowIndex++;
        
        // 列順序ベースでマッピング
        const mapped = mapTypeFRowByIndex(row, file);
        
        if (!mapped.name) {
          console.warn(`⚠️  [${path.basename(file)} row ${idx + 1}] 会社名がありません`);
          continue;
        }

        // 企業の特定: 会社名・都道府県・代表者名で特定
        let found: { ref: DocumentReference; data: any; candidates?: Candidate[]; allCandidates?: any[] } | null = null;

        // 会社名・都道府県・代表者名で検索
        found = await findCompanyDocByNamePrefectureRepresentative(
          mapped.name,
          mapped.prefecture,
          mapped.representativeName
        );
        
        if (!found) {
          unresolvedCount++;
          console.warn(
            `⚠️  [${path.basename(file)} row ${idx + 1}] 既存ドキュメントが見つからなかったため新規作成します (name="${mapped.name}", prefecture="${mapped.prefecture ?? ""}", representativeName="${mapped.representativeName ?? ""}")`
          );

          // ⭐ 新規作成
          const newData: Record<string, any> = {
            ...COMPANY_TEMPLATE,
            ...mapped,
            csvType: "type_f",
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          };

          // tagsをクリーンアップ
          if (newData.tags) {
            newData.tags = cleanTags(newData.tags);
          }

          // ドキュメントサイズをチェック
          const estimatedSize = JSON.stringify(newData).length;
          const MAX_DOC_SIZE = 1000000;
          if (estimatedSize > MAX_DOC_SIZE) {
            console.warn(
              `⚠️  [${path.basename(file)} row ${idx + 1}] ドキュメントサイズが大きすぎます: ${estimatedSize} bytes`
            );
          }

          // 数値のみのドキュメントIDを生成
          const docId = generateNumericDocId(mapped.corporateNumber, globalRowIndex);
          const newRef: DocumentReference = companiesCol.doc(docId);

          if (DRY_RUN) {
            if (createdCount < 20) {
              console.log(
                `🆕 (DRY_RUN) 新規作成予定 docId="${docId}" データ:`,
                newData
              );
            }
          } else {
            await newRef.set(newData);
          }

          createdCount++;
          if (createdCount % 500 === 0) {
            console.log(`  🆕 ここまでの新規作成件数: ${createdCount} 件`);
          }

          continue;
        }

        // === 既存ドキュメントが見つかった場合の更新処理 ===
        const { ref, data: current, allCandidates } = found;
        
        // 複数の候補がある場合、統合処理を実行
        let documentsToMerge: Array<{ ref: DocumentReference; data: any }> = [];
        if (allCandidates && allCandidates.length > 1) {
          // スコアが高い候補を統合対象とする（スコア60以上、または上位2つが同じ企業の可能性がある場合）
          const topScore = allCandidates[0]?.score || 0;
          const secondScore = allCandidates[1]?.score || 0;
          
          // 高スコアの候補を統合対象に追加
          for (const candidate of allCandidates) {
            if (candidate && candidate.ref && candidate.data) {
              if (candidate.score >= 60 || (candidate.score >= 30 && Math.abs(candidate.score - topScore) <= 20)) {
                documentsToMerge.push({ ref: candidate.ref, data: candidate.data });
              }
            }
          }
          
          // 重複を除去（同じrefは1つだけ）
          const seenRefs = new Set<string>();
          documentsToMerge = documentsToMerge.filter(doc => {
            if (seenRefs.has(doc.ref.id)) {
              return false;
            }
            seenRefs.add(doc.ref.id);
            return true;
          });
        }
        
        // allCandidatesが空または条件に合致する候補がない場合は、見つかったドキュメントを使用
        if (documentsToMerge.length === 0) {
          if (ref && current) {
            documentsToMerge = [{ ref, data: current }];
          } else {
            console.warn(
              `⚠️  [${path.basename(file)} row ${idx + 1}] 統合対象のドキュメントが見つかりませんでした。スキップします。`
            );
            continue;
          }
        }

        // 統合先のドキュメント（スコアが最も高いもの）
        const targetDoc = documentsToMerge[0];
        const targetRef = targetDoc.ref;
        const targetData = targetDoc.data;

        // 複数のドキュメントを統合して、各フィールドが多く埋まっている状態にする
        // CSVから入れたフィールド以外の値があれば統合する
        let mergedData: Record<string, any> = { ...targetData };
        
        // CSVから取り込むフィールドのセットを作成
        const csvFields = new Set(Object.keys(mapped));
        
        // 統合対象のドキュメントから情報をマージ（CSVから入れたフィールド以外の値があれば統合）
        for (let i = 1; i < documentsToMerge.length; i++) {
          const doc = documentsToMerge[i];
          for (const [field, value] of Object.entries(doc.data)) {
            // CSVから取り込むフィールドは統合しない（CSVの値で上書きするため）
            if (csvFields.has(field)) {
              continue;
            }
            
            const currentValue = mergedData[field];
            
            // 既存の値が空で、新しい値がある場合は採用
            if (isEmptyValue(currentValue) && !isEmptyValue(value)) {
              mergedData[field] = value;
            }
            // 配列の場合は統合
            else if (Array.isArray(currentValue) && Array.isArray(value)) {
              const combined = [...new Set([...currentValue, ...value])];
              mergedData[field] = combined;
            }
            // 文字列の場合は長い方を採用
            else if (typeof currentValue === "string" && typeof value === "string") {
              if (value.length > currentValue.length) {
                mergedData[field] = value;
              }
            }
          }
        }

        // CSVからマッピングされたデータを全て置き換える（今回取り込むフィールドを正とする）
        const updateData: Record<string, any> = {};
        for (const [field, csvValue] of Object.entries(mapped)) {
          // CSVに値がある場合は常にCSVを優先（置き換え）
          if (!isEmptyValue(csvValue)) {
            if (field === "corporateNumber") {
              // corporateNumberは13桁の場合のみ設定、そうでない場合はnull
              const validated = validateCorporateNumber(csvValue);
              if (validated) {
                updateData[field] = validated;
              } else {
                updateData[field] = null;
              }
            } else {
              updateData[field] = csvValue;
            }
          } else if (field === "corporateNumber" && mergedData[field]) {
            // CSVにcorporateNumberがない場合、既存値が13桁でない場合はnullにする
            const validated = validateCorporateNumber(mergedData[field]);
            if (!validated) {
              updateData[field] = null;
            }
          }
        }
        
        // CSVから取り込まないフィールドで、統合した値があれば保持する
        for (const [field, value] of Object.entries(mergedData)) {
          // CSVから取り込むフィールドは既にupdateDataに設定されているのでスキップ
          if (csvFields.has(field)) {
            continue;
          }
        }

        // corporateNumberが13桁でない場合はnullにする
        if (updateData.corporateNumber) {
          const validated = validateCorporateNumber(updateData.corporateNumber);
          if (!validated) {
            updateData.corporateNumber = null;
          } else {
            updateData.corporateNumber = validated;
          }
        } else if (mergedData.corporateNumber) {
          const validated = validateCorporateNumber(mergedData.corporateNumber);
          if (!validated) {
            updateData.corporateNumber = null;
          }
        }

        // tagsをクリーンアップ（既存のtagsも含めて）
        if (mergedData?.tags) {
          const cleanedTags = cleanTags(mergedData.tags);
          if (cleanedTags.length !== mergedData.tags.length || JSON.stringify(cleanedTags) !== JSON.stringify(mergedData.tags)) {
            updateData.tags = cleanedTags;
          }
        }
        if (updateData.tags) {
          updateData.tags = cleanTags(updateData.tags);
        }

        // csvTypeを設定
        updateData.csvType = "type_f";
        updateData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

        // 統合対象のドキュメントが複数ある場合、不要なドキュメントを削除
        if (documentsToMerge.length > 1 && !DRY_RUN) {
          const batch = db.batch();
          for (let i = 1; i < documentsToMerge.length; i++) {
            batch.delete(documentsToMerge[i].ref);
          }
          await batch.commit();
          console.log(
            `🔄 タイプF: ${documentsToMerge.length - 1}件の重複ドキュメントを削除しました (統合先: ${targetRef.id})`
          );
        }

        if (Object.keys(updateData).length === 0) continue;

        // 更新後のドキュメントサイズをチェック
        const finalData = { ...mergedData, ...updateData };
        const estimatedSize = JSON.stringify(finalData).length;
        const MAX_DOC_SIZE = 1000000;
        if (estimatedSize > MAX_DOC_SIZE) {
          console.warn(
            `⚠️  [${path.basename(file)} row ${idx + 1}] 更新後のドキュメントサイズが大きすぎます: ${estimatedSize} bytes, docId="${targetRef.id}"`
          );
        }

        if (DRY_RUN) {
          if (updatedCount < 20) {
            console.log(
              `📝 (DRY_RUN) docId="${targetRef.id}" 更新予定:`,
              updateData
            );
            if (documentsToMerge.length > 1) {
              console.log(
                `  🔄 統合対象: ${documentsToMerge.map(d => d.ref.id).join(", ")}`
              );
            }
          }
        } else {
          await targetRef.update(updateData);
        }

        updatedCount++;
        if (updatedCount % 500 === 0) {
          console.log(`  ✅ ここまでの更新件数: ${updatedCount} 件`);
        }
      }
      
      continue; // タイプFの処理が完了したら次のファイルへ
    }

    // タイプGのCSVかどうかを判定
    const isTypeG = isTypeGCSV(file);
    
    if (isTypeG) {
      // タイプG: 企業名で特定、JSON形式のフィールドは上書き
      const rows = loadCsvRows(file);
      totalRows += rows.length;

      if (rows.length === 0) continue;

      console.log("🔎 タイプG: 企業名で特定、JSON形式のフィールドは上書き");

      // === ヘッダーごとに値サンプルを集めて、フィールド推測マップを作成 ===
      const headerToSamples: Record<string, string[]> = {};
      const headers = Object.keys(rows[0]);

      for (const h of headers) {
        headerToSamples[h] = [];
      }

      for (const row of rows) {
        for (const h of headers) {
          const v = row[h];
          if (v && headerToSamples[h].length < 50) {
            headerToSamples[h].push(v);
          }
        }
      }

      const headerToField: Record<string, string | null> = {};
      console.log("🔎 ヘッダーごとのフィールド推測:");
      for (const h of headers) {
        const field = inferFieldForHeader(h, headerToSamples[h] || []);
        headerToField[h] = field;
        if (field) {
          console.log(`  - "${h}" => "${field}"`);
        } else {
          console.log(`  - "${h}" => (未マッピング)`);
        }
      }

      // === 行ごとの更新処理 ===
      let idx = 0;
      for (const row of rows) {
        idx++;
        globalRowIndex++;
        
        let name =
          trim(row["会社名"]) ??
          trim(row["企業名"]) ??
          trim(row["name"]);
        
        if (!name) {
          console.warn(`⚠️  [${path.basename(file)} row ${idx}] 会社名がありません`);
          continue;
        }

        // 「（株）」を「株式会社」に変換
        const normalizedName = normalizeCompanyNameFormat(name);
        if (normalizedName) {
          name = normalizedName;
        }

        const corporateNumber =
          trim(row["corporateNumber"]) ??
          trim(row["法人番号"]) ??
          trim(row["corporate_number"]);

        // 「日経バリューサーチ」の処理
        const isNikkeiValueSearch = name === "日経バリューサーチ" || name.includes("日経バリューサーチ");
        let extractedName: string | null = null;
        let jsonExtractedFields: Record<string, any> = {};

        if (isNikkeiValueSearch) {
          // CSVデータから企業名を抽出
          const csvData = buildUpdateFromCsvRow(row, headerToField, file);
          
          // ① フィールド内から企業名を抽出
          extractedName = extractCompanyNameFromFields(csvData);
          
          // ② JSON形式のフィールドから企業名とフィールド情報を抽出
          if (!extractedName) {
            // JSON形式のフィールドを探す
            for (const [field, value] of Object.entries(csvData)) {
              if (isJsonValue(value)) {
                const jsonName = extractCompanyNameFromJson(value);
                if (jsonName) {
                  extractedName = jsonName;
                  jsonExtractedFields = extractFieldsFromJson(value, file);
                  console.log(`  📝 [${path.basename(file)} row ${idx}] JSONから企業名を抽出: "${extractedName}"`);
                  break;
                }
              }
            }
          }
          
          // ③ 企業HPから企業名を取得
          if (!extractedName) {
            const url = csvData.companyUrl || row["URL"] || row["contactUrl"];
            if (url) {
              extractedName = await extractCompanyNameFromUrl(url);
              if (extractedName) {
                console.log(`  📝 [${path.basename(file)} row ${idx}] URLから企業名を抽出: "${extractedName}"`);
              }
            }
          }

          if (extractedName) {
            name = extractedName;
          } else {
            console.warn(`  ⚠️  [${path.basename(file)} row ${idx}] 企業名を抽出できませんでした`);
          }
        }

        // タイプG: 企業名で特定（法人番号は補助的に使用）
        let found: { ref: DocumentReference; data: any } | null = null;

        // ① 企業名で検索（優先）
        found = await findCompanyDocByNameForTypeG(name, corporateNumber);

        if (!found) {
          unresolvedCount++;
          console.warn(
            `⚠️  [${path.basename(file)} row ${idx}] 既存ドキュメントが見つからなかったため新規作成します (name="${name}", corporateNumber="${corporateNumber ?? ""}")`
          );

          // ⭐ 新規作成
          const csvDataForNew = buildUpdateFromCsvRow(row, headerToField, file);

          // 「日経バリューサーチ」の場合、JSONから抽出したフィールドをマージ
          if (isNikkeiValueSearch && Object.keys(jsonExtractedFields).length > 0) {
            Object.assign(csvDataForNew, jsonExtractedFields);
          }

          // name が mapping されていない場合の保険
          if (!csvDataForNew.name) {
            csvDataForNew.name = truncateFieldValue("name", name);
          }

          // corporateNumber フィールドも必ず持たせる
          if (corporateNumber && !csvDataForNew.corporateNumber) {
            const validated = validateCorporateNumber(corporateNumber);
            if (validated) {
              csvDataForNew.corporateNumber = validated;
            }
          }

          const newData: Record<string, any> = {
            ...COMPANY_TEMPLATE,
            ...csvDataForNew,
            csvType: "type_g",
            createdAt: admin.firestore.FieldValue.serverTimestamp(),
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          };

          // tagsをクリーンアップ
          if (newData.tags) {
            newData.tags = cleanTags(newData.tags);
          }

          // ドキュメントサイズをチェック
          const estimatedSize = JSON.stringify(newData).length;
          const MAX_DOC_SIZE = 1000000;
          if (estimatedSize > MAX_DOC_SIZE) {
            console.warn(
              `⚠️  [${path.basename(file)} row ${idx}] ドキュメントサイズが大きすぎます: ${estimatedSize} bytes`
            );
          }

          // 数値のみのドキュメントIDを生成
          const docId = generateNumericDocId(corporateNumber, globalRowIndex);
          const newRef: DocumentReference = companiesCol.doc(docId);

          if (DRY_RUN) {
            if (createdCount < 20) {
              console.log(
                `🆕 (DRY_RUN) 新規作成予定 docId="${docId}" データ:`,
                newData
              );
            }
          } else {
            await newRef.set(newData);
          }

          createdCount++;
          if (createdCount % 500 === 0) {
            console.log(`  🆕 ここまでの新規作成件数: ${createdCount} 件`);
          }

          continue;
        }

        // === 既存ドキュメントが見つかった場合の更新処理 ===
        const { ref, data: current } = found;
        const csvData = buildUpdateFromCsvRow(row, headerToField, file);
        const updateData: Record<string, any> = {};

        // 「日経バリューサーチ」の場合、JSONから抽出したフィールドをマージ
        if (isNikkeiValueSearch && Object.keys(jsonExtractedFields).length > 0) {
          Object.assign(csvData, jsonExtractedFields);
        }
        
        // 指定URLを含むフィールドを削除
        const DELETE_URL = "https://valuesearch.nikkei.com/vs.assets/help/views/customer-support.html";
        for (const [field, value] of Object.entries(current)) {
          if (typeof value === "string" && value.includes(DELETE_URL)) {
            updateData[field] = admin.firestore.FieldValue.delete();
            console.log(`  🗑️  [${path.basename(file)} row ${idx}] フィールド "${field}" を削除（指定URLを含む）`);
          }
        }
        
        // 既存ドキュメント内のJSON形式のフィールドを解析して各フィールドに振り分け
        for (const [field, value] of Object.entries(current)) {
          if (isJsonValue(value)) {
            const jsonStr = typeof value === "string" ? value : JSON.stringify(value);
            const extractedFields = extractFieldsFromJson(jsonStr, file);
            // 抽出したフィールドをcsvDataにマージ（既存値が空またはJSON形式の場合のみ）
            for (const [extractedField, extractedValue] of Object.entries(extractedFields)) {
              if (extractedValue !== null && extractedValue !== undefined && extractedValue !== "") {
                const currentFieldValue = current?.[extractedField];
                if (!currentFieldValue || isJsonValue(currentFieldValue)) {
                  csvData[extractedField] = extractedValue;
                }
              }
            }
          }
        }

        // 既存のnameが「日経バリューサーチ」の場合は上書き
        const currentName = current?.name;
        const shouldUpdateName = isNikkeiValueSearch || 
          (currentName === "日経バリューサーチ" || currentName?.includes("日経バリューサーチ"));

        // 既存のnameフィールドに「（株）」が含まれている場合は正規化
        if (currentName && currentName.includes("（株）")) {
          const normalizedCurrentName = normalizeCompanyNameFormat(currentName);
          if (normalizedCurrentName && normalizedCurrentName !== currentName) {
            updateData["name"] = normalizedCurrentName;
          }
        }

        for (const [field, csvValue] of Object.entries(csvData)) {
          const curValue = current?.[field];
          
          // 指定URLを含む値はスキップ
          if (typeof csvValue === "string" && csvValue.includes("https://valuesearch.nikkei.com/vs.assets/help/views/customer-support.html")) {
            continue;
          }

          if (field === "name") {
            // nameは常に上書き（「日経バリューサーチ」の場合は特に）
            // CSVの値も正規化済みなので、そのまま使用
            if (shouldUpdateName || curValue !== csvValue) {
              updateData[field] = csvValue || name;
            }
          } else {
            // タイプGの特別処理: JSON形式の場合はCSVの内容で上書き
            if (isJsonValue(curValue)) {
              // 既存値がJSON形式の場合はCSVの内容で上書き
              if (csvValue !== undefined && csvValue !== null && csvValue !== "") {
                updateData[field] = csvValue;
              }
            } else if (
              curValue === undefined ||
              curValue === null ||
              curValue === ""
            ) {
              // 既存値が空の場合は補完
              if (csvValue !== undefined && csvValue !== null && csvValue !== "") {
                updateData[field] = csvValue;
              }
            }
            // 既存値が空でなく、JSON形式でもない場合はそのまま保持（更新しない）
          }
        }

        // tagsをクリーンアップ（既存のtagsも含めて）
        if (current?.tags) {
          const cleanedTags = cleanTags(current.tags);
          if (cleanedTags.length !== current.tags.length || JSON.stringify(cleanedTags) !== JSON.stringify(current.tags)) {
            updateData.tags = cleanedTags;
          }
        }
        if (updateData.tags) {
          updateData.tags = cleanTags(updateData.tags);
        }

        // csvTypeを設定
        updateData.csvType = "type_g";
        updateData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

        if (Object.keys(updateData).length === 0) continue;

        // 更新後のドキュメントサイズをチェック
        const mergedData = { ...current, ...updateData };
        const estimatedSize = JSON.stringify(mergedData).length;
        const MAX_DOC_SIZE = 1000000;
        if (estimatedSize > MAX_DOC_SIZE) {
          console.warn(
            `⚠️  [${path.basename(file)} row ${idx}] 更新後のドキュメントサイズが大きすぎます: ${estimatedSize} bytes, docId="${ref.id}"`
          );
        }

        if (DRY_RUN) {
          if (updatedCount < 20) {
            console.log(
              `📝 (DRY_RUN) docId="${ref.id}" 更新予定:`,
              updateData
            );
          }
        } else {
          await ref.update(updateData);
        }

        updatedCount++;
        if (updatedCount % 500 === 0) {
          console.log(`  ✅ ここまでの更新件数: ${updatedCount} 件`);
        }
      }
      
      continue; // タイプGの処理が完了したら次のファイルへ
    }

    // 通常のCSV（ヘッダーベース）の処理
    const rows = loadCsvRows(file);
    totalRows += rows.length;

    if (rows.length === 0) continue;

    // === ヘッダーごとに値サンプルを集めて、フィールド推測マップを作成 ===
    const headerToSamples: Record<string, string[]> = {};
    const headers = Object.keys(rows[0]);

    for (const h of headers) {
      headerToSamples[h] = [];
    }

    for (const row of rows) {
      for (const h of headers) {
        const v = row[h];
        if (v && headerToSamples[h].length < 50) {
          headerToSamples[h].push(v);
        }
      }
    }

    const headerToField: Record<string, string | null> = {};
    console.log("🔎 ヘッダーごとのフィールド推測:");
    for (const h of headers) {
      const field = inferFieldForHeader(h, headerToSamples[h] || []);
      headerToField[h] = field;
      if (field) {
        console.log(`  - "${h}" => "${field}"`);
      } else {
        console.log(`  - "${h}" => (未マッピング)`);
      }
    }

    // === 行ごとの更新処理 ===
    let idx = 0;
    for (const row of rows) {
      idx++;
      globalRowIndex++;
      
      let name =
        trim(row["企業名"]) ??
        trim(row["会社名"]) ??
        trim(row["name"]);
      
      // 「（株）」を「株式会社」に変換
      if (name) {
        const normalizedName = normalizeCompanyNameFormat(name);
        if (normalizedName) {
          name = normalizedName;
        }
      }

      const corporateNumber =
        trim(row["corporateNumber"]) ??
        trim(row["法人番号"]) ??
        trim(row["corporate_number"]);

      // 「日経バリューサーチ」の処理
      const isNikkeiValueSearch = name === "日経バリューサーチ" || name?.includes("日経バリューサーチ");
      let extractedName: string | null = null;
      let jsonExtractedFields: Record<string, any> = {};

      if (isNikkeiValueSearch && name) {
        // CSVデータから企業名を抽出
        const csvData = buildUpdateFromCsvRow(row, headerToField, file);
        
        // ① フィールド内から企業名を抽出
        extractedName = extractCompanyNameFromFields(csvData);
        
        // ② JSON形式のフィールドから企業名とフィールド情報を抽出
        if (!extractedName) {
          // JSON形式のフィールドを探す
          for (const [field, value] of Object.entries(csvData)) {
            if (isJsonValue(value)) {
              const jsonName = extractCompanyNameFromJson(value);
              if (jsonName) {
                extractedName = jsonName;
                jsonExtractedFields = extractFieldsFromJson(value);
                console.log(`  📝 [${path.basename(file)} row ${idx}] JSONから企業名を抽出: "${extractedName}"`);
                break;
              }
            }
          }
        }
        
        // ③ 企業HPから企業名を取得
        if (!extractedName) {
          const url = csvData.companyUrl || row["URL"] || row["contactUrl"];
          if (url) {
            extractedName = await extractCompanyNameFromUrl(url);
            if (extractedName) {
              console.log(`  📝 [${path.basename(file)} row ${idx}] URLから企業名を抽出: "${extractedName}"`);
            }
          }
        }

        if (extractedName) {
          name = extractedName;
        } else {
          console.warn(`  ⚠️  [${path.basename(file)} row ${idx}] 企業名を抽出できませんでした`);
        }
      }

      let found: { ref: DocumentReference; data: any } | null = null;

      if (corporateNumber) {
        // ① 法人番号で検索
        found = await findCompanyDocByCorporateNumber(corporateNumber);
        if (!found) {
          notFoundCount++;
          if (notFoundCount <= 10) {
            console.warn(
              `⚠️  [${path.basename(file)} row ${idx}] corporateNumber="${corporateNumber}" に該当ドキュメントなし`
            );
          }
        }
      }

      // ② 法人番号がない or 見つからなかった場合 → 名前＋メタ情報でフォールバック
      if (!found) {
        const byName = await findCompanyDocByNameAndMeta(row);
        if (!byName) {
          unresolvedCount++;
          const n =
            row["企業名"] ?? row["会社名"] ?? row["name"] ?? "";

          console.warn(
            `⚠️  [${path.basename(file)} row ${idx}] 既存ドキュメントが見つからなかったため新規作成します (corporateNumber="${corporateNumber ?? ""}", name="${n}")`
          );

          // ⭐ ここで新規作成に切り替え
          const csvDataForNew = buildUpdateFromCsvRow(row, headerToField, file);

          // 「日経バリューサーチ」の場合、JSONから抽出したフィールドをマージ
          if (isNikkeiValueSearch && Object.keys(jsonExtractedFields).length > 0) {
            Object.assign(csvDataForNew, jsonExtractedFields);
          }

          // name が mapping されていない場合の保険
          if (name && !csvDataForNew.name) {
            csvDataForNew.name = truncateFieldValue("name", name);
          }

          // prefecture / address も CSV からそのまま入れておく（ヘッダーマッチ漏れ対策）
          const prefFromRow =
            trim(row["都道府県"]) ??
            trim(row["prefecture"]);
          if (prefFromRow && !csvDataForNew.prefecture) {
            csvDataForNew.prefecture = prefFromRow;
          }

          const addrFromRow =
            trim(row["会社住所"]) ??
            trim(row["住所"]) ??
            trim(row["所在地"]) ??
            trim(row["本社所在地"]) ??
            trim(row["本社住所"]) ??
            trim(row["address"]);
          if (addrFromRow && !csvDataForNew.address) {
            csvDataForNew.address = truncateFieldValue("address", addrFromRow);
          }

          // corporateNumber フィールドも必ず持たせる
          if (corporateNumber && !csvDataForNew.corporateNumber) {
            csvDataForNew.corporateNumber = corporateNumber;
          }

          const newData: Record<string, any> = {
            ...COMPANY_TEMPLATE,
            ...csvDataForNew,
          };

          // tagsをクリーンアップ
          if (newData.tags) {
            newData.tags = cleanTags(newData.tags);
          }

          // ドキュメントサイズをチェック（Firestoreの1MB制限対策）
          const estimatedSize = JSON.stringify(newData).length;
          const MAX_DOC_SIZE = 1000000; // 1MB = 1,048,576 bytes、安全マージン込みで1,000,000 bytes
          if (estimatedSize > MAX_DOC_SIZE) {
            console.warn(
              `⚠️  [${path.basename(file)} row ${idx}] ドキュメントサイズが大きすぎます: ${estimatedSize} bytes (制限: ${MAX_DOC_SIZE} bytes)`
            );
            // 大きなフィールドをさらに切り詰める
            if (newData.shareholders && typeof newData.shareholders === "string") {
              newData.shareholders = newData.shareholders.substring(0, 50000);
            }
            if (newData.executives && typeof newData.executives === "string") {
              newData.executives = newData.executives.substring(0, 50000);
            }
            if (newData.overview && typeof newData.overview === "string") {
              newData.overview = newData.overview.substring(0, 100000);
            }
            if (newData.companyDescription && typeof newData.companyDescription === "string") {
              newData.companyDescription = newData.companyDescription.substring(0, 100000);
            }
            const newSize = JSON.stringify(newData).length;
            console.warn(
              `  → 切り詰め後: ${newSize} bytes (削減: ${estimatedSize - newSize} bytes)`
            );
          }

          // 数値のみのドキュメントIDを生成（既存スキーマと同じ形式）
          const docId = generateNumericDocId(corporateNumber, globalRowIndex);
          const newRef: DocumentReference = companiesCol.doc(docId);

          if (DRY_RUN) {
            if (createdCount < 20) {
              console.log(
                `🆕 (DRY_RUN) 新規作成予定 docId="${docId}" データ:`,
                newData
              );
            }
          } else {
            await newRef.set(newData);
          }

          createdCount++;
          if (createdCount % 500 === 0) {
            console.log(`  🆕 ここまでの新規作成件数: ${createdCount} 件`);
          }

          // 新規作成したので、この行の処理は完了
          continue;
        }
        found = byName;
      }

      // === ここからは「既存ドキュメントが見つかった」ケース ===
      const { ref, data: current } = found;
      const csvData = buildUpdateFromCsvRow(row, headerToField, file);

      // 「日経バリューサーチ」の場合、JSONから抽出したフィールドをマージ
      if (isNikkeiValueSearch && Object.keys(jsonExtractedFields).length > 0) {
        Object.assign(csvData, jsonExtractedFields);
      }

      const updateData: Record<string, any> = {};

        // 既存のnameが「日経バリューサーチ」の場合は上書き
        const currentName = current?.name;
        const shouldUpdateName = isNikkeiValueSearch || 
          (currentName === "日経バリューサーチ" || currentName?.includes("日経バリューサーチ"));

        // 既存のnameフィールドに「（株）」が含まれている場合は正規化
        if (currentName && currentName.includes("（株）")) {
          const normalizedCurrentName = normalizeCompanyNameFormat(currentName);
          if (normalizedCurrentName && normalizedCurrentName !== currentName) {
            updateData["name"] = normalizedCurrentName;
          }
        }

        // 財務数値フィールドのリスト（タイプJの場合は常に上書き）
        const financialFields = [
          "capitalStock",
          "revenue",
          "revenueFromStatements",
          "revenue1",
          "revenue2",
          "revenue3",
          "revenue4",
          "revenue5",
          "latestRevenue",
          "latestProfit",
          "profit1",
          "profit2",
          "profit3",
          "profit4",
          "profit5",
          "totalAssets",
          "totalLiabilities",
          "netAssets",
          "operatingIncome"
        ];
        const isTypeJ = isTypeJCSV(file);

        for (const [field, csvValue] of Object.entries(csvData)) {
          const curValue = current?.[field];
          const isFinancialField = financialFields.includes(field);

          if (field === "name") {
            // nameは常に上書き（「日経バリューサーチ」の場合は特に）
            // CSVの値も正規化済みなので、そのまま使用
            if (shouldUpdateName || curValue !== csvValue) {
              updateData[field] = csvValue || name;
            }
          } else if (isFinancialField && isTypeJ && csvValue !== null && csvValue !== undefined) {
            // タイプJの財務数値フィールドは常にCSVの値（実値に変換済み）で上書き
            updateData[field] = csvValue;
          } else {
          // JSON形式の場合はCSVの内容で上書き
          if (isJsonValue(curValue)) {
            updateData[field] = csvValue;
          } else if (
            curValue === undefined ||
            curValue === null ||
            curValue === ""
          ) {
            updateData[field] = csvValue;
          }
        }
      }

      // tagsをクリーンアップ（既存のtagsも含めて）
      if (current?.tags) {
        const cleanedTags = cleanTags(current.tags);
        if (cleanedTags.length !== current.tags.length || JSON.stringify(cleanedTags) !== JSON.stringify(current.tags)) {
          updateData.tags = cleanedTags;
        }
      }
      if (updateData.tags) {
        updateData.tags = cleanTags(updateData.tags);
      }

      if (Object.keys(updateData).length === 0) continue;

      // 更新後のドキュメントサイズをチェック（Firestoreの1MB制限対策）
      const mergedData = { ...current, ...updateData };
      const estimatedSize = JSON.stringify(mergedData).length;
      const MAX_DOC_SIZE = 1000000; // 1MB = 1,048,576 bytes、安全マージン込みで1,000,000 bytes
      if (estimatedSize > MAX_DOC_SIZE) {
        console.warn(
          `⚠️  [${path.basename(file)} row ${idx}] 更新後のドキュメントサイズが大きすぎます: ${estimatedSize} bytes (制限: ${MAX_DOC_SIZE} bytes), docId="${ref.id}"`
        );
        // 大きなフィールドをさらに切り詰める
        for (const [field, value] of Object.entries(updateData)) {
          if (typeof value === "string") {
            if (field === "shareholders" || field === "executives") {
              updateData[field] = value.substring(0, 50000);
            } else if (field === "overview" || field === "companyDescription") {
              updateData[field] = value.substring(0, 100000);
            }
          }
        }
        const newMergedData = { ...current, ...updateData };
        const newSize = JSON.stringify(newMergedData).length;
        console.warn(
          `  → 切り詰め後: ${newSize} bytes (削減: ${estimatedSize - newSize} bytes)`
        );
      }

      if (DRY_RUN) {
        if (updatedCount < 20) {
          console.log(
            `📝 (DRY_RUN) docId="${ref.id}" 更新予定:`,
            updateData
          );
        }
      } else {
        await ref.update(updateData);
      }

      updatedCount++;
      if (updatedCount % 500 === 0) {
        console.log(`  ✅ ここまでの更新件数: ${updatedCount} 件`);
      }
    }
  }

  console.log("\n✅ バックフィル処理完了");
  console.log(`  📊 CSV 総行数: ${totalRows}`);
  console.log(`  ✨ 既存更新件数: ${updatedCount}`);
  console.log(`  🆕 新規作成件数: ${createdCount}`);
  console.log(`  ❓ corporateNumber で見つからなかった件数: ${notFoundCount}`);
  console.log(`  ⚠️ 名前等でも既存が見つからず新規作成に回った件数: ${unresolvedCount}`);

  if (DRY_RUN) {
    console.log(
      "\n💡 実際に Firestore を更新するには、--dry-run フラグを外して実行してください。"
    );
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});