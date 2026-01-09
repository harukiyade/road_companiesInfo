/* 
  テストドキュメントを削除して、ヘッダー解析により全159フィールドで新規作成するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/delete_and_recreate_with_header_analysis.ts
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

const COLLECTION_NAME = "companies_new";

// 各グループの代表ファイル
const GROUP_REPRESENTATIVES = {
  group1: "csv/111.csv",
  group2: "csv/118.csv",
  group3: "csv/38.csv",
  group4: "csv/107.csv",
  group5: "csv/110.csv",
  group6: "csv/119.csv",
  group7: "csv/122.csv",
};

// 全159フィールドのテンプレート
const COMPANY_TEMPLATE: Record<string, any> = {
  // 基本情報（14フィールド）
  name: null,
  nameEn: null,
  kana: null,
  corporateNumber: null,
  corporationType: null,
  nikkeiCode: null,
  badges: [],
  tags: [],
  createdAt: null,
  updatedAt: null,
  updateDate: null,
  updateCount: null,
  changeCount: null,
  qualificationGrade: null,
  registrant: null,
  
  // 所在地情報（6フィールド）
  prefecture: null,
  address: null,
  headquartersAddress: null,
  postalCode: null,
  location: null,
  departmentLocation: null,
  
  // 連絡先情報（6フィールド）
  phoneNumber: null,
  contactPhoneNumber: null,
  fax: null,
  email: null,
  companyUrl: null,
  contactFormUrl: null,
  
  // 代表者情報（10フィールド）
  representativeName: null,
  representativeKana: null,
  representativeTitle: null,
  representativeBirthDate: null,
  representativePhone: null,
  representativePostalCode: null,
  representativeHomeAddress: null,
  representativeRegisteredAddress: null,
  representativeAlmaMater: null,
  executives: null,
  
  // 役員情報（20フィールド）
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
  
  // 業種情報（13フィールド）
  industry: null,
  industryLarge: null,
  industryMiddle: null,
  industrySmall: null,
  industryDetail: null,
  industries: [],
  industryCategories: null,
  businessDescriptions: null,
  businessItems: [],
  businessSummary: null,
  specialties: null,
  demandProducts: null,
  specialNote: null,
  
  // 財務情報（29フィールド）
  capitalStock: null,
  revenue: null,
  latestRevenue: null,
  latestProfit: null,
  revenueFromStatements: null,
  operatingIncome: null,
  totalAssets: null,
  totalLiabilities: null,
  netAssets: null,
  issuedShares: null,
  financials: null,
  listing: null,
  marketSegment: null,
  latestFiscalYearMonth: null,
  fiscalMonth: null,
  fiscalMonth1: null,
  fiscalMonth2: null,
  fiscalMonth3: null,
  fiscalMonth4: null,
  fiscalMonth5: null,
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
  
  // 企業規模・組織（10フィールド）
  employeeCount: null,
  employeeNumber: null,
  factoryCount: null,
  officeCount: null,
  storeCount: null,
  averageAge: null,
  averageYearsOfService: null,
  averageOvertimeHours: null,
  averagePaidLeave: null,
  femaleExecutiveRatio: null,
  
  // 設立・沿革（5フィールド）
  established: null,
  dateOfEstablishment: null,
  founding: null,
  foundingYear: null,
  acquisition: null,
  
  // 取引先・関係会社（7フィールド）
  clients: null,
  suppliers: [],
  subsidiaries: [],
  affiliations: null,
  shareholders: null,
  banks: [],
  bankCorporateNumber: null,
  
  // 部署・拠点情報（21フィールド）
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
  
  // 企業説明（4フィールド）
  overview: null,
  companyDescription: null,
  salesNotes: null,
  
  // SNS・外部リンク（9フィールド）
  urls: [],
  profileUrl: null,
  externalDetailUrl: null,
  facebook: null,
  linkedin: null,
  wantedly: null,
  youtrust: null,
  metaKeywords: null,
  metaDescription: null,
  
  // 取引状態・内部管理（4フィールド）
  tradingStatus: null,
  adExpiration: null,
  numberOfActivity: null,
  transportation: null,
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
// ヘルパー関数
// ==============================
function trim(v?: string | null): string | null {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function generateNumericDocId(
  corporateNumber: string | null | undefined,
  index: number,
  existingDocId?: string
): string {
  if (
    corporateNumber &&
    typeof corporateNumber === "string" &&
    /^[0-9]+$/.test(corporateNumber.trim())
  ) {
    return corporateNumber.trim();
  }
  if (existingDocId && /^[0-9]+$/.test(existingDocId)) {
    return existingDocId;
  }
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 代表者名から生年月日を抽出
function extractBirthDateFromRepresentativeName(representativeName: string | null | undefined): string | null {
  if (!representativeName || typeof representativeName !== "string") return null;
  
  const trimmed = representativeName.trim();
  if (!trimmed) return null;
  
  const birthdatePatterns = [
    /(19\d{2}|20\d{2})[\/年-](\d{1,2})[\/月-](\d{1,2})/g,
    /(19\d{2}|20\d{2})\/(\d{1,2})\/(\d{1,2})/g,
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
        
        if (year >= 1900 && year <= 2100 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
          return dateStr;
        }
      }
    }
  }
  
  return null;
}

// 代表者名から個人名（氏名）のみを抽出
function extractPersonNameFromRepresentative(representativeName: string | null | undefined): string | null {
  if (!representativeName || typeof representativeName !== "string") return null;
  
  let trimmed = representativeName.trim();
  if (!trimmed) return null;
  
  const titles = [
    "代表取締役", "代表取締役社長", "代表取締役会長", "代表取締役専務",
    "代表取締役常務", "代表取締役副社長", "取締役社長", "取締役会長",
    "社長", "会長", "専務", "常務", "副社長", "代表", "代表者", "CEO", "ceo"
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
  
  trimmed = trimmed.replace(/[（(].*?[）)]/g, "").trim();
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})[\/年-]\d{1,2}[\/月-]\d{1,2}/g, "").trim();
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})\/\d{1,2}\/\d{1,2}/g, "").trim();
  
  if (/^[\d\s\-・、,，.。]+$/.test(trimmed)) {
    return null;
  }
  
  return trimmed || null;
}

// 代表者名を処理して、個人名と生年月日を分離
function processRepresentativeName(representativeName: string | null | undefined, mapped: Record<string, any>): void {
  if (!representativeName || typeof representativeName !== "string") return;
  
  const trimmed = representativeName.trim();
  if (!trimmed) return;
  
  const birthDate = extractBirthDateFromRepresentativeName(trimmed);
  if (birthDate && !mapped.representativeBirthDate) {
    mapped.representativeBirthDate = birthDate;
  }
  
  const personName = extractPersonNameFromRepresentative(trimmed);
  if (personName) {
    mapped.representativeName = personName;
  } else {
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

// 郵便番号の検証
function validatePostalCode(value: string | null | undefined): string | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/[^\d-]/g, "");
  if (!cleaned) return null;
  
  // ハイフンなしの7桁
  if (/^\d{7}$/.test(cleaned)) {
    return `${cleaned.slice(0, 3)}-${cleaned.slice(3)}`;
  }
  // ハイフンありの形式
  if (/^\d{3}-\d{4}$/.test(cleaned)) {
    return cleaned;
  }
  return null;
}

// 数値のパース（カンマ除去）
function parseNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/[,，]/g, "");
  if (!cleaned) return null;
  const num = Number(cleaned);
  return Number.isFinite(num) && !Number.isNaN(num) ? num : null;
}

// 業種がどこまであるかを判定（行データから）
function detectIndustryEndIndex(row: string[], headers: string[]): number {
  // ヘッダーで業種の位置を特定
  let industryStartIndex = -1;
  let industryEndIndex = -1;
  
  for (let i = 0; i < headers.length; i++) {
    const header = trim(headers[i] || "");
    if (header && (header.startsWith("業種") || header === "業種（細）")) {
      if (industryStartIndex === -1) {
        industryStartIndex = i;
      }
      industryEndIndex = i;
    }
  }
  
  if (industryStartIndex === -1) {
    return -1;
  }
  
  // 行データで業種がどこまであるかを判定
  // 業種の後に郵便番号パターン（数字とハイフン）が来る位置を探す
  // または、ヘッダーで「郵便番号」と定義されている位置を探す
  let postalCodeHeaderIndex = -1;
  for (let i = 0; i < headers.length; i++) {
    const header = trim(headers[i] || "");
    if (header === "郵便番号") {
      postalCodeHeaderIndex = i;
      break;
    }
  }
  
  // 郵便番号ヘッダーが見つかった場合、その前の位置が業種の終了位置
  if (postalCodeHeaderIndex > industryEndIndex) {
    // 郵便番号の直前までが業種
    return postalCodeHeaderIndex - 1;
  }
  
  // ヘッダーで郵便番号が見つからない場合、行データから判定
  for (let i = industryEndIndex + 1; i < row.length && i < headers.length; i++) {
    const value = trim(row[i]);
    if (!value) continue;
    
    // 郵便番号パターンを検出
    const postalPattern = /^\d{3}-?\d{4}$/;
    if (postalPattern.test(value.replace(/[^\d-]/g, ""))) {
      return i - 1; // 業種の終了位置
    }
    
    // 住所っぽい文字列（都道府県名を含む）が来たら業種は終了
    const prefecturePattern = /^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)/;
    if (prefecturePattern.test(value)) {
      return i - 1;
    }
  }
  
  // 見つからない場合は、ヘッダーで定義された最後の業種位置を返す
  return industryEndIndex;
}

// ヘッダー名からフィールド名へのマッピング
function mapHeaderToField(header: string): string | null {
  const headerNorm = trim(header)?.toLowerCase() || "";
  
  const mapping: Record<string, string> = {
    "会社名": "name",
    "都道府県": "prefecture",
    "代表者名": "representativeName",
    "法人番号": "corporateNumber",
    "url": "companyUrl",
    "業種1": "industryLarge",
    "業種2": "industryMiddle",
    "業種3": "industrySmall",
    "業種4": "industryDetail",
    "郵便番号": "postalCode",
    "住所": "address",
    "設立": "established",
    "電話番号(窓口)": "contactPhoneNumber",
  "代表者郵便番号": "representativePostalCode",
  "代表者住所": "representativeHomeAddress",
  "代表者郵便": "representativePostalCode",
  "代表者郵便番": "representativePostalCode",
    "代表者誕生日": "representativeBirthDate",
    "資本金": "capitalStock",
    "上場": "listing",
    "直近決算年月": "latestFiscalYearMonth",
    "直近売上": "latestRevenue",
    "直近利益": "latestProfit",
    "説明": "companyDescription",
    "概要": "overview",
    "仕入れ先": "suppliers",
    "取引先": "clients",
    "取引先銀行": "banks",
    "取締役": "executives",
    "株主": "shareholders",
    "社員数": "employeeCount",
    "オフィス数": "officeCount",
    "工場数": "factoryCount",
    "店舗数": "storeCount",
  };
  
  return mapping[header] || null;
}

// CSV行をマッピング（ヘッダー解析版）
function mapCsvRowToCompany(row: string[], headers: string[]): Record<string, any> {
  const mapped: Record<string, any> = { ...COMPANY_TEMPLATE };
  
  // ヘッダー名からインデックスのマップを作成
  const headerIndexMap: Record<string, number> = {};
  for (let i = 0; i < headers.length; i++) {
    const header = trim(headers[i]);
    if (header) {
      headerIndexMap[header] = i;
    }
  }
  
  // 業種の開始位置と終了位置を特定
  let industryStartIndex = -1;
  let industryEndIndex = -1;
  for (let i = 0; i < headers.length; i++) {
    const header = trim(headers[i] || "");
    if (header && (header.startsWith("業種") || header === "業種（細）")) {
      if (industryStartIndex === -1) {
        industryStartIndex = i;
      }
      industryEndIndex = i;
    }
  }
  
  // 郵便番号ヘッダーの位置を特定
  let postalCodeHeaderIndex = -1;
  for (let i = 0; i < headers.length; i++) {
    const header = trim(headers[i] || "");
    if (header === "郵便番号") {
      postalCodeHeaderIndex = i;
      break;
    }
  }
  
  // データ行で業種がどこまであるかを動的に判定
  let actualIndustryEndIndex = industryEndIndex;
  if (industryStartIndex >= 0 && postalCodeHeaderIndex > industryEndIndex) {
    // 郵便番号ヘッダーの直前までが業種の可能性がある
    // データ行で業種4以降があるかチェック
    for (let i = industryEndIndex + 1; i < postalCodeHeaderIndex && i < row.length; i++) {
      const value = trim(row[i]);
      if (!value) continue;
      
      // 郵便番号パターンが来たら業種は終了
      const postalPattern = /^\d{3}-?\d{4}$/;
      if (postalPattern.test(value.replace(/[^\d-]/g, ""))) {
        actualIndustryEndIndex = i - 1;
        break;
      }
      
      // 都道府県名が来たら業種は終了
      const prefecturePattern = /^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)/;
      if (prefecturePattern.test(value)) {
        actualIndustryEndIndex = i - 1;
        break;
      }
      
      // 業種っぽい値（「業」「サービス」「製造」などを含む）なら業種として扱う
      if (/業|サービス|製造|卸|小売|建設|不動産|運輸|物流|IT|情報|ソフト|システム|医療|福祉|教育|金融|保険|広告|人材|コンサル|飲食|宿泊|農業|漁業|鉱業|電気|ガス|水道|通信|メディア|エネルギー/.test(value)) {
        actualIndustryEndIndex = i;
      } else {
        // 業種っぽくない値が来たら業種は終了
        actualIndustryEndIndex = i - 1;
        break;
      }
    }
  }
  
  // 業種の処理（ヘッダー定義 + データ行の動的判定）
  if (industryStartIndex >= 0) {
    for (let i = industryStartIndex; i <= actualIndustryEndIndex && i < row.length; i++) {
      const value = trim(row[i]);
      if (!value) continue;
      
      const header = trim(headers[i] || "");
      if (header && header.startsWith("業種")) {
        if (header === "業種1") {
          mapped.industryLarge = value;
        } else if (header === "業種2") {
          mapped.industryMiddle = value;
        } else if (header === "業種3") {
          mapped.industrySmall = value;
        } else if (header === "業種4" || header === "業種（細）") {
          mapped.industryDetail = value;
        }
      } else {
        // ヘッダーに業種4がないが、データ行に業種4がある場合
        if (i === industryEndIndex + 1) {
          mapped.industryDetail = value;
        } else if (i > industryEndIndex + 1) {
          // 業種5以降
          if (!mapped.industryCategories) {
            mapped.industryCategories = [];
          }
          if (Array.isArray(mapped.industryCategories)) {
            mapped.industryCategories.push(value);
          } else {
            mapped.industryCategories = [mapped.industryCategories, value];
          }
        }
      }
    }
  }
  
  // 業種以外のフィールドをヘッダー名ベースでマッピング（順序に依存しない）
  for (const [header, index] of Object.entries(headerIndexMap)) {
    if (index >= row.length) continue;
    
    // 業種の範囲内はスキップ（既に処理済み）
    if (industryStartIndex >= 0 && index >= industryStartIndex && index <= actualIndustryEndIndex) {
      continue;
    }
    
    const value = trim(row[index]);
    if (!value) continue;
    
    // 業種以外のフィールドをヘッダー名からマッピング
    const fieldName = mapHeaderToField(header);
    if (fieldName) {
      if (fieldName === "representativeName") {
        processRepresentativeName(value, mapped);
      } else if (fieldName === "representativePostalCode") {
        const postalCode = validatePostalCode(value);
        if (postalCode) {
          mapped[fieldName] = postalCode;
        }
      } else if (fieldName === "representativeHomeAddress") {
        // 代表者住所の値検証：郵便番号、電話番号、生年月日パターンは除外
        const postalPattern = /^\d{3}-?\d{4}$/;
        const phonePattern = /^[\d\-\(\)]+$/;
        const birthdatePattern = /^(19\d{2}|20\d{2})[\/年-]\d{1,2}[\/月-]\d{1,2}$/;
        
        const cleaned = value.replace(/[^\d-]/g, "");
        if (postalPattern.test(cleaned) || phonePattern.test(value) || birthdatePattern.test(value)) {
          // 郵便番号、電話番号、生年月日パターンの場合はnull
          mapped[fieldName] = null;
        } else {
          mapped[fieldName] = value;
        }
      } else if (fieldName === "postalCode") {
        const postalCode = validatePostalCode(value);
        if (postalCode) {
          mapped[fieldName] = postalCode;
        }
      } else if (fieldName === "capitalStock" || fieldName === "latestRevenue" || fieldName === "latestProfit" || fieldName === "employeeCount" || fieldName === "officeCount" || fieldName === "factoryCount" || fieldName === "storeCount") {
        const num = parseNumeric(value);
        if (num !== null) {
          mapped[fieldName] = num;
        }
      } else if (fieldName === "suppliers" || fieldName === "banks") {
        // 配列フィールド
        if (value) {
          mapped[fieldName] = [value];
        }
      } else {
        mapped[fieldName] = value;
      }
    }
  }
  
  // タイムスタンプ
  mapped.createdAt = admin.firestore.FieldValue.serverTimestamp();
  mapped.updatedAt = admin.firestore.FieldValue.serverTimestamp();
  
  return mapped;
}

// ==============================
// メイン処理
// ==============================
async function main() {
  try {
    console.log("\n🗑️  既存のテストドキュメントを削除中...\n");
    
    // 最新のログファイルから削除対象のIDを取得
    const logFiles = fs.readdirSync(".")
      .filter(f => f.startsWith("created_test_companies_") && f.endsWith(".txt"))
      .sort()
      .reverse();
    
    const docIdsToDelete: string[] = [];
    
    if (logFiles.length > 0) {
      const latestLog = logFiles[0];
      console.log(`📄 ログファイルを読み込み: ${latestLog}`);
      const content = fs.readFileSync(latestLog, "utf8");
      const lines = content.split("\n").filter(l => l.trim());
      
      for (const line of lines) {
        const match = line.match(/:\s*(\d+)\s*\(/);
        if (match) {
          docIdsToDelete.push(match[1]);
        }
      }
    }
    
    console.log(`📋 削除対象: ${docIdsToDelete.length}件\n`);
    
    // バッチ削除
    const BATCH_SIZE = 500;
    let deletedCount = 0;
    
    for (let i = 0; i < docIdsToDelete.length; i += BATCH_SIZE) {
      const batch = db.batch();
      const batchIds = docIdsToDelete.slice(i, i + BATCH_SIZE);
      
      for (const docId of batchIds) {
        const ref = companiesCol.doc(docId);
        batch.delete(ref);
      }
      
      await batch.commit();
      deletedCount += batchIds.length;
      console.log(`✅ ${deletedCount}/${docIdsToDelete.length}件削除完了`);
    }
    
    console.log(`\n✨ 削除完了: ${deletedCount}件\n`);
    
    // 新規作成
    console.log("📝 ヘッダー解析により全159フィールドで新規ドキュメントを作成中...\n");
    
    const createdDocIds: string[] = [];
    let globalIndex = 0;
    
    for (const [groupName, csvPath] of Object.entries(GROUP_REPRESENTATIVES)) {
      if (!fs.existsSync(csvPath)) {
        console.warn(`⚠️  ファイルが見つかりません: ${csvPath}`);
        continue;
      }
      
      console.log(`📂 ${groupName}: ${csvPath}`);
      
      const csvContent = fs.readFileSync(csvPath, "utf8");
      const records = parse(csvContent, {
        columns: false,
        skip_empty_lines: true,
        encoding: "utf8",
        relax_column_count: true,
        relax_quotes: true,
      }) as string[][];
      
      if (records.length < 2) {
        console.warn(`⚠️  ヘッダーまたはデータ行が不足しています`);
        continue;
      }
      
      // ヘッダー行を取得
      const headers = records[0].map(h => trim(h) || "");
      
      console.log(`  📋 ヘッダー数: ${headers.length}`);
      console.log(`  📋 ヘッダー: ${headers.slice(0, 10).join(", ")}...`);
      
      // 最初の5行のみ処理（ヘッダーを除く）
      const rowsToProcess = records.slice(1, 6);
      
      for (let i = 0; i < rowsToProcess.length; i++) {
        const row = rowsToProcess[i];
        const mapped = mapCsvRowToCompany(row, headers);
        
        const docId = generateNumericDocId(mapped.corporateNumber, globalIndex);
        globalIndex++;
        
        const ref = companiesCol.doc(docId);
        await ref.set(mapped);
        
        const companyName = mapped.name || docId;
        createdDocIds.push(`${groupName} - ${path.basename(csvPath)} - 行${i + 1}: ${docId} (${companyName})`);
        console.log(`  ✅ 行${i + 1}: ${docId} (${companyName})`);
        console.log(`     - 代表者郵便番号: ${mapped.representativePostalCode || "null"}`);
        console.log(`     - 代表者住所: ${mapped.representativeHomeAddress || "null"}`);
      }
      
      console.log("");
    }
    
    // ログファイルに保存
    const timestamp = Date.now();
    const logFileName = `created_test_companies_${timestamp}.txt`;
    fs.writeFileSync(logFileName, createdDocIds.join("\n"));
    
    console.log(`\n✨ 作成完了: ${createdDocIds.length}件`);
    console.log(`📄 ログファイル: ${logFileName}\n`);
    
    // フィールド数の確認
    if (createdDocIds.length > 0) {
      const firstDocId = createdDocIds[0].split(": ")[1].split(" ")[0];
      const sampleDoc = await companiesCol.doc(firstDocId).get();
      if (sampleDoc.exists) {
        const data = sampleDoc.data();
        const fieldCount = Object.keys(data || {}).length;
        console.log(`📊 サンプルドキュメントのフィールド数: ${fieldCount}フィールド\n`);
      }
    }
    
  } catch (error) {
    console.error("❌ エラーが発生しました:", error);
    process.exit(1);
  }
}

main().catch(console.error);

