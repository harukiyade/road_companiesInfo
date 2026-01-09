/* 
  各グループの代表ファイルの最初の5行を新規作成するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/create_test_companies_by_group.ts
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

// 各グループの代表ファイル
const GROUP_REPRESENTATIVES = {
  group1: "csv/111.csv", // グループ1の代表
  group2: "csv/118.csv", // グループ2の代表
  group3: "csv/38.csv",  // グループ3の代表
  group4: "csv/107.csv", // グループ4の代表
  group5: "csv/110.csv", // グループ5の代表
  group6: "csv/119.csv", // グループ6の代表
  group7: "csv/122.csv", // グループ7の代表
};

// 無視するフィールド
const IGNORE_FIELDS = new Set([
  "ID",
  "取引種別",
  "SBフラグ",
  "NDA",
  "AD",
  "ステータス",
  "備考",
  "Unnamed: 38",
  "Unnamed: 39",
  "Unnamed: 40",
  "Unnamed: 41",
  "Unnamed: 42",
  "Unnamed: 43",
  "Unnamed: 44",
  "Unnamed: 45",
]);

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

const BATCH_LIMIT = 500;

// 文字列のトリム
function trim(v: any): string {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

// 郵便番号の正規化と検証
function isPostalCode(value: string): boolean {
  const trimmed = trim(value);
  if (!trimmed) return false;
  
  // 3桁-4桁の形式
  if (/^\d{3}-\d{4}$/.test(trimmed)) return true;
  
  // 7桁の数字
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 7) return true;
  
  return false;
}

// 郵便番号の正規化
function normalizePostalCode(value: string): string | null {
  const trimmed = trim(value);
  if (!trimmed) return null;
  
  if (/^\d{3}-\d{4}$/.test(trimmed)) return trimmed;
  
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 7) {
    return `${digits.slice(0, 3)}-${digits.slice(3)}`;
  }
  
  return null;
}

// 法人番号の正規化と検証
function normalizeCorporateNumber(value: string): string | null {
  const trimmed = trim(value);
  if (!trimmed) return null;
  
  // 指数表記の処理
  if (/^\d+\.\d+E\+\d+$/i.test(trimmed)) {
    try {
      const num = parseFloat(trimmed);
      const digits = Math.floor(num).toString().replace(/\D/g, "");
      if (digits.length === 13 && !isInvalidCorporateNumber(digits)) {
        return digits;
      }
    } catch {
      return null;
    }
  }
  
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 13 && !isInvalidCorporateNumber(digits)) {
    return digits;
  }
  
  return null;
}

// 無効な法人番号パターンを検出
function isInvalidCorporateNumber(digits: string): boolean {
  if (digits.length !== 13) return true;
  
  // 同じ数字の繰り返し
  if (/^(\d)\1{12}$/.test(digits)) return true;
  
  // 0のみ
  if (digits === "0000000000000") return true;
  
  // 9で始まり残りが0のみ
  if (/^9\d{2}0{10}$/.test(digits)) return true;
  
  // 1で始まり残りが0のみ
  if (/^10{12}$/.test(digits)) return true;
  
  return false;
}

// 都道府県の抽出
const PREF_LIST = [
  "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
  "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
  "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
  "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
  "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
  "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
  "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
];

function extractPrefecture(addr: string): string | undefined {
  const v = trim(addr);
  if (!v) return;
  for (const p of PREF_LIST) {
    if (v.startsWith(p)) return p;
    if (v.includes(p)) return p;
  }
  return;
}

// 数値パース
function parseNumeric(value: string): number | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/[,\s]/g, "");
  if (!cleaned) return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

// 財務数値のパース
function parseFinancialNumeric(value: string): number | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/[,\s]/g, "");
  if (!cleaned || cleaned === "0" || cleaned === "非上場") return null;
  
  const num = Number(cleaned);
  if (!Number.isFinite(num) || num === 0) return null;
  
  // 千円単位として扱う
  return Math.round(num * 1000);
}

// セルの値の内容からフィールドタイプを判定
function detectFieldType(value: string, header: string): string | null {
  const trimmed = trim(value);
  if (!trimmed) return null;

  // 郵便番号
  if (isPostalCode(trimmed)) return "postalCode";
  
  // URL
  if (trimmed.startsWith("http://") || trimmed.startsWith("https://")) return "url";
  
  // 電話番号（数字とハイフンを含む）
  if (/^[\d\-\(\)]+$/.test(trimmed) && trimmed.length >= 10 && trimmed.length <= 15) {
    if (!isPostalCode(trimmed)) return "phone";
  }
  
  // 住所（都道府県名を含む）
  if (extractPrefecture(trimmed)) return "address";
  
  // 業種（「業」を含む文字列）
  if (/業|サービス|製造|卸|小売|建設|不動産|運輸|物流|IT|情報|ソフト|システム|医療|福祉|教育|金融|保険|広告|人材|コンサル|飲食|宿泊|農業|漁業|鉱業|電気|ガス|水道|通信|メディア|エネルギー/.test(trimmed)) {
    return "industry";
  }
  
  // 年（4桁の数字のみ、または「年」を含む）
  if (/^\d{4}$/.test(trimmed) || /^\d{4}年/.test(trimmed)) return "year";
  
  // 日付（YYYY/MM/DD形式など）
  if (/^\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}$/.test(trimmed)) return "date";
  
  // 数値（財務数値の可能性）
  if (/^[\d,]+$/.test(trimmed) && trimmed.length > 3) {
    const num = parseFinancialNumeric(trimmed);
    if (num !== null && num > 1000) return "financial";
  }
  
  return null;
}

// 行データを解析（列インデックスベース）
function analyzeRow(
  cells: string[],
  headers: string[],
  groupNumber: number
): Record<string, any> {
  // 既存のフィールド一覧に合わせて初期化（nullでも良い）
  const result: Record<string, any> = {
    acquisition: null,
    adExpiration: null,
    address: null,
    affiliations: null,
    businessDescriptions: null,
    capitalStock: null,
    changeCount: null,
    clients: null,
    companyDescription: null,
    companyUrl: null,
    contactFormUrl: null,
    corporateNumber: null,
    corporationType: null,
    demandProducts: null,
    email: null,
    employeeCount: null,
    established: null,
    executives: null,
    factoryCount: null,
    fax: null,
    financials: null,
    fiscalMonth: null,
    foundingYear: null,
    industries: null,
    industry: null,
    industryCategories: null,
    industryDetail: null,
    industryLarge: null,
    industryMiddle: null,
    industrySmall: null,
    linkedin: null,
    listing: null,
    marketSegment: null,
    metaDescription: null,
    metaKeywords: null,
    name: null,
    officeCount: null,
    overview: null,
    phoneNumber: null,
    postalCode: null,
    prefecture: null,
    registrant: null,
    representativeAlmaMater: null,
    representativeBirthDate: null,
    representativeHomeAddress: null,
    representativeKana: null,
    representativeName: null,
    representativePhone: null,
    representativeRegisteredAddress: null,
    representativeTitle: null,
    revenue: null,
    salesNotes: null,
    shareholders: null,
    storeCount: null,
    suppliers: null,
    tags: null,
    updateCount: null,
    urls: null,
    wantedly: null,
    youtrust: null,
  };

  const industries: string[] = [];
  let foundPostalCode = false;
  let addressParts: string[] = [];
  
  // グループ別の業種開始インデックスを特定
  let industryStartIndex = -1;
  let industryEndIndex = -1;
  
  // ヘッダーで定義されている業種フィールドの位置を特定
  for (let i = 0; i < headers.length; i++) {
    const header = trim(headers[i] || "");
    if (header.startsWith("業種") || header === "業種（細）") {
      if (industryStartIndex === -1) {
        industryStartIndex = i;
      }
      industryEndIndex = i;
    }
  }

  // グループ別の業種開始位置を決定（ヘッダーではなく、実際のデータ位置から判定）
  let actualIndustryStartIndex = industryStartIndex;
  if (groupNumber === 1 || groupNumber === 4 || groupNumber === 5) {
    // グループ1,4,5: 業種3以降
    // 業種1のインデックスを探す
    for (let i = 0; i < headers.length; i++) {
      if (trim(headers[i] || "") === "業種1") {
        actualIndustryStartIndex = i + 2; // 業種3のインデックス
        break;
      }
    }
  } else if (groupNumber === 2 || groupNumber === 3 || groupNumber === 6 || groupNumber === 7) {
    // グループ2,3,6,7: 業種2以降
    // 業種1のインデックスを探す
    for (let i = 0; i < headers.length; i++) {
      if (trim(headers[i] || "") === "業種1") {
        actualIndustryStartIndex = i + 1; // 業種2のインデックス
        break;
      }
    }
  }
  

  // 列インデックスベースで処理
  for (let i = 0; i < cells.length && i < headers.length; i++) {
    const header = trim(headers[i] || "");
    const value = trim(cells[i] || "");

    // 無視するフィールドはスキップ
    if (IGNORE_FIELDS.has(header) || header.startsWith("Unnamed:")) continue;

    // 空欄はスキップ
    if (!value) continue;

    // 基本フィールド（ヘッダー名で判定）
    if (header === "会社名" || header === "企業名" || header === "name") {
      result.name = value;
      continue;
    }
    if (header === "都道府県") {
      result.prefecture = value;
      continue;
    }
    if (header === "代表者名") {
      result.representativeName = value;
      continue;
    }
    if (header === "法人番号" && groupNumber !== 4 && groupNumber !== 5) {
      const corpNum = normalizeCorporateNumber(value);
      if (corpNum) {
        result.corporateNumber = corpNum;
      }
      continue;
    }
    if (header === "URL" || header === "url") {
      if (value.startsWith("http://") || value.startsWith("https://")) {
        result.companyUrl = value;
      }
      continue;
    }

    // ヘッダーで定義されている業種フィールドの処理
    if (header.startsWith("業種") || header === "業種（細）") {
      industries.push(value);
      continue;
    }

    // 業種開始位置以降で、まだ郵便番号が見つかっていない場合
    // ヘッダー名ではなく、実際のデータの内容から判定
    if (i >= actualIndustryStartIndex && !foundPostalCode) {
      // まず郵便番号かどうかを確認（3桁-4桁の形式）
      if (isPostalCode(value)) {
        result.postalCode = normalizePostalCode(value);
        foundPostalCode = true;
        // 次の列から住所として扱う
        continue;
      } else {
        // 郵便番号でない場合、業種らしい文字列かどうかを判定
        // 「業」を含む文字列で、郵便番号や住所でない場合
        const looksLikeIndustry = /業|サービス|製造|卸|小売|建設|不動産|運輸|物流|IT|情報|ソフト|システム|医療|福祉|教育|金融|保険|広告|人材|コンサル|飲食|宿泊|農業|漁業|鉱業|電気|ガス|水道|通信|メディア|エネルギー/.test(value);
        const isAddress = extractPrefecture(value) !== undefined;
        
        if (looksLikeIndustry && !isAddress && !isPostalCode(value) && value.length > 2) {
          // 業種として扱う（ヘッダーに定義されていない業種4, 5, 6など）
          industries.push(value);
          continue;
        }
      }
    }

    // 郵便番号が見つかった後の住所処理
    if (foundPostalCode && addressParts.length === 0) {
      // 住所として扱う列（郵便番号の直後の1列のみ）
      // 都道府県名を含むことを確認
      const isAddressLike = extractPrefecture(value) !== undefined;
      if (isAddressLike) {
        addressParts.push(value);
        continue;
      } else {
        // 都道府県名を含まない場合は、住所の収集を終了
        // 次の主要フィールドに到達したと判断
        foundPostalCode = false; // 住所の収集を終了
      }
    }

    // その他のフィールド（ヘッダー名と値の内容から判定）
    if (header === "電話番号(窓口)" || header === "電話番号") {
      // 電話番号の検証（数字とハイフンを含む、適切な長さ）
      if (/^[\d\-\(\)]+$/.test(value) && value.length >= 10 && value.length <= 15 && !isPostalCode(value)) {
        result.phoneNumber = value;
      }
      continue;
    }
    if (header === "設立") {
      // 年のみ抽出（日付形式から年を抽出）
      const yearMatch = value.match(/(\d{4})/);
      if (yearMatch) {
        const year = parseInt(yearMatch[1]);
        if (year >= 1800 && year <= 2100) {
          result.established = year;
          result.foundingYear = year;
        }
      }
      continue;
    }
    if (header === "資本金") {
      const capital = parseFinancialNumeric(value);
      if (capital !== null) {
        result.capitalStock = capital;
      }
      continue;
    }
    if (header === "直近売上") {
      const rev = parseFinancialNumeric(value);
      if (rev !== null) {
        result.revenue = rev;
      }
      continue;
    }
    if (header === "社員数") {
      const emp = parseNumeric(value);
      if (emp !== null && emp > 0) {
        result.employeeCount = emp;
      }
      continue;
    }
    if (header === "オフィス数") {
      const off = parseNumeric(value);
      if (off !== null && off >= 0) {
        result.officeCount = off;
      }
      continue;
    }
    if (header === "工場数") {
      const fac = parseNumeric(value);
      if (fac !== null && fac >= 0) {
        result.factoryCount = fac;
      }
      continue;
    }
    if (header === "店舗数") {
      const sto = parseNumeric(value);
      if (sto !== null && sto >= 0) {
        result.storeCount = sto;
      }
      continue;
    }
    if (header === "説明") {
      // 説明は文字列で、数値のみの場合は無視
      if (!/^\d+$/.test(value) && value.length > 3) {
        result.companyDescription = value;
      }
      continue;
    }
    if (header === "概要") {
      // 概要は文字列で、数値のみの場合は無視
      if (!/^\d+$/.test(value) && value.length > 3) {
        result.overview = value;
      }
      continue;
    }
    if (header === "仕入れ先") {
      result.suppliers = value;
      continue;
    }
    if (header === "取引先") {
      result.clients = value;
      continue;
    }
    if (header === "取締役") {
      result.executives = value;
      continue;
    }
    if (header === "株主") {
      result.shareholders = value;
      continue;
    }
    if (header === "上場") {
      // 上場は「非上場」以外で、数値のみや日付形式の場合は無視
      if (value !== "非上場" && !/^\d+$/.test(value) && !/^\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}/.test(value) && value.length > 0) {
        result.listing = value;
      }
      continue;
    }
    if (header === "直近決算年月") {
      const monthMatch = value.match(/(\d{1,2})月/);
      if (monthMatch) {
        const month = parseInt(monthMatch[1]);
        if (month >= 1 && month <= 12) {
          result.fiscalMonth = month;
        }
      }
      continue;
    }
    if (header === "代表者誕生日") {
      // 誕生日は日付形式で、住所や郵便番号でないことを確認
      if (!extractPrefecture(value) && !isPostalCode(value) && /^\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}/.test(value)) {
        result.representativeBirthDate = value;
      }
      continue;
    }
    if (header === "代表者郵便番号") {
      // 代表者郵便番号をrepresentativeRegisteredAddressに設定
      const normalized = normalizePostalCode(value);
      if (normalized) {
        result.representativeRegisteredAddress = normalized;
      }
      continue;
    }
    if (header === "代表者住所") {
      // 代表者住所は郵便番号や電話番号でないことを確認
      // 都道府県名を含むことを確認
      if (!isPostalCode(value) && !/^[\d\-\(\)]+$/.test(value) && extractPrefecture(value)) {
        result.representativeHomeAddress = value;
      }
      continue;
    }
  }

  // 住所の結合
  if (addressParts.length > 0) {
    result.address = addressParts[0]; // 最初の1つのみ（都道府県名を含む）
    if (!result.prefecture) {
      result.prefecture = extractPrefecture(result.address);
    }
  } else {
    // フォールバック: 郵便番号の次の列を直接探す
    if (result.postalCode) {
      // 郵便番号が見つかった列の次の列を確認
      for (let i = 0; i < cells.length; i++) {
        if (isPostalCode(trim(cells[i] || ""))) {
          // 次の列が住所の可能性
          if (i + 1 < cells.length) {
            const nextValue = trim(cells[i + 1] || "");
            if (extractPrefecture(nextValue)) {
              result.address = nextValue;
              if (!result.prefecture) {
                result.prefecture = extractPrefecture(nextValue);
              }
            }
          }
          break;
        }
      }
    }
    // さらにフォールバック: ヘッダー名で「住所」を探す
    if (!result.address) {
      for (let i = 0; i < headers.length; i++) {
        if (trim(headers[i] || "") === "住所") {
          const addr = trim(cells[i] || "");
          if (addr && extractPrefecture(addr)) {
            result.address = addr;
            if (!result.prefecture) {
              result.prefecture = extractPrefecture(addr);
            }
          }
          break;
        }
      }
    }
  }

  // 郵便番号のフォールバック
  if (!result.postalCode) {
    for (let i = 0; i < headers.length; i++) {
      if (headers[i] === "郵便番号") {
        const postal = normalizePostalCode(trim(cells[i] || ""));
        if (postal) {
          result.postalCode = postal;
        }
        break;
      }
    }
  }

  // 業種の設定
  if (industries.length > 0) {
    result.industries = industries;
    result.industry = industries[0];
    if (industries.length >= 1) {
      result.industryLarge = industries[0];
    }
    if (industries.length >= 2) {
      result.industryMiddle = industries[1];
    }
    if (industries.length >= 3) {
      result.industrySmall = industries[2];
    }
    if (industries.length >= 4) {
      result.industryDetail = industries.slice(3);
    }
  }

  // nullのフィールドを削除（undefinedは残す）
  Object.keys(result).forEach(key => {
    if (result[key] === null) {
      delete result[key];
    }
  });

  return result;
}

// 数値IDを生成
function generateNumericId(): string {
  const timestamp = Date.now();
  const random = Math.floor(Math.random() * 10000);
  return `${timestamp}${String(random).padStart(4, "0")}`;
}

// メイン処理
async function main() {
  console.log("🚀 各グループの代表ファイルの最初の5行を新規作成します\n");

  const createdDocIds: { group: string; csvFile: string; rowNum: number; docId: string; companyName: string }[] = [];
  let totalCreated = 0;

  for (const [groupName, filePath] of Object.entries(GROUP_REPRESENTATIVES)) {
    const fileName = path.basename(filePath);
    const groupNumber = parseInt(groupName.replace("group", ""));
    
    console.log(`\n📄 処理中: ${fileName} (グループ${groupNumber})`);

    try {
      if (!fs.existsSync(filePath)) {
        console.log(`  ⚠️  ファイルが見つかりません: ${filePath}`);
        continue;
      }

      const csvContent = fs.readFileSync(filePath, "utf8");
      // 行配列として読み込む（列インデックスベースで処理）
      const records: string[][] = parse(csvContent, {
        columns: false,
        skip_empty_lines: true,
        relax_column_count: true,
        relax_quotes: true,
        skip_records_with_error: true,
      });

      if (records.length < 2) {
        console.log("  ⚠️  CSVにヘッダー行以外のデータがありません。スキップします。");
        continue;
      }

      const headers = records[0].map(h => trim(h));
      const dataRows = records.slice(1);
      console.log(`  📊 ヘッダー数: ${headers.length}, データ行数: ${dataRows.length}`);
      console.log(`  📝 処理対象: ${Math.min(dataRows.length, 5)}行`);

      let batch: WriteBatch = db.batch();
      let batchCount = 0;

      for (let i = 0; i < Math.min(dataRows.length, 5); i++) {
        const cells = dataRows[i];
        const companyName = trim(cells[0] || "");

        if (!companyName) {
          console.log(`    ⚠️  行${i + 1}: 会社名が取得できませんでした。スキップします。`);
          continue;
        }

        console.log(`\n  [行${i + 1}] 解析中: ${companyName}`);

        // 行データを解析（列インデックスベース）
        const data = analyzeRow(cells, headers, groupNumber);

        if (!data.name) {
          console.log("    ⚠️  会社名が取得できませんでした。スキップします。");
          continue;
        }

        // 新規ドキュメントを作成（数値ID）
        const docId = generateNumericId();
        const docRef = companiesCol.doc(docId);

        const createData: Record<string, any> = {
          ...data,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        };

        batch.set(docRef, createData);
        batchCount++;
        totalCreated++;

        console.log(`    ✅ 解析完了: ${data.name}`);
        console.log(`    ✅ 新規作成: ドキュメントID ${docId}`);
        createdDocIds.push({ group: groupName, csvFile: fileName, rowNum: i + 1, docId, companyName: data.name });

        if (batchCount >= BATCH_LIMIT) {
          await batch.commit();
          batch = db.batch();
          batchCount = 0;
          console.log("    ✅ バッチコミット: 500件");
        }
      }

      if (batchCount > 0) {
        await batch.commit();
        console.log(`    ✅ 最終バッチコミット: ${batchCount}件`);
      }
      console.log(`  ✅ 完了: ${fileName}`);

    } catch (err: any) {
      console.error(`  ❌ ファイル処理エラー (${fileName}): ${err.message}`);
    }
  }

  // 結果をファイルに保存
  const timestamp = Date.now();
  const outputFile = `created_test_companies_${timestamp}.txt`;
  const outputContent = createdDocIds
    .map(item => `${item.group} - ${item.csvFile} - 行${item.rowNum}: ${item.docId} (${item.companyName})`)
    .join("\n");
  fs.writeFileSync(outputFile, outputContent, "utf8");

  console.log("\n" + "=".repeat(80));
  console.log(`✅ 処理完了: ${totalCreated}件のドキュメントを新規作成しました`);
  console.log(`📄 結果ファイル: ${outputFile}`);
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

