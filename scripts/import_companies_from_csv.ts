/* 
  企業情報 CSV を Firestore `companies_new` にマージするスクリプト

  使い方（CSV 1個の場合）:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/import_companies_from_csv.ts ./csv/companies.csv

  使い方（フォルダ内の CSV を全部まとめて処理する場合）:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/import_companies_from_csv.ts ./csv

  ※ サービスアカウントキーを第1引数に渡す場合:
    npx ts-node scripts/import_companies_from_csv.ts serviceAccountKey.json ./csv

  ※ 途中から再開したい場合:
    - ファイル単位で再開:
        START_FROM_FILE=119.csv \
        npx ts-node scripts/import_companies_from_csv.ts ./csv/new/output_csv
    - ファイル内の特定行から再開:
        START_FROM_FILE=119.csv START_FROM_ROW=200 \
        npx ts-node scripts/import_companies_from_csv.ts ./csv/new/output_csv
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

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  // 環境変数またはコマンドライン引数からサービスアカウントキーのパスを取得
  // 使用方法: npx ts-node scripts/import_companies_from_csv.ts <csv-file or dir> [serviceAccountKey.json]
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  
  // コマンドライン引数から取得を試みる（第2 or 第3引数）
  // 先に .json を見つけたらそれをサービスアカウントキーとみなし、それ以外を CSV/ディレクトリとする
  const arg2 = process.argv[2];
  const arg3 = process.argv[3];
  if (!serviceAccountPath) {
    if (arg2 && arg2.endsWith(".json") && fs.existsSync(arg2)) {
      serviceAccountPath = arg2;
      // CSV/ディレクトリパスは第3引数
      if (arg3) {
        process.argv[2] = arg3;
      }
    } else if (arg3 && arg3.endsWith(".json") && fs.existsSync(arg3)) {
      serviceAccountPath = arg3;
      // CSV/ディレクトリパスは第2引数を維持
    }
  }
  
  // デフォルトのパスを試す（相対パスと絶対パス）
  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
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
    console.error("");
    console.error("   以下のいずれかの方法でサービスアカウントキーファイルを指定してください:");
    console.error("");
    console.error("   方法1 - 環境変数（推奨）:");
    console.error("     export GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json");
    console.error("     npx ts-node scripts/import_companies_from_csv.ts <csv-file or dir>");
    console.error("");
    console.error("   方法2 - コマンドライン引数:");
    console.error("     npx ts-node scripts/import_companies_from_csv.ts <serviceAccountKey.json> <csv-file or dir>");
    console.error("");
    console.error("   方法3 - デフォルトパス:");
    console.error("     プロジェクトルートに以下のいずれかのファイル名で配置:");
    console.error("     - serviceAccountKey.json");
    console.error("     - service-account-key.json");
    console.error("     - firebase-service-account.json");
    console.error("");
    console.error(`   現在の作業ディレクトリ: ${process.cwd()}`);
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
      console.error("   サービスアカウントキーファイルに 'project_id' が含まれているか、");
      console.error("   環境変数 GCLOUD_PROJECT または GCP_PROJECT が設定されているか確認してください");
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

// 既存ドキュメント検索を高速化するための簡易キャッシュ
const cacheByCorporateNumber = new Map<string, DocumentReference<DocumentData> | null>();
const cacheByName = new Map<string, DocumentReference<DocumentData> | null>();
const cacheByNameAndAddress = new Map<string, DocumentReference<DocumentData> | null>();

// ==============================
// リトライヘルパー
// ==============================

// リトライ可能なエラーコード
const RETRYABLE_ERROR_CODES = [14, 4, 8]; // UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED

// リトライ可能なエラーかどうかを判定
function isRetryableError(error: any): boolean {
  if (!error || !error.code) return false;
  return RETRYABLE_ERROR_CODES.includes(error.code);
}

// リトライ付きで関数を実行
async function retryOperation<T>(
  operation: () => Promise<T>,
  operationName: string,
  maxRetries: number = 5,
  baseDelayMs: number = 1000
): Promise<T> {
  let lastError: any;
  
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await operation();
    } catch (error: any) {
      lastError = error;
      
      if (!isRetryableError(error)) {
        // リトライ不可能なエラーは即座にスロー
        throw error;
      }
      
      if (attempt < maxRetries) {
        // 指数バックオフで待機時間を計算（最大60秒）
        const delayMs = Math.min(baseDelayMs * Math.pow(2, attempt), 60000);
        const jitter = Math.random() * 1000; // 0-1秒のランダムなジッター
        const totalDelay = delayMs + jitter;
        
        console.warn(
          `⚠️  [${operationName}] エラー発生 (試行 ${attempt + 1}/${maxRetries + 1}): ${error.message || error.code}`
        );
        console.warn(`    ${Math.round(totalDelay / 1000)}秒後にリトライします...`);
        
        await new Promise(resolve => setTimeout(resolve, totalDelay));
      } else {
        // 最大リトライ回数に達した
        console.error(`❌ [${operationName}] 最大リトライ回数に達しました`);
        throw lastError;
      }
    }
  }
  
  throw lastError;
}

// ==============================
// ヘルパー
// ==============================

function normalizeHeader(h: string): string {
  return h
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/_/g, "")
    .replace(/[（）()]/g, "");
}

function isEmptyValue(v: any): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  return false;
}

function normalizeCompanyName(name: string): string {
  return name.trim().replace(/\s+/g, "");
}

function normalizeAddress(addr: string): string {
  return addr.trim().replace(/\s+/g, "");
}

// 数値フィールドはざっくり数値変換しておく（カンマ除去）
const NUMERIC_FIELDS = new Set<string>([
  "capitalStock",
  "revenue",
  "employeeCount",
  "factoryCount",
  "officeCount",
  "storeCount",
  "foundingYear",
  "fiscalMonth",
  "changeCount",
  "updateCount",
]);

// companies_new の新規ドキュメント用テンプレート
// 指定されたフィールド一覧のみを使用し、存在しないフィールドはnullまたは適切な初期値で埋める
const COMPANY_TEMPLATE: Record<string, any> = {
  acquisition: null,
  adExpiration: null,
  address: null,
  businessDescriptions: null,
  capitalStock: null,
  changeCount: null,
  clients: null,
  companyDescription: null,
  companyUrl: null,
  contactFormUrl: null,
  corporateNumber: null,
  corporationType: null,
  createdAt: null,
  demandProducts: null,
  email: null,
  employeeCount: null,
  established: null,
  executives: null,
  facebook: null,
  factoryCount: null,
  fax: null,
  financials: null,
  fiscalMonth: null,
  foundingYear: null,
  headquartersAddress: null,
  industries: [],
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
  shareholders: [],
  storeCount: null,
  suppliers: [],
  tags: [],
  updateCount: null,
  updatedAt: null,
  urls: [],
  wantedly: null,
  youtrust: null,
};

// 既存データの型に合わせて値を変換する（※現状は未使用だが保持）
function castValueToMatchExistingType(
  field: string,
  raw: string,
  existingValue: any
): any {
  const v = raw.trim();
  if (v === "") return null;

  // 既存データの型を優先
  if (existingValue !== null && existingValue !== undefined) {
    const existingType = typeof existingValue;
    
    // 既存が数値の場合
    if (existingType === "number") {
      const n = Number(v.replace(/[,，]/g, ""));
      if (!Number.isNaN(n)) return n;
      // 数値に変換できない場合は既存の型を維持（既存値を保持）
      return existingValue;
    }
    
    // 既存が配列の場合
    if (Array.isArray(existingValue)) {
      // 配列の場合は、文字列を配列に変換（カンマ区切りなど）
      if (v.includes(",")) {
        return v
          .split(",")
          .map((s) => s.trim())
          .filter((s) => s !== "");
      }
      return [v];
    }
    
    // 既存がオブジェクトの場合
    if (existingType === "object" && !Array.isArray(existingValue)) {
      // オブジェクトの場合は既存の型を維持（既存値を保持）
      return existingValue;
    }
    
    // 既存が文字列の場合は文字列として保持
    if (existingType === "string") {
      return v;
    }
    
    // 既存がbooleanの場合
    if (existingType === "boolean") {
      const lower = v.toLowerCase();
      if (lower === "true" || lower === "1" || lower === "yes") return true;
      if (lower === "false" || lower === "0" || lower === "no") return false;
      return existingValue; // 変換できない場合は既存値を保持
    }
  }

  // 既存データがない場合は、フィールド定義に基づいて型を推測
  if (NUMERIC_FIELDS.has(field)) {
    const n = Number(v.replace(/[,，]/g, ""));
    if (!Number.isNaN(n)) return n;
    return v; // 数値にできなければ文字列のまま
  }

  // それ以外は文字列で保持
  return v;
}

// 従来のcastValue関数（後方互換性のため保持）
// ※ corporateNumber は指数表記を数値文字列に直す
function castValue(field: string, raw: string): any {
  const v = raw.trim();
  if (v === "") return null;

  if (field === "corporateNumber") {
    const s = v.replace(/"/g, "");
    // 例: 3.12E+12 / 3.12e+12
    if (/^\d+(\.\d+)?e\+\d+$/i.test(s)) {
      const n = Number(s);
      if (!Number.isNaN(n)) {
        return Math.round(n).toString();
      }
    }
    return s;
  }

  if (NUMERIC_FIELDS.has(field)) {
    const n = Number(v.replace(/[,，]/g, ""));
    if (!Number.isNaN(n)) return n;
    return v;
  }

  return v;
}

// ==============================
// CSV → companies_new フィールドマッピング定義
// （必要に応じてここにどんどん alias を足してください）
// ==============================

type FieldConfig = {
  target: string; // companies_new 側のフィールド名
  aliases: string[]; // CSV カラムに現れ得る名称
};

const FIELD_CONFIGS: FieldConfig[] = [
  {
    target: "name",
    aliases: [
      "会社名",
      "企業名",
      "社名",
      "会社",
      "company",
      "companyname",
      "company_name",
    ],
  },
  {
    target: "representativeName",
    aliases: [
      "代表者名",
      "代表者",
      "代表取締役",
      "代表取締役名",
      "代表",
      "社長",
      "representative",
      "ceo",
    ],
  },
  {
    target: "corporateNumber",
    aliases: ["法人番号", "corporatenumber", "corporate_number", "houjinbango"],
  },
  {
    target: "companyUrl",
    aliases: [
      "hp",
      "hpurl",
      "ホームページ",
      "ホームページurl",
      "website",
      "webサイト",
      "url",
      "会社url",
      "companyurl",
      "detailurl",
      "企業ホームページurl",
      "企業ホームページURL",
    ],
  },
  {
    target: "contactFormUrl",
    aliases: [
      "問い合わせフォーム",
      "お問い合わせフォーム",
      "問合せフォーム",
      "contactform",
      "contact_form",
      "contacturl",
      "お問い合わせurl",
      "contacturl",
      "お問い合わせURL",
      "お問い合わせurl",
    ],
  },
  {
    target: "headquartersAddress",
    aliases: [
      "本社住所",
      "本店所在地",
      "所在地",
      "住所",
      "headquartersaddress",
      "headquarters",
      "address",
    ],
  },
  {
    target: "address",
    aliases: ["address", "住所", "所在地"],
  },
  {
    target: "prefecture",
    aliases: ["都道府県", "prefecture"],
  },
  {
    target: "postalCode",
    aliases: ["郵便番号", "postalcode", "postal_code", "zipcode"],
  },
  {
    target: "capitalStock",
    aliases: ["資本金", "資本金（千円）", "capital", "capitalstock"],
  },
  {
    target: "revenue",
    aliases: ["売上", "売上高", "売上（千円）", "revenue", "sales"],
  },
  {
    target: "employeeCount",
    aliases: [
      "従業員数",
      "社員数",
      "従業員数（人）",
      "employee",
      "employeecount",
      "employees",
    ],
  },
  {
    target: "overview",
    aliases: ["概要", "会社概要", "overview", "gaiyou"],
  },
  {
    target: "companyDescription",
    aliases: [
      "説明",
      "説明文",
      "description",
      "companydescription",
      "history",
      "沿革",
    ],
  },
  {
    target: "businessDescriptions",
    aliases: [
      "事業内容",
      "事業内容説明",
      "businessdescriptions",
      "businessdescription",
      "会社情報・備考",
      "得意分野",
    ],
  },
  {
    target: "industry",
    aliases: [
      "業種",
      "業種1",
      "業種１",
      "industry",
      "industry1",
      "primaryindustry",
      "業種（分類１）",
      "業種（分類2）",
      "業種（分類３）",
    ],
  },
  {
    target: "industryLarge",
    aliases: [
      "業界大分類",
      "業種大",
      "業種（大）",
      "業種1",
      "業種（分類１）",
      "industrylarge",
      "industryLarge",
      // "業界" は除外（「業界(最大3つ)」の特別処理と競合するため）
    ],
  },
  {
    target: "industryMiddle",
    aliases: [
      "業界中分類",
      "業種中",
      "業種（中）",
      "業種2",
      "業種（分類２）",
      "industrymiddle",
      "industryMiddle",
    ],
  },
  {
    target: "industrySmall",
    aliases: [
      "業界小分類",
      "業種小",
      "業種（小）",
      "業種3",
      "業種（分類３）",
      "industrysmall",
      "industrySmall",
    ],
  },
  {
    target: "industryDetail",
    aliases: [
      "業界細分類",
      "業種細",
      "業種（細）",
      "業種4",
      "業種（分類４）",
      "industrydetail",
      "industryDetail",
    ],
  },
  {
    target: "industries",
    aliases: [
      "業種リスト",
      "業種一覧",
      "industries",
      "industrylist",
      "業種2",
      "業種3",
      "業種（分類１）",
      "業種（分類2）",
      "業種（分類３）",
    ],
  },
  {
    target: "phoneNumber",
    aliases: ["電話番号", "電話", "phonenumber", "phone", "tel"],
  },
  {
    target: "fax",
    aliases: ["fax", "fax番号", "ファックス", "FAX番号"],
  },
  {
    target: "email",
    aliases: ["メール", "email", "e-mail", "メールアドレス"],
  },
  {
    target: "foundingYear",
    aliases: ["設立年", "創業年", "foundingyear", "founded", "established"],
  },
  {
    target: "established",
    aliases: ["設立年月日", "設立日", "established", "establishmentdate"],
  },
  {
    target: "listing",
    aliases: ["上場", "上場区分", "listing", "listed"],
  },
  {
    target: "tags",
    aliases: ["タグ", "tags", "tag"],
  },
  {
    target: "urls",
    aliases: ["urls", "url一覧", "urlリスト"],
  },
  // ---- inserted new FieldConfig entries ----
  {
    target: "representativeBirthDate",
    aliases: [
      "代表者誕生日",
      "代表者生年月日",
      "代表者誕生日日付",
      "社長誕生日",
      "社長生年月日",
    ],
  },
  {
    target: "clients",
    aliases: [
      "取引先",
      "主要取引先",
      "顧客",
      "client",
      "clients",
      "子会社・関連会社",
      "国内・海外の子会社",
    ],
  },
  {
    target: "suppliers",
    aliases: [
      "仕入れ先",
      "主要仕入先",
      "仕入先",
      "取引先銀行",
      "メインバンク",
      "取引銀行",
      "取引先銀行名",
      "banks",
      "[募集人数][実績][主な取引銀行]",
    ],
  },
  {
    target: "executives",
    aliases: [
      "取締役",
      "役員",
      "executives",
      "boardmembers",
      "役員一覧",
    ],
  },
  {
    target: "shareholders",
    aliases: [
      "株主",
      "株主構成",
      "shareholders",
    ],
  },
  {
    target: "officeCount",
    aliases: [
      "オフィス数",
      "事業所数",
      "拠点数",
      "officecount",
      "[国内の事業所]",
    ],
  },
  {
    target: "factoryCount",
    aliases: [
      "工場数",
      "工場拠点数",
      "factorycount",
    ],
  },
  {
    target: "storeCount",
    aliases: [
      "店舗数",
      "店舗拠点数",
      "storecount",
    ],
  },
  {
    target: "fiscalMonth",
    aliases: [
      "決算月",
      "決算月1",
      "fiscalmonth", 
      "fiscalMonth", 
      "決算期",
    ],
  },
  {
    target: "financials",
    aliases: [
      "決算情報",
      "決算",
      "利益1",
      "利益2",
      "利益3",
      "利益4",
      "利益5",
      "経常利益",
    ],
  },
];

// ヘッダ → target フィールド名 のマップを作る
function buildHeaderToFieldMap(headers: string[]): Record<string, string> {
  const map: Record<string, string> = {};

  // FIELD_CONFIGS の alias を正規化したマップ
  const normalizedAliasMap: { target: string; aliasNorms: string[] }[] =
    FIELD_CONFIGS.map((cfg) => ({
      target: cfg.target,
      aliasNorms: cfg.aliases.map((a) => normalizeHeader(a)),
    }));

  // COMPANY_TEMPLATE のフィールド名も正規化しておき、
  // 「ヘッダ名 = フィールド名」のケースは自動でマッピングする
  const templateFieldNorms: { field: string; norm: string }[] = Object.keys(
    COMPANY_TEMPLATE
  ).map((field) => ({
    field,
    norm: normalizeHeader(field),
  }));

  const unmappedHeaders: string[] = [];

  for (const header of headers) {
    // 「業界(最大3つ)」のような特別処理が必要なヘッダーはマッピングしない
    const norm = normalizeHeader(header);
    if (
      (norm.includes("業界") || norm.includes("industry")) &&
      (norm.includes("最大3") || norm.includes("max3"))
    ) {
      // このヘッダーは特別処理で処理されるため、マッピングしない
      continue;
    }
    
    let matchedTarget: string | null = null;

    // 1) まず FIELD_CONFIGS の alias でマッチを試みる
    for (const cfg of normalizedAliasMap) {
      for (const aliasNorm of cfg.aliasNorms) {
        if (
          norm === aliasNorm ||
          norm.includes(aliasNorm) ||
          aliasNorm.includes(norm)
        ) {
          matchedTarget = cfg.target;
          break;
        }
      }
      if (matchedTarget) break;
    }

    // 2) alias でマッチしなければ、COMPANY_TEMPLATE のフィールド名でマッチを試す
    if (!matchedTarget) {
      for (const tf of templateFieldNorms) {
        if (norm === tf.norm) {
          matchedTarget = tf.field;
          break;
        }
      }
    }

    if (matchedTarget) {
      map[header] = matchedTarget;
    } else {
      unmappedHeaders.push(header);
    }
  }

  if (unmappedHeaders.length > 0) {
    console.log(
      "⚠️ マッピングされなかったヘッダ:",
      unmappedHeaders.map((h) => `"${h}"`).join(", ")
    );
    console.log(
      "   → FIELD_CONFIGS に alias を追加するか、companies_new のフィールド名と同じヘッダ名にすると自動マッピングされます。"
    );
  }

  return map;
}

// 生年月日パターンを検出して抽出
function extractBirthDateFromValue(value: string): { date: string | null; cleaned: string } {
  if (!value || typeof value !== "string") {
    return { date: null, cleaned: value || "" };
  }

  let cleaned = value.trim();
  let extractedDate: string | null = null;

  // 生年月日パターン（1900-2100年の範囲）
  const birthdatePatterns = [
    /(19\d{2}|20\d{2})[\/年-](\d{1,2})[\/月-](\d{1,2})/g,  // 1977/1/1, 1977-1-1, 1977年1月1日
    /(19\d{2}|20\d{2})\/(\d{1,2})\/(\d{1,2})/g,            // 1977/1/1
  ];

  for (const pattern of birthdatePatterns) {
    const match = cleaned.match(pattern);
    if (match) {
      // 最初のマッチを取得
      const dateStr = match[0];
      const parts = dateStr.split(/[\/年-]/);
      if (parts.length >= 3) {
        const year = parseInt(parts[0]);
        const month = parseInt(parts[1]);
        const day = parseInt(parts[2]);
        
        // 有効な生年月日かチェック
        if (year >= 1900 && year <= 2100 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
          extractedDate = dateStr;
          // 生年月日部分を除去
          cleaned = cleaned.replace(pattern, "").trim();
          // 前後の記号やスペースを除去
          cleaned = cleaned.replace(/^[\s・、,，\-]/g, "").replace(/[\s・、,，\-]$/g, "").trim();
          break;
        }
      }
    }
  }

  return { date: extractedDate, cleaned };
}

// 代表者名から個人名（氏名）のみを抽出
function extractPersonNameFromRepresentative(representativeName: string | null | undefined): string | null {
  if (!representativeName || typeof representativeName !== "string") return null;
  
  let trimmed = representativeName.trim();
  if (!trimmed) return null;
  
  // 役職名を除去
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
  
  // カッコ内の情報を除去
  trimmed = trimmed.replace(/[（(].*?[）)]/g, "").trim();
  
  // 生年月日パターンを除去
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})[\/年-]\d{1,2}[\/月-]\d{1,2}/g, "").trim();
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})\/\d{1,2}\/\d{1,2}/g, "").trim();
  
  // 数字や記号のみの場合はnull
  if (/^[\d\s\-・、,，.。]+$/.test(trimmed)) {
    return null;
  }
  
  return trimmed || null;
}

// 1行分の CSV → companies_new 用の部分データ
function mapRowToCompanyFields(
  row: Record<string, string>,
  headerToField: Record<string, string>
): Record<string, any> {
  const result: Record<string, any> = {};
  
  // 業界フィールドの処理用（「業界(最大3つ)」のようなカンマ区切りを処理）
  let industryFieldProcessed = false;

  // まず、業界フィールドを特別処理（他のフィールド処理より先に実行）
  for (const [header, rawValue] of Object.entries(row)) {
    if (rawValue === undefined || rawValue === null) continue;
    const v = String(rawValue);
    if (v.trim() === "") continue;

    // 業界フィールドの特別処理（「業界(最大3つ)」のようなカンマ区切りの値を処理）
    const normalizedHeader = normalizeHeader(header);
    if (
      (normalizedHeader.includes("業界") || normalizedHeader.includes("industry")) &&
      (normalizedHeader.includes("最大3") || normalizedHeader.includes("max3"))
    ) {
      industryFieldProcessed = true;
      
      // カンマ区切りで分割
      const industries = v.split(",").map((s: string) => s.trim()).filter((s: string) => s !== "");
      if (industries.length > 0) {
        result.industryLarge = industries[0] || null;
      }
      if (industries.length > 1) {
        result.industryMiddle = industries[1] || null;
      }
      if (industries.length > 2) {
        result.industrySmall = industries[2] || null;
      }
      if (industries.length > 3) {
        // 4番目以降はindustryDetailにまとめる（カンマ区切りで結合）
        result.industryDetail = industries.slice(3).join(",") || null;
      }
      // このヘッダーは処理済みなので、後続のループでスキップするためにheaderToFieldから除外
      // （実際には後続でtargetが取得できなくなるので自動的にスキップされる）
    }
  }

  // 通常のフィールド処理
  for (const [header, rawValue] of Object.entries(row)) {
    const target = headerToField[header];
    if (!target) continue;

    if (rawValue === undefined || rawValue === null) continue;
    const v = String(rawValue);
    if (v.trim() === "") continue;

    // 業界フィールドが既に処理済みの場合は、個別の業界フィールドは上書きしない
    if (
      industryFieldProcessed &&
      (target === "industryLarge" || target === "industryMiddle" || target === "industrySmall" || target === "industryDetail")
    ) {
      // 既に業界フィールドで処理されている場合はスキップ
      continue;
    }

    // 代表者名の特別処理
    if (target === "representativeName") {
      // 生年月日を抽出
      const { date, cleaned } = extractBirthDateFromValue(v);
      
      // 生年月日を抽出した場合、representativeBirthDateに設定（既に値がある場合は上書きしない）
      if (date && !result.representativeBirthDate) {
        result.representativeBirthDate = date;
      }
      
      // 個人名（氏名）のみを抽出
      const personName = extractPersonNameFromRepresentative(cleaned);
      if (personName) {
        result[target] = personName;
      } else if (cleaned && cleaned.trim().length > 0) {
        // 個人名として抽出できなかった場合でも、生年月日以外の部分があれば使用
        result[target] = cleaned;
      }
    } else {
      result[target] = castValue(target, v);
    }
  }

  return result;
}

// ドキュメントIDを数字のみの文字列に統一する
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
    const directSnap = await retryOperation(
      () => directRef.get(),
      `法人番号直接参照: ${normalizedCorpNum}`
    );
    if (directSnap.exists) {
      cacheByCorporateNumber.set(normalizedCorpNum, directRef);
      return { ref: directRef, matchedBy: "corporateNumber" };
    }

    // 念のため、corporateNumber フィールドでの検索もフォールバックとして試す
    const snapByCorp = await retryOperation(
      () => companiesCol
        .where("corporateNumber", "==", normalizedCorpNum)
        .limit(1)
        .get(),
      `法人番号検索: ${normalizedCorpNum}`
    );
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
    const nameNorm = normalizeCompanyName(companyName);

    // CSV側の住所候補を収集（重複を避ける）
    const csvAddresses: { type: "headquartersAddress" | "address"; value: string }[] = [];
    if (headquartersAddress && headquartersAddress.trim()) {
      csvAddresses.push({ type: "headquartersAddress", value: headquartersAddress.trim() });
    }
    if (address && address.trim()) {
      // headquartersAddress と同じ文字列なら重複させない
      if (!headquartersAddress || headquartersAddress.trim() !== address.trim()) {
        csvAddresses.push({ type: "address", value: address.trim() });
      }
    }

    // 企業名でまず検索して、取得したドキュメントの中で住所が一致するものを探す
    // （Firestoreのwhereクエリでは複数フィールドのOR検索ができないため）
    // キャッシュキーで企業名+住所の組み合わせをチェック
    const csvAddrNorm = csvAddresses.length > 0 ? normalizeAddress(csvAddresses[0].value) : "";
    const cacheKeyForName = `${nameNorm}|${csvAddrNorm}`;
    const cachedByNameAndAddr = cacheByNameAndAddress.get(cacheKeyForName);
    if (cachedByNameAndAddr !== undefined) {
      if (cachedByNameAndAddr) {
        return { ref: cachedByNameAndAddr, matchedBy: "nameAndHeadquartersAddress" };
      }
      // キャッシュにない場合は検索をスキップ（既に検索済み）
      return null;
    }
    
    const nameSnap = await retryOperation(
      () => companiesCol
        .where("name", "==", nameTrimmed)
        .limit(50) // 同名企業の上限を削減（パフォーマンス向上）
        .get(),
      `企業名検索: ${nameTrimmed}`
    );

    if (!nameSnap.empty) {
      // 取得したドキュメントの中で、住所が一致するものを探す
      for (const doc of nameSnap.docs) {
        const data = doc.data();
        const docHeadquartersAddress = data.headquartersAddress ? String(data.headquartersAddress).trim() : "";
        const docAddress = data.address ? String(data.address).trim() : "";

        // CSV側の各住所候補と、ドキュメント側のaddress/headquartersAddressの両方を比較
        for (const csvAddr of csvAddresses) {
          const csvAddrValue = csvAddr.value;
          const csvAddrNorm = normalizeAddress(csvAddrValue);

          // ドキュメント側のheadquartersAddressと比較
          if (docHeadquartersAddress && normalizeAddress(docHeadquartersAddress) === csvAddrNorm) {
            const cacheKey = `${nameNorm}|${csvAddr.type}:${csvAddrNorm}`;
            cacheByNameAndAddress.set(cacheKey, doc.ref);
            return { ref: doc.ref, matchedBy: "nameAndHeadquartersAddress" };
          }

          // ドキュメント側のaddressと比較
          if (docAddress && normalizeAddress(docAddress) === csvAddrNorm) {
            const cacheKey = `${nameNorm}|${csvAddr.type}:${csvAddrNorm}`;
            cacheByNameAndAddress.set(cacheKey, doc.ref);
            return { ref: doc.ref, matchedBy: "nameAndAddress" };
          }

          // さらに、CSV側の住所がheadquartersAddressの場合、ドキュメント側のaddressとも比較
          if (csvAddr.type === "headquartersAddress" && docAddress && normalizeAddress(docAddress) === csvAddrNorm) {
            const cacheKey = `${nameNorm}|headquartersAddress:${csvAddrNorm}`;
            cacheByNameAndAddress.set(cacheKey, doc.ref);
            return { ref: doc.ref, matchedBy: "nameAndAddress" };
          }

          // CSV側の住所がaddressの場合、ドキュメント側のheadquartersAddressとも比較
          if (csvAddr.type === "address" && docHeadquartersAddress && normalizeAddress(docHeadquartersAddress) === csvAddrNorm) {
            const cacheKey = `${nameNorm}|address:${csvAddrNorm}`;
            cacheByNameAndAddress.set(cacheKey, doc.ref);
            return { ref: doc.ref, matchedBy: "nameAndHeadquartersAddress" };
          }
        }
      }
    }

    // 見つからなかった場合はキャッシュに記録（すべての候補に対して）
    // キャッシュキーを最適化（最初の住所のみをキャッシュキーとして使用）
    if (csvAddresses.length > 0) {
      const firstAddr = csvAddresses[0];
      const csvAddrNorm = normalizeAddress(firstAddr.value);
      const cacheKey = `${nameNorm}|${firstAddr.type}:${csvAddrNorm}`;
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
    let snapByName = await retryOperation(
      () => companiesCol
        .where("name", "==", companyName.trim())
        .limit(1)
        .get(),
      `企業名検索(name): ${companyName.trim()}`
    );
    if (!snapByName.empty) {
      const ref = snapByName.docs[0].ref;
      cacheByName.set(normalizedName, ref);
      return { ref, matchedBy: "companyName" };
    }

    // 旧スキーマ: companyName フィールドでの検索もフォールバックで試す
    snapByName = await retryOperation(
      () => companiesCol
        .where("companyName", "==", companyName.trim())
        .limit(1)
        .get(),
      `企業名検索(companyName): ${companyName.trim()}`
    );
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

// ==============================
// メイン処理
// ==============================

async function main() {
  // CSVファイル or ディレクトリパスを取得
  let csvPath = process.argv[2];
  
  if (!csvPath) {
    console.error("❌ エラー: CSVファイルまたはディレクトリのパスが指定されていません");
    console.error("");
    console.error("   使用方法:");
    console.error("     npx ts-node scripts/import_companies_from_csv.ts <csv-file or dir>");
    console.error("     または");
    console.error("     npx ts-node scripts/import_companies_from_csv.ts <serviceAccountKey.json> <csv-file or dir>");
    process.exit(1);
  }

  const absPath = path.resolve(csvPath);
  if (!fs.existsSync(absPath)) {
    console.error(`CSV ファイル/ディレクトリが見つかりません: ${absPath}`);
    process.exit(1);
  }

  const stats = fs.statSync(absPath);
  let csvFiles: string[] = [];

  if (stats.isDirectory()) {
    // ディレクトリ配下の .csv をすべて対象にする（再帰的に）
    const findCSVFiles = (dir: string): string[] => {
      const files: string[] = [];
      const entries = fs.readdirSync(dir);
      for (const entry of entries) {
        const fullPath = path.join(dir, entry);
        const stat = fs.statSync(fullPath);
        if (stat.isDirectory()) {
          files.push(...findCSVFiles(fullPath));
        } else if (entry.toLowerCase().endsWith(".csv")) {
          files.push(fullPath);
        }
      }
      return files;
    };
    csvFiles = findCSVFiles(absPath).sort();
    if (csvFiles.length === 0) {
      console.log(`指定ディレクトリ内に CSV ファイルがありません: ${absPath}`);
      return;
    }
    console.log(`📂 ディレクトリ内の CSV を再帰的に処理します: ${absPath}`);
    console.log(`   見つかった CSV ファイル数: ${csvFiles.length} 件`);
  } else {
    // 単一 CSV
    csvFiles = [absPath];
  }

  // 🔁 再開用設定
  const startFromFile = process.env.START_FROM_FILE || "";
  const startFromRowEnv = process.env.START_FROM_ROW;
  const startFromRow =
    startFromRowEnv && !Number.isNaN(Number(startFromRowEnv))
      ? Math.max(1, Number(startFromRowEnv))
      : 1;
  let started = !startFromFile; // START_FROM_FILE 未指定なら最初から開始

  let createdCount = 0;
  let updatedCount = 0;
  let skippedNoKey = 0;

  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 500; // Firestoreの最大バッチサイズに設定

  let globalRowIndex = 0;

  for (const csvFile of csvFiles) {
    const baseName = path.basename(csvFile);

    // START_FROM_FILE が指定されている場合、該当ファイルに到達するまでスキップ
    if (!started) {
      if (baseName === startFromFile) {
        started = true;
        console.log(
          `▶️  ここから再開: ${baseName}` +
            (startFromRow > 1 ? ` (row >= ${startFromRow})` : "")
        );
      } else {
        console.log(`⏭  スキップ: ${baseName}`);
        continue;
      }
    }

    // CSV読み込みログを削減（10ファイルごとに1回のみ）
    const fileIndex = csvFiles.indexOf(csvFile);
    if (fileIndex % 10 === 0 || fileIndex === csvFiles.length - 1) {
      console.log(`\n📄 CSV 読み込み中 (${fileIndex + 1}/${csvFiles.length}): ${csvFile}`);
    }
    const content = fs.readFileSync(csvFile, "utf8");

    const records: Record<string, string>[] = parse(content, {
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
      // 51.csv などで発生する不正なクォートを許容
      relax_quotes: true,
      // どうしてもパースできない行はスキップして処理を継続
      skip_records_with_error: true,
    });

    if (records.length === 0) {
      console.log("CSV に有効なレコードがありません。スキップします。");
      continue;
    }

    const headers = Object.keys(records[0] || {});
    const headerToField = buildHeaderToFieldMap(headers);

    // ヘッダーマッピングのログを削減（最初のファイルのみ表示）
    if (csvFiles.indexOf(csvFile) === 0 || (startFromFile && baseName === startFromFile)) {
      console.log("🧭 ヘッダ → フィールド対応:");
      Object.entries(headerToField).forEach(([h, f]) => {
        console.log(`  "${h}" -> "${f}"`);
      });
    }

    let rowSkipLogged = false;

    for (let i = 0; i < records.length; i++) {
      const rowNumber = i + 1;

      // ファイル内の指定行までスキップ（START_FROM_FILE と組み合わせる）
      if (
        startFromFile &&
        baseName === startFromFile &&
        startFromRow > 1 &&
        rowNumber < startFromRow
      ) {
        if (!rowSkipLogged) {
          console.log(
            `⏭  ${baseName}: ${startFromRow} 行目未満をスキップ中…`
          );
          rowSkipLogged = true;
        }
        continue;
      }

      const row = records[i];
      globalRowIndex++;
      const mapped = mapRowToCompanyFields(row, headerToField);

      // 法人番号・企業名・住所を取得（空白を除去）
      const corporateNumber =
        typeof mapped.corporateNumber === "string" && mapped.corporateNumber.trim()
          ? mapped.corporateNumber.trim()
          : null;
      const companyName =
        typeof mapped.name === "string" && mapped.name.trim()
          ? mapped.name.trim()
          : null;
      const headquartersAddress =
        typeof mapped.headquartersAddress === "string" && mapped.headquartersAddress.trim()
          ? mapped.headquartersAddress.trim()
          : null;
      const address =
        typeof mapped.address === "string" && mapped.address.trim()
          ? mapped.address.trim()
          : null;

      let targetRef: DocumentReference<DocumentData>;
      let writeData: Record<string, any> = {};

      const rowLabel = `${baseName} row ${rowNumber}`;

      // キー（法人番号／会社名）が無い場合でも、行に何らかのデータがあれば新規作成する
      if (!corporateNumber && !companyName) {
        if (Object.keys(mapped).length === 0) {
          // マッピング結果も空なら完全に無意味な行としてスキップ
          skippedNoKey++;
          if (skippedNoKey <= 5) {
            console.warn(
              `⚠️ [${rowLabel}] name / corporateNumber が見つからず、他の項目も空のためスキップ`
            );
          }
          continue;
        }

        // companies_new 内にキーで紐づけられないが、データはあるので常に新規作成
        const docId = generateNumericDocId(null, globalRowIndex);
        targetRef = companiesCol.doc(docId);
        
        // テンプレートをベースに CSV の値で上書きすることで、フィールド構成を揃える
        writeData = {
          ...COMPANY_TEMPLATE,
          ...mapped,
        };
        createdCount++;

        // ログ出力を削減（100件ごとに1回のみ、または最初の5件）
        if (createdCount <= 5 || createdCount % 100 === 0) {
          console.log(`✨ [${rowLabel}] キーなし新規作成 (docId: ${docId})`);
        }

        batch.set(targetRef, writeData, { merge: true });
      } else {
        // 既存企業の検索（法人番号 → 企業名 + 住所 → 企業名のみ の順で検索）
        const existingResult = await findExistingCompanyDoc(
          corporateNumber,
          companyName,
          headquartersAddress,
          address
        );

        if (existingResult) {
          // 既存企業が見つかった場合：既存データの型を優先し、不足フィールドを追加
          const snap = await retryOperation(
            () => existingResult.ref.get(),
            `既存企業データ取得: ${existingResult.ref.id}`
          );
          const current = snap.data() || {};

          const mergedFields: string[] = [];
          const addedFields: string[] = [];
          
          // CSVからマッピングされたデータを処理（スキーマに含まれるフィールドのみ）
          for (const [field, csvValue] of Object.entries(mapped)) {
            // スキーマに含まれていないフィールドは無視
            if (!(field in COMPANY_TEMPLATE)) {
              continue;
            }
            
            const existingValue = current[field];
            
            // 既存データにフィールドが存在しない場合 → 追加
            if (existingValue === undefined) {
              writeData[field] = castValue(field, String(csvValue));
              addedFields.push(field);
            }
            // 既存データがnull/空の場合 → 補完
            else if (isEmptyValue(existingValue)) {
              writeData[field] = castValue(field, String(csvValue));
              mergedFields.push(field);
            }
            // 既存データに値がある場合 → 既存データ優先（上書きしない）
          }
          
          // スキーマに含まれているが、既存データにないフィールドをnullで補完
          for (const field of Object.keys(COMPANY_TEMPLATE)) {
            if (current[field] === undefined && !(field in writeData)) {
              writeData[field] = COMPANY_TEMPLATE[field];
              addedFields.push(field);
            }
          }

          if (Object.keys(writeData).length === 0) {
            // 更新するフィールドがない場合はスキップ
            continue;
          }

          targetRef = existingResult.ref;
          updatedCount++;
          
          let matchInfo: string;
          switch (existingResult.matchedBy) {
            case "corporateNumber":
              matchInfo = `法人番号: ${corporateNumber}`;
              break;
            case "nameAndHeadquartersAddress":
              matchInfo = `企業名+本社住所: ${companyName} / ${headquartersAddress}`;
              break;
            case "nameAndAddress":
              matchInfo = `企業名+住所: ${companyName} / ${address}`;
              break;
            case "companyName":
            default:
              matchInfo = `企業名: ${companyName}`;
              break;
          }
          
          const updateInfo: string[] = [];
          if (mergedFields.length > 0) {
            updateInfo.push(`補完: ${mergedFields.join(", ")}`);
          }
          if (addedFields.length > 0) {
            updateInfo.push(`追加: ${addedFields.join(", ")}`);
          }
          
          // ログ出力を削減（100件ごとに1回のみ、または最初の5件）
          if (updatedCount <= 5 || updatedCount % 100 === 0) {
            console.log(
              `🔄 [${rowLabel}] 既存企業を更新 (${matchInfo}) - ${updateInfo.join(", ")}`
            );
          }
          
          // merge:true で既存データを上書きしない（既存データ優先）
          batch.set(targetRef, writeData, { merge: true });
        } else {
          // 新規作成（法人番号/企業名はあるが、既存ドキュメントが見つからない場合）
          const docId = generateNumericDocId(corporateNumber, globalRowIndex);
          targetRef = companiesCol.doc(docId);

          writeData = {
            ...COMPANY_TEMPLATE,
            ...mapped,
          };
          createdCount++;
          
          const keyInfo = corporateNumber 
            ? `法人番号: ${corporateNumber}` 
            : `企業名: ${companyName}`;
          // ログ出力を削減（100件ごとに1回のみ、または最初の5件）
          if (createdCount <= 5 || createdCount % 100 === 0) {
            console.log(`✨ [${rowLabel}] 新規企業を作成 (docId: ${docId}, ${keyInfo})`);
          }

          batch.set(targetRef, writeData, { merge: true });
        }
      }

      batchCount++;

      if (batchCount >= BATCH_LIMIT) {
        // ログ出力を削減（100件ごとに1回のみ）
        if (batchCount % 100 === 0 || batchCount === BATCH_LIMIT) {
          console.log(`💾 バッチコミット (${batchCount} 件) ...`);
        }
        await retryOperation(
          () => batch.commit(),
          `バッチコミット (${batchCount}件)`
        );
        batch = db.batch();
        batchCount = 0;
      }
    }
  }

  if (batchCount > 0) {
    console.log(`💾 最後のバッチコミット (${batchCount} 件) ...`);
    await retryOperation(
      () => batch.commit(),
      `最後のバッチコミット (${batchCount}件)`
    );
  }

  console.log("\n✅ インポート完了");
  console.log(`  ✨ 新規作成: ${createdCount} 件`);
  console.log(`  🔄 既存更新: ${updatedCount} 件`);
  console.log(`     - 既存データの型を優先`);
  console.log(`     - 既存にないフィールドを追加`);
  console.log(`     - 不足しているフィールド（null/空）を補完`);
  if (skippedNoKey > 0) {
    console.log(`  ⚠️  キー不足スキップ: ${skippedNoKey} 件`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});