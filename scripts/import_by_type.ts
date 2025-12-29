/*
  タイプ別CSV→companies_newインポートスクリプト
  
  全てのCSVヘッダーを明示的にマッピングし、漏れなくDBに格納します。
  
  使い方:
    npx ts-node scripts/import_by_type.ts --type=A [--dry-run]
    npx ts-node scripts/import_by_type.ts --type=B [--dry-run]
    npx ts-node scripts/import_by_type.ts --type=C [--dry-run]
    npx ts-node scripts/import_by_type.ts --type=D [--dry-run]
    npx ts-node scripts/import_by_type.ts --type=E [--dry-run]
    npx ts-node scripts/import_by_type.ts --type=F51 [--dry-run]
    npx ts-node scripts/import_by_type.ts --type=F130 [--dry-run]
    npx ts-node scripts/import_by_type.ts --type=F132 [--dry-run]
    npx ts-node scripts/import_by_type.ts --type=ALL [--dry-run]  # 全タイプ実行
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
const DRY_RUN = process.argv.includes("--dry-run");
const FORCE_OVERWRITE = process.argv.includes("--force"); // 強制上書きモード
const PARALLEL_COUNT = 20; // 並列処理数（爆速化）

// 既存の値が「想定外」かどうかをチェック
function isUnexpectedValue(field: string, existingValue: any): boolean {
  if (existingValue === null || existingValue === undefined || existingValue === "") {
    return false; // 空は想定通り
  }
  
  // shareholders, executives が配列になっている場合は想定外（文字列であるべき）
  if ((field === "shareholders" || field === "executives") && Array.isArray(existingValue)) {
    return true;
  }
  
  // nameフィールドに都道府県名のみが入っている場合は想定外（個人名は許可）
  if (field === "name" && typeof existingValue === "string") {
    const v = existingValue.trim();
    
    // 都道府県名のみの場合は想定外
    const prefs = ["北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
      "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
      "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
      "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
      "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
      "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
      "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"];
    
    if (prefs.includes(v)) {
      return true; // 都道府県名のみは想定外
    }
    
    // 注: 個人名が社名のケース（個人事業主など）もあるため、人名パターンは想定外としない
  }
  
  return false;
}

// コマンドライン引数からタイプを取得
function getTargetType(): string {
  const typeArg = process.argv.find((a) => a.startsWith("--type="));
  if (!typeArg) {
    console.error("❌ エラー: --type=X を指定してください（A, B, C, D, E, F51, F130, F132, ALL）");
    process.exit(1);
  }
  return typeArg.split("=")[1].toUpperCase();
}

// ==============================
// タイプ別ファイルリスト
// ==============================

const TYPE_FILES: Record<string, string[]> = {
  A: [
    "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
    "20", "21", "22", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35",
    "39", "52", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64",
    "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76", "77",
    "101", "104"
  ].map((n) => `csv/${n}.csv`),
  
  B: ["1", "2", "53", "103", "106", "126"].map((n) => `csv/${n}.csv`),
  
  C: [
    "23", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89",
    "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "100", "102", "105"
  ].map((n) => `csv/${n}.csv`),
  
  D: [
    // 法人番号とIDがあるファイル（都道府県あり）
    "24", "36", "37", "38", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50",
    "107", "108", "109", "110", "111", "112", "113", "114", "115", "116", "117", "118", "119",
    "120", "121", "122", "123", "124", "125", "132", "133", "134"
  ].map((n) => `csv/${n}.csv`),
  
  E: [
    // 都道府県あり、法人番号なし
    "3", "4", "5", "6"
  ].map((n) => `csv/${n}.csv`),
  
  F51: ["51"].map((n) => `csv/${n}.csv`),
  F130: ["130", "131"].map((n) => `csv/${n}.csv`),
  F127: ["127", "128"].map((n) => `csv/${n}.csv`),
  F129: ["129"].map((n) => `csv/${n}.csv`),
  F132: ["132"].map((n) => `csv/${n}.csv`),
};

// ==============================
// タイプ別ヘッダーマッピング（完全版）
// ==============================

// タイプA/B: 基本形式
const MAPPING_AB: Record<string, string> = {
  "法人番号": "corporateNumber",
  "会社名": "name",
  "電話番号": "phoneNumber",
  "会社郵便番号": "postalCode",
  "会社住所": "address",
  "URL": "companyUrl",
  "代表者名": "representativeName",
  "代表者郵便番号": "representativeRegisteredAddress",
  "代表者住所": "representativeHomeAddress",
  "代表者誕生日": "representativeBirthDate",
  "営業種目": "businessDescriptions",
  "設立": "established",
  "株主": "shareholders",
  "取締役": "executives",
  "概況": "overview",
  "業種-大": "industryLarge",
  "業種-中": "industryMiddle",
  "業種-小": "industrySmall",
  "業種-細": "industryDetail",
};

// タイプC: 重複ヘッダー形式（位置ベースで処理）
const MAPPING_C_BY_INDEX: Record<number, string> = {
  0: "name",              // 会社名
  1: "phoneNumber",       // 電話番号
  2: "postalCode",        // 郵便番号（会社）
  3: "address",           // 住所（会社）
  4: "companyUrl",        // URL
  5: "representativeName", // 代表者
  6: "representativeRegisteredAddress", // 郵便番号（代表者）
  7: "representativeHomeAddress", // 住所（代表者）
  8: "foundingYear",      // 創業
  9: "",                  // 空列
  10: "established",      // 設立
  11: "shareholders",     // 株式保有率
  12: "executives",       // 役員
  13: "overview",         // 概要
  14: "industryLarge",    // 業種（大）
  15: "industryMiddle",   // 業種（中）
  16: "industrySmall",    // 業種（小）
  17: "industryDetail",   // 業種（細）
};

// タイプD: 都道府県・ID詳細形式（位置ベースで処理 - ヘッダーとデータの列数不一致に対応）
const MAPPING_D_BY_INDEX: Record<number, string> = {
  0: "name",                        // 会社名
  1: "prefecture",                  // 都道府県
  2: "representativeName",          // 代表者名
  3: "corporateNumber",             // 法人番号
  4: "metaDescription",            // ID
  5: "tags",                        // 取引種別
  6: "tags",                        // SBフラグ
  7: "tags",                        // NDA
  8: "tags",                        // AD
  9: "tags",                        // ステータス
  10: "salesNotes",                 // 備考
  11: "companyUrl",                 // URL
  12: "industryLarge",              // 業種1
  13: "industryMiddle",             // 業種2
  14: "industrySmall",              // 業種3
  15: "industryDetail",             // 業種-細（データに含まれるがヘッダーにない場合がある）
  16: "postalCode",                 // 郵便番号
  17: "address",                    // 住所
  18: "established",                // 設立
  19: "phoneNumber",                // 電話番号(窓口)
  20: "representativeRegisteredAddress", // 代表者郵便番号
  21: "representativeHomeAddress",   // 代表者住所
  22: "representativeBirthDate",    // 代表者誕生日
  23: "capitalStock",               // 資本金
  24: "listing",                    // 上場
  25: "fiscalMonth",                // 直近決算年月
  26: "revenue",                    // 直近売上
  27: "financials",                 // 直近利益
  28: "companyDescription",         // 説明
  29: "overview",                   // 概要
  30: "suppliers",                  // 仕入れ先
  31: "clients",                    // 取引先
  32: "suppliers",                  // 取引先銀行
  33: "executives",                 // 取締役
  34: "shareholders",               // 株主
  35: "employeeCount",              // 社員数
  36: "officeCount",                // オフィス数
  37: "factoryCount",               // 工場数
  38: "storeCount",                 // 店舗数
};

// タイプD: ヘッダー名ベースのマッピング（フォールバック用）
const MAPPING_D: Record<string, string> = {
  "会社名": "name",
  "都道府県": "prefecture",
  "代表者名": "representativeName",
  "法人番号": "corporateNumber",
  "ID": "metaDescription", // 内部IDはmetaDescriptionに
  "取引種別": "tags", // tagsに追加
  "SBフラグ": "tags", // tagsに追加
  "NDA": "tags", // tagsに追加
  "AD": "tags", // tagsに追加
  "ステータス": "tags", // tagsに追加
  "備考": "salesNotes",
  "URL": "companyUrl",
  "業種1": "industryLarge",
  "業種2": "industryMiddle",
  "業種3": "industrySmall",
  "郵便番号": "postalCode",
  "住所": "address",
  "設立": "established",
  "電話番号(窓口)": "phoneNumber",
  "代表者郵便番号": "representativeRegisteredAddress",
  "代表者住所": "representativeHomeAddress",
  "代表者誕生日": "representativeBirthDate",
  "資本金": "capitalStock",
  "上場": "listing",
  "直近決算年月": "fiscalMonth",
  "直近売上": "revenue",
  "直近利益": "financials",
  "説明": "companyDescription",
  "概要": "overview",
  "仕入れ先": "suppliers",
  "取引先": "clients",
  "取引先銀行": "suppliers", // 銀行もsuppliersに追加
  "取締役": "executives",
  "株主": "shareholders",
  "社員数": "employeeCount",
  "オフィス数": "officeCount",
  "工場数": "factoryCount",
  "店舗数": "storeCount",
};

// タイプE: 都道府県形式（法人番号なし）- タイプDとほぼ同じ
const MAPPING_E = { ...MAPPING_D };

// タイプF51: 求人情報形式（重複ヘッダー「ジャンル」があるため、位置ベースで処理）
const MAPPING_F51_BY_INDEX: Record<number, string> = {
  0: "industryCategories", // ジャンル（1回目）
  1: "industryLarge",       // 業種（分類１）
  2: "industryMiddle",      // 業種（分類２）
  3: "industrySmall",       // 業種（分類３）
  4: "corporateNumber",     // 法人番号
  5: "name",                // 企業名
  6: "phoneNumber",         // 電話番号
  7: "fax",                 // FAX番号
  8: "email",               // メールアドレス
  9: "postalCode",          // 郵便番号
  10: "address",            // 住所
  11: "companyUrl",         // 企業ホームページURL
  12: "contactFormUrl",     // お問い合わせURL
  13: "representativeName",  // 代表者名
  14: "metaDescription",     // 部署・拠点名
  15: "companyDescription", // 会社情報・備考
  16: "businessDescriptions", // 得意分野
  17: "established",        // 設立年月日
  18: "listing",            // 上場区分
  19: "employeeCount",      // 従業員数
  20: "clients",            // 子会社・関連会社
  21: "shareholders",      // 主要株主
  22: "fiscalMonth",        // 決算期
  23: "capitalStock",       // 資本金
  24: "revenue",            // 売上高
  25: "financials",        // 経常利益
  26: "tags",               // ジャンル（2回目、重複なのでtagsに）
  27: "industry",           // 業種1
  28: "businessDescriptions", // 事業内容
  29: "foundingYear",       // 創業
  30: "suppliers",          // [募集人数][実績][主な取引銀行]
  31: "metaDescription",    // [平均年齢][平均勤続年数]
  32: "metaDescription",    // [月平均所定外労働時間][平均有給休暇取得日数][役員及び管理的地位にある者に占める女性の割合]
  33: "officeCount",        // [国内の事業所]
  34: "clients",            // 国内・海外の子会社
  35: "metaKeywords",       // [交通機関][加盟団体]
};

// タイプF51: ヘッダー名ベースのマッピング（フォールバック用）
const MAPPING_F51: Record<string, string> = {
  "ジャンル": "industryCategories",
  "業種（分類１）": "industryLarge",
  "業種（分類２）": "industryMiddle",
  "業種（分類３）": "industrySmall",
  "法人番号": "corporateNumber",
  "企業名": "name",
  "電話番号": "phoneNumber",
  "FAX番号": "fax",
  "メールアドレス": "email",
  "郵便番号": "postalCode",
  "住所": "address",
  "企業ホームページURL": "companyUrl",
  "お問い合わせURL": "contactFormUrl",
  "代表者名": "representativeName",
  "部署・拠点名": "metaDescription",
  "会社情報・備考": "companyDescription",
  "得意分野": "businessDescriptions",
  "設立年月日": "established",
  "上場区分": "listing",
  "従業員数": "employeeCount",
  "子会社・関連会社": "clients",
  "主要株主": "shareholders",
  "決算期": "fiscalMonth",
  "資本金": "capitalStock",
  "売上高": "revenue",
  "経常利益": "financials",
  "業種1": "industry",
  "事業内容": "businessDescriptions",
  "創業": "foundingYear",
  "[募集人数][実績][主な取引銀行]": "suppliers",
  "[平均年齢][平均勤続年数]": "metaDescription",
  "[月平均所定外労働時間][平均有給休暇取得日数][役員及び管理的地位にある者に占める女性の割合]": "metaDescription",
  "[国内の事業所]": "officeCount",
  "国内・海外の子会社": "clients",
  "[交通機関][加盟団体]": "metaKeywords",
};

// タイプF130: 英語ヘッダー
const MAPPING_F130: Record<string, string> = {
  "name": "name",
  "corporateNumber": "corporateNumber",
  "representative": "representativeName",
  "sales": "revenue",
  "capital": "capitalStock",
  "listing": "listing",
  "address": "address",
  "employees": "employeeCount",
  "founded": "established",
  "fiscalMonth": "fiscalMonth",
  "industries": "industry",
  "tel": "phoneNumber",
  "url": "companyUrl",
  "departments": "metaDescription",
  "people": "metaDescription",
  "rawText": "overview",
};

// タイプF132: 詳細形式
const MAPPING_F132: Record<string, string> = {
  "会社名": "name",
  "都道府県": "prefecture",
  "代表者名": "representativeName",
  "法人番号": "corporateNumber",
  "ID": "metaDescription", // 内部IDはmetaDescriptionに
  "種別": "tags", // tagsに追加
  "状態": "tags", // tagsに追加
  "NDA締結": "tags", // tagsに追加
  "AD締結": "tags", // tagsに追加
  "URL": "companyUrl",
  "担当者": "registrant",
  "業種1": "industryLarge",
  "業種2": "industryMiddle",
  "業種3": "industrySmall",
  "住所": "address",
  "設立": "established",
  "電話番号(窓口)": "phoneNumber",
  "郵便番号": "postalCode",
  "代表者誕生日": "representativeBirthDate",
  "資本金": "capitalStock",
  "上場": "listing",
  "決算月1": "fiscalMonth",
  "売上1": "revenue",
  "利益1": "financials",
  "決算月2": "metaDescription", // 過去の決算情報はmetaDescriptionに
  "売上2": "metaDescription",
  "利益2": "metaDescription",
  "決算月3": "metaDescription",
  "売上3": "metaDescription",
  "利益3": "metaDescription",
  "決算月4": "metaDescription",
  "売上4": "metaDescription",
  "利益4": "metaDescription",
  "決算月5": "metaDescription",
  "売上5": "metaDescription",
  "利益5": "metaDescription",
  "説明": "companyDescription",
  "概要": "overview",
  "仕入れ先": "suppliers",
  "取引先": "clients",
  "取引先銀行": "suppliers",
  "取締役": "executives",
  "株主": "shareholders",
  "社員数": "employeeCount",
  "オフィス数": "officeCount",
  "工場数": "factoryCount",
  "店舗数": "storeCount",
  "売DM最終送信日時": "metaDescription",
  "買DM最終送信日時": "metaDescription",
  "売手紙最終送付日時": "metaDescription",
  "買手最終荷電日時": "metaDescription",
  "社長手紙最終送付日時": "metaDescription",
  "SDS手紙最終送付日時": "metaDescription",
  "SDS社長手紙最終送付日時": "metaDescription",
};

// ==============================
// companies_new テンプレート
// ==============================
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
  shareholders: null,
  storeCount: null,
  suppliers: [],
  tags: [],
  updateCount: null,
  updatedAt: null,
  urls: [],
  wantedly: null,
  youtrust: null,
};

const NUMERIC_FIELDS = new Set<string>([
  "capitalStock", "employeeCount", "revenue", "factoryCount",
  "officeCount", "storeCount", "changeCount", "updateCount",
]);

const ARRAY_FIELDS = new Set<string>([
  "industries", "suppliers", "tags", "urls", "clients",
]);

// 文字列として保存するフィールド（カンマ区切りのまま保存）
const STRING_FIELDS = new Set<string>([
  "shareholders", "executives",
]);

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
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
    ];

    for (const pth of defaultPaths) {
      const resolved = path.resolve(pth);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        console.log(`ℹ️  サービスアカウントキー: ${resolved}`);
        break;
      }
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: serviceAccount.project_id,
  });
  console.log(`✅ Firebase 初期化完了 (Project: ${serviceAccount.project_id})`);
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// ==============================
// ヘルパー関数
// ==============================

const MAX_FIELD_LENGTH = 50000; // 50KB以下に制限（Firestoreは1MB制限）

function trim(v?: string | null): string | null {
  if (v === undefined || v === null) return null;
  let s = String(v).trim();
  if (s === "") return null;
  // 長すぎるテキストは切り詰め
  if (s.length > MAX_FIELD_LENGTH) {
    s = s.substring(0, MAX_FIELD_LENGTH) + "...(truncated)";
  }
  return s;
}

function parseNumeric(v: string): number | null {
  const cleaned = v.replace(/[^\d.-]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

// 値が適切なフィールドタイプかチェック
function isValueValidForField(value: string, field: string): boolean {
  const v = value.trim();
  if (!v) return false;

  // nameフィールド: 会社名パターンのみ許可（代表者名を除外）
  if (field === "name") {
    // 会社名・法人名パターン
    const legalEntityPatterns = [
      "株式会社", "有限会社", "合同会社", "合名会社", "合資会社",
      "一般社団法人", "一般財団法人", "公益社団法人", "公益財団法人",
      "特定非営利活動法人", "NPO法人",
      "学校法人", "医療法人", "社会福祉法人", "宗教法人",
      "相互会社", // 保険会社など
      "税理士法人", "弁護士法人", "司法書士法人", "行政書士法人", "監査法人", "特許業務法人",
      "農業協同組合", "漁業協同組合", "生活協同組合", "協同組合", "協組",
      "信用金庫", "信用組合", "労働金庫",
      "労働組合", "組合",
      "教団", "教会", "神社", "寺院", "寺",
      "商工会", "商工会議所",
      "事務所", "事業所", "研究所", "製作所", "工業所",
      "銀行", "証券", "保険",
      "工業", "産業", "商事", "商会", "物産", "通商",
      "建設", "工務店", "設計",
      "運輸", "運送", "物流", "倉庫",
      "不動産", "開発",
      "サービス", "システム", "ソリューション",
      "ホールディングス", "グループ", "コーポレーション",
    ];
    
    for (const pattern of legalEntityPatterns) {
      if (v.includes(pattern)) {
        return true;
      }
    }
    
    // 組織名のキーワードを含む場合は許可
    const organizationKeywords = [
      "社", "会", "庫", "組合", "団体", "法人", "財団", "基金", "協会",
      "赤十字", "金庫", "中央", "地方", "公共", "公営", "公社",
    ];
    
    for (const keyword of organizationKeywords) {
      if (v.includes(keyword)) {
        return true;
      }
    }
    
    // 長い名前（11文字以上）は会社名の可能性が高い
    if (v.length >= 11) {
      return true;
    }
    
    // 注: 個人名が社名のケース（個人事業主など）もあるため、人名パターンでもスキップしない
    
    // 7-10文字で組織名キーワードがない場合も、都道府県名でなければ許可
    if (v.length >= 7 && v.length <= 10) {
      const prefs = ["北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
        "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
        "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
        "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
        "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
        "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
        "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"];
      if (!prefs.includes(v)) {
        return true; // 都道府県名でなければ許可
      }
    }
    
    // それ以外は許可（会社名の可能性がある）
    return true;
  }

  // representativeNameフィールド: 人名パターン
  if (field === "representativeName") {
    // 会社名パターンは除外
    if (v.includes("株式会社") || v.includes("有限会社") || 
        v.includes("合同会社") || v.includes("合名会社")) {
      return false;
    }
    // 人名パターン（2-20文字、漢字・カナ・ひらがなを含む）
    if (/^[一-龥ぁ-んァ-ン・\s]{2,20}$/.test(v)) {
      return true;
    }
    return false;
  }

  // postalCode: 郵便番号パターン
  if (field === "postalCode" || field === "representativeRegisteredAddress") {
    return /^\d{3}-?\d{4}$/.test(v);
  }

  // phoneNumber, fax: 電話番号パターン
  if (field === "phoneNumber" || field === "fax") {
    if (!/^0\d/.test(v)) return false;
    const digits = v.replace(/\D/g, "");
    return digits.length >= 9 && digits.length <= 11;
  }

  // corporateNumber: 13桁の数字
  if (field === "corporateNumber") {
    return /^\d{13}$/.test(v.replace(/\D/g, ""));
  }

  // companyUrl, contactFormUrl: URLパターン（より緩い検証）
  if (field === "companyUrl" || field === "contactFormUrl") {
    // http:// または https:// で始まる
    if (/^https?:\/\//i.test(v)) return true;
    // ドメイン形式（.co.jp, .com, .jp など）
    if (/\.(co\.jp|com|jp|net|org|io|co|info|biz)/i.test(v)) return true;
    // www. で始まる
    if (/^www\./i.test(v)) return true;
    return false;
  }

  // email: メールアドレスパターン
  if (field === "email") {
    return /^[^@\s]+@[^@\s]+\.[^@\s]+$/.test(v);
  }

  // address, headquartersAddress: 住所パターン（都道府県を含む）
  if (field === "address" || field === "headquartersAddress" || field === "representativeHomeAddress") {
    const prefs = ["北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
      "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
      "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
      "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
      "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
      "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
      "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"];
    return prefs.some((p) => v.includes(p)) || /[市区町村]/.test(v);
  }

  // prefecture: 都道府県名のみ
  if (field === "prefecture") {
    const prefs = ["北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
      "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
      "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
      "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
      "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
      "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
      "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"];
    return prefs.includes(v) || prefs.some((p) => v.startsWith(p));
  }

  // その他のフィールドは基本的に許可
  return true;
}

function generateNumericDocId(corporateNumber: string | null, rowIndex: number): string {
  if (corporateNumber && /^[0-9]+$/.test(corporateNumber.trim())) {
    return corporateNumber.trim();
  }
  const timestamp = Date.now();
  const paddedIndex = String(rowIndex).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// CSVを位置ベースでパース（重複ヘッダー対応）
function parseCSVLineByPosition(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += char;
    }
  }
  result.push(current);
  return result;
}

// ==============================
// Firestore検索
// ==============================

async function findExistingDoc(
  corporateNumber: string | null,
  name: string | null
): Promise<{ ref: DocumentReference; data: any } | null> {
  if (corporateNumber) {
    const idCandidate = corporateNumber.trim();
    const byId = await companiesCol.doc(idCandidate).get();
    if (byId.exists) {
      return { ref: byId.ref, data: byId.data() };
    }
    const snap = await companiesCol.where("corporateNumber", "==", idCandidate).limit(1).get();
    if (!snap.empty) {
      return { ref: snap.docs[0].ref, data: snap.docs[0].data() };
    }
  }
  
  if (name) {
    const snap = await companiesCol.where("name", "==", name.trim()).limit(1).get();
    if (!snap.empty) {
      return { ref: snap.docs[0].ref, data: snap.docs[0].data() };
    }
  }
  
  return null;
}

// ==============================
// タイプ別処理関数
// ==============================

type CsvRow = Record<string, string>;

async function processTypeAB(files: string[], mapping: Record<string, string>) {
  console.log(`\n📥 タイプA/B の処理開始 (${files.length} ファイル)`);
  
  let totalRows = 0;
  let createdCount = 0;
  let updatedCount = 0;
  let globalIndex = 0;
  let unmappedHeaders = new Set<string>();

  for (const file of files) {
    if (!fs.existsSync(file)) {
      console.warn(`⚠️  ファイルなし: ${file}`);
      continue;
    }

    const buf = fs.readFileSync(file);
    const records: CsvRow[] = parse(buf, { columns: true, skip_empty_lines: true, relax_quotes: true });
    console.log(`  📄 ${path.basename(file)}: ${records.length} 行`);
    totalRows += records.length;

    for (const row of records) {
      globalIndex++;
      const data: Record<string, any> = { ...COMPANY_TEMPLATE };

      for (const [header, value] of Object.entries(row)) {
        const trimmedValue = trim(value);
        if (!trimmedValue) continue;

        const field = mapping[header];
        if (!field) {
          unmappedHeaders.add(header);
          continue;
        }
        if (field === "") continue; // 明示的に無視

        // 値の型チェック（nameに代表者名が入らないように）
        if (!isValueValidForField(trimmedValue, field)) {
          if (field === "name") {
            console.warn(`  ⚠️  [${path.basename(file)} row ${globalIndex}] nameフィールドに不適切な値: "${trimmedValue}" (スキップ)`);
          }
          continue;
        }

        if (NUMERIC_FIELDS.has(field)) {
          const num = parseNumeric(trimmedValue);
          if (num !== null) data[field] = num;
        } else if (ARRAY_FIELDS.has(field)) {
          if (!data[field] || !Array.isArray(data[field])) {
            data[field] = [];
          }
          data[field].push(trimmedValue);
        } else {
          data[field] = trimmedValue;
        }
      }

      const corporateNumber = data.corporateNumber as string | null;
      const name = data.name as string | null;

      if (!name && !corporateNumber) continue;

      const existing = await findExistingDoc(corporateNumber, name);

      if (existing) {
        if (FORCE_OVERWRITE) {
          // 強制上書きモード: 値があるフィールドは全て上書き
          const updateData: Record<string, any> = {};
          for (const [field, value] of Object.entries(data)) {
            if (value === null || value === undefined) continue;
            if (Array.isArray(value) && value.length === 0) continue;
            if (typeof value === "string" && value === "") continue;
            updateData[field] = value;
          }
          if (Object.keys(updateData).length > 0) {
            if (!DRY_RUN) {
              await existing.ref.update(updateData);
            }
            updatedCount++;
          }
        } else {
          // 通常モード: 空のフィールドは補完、想定外の値は上書き、配列フィールドはマージ
          const updateData: Record<string, any> = {};
          for (const [field, value] of Object.entries(data)) {
            if (value === null || value === undefined) continue;
            if (Array.isArray(value) && value.length === 0) continue;
            if (typeof value === "string" && value === "") continue;
            const curValue = existing.data?.[field];
            
            // 想定外の値が入っている場合は上書き
            if (isUnexpectedValue(field, curValue)) {
              updateData[field] = value;
            }
            // 配列フィールドの場合はマージ
            else if (ARRAY_FIELDS.has(field) && Array.isArray(value)) {
              const existingArr = Array.isArray(curValue) ? curValue : [];
              const newItems = value.filter((v: any) => !existingArr.includes(v));
              if (newItems.length > 0) {
                updateData[field] = [...existingArr, ...newItems];
              }
            }
            // 空のフィールドは補完
            else if (curValue === undefined || curValue === null || curValue === "" || 
                (Array.isArray(curValue) && curValue.length === 0)) {
              updateData[field] = value;
            }
          }
          if (Object.keys(updateData).length > 0) {
            if (!DRY_RUN) {
              await existing.ref.update(updateData);
            }
            updatedCount++;
          }
        }
      } else {
        // 新規作成
        const docId = generateNumericDocId(corporateNumber, globalIndex);
        if (!DRY_RUN) {
          await companiesCol.doc(docId).set(data);
        }
        createdCount++;
      }

      if ((createdCount + updatedCount) % 500 === 0 && (createdCount + updatedCount) > 0) {
        console.log(`  ✅ 処理済み: ${createdCount + updatedCount} 件`);
      }
    }
  }

  console.log(`\n✅ タイプA/B 完了`);
  console.log(`  📊 総行数: ${totalRows}`);
  console.log(`  🆕 新規作成: ${createdCount}`);
  console.log(`  🔄 更新: ${updatedCount}`);
  if (unmappedHeaders.size > 0) {
    console.log(`  ⚠️  未マッピングヘッダー: ${[...unmappedHeaders].join(", ")}`);
  }
}

async function processTypeC(files: string[]) {
  console.log(`\n📥 タイプC の処理開始 (${files.length} ファイル)`);
  console.log(`  ℹ️  位置ベースでマッピング（重複ヘッダー対応）`);

  let totalRows = 0;
  let createdCount = 0;
  let updatedCount = 0;
  let globalIndex = 0;

  for (const file of files) {
    if (!fs.existsSync(file)) {
      console.warn(`⚠️  ファイルなし: ${file}`);
      continue;
    }

    const content = fs.readFileSync(file, "utf8");
    const lines = content.split(/\r?\n/).filter((l) => l.trim() !== "");
    if (lines.length <= 1) continue;

    console.log(`  📄 ${path.basename(file)}: ${lines.length - 1} 行`);
    totalRows += lines.length - 1;

    for (let i = 1; i < lines.length; i++) {
      globalIndex++;
      const values = parseCSVLineByPosition(lines[i]);
      const data: Record<string, any> = { ...COMPANY_TEMPLATE };

      for (let col = 0; col < values.length; col++) {
        const trimmedValue = trim(values[col]);
        if (!trimmedValue) continue;

        const field = MAPPING_C_BY_INDEX[col];
        if (!field || field === "") continue;

        // 値の型チェック
        if (!isValueValidForField(trimmedValue, field)) {
          if (field === "name") {
            console.warn(`  ⚠️  [${path.basename(file)} row ${i}] nameフィールドに不適切な値: "${trimmedValue}" (スキップ)`);
          }
          continue;
        }

        if (NUMERIC_FIELDS.has(field)) {
          const num = parseNumeric(trimmedValue);
          if (num !== null) data[field] = num;
        } else if (ARRAY_FIELDS.has(field)) {
          if (!data[field] || !Array.isArray(data[field])) {
            data[field] = [];
          }
          data[field].push(trimmedValue);
        } else {
          data[field] = trimmedValue;
        }
      }

      const corporateNumber = data.corporateNumber as string | null;
      const name = data.name as string | null;

      if (!name) continue;

      const existing = await findExistingDoc(corporateNumber, name);

      if (existing) {
        if (FORCE_OVERWRITE) {
          // 強制上書きモード
          const updateData: Record<string, any> = {};
          for (const [field, value] of Object.entries(data)) {
            if (value === null || value === undefined) continue;
            if (Array.isArray(value) && value.length === 0) continue;
            if (typeof value === "string" && value === "") continue;
            updateData[field] = value;
          }
          if (Object.keys(updateData).length > 0) {
            if (!DRY_RUN) {
              await existing.ref.update(updateData);
            }
            updatedCount++;
          }
        } else {
          // 通常モード: 空のフィールドは補完、想定外の値は上書き、配列フィールドはマージ
          const updateData: Record<string, any> = {};
          for (const [field, value] of Object.entries(data)) {
            if (value === null || value === undefined) continue;
            if (Array.isArray(value) && value.length === 0) continue;
            if (typeof value === "string" && value === "") continue;
            const curValue = existing.data?.[field];
            
            // 想定外の値が入っている場合は上書き
            if (isUnexpectedValue(field, curValue)) {
              updateData[field] = value;
            }
            // 配列フィールドの場合はマージ
            else if (ARRAY_FIELDS.has(field) && Array.isArray(value)) {
              const existingArr = Array.isArray(curValue) ? curValue : [];
              const newItems = value.filter((v: any) => !existingArr.includes(v));
              if (newItems.length > 0) {
                updateData[field] = [...existingArr, ...newItems];
              }
            }
            // 空のフィールドは補完
            else if (curValue === undefined || curValue === null || curValue === "" ||
                (Array.isArray(curValue) && curValue.length === 0)) {
              updateData[field] = value;
            }
          }
          if (Object.keys(updateData).length > 0) {
            if (!DRY_RUN) {
              await existing.ref.update(updateData);
            }
            updatedCount++;
          }
        }
      } else {
        const docId = generateNumericDocId(corporateNumber, globalIndex);
        if (!DRY_RUN) {
          await companiesCol.doc(docId).set(data);
        }
        createdCount++;
      }

      if ((createdCount + updatedCount) % 500 === 0 && (createdCount + updatedCount) > 0) {
        console.log(`  ✅ 処理済み: ${createdCount + updatedCount} 件`);
      }
    }
  }

  console.log(`\n✅ タイプC 完了`);
  console.log(`  📊 総行数: ${totalRows}`);
  console.log(`  🆕 新規作成: ${createdCount}`);
  console.log(`  🔄 更新: ${updatedCount}`);
}

async function processTypeD(files: string[]) {
  console.log(`\n📥 タイプD の処理開始 (${files.length} ファイル)`);
  console.log(`  ℹ️  ヘッダー読み取り + 値検証でマッピング`);

  let totalRows = 0;
  let createdCount = 0;
  let updatedCount = 0;
  let globalIndex = 0;

  // 拡張マッピング（各CSVのヘッダーに対応）
  const extendedMapping: Record<string, string> = {
    ...MAPPING_D,
    "会社ID": "metaDescription",
    "リストID": "metaDescription",
    "状態": "tags",
  };

  for (const file of files) {
    if (!fs.existsSync(file)) {
      console.warn(`⚠️  ファイルなし: ${file}`);
      continue;
    }

    const content = fs.readFileSync(file, "utf8");
    const lines = content.split(/\r?\n/).filter((l) => l.trim() !== "");
    if (lines.length <= 1) continue;

    // ヘッダーを読み取る
    const headerValues = parseCSVLineByPosition(lines[0]);
    const headers = headerValues.map(h => h.trim().replace(/^"|"$/g, ""));
    const headerCount = headers.length;

    console.log(`  📄 ${path.basename(file)}: ${lines.length - 1} 行, ヘッダー ${headerCount} 列`);
    totalRows += lines.length - 1;

    for (let i = 1; i < lines.length; i++) {
      globalIndex++;
      const values = parseCSVLineByPosition(lines[i]);
      const data: Record<string, any> = { ...COMPANY_TEMPLATE };

      // ヘッダー数とデータ列数の比較
      const dataColCount = values.length;
      const hasExtraColumn = dataColCount > headerCount;

      for (let col = 0; col < values.length; col++) {
        const trimmedValue = trim(values[col]);
        if (!trimmedValue) continue;

        let field: string | undefined;

        // ヘッダーから列数が多い場合（119.csvのように業種-細が追加されている）
        if (hasExtraColumn && col >= 15) {
          // 業種-細の追加列を考慮
          if (col === 15) {
            // 値が業種っぽいか郵便番号っぽいかで判定
            if (/^\d{3}-?\d{4}$/.test(trimmedValue)) {
              field = "postalCode";
            } else {
              field = "industryDetail";
            }
          } else {
            // 16列目以降は1列ずれ
            const adjustedCol = col - 1;
            if (adjustedCol < headers.length) {
              const header = headers[adjustedCol];
              field = extendedMapping[header];
            } else {
              field = MAPPING_D_BY_INDEX[col];
            }
          }
        } else {
          // ヘッダーベースのマッピング
          if (col < headers.length) {
            const header = headers[col];
            field = extendedMapping[header];
          }
        }

        if (!field || field === "") continue;

        // 値の内容から適切なフィールドを再検証
        const correctedField = validateAndCorrectField(field, trimmedValue);
        if (!correctedField) continue;
        field = correctedField;

        // 値の型チェック
        if (!isValueValidForField(trimmedValue, field)) {
          continue;
        }

        if (NUMERIC_FIELDS.has(field)) {
          const num = parseNumeric(trimmedValue);
          if (num !== null) data[field] = num;
        } else if (ARRAY_FIELDS.has(field)) {
          if (!data[field] || !Array.isArray(data[field])) {
            data[field] = [];
          }
          data[field].push(trimmedValue);
        } else if (field === "tags" || field === "metaDescription" || field === "metaKeywords") {
          // 複数行のテキストフィールドは追記
          if (data[field] && typeof data[field] === "string") {
            data[field] = data[field] + "\n" + trimmedValue;
          } else {
            data[field] = trimmedValue;
          }
        } else {
          data[field] = trimmedValue;
        }
      }

      const corporateNumber = data.corporateNumber as string | null;
      const name = data.name as string | null;

      if (!name && !corporateNumber) continue;

      const existing = await findExistingDoc(corporateNumber, name);

      if (existing) {
        if (FORCE_OVERWRITE) {
          // 強制上書きモード
          const updateData: Record<string, any> = {};
          for (const [field, value] of Object.entries(data)) {
            if (value === null || value === undefined) continue;
            if (Array.isArray(value) && value.length === 0) continue;
            if (typeof value === "string" && value === "") continue;
            updateData[field] = value;
          }
          if (Object.keys(updateData).length > 0) {
            if (!DRY_RUN) {
              await existing.ref.update(updateData);
            }
            updatedCount++;
          }
        } else {
          // 通常モード: 空のフィールドは補完、想定外の値は上書き、配列フィールドはマージ
          const updateData: Record<string, any> = {};
          for (const [field, value] of Object.entries(data)) {
            if (value === null || value === undefined) continue;
            if (Array.isArray(value) && value.length === 0) continue;
            if (typeof value === "string" && value === "") continue;
            const curValue = existing.data?.[field];
            
            // 想定外の値が入っている場合は上書き
            if (isUnexpectedValue(field, curValue)) {
              updateData[field] = value;
            }
            // 配列フィールドの場合はマージ
            else if (ARRAY_FIELDS.has(field) && Array.isArray(value)) {
              const existingArr = Array.isArray(curValue) ? curValue : [];
              const newItems = value.filter((v: any) => !existingArr.includes(v));
              if (newItems.length > 0) {
                updateData[field] = [...existingArr, ...newItems];
              }
            }
            // 空のフィールドは補完
            else if (curValue === undefined || curValue === null || curValue === "" ||
                (Array.isArray(curValue) && curValue.length === 0)) {
              updateData[field] = value;
            }
          }
          if (Object.keys(updateData).length > 0) {
            if (!DRY_RUN) {
              await existing.ref.update(updateData);
            }
            updatedCount++;
          }
        }
      } else {
        const docId = generateNumericDocId(corporateNumber, globalIndex);
        if (!DRY_RUN) {
          await companiesCol.doc(docId).set(data);
        }
        createdCount++;
      }

      if ((createdCount + updatedCount) % 500 === 0 && (createdCount + updatedCount) > 0) {
        console.log(`  ✅ 処理済み: ${createdCount + updatedCount} 件`);
      }
    }
  }

  console.log(`\n✅ タイプD 完了`);
  console.log(`  📊 総行数: ${totalRows}`);
  console.log(`  🆕 新規作成: ${createdCount}`);
  console.log(`  🔄 更新: ${updatedCount}`);
}

// 値の内容からフィールドを検証・修正
function validateAndCorrectField(field: string, value: string): string | undefined {
  const v = value.trim();

  // 郵便番号フィールドに住所が入っている場合
  if (field === "postalCode") {
    if (/^\d{3}-?\d{4}$/.test(v)) {
      return "postalCode";
    }
    // 住所パターンなら address に変更
    if (/^(北海道|東京都|.+[都道府県]).+/.test(v)) {
      return "address";
    }
    return undefined; // スキップ
  }

  // 住所フィールドに郵便番号が入っている場合
  if (field === "address") {
    if (/^\d{3}-?\d{4}$/.test(v)) {
      return "postalCode";
    }
    return "address";
  }

  // 設立フィールドに住所が入っている場合
  if (field === "established") {
    if (/^(北海道|東京都|.+[都道府県]).+/.test(v)) {
      return "address";
    }
    // 日付パターン
    if (/\d{4}[年/-]\d{1,2}[月/-]?\d{0,2}/.test(v) || /\d{1,2}月\d{1,2}日\d{4}年/.test(v)) {
      return "established";
    }
  }

  // 電話番号フィールドに日付が入っている場合
  if (field === "phoneNumber") {
    if (/\d{4}[年/-]\d{1,2}[月/-]?\d{0,2}/.test(v)) {
      return "established";
    }
    if (/^\d{2,4}-\d{2,4}-\d{4}$/.test(v)) {
      return "phoneNumber";
    }
  }

  return field;
}

async function processWithMapping(
  typeName: string,
  files: string[],
  mapping: Record<string, string>
) {
  console.log(`\n📥 ${typeName} の処理開始 (${files.length} ファイル)`);

  let totalRows = 0;
  let createdCount = 0;
  let updatedCount = 0;
  let globalIndex = 0;
  let unmappedHeaders = new Set<string>();

  for (const file of files) {
    if (!fs.existsSync(file)) {
      console.warn(`⚠️  ファイルなし: ${file}`);
      continue;
    }

    const buf = fs.readFileSync(file);
    let records: CsvRow[];
    try {
      records = parse(buf, { 
        columns: true, 
        skip_empty_lines: true, 
        relax_quotes: true,
        relax_column_count: true,
        bom: true, // BOM対応
      });
    } catch (e) {
      console.warn(`⚠️  パースエラー: ${file} - ${e}`);
      continue;
    }
    
    // ヘッダー数を確認
    if (records.length > 0) {
      const firstRow = records[0];
      const headerCount = Object.keys(firstRow).length;
      const expectedHeaders = Object.keys(mapping).filter(h => mapping[h] !== "");
      if (headerCount !== expectedHeaders.length) {
        console.warn(`  ⚠️  [${path.basename(file)}] ヘッダー数不一致: 期待=${expectedHeaders.length}, 実際=${headerCount}`);
      }
    }
    
    console.log(`  📄 ${path.basename(file)}: ${records.length} 行`);
    totalRows += records.length;

    for (const row of records) {
      globalIndex++;
      const data: Record<string, any> = { ...COMPANY_TEMPLATE };

      for (const [header, value] of Object.entries(row)) {
        const cleanHeader = header.trim().replace(/^"|"$/g, "");
        const trimmedValue = trim(value);
        if (!trimmedValue) continue;

        let field = mapping[cleanHeader];
        
        // マッピングにない場合、Unnamed列はmetaDescriptionに追加
        if (field === undefined) {
          if (cleanHeader.startsWith("Unnamed")) {
            // Unnamed列はmetaDescriptionに追加
            if (!data.metaDescription || typeof data.metaDescription !== "string") {
              data.metaDescription = "";
            }
            data.metaDescription = (data.metaDescription ? data.metaDescription + "\n" : "") + trimmedValue;
            continue;
          }
          unmappedHeaders.add(cleanHeader);
          continue;
        }
        if (field === "") continue;

        // 値の型チェック（nameに代表者名が入らないように）
        if (!isValueValidForField(trimmedValue, field)) {
          if (field === "name") {
            console.warn(`  ⚠️  [${path.basename(file)} row ${globalIndex}] nameフィールドに不適切な値: "${trimmedValue}" (スキップ)`);
          } else if (field === "companyUrl" || field === "contactFormUrl") {
            // URLフィールドにURL形式でない値が入っている場合は警告
            console.warn(`  ⚠️  [${path.basename(file)} row ${globalIndex}] ${field}フィールドにURL形式でない値: "${trimmedValue.substring(0, 50)}" (スキップ)`);
          } else if (field === "postalCode") {
            // 郵便番号フィールドに郵便番号形式でない値が入っている場合は警告
            if (!/^\d{3}-?\d{4}$/.test(trimmedValue)) {
              console.warn(`  ⚠️  [${path.basename(file)} row ${globalIndex}] postalCodeフィールドに郵便番号形式でない値: "${trimmedValue.substring(0, 50)}" (スキップ)`);
            }
          }
          continue;
        }

        if (NUMERIC_FIELDS.has(field)) {
          const num = parseNumeric(trimmedValue);
          if (num !== null) data[field] = num;
        } else if (ARRAY_FIELDS.has(field)) {
          if (!data[field] || !Array.isArray(data[field])) {
            data[field] = [];
          }
          data[field].push(trimmedValue);
        } else {
          // 既に値がある場合は追記
          if (data[field] && typeof data[field] === "string") {
            data[field] = data[field] + "\n" + trimmedValue;
          } else {
            data[field] = trimmedValue;
          }
        }
      }

      const corporateNumber = data.corporateNumber as string | null;
      const name = data.name as string | null;

      if (!name && !corporateNumber) continue;

      const existing = await findExistingDoc(corporateNumber, name);

      if (existing) {
        if (FORCE_OVERWRITE) {
          // 強制上書きモード
          const updateData: Record<string, any> = {};
          for (const [field, value] of Object.entries(data)) {
            if (value === null || value === undefined) continue;
            if (Array.isArray(value) && value.length === 0) continue;
            if (typeof value === "string" && value === "") continue;
            updateData[field] = value;
          }
          if (Object.keys(updateData).length > 0) {
            if (!DRY_RUN) {
              await existing.ref.update(updateData);
            }
            updatedCount++;
          }
        } else {
          // 通常モード: 空のフィールドは補完、想定外の値は上書き、配列フィールドはマージ
          const updateData: Record<string, any> = {};
          for (const [field, value] of Object.entries(data)) {
            if (value === null || value === undefined) continue;
            if (Array.isArray(value) && value.length === 0) continue;
            if (typeof value === "string" && value === "") continue;
            const curValue = existing.data?.[field];
            
            // 想定外の値が入っている場合は上書き
            if (isUnexpectedValue(field, curValue)) {
              updateData[field] = value;
            }
            // 配列フィールドの場合はマージ
            else if (ARRAY_FIELDS.has(field) && Array.isArray(value)) {
              const existingArr = Array.isArray(curValue) ? curValue : [];
              const newItems = value.filter((v: any) => !existingArr.includes(v));
              if (newItems.length > 0) {
                updateData[field] = [...existingArr, ...newItems];
              }
            }
            // 空のフィールドは補完
            else if (curValue === undefined || curValue === null || curValue === "" ||
                (Array.isArray(curValue) && curValue.length === 0)) {
              updateData[field] = value;
            }
          }
          if (Object.keys(updateData).length > 0) {
            if (!DRY_RUN) {
              await existing.ref.update(updateData);
            }
            updatedCount++;
          }
        }
      } else {
        const docId = generateNumericDocId(corporateNumber, globalIndex);
        if (!DRY_RUN) {
          await companiesCol.doc(docId).set(data);
        }
        createdCount++;
      }

      if ((createdCount + updatedCount) % 500 === 0 && (createdCount + updatedCount) > 0) {
        console.log(`  ✅ 処理済み: ${createdCount + updatedCount} 件`);
      }
    }
  }

  console.log(`\n✅ ${typeName} 完了`);
  console.log(`  📊 総行数: ${totalRows}`);
  console.log(`  🆕 新規作成: ${createdCount}`);
  console.log(`  🔄 更新: ${updatedCount}`);
  if (unmappedHeaders.size > 0) {
    console.log(`  ⚠️  未マッピングヘッダー: ${[...unmappedHeaders].join(", ")}`);
  }
}

async function processTypeF51(files: string[]) {
  console.log(`\n📥 タイプF51 の処理開始 (${files.length} ファイル)`);
  console.log(`  ℹ️  位置ベースでマッピング（重複ヘッダー対応）`);

  let totalRows = 0;
  let createdCount = 0;
  let updatedCount = 0;
  let globalIndex = 0;

  for (const file of files) {
    if (!fs.existsSync(file)) {
      console.warn(`⚠️  ファイルなし: ${file}`);
      continue;
    }

    const content = fs.readFileSync(file, "utf8");
    const lines = content.split(/\r?\n/).filter((l) => l.trim() !== "");
    if (lines.length <= 1) continue;

    console.log(`  📄 ${path.basename(file)}: ${lines.length - 1} 行`);
    totalRows += lines.length - 1;

    for (let i = 1; i < lines.length; i++) {
      globalIndex++;
      const values = parseCSVLineByPosition(lines[i]);
      const data: Record<string, any> = { ...COMPANY_TEMPLATE };

      for (let col = 0; col < values.length; col++) {
        const trimmedValue = trim(values[col]);
        if (!trimmedValue) continue;

        const field = MAPPING_F51_BY_INDEX[col];
        if (!field || field === "") continue;

        // 値の型チェック
        if (!isValueValidForField(trimmedValue, field)) {
          if (field === "name") {
            console.warn(`  ⚠️  [${path.basename(file)} row ${i}] nameフィールドに不適切な値: "${trimmedValue}" (スキップ)`);
          }
          continue;
        }

        if (NUMERIC_FIELDS.has(field)) {
          const num = parseNumeric(trimmedValue);
          if (num !== null) data[field] = num;
        } else if (ARRAY_FIELDS.has(field)) {
          if (!data[field] || !Array.isArray(data[field])) {
            data[field] = [];
          }
          data[field].push(trimmedValue);
        } else if (field === "tags" || field === "metaDescription" || field === "metaKeywords") {
          // 複数行のテキストフィールドは追記
          if (data[field] && typeof data[field] === "string") {
            data[field] = data[field] + "\n" + trimmedValue;
          } else {
            data[field] = trimmedValue;
          }
        } else {
          data[field] = trimmedValue;
        }
      }

      const corporateNumber = data.corporateNumber as string | null;
      const name = data.name as string | null;

      if (!name && !corporateNumber) continue;

      const existing = await findExistingDoc(corporateNumber, name);

      if (existing) {
        if (FORCE_OVERWRITE) {
          // 強制上書きモード
          const updateData: Record<string, any> = {};
          for (const [field, value] of Object.entries(data)) {
            if (value === null || value === undefined) continue;
            if (Array.isArray(value) && value.length === 0) continue;
            if (typeof value === "string" && value === "") continue;
            updateData[field] = value;
          }
          if (Object.keys(updateData).length > 0) {
            if (!DRY_RUN) {
              await existing.ref.update(updateData);
            }
            updatedCount++;
          }
        } else {
          // 通常モード: 空のフィールドは補完、想定外の値は上書き、配列フィールドはマージ
          const updateData: Record<string, any> = {};
          for (const [field, value] of Object.entries(data)) {
            if (value === null || value === undefined) continue;
            if (Array.isArray(value) && value.length === 0) continue;
            if (typeof value === "string" && value === "") continue;
            const curValue = existing.data?.[field];
            
            // 想定外の値が入っている場合は上書き
            if (isUnexpectedValue(field, curValue)) {
              updateData[field] = value;
            }
            // 配列フィールドの場合はマージ
            else if (ARRAY_FIELDS.has(field) && Array.isArray(value)) {
              const existingArr = Array.isArray(curValue) ? curValue : [];
              const newItems = value.filter((v: any) => !existingArr.includes(v));
              if (newItems.length > 0) {
                updateData[field] = [...existingArr, ...newItems];
              }
            }
            // 空のフィールドは補完
            else if (curValue === undefined || curValue === null || curValue === "" ||
                (Array.isArray(curValue) && curValue.length === 0)) {
              updateData[field] = value;
            }
          }
          if (Object.keys(updateData).length > 0) {
            if (!DRY_RUN) {
              await existing.ref.update(updateData);
            }
            updatedCount++;
          }
        }
      } else {
        const docId = generateNumericDocId(corporateNumber, globalIndex);
        if (!DRY_RUN) {
          await companiesCol.doc(docId).set(data);
        }
        createdCount++;
      }

      if ((createdCount + updatedCount) % 500 === 0 && (createdCount + updatedCount) > 0) {
        console.log(`  ✅ 処理済み: ${createdCount + updatedCount} 件`);
      }
    }
  }

  console.log(`\n✅ タイプF51 完了`);
  console.log(`  📊 総行数: ${totalRows}`);
  console.log(`  🆕 新規作成: ${createdCount}`);
  console.log(`  🔄 更新: ${updatedCount}`);
}

// ==============================
// メイン処理
// ==============================

async function main() {
  const targetType = getTargetType();
  console.log(DRY_RUN ? "🔍 DRY_RUN モード" : "⚠️  本番モード");
  if (FORCE_OVERWRITE) {
    console.log("🔄 強制上書きモード（全フィールドを上書き）");
  }
  console.log("");
  console.log(`📌 対象タイプ: ${targetType}`);

  const runAll = targetType === "ALL";

  if (runAll || targetType === "B") {
    await processTypeAB(TYPE_FILES.B, MAPPING_AB);
  }
  if (runAll || targetType === "A") {
    await processTypeAB(TYPE_FILES.A, MAPPING_AB);
  }
  if (runAll || targetType === "C") {
    await processTypeC(TYPE_FILES.C);
  }
  if (runAll || targetType === "D") {
    await processTypeD(TYPE_FILES.D);
  }
  if (runAll || targetType === "E") {
    await processWithMapping("タイプE", TYPE_FILES.E, MAPPING_E);
  }
  if (runAll || targetType === "F51") {
    await processTypeF51(TYPE_FILES.F51);
  }
  if (runAll || targetType === "F130") {
    await processWithMapping("タイプF130", TYPE_FILES.F130, MAPPING_F130);
  }
  if (runAll || targetType === "F132") {
    await processWithMapping("タイプF132", TYPE_FILES.F132, MAPPING_F132);
  }

  console.log("\n========================================");
  console.log("✅ 全処理完了");
  if (DRY_RUN) {
    console.log("💡 --dry-run を外すと実際にDBに書き込みます");
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

