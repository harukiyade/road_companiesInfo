/* 
  インテリジェントな値分析によるCSV列ズレ修正スクリプト
  
  各行の各セルを分析して、値のパターンから適切なフィールドを自動判定します。
  正常なドキュメントのパターンを学習して、より正確なマッピングを実現します。
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/fix_csv_with_intelligent_mapping.ts
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
const BATCH_LIMIT = 500;

// 処理対象のファイル
const TARGET_FILES = [
  "csv/38.csv",
  "csv/107.csv",
  "csv/108.csv",
  "csv/109.csv",
  "csv/110.csv",
  "csv/111.csv",
  "csv/112.csv",
  "csv/113.csv",
  "csv/114.csv",
  "csv/115.csv",
  "csv/116.csv",
  "csv/117.csv",
  "csv/118.csv",
  "csv/119.csv",
  "csv/120.csv",
  "csv/121.csv",
  "csv/122.csv",
  "csv/123.csv",
  "csv/124.csv",
  "csv/125.csv",
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
  "Unnamed: 38",
  "Unnamed: 39",
  "Unnamed: 40",
  "Unnamed: 41",
  "Unnamed: 42",
  "Unnamed: 43",
  "Unnamed: 44",
  "Unnamed: 45",
  "Unnamed: 46",
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

// ==============================
// 値のパターン分析とフィールド判定
// ==============================

function trim(value: string | null | undefined): string {
  if (!value) return "";
  return String(value).trim();
}

// 都道府県のリスト
const PREFECTURES = [
  "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
  "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
  "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
  "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
  "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
  "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
  "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
];

// 正規表現パターン
const PATTERNS = {
  corporateNumber: /^\d{13}$/,
  postalCode: /^\d{3}-?\d{4}$/,
  phoneNumber: /^[\d\-\(\)]+$/,
  url: /^https?:\/\//i,
  date: /^\d{4}[年\/\-]\d{1,2}[月\/\-]\d{1,2}[日]?/,
  year: /^\d{4}年/,
  number: /^[\d,]+$/,
  japaneseName: /^[ぁ-んァ-ヶー一-龠々]+$/,
  address: /[都道府県市区町村]/,
  industry: /[業|店|所|場|館|社|会|組|団|協|連|合|体|機構|組合|組合|組合]/,
};

// 値の型を判定
type ValueType = 
  | "corporateNumber"
  | "postalCode"
  | "phoneNumber"
  | "url"
  | "date"
  | "year"
  | "number"
  | "prefecture"
  | "address"
  | "name"
  | "representativeName"
  | "industry"
  | "text"
  | "unknown";

function detectValueType(value: string): ValueType {
  const trimmed = trim(value);
  if (!trimmed) return "unknown";

  // 法人番号（13桁の数字）- 最優先
  const corporateDigits = trimmed.replace(/\D/g, "");
  if (corporateDigits.length === 13 && /^\d{13}$/.test(corporateDigits)) {
    // 指数表記もチェック
    if (/^\d+\.\d+E\+\d+$/i.test(trimmed)) {
      try {
        const num = parseFloat(trimmed);
        const digits = Math.floor(num).toString();
        if (digits.length === 13) return "corporateNumber";
      } catch {}
    }
    return "corporateNumber";
  }

  // 郵便番号（3桁-4桁）- 郵便番号形式でない場合は除外
  const postalDigits = trimmed.replace(/\D/g, "");
  if (postalDigits.length === 7) {
    // 郵便番号として有効かチェック（先頭が0-9で、7桁）
    if (/^\d{7}$/.test(postalDigits)) {
      return "postalCode";
    }
  }

  // URL - 最優先
  if (PATTERNS.url.test(trimmed)) return "url";

  // 電話番号（数字とハイフン、括弧のみ、10-15文字）
  const phonePattern = /^[\d\-\(\)]+$/;
  if (phonePattern.test(trimmed) && trimmed.length >= 10 && trimmed.length <= 15) {
    // 郵便番号でないことを確認
    if (postalDigits.length !== 7) {
      return "phoneNumber";
    }
  }

  // 日付形式
  if (PATTERNS.date.test(trimmed)) return "date";

  // 年形式
  if (PATTERNS.year.test(trimmed)) return "year";

  // 数値（郵便番号や電話番号でないことを確認）
  if (postalDigits.length !== 7 && !phonePattern.test(trimmed)) {
    const numStr = trimmed.replace(/[,\s]/g, "");
    if (/^[\d,]+$/.test(numStr)) {
      const num = Number(numStr);
      if (Number.isFinite(num) && num > 0 && num < 1e15) {
        return "number";
      }
    }
  }

  // 都道府県（完全一致または先頭一致）
  for (const pref of PREFECTURES) {
    if (trimmed === pref || (trimmed.startsWith(pref) && trimmed.length <= pref.length + 2)) {
      return "prefecture";
    }
  }

  // 住所（都道府県を含む、長い文字列）
  if (PATTERNS.address.test(trimmed) && trimmed.length > 8) {
    // 郵便番号や電話番号でないことを確認
    if (postalDigits.length !== 7 && !phonePattern.test(trimmed)) {
      return "address";
    }
  }

  // 会社名（株式会社などを含む）
  if (trimmed.includes("株式会社") || trimmed.includes("（株）") || 
      trimmed.includes("有限会社") || trimmed.includes("合同会社") ||
      trimmed.includes("合資会社") || trimmed.includes("合名会社") ||
      trimmed.includes("一般社団法人") || trimmed.includes("一般財団法人")) {
    return "name";
  }

  // 代表者名（日本語の名前らしい文字列、短い）
  if (PATTERNS.japaneseName.test(trimmed) && trimmed.length >= 2 && trimmed.length <= 15) {
    // 業種っぽい文字列は除外
    if (!PATTERNS.industry.test(trimmed) && 
        !trimmed.includes("株式会社") && 
        !trimmed.includes("有限会社")) {
      // 数値のみの文字列は除外
      if (!/^\d+$/.test(trimmed)) {
        return "representativeName";
      }
    }
  }

  // 業種（業種らしいキーワードを含む）
  if (PATTERNS.industry.test(trimmed) || 
      trimmed.includes("業") || 
      trimmed.includes("店") || 
      trimmed.includes("所") ||
      trimmed.includes("場") ||
      trimmed.includes("館")) {
    // 会社名でないことを確認
    if (!trimmed.includes("株式会社") && !trimmed.includes("有限会社")) {
      return "industry";
    }
  }

  return "text";
}

// 正常なドキュメントからパターンを学習
async function learnFieldPatterns(): Promise<Map<string, Set<ValueType>>> {
  console.log("📚 正常なドキュメントからパターンを学習中...");
  
  const patterns = new Map<string, Set<ValueType>>();
  
  try {
    // Firestoreの制限: 複数の!=フィルターは使えないため、1つだけ使用
    const snapshot = await companiesCol
      .where("corporateNumber", "!=", null)
      .limit(100)
      .get();

    console.log(`  📊 ${snapshot.size}件のドキュメントを分析`);

    snapshot.forEach((doc) => {
      const data = doc.data();
      
      // 各フィールドの値の型を記録
      const fieldTypes: Record<string, ValueType> = {
        name: detectValueType(data.name || ""),
        corporateNumber: detectValueType(data.corporateNumber || ""),
        postalCode: detectValueType(data.postalCode || ""),
        address: detectValueType(data.address || ""),
        phoneNumber: detectValueType(data.phoneNumber || ""),
        companyUrl: detectValueType(data.companyUrl || ""),
        representativeName: detectValueType(data.representativeName || ""),
        established: detectValueType(data.established || ""),
        prefecture: detectValueType(data.prefecture || ""),
      };

      for (const [field, type] of Object.entries(fieldTypes)) {
        if (type !== "unknown") {
          if (!patterns.has(field)) {
            patterns.set(field, new Set());
          }
          patterns.get(field)!.add(type);
        }
      }
    });

    console.log("  ✅ パターン学習完了");
  } catch (err: any) {
    console.log(`  ⚠️  パターン学習エラー: ${err.message}`);
  }

  return patterns;
}

// 値がフィールドに適合するか判定
function isValueSuitableForField(value: string, field: string, learnedPatterns: Map<string, Set<ValueType>>): boolean {
  const valueType = detectValueType(value);
  if (valueType === "unknown") return false;

  const expectedTypes = learnedPatterns.get(field);
  if (expectedTypes && expectedTypes.has(valueType)) {
    return true;
  }

  // フォールバック: 一般的なルール
  switch (field) {
    case "name":
      return valueType === "name";
    case "corporateNumber":
      return valueType === "corporateNumber";
    case "postalCode":
      return valueType === "postalCode";
    case "address":
      return valueType === "address";
    case "phoneNumber":
      return valueType === "phoneNumber";
    case "companyUrl":
      return valueType === "url";
    case "representativeName":
      return valueType === "representativeName";
    case "prefecture":
      return valueType === "prefecture";
    case "established":
      return valueType === "date" || valueType === "year";
    default:
      return true; // その他のフィールドは基本的に受け入れる
  }
}

// 行データをインテリジェントにマッピング
function mapRowDataIntelligently(
  row: string[],
  headers: string[],
  fileName: string,
  learnedPatterns: Map<string, Set<ValueType>>
): Record<string, any> {
  const result: Record<string, any> = {
    industries: [],
  };

  // ヘッダーのインデックスマップ（無視フィールドを除外）
  const headerIndexMap = new Map<string, number>();
  headers.forEach((header, index) => {
    if (!IGNORE_FIELDS.has(header)) {
      headerIndexMap.set(header, index);
    }
  });

  // 郵便番号の位置を特定（値の型から判定）
  let postalCodeIndex = -1;
  for (let i = 0; i < row.length; i++) {
    const value = trim(row[i]);
    if (!value) continue;
    
    const valueType = detectValueType(value);
    if (valueType === "postalCode") {
      postalCodeIndex = i;
      break;
    }
  }

  // ヘッダーからも郵便番号の位置を確認
  if (postalCodeIndex === -1) {
    const headerPostalIndex = headerIndexMap.get("郵便番号");
    if (headerPostalIndex !== undefined) {
      postalCodeIndex = headerPostalIndex;
    }
  }

  // 各セルを分析してフィールド候補を収集
  const cellCandidates = new Map<number, Array<{ field: string; score: number }>>();
  
  for (let i = 0; i < row.length; i++) {
    const value = trim(row[i]);
    if (!value) continue;

    const candidates: Array<{ field: string; score: number }> = [];
    const valueType = detectValueType(value);

    // 郵便番号の位置より前で、郵便番号形式でない値は業種の可能性が高い
    if (postalCodeIndex >= 0 && i < postalCodeIndex) {
      if (valueType !== "postalCode" && valueType !== "corporateNumber" && 
          valueType !== "url" && valueType !== "phoneNumber" &&
          valueType !== "prefecture" && valueType !== "name" &&
          valueType !== "representativeName" && valueType !== "date") {
        // 業種として扱う
        candidates.push({ field: "industries", score: 80 });
      }
    }

    // ヘッダー名から推測（高い優先度）
    if (i < headers.length) {
      const header = headers[i];
      if (!IGNORE_FIELDS.has(header)) {
        const field = mapHeaderToField(header);
        if (field) {
          // 値がフィールドに適合するかチェック
          if (isValueSuitableForField(value, field, learnedPatterns)) {
            candidates.push({ field, score: 100 });
          } else {
            // 適合しない場合でも、ヘッダー名を優先（ただし低いスコア）
            candidates.push({ field, score: 30 });
          }
        }
      }
    }

    // 値の型から推測
    const typeBasedFields = getFieldsByValueType(valueType);
    for (const field of typeBasedFields) {
      if (isValueSuitableForField(value, field, learnedPatterns)) {
        const existing = candidates.find(c => c.field === field);
        if (!existing) {
          // 業種フィールドの場合は少し低いスコア
          const score = field === "industries" ? 40 : 50;
          candidates.push({ field, score });
        }
      }
    }

    if (candidates.length > 0) {
      cellCandidates.set(i, candidates);
    }
  }

  // 各フィールドに最適なセルを割り当て
  const fieldAssignments = new Map<string, { index: number; value: string; score: number }>();
  const usedIndices = new Set<number>();

  // 優先度の高いフィールドから割り当て
  const priorityFields = [
    "name", "corporateNumber", "prefecture", "postalCode", "address",
    "phoneNumber", "companyUrl", "representativeName", "established"
  ];

  for (const field of priorityFields) {
    let bestCandidate: { index: number; value: string; score: number } | null = null;

    for (const [index, candidates] of cellCandidates.entries()) {
      if (usedIndices.has(index)) continue;

      const candidate = candidates.find(c => c.field === field);
      if (candidate && (!bestCandidate || candidate.score > bestCandidate.score)) {
        bestCandidate = { index, value: trim(row[index]), score: candidate.score };
      }
    }

    if (bestCandidate) {
      fieldAssignments.set(field, bestCandidate);
      usedIndices.add(bestCandidate.index);
    }
  }

  // その他のフィールドを割り当て
  for (const [index, candidates] of cellCandidates.entries()) {
    if (usedIndices.has(index)) continue;

    // 最もスコアの高い候補を選択
    const bestCandidate = candidates.reduce((best, current) => 
      current.score > best.score ? current : best
    );

    if (bestCandidate && !fieldAssignments.has(bestCandidate.field)) {
      fieldAssignments.set(bestCandidate.field, {
        index,
        value: trim(row[index]),
        score: bestCandidate.score
      });
      usedIndices.add(index);
    }
  }

  // 結果を構築
  for (const [field, assignment] of fieldAssignments.entries()) {
    const value = assignment.value;
    
    // フィールドごとの処理
    if (field === "corporateNumber") {
      const normalized = normalizeCorporateNumber(value);
      if (normalized) result[field] = normalized;
    } else if (field === "postalCode") {
      const normalized = normalizePostalCode(value);
      if (normalized) result[field] = normalized;
    } else if (field === "industries" || field === "industry") {
      if (!result.industries) result.industries = [];
      result.industries.push(value);
    } else if (["capitalStock", "revenue", "latestProfit"].includes(field)) {
      const num = parseFinancialNumeric(value, field);
      if (num !== null) result[field] = num;
    } else if (["employeeCount", "officeCount", "factoryCount", "storeCount"].includes(field)) {
      const num = parseNumeric(value);
      if (num !== null) result[field] = num;
    } else {
      result[field] = value;
    }
  }

  // 郵便番号の位置より前の列で、まだマッピングされていない業種を検出
  if (postalCodeIndex >= 0) {
    const industryStartIndex = headerIndexMap.get("業種1") ?? -1;
    if (industryStartIndex >= 0) {
      for (let i = industryStartIndex; i < postalCodeIndex; i++) {
        const value = trim(row[i]);
        if (!value) continue;

        // 既にマッピングされている列はスキップ
        if (usedIndices.has(i)) continue;

        // ヘッダーが無視フィールドの場合はスキップ
        if (i < headers.length && IGNORE_FIELDS.has(headers[i])) continue;

        const valueType = detectValueType(value);
        
        // 郵便番号形式でない場合、業種として扱う
        if (valueType !== "postalCode" && valueType !== "corporateNumber" &&
            valueType !== "url" && valueType !== "phoneNumber" &&
            valueType !== "prefecture" && valueType !== "name" &&
            valueType !== "representativeName" && valueType !== "date" &&
            valueType !== "number") {
          if (!result.industries) result.industries = [];
          result.industries.push(value);
        }
      }
    }
  }

  // 業種の処理
  const industryFields = ["industry", "industryLarge", "industryMiddle", "industrySmall", "industryDetail"];
  for (const field of industryFields) {
    if (result[field] && !result.industries) {
      result.industries = [];
    }
    if (result[field]) {
      result.industries.push(result[field]);
      delete result[field];
    }
  }

  // industries配列をクリーンアップ（重複除去）
  if (result.industries) {
    const seen = new Set<string>();
    result.industries = result.industries
      .filter((v: any) => {
        const trimmed = trim(v);
        if (!trimmed) return false;
        if (seen.has(trimmed)) return false;
        seen.add(trimmed);
        return true;
      });
    
    if (result.industries.length > 0) {
      result.industry = result.industries[0];
    }
  }

  return result;
}

// ヘッダー名をフィールド名にマッピング
function mapHeaderToField(header: string): string | null {
  const mapping: Record<string, string> = {
    "会社名": "name",
    "都道府県": "prefecture",
    "代表者名": "representativeName",
    "法人番号": "corporateNumber",
    "URL": "companyUrl",
    "業種1": "industry",
    "業種2": "industries",
    "業種3": "industries",
    "業種4": "industries",
    "業種（細）": "industryDetail",
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

// 値の型からフィールド候補を取得
function getFieldsByValueType(valueType: ValueType): string[] {
  const mapping: Record<ValueType, string[]> = {
    corporateNumber: ["corporateNumber"],
    postalCode: ["postalCode", "representativeRegisteredAddress"],
    phoneNumber: ["phoneNumber"],
    url: ["companyUrl"],
    date: ["established", "representativeBirthDate", "fiscalMonth"],
    year: ["established", "foundingYear"],
    number: ["capitalStock", "revenue", "latestProfit", "employeeCount", "officeCount", "factoryCount", "storeCount"],
    prefecture: ["prefecture"],
    address: ["address", "representativeHomeAddress"],
    name: ["name"],
    representativeName: ["representativeName"],
    industry: ["industry", "industries", "industryLarge", "industryMiddle", "industrySmall", "industryDetail"],
    text: ["companyDescription", "overview", "executives", "shareholders", "suppliers", "clients", "banks"],
    unknown: [],
  };

  return mapping[valueType] || [];
}

// 法人番号の正規化
function normalizeCorporateNumber(value: string): string | null {
  const trimmed = trim(value);
  if (!trimmed) return null;
  
  // 指数表記を処理
  if (/^\d+\.\d+E\+\d+$/i.test(trimmed)) {
    try {
      const num = parseFloat(trimmed);
      const digits = Math.floor(num).toString().replace(/\D/g, "");
      if (digits.length === 13) return digits;
    } catch {
      return null;
    }
  }
  
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 13) return digits;
  
  return null;
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

// 数値パース
function parseNumeric(value: string): number | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/[,\s]/g, "");
  if (!cleaned) return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

// 財務数値のパース
function parseFinancialNumeric(value: string, field: string): number | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/[,\s]/g, "");
  if (!cleaned || cleaned === "0" || cleaned === "非上場") return null;
  
  const num = Number(cleaned);
  if (!Number.isFinite(num) || num === 0) return null;
  
  const financialFields = ["capitalStock", "revenue", "latestProfit", "profit"];
  if (financialFields.includes(field)) {
    return Math.round(num * 1000);
  }
  
  return num;
}

// 企業を検索
async function findCompany(data: Record<string, any>): Promise<DocumentReference | null> {
  if (data.corporateNumber) {
    try {
      const snapshot = await companiesCol
        .where("corporateNumber", "==", data.corporateNumber)
        .limit(1)
        .get();
      if (!snapshot.empty) return snapshot.docs[0].ref;
    } catch (err) {}
  }

  if (data.name && data.prefecture && data.representativeName) {
    try {
      const snapshot = await companiesCol
        .where("name", "==", data.name)
        .where("prefecture", "==", data.prefecture)
        .where("representativeName", "==", data.representativeName)
        .limit(1)
        .get();
      if (!snapshot.empty) return snapshot.docs[0].ref;
    } catch (err) {}
  }

  if (data.name) {
    try {
      const snapshot = await companiesCol
        .where("name", "==", data.name)
        .limit(1)
        .get();
      if (!snapshot.empty) return snapshot.docs[0].ref;
    } catch (err) {}
  }

  return null;
}

// メイン処理
async function main() {
  console.log("🚀 インテリジェントなCSV列ズレ修正を開始します\n");

  // パターン学習
  const learnedPatterns = await learnFieldPatterns();

  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalCreated = 0;
  let totalErrors = 0;

  for (const filePath of TARGET_FILES) {
    const resolvedPath = path.resolve(filePath);
    
    if (!fs.existsSync(resolvedPath)) {
      console.log(`⚠️  ファイルが見つかりません: ${filePath}`);
      continue;
    }

    console.log(`\n📄 処理中: ${path.basename(filePath)}`);

    try {
      const content = fs.readFileSync(resolvedPath, "utf8");
      const records: string[][] = parse(content, {
        columns: false,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_records_with_error: true,
      });

      if (records.length === 0) {
        console.log("  ⚠️  データがありません");
        continue;
      }

      const headers = records[0];
      console.log(`  📊 ヘッダー数: ${headers.length}, データ行数: ${records.length - 1}`);

      let batch: WriteBatch = db.batch();
      let batchCount = 0;

      for (let i = 1; i < records.length; i++) {
        const row = records[i];
        
        try {
          const mappedData = mapRowDataIntelligently(row, headers, path.basename(filePath), learnedPatterns);
          
          if (!mappedData.name) {
            totalErrors++;
            continue;
          }

          totalProcessed++;

          const existingRef = await findCompany(mappedData);
          
          const updateData: Record<string, any> = {
            ...mappedData,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          };

          if (existingRef) {
            batch.update(existingRef, updateData);
            totalUpdated++;
          } else {
            const newRef = companiesCol.doc();
            batch.set(newRef, {
              ...updateData,
              createdAt: admin.firestore.FieldValue.serverTimestamp(),
            });
            totalCreated++;
          }

          batchCount++;

          if (batchCount >= BATCH_LIMIT) {
            try {
              await batch.commit();
              console.log(`  ✅ バッチコミット: ${batchCount}件`);
            } catch (err: any) {
              console.log(`  ❌ バッチコミットエラー: ${err.message}`);
            }
            batch = db.batch();
            batchCount = 0;
          }
        } catch (err: any) {
          console.log(`  ❌ 行${i + 1}の処理エラー: ${err.message}`);
          totalErrors++;
        }
      }

      if (batchCount > 0) {
        try {
          await batch.commit();
          console.log(`  ✅ 最終バッチコミット: ${batchCount}件`);
        } catch (err: any) {
          console.log(`  ❌ 最終バッチコミットエラー: ${err.message}`);
        }
      }

      console.log(`  ✅ 完了: ${path.basename(filePath)}`);
    } catch (err: any) {
      console.log(`  ❌ エラー: ${err.message}`);
      totalErrors++;
    }
  }

  console.log("\n" + "=".repeat(80));
  console.log("📊 処理結果サマリー");
  console.log("=".repeat(80));
  console.log(`総処理行数: ${totalProcessed}`);
  console.log(`更新件数: ${totalUpdated}`);
  console.log(`新規作成件数: ${totalCreated}`);
  console.log(`エラー件数: ${totalErrors}`);
  console.log("\n✅ すべての処理が完了しました");
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

