/* 
  Gemini APIを使ったCSV列ズレ修正スクリプト
  
  1. 正常なドキュメントから各フィールドのパターンを学習（Geminiで分析）
  2. CSVのヘッダーとサンプルデータをGeminiで解析
  3. 各行の各セルをGeminiで分析して、どのフィールドに適合するかを判断
  4. 適切なフィールドにマッピングしてDBに保存
  
  使い方:
    GEMINI_API_KEY=your_api_key \
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/fix_csv_with_gemini_analysis.ts
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
import { GoogleGenerativeAI } from "@google/generative-ai";

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
// Gemini API 初期化
// ==============================
let genAI: GoogleGenerativeAI | null = null;

function initGemini(): boolean {
  // Gemini APIは無効化されています
  // 再度有効化する場合は、この関数の内容を元に戻してください
  return false;
  
  // 以下はコメントアウト（将来の使用に備えて保持）
  /*
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey || apiKey.trim().length === 0) {
    console.warn("⚠️  GEMINI_API_KEYが設定されていません。Gemini機能は無効です。");
    return false;
  }

  try {
    genAI = new GoogleGenerativeAI(apiKey);
    console.log("✅ Gemini API 初期化完了");
    return true;
  } catch (err: any) {
    console.warn(`⚠️  Gemini API 初期化エラー: ${err.message}`);
    return false;
  }
  */
}

const GEMINI_ENABLED = initGemini();
// デフォルトモデル名: gemini-flash-latest を使用（ログで確認済み）
// 環境変数 GEMINI_MODEL で指定可能（例: gemini-flash-latest, gemini-pro, gemini-1.5-pro）
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-flash-latest";

// ==============================
// ユーティリティ関数
// ==============================

function trim(value: string | null | undefined): string {
  if (!value) return "";
  return String(value).trim();
}

/**
 * JSON文字列を抽出（fence除去、部分抽出対応）
 * Gemini APIの応答からJSONを安全に抽出する
 */
function extractJsonFromText(text: string): string | null {
  if (!text || typeof text !== "string") return null;

  const trimmed = text.trim();
  if (!trimmed) return null;

  // 1. ```json ... ``` または ``` ... ``` の除去
  let cleaned = trimmed;
  if (cleaned.startsWith("```")) {
    cleaned = cleaned.replace(/^```[a-zA-Z]*\n?/, "").replace(/```$/gm, "").trim();
  }

  // 2. { ... } の部分を抽出（前後に説明文があっても抽出）
  // ネストされたJSONに対応するため、{と}のバランスを取る
  let braceCount = 0;
  let startIndex = -1;
  for (let i = 0; i < cleaned.length; i++) {
    if (cleaned[i] === '{') {
      if (startIndex === -1) startIndex = i;
      braceCount++;
    } else if (cleaned[i] === '}') {
      braceCount--;
      if (braceCount === 0 && startIndex !== -1) {
        return cleaned.substring(startIndex, i + 1);
      }
    }
  }

  // 3. バランスが取れない場合は、最初の{から最後の}までを抽出（フォールバック）
  const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
  if (jsonMatch) {
    return jsonMatch[0];
  }

  // 4. そのまま返す（既にJSON形式の可能性）
  return cleaned;
}

// 正常なドキュメントからフィールドパターンを学習（Geminiで分析）
async function learnFieldPatternsWithGemini(): Promise<Map<string, string>> {
  console.log("📚 正常なドキュメントからフィールドパターンを学習中（Gemini）...");
  
  const patterns = new Map<string, string>();

  if (!GEMINI_ENABLED || !genAI) {
    console.log("  ⚠️  Geminiが無効のため、デフォルトパターンを使用");
    return patterns;
  }

  try {
    // 正常なドキュメントを取得
    const snapshot = await companiesCol
      .where("corporateNumber", "!=", null)
      .limit(20)
      .get();

    if (snapshot.empty) {
      console.log("  ⚠️  サンプルドキュメントが見つかりません");
      return patterns;
    }

    // サンプルデータを準備
    const samples: Record<string, any>[] = [];
    snapshot.forEach((doc) => {
      const data = doc.data();
      samples.push({
        name: data.name || "",
        corporateNumber: data.corporateNumber || "",
        postalCode: data.postalCode || "",
        address: data.address || "",
        phoneNumber: data.phoneNumber || "",
        companyUrl: data.companyUrl || "",
        representativeName: data.representativeName || "",
        prefecture: data.prefecture || "",
        established: data.established || "",
      });
    });

    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    
    const prompt = `あなたは日本の企業データベースのフィールド分析エキスパートです。

以下の正常な企業データのサンプルを分析して、各フィールドにどのような形式の値が入るかを説明してください。

サンプルデータ:
${JSON.stringify(samples, null, 2)}

以下のフィールドについて、値の形式を説明してください：
- name (会社名)
- corporateNumber (法人番号)
- postalCode (郵便番号)
- address (住所)
- phoneNumber (電話番号)
- companyUrl (企業URL)
- representativeName (代表者名)
- prefecture (都道府県)
- established (設立日)

各フィールドについて、以下の形式で回答してください：
{
  "fieldName": "値の形式の説明（例: 3桁-4桁の数値、https://で始まるURLなど）"
}

JSON形式で回答してください。`;

    // SDKレスポンスをリトライ付きで取得
    const text = await callGeminiWithRetry(model, prompt);
    if (!text) {
      return patterns; // エラー時は空のパターンを返す
    }

    // JSONを抽出（堅牢な方法）
    const jsonStr = extractJsonFromText(text);
    if (jsonStr) {
      try {
        const parsed = JSON.parse(jsonStr);
        for (const [field, description] of Object.entries(parsed)) {
          patterns.set(field, String(description));
        }
        console.log(`  ✅ ${patterns.size}個のフィールドパターンを学習`);
      } catch (parseErr: any) {
        console.log(`  ⚠️  JSONパースエラー: ${parseErr.message}`);
        console.log(`  ⚠️  抽出されたJSON（先頭200文字）: ${jsonStr.substring(0, 200)}`);
      }
    } else {
      console.log("  ⚠️  JSON形式の応答が取得できませんでした");
      console.log(`  ⚠️  応答テキスト（先頭200文字）: ${text.substring(0, 200)}`);
    }
  } catch (err: any) {
    console.log(`  ⚠️  パターン学習エラー: ${err.message}`);
  }

  return patterns;
}

// CSVのヘッダーとサンプルデータをGeminiで解析
async function analyzeCSVWithGemini(
  headers: string[],
  sampleRows: string[][],
  fieldPatterns: Map<string, string>
): Promise<Map<number, string>> {
  if (!GEMINI_ENABLED || !genAI) {
    return new Map();
  }

  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });

    // サンプル行を準備（最大5行）
    const sampleData = sampleRows.slice(0, 5).map((row, idx) => ({
      rowIndex: idx + 1,
      cells: row.map((cell, colIdx) => ({
        columnIndex: colIdx,
        header: headers[colIdx] || `Column${colIdx}`,
        value: trim(cell),
      })),
    }));

    const fieldPatternsText = Array.from(fieldPatterns.entries())
      .map(([field, pattern]) => `- ${field}: ${pattern}`)
      .join("\n");

    const prompt = `あなたは日本の企業データベースのCSV解析エキスパートです。

CSVのヘッダーとサンプルデータを分析して、各列がどのフィールドに対応するかを判断してください。

ヘッダー:
${headers.map((h, i) => `${i}: ${h}`).join("\n")}

サンプルデータ:
${JSON.stringify(sampleData, null, 2)}

各フィールドの値の形式:
${fieldPatternsText}

注意事項:
- 無視すべきフィールド: ${Array.from(IGNORE_FIELDS).join(", ")}
- 郵便番号は3桁-4桁の数値形式（例: 123-4567）
- 法人番号は13桁の数値
- 代表者名は個人名らしい文字列
- 住所は「都道府県」を含む文字列
- 企業URLはhttps://で始まるURL
- 業種は「業」「店」「所」などのキーワードを含む文字列

各列について、以下の形式で回答してください：
{
  "columnMappings": {
    "列インデックス": "フィールド名（companies_newのフィールド名）"
  }
}

列インデックスは0から始まります。無視すべき列はマッピングしないでください。
JSON形式で回答してください。`;

    // SDKレスポンスをリトライ付きで取得
    const text = await callGeminiWithRetry(model, prompt);
    if (!text) {
      return new Map(); // エラー時は空のマッピングを返す
    }

    // JSONを抽出（堅牢な方法）
    const jsonStr = extractJsonFromText(text);
    if (jsonStr) {
      try {
        const parsed = JSON.parse(jsonStr);
        const mappings = new Map<number, string>();
        
        if (parsed.columnMappings) {
          for (const [colIdx, field] of Object.entries(parsed.columnMappings)) {
            mappings.set(Number(colIdx), String(field));
          }
        }

        console.log(`  ✅ Gemini解析: ${mappings.size}個の列マッピングを取得`);
        return mappings;
      } catch (parseErr: any) {
        console.log(`  ⚠️  JSONパースエラー: ${parseErr.message}`);
      }
    }
  } catch (err: any) {
    console.log(`  ⚠️  Gemini解析エラー: ${err.message}`);
  }

  return new Map();
}

// 値の型を判定（簡易版）
function detectValueType(value: string): string {
  const trimmed = trim(value);
  if (!trimmed) return "unknown";

  if (/^\d{13}$/.test(trimmed.replace(/\D/g, ""))) return "corporateNumber";
  if (/^\d{3}-?\d{4}$/.test(trimmed.replace(/\D/g, ""))) return "postalCode";
  if (/^https?:\/\//i.test(trimmed)) return "url";
  if (/^[\d\-\(\)]+$/.test(trimmed) && trimmed.length >= 10 && trimmed.length <= 15) return "phoneNumber";
  if (/^\d{4}[年\/\-]\d{1,2}[月\/\-]\d{1,2}/.test(trimmed)) return "date";
  if (/^\d{4}年/.test(trimmed)) return "year";
  if (/^[\d,]+$/.test(trimmed.replace(/[,\s]/g, ""))) return "number";
  if (trimmed.includes("都道府県") || /^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)/.test(trimmed)) return "prefecture";
  if (/[都道府県市区町村]/.test(trimmed) && trimmed.length > 8) return "address";
  if (trimmed.includes("株式会社") || trimmed.includes("有限会社")) return "name";
  if (/[業|店|所|場|館]/.test(trimmed)) return "industry";
  
  return "text";
}

// 値の型からフィールド候補を取得
function getFieldsByValueType(valueType: string): string[] {
  const mapping: Record<string, string[]> = {
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
    industry: ["industry", "industries", "industryLarge", "industryMiddle", "industrySmall", "industryDetail"],
    text: ["companyDescription", "overview", "executives", "shareholders", "suppliers", "clients", "banks"],
  };

  return mapping[valueType] || [];
}

// 値がフィールドに適合するか判定
function isValueSuitableForField(value: string, field: string, fieldPatterns: Map<string, string>): boolean {
  const valueType = detectValueType(value);
  
  // フィールドパターンから判定
  const pattern = fieldPatterns.get(field);
  if (pattern) {
    // パターンに基づいて判定（簡易版）
    if (field === "postalCode" && valueType !== "postalCode") return false;
    if (field === "corporateNumber" && valueType !== "corporateNumber") return false;
    if (field === "companyUrl" && valueType !== "url") return false;
    if (field === "phoneNumber" && valueType !== "phoneNumber") return false;
    if (field === "prefecture" && valueType !== "prefecture") return false;
    if (field === "address" && valueType !== "address") return false;
    if (field === "name" && valueType !== "name") return false;
  }

  return true;
}

// フィールドに値を設定
function setFieldValue(result: Record<string, any>, field: string, value: string): void {
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

// Gemini解析結果の型定義
type GeminiAnalysis = {
  industries?: string[];
  industry?: string;
  [key: string]: any;
};

// Gemini API呼び出しをリトライ付きで実行
async function callGeminiWithRetry(
  model: any,
  prompt: string,
  maxRetries: number = 3,
  retryDelay: number = 1000
): Promise<string | null> {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const genResult = await model.generateContent(prompt);
      const response = genResult.response;
      return response.text();
    } catch (err: any) {
      const errorMsg = err.message || String(err);
      
      // 503エラー（サービス過負荷）の場合はリトライ
      if (errorMsg.includes("503") || errorMsg.includes("Service Unavailable") || errorMsg.includes("overloaded")) {
        if (attempt < maxRetries) {
          const delay = retryDelay * attempt; // 指数バックオフ
          console.log(`  ⚠️  Gemini API過負荷 (試行 ${attempt}/${maxRetries})。${delay}ms後にリトライ...`);
          await new Promise(resolve => setTimeout(resolve, delay));
          continue;
        } else {
          console.log(`  ⚠️  Gemini API過負荷: 最大リトライ回数に達しました。スキップします。`);
          return null;
        }
      }
      
      // その他のエラーは即座にスロー
      throw err;
    }
  }
  return null;
}

// 各行の各セルをGeminiで分析してフィールドを判定（補正用）
async function analyzeRowCellsWithGemini(
  row: string[],
  headers: string[],
  columnMappings: Map<number, string>,
  fieldPatterns: Map<string, string>
): Promise<GeminiAnalysis> {
  const analysis: GeminiAnalysis = {
    industries: [],
  };

  if (!GEMINI_ENABLED || !genAI) {
    return analysis;
  }

  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });

    // 各セルの値を準備
    const cells = row.map((cell, idx) => ({
      columnIndex: idx,
      header: headers[idx] || `Column${idx}`,
      value: trim(cell),
      suggestedField: columnMappings.get(idx) || null,
    })).filter(cell => cell.value && !IGNORE_FIELDS.has(cell.header));

    const fieldPatternsText = Array.from(fieldPatterns.entries())
      .map(([field, pattern]) => `- ${field}: ${pattern}`)
      .join("\n");

    const prompt = `あなたは日本の企業データベースのデータ解析エキスパートです。

CSVの各行の各セルを分析して、どのフィールドに適合するかを判断してください。

セルデータ:
${JSON.stringify(cells, null, 2)}

各フィールドの値の形式:
${fieldPatternsText}

注意事項:
- 郵便番号は3桁-4桁の数値形式（例: 123-4567）。それ以外の文字列が郵便番号の位置にあれば、それは業種として扱う
- 法人番号は13桁の数値。それ以外は無視
- 代表者名は個人名らしい文字列（2-15文字程度）
- 住所は「都道府県」を含む文字列
- 企業URLはhttps://で始まるURL
- 業種は「業」「店」「所」などのキーワードを含む文字列
- 数値フィールド（capitalStock, revenue, latestProfit, employeeCount等）は数値のみ

各セルについて、以下の形式で回答してください：
{
  "cellMappings": [
    {
      "columnIndex": 列インデックス,
      "field": "フィールド名",
      "value": "正規化された値",
      "confidence": 0.0-1.0の信頼度
    }
  ]
}

信頼度が0.7未満のマッピングは無視してください。
JSON形式で回答してください。`;

    // SDKレスポンスをリトライ付きで取得
    const text = await callGeminiWithRetry(model, prompt);
    if (!text) {
      return analysis; // エラー時は空の分析結果を返す
    }

    // JSONを抽出（堅牢な方法）
    const jsonStr = extractJsonFromText(text);
    if (jsonStr) {
      try {
        const parsed = JSON.parse(jsonStr);
        
        if (parsed.cellMappings && Array.isArray(parsed.cellMappings)) {
          for (const mapping of parsed.cellMappings) {
            if (mapping.confidence >= 0.7 && mapping.field && mapping.value) {
              const field = String(mapping.field);
              const value = String(mapping.value);

              // フィールドごとの処理（analysisオブジェクトに設定）
              if (field === "corporateNumber") {
                const normalized = normalizeCorporateNumber(value);
                if (normalized) {
                  (analysis as Record<string, any>)[field] = normalized;
                }
              } else if (field === "postalCode") {
                const normalized = normalizePostalCode(value);
                if (normalized) {
                  (analysis as Record<string, any>)[field] = normalized;
                }
              } else if (field === "industries" || field === "industry") {
                if (!analysis.industries) {
                  analysis.industries = [];
                }
                analysis.industries.push(value);
              } else if (["capitalStock", "revenue", "latestProfit"].includes(field)) {
                const num = parseFinancialNumeric(value, field);
                if (num !== null) {
                  (analysis as Record<string, any>)[field] = num;
                }
              } else if (["employeeCount", "officeCount", "factoryCount", "storeCount"].includes(field)) {
                const num = parseNumeric(value);
                if (num !== null) {
                  (analysis as Record<string, any>)[field] = num;
                }
              } else {
                (analysis as Record<string, any>)[field] = value;
              }
            }
          }
        }
      } catch (parseErr: any) {
        // JSONパースエラーは無視して続行（フォールバック）
      }
    }
  } catch (err: any) {
    console.log(`  ⚠️  セル解析エラー: ${err.message}`);
  }

  // industries配列をクリーンアップ
  if (analysis.industries) {
    const seen = new Set<string>();
    analysis.industries = analysis.industries
      .filter((v: any) => {
        const trimmed = trim(v);
        if (!trimmed) return false;
        if (seen.has(trimmed)) return false;
        seen.add(trimmed);
        return true;
      });
    
    if (analysis.industries.length > 0) {
      analysis.industry = analysis.industries[0];
    }
  }

  return analysis;
}

// 法人番号の正規化
function normalizeCorporateNumber(value: string): string | null {
  const trimmed = trim(value);
  if (!trimmed) return null;
  
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
  console.log("🚀 Gemini APIを使ったCSV列ズレ修正を開始します\n");

  // Gemini APIは無効化されています
  if (!GEMINI_ENABLED) {
    console.error("❌ Gemini APIが無効です。このスクリプトはGemini APIが必要です。");
    console.error("   再度有効化する場合は、initGemini()関数のコメントアウトを解除してください。");
    process.exit(1);
  }

  // フィールドパターンを学習
  const fieldPatterns = await learnFieldPatternsWithGemini();

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

      // CSV全体をGeminiで解析（列マッピングを取得）
      const sampleRows = records.slice(1, Math.min(6, records.length));
      const columnMappings = await analyzeCSVWithGemini(headers, sampleRows, fieldPatterns);

      let batch: WriteBatch = db.batch();
      let batchCount = 0;

      // 各行を処理（列マッピングをベースに、必要に応じてGeminiで補正）
      for (let i = 1; i < records.length; i++) {
        const row = records[i];
        
        try {
          // まず列マッピングを使って基本的なマッピング
          const mappedData: Record<string, any> = {
            industries: [],
          };

          // 列マッピングに基づいてマッピング
          for (let colIdx = 0; colIdx < row.length; colIdx++) {
            const value = trim(row[colIdx]);
            if (!value) continue;

            const header = headers[colIdx] || "";
            if (IGNORE_FIELDS.has(header)) continue;

            const field = columnMappings.get(colIdx);
            if (!field) {
              // マッピングがない場合、値の型から推測
              const valueType = detectValueType(value);
              const suggestedFields = getFieldsByValueType(valueType);
              if (suggestedFields.length > 0) {
                // 最初の候補を使用
                const suggestedField = suggestedFields[0];
                if (isValueSuitableForField(value, suggestedField, fieldPatterns)) {
                  setFieldValue(mappedData, suggestedField, value);
                }
              }
              continue;
            }

            // フィールドに値を設定
            setFieldValue(mappedData, field, value);
          }

          // 信頼度が低い場合や重要なフィールドが欠けている場合、Geminiで補正
          if (!mappedData.name || (!mappedData.postalCode && !mappedData.address)) {
            const geminiData = await analyzeRowCellsWithGemini(row, headers, columnMappings, fieldPatterns);
            // Geminiの結果で補完
            for (const [key, value] of Object.entries(geminiData)) {
              if (value && !mappedData[key]) {
                mappedData[key] = value;
              }
            }
          }
          
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

