/* 
  指定されたCSVファイルの最初の5行を新規作成するスクリプト（Gemini使用）
  
  使い方:
    GEMINI_API_KEY=... GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/create_companies_from_csv_first_5.ts
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
  "取引種別",
  "SBフラグ",
  "NDA",
  "AD",
  "ステータス",
  "備考",
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
  if (!apiKey) {
    console.log("⚠️  GEMINI_API_KEYが設定されていません。Gemini機能は無効です。");
    return false;
  }

  try {
    genAI = new GoogleGenerativeAI(apiKey);
    console.log("✅ Gemini API 初期化完了");
    return true;
  } catch (err: any) {
    console.log(`⚠️  Gemini API 初期化エラー: ${err.message}`);
    return false;
  }
  */
}

const GEMINI_ENABLED = initGemini();
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-flash-latest";

// 文字列のトリム
function trim(v: any): string {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

// JSON文字列を抽出（fence除去、部分抽出対応）
function extractJsonFromText(text: string): string | null {
  if (!text || typeof text !== "string") return null;
  const trimmed = text.trim();
  if (!trimmed) return null;

  // 1. ```json ... ``` または ``` ... ``` の除去
  let cleaned = trimmed;
  if (cleaned.startsWith("```")) {
    cleaned = cleaned.replace(/^```[a-zA-Z]*\n?/, "").replace(/```$/, "").trim();
  }

  // 2. { ... } の部分を抽出（ネストされたJSONに対応）
  let braceCount = 0;
  let startIndex = -1;
  let endIndex = -1;
  for (let i = 0; i < cleaned.length; i++) {
    if (cleaned[i] === '{') {
      if (startIndex === -1) startIndex = i;
      braceCount++;
    } else if (cleaned[i] === '}') {
      braceCount--;
      if (braceCount === 0 && startIndex !== -1) {
        endIndex = i;
        break;
      }
    }
  }
  if (startIndex !== -1 && endIndex !== -1) {
    return cleaned.substring(startIndex, endIndex + 1);
  }

  // 3. バランスが取れない場合は、最初の{から最後の}までを抽出（フォールバック）
  const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
  if (jsonMatch) {
    return jsonMatch[0];
  }
  return cleaned;
}

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
          const delay = retryDelay * attempt;
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

// 正常なドキュメントからフィールドパターンを学習
async function learnFieldPatternsWithGemini(): Promise<Map<string, string>> {
  console.log("📚 正常なドキュメントからフィールドパターンを学習中（Gemini）...");
  
  const patterns = new Map<string, string>();

  if (!GEMINI_ENABLED || !genAI) {
    console.log("  ⚠️  Geminiが無効のため、デフォルトパターンを使用");
    return patterns;
  }

  try {
    const snapshot = await companiesCol
      .where("corporateNumber", "!=", null)
      .limit(20)
      .get();

    if (snapshot.empty) {
      console.log("  ⚠️  学習用のサンプルドキュメントが見つかりませんでした");
      return patterns;
    }

    const samples: any[] = [];
    snapshot.forEach((doc: any) => {
      const data = doc.data();
      if (data.name && data.corporateNumber) {
        samples.push({
          name: data.name,
          corporateNumber: data.corporateNumber,
          postalCode: data.postalCode || "",
          address: data.address || "",
          representativeName: data.representativeName || "",
          industry: data.industry || "",
        });
      }
    });

    if (samples.length === 0) {
      console.log("  ⚠️  有効なサンプルデータがありません");
      return patterns;
    }

    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    const prompt = `以下のFirestoreドキュメントのサンプルから、各フィールドの値のパターンを学習してください。

サンプルデータ:
${JSON.stringify(samples, null, 2)}

以下のフィールドについて、値の特徴を分析してください：
- corporateNumber (法人番号): 13桁の数字のみ（それ以外は無効）
- postalCode (郵便番号): 3桁-4桁の形式（例: 450-0002）
- address (住所): 都道府県名を含む住所文字列
- representativeName (代表者名): 人名らしい文字列
- industry (業種): 業種名の文字列

各フィールドの値の特徴を簡潔に説明してください。JSON形式で回答してください。
{
  "corporateNumber": "特徴説明",
  "postalCode": "特徴説明",
  "address": "特徴説明",
  "representativeName": "特徴説明",
  "industry": "特徴説明"
}`;

    const text = await callGeminiWithRetry(model, prompt);
    if (text) {
      const jsonStr = extractJsonFromText(text);
      if (jsonStr) {
        try {
          const parsed = JSON.parse(jsonStr);
          for (const [field, pattern] of Object.entries(parsed)) {
            if (pattern && typeof pattern === "string") {
              patterns.set(field, pattern);
            }
          }
          console.log(`  ✅ ${patterns.size}個のパターンを学習しました`);
        } catch (parseError: any) {
          console.log(`  ⚠️  パターン学習エラー: JSONパース失敗 - ${parseError.message}`);
        }
      } else {
        console.log("  ⚠️  JSON形式の応答が取得できませんでした");
      }
    } else {
      console.log("  ⚠️  Geminiからの応答がありませんでした（パターン学習）");
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
  const columnMappings = new Map<number, string>();

  if (!GEMINI_ENABLED || !genAI) {
    return columnMappings;
  }

  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });
    
    const patternsText = Array.from(fieldPatterns.entries())
      .map(([field, pattern]) => `- ${field}: ${pattern}`)
      .join("\n");

    const prompt = `CSVファイルのヘッダーとサンプルデータを分析し、各列がFirestoreのcompanies_newコレクションのどのフィールドに対応するかを判定してください。

CSVヘッダー:
${headers.map((h, i) => `${i}: ${h}`).join("\n")}

サンプルデータ（最初の3行）:
${sampleRows.slice(0, 3).map((row, i) => `行${i + 1}: ${row.map((cell, j) => `[${j}]${cell}`).join(", ")}`).join("\n")}

学習済みフィールドパターン:
${patternsText || "なし"}

Firestoreのcompanies_newコレクションの主要フィールド:
- name (会社名)
- corporateNumber (法人番号): 13桁の数字のみ
- postalCode (郵便番号): 3桁-4桁の形式
- address (住所)
- prefecture (都道府県)
- representativeName (代表者名)
- phoneNumber (電話番号)
- companyUrl (企業URL)
- industry (業種)
- industries (業種配列)
- established (設立日)
- capitalStock (資本金)
- revenue (売上)
- employeeCount (従業員数)
- officeCount (事業所数)
- factoryCount (工場数)
- storeCount (店舗数)

各列インデックスがどのフィールドに対応するかを判定してください。
無視すべきフィールド: ${Array.from(IGNORE_FIELDS).join(", ")}

JSON形式で回答してください:
{
  "columnMappings": {
    "列インデックス": "フィールド名"
  }
}`;

    const text = await callGeminiWithRetry(model, prompt);
    if (text) {
      const jsonStr = extractJsonFromText(text);
      if (jsonStr) {
        try {
          const parsed = JSON.parse(jsonStr);
          if (parsed.columnMappings && typeof parsed.columnMappings === "object") {
            for (const [colIdx, field] of Object.entries(parsed.columnMappings)) {
              const idx = parseInt(colIdx);
              if (!isNaN(idx) && typeof field === "string") {
                columnMappings.set(idx, field);
              }
            }
            console.log(`  ✅ ${columnMappings.size}個の列マッピングを取得しました`);
          }
        } catch (parseError: any) {
          console.log(`  ⚠️  Gemini解析エラー: JSONパース失敗 - ${parseError.message}`);
        }
      }
    } else {
      console.log("  ⚠️  Geminiからの応答がありませんでした（CSV解析）");
    }
  } catch (err: any) {
    console.log(`  ⚠️  CSV解析エラー: ${err.message}`);
  }

  return columnMappings;
}

// Gemini解析結果の型定義
type GeminiAnalysis = {
  industries?: string[];
  industry?: string;
  [key: string]: any;
};

// 各行の各セルをGeminiで分析してフィールドを判定
async function analyzeRowCellsWithGemini(
  row: string[],
  headers: string[],
  columnMappings: Map<number, string>,
  fieldPatterns: Map<string, string>
): Promise<GeminiAnalysis> {
  const result: GeminiAnalysis = { industries: [] };

  if (!GEMINI_ENABLED || !genAI) {
    return result;
  }

  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });

    const patternsText = Array.from(fieldPatterns.entries())
      .map(([field, pattern]) => `- ${field}: ${pattern}`)
      .join("\n");

    const columnMappingsText = Array.from(columnMappings.entries())
      .map(([idx, field]) => `列${idx} (${headers[idx]}): ${field}`)
      .join("\n");

    const prompt = `CSVファイルの1行分のデータを分析し、各セルの値がFirestoreのcompanies_newコレクションのどのフィールドに対応するかを判定してください。

CSVヘッダーと値:
${row.map((cell, i) => `[${i}] ${headers[i] || "不明"}: "${cell}"`).join("\n")}

既存の列マッピング（参考）:
${columnMappingsText || "なし"}

学習済みフィールドパターン:
${patternsText || "なし"}

Firestoreのcompanies_newコレクションの主要フィールド:
- name (会社名): 会社名らしい文字列
- corporateNumber (法人番号): 13桁の数字のみ（無効な値は無視）
- postalCode (郵便番号): 3桁-4桁の形式（例: 450-0002）
- address (住所): 都道府県名を含む住所文字列
- prefecture (都道府県): 都道府県名
- representativeName (代表者名): 人名らしい文字列
- phoneNumber (電話番号): 電話番号形式
- companyUrl (企業URL): https://で始まるURL
- industry (業種): 業種名の文字列
- industries (業種配列): 業種名の配列
- established (設立日): 日付形式
- capitalStock (資本金): 数値
- revenue (売上): 数値
- employeeCount (従業員数): 数値
- officeCount (事業所数): 数値
- factoryCount (工場数): 数値
- storeCount (店舗数): 数値

各セルの値を分析し、最も適切なフィールドにマッピングしてください。
- ヘッダー名を参考にしつつ、値の内容から最も適切なフィールドを判定してください
- 空白の値は無視してください
- 無効な法人番号（例: 9180000000000のようなプレースホルダー値）は無視してください
- 業種らしい文字列は industries 配列に追加してください
- 同じフィールドに複数の値がマッピングされる場合は、最も信頼度の高いものを選択してください

各セルについて、以下の形式で回答してください：
{
  "cellMappings": [
    {
      "columnIndex": 列インデックス,
      "field": "フィールド名（companies_newのフィールド名）",
      "value": "正規化された値（空白の場合はnull）",
      "confidence": 0.0-1.0の信頼度,
      "reason": "なぜこのフィールドに適合すると判断したかの理由"
    }
  ]
}

信頼度が0.7未満のマッピングは無視してください。
JSON形式で回答してください。`;

    const text = await callGeminiWithRetry(model, prompt);
    if (!text) {
      return result;
    }

    const jsonStr = extractJsonFromText(text);
    if (jsonStr) {
      try {
        const parsed = JSON.parse(jsonStr);
        
        if (parsed.cellMappings && Array.isArray(parsed.cellMappings)) {
          // 信頼度でソート（高い順）
          const sortedMappings = parsed.cellMappings
            .filter((m: any) => m.confidence >= 0.7 && m.field && m.value && m.value !== "null")
            .sort((a: any, b: any) => (b.confidence || 0) - (a.confidence || 0));

          // 各フィールドに対して最も信頼度の高いマッピングを選択
          const fieldMap = new Map<string, { value: string; confidence: number }>();
          
          for (const mapping of sortedMappings) {
            const field = String(mapping.field);
            const value = String(mapping.value);
            const confidence = Number(mapping.confidence) || 0;

            // 既に同じフィールドにマッピングがある場合、信頼度が高い方を選択
            if (!fieldMap.has(field) || fieldMap.get(field)!.confidence < confidence) {
              fieldMap.set(field, { value, confidence });
            }
          }

          // マッピングされた値を処理
          for (const [field, mapping] of fieldMap.entries()) {
            const value = mapping.value;

            // フィールドごとの処理
            if (field === "corporateNumber") {
              const normalized = normalizeCorporateNumber(value);
              if (normalized) {
                (result as Record<string, any>)[field] = normalized;
              }
            } else if (field === "postalCode") {
              const normalized = normalizePostalCode(value);
              if (normalized) {
                (result as Record<string, any>)[field] = normalized;
              }
            } else if (field === "industries" || field === "industry") {
              if (!result.industries) {
                result.industries = [];
              }
              if (!result.industries.includes(value)) {
                result.industries.push(value);
              }
            } else if (["capitalStock", "revenue", "latestProfit"].includes(field)) {
              const num = parseFinancialNumeric(value, field);
              if (num !== null) {
                (result as Record<string, any>)[field] = num;
              }
            } else if (["employeeCount", "officeCount", "factoryCount", "storeCount"].includes(field)) {
              const num = parseNumeric(value);
              if (num !== null) {
                (result as Record<string, any>)[field] = num;
              }
            } else {
              // 既に値が設定されている場合、信頼度が高い方のみ更新
              if (!(result as Record<string, any>)[field] || mapping.confidence > 0.9) {
                (result as Record<string, any>)[field] = value;
              }
            }
          }
        }
      } catch (parseErr: any) {
        console.log(`  ⚠️  JSONパースエラー: ${parseErr.message}`);
      }
    }
  } catch (err: any) {
    console.log(`  ⚠️  行解析エラー: ${err.message}`);
  }

  // industries配列をクリーンアップ
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

// 法人番号の正規化
function normalizeCorporateNumber(value: string): string | null {
  const trimmed = trim(value);
  if (!trimmed) return null;
  
  if (/^\d+\.\d+E\+\d+$/i.test(trimmed)) {
    try {
      const num = parseFloat(trimmed);
      const digits = Math.floor(num).toString().replace(/\D/g, "");
      if (digits.length === 13) {
        // 無効なパターンをチェック
        if (isInvalidCorporateNumber(digits)) return null;
        return digits;
      }
    } catch {
      return null;
    }
  }
  
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 13) {
    // 無効なパターンをチェック
    if (isInvalidCorporateNumber(digits)) return null;
    return digits;
  }
  
  return null;
}

// 無効な法人番号パターンを検出
function isInvalidCorporateNumber(digits: string): boolean {
  if (digits.length !== 13) return true;
  
  // 同じ数字の繰り返し（例: 9180000000000, 1111111111111）
  if (/^(\d)\1{12}$/.test(digits)) return true;
  
  // 0のみ（例: 0000000000000）
  if (digits === "0000000000000") return true;
  
  // 9で始まり残りが0のみ（例: 9180000000000, 9000000000000）
  if (/^9\d{2}0{10}$/.test(digits)) return true;
  
  // 1で始まり残りが0のみ（例: 1000000000000）
  if (/^10{12}$/.test(digits)) return true;
  
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

// メイン処理
async function main() {
  console.log("🚀 各CSVファイルの最初の5行をGeminiで分析して新規作成します\n");

  // Gemini APIは無効化されています
  if (!GEMINI_ENABLED) {
    console.error("❌ Gemini APIが無効です。このスクリプトはGemini APIが必要です。");
    console.error("   再度有効化する場合は、initGemini()関数のコメントアウトを解除してください。");
    process.exit(1);
  }

  // フィールドパターンを学習
  const fieldPatterns = await learnFieldPatternsWithGemini();
  console.log();

  const createdDocIds: { csvFile: string; rowNum: number; docId: string; companyName: string }[] = [];
  let totalCreatedCount = 0;

  for (const filePath of TARGET_FILES) {
    const fileName = path.basename(filePath);
    console.log(`\n📄 処理中: ${fileName}`);

    try {
      const csvContent = fs.readFileSync(filePath, "utf8");
      const records: Record<string, string>[] = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        relax_column_count: true,
        relax_quotes: true,
        skip_records_with_error: true,
      });

      if (records.length === 0) {
        console.log("  ⚠️  CSVに有効なレコードがありません。スキップします。");
        continue;
      }

      const headers = Object.keys(records[0]);
      console.log(`  📊 ヘッダー数: ${headers.length}, データ行数: ${records.length}`);
      console.log(`  📝 処理対象: ${Math.min(records.length, 5)}行`);

      // GeminiでCSVヘッダーとサンプルデータを解析し、列マッピングを取得
      const sampleRows = records.slice(0, 5).map(r => Object.values(r));
      const columnMappings = await analyzeCSVWithGemini(headers, sampleRows, fieldPatterns);

      let batch: WriteBatch = db.batch();
      let batchCount = 0;

      for (let i = 0; i < Math.min(records.length, 5); i++) {
        const row = Object.values(records[i]);
        const csvCompanyName = records[i]["会社名"] || records[i]["企業名"] || "(不明)";
        console.log(`\n  [行${i + 1}] Geminiで分析中...`);

        // Geminiで各セルの値を分析し、フィールドを判定
        const geminiAnalysis = await analyzeRowCellsWithGemini(
          row,
          headers,
          columnMappings,
          fieldPatterns
        );

        if (!geminiAnalysis.name) {
          console.log("    ⚠️  会社名が取得できませんでした。スキップします。");
          continue;
        }

        // 新規ドキュメントを作成
        const docRef = companiesCol.doc();

        const createData: Record<string, any> = {
          ...geminiAnalysis,
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        };

        // industries配列の処理
        if (geminiAnalysis.industries && Array.isArray(geminiAnalysis.industries) && geminiAnalysis.industries.length > 0) {
          createData.industries = geminiAnalysis.industries;
        }

        batch.set(docRef, createData);
        batchCount++;
        totalCreatedCount++;

        console.log(`    ✅ 分析完了: ${geminiAnalysis.name}`);
        console.log(`    ✅ 新規作成: ドキュメントID ${docRef.id}`);
        createdDocIds.push({ csvFile: fileName, rowNum: i + 1, docId: docRef.id, companyName: geminiAnalysis.name });

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
  const outputFile = `created_docs_first_5_rows_${timestamp}.txt`;
  const outputContent = createdDocIds
    .map(item => `${item.csvFile} - 行${item.rowNum}: ${item.docId} (${item.companyName})`)
    .join("\n");
  fs.writeFileSync(outputFile, outputContent, "utf8");

  console.log("\n" + "=".repeat(80));
  console.log(`✅ 処理完了: ${totalCreatedCount}件のドキュメントを新規作成しました`);
  console.log(`📄 結果ファイル: ${outputFile}`);
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

