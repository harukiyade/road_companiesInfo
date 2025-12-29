/* 
  各CSVファイルの最初の5行のみをGeminiで分析して更新するスクリプト
  
  使い方:
    GEMINI_API_KEY=your_api_key \
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/update_first_5_rows_with_gemini.ts
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
const ROWS_TO_PROCESS = 5; // 各CSVファイルの最初の5行のみ

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

  // 2. { ... } の部分を抽出（ネストされたJSONに対応）
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
      console.log("  ⚠️  サンプルドキュメントが見つかりません");
      return patterns;
    }

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

    const text = await callGeminiWithRetry(model, prompt);
    if (!text) {
      return patterns;
    }

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
      }
    }
  } catch (err: any) {
    console.log(`  ⚠️  パターン学習エラー: ${err.message}`);
  }

  return patterns;
}

// 各行をGeminiで分析してフィールドマッピングを取得
async function analyzeRowWithGemini(
  row: string[],
  headers: string[],
  fieldPatterns: Map<string, string>
): Promise<Record<string, any>> {
  const result: Record<string, any> = {
    industries: [],
  };

  if (!GEMINI_ENABLED || !genAI) {
    return result;
  }

  try {
    const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });

    // 各セルの値を準備（空白も含めるが、無視フィールドは除外）
    const cells = row.map((cell, idx) => ({
      columnIndex: idx,
      header: headers[idx] || `Column${idx}`,
      value: trim(cell),
      isEmpty: !trim(cell),
    })).filter(cell => !IGNORE_FIELDS.has(cell.header));

    const fieldPatternsText = Array.from(fieldPatterns.entries())
      .map(([field, pattern]) => `- ${field}: ${pattern}`)
      .join("\n");

    const prompt = `あなたは日本の企業データベースのデータ解析エキスパートです。

CSVの1行（1つの企業の情報）を分析して、各セルの値がどのフィールドに適合するかを判断してください。

重要: この行には1つの企業の情報のみが含まれています。列がずれている可能性があるため、ヘッダー名だけでなく、値の内容から判断してください。

ヘッダー（列がずれている可能性があります）:
${headers.map((h, i) => `${i}: "${h}"`).join("\n")}

セルデータ（この行のすべてのセル）:
${JSON.stringify(cells, null, 2)}

各フィールドの値の形式:
${fieldPatternsText}

フィールドマッピングルール:
- name (会社名): 「株式会社」「有限会社」などを含む企業名
- corporateNumber (法人番号): 13桁の数字のみ（それ以外は無視）
- postalCode (郵便番号): 3桁-4桁の数値形式（例: 123-4567）。それ以外の文字列が郵便番号の位置にあれば、それは業種として扱う
- address (住所): 「都道府県」を含む長い文字列（8文字以上）
- prefecture (都道府県): 「都道府県」で終わる短い文字列（例: 東京都、愛知県）
- phoneNumber (電話番号): 数字とハイフンを含む10-15文字の文字列
- companyUrl (企業URL): https://またはhttp://で始まるURL
- representativeName (代表者名): 個人名らしい文字列（2-15文字程度、業種っぽくない）
- established (設立日): 日付形式（YYYY年M月D日、YYYY/M/Dなど）
- industry (業種): 「業」「店」「所」などのキーワードを含む文字列
- industries (業種配列): 複数の業種を配列として
- capitalStock (資本金): 数値（千円単位、必要に応じて1000倍）
- revenue (売上): 数値（千円単位、必要に応じて1000倍）
- latestProfit (利益): 数値（千円単位、必要に応じて1000倍）
- employeeCount (社員数): 数値

注意事項:
1. ヘッダー名を参考にするが、値の内容が重要です
2. 空白のセルは無視してください
3. 列がずれている可能性があるため、値の内容から判断してください
4. この行には1つの企業の情報のみが含まれています
5. 同じフィールドに複数の値がマッピングされる場合は、最も信頼度の高いものを選択してください

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
                result[field] = normalized;
              }
            } else if (field === "postalCode") {
              const normalized = normalizePostalCode(value);
              if (normalized) {
                result[field] = normalized;
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
                result[field] = num;
              }
            } else if (["employeeCount", "officeCount", "factoryCount", "storeCount"].includes(field)) {
              const num = parseNumeric(value);
              if (num !== null) {
                result[field] = num;
              }
            } else {
              // 既に値が設定されている場合、信頼度が高い方のみ更新
              if (!result[field] || mapping.confidence > 0.9) {
                result[field] = value;
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

// 企業を検索
async function findCompany(data: Record<string, any>): Promise<DocumentReference | null> {
  // 法人番号が有効な場合のみ検索（無効なプレースホルダー値はスキップ）
  if (data.corporateNumber && !isInvalidCorporateNumber(data.corporateNumber)) {
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
  console.log("🚀 各CSVファイルの最初の5行をGeminiで分析して更新します\n");

  // Gemini APIは無効化されています
  if (!GEMINI_ENABLED) {
    console.error("❌ Gemini APIが無効です。このスクリプトはGemini APIが必要です。");
    console.error("   再度有効化する場合は、initGemini()関数のコメントアウトを解除してください。");
    process.exit(1);
  }

  // フィールドパターンを学習
  const fieldPatterns = await learnFieldPatternsWithGemini();

  const updatedDocs: Array<{ csvFile: string; rowIndex: number; docId: string; companyName: string }> = [];

  for (const filePath of TARGET_FILES) {
    const resolvedPath = path.resolve(filePath);
    const fileName = path.basename(filePath);
    
    if (!fs.existsSync(resolvedPath)) {
      console.log(`⚠️  ファイルが見つかりません: ${filePath}`);
      continue;
    }

    console.log(`\n📄 処理中: ${fileName}`);

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

      // 最初の5行のみを処理
      const rowsToProcess = records.slice(1, Math.min(ROWS_TO_PROCESS + 1, records.length));
      console.log(`  📝 処理対象: ${rowsToProcess.length}行\n`);

      for (let i = 0; i < rowsToProcess.length; i++) {
        const row = rowsToProcess[i];
        const rowIndex = i + 1;
        
        console.log(`  [行${rowIndex}] Geminiで分析中...`);
        
        try {
          // Geminiで行を分析
          const mappedData = await analyzeRowWithGemini(row, headers, fieldPatterns);
          
          if (!mappedData.name) {
            console.log(`    ⚠️  会社名が取得できませんでした。スキップします。`);
            continue;
          }

          console.log(`    ✅ 分析完了: ${mappedData.name}`);

          const existingRef = await findCompany(mappedData);
          
          const updateData: Record<string, any> = {
            ...mappedData,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          };

          let docId: string;
          if (existingRef) {
            await existingRef.update(updateData);
            docId = existingRef.id;
            console.log(`    ✅ 更新: ドキュメントID ${docId}`);
          } else {
            const newRef = companiesCol.doc();
            await newRef.set({
              ...updateData,
              createdAt: admin.firestore.FieldValue.serverTimestamp(),
            });
            docId = newRef.id;
            console.log(`    ✅ 新規作成: ドキュメントID ${docId}`);
          }

          updatedDocs.push({
            csvFile: fileName,
            rowIndex: rowIndex,
            docId: docId,
            companyName: mappedData.name,
          });

          // 少し待機（API制限対策）
          await new Promise(resolve => setTimeout(resolve, 500));
        } catch (err: any) {
          console.log(`    ❌ エラー: ${err.message}`);
        }
      }

      console.log(`  ✅ 完了: ${fileName}`);
    } catch (err: any) {
      console.log(`  ❌ エラー: ${err.message}`);
    }
  }

  console.log("\n" + "=".repeat(80));
  console.log("📊 更新結果サマリー");
  console.log("=".repeat(80));
  console.log(`総更新件数: ${updatedDocs.length}\n`);

  console.log("更新されたドキュメント:");
  updatedDocs.forEach((doc, index) => {
    console.log(`  [${index + 1}] ${doc.csvFile} - 行${doc.rowIndex}`);
    console.log(`      ドキュメントID: ${doc.docId}`);
    console.log(`      会社名: ${doc.companyName}`);
    console.log("");
  });

  // ファイルに出力
  const outputFile = `updated_docs_first_5_rows_${Date.now()}.txt`;
  const outputContent = updatedDocs.map(doc => 
    `${doc.csvFile} - 行${doc.rowIndex}: ${doc.docId} (${doc.companyName})`
  ).join("\n");
  
  fs.writeFileSync(outputFile, outputContent);
  console.log(`\n✅ 更新されたドキュメントIDを ${outputFile} に保存しました`);
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

