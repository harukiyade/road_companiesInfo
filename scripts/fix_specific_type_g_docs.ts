/*
  特定のタイプGドキュメントのJSON形式フィールドを解析して各フィールドに割り当てるスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_specific_type_g_docs.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentReference } from "firebase-admin/firestore";
import axios from "axios";
import * as cheerio from "cheerio";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// 対象のドキュメントID
const TARGET_DOC_IDS = [
  "3430001051236",
  "5430001089258",
  "5430001094489",
  "6450001013611"
];

// Firebase初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: serviceAccount.project_id,
  });

  console.log(`✅ Firebase 初期化完了`);
}

const db: Firestore = admin.firestore();

function trim(v?: string | null): string | null {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function parseNumeric(v: string): number | null {
  const cleaned = v.replace(/[^\d.-]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
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
function extractFieldsFromJson(jsonStr: string | null | undefined): Record<string, any> {
  const result: Record<string, any> = {};
  if (!jsonStr) return result;

  try {
    let parsed: any;
    if (typeof jsonStr === "string") {
      parsed = JSON.parse(jsonStr);
    } else {
      parsed = jsonStr;
    }

    // パターン1: 企業サマリ形式（日経バリューサーチの標準形式）
    let kv = parsed?.企業サマリ?.kv;
    
    // パターン2: addressフィールドに直接kvがある形式
    if (!kv && parsed?.kv) {
      kv = parsed.kv;
    }

    if (!kv) return result;

    // 各フィールドをマッピング
    if (kv.会社名 || kv.商号) {
      result.name = normalizeCompanyNameFormat(kv.会社名 || kv.商号);
    }
    if (kv.英文名) {
      result.nameEn = trim(kv.英文名);
    }
    if (kv.法人番号) {
      const digits = String(kv.法人番号).replace(/\D/g, "");
      if (digits.length === 13) {
        result.corporateNumber = digits;
      }
    }
    if (kv.本社住所 || kv.登記簿住所) {
      result.address = trim(kv.本社住所 || kv.登記簿住所);
    }
    if (kv.業種) {
      result.industry = trim(kv.業種);
    }
    if (kv.資本金) {
      const num = parseNumeric(kv.資本金);
      if (num !== null) result.capitalStock = num;
    }
    if (kv.売上高 || kv["売上高（単独）"]) {
      const num = parseNumeric(kv.売上高 || kv["売上高（単独）"]);
      if (num !== null) result.revenue = num;
    }
    if (kv.従業員数) {
      const num = parseNumeric(kv.従業員数);
      if (num !== null) result.employeeCount = num;
    }
    if (kv.設立年月日 || kv.設立日) {
      result.established = trim(kv.設立年月日 || kv.設立日);
    }
    if (kv.決算月) {
      result.fiscalMonth = trim(kv.決算月);
    }
    if (kv.代表者名 || kv.代表者 || kv.代表取締役) {
      result.representativeName = trim(kv.代表者名 || kv.代表者 || kv.代表取締役);
    }
    if (kv.事業内容) {
      result.businessDescriptions = trim(kv.事業内容);
    }
    if (kv.URL || kv.会社HP) {
      const url = trim(kv.URL || kv.会社HP);
      if (url && url !== "ー" && url !== "-") {
        result.companyUrl = url;
      }
    }
    if (kv.所属団体) {
      result.affiliations = trim(kv.所属団体);
    }
    if (kv.都道府県) {
      result.prefecture = trim(kv.都道府県);
    }
    if (kv.郵便番号) {
      const postal = String(kv.郵便番号).replace(/\D/g, "");
      if (postal.length === 7) {
        result.postalCode = postal.replace(/(\d{3})(\d{4})/, "$1-$2");
      }
    }
    if (kv.電話番号) {
      result.phoneNumber = trim(kv.電話番号);
    }
    if (kv.発行済株式数) {
      const num = parseNumeric(kv.発行済株式数);
      if (num !== null) result.issuedShares = num;
    }
    if (kv.上場区分 || kv.上場) {
      result.listing = trim(kv.上場区分 || kv.上場);
    }
    if (kv.日経会社コード) {
      result.nikkeiCode = trim(kv.日経会社コード);
    }

    // tablesからファイナンス情報を抽出
    if (parsed?.tables && Array.isArray(parsed.tables)) {
      for (const table of parsed.tables) {
        if (table.title === "ファイナンス情報" && table.rows && Array.isArray(table.rows)) {
          // 最新の行から資本金を取得
          for (const row of table.rows) {
            if (Array.isArray(row) && row.length >= 2) {
              const capitalStr = row[1]; // 資本金の列
              if (capitalStr) {
                const num = parseNumeric(capitalStr);
                if (num !== null && !result.capitalStock) {
                  result.capitalStock = num;
                }
              }
            }
          }
        }
      }
    }
  } catch (e) {
    // JSONパースエラーは無視
    console.warn(`  ⚠️ JSONパースエラー: ${e}`);
  }

  return result;
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
      validateStatus: (status) => status < 500
    });

    if (response.status !== 200) {
      return null;
    }

    const $ = cheerio.load(response.data);
    
    // 1. <title>タグから企業名を抽出
    const title = $("title").text().trim();
    if (title) {
      if (title.includes("株式会社") || title.includes("（株）")) {
        const patterns = [
          /([^|｜\-–—\s]{2,30}(?:株式会社|（株）)[^|｜\-–—\s]{0,20})/,
          /([^|｜\-–—\s]+(?:株式会社|（株）)[^|｜\-–—\s]*)/
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

    // 4. ページ内のテキストから「株式会社」を含む最初の文字列を抽出
    const bodyText = $("body").text();
    const companyMatch = bodyText.match(/([^。\n\s]{2,30}(?:株式会社|（株）)[^。\n\s]{0,20})/);
    if (companyMatch && companyMatch[1] && companyMatch[1].length <= 50) {
      const extracted = normalizeCompanyNameFormat(companyMatch[1]);
      if (extracted && extracted.length <= 50) {
        return extracted;
      }
    }
  } catch (e: any) {
    // エラーは無視
  }

  return null;
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log("📊 特定ドキュメントのJSON形式フィールド解析処理を開始します\n");

  let processedCount = 0;
  let updatedCount = 0;

  for (const docId of TARGET_DOC_IDS) {
    console.log(`\n📄 ドキュメントID: ${docId}`);
    
    const docRef = db.collection(COLLECTION_NAME).doc(docId);
    const docSnap = await docRef.get();

    if (!docSnap.exists) {
      console.warn(`  ⚠️  ドキュメントが見つかりません: ${docId}`);
      continue;
    }

    const data = docSnap.data();
    if (!data) {
      console.warn(`  ⚠️  ドキュメントデータが空です: ${docId}`);
      continue;
    }

    processedCount++;
    const updateData: Record<string, any> = {};
    let hasJsonFields = false;

    // JSON形式のフィールドを探す
    for (const [field, value] of Object.entries(data)) {
      if (isJsonValue(value)) {
        hasJsonFields = true;
        console.log(`  📝 JSON形式のフィールドを検出: ${field}`);
        
        // JSONからフィールド情報を抽出
        const extractedFields = extractFieldsFromJson(value);
        
        // 抽出したフィールドをupdateDataにマージ
        for (const [extractedField, extractedValue] of Object.entries(extractedFields)) {
          if (extractedValue !== null && extractedValue !== undefined && extractedValue !== "") {
            // 既存の値が空の場合、またはJSON形式の場合は上書き
            const currentValue = data[extractedField];
            const shouldOverwrite = 
              !currentValue || 
              isJsonValue(currentValue) || 
              currentValue === "日経バリューサーチ" ||
              currentValue === "上場" ||
              (extractedField === "companyUrl" && currentValue.includes("nikkei.com")) ||
              (extractedField === "representativeName" && (currentValue === "上場" || currentValue.length < 2));
            
            if (shouldOverwrite) {
              updateData[extractedField] = extractedValue;
              console.log(`    ✅ ${extractedField}: ${extractedValue}`);
            } else {
              console.log(`    ⏭️  ${extractedField}: 既存値があるためスキップ (既存: ${currentValue})`);
            }
          }
        }
      }
    }

    // nameが「日経バリューサーチ」の場合は修正
    const currentName = data.name;
    const isNikkeiValueSearch = currentName === "日経バリューサーチ" || currentName?.includes("日経バリューサーチ");
    
    if (isNikkeiValueSearch) {
      let extractedName: string | null = null;

      // ① JSON形式のフィールドから企業名を抽出（優先）
      for (const [field, value] of Object.entries(data)) {
        if (isJsonValue(value)) {
          // JSONから直接企業名を抽出
          try {
            let parsed: any;
            if (typeof value === "string") {
              parsed = JSON.parse(value);
            } else {
              parsed = value;
            }

            // パターン1: 企業サマリ形式
            let kv = parsed?.企業サマリ?.kv;
            // パターン2: addressフィールドに直接kvがある形式
            if (!kv && parsed?.kv) {
              kv = parsed.kv;
            }

            if (kv) {
              if (kv.会社名 || kv.商号) {
                extractedName = normalizeCompanyNameFormat(kv.会社名 || kv.商号);
                if (extractedName) {
                  console.log(`  📝 JSONから企業名を抽出: "${extractedName}"`);
                  break;
                }
              }
            }
          } catch (e) {
            // パースエラーは無視
          }
        }
      }

      // ② フィールド内から企業名を抽出
      if (!extractedName) {
        const fields = ["overview", "companyDescription", "businessDescriptions"];
        for (const field of fields) {
          const value = data[field];
          if (value && typeof value === "string" && !value.includes("日経バリューサーチ")) {
            const lines = value.split(/\n|。/);
            for (const line of lines) {
              const trimmed = line.trim();
              if (trimmed.length > 2 && trimmed.length < 50) {
                if (trimmed.includes("株式会社") || trimmed.includes("（株）")) {
                  extractedName = normalizeCompanyNameFormat(trimmed);
                  if (extractedName) {
                    console.log(`  📝 フィールドから企業名を抽出: "${extractedName}"`);
                    break;
                  }
                }
              }
            }
            if (extractedName) break;
          }
        }
      }

      // ③ 企業HPから企業名を取得
      if (!extractedName) {
        const url = data.companyUrl || data.contactUrl;
        if (url && url !== "ー" && url !== "-") {
          extractedName = await extractCompanyNameFromUrl(url);
          if (extractedName) {
            console.log(`  📝 URLから企業名を抽出: "${extractedName}"`);
          }
        }
      }

      if (extractedName) {
        updateData.name = extractedName;
        console.log(`  ✅ 企業名を修正: "${currentName}" → "${extractedName}"`);
      } else {
        console.warn(`  ⚠️  企業名を抽出できませんでした`);
      }
    }

    // 更新実行
    if (Object.keys(updateData).length > 0) {
      if (!DRY_RUN) {
        await docRef.update({
          ...updateData,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        });
        updatedCount++;
        console.log(`  ✅ 更新完了: ${docId}`);
      } else {
        console.log(`  📝 (DRY_RUN) 更新予定:`, updateData);
      }
    } else {
      console.log(`  ℹ️  更新する内容がありません`);
    }
  }

  console.log(`\n✅ 処理完了`);
  console.log(`  - 処理ドキュメント数: ${processedCount} 件`);
  console.log(`  - 更新: ${updatedCount} 件`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に保存するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

