/*
  タイプGの既存ドキュメントのnameフィールドを修正するスクリプト
  
  - nameフィールドに「（株）」が含まれているものを「株式会社」に正規化
  - 既に「株式会社」になっているもの（前株・後株問わず）はそのまま
  - nameが「日経バリューサーチ」になっているものを正式名称に変更
  
  使い方:
    # DRY RUN
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_type_g_names.ts --dry-run
    
    # 実行
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_type_g_names.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentReference } from "firebase-admin/firestore";
import axios from "axios";
import * as cheerio from "cheerio";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

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
      validateStatus: (status) => status < 500
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

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log("📊 タイプGのnameフィールド修正処理を開始します\n");

  let processedCount = 0;
  let normalizedCount = 0;
  let nikkeiValueSearchFixedCount = 0;

  // タイプGのドキュメントを取得
  const snapshot = await db.collection(COLLECTION_NAME)
    .where("csvType", "==", "type_g")
    .get();

  console.log(`📄 タイプGのドキュメント数: ${snapshot.size} 件\n`);

  for (const doc of snapshot.docs) {
    const data = doc.data();
    const currentName = data.name;
    
    if (!currentName || typeof currentName !== "string") {
      continue;
    }

    processedCount++;
    const updateData: Record<string, any> = {};
    let needsUpdate = false;

    // 1. 「（株）」を「株式会社」に正規化（既に「株式会社」になっているものはそのまま）
    if (currentName.includes("（株）")) {
      const normalizedName = normalizeCompanyNameFormat(currentName);
      if (normalizedName && normalizedName !== currentName) {
        updateData.name = normalizedName;
        needsUpdate = true;
        normalizedCount++;
        if (normalizedCount <= 20 || processedCount % 100 === 0) {
          console.log(`  📝 [${doc.id}] 「（株）」正規化: "${currentName}" → "${normalizedName}"`);
        }
      }
    }

    // 2. 「日経バリューサーチ」の処理
    const isNikkeiValueSearch = currentName === "日経バリューサーチ" || currentName.includes("日経バリューサーチ");
    if (isNikkeiValueSearch) {
      let extractedName: string | null = null;

      // ① フィールド内から企業名を抽出
      extractedName = extractCompanyNameFromFields(data);
      
      // ② JSON形式のフィールドから企業名を抽出
      if (!extractedName) {
        // JSON形式のフィールドを探す
        for (const [field, value] of Object.entries(data)) {
          if (isJsonValue(value)) {
            const jsonName = extractCompanyNameFromJson(value);
            if (jsonName) {
              extractedName = jsonName;
              console.log(`  📝 [${doc.id}] JSONから企業名を抽出: "${jsonName}"`);
              break;
            }
          }
        }
      }
      
      // ③ 企業HPから企業名を取得
      if (!extractedName) {
        const url = data.companyUrl || data.contactUrl;
        if (url) {
          extractedName = await extractCompanyNameFromUrl(url);
          if (extractedName) {
            console.log(`  📝 [${doc.id}] URLから企業名を抽出: "${extractedName}"`);
          }
        }
      }

      if (extractedName) {
        // 抽出された企業名に「（株）」が含まれている場合は「株式会社」に正規化
        let finalExtractedName = extractedName;
        if (extractedName.includes("（株）")) {
          const normalizedExtractedName = normalizeCompanyNameFormat(extractedName);
          if (normalizedExtractedName) {
            finalExtractedName = normalizedExtractedName;
          }
        }
        updateData.name = finalExtractedName;
        needsUpdate = true;
        nikkeiValueSearchFixedCount++;
        if (finalExtractedName !== extractedName) {
          console.log(`  ✅ [${doc.id}] 日経バリューサーチを修正（「（株）」正規化）: "${currentName}" → "${extractedName}" → "${finalExtractedName}"`);
        } else {
          console.log(`  ✅ [${doc.id}] 日経バリューサーチを修正: "${currentName}" → "${finalExtractedName}"`);
        }
      } else {
        console.warn(`  ⚠️  [${doc.id}] 企業名を抽出できませんでした (name="${currentName}")`);
      }
    }

    // 更新実行
    if (needsUpdate && Object.keys(updateData).length > 0) {
      if (!DRY_RUN) {
        await doc.ref.update({
          ...updateData,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        });
      }
    }

    // 進捗表示
    if (processedCount % 100 === 0) {
      console.log(`  📊 処理中: ${processedCount} / ${snapshot.size} 件`);
    }
  }

  console.log(`\n✅ 処理完了`);
  console.log(`  - 処理ドキュメント数: ${processedCount} 件`);
  console.log(`  - 「（株）」正規化: ${normalizedCount} 件`);
  console.log(`  - 「日経バリューサーチ」修正: ${nikkeiValueSearchFixedCount} 件`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に保存するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});


