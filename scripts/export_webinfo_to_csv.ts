/* eslint-disable no-console */

/**
 * scripts/export_webinfo_to_csv.ts
 *
 * ✅ 目的
 * - companies_new を全件走査
 * - companies_webInfo が無ければ Web から取得して保存
 * - 取得内容を CSV に出力
 *
 * ✅ 今回の修正ポイント
 * 1) Gemini API はデフォルトで無効（USE_GEMINI=true で有効化可能）
 *    - 正規表現ベースの抽出のみで動作
 *    - USE_GEMINI=true かつ GEMINI_API_KEY が設定されている場合のみAI補完を実行
 * 2) Google検索依存を完全排除
 *    - 決算公告系、非上場財務DB系、推定売上系、人材/採用系を優先
 *    - 各サービスの検索URLを直接組み立てて候補URLを生成
 * 4) 財務情報抽出の強化（総資産/総負債/純資産/営業利益/経常利益など）
 * 5) ToS/robots に配慮した実装
 *    - リトライ機能（429/5xxエラー対応）
 *    - レート制限（リクエスト間隔の確保）
 *    - 低頻度アクセス
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 *
 * ✅ オプションENV（Gemini APIを使用する場合のみ）
 * - USE_GEMINI=true (Gemini APIを有効化する場合)
 * - GEMINI_API_KEY=... (USE_GEMINI=true の場合に必須)
 * - GEMINI_MODEL=gemini-1.5-flash-latest (デフォルト: gemini-1.5-flash-latest)
 * - GEMINI_MAX_CHARS=12000
 * - GEMINI_TIMEOUT_MS=12000
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";
import fetch from "node-fetch";
import * as cheerio from "cheerio";
import { GoogleGenerativeAI } from "@google/generative-ai";

// ------------------------------
// Firebase Admin SDK 初期化
// ------------------------------
if (!admin.apps.length) {
  try {
    const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
    
    // デバッグログ: 設定されているパスを表示
    console.log(`[Firebase初期化] FIREBASE_SERVICE_ACCOUNT_KEY: ${serviceAccountPath || "未設定"}`);
    
    // FIREBASE_SERVICE_ACCOUNT_KEY が設定されていない場合
    if (!serviceAccountPath) {
      console.error("❌ エラー: FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません。");
      console.error("");
      console.error("以下のように環境変数を設定してから再実行してください:");
      console.error('  export FIREBASE_SERVICE_ACCOUNT_KEY="/absolute/path/to/serviceAccount.json"');
      console.error("  npx tsx scripts/export_webinfo_to_csv.ts");
      process.exit(1);
    }
    
    // ファイルが存在しない場合
    if (!fs.existsSync(serviceAccountPath)) {
      console.error(`❌ エラー: サービスアカウントキーファイルが存在しません。`);
      console.error(`   確認したパス: ${serviceAccountPath}`);
      console.error("");
      console.error("以下の点を確認してください:");
      console.error("  1. ファイルパスが正しいか");
      console.error("  2. ファイルが実際に存在するか");
      console.error("  3. 環境変数の設定が正しいか");
      console.error("");
      console.error("正しいパスを設定してから再実行してください:");
      console.error('  export FIREBASE_SERVICE_ACCOUNT_KEY="/absolute/path/to/serviceAccount.json"');
      console.error("  npx tsx scripts/export_webinfo_to_csv.ts");
      process.exit(1);
    }
    
    // サービスアカウントキーを読み込んで初期化
    console.log(`[Firebase初期化] サービスアカウントキーを読み込み中: ${serviceAccountPath}`);
    const serviceAccount = JSON.parse(
      fs.readFileSync(serviceAccountPath, "utf8")
    );
    
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: "albert-ma",
    });
    
    console.log("[Firebase初期化] ✅ 初期化が完了しました");
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", error);
    console.error("");
    console.error("以下の点を確認してください:");
    console.error("  1. サービスアカウントキーファイルが有効なJSON形式か");
    console.error("  2. ファイルの読み取り権限があるか");
    console.error("  3. 環境変数の設定が正しいか");
    console.error("");
    console.error("環境変数を再設定してから再実行してください:");
    console.error('  export FIREBASE_SERVICE_ACCOUNT_KEY="/absolute/path/to/serviceAccount.json"');
    console.error("  npx tsx scripts/export_webinfo_to_csv.ts");
    process.exit(1);
  }
}

const db = admin.firestore();

// ------------------------------
// Gemini 初期化（APIキーがあれば有効、デフォルトは無効）
// ------------------------------
const USE_GEMINI = process.env.USE_GEMINI === "true"; // 明示的に有効化する場合のみ
const GEMINI_API_KEY = USE_GEMINI ? (process.env.GEMINI_API_KEY || "") : "";
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-1.5-flash-latest";
const GEMINI_MAX_CHARS = Number(process.env.GEMINI_MAX_CHARS || 12000);
const GEMINI_TIMEOUT_MS = Number(process.env.GEMINI_TIMEOUT_MS || 12000);

const genAI = (USE_GEMINI && GEMINI_API_KEY) ? new GoogleGenerativeAI(GEMINI_API_KEY) : null;

// ------------------------------
// util
// ------------------------------
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * リトライ付きHTTPリクエスト（ToS/robots配慮）
 */
async function fetchWithRetry(
  url: string,
  options: any = {},
  maxRetries: number = 2,
  retryDelay: number = 2000
): Promise<any> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const res: any = await fetch(url, {
        ...options,
        headers: {
          "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          ...options.headers,
        },
        timeout: 10000,
      } as any);

      // 429 Too Many Requests の場合は待機
      if (res.status === 429) {
        const retryAfter = res.headers.get("retry-after");
        const waitTime = retryAfter ? parseInt(retryAfter) * 1000 : retryDelay * (attempt + 1);
        console.warn(`[fetchWithRetry] 429 for ${url}, waiting ${waitTime}ms`);
        await sleep(waitTime);
        continue;
      }

      // 5xxエラーの場合はリトライ
      if (res.status >= 500 && attempt < maxRetries) {
        await sleep(retryDelay * (attempt + 1));
        continue;
      }

      return res;
    } catch (e) {
      if (attempt < maxRetries) {
        await sleep(retryDelay * (attempt + 1));
        continue;
      }
      console.warn(`[fetchWithRetry] failed after ${maxRetries} retries for ${url}:`, (e as any)?.message || e);
      return null;
    }
  }
  return null;
}

/**
 * CSVの値をエスケープ（DB投入用：空値は空欄）
 */
function escapeCsvValue(value: any): string {
  if (value === null || value === undefined || value === "") {
    return "";
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return "";
    }
    return `"${value.join("; ").replace(/"/g, '""')}"`;
  }

  const str = String(value);
  if (str.trim() === "") {
    return "";
  }

  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }

  return str;
}

/**
 * 数値を抽出（正規表現でパターンマッチ）
 */
function extractNumber(text: string, pattern: RegExp): number | null {
  const match = text.match(pattern);
  if (match) {
    const numStr =
      match[1]?.replace(/,/g, "").replace(/[^\d.]/g, "") ||
      match[0]?.replace(/,/g, "").replace(/[^\d.]/g, "");
    if (!numStr) return null;
    const num = parseFloat(numStr);
    return isNaN(num) ? null : num;
  }
  return null;
}

/**
 * 金額を千円単位に正規化
 */
function normalizeToThousandYen(value: number, unit: string): number {
  const unitLower = unit.toLowerCase();
  if (unitLower.includes("億")) {
    return value * 100000;
  } else if (unitLower.includes("百万")) {
    return value * 1000;
  } else if (unitLower.includes("万円")) {
    return value * 10;
  } else if (unitLower.includes("千円")) {
    return value;
  } else if (unitLower.includes("円") && !unitLower.includes("千") && !unitLower.includes("万") && !unitLower.includes("億")) {
    return value / 1000;
  }
  return value;
}

/**
 * リスト項目を抽出
 */
function extractListItems(text: string, patterns: RegExp[]): string[] {
  const items: string[] = [];
  for (const pattern of patterns) {
    const matches = text.matchAll(new RegExp(pattern, "gi"));
    for (const match of matches) {
      const item = (match as any)[1] || (match as any)[2];
      if (item && !items.includes(item.trim())) {
        items.push(item.trim());
      }
    }
  }
  return items;
}

/**
 * URLが公式サイトっぽいか判定
 */
function isOfficialSite(url: string, companyName: string): boolean {
  try {
    const domain = new URL(url).hostname.toLowerCase();
    const nameLower = companyName.toLowerCase().replace(/\s+/g, "");
    return (
      domain.includes(nameLower) ||
      domain.includes("corp") ||
      domain.includes("company") ||
      domain.includes("inc") ||
      domain.includes("co.jp") ||
      domain.endsWith(".jp")
    );
  } catch {
    return false;
  }
}

// ------------------------------
// URL候補生成（Google検索非依存）
// ------------------------------

/**
 * 決算公告系の検索URLを生成
 */
function generateKessanAnnouncementUrls(
  companyName: string,
  corporateNumber: string | null
): string[] {
  const urls: string[] = [];
  
  // catr.jp（官報決算データベース）
  if (corporateNumber) {
    urls.push(`https://catr.jp/s/?q=${encodeURIComponent(corporateNumber)}`);
  }
  urls.push(`https://catr.jp/s/?q=${encodeURIComponent(companyName)}`);
  
  // 官報情報検索
  urls.push(`https://kanpou.npb.go.jp/search?q=${encodeURIComponent(companyName)}`);
  
  return urls;
}

/**
 * 非上場財務・企業DB系の検索URLを生成
 */
function generateFinancialDbUrls(
  companyName: string,
  corporateNumber: string | null
): string[] {
  const urls: string[] = [];
  
  // 企業INDEXナビ（g-search）
  urls.push(`https://cnavi-app.g-search.or.jp/search?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    urls.push(`https://cnavi-app.g-search.or.jp/search?q=${encodeURIComponent(corporateNumber)}`);
  }
  
  // バフェットコード
  urls.push(`https://www.buffett-code.com/global_screening?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    urls.push(`https://www.buffett-code.com/global_screening?q=${encodeURIComponent(corporateNumber)}`);
  }
  
  // 全国法人リスト（houjin.jp）
  urls.push(`https://houjin.jp/search?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    urls.push(`https://houjin.jp/search?q=${encodeURIComponent(corporateNumber)}`);
  }
  
  // Alarmbox
  urls.push(`https://alarmbox.jp/companyinfo/?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    urls.push(`https://alarmbox.jp/companyinfo/?q=${encodeURIComponent(corporateNumber)}`);
  }
  
  // TDB企業サーチ（検索URLのみ生成、アクセスは失敗してもOK）
  urls.push(`https://www.tdb.co.jp/search/company/?q=${encodeURIComponent(companyName)}`);
  
  // TSR企業情報（検索URLのみ生成）
  urls.push(`https://www.tsr-net.co.jp/search/?q=${encodeURIComponent(companyName)}`);
  
  return urls;
}

/**
 * 推定売上系の検索URLを生成
 */
function generateRevenueEstimateUrls(companyName: string): string[] {
  const urls: string[] = [];
  
  // 売上高推定サービス（例）
  urls.push(`https://www.zenkoku-net.com/search?q=${encodeURIComponent(companyName)}`);
  
  return urls;
}

/**
 * 人材/採用系の検索URLを生成（各サイトの検索URLパターンを最適化）
 */
function generateRecruitmentServiceUrls(companyName: string): string[] {
  const urls: string[] = [];
  
  // Green: 企業検索
  urls.push(`https://www.green-japan.com/search?q=${encodeURIComponent(companyName)}`);
  urls.push(`https://www.green-japan.com/search/companies?q=${encodeURIComponent(companyName)}`);
  
  // Wantedly: 企業検索
  urls.push(`https://www.wantedly.com/companies?query=${encodeURIComponent(companyName)}`);
  urls.push(`https://www.wantedly.com/search?q=${encodeURIComponent(companyName)}&type=companies`);
  
  // 複業クラウド: 企業検索
  urls.push(`https://fukuigyo-cloud.com/search?keyword=${encodeURIComponent(companyName)}`);
  urls.push(`https://fukuigyo-cloud.com/companies?q=${encodeURIComponent(companyName)}`);
  
  // Talentio: 企業検索
  urls.push(`https://talentio.com/search?q=${encodeURIComponent(companyName)}`);
  urls.push(`https://talentio.com/companies?q=${encodeURIComponent(companyName)}`);
  
  // HERP: 企業検索
  urls.push(`https://herp.careers/search?q=${encodeURIComponent(companyName)}`);
  urls.push(`https://herp.careers/companies?q=${encodeURIComponent(companyName)}`);
  
  // Offers: 企業検索
  urls.push(`https://offers.jp/search?q=${encodeURIComponent(companyName)}`);
  urls.push(`https://offers.jp/companies?q=${encodeURIComponent(companyName)}`);
  
  // doocyjob: 企業検索
  urls.push(`https://doocyjob.com/search?q=${encodeURIComponent(companyName)}`);
  urls.push(`https://doocyjob.com/companies?q=${encodeURIComponent(companyName)}`);
  
  // doda: 企業検索
  urls.push(`https://doda.jp/DodaFront/View/CompanySearchList.action?searchCondition.companyName=${encodeURIComponent(companyName)}`);
  urls.push(`https://doda.jp/company/search?q=${encodeURIComponent(companyName)}`);
  
  // workship: 企業検索
  urls.push(`https://workship.jp/search?q=${encodeURIComponent(companyName)}`);
  urls.push(`https://workship.jp/companies?q=${encodeURIComponent(companyName)}`);
  
  // type転職: 企業検索
  urls.push(`https://type.jp/search?q=${encodeURIComponent(companyName)}`);
  urls.push(`https://type.jp/companies?q=${encodeURIComponent(companyName)}`);
  
  // マイナビ転職: 企業検索
  urls.push(`https://tenshoku.mynavi.jp/company/search?q=${encodeURIComponent(companyName)}`);
  urls.push(`https://tenshoku.mynavi.jp/company/?q=${encodeURIComponent(companyName)}`);
  
  // マイナビ2026: 企業検索
  urls.push(`https://job.mynavi.jp/26/pc/search/corp.html?tab=corp&q=${encodeURIComponent(companyName)}`);
  urls.push(`https://job.mynavi.jp/26/pc/search/corp.html?q=${encodeURIComponent(companyName)}`);
  
  return urls;
}

/**
 * 会社名/住所/法人番号から候補URLを生成（Google検索非依存）
 */
function generateCandidateUrls(
  name: string,
  address: string | null,
  corporateNumber: string | null,
  existingHomepage: string | null
): string[] {
  const urls: string[] = [];
  
  // 1. 決算公告系（最優先）
  urls.push(...generateKessanAnnouncementUrls(name, corporateNumber));
  
  // 2. 非上場財務・企業DB系
  urls.push(...generateFinancialDbUrls(name, corporateNumber));
  
  // 3. 推定売上系
  urls.push(...generateRevenueEstimateUrls(name));
  
  // 4. 人材/採用系
  urls.push(...generateRecruitmentServiceUrls(name));
  
  // 5. 既存HP（最後に追加）
  if (existingHomepage) {
    urls.push(existingHomepage);
  }
  
  // 重複除去して最大20件
  const unique = Array.from(new Set(urls));
  return unique.slice(0, 20);
}

/**
 * HTMLから詳細な企業情報を抽出（正規表現/ルールベース、財務強化版）
 * 各サイトの構造に合わせた抽出ロジックを含む
 */
function parseDetailedInfoFromHtml(
  html: string,
  url: string,
  companyName: string
): Partial<any> {
  const $ = cheerio.load(html);
  const info: Partial<any> = {
    sourceUrls: [url],
    licenses: [],
    banks: [],
    directors: [],
    sns: [],
    officers: [],
    shareholders: [],
  };

  const title = $("title").first().text().trim() || "";
  const metaDescription =
    $('meta[name="description"]').attr("content")?.trim() || "";
  const text = $("body").text().replace(/\s+/g, " ").trim();
  const bodyHtml = $("body").html() || "";
  const urlLower = url.toLowerCase();

  // 各サイトの構造に合わせた情報抽出（強化版）
  if (urlLower.includes("green-japan.com")) {
    // Green: 構造化データや特定のクラスから情報を抽出
    $('[data-company-name], .company-name, .companyName, h1, h2').each((_, el) => {
      const name = $(el).text().trim();
      if (name && name.includes(companyName.substring(0, 5))) {
        info.companyName = name;
      }
    });
    $('.company-description, .companyDescription, [data-company-description], .company-profile, .profile-description').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
    $('.company-industry, .industry, [data-industry], .industry-tag, .tag').each((_, el) => {
      const industry = $(el).text().trim();
      if (industry && industry.length > 1 && industry.length < 50) {
        if (!info.industry) info.industry = industry;
      }
    });
    $('.company-employee-count, .employeeCount, [data-employee-count], .employee-number').each((_, el) => {
      const count = extractNumber($(el).text(), /(\d+)/);
      if (count) info.employeeCount = count;
    });
    // Green: 資本金、売上などの財務情報
    $('.company-capital, .capital, [data-capital]').each((_, el) => {
      const capitalText = $(el).text();
      const capital = extractNumber(capitalText, /([\d,]+)/);
      if (capital) {
        info.capital = normalizeToThousandYen(capital, capitalText);
      }
    });
    // Green: 代表者情報
    $('.representative, .ceo, [data-representative]').each((_, el) => {
      const rep = $(el).text().trim();
      if (rep && rep.length > 1) {
        if (!info.representative) info.representative = rep;
        if (!info.representativeName) info.representativeName = rep;
      }
    });
    // Green: 会社URL
    $('a[href*="http"], .company-url, [data-url]').each((_, el) => {
      const href = $(el).attr("href") || $(el).text();
      if (href && (href.startsWith("http") || href.includes("www"))) {
        try {
          const urlObj = new URL(href.startsWith("http") ? href : `https://${href}`);
          if (isOfficialSite(urlObj.href, companyName)) {
            info.website = urlObj.href;
            info.companyUrl = urlObj.href;
          }
        } catch {}
      }
    });
  } else if (urlLower.includes("wantedly.com")) {
    // Wantedly: 構造化データから情報を抽出
    $('.company-profile-description, .companyDescription, .company-about, .about').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
    $('.company-industry, .industry-tag, .tag, [data-industry]').each((_, el) => {
      const industry = $(el).text().trim();
      if (industry && industry.length > 1 && industry.length < 50) {
        if (!info.industry) info.industry = industry;
      }
    });
    $('.company-size, .companySize, .employee-count, [data-employee-count]').each((_, el) => {
      const sizeText = $(el).text();
      const count = extractNumber(sizeText, /(\d+)/);
      if (count) info.employeeCount = count;
    });
    // Wantedly: 代表者情報
    $('.founder, .ceo, .representative, [data-founder]').each((_, el) => {
      const rep = $(el).text().trim();
      if (rep && rep.length > 1) {
        if (!info.representative) info.representative = rep;
        if (!info.representativeName) info.representativeName = rep;
      }
    });
    // Wantedly: 会社URL
    $('a[href*="http"], .website, [data-website]').each((_, el) => {
      const href = $(el).attr("href") || $(el).text();
      if (href && href.startsWith("http")) {
        try {
          const urlObj = new URL(href);
          if (isOfficialSite(urlObj.href, companyName)) {
            info.website = urlObj.href;
            info.companyUrl = urlObj.href;
          }
        } catch {}
      }
    });
  } else if (urlLower.includes("fukuigyo-cloud.com")) {
    // 複業クラウド: 企業情報を抽出
    $('.company-info, .company-detail, .company-profile').each((_, el) => {
      const infoText = $(el).text();
      const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
      if (industry && !info.industry) info.industry = industry[1].trim();
      const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
      if (employees) info.employeeCount = employees;
      const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
      if (capital) info.capital = normalizeToThousandYen(capital, infoText);
    });
    $('.company-description, .description').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
  } else if (urlLower.includes("talentio.com")) {
    // Talentio: 企業情報を抽出
    $('.company-profile, .company-info').each((_, el) => {
      const infoText = $(el).text();
      const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
      if (industry && !info.industry) info.industry = industry[1].trim();
      const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
      if (employees) info.employeeCount = employees;
    });
    $('.company-description, .description').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
  } else if (urlLower.includes("herp.careers")) {
    // HERP: 企業情報を抽出
    $('.company-detail, .company-info').each((_, el) => {
      const infoText = $(el).text();
      const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
      if (industry && !info.industry) info.industry = industry[1].trim();
      const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
      if (employees) info.employeeCount = employees;
    });
    $('.company-description, .description').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
  } else if (urlLower.includes("offers.jp")) {
    // Offers: 企業情報セクションから抽出
    $('.company-info, .companyInfo, .company-detail').each((_, el) => {
      const infoText = $(el).text();
      const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
      if (industry && !info.industry) info.industry = industry[1].trim();
      const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
      if (employees) info.employeeCount = employees;
      const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
      if (capital) info.capital = normalizeToThousandYen(capital, infoText);
    });
    $('.company-description, .description, .company-about').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
  } else if (urlLower.includes("doocyjob.com")) {
    // doocyjob: 企業情報を抽出
    $('.company-info, .company-detail').each((_, el) => {
      const infoText = $(el).text();
      const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
      if (industry && !info.industry) info.industry = industry[1].trim();
      const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
      if (employees) info.employeeCount = employees;
    });
    $('.company-description, .description').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
  } else if (urlLower.includes("doda.jp")) {
    // doda: 企業情報テーブルから抽出
    $('th, .label, dt').each((_, el) => {
      const label = $(el).text().trim();
      const value = $(el).next('td, .value, dd').text().trim();
      if (label.includes("業種") && value && !info.industry) info.industry = value;
      if (label.includes("従業員数") && value) {
        const count = extractNumber(value, /(\d+)/);
        if (count) info.employeeCount = count;
      }
      if (label.includes("資本金") && value) {
        const capital = extractNumber(value, /([\d,]+)/);
        if (capital) info.capital = normalizeToThousandYen(capital, value);
      }
      if (label.includes("売上") && value) {
        const revenue = extractNumber(value, /([\d,]+)/);
        if (revenue) info.revenue = normalizeToThousandYen(revenue, value);
      }
      if (label.includes("代表者") && value) {
        if (!info.representative) info.representative = value;
        if (!info.representativeName) info.representativeName = value;
      }
      if (label.includes("本社") && value) {
        if (!info.headquartersAddress) info.headquartersAddress = value;
      }
    });
    $('.company-description, .description, .company-about').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
  } else if (urlLower.includes("workship.jp")) {
    // workship: 企業情報を抽出
    $('.company-info, .company-detail').each((_, el) => {
      const infoText = $(el).text();
      const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
      if (industry && !info.industry) info.industry = industry[1].trim();
      const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
      if (employees) info.employeeCount = employees;
    });
    $('.company-description, .description').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
  } else if (urlLower.includes("type.jp")) {
    // type転職: 企業情報を抽出
    $('.company-info, .company-detail, .company-profile').each((_, el) => {
      const infoText = $(el).text();
      const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
      if (industry && !info.industry) info.industry = industry[1].trim();
      const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
      if (employees) info.employeeCount = employees;
      const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
      if (capital) info.capital = normalizeToThousandYen(capital, infoText);
    });
    $('.company-description, .description, .company-about').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
  } else if (urlLower.includes("mynavi.jp") || urlLower.includes("job.mynavi.jp")) {
    // マイナビ転職/マイナビ2026: 企業情報を抽出
    $('.company-info, .company-detail, .company-profile, .company-data').each((_, el) => {
      const infoText = $(el).text();
      const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
      if (industry && !info.industry) info.industry = industry[1].trim();
      const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
      if (employees) info.employeeCount = employees;
      const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
      if (capital) info.capital = normalizeToThousandYen(capital, infoText);
      const revenue = extractNumber(infoText, /売上高[：:]\s*([\d,]+)/i);
      if (revenue) info.revenue = normalizeToThousandYen(revenue, infoText);
    });
    $('.company-description, .description, .company-about, .company-overview').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
  } else if (urlLower.includes("houjin.jp")) {
    // 全国法人リスト: 企業情報を抽出
    $('.company-info, .company-detail, .company-data, table').each((_, el) => {
      const infoText = $(el).text();
      const address = infoText.match(/所在地[：:]\s*([^\n]+)/i);
      if (address && !info.address) info.address = address[1].trim();
      const phone = infoText.match(/電話番号[：:]\s*([0-9-()]+)/i);
      if (phone && !info.contactPhone) info.contactPhone = phone[1].trim();
      const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
      if (capital) info.capital = normalizeToThousandYen(capital, infoText);
      const established = infoText.match(/設立[：:]\s*(\d{4})年(\d{1,2})月(\d{1,2})日?/i);
      if (established && !info.establishedDate) {
        info.establishedDate = `${established[1]}-${established[2].padStart(2, "0")}-${(established[3] || "01").padStart(2, "0")}`;
      }
    });
  } else if (urlLower.includes("alarmbox.jp")) {
    // Alarmbox: 企業情報を抽出
    $('.company-info, .company-detail, .company-data, .company-profile').each((_, el) => {
      const infoText = $(el).text();
      const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
      if (industry && !info.industry) info.industry = industry[1].trim();
      const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
      if (employees) info.employeeCount = employees;
      const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
      if (capital) info.capital = normalizeToThousandYen(capital, infoText);
      const revenue = extractNumber(infoText, /売上高[：:]\s*([\d,]+)/i);
      if (revenue) info.revenue = normalizeToThousandYen(revenue, infoText);
    });
    $('.company-description, .description, .company-about').each((_, el) => {
      const desc = $(el).text().trim();
      if (desc && desc.length > 20) {
        if (!info.companyDescription || desc.length > info.companyDescription.length) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    });
  } else if (urlLower.includes("g-search.or.jp") || urlLower.includes("cnavi-app")) {
    // 企業INDEXナビ: 企業情報を抽出
    $('.company-info, .company-detail, .company-data, table').each((_, el) => {
      const infoText = $(el).text();
      const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
      if (industry && !info.industry) info.industry = industry[1].trim();
      const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
      if (employees) info.employeeCount = employees;
      const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
      if (capital) info.capital = normalizeToThousandYen(capital, infoText);
      const revenue = extractNumber(infoText, /売上高[：:]\s*([\d,]+)/i);
      if (revenue) info.revenue = normalizeToThousandYen(revenue, infoText);
      const totalAssets = extractNumber(infoText, /総資産[：:]\s*([\d,]+)/i);
      if (totalAssets) info.totalAssets = normalizeToThousandYen(totalAssets, infoText);
      const netAssets = extractNumber(infoText, /純資産[：:]\s*([\d,]+)/i);
      if (netAssets) info.netAssets = normalizeToThousandYen(netAssets, infoText);
    });
  } else if (urlLower.includes("buffett-code.com")) {
    // バフェットコード: 財務情報を重点的に抽出
    $('.financial-data, .company-data, table, .data-table').each((_, el) => {
      const tableText = $(el).text();
      const revenue = extractNumber(tableText, /売上高[：:]\s*([\d,]+)/i);
      if (revenue) info.revenue = normalizeToThousandYen(revenue, tableText);
      const profit = extractNumber(tableText, /純利益[：:]\s*([\d,]+)/i);
      if (profit) info.latestProfit = normalizeToThousandYen(profit, tableText);
      const operatingIncome = extractNumber(tableText, /営業利益[：:]\s*([\d,]+)/i);
      if (operatingIncome) info.operatingIncome = normalizeToThousandYen(operatingIncome, tableText);
      const totalAssets = extractNumber(tableText, /総資産[：:]\s*([\d,]+)/i);
      if (totalAssets) info.totalAssets = normalizeToThousandYen(totalAssets, tableText);
      const netAssets = extractNumber(tableText, /純資産[：:]\s*([\d,]+)/i);
      if (netAssets) info.netAssets = normalizeToThousandYen(netAssets, tableText);
      const capital = extractNumber(tableText, /資本金[：:]\s*([\d,]+)/i);
      if (capital) info.capital = normalizeToThousandYen(capital, tableText);
    });
  } else if (urlLower.includes("catr.jp")) {
    // 決算公告サイト: 財務情報を重点的に抽出
    $('.financial-data, .kessan-data, table').each((_, el) => {
      const tableText = $(el).text();
      // 総資産
      const totalAssets = extractNumber(tableText, /総資産[：:]\s*([\d,]+)/i);
      if (totalAssets) info.totalAssets = normalizeToThousandYen(totalAssets, tableText);
      // 総負債
      const totalLiabilities = extractNumber(tableText, /総負債[：:]\s*([\d,]+)/i);
      if (totalLiabilities) info.totalLiabilities = normalizeToThousandYen(totalLiabilities, tableText);
      // 純資産
      const netAssets = extractNumber(tableText, /純資産[：:]\s*([\d,]+)/i);
      if (netAssets) info.netAssets = normalizeToThousandYen(netAssets, tableText);
      // 資本金
      const capital = extractNumber(tableText, /資本金[：:]\s*([\d,]+)/i);
      if (capital) info.capital = normalizeToThousandYen(capital, tableText);
      // 決算月
      const fiscalMonth = tableText.match(/決算期[：:]\s*(\d{1,2})月/i);
      if (fiscalMonth) {
        info.fiscalMonth = `${fiscalMonth[1]}月`;
        info.settlementMonth = `${fiscalMonth[1]}月`;
      }
    });
  }

  // 上場区分・証券コード
  const listingMatch = text.match(
    /(東証|名証|福証|札証|上場|非上場|未上場|マザーズ|グロース|スタンダード|プライム)/i
  );
  if (listingMatch) {
    info.listingStatus = listingMatch[1];
  }

  const securitiesMatch = text.match(/証券コード[：:]\s*(\d{4})/i);
  if (securitiesMatch) {
    info.securitiesCode = securitiesMatch[1];
  }

  // HP（公式サイトっぽい場合）
  if (isOfficialSite(url, companyName)) {
    info.website = url;
  }

  // 問い合わせフォーム
  const contactFormMatch = text.match(
    /(お問い合わせ|問い合わせ|コンタクト|contact)[^。]*?([^\s]+\.(html|php|aspx?|jsp))/i
  );
  if (contactFormMatch) {
    try {
      const baseUrl = new URL(url).origin;
      info.contactFormUrl = new URL(contactFormMatch[2], baseUrl).href;
    } catch {}
  }

  // 郵便番号
  const postalMatch = text.match(/(〒|郵便番号)[：:\s]*(\d{3}-?\d{4})/i);
  if (postalMatch) {
    info.postalCode = postalMatch[2].replace(/-/g, "");
  }
  
  // 住所（郵便番号と都道府県から抽出）
  if (!info.headquartersAddress) {
    const addressPatterns = [
      /(〒\d{3}-?\d{4})[\s　]*([^\n\r]{10,100})/i,
      /(本社|本社所在地|所在地)[：:]\s*([^\n\r]{10,100})/i,
      /(住所|所在地)[：:]\s*([^\n\r]{10,100})/i,
    ];
    for (const pattern of addressPatterns) {
      const match = text.match(pattern);
      if (match) {
        const address = match[2] || match[1];
        if (address && address.length > 5 && address.length < 200) {
          info.headquartersAddress = address.trim();
          break;
        }
      }
    }
  }
  
  // 都道府県の抽出
  if (!info.prefecture) {
    const prefectureMatch = text.match(/(東京都|北海道|(?:大阪|京都|神奈川|埼玉|千葉|兵庫|福岡|愛知|静岡|宮城|新潟|長野|広島|福島|群馬|栃木|茨城|岐阜|山梨|愛媛|熊本|大分|宮崎|鹿児島|沖縄|青森|岩手|秋田|山形|石川|富山|福井|滋賀|三重|和歌山|鳥取|島根|岡山|山口|徳島|香川|高知|佐賀|長崎|奈良)[県府])/);
    if (prefectureMatch) {
      info.prefecture = prefectureMatch[1];
    }
  }
  
  // 市区町村の抽出
  if (!info.city) {
    const cityMatch = text.match(/(?:東京都|北海道|(?:大阪|京都|神奈川|埼玉|千葉|兵庫|福岡|愛知|静岡|宮城|新潟|長野|広島|福島|群馬|栃木|茨城|岐阜|山梨|愛媛|熊本|大分|宮崎|鹿児島|沖縄|青森|岩手|秋田|山形|石川|富山|福井|滋賀|三重|和歌山|鳥取|島根|岡山|山口|徳島|香川|高知|佐賀|長崎|奈良)[県府])[\s　]*([^\s　]{2,20}[市区町村])/);
    if (cityMatch) {
      info.city = cityMatch[1];
    }
  }

  // 資本金（単位変換: 万円→千円、百万円→千円）
  const capitalPatterns = [
    /資本金[：:]\s*([\d,]+)\s*(億円|百万円|万円|千円|円)/i,
    /資本金[：:]\s*([\d,]+)\s*(億|百万|万|千)/i,
  ];
  for (const pattern of capitalPatterns) {
    const match = text.match(pattern);
    if (match) {
      const value = parseFloat(match[1].replace(/,/g, ""));
      if (!isNaN(value)) {
        info.capital = normalizeToThousandYen(value, match[2] || "");
        break;
      }
    }
  }

  // 売上高（単位変換、複数パターン対応）
  const revenuePatterns = [
    /(売上高|売上|営業収益|経常収益)[：:]\s*([\d,]+)\s*(億円|百万円|万円|千円|円)/i,
    /(売上高|売上|営業収益|経常収益)[：:]\s*([\d,]+)\s*(億|百万|万|千)/i,
  ];
  for (const pattern of revenuePatterns) {
    const match = text.match(pattern);
    if (match) {
      const value = parseFloat(match[2].replace(/,/g, ""));
      if (!isNaN(value)) {
        info.revenue = normalizeToThousandYen(value, match[3] || "");
        info.latestRevenue = info.revenue; // 最新年度として設定
        break;
      }
    }
  }

  // 利益（営業利益/経常利益/当期純利益を区別）
  const operatingProfitPatterns = [
    /(営業利益)[：:]\s*([\d,]+)\s*(億円|百万円|万円|千円|円)/i,
    /(営業利益)[：:]\s*([\d,]+)\s*(億|百万|万|千)/i,
  ];
  for (const pattern of operatingProfitPatterns) {
    const match = text.match(pattern);
    if (match) {
      const value = parseFloat(match[2].replace(/,/g, ""));
      if (!isNaN(value)) {
        info.operatingIncome = normalizeToThousandYen(value, match[3] || "");
        break;
      }
    }
  }

  const ordinaryProfitPatterns = [
    /(経常利益)[：:]\s*([\d,]+)\s*(億円|百万円|万円|千円|円)/i,
    /(経常利益)[：:]\s*([\d,]+)\s*(億|百万|万|千)/i,
  ];
  for (const pattern of ordinaryProfitPatterns) {
    const match = text.match(pattern);
    if (match) {
      const value = parseFloat(match[2].replace(/,/g, ""));
      if (!isNaN(value)) {
        info.profit = normalizeToThousandYen(value, match[3] || "");
        info.latestProfit = info.profit;
        break;
      }
    }
  }

  // 当期純利益（最優先で利益として設定）
  const netProfitPatterns = [
    /(当期純利益|純利益)[：:]\s*([\d,]+)\s*(億円|百万円|万円|千円|円)/i,
    /(当期純利益|純利益)[：:]\s*([\d,]+)\s*(億|百万|万|千)/i,
  ];
  for (const pattern of netProfitPatterns) {
    const match = text.match(pattern);
    if (match) {
      const value = parseFloat(match[2].replace(/,/g, ""));
      if (!isNaN(value)) {
        info.profit = normalizeToThousandYen(value, match[3] || "");
        info.latestProfit = info.profit;
        break;
      }
    }
  }

  // 総資産（単位変換）
  const totalAssetsPatterns = [
    /(総資産|資産合計|資産総額)[：:]\s*([\d,]+)\s*(億円|百万円|万円|千円|円)/i,
    /(総資産|資産合計|資産総額)[：:]\s*([\d,]+)\s*(億|百万|万|千)/i,
  ];
  for (const pattern of totalAssetsPatterns) {
    const match = text.match(pattern);
    if (match) {
      const value = parseFloat(match[2].replace(/,/g, ""));
      if (!isNaN(value)) {
        info.totalAssets = normalizeToThousandYen(value, match[3] || "");
        break;
      }
    }
  }

  // 総負債（単位変換）
  const totalLiabilitiesPatterns = [
    /(総負債|負債合計|負債総額)[：:]\s*([\d,]+)\s*(億円|百万円|万円|千円|円)/i,
    /(総負債|負債合計|負債総額)[：:]\s*([\d,]+)\s*(億|百万|万|千)/i,
  ];
  for (const pattern of totalLiabilitiesPatterns) {
    const match = text.match(pattern);
    if (match) {
      const value = parseFloat(match[2].replace(/,/g, ""));
      if (!isNaN(value)) {
        info.totalLiabilities = normalizeToThousandYen(value, match[3] || "");
        break;
      }
    }
  }

  // 純資産（単位変換）
  const netAssetsPatterns = [
    /(純資産|自己資本|株主持分)[：:]\s*([\d,]+)\s*(億円|百万円|万円|千円|円)/i,
    /(純資産|自己資本|株主持分)[：:]\s*([\d,]+)\s*(億|百万|万|千)/i,
  ];
  for (const pattern of netAssetsPatterns) {
    const match = text.match(pattern);
    if (match) {
      const value = parseFloat(match[2].replace(/,/g, ""));
      if (!isNaN(value)) {
        info.netAssets = normalizeToThousandYen(value, match[3] || "");
        break;
      }
    }
  }

  // 業種（複数パターンで抽出）
  if (!info.industry) {
    const industryMatch = text.match(/業種[：:]\s*([^\n\r]+)/i);
    if (industryMatch) {
      info.industry = industryMatch[1].trim();
    }
  }
  // 業種の別パターン
  if (!info.industry) {
    const industryPatterns = [
      /事業内容[：:]\s*([^\n\r]{10,100})/i,
      /主な事業[：:]\s*([^\n\r]{10,100})/i,
      /事業領域[：:]\s*([^\n\r]{10,100})/i,
    ];
    for (const pattern of industryPatterns) {
      const match = text.match(pattern);
      if (match) {
        info.industry = match[1].trim();
        break;
      }
    }
  }

  // 免許/事業者登録
  const licensePatterns = [
    /(建設業許可|宅地建物取引業|古物商|旅行業|食品衛生法|薬局|運送業)[：:]\s*([^\n\r]+)/gi,
  ];
  info.licenses = extractListItems(text, licensePatterns);

  // 取引先銀行
  const bankPatterns = [
    /(取引銀行|主要取引銀行|メインバンク)[：:]\s*([^\n\r]+)/gi,
  ];
  info.banks = extractListItems(text, bankPatterns);

  // 企業説明・概要
  const descriptionMatch = text.match(
    /(企業概要|会社概要|事業内容)[：:]\s*([^\n\r]{50,500})/i
  );
  if (descriptionMatch) {
    info.companyDescription = descriptionMatch[2].trim();
    info.companyOverview = descriptionMatch[2].trim();
  } else if (metaDescription) {
    info.companyDescription = metaDescription;
    info.companyOverview = metaDescription;
  } else if (title) {
    info.companyOverview = title;
  }

  // 取締役・代表者（複数パターンで抽出）
  const directorPatterns = [
    /(代表取締役|取締役)[：:]\s*([^\n\r]+)/gi,
    /(社長|CEO|代表)[：:]\s*([^\n\r]+)/gi,
    /(代表者|代表取締役社長)[：:]\s*([^\n\r]+)/gi,
    /(代表取締役|取締役社長)[：:]\s*([^\n\r]+)/gi,
  ];
  info.directors = extractListItems(text, directorPatterns);
  
  // 代表者名を個別に抽出（最初の代表者を優先）
  if (!info.representative && info.directors && info.directors.length > 0) {
    const firstDirector = info.directors[0];
    // 「代表取締役 山田太郎」のような形式から名前を抽出
    const nameMatch = firstDirector.match(/(?:代表取締役|取締役|社長|CEO|代表)[\s　]*([^\s　]+[\s　]+[^\s　]+)/);
    if (nameMatch) {
      info.representative = nameMatch[1].trim();
      info.representativeName = nameMatch[1].trim();
    } else {
      // そのまま使用
      info.representative = firstDirector.trim();
      info.representativeName = firstDirector.trim();
    }
  }
  
  // 代表者名の別パターン
  if (!info.representative) {
    const repPatterns = [
      /代表者[：:]\s*([^\n\r]+)/i,
      /代表取締役[：:]\s*([^\n\r]+)/i,
      /社長[：:]\s*([^\n\r]+)/i,
      /CEO[：:]\s*([^\n\r]+)/i,
    ];
    for (const pattern of repPatterns) {
      const match = text.match(pattern);
      if (match) {
        const repName = match[1].trim();
        // 役職名を除去
        const cleanName = repName.replace(/^(代表取締役|取締役|社長|CEO|代表)[\s　]*/, "").trim();
        if (cleanName && cleanName.length > 1) {
          info.representative = cleanName;
          info.representativeName = cleanName;
          break;
        }
      }
    }
  }

  // 社員数
  const employeePattern = /(従業員数|社員数|従業員)[：:]\s*([\d,]+)\s*(人|名)/i;
  const employeeMatch = extractNumber(text, employeePattern);
  if (employeeMatch) {
    info.employeeCount = employeeMatch;
  }

  // オフィス数・工場数・店舗数
  const officePattern = /(オフィス|事業所)[：:]\s*([\d,]+)\s*(箇所|ヶ所|件)/i;
  const officeMatch = extractNumber(text, officePattern);
  if (officeMatch) {
    info.officeCount = officeMatch;
  }

  const factoryPattern = /(工場|製造所)[：:]\s*([\d,]+)\s*(箇所|ヶ所|件)/i;
  const factoryMatch = extractNumber(text, factoryPattern);
  if (factoryMatch) {
    info.factoryCount = factoryMatch;
  }

  const storePattern = /(店舗|ショップ)[：:]\s*([\d,]+)\s*(箇所|ヶ所|件)/i;
  const storeMatch = extractNumber(text, storePattern);
  if (storeMatch) {
    info.storeCount = storeMatch;
  }

  // メールアドレス
  const emailMatch = text.match(
    /([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/i
  );
  if (emailMatch) {
    info.contactEmail = emailMatch[1];
    info.email = emailMatch[1];
  }

  // 電話番号
  const phoneMatch = text.match(/(電話|TEL|Tel)[：:]\s*([0-9-()]+)/i);
  if (phoneMatch) {
    info.contactPhone = phoneMatch[2].replace(/[^\d-]/g, "");
    info.phoneNumber = phoneMatch[2].replace(/[^\d-]/g, "");
  } else {
    const telMatch = text.match(/(0\d{1,4}-\d{1,4}-\d{3,4})/);
    if (telMatch) {
      info.contactPhone = telMatch[1];
      info.phoneNumber = telMatch[1];
    }
  }

  // 決算月・決算期（複数パターン）
  if (!info.fiscalMonth && !info.settlementMonth) {
    const fiscalPatterns = [
      /決算期[：:]\s*(\d{1,2})月/i,
      /決算月[：:]\s*(\d{1,2})月/i,
      /(?:3|6|9|12)月決算/i,
      /(?:年1回|年2回|四半期)[\s　]*決算[：:]\s*(\d{1,2})月/i,
    ];
    for (const pattern of fiscalPatterns) {
      const match = text.match(pattern);
      if (match) {
        const month = match[1] || (text.includes("3月決算") ? "3" : text.includes("6月決算") ? "6" : text.includes("9月決算") ? "9" : text.includes("12月決算") ? "12" : null);
        if (month) {
          info.fiscalMonth = `${month}月`;
          info.settlementMonth = `${month}月`;
          break;
        }
      }
    }
  }
  
  // 設立年月日
  if (!info.establishedDate && !info.establishmentDate) {
    const establishedPatterns = [
      /設立[：:]\s*(\d{4})年(\d{1,2})月(\d{1,2})日/i,
      /設立[：:]\s*(\d{4})\/(\d{1,2})\/(\d{1,2})/i,
      /設立[：:]\s*(\d{4})年(\d{1,2})月/i,
      /設立[：:]\s*(\d{4})\/(\d{1,2})/i,
      /設立年月日[：:]\s*(\d{4})年(\d{1,2})月(\d{1,2})日/i,
    ];
    for (const pattern of establishedPatterns) {
      const match = text.match(pattern);
      if (match) {
        const year = match[1];
        const month = match[2]?.padStart(2, "0") || "01";
        const day = match[3]?.padStart(2, "0") || "01";
        const dateStr = `${year}-${month}-${day}`;
        info.establishedDate = dateStr;
        info.establishmentDate = dateStr;
        break;
      }
    }
  }
  
  // 法人番号
  if (!info.corporateNumber) {
    const corporateNumberMatch = text.match(/法人番号[：:]\s*(\d{13})/i);
    if (corporateNumberMatch) {
      info.corporateNumber = corporateNumberMatch[1];
    }
  }
  
  // FAX
  const faxMatch = text.match(/(FAX|Fax|fax)[：:]\s*([0-9-()]+)/i);
  if (faxMatch) {
    info.fax = faxMatch[2].replace(/[^\d-]/g, "");
  }

  // SNS
  const snsPatterns = [
    /https?:\/\/(www\.)?(twitter\.com|x\.com)\/[A-Za-z0-9_]+/gi,
    /https?:\/\/(www\.)?facebook\.com\/[^"'\s]+/gi,
    /https?:\/\/(www\.)?instagram\.com\/[^"'\s]+/gi,
    /https?:\/\/(www\.)?linkedin\.com\/company\/[^"'\s]+/gi,
    /https?:\/\/(www\.)?wantedly\.com\/[^"'\s]+/gi,
    /https?:\/\/(www\.)?youtrust\.jp\/[^"'\s]+/gi,
  ];
  for (const re of snsPatterns) {
    const m = bodyHtml.match(re);
    if (m) {
      info.sns.push(...m);
    }
  }
  info.sns = Array.from(new Set(info.sns));

  // 決算月/決算期（既に抽出済みの場合はスキップ）
  if (!info.settlementMonth && !info.fiscalMonth) {
    const settlementMonthMatch = text.match(/決算月[：:]\s*(\d{1,2})月/i);
    if (settlementMonthMatch) {
      info.settlementMonth = `${settlementMonthMatch[1]}月`;
      info.fiscalMonth = `${settlementMonthMatch[1]}月`;
    }
  } else {
    const settlementPeriodMatch = text.match(/決算期[：:]\s*(\d{1,2})月/i);
    if (settlementPeriodMatch) {
      info.settlementMonth = `${settlementPeriodMatch[1]}月`;
      info.fiscalMonth = `${settlementPeriodMatch[1]}月`;
    } else {
      const fiscalYearMatch = text.match(/事業年度[：:]\s*(\d{4})年(\d{1,2})月/i);
      if (fiscalYearMatch) {
        info.settlementMonth = `${fiscalYearMatch[2]}月`;
        info.fiscalMonth = `${fiscalYearMatch[2]}月`;
        info.latestFiscalYearMonth = `${fiscalYearMatch[1]}年${fiscalYearMatch[2]}月`;
      }
    }
  }

  // 代表者
  const repMatch = text.match(/(代表取締役|代表者|社長)[：:]\s*([^\n\r]+)/i);
  if (repMatch) {
    info.representative = repMatch[2].trim();
    info.representativeName = repMatch[2].trim();
  }

  // 代表者役職
  const repTitleMatch = text.match(/(代表取締役|代表者|社長|CEO|COO|CFO)/i);
  if (repTitleMatch) {
    info.representativeTitle = repTitleMatch[1];
  }

  // 代表者カナ
  const repKanaMatch = text.match(/(代表取締役|代表者)[（(]([ァ-ヶー]+)[）)]/i);
  if (repKanaMatch) {
    info.representativeKana = repKanaMatch[2];
  }

  // 代表者住所
  const repAddressMatch = text.match(
    /(代表者住所|代表取締役住所)[：:]\s*([^\n\r]+)/i
  );
  if (repAddressMatch) {
    info.representativeAddress = repAddressMatch[2].trim();
  }

  // 代表者出身校
  const repSchoolMatch = text.match(/(出身校|学歴)[：:]\s*([^\n\r]+)/i);
  if (repSchoolMatch) {
    info.representativeSchool = repSchoolMatch[2].trim();
  }

  // 代表者生年月日
  const birthMatch = text.match(
    /(生年月日|誕生日)[：:]\s*(\d{4}[年/-]\d{1,2}[月/-]\d{1,2})/i
  );
  if (birthMatch) {
    info.representativeBirthDate = birthMatch[2];
  }

  // 役員名
  const officerPatterns = [
    /(役員|取締役|監査役)[：:]\s*([^\n\r]+)/gi,
  ];
  info.officers = extractListItems(text, officerPatterns);

  // 株主
  const shareholderPatterns = [
    /(主要株主|株主)[：:]\s*([^\n\r]+)/gi,
  ];
  info.shareholders = extractListItems(text, shareholderPatterns);

  // 自己資本比率
  const equityRatioPattern =
    /(自己資本比率|Equity Ratio)[：:]\s*([\d.]+)\s*%/i;
  const equityRatioMatch = extractNumber(text, equityRatioPattern);
  if (equityRatioMatch) {
    info.equityRatio = equityRatioMatch;
  }

  // 上場区分（listing）
  if (info.listingStatus) {
    info.listing = info.listingStatus;
  }

  // companyUrl
  if (info.website) {
    info.companyUrl = info.website;
  }

  return info;
}

/**
 * Gemini でHTMLテキストから構造化抽出
 */
async function extractWithGemini(
  pageText: string,
  url: string,
  companyName: string
): Promise<Partial<any> | null> {
  if (!genAI) return null;

  const model = genAI.getGenerativeModel({ model: GEMINI_MODEL });

  const clipped = pageText.slice(0, GEMINI_MAX_CHARS);

  const prompt = `
あなたは日本企業のWebページから企業情報を抽出してJSONで返すエージェントです。
以下のページ本文はノイズも含みます。会社名に最も合致する情報のみを抽出してください。

# 会社名
${companyName}

# URL
${url}

# 本文
${clipped}

# 指示
- JSONのみを返してください（コードフェンス不要）。
- 不明な項目は null または 空配列。
- 金額はできるだけ「千円」単位に正規化してください。
  - 例: 1億円 -> 100000（千円）
  - 例: 500万円 -> 5000（千円）
- 抽出対象キー:
{
  "listingStatus": string|null,
  "securitiesCode": string|null,
  "website": string|null,
  "contactFormUrl": string|null,
  "capital": number|null,
  "revenue": number|null,
  "latestRevenue": number|null,
  "profit": number|null,
  "latestProfit": number|null,
  "operatingIncome": number|null,
  "totalAssets": number|null,
  "totalLiabilities": number|null,
  "netAssets": number|null,
  "industry": string|null,
  "licenses": string[],
  "banks": string[],
  "companyDescription": string|null,
  "companyOverview": string|null,
  "directors": string[],
  "employeeCount": number|null,
  "officeCount": number|null,
  "factoryCount": number|null,
  "storeCount": number|null,
  "contactEmail": string|null,
  "contactPhone": string|null,
  "fax": string|null,
  "sns": string[],
  "settlementMonth": string|null,
  "fiscalMonth": string|null,
  "latestFiscalYearMonth": string|null,
  "representative": string|null,
  "representativeName": string|null,
  "representativeTitle": string|null,
  "representativeKana": string|null,
  "representativeAddress": string|null,
  "representativeSchool": string|null,
  "representativeBirthDate": string|null,
  "officers": string[],
  "shareholders": string[],
  "equityRatio": number|null,
  "postalCode": string|null,
  "phoneNumber": string|null,
  "email": string|null,
  "companyUrl": string|null,
  "listing": string|null
}
`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), GEMINI_TIMEOUT_MS);

  try {
    const res = await model.generateContent({
      contents: [{ role: "user", parts: [{ text: prompt }] }],
      generationConfig: {
        temperature: 0,
      },
    });

    const text = res.response.text().trim();
    const normalized = text
      .replace(/^```json\s*/i, "")
      .replace(/^```\s*/i, "")
      .replace(/```$/i, "")
      .trim();

    const json = JSON.parse(normalized);

    // 最低限補正
    const out: Partial<any> = {
      ...json,
      sourceUrls: [url],
      website: json.website || null,
    };

    if (Array.isArray(out.sns)) {
      out.sns = Array.from(new Set(out.sns.filter(Boolean)));
    }
    if (Array.isArray(out.licenses)) {
      out.licenses = Array.from(new Set(out.licenses.filter(Boolean)));
    }
    if (Array.isArray(out.banks)) {
      out.banks = Array.from(new Set(out.banks.filter(Boolean)));
    }
    if (Array.isArray(out.directors)) {
      out.directors = Array.from(new Set(out.directors.filter(Boolean)));
    }
    if (Array.isArray(out.officers)) {
      out.officers = Array.from(new Set(out.officers.filter(Boolean)));
    }
    if (Array.isArray(out.shareholders)) {
      out.shareholders = Array.from(new Set(out.shareholders.filter(Boolean)));
    }

    return out;
  } catch (e: any) {
    const errorMessage = e?.message || String(e);
    // モデルが見つからないエラーの場合は詳細なメッセージを表示
    if (errorMessage.includes("not found") || errorMessage.includes("404")) {
      console.warn(`[gemini] モデルエラー for ${url}: ${errorMessage}`);
      console.warn(`[gemini] 使用中のモデル: ${GEMINI_MODEL}`);
      console.warn(`[gemini] 環境変数 GEMINI_MODEL を確認してください（例: gemini-1.5-flash-latest, gemini-pro）`);
    } else {
      console.warn(`[gemini] parse failed for ${url}:`, errorMessage);
    }
    return null;
  } finally {
    clearTimeout(timer);
  }
}

/**
 * 2つのinfoをマージ（base優先、足りない所だけaddonで補完）
 */
function mergeTwoInfo(base: Partial<any>, addon: Partial<any> | null): Partial<any> {
  if (!addon) return base;

  const out: Partial<any> = { ...base };

  const setIfEmpty = (key: string) => {
    const b = (out as any)[key];
    const a = (addon as any)[key];

    const isEmpty =
      b === null ||
      b === undefined ||
      b === "" ||
      (Array.isArray(b) && b.length === 0);

    if (isEmpty && a !== null && a !== undefined && a !== "") {
      (out as any)[key] = a;
    }
  };

  const singleKeys = [
    "listingStatus",
    "securitiesCode",
    "website",
    "companyUrl",
    "contactFormUrl",
    "constructionEvaluation",
    "capital",
    "revenue",
    "latestRevenue",
    "profit",
    "latestProfit",
    "operatingIncome",
    "totalAssets",
    "totalLiabilities",
    "netAssets",
    "industry",
    "companyDescription",
    "companyOverview",
    "employeeCount",
    "officeCount",
    "factoryCount",
    "storeCount",
    "contactEmail",
    "email",
    "contactPhone",
    "phoneNumber",
    "fax",
    "settlementMonth",
    "fiscalMonth",
    "latestFiscalYearMonth",
    "representative",
    "representativeName",
    "representativeTitle",
    "representativeKana",
    "representativeAddress",
    "representativeSchool",
    "representativeBirthDate",
    "equityRatio",
    "postalCode",
    "listing",
  ];

  for (const k of singleKeys) setIfEmpty(k);

  const mergeArray = (key: string) => {
    const b = Array.isArray((out as any)[key]) ? (out as any)[key] : [];
    const a = Array.isArray((addon as any)[key]) ? (addon as any)[key] : [];
    if (a.length === 0) return;
    (out as any)[key] = Array.from(new Set([...b, ...a]));
  };

  for (const k of ["licenses", "banks", "directors", "sns", "officers", "shareholders", "sourceUrls"]) {
    mergeArray(k);
  }

  // sourceUrls は必ずURLを含める
  out.sourceUrls = Array.from(new Set([...(out.sourceUrls || []), ...((addon as any).sourceUrls || [])]));

  return out;
}

/**
 * 複数の情報をマージ
 */
function mergeWebInfo(infos: Partial<any>[]): Partial<any> {
  const merged: Partial<any> = {
    sourceUrls: [],
    licenses: [],
    banks: [],
    directors: [],
    sns: [],
    officers: [],
    shareholders: [],
  };

  for (const info of infos) {
    if (!merged.listingStatus && info.listingStatus) merged.listingStatus = info.listingStatus;
    if (!merged.securitiesCode && info.securitiesCode) merged.securitiesCode = info.securitiesCode;
    if (!merged.website && info.website) merged.website = info.website;
    if (!merged.companyUrl && info.companyUrl) merged.companyUrl = info.companyUrl;
    if (!merged.contactFormUrl && info.contactFormUrl) merged.contactFormUrl = info.contactFormUrl;
    if (!merged.constructionEvaluation && info.constructionEvaluation) merged.constructionEvaluation = info.constructionEvaluation;
    if (!merged.capital && info.capital) merged.capital = info.capital;
    if (!merged.revenue && info.revenue) merged.revenue = info.revenue;
    if (!merged.latestRevenue && info.latestRevenue) merged.latestRevenue = info.latestRevenue;
    if (!merged.profit && info.profit) merged.profit = info.profit;
    if (!merged.latestProfit && info.latestProfit) merged.latestProfit = info.latestProfit;
    if (!merged.operatingIncome && info.operatingIncome) merged.operatingIncome = info.operatingIncome;
    if (!merged.totalAssets && info.totalAssets) merged.totalAssets = info.totalAssets;
    if (!merged.totalLiabilities && info.totalLiabilities) merged.totalLiabilities = info.totalLiabilities;
    if (!merged.netAssets && info.netAssets) merged.netAssets = info.netAssets;
    if (!merged.industry && info.industry) merged.industry = info.industry;
    if (!merged.companyDescription && info.companyDescription) merged.companyDescription = info.companyDescription;
    if (!merged.companyOverview && info.companyOverview) merged.companyOverview = info.companyOverview;
    if (!merged.employeeCount && info.employeeCount) merged.employeeCount = info.employeeCount;
    if (!merged.officeCount && info.officeCount) merged.officeCount = info.officeCount;
    if (!merged.factoryCount && info.factoryCount) merged.factoryCount = info.factoryCount;
    if (!merged.storeCount && info.storeCount) merged.storeCount = info.storeCount;
    if (!merged.contactEmail && info.contactEmail) merged.contactEmail = info.contactEmail;
    if (!merged.email && info.email) merged.email = info.email;
    if (!merged.contactPhone && info.contactPhone) merged.contactPhone = info.contactPhone;
    if (!merged.phoneNumber && info.phoneNumber) merged.phoneNumber = info.phoneNumber;
    if (!merged.fax && info.fax) merged.fax = info.fax;
    if (!merged.settlementMonth && info.settlementMonth) merged.settlementMonth = info.settlementMonth;
    if (!merged.fiscalMonth && info.fiscalMonth) merged.fiscalMonth = info.fiscalMonth;
    if (!merged.latestFiscalYearMonth && info.latestFiscalYearMonth) merged.latestFiscalYearMonth = info.latestFiscalYearMonth;
    if (!merged.representative && info.representative) merged.representative = info.representative;
    if (!merged.representativeName && info.representativeName) merged.representativeName = info.representativeName;
    if (!merged.representativeTitle && info.representativeTitle) merged.representativeTitle = info.representativeTitle;
    if (!merged.representativeKana && info.representativeKana) merged.representativeKana = info.representativeKana;
    if (!merged.representativeAddress && info.representativeAddress) merged.representativeAddress = info.representativeAddress;
    if (!merged.representativeSchool && info.representativeSchool) merged.representativeSchool = info.representativeSchool;
    if (!merged.representativeBirthDate && info.representativeBirthDate) merged.representativeBirthDate = info.representativeBirthDate;
    if (!merged.equityRatio && info.equityRatio) merged.equityRatio = info.equityRatio;
    if (!merged.postalCode && info.postalCode) merged.postalCode = info.postalCode;
    if (!merged.listing && info.listing) merged.listing = info.listing;

    if (info.licenses) merged.licenses = [...new Set([...(merged.licenses || []), ...info.licenses])];
    if (info.banks) merged.banks = [...new Set([...(merged.banks || []), ...info.banks])];
    if (info.directors) merged.directors = [...new Set([...(merged.directors || []), ...info.directors])];
    if (info.sns) merged.sns = [...new Set([...(merged.sns || []), ...info.sns])];
    if (info.officers) merged.officers = [...new Set([...(merged.officers || []), ...info.officers])];
    if (info.shareholders) merged.shareholders = [...new Set([...(merged.shareholders || []), ...info.shareholders])];
    if (info.sourceUrls) merged.sourceUrls = [...new Set([...(merged.sourceUrls || []), ...info.sourceUrls])];
  }

  return merged;
}

/**
 * 検索結果ページから企業詳細ページのURLを抽出
 */
async function extractDetailUrlsFromSearchPage(
  searchUrl: string,
  companyName: string
): Promise<string[]> {
  const detailUrls: string[] = [];
  
  try {
    const res = await fetchWithRetry(searchUrl, {}, 1, 2000);
    if (!res || !res.ok) return detailUrls;

    const html = await res.text();
    const $ = cheerio.load(html);
    const urlLower = searchUrl.toLowerCase();

    // 各サービスの検索結果ページから企業詳細ページのURLを抽出（強化版）
    if (urlLower.includes("green-japan.com")) {
      // Green: 検索結果から企業ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/companies/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#") && !href.includes("?page=")) {
          const fullUrl = href.startsWith("http") ? href : `https://www.green-japan.com${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // データ属性からも取得を試みる
      $('[data-company-url], [data-href*="/company/"], [data-url*="/company/"]').each((_, el) => {
        const dataUrl = $(el).attr("data-company-url") || $(el).attr("data-href") || $(el).attr("data-url");
        if (dataUrl && !dataUrl.includes("/search")) {
          const fullUrl = dataUrl.startsWith("http") ? dataUrl : `https://www.green-japan.com${dataUrl}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // カード要素からも取得
      $('.company-card, .company-item, [data-company-id]').each((_, el) => {
        const cardLink = $(el).find('a[href*="/company/"]').attr("href");
        if (cardLink) {
          const fullUrl = cardLink.startsWith("http") ? cardLink : `https://www.green-japan.com${cardLink}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("wantedly.com")) {
      // Wantedly: 企業ページのリンクを抽出
      $('a[href*="/companies/"], a[href*="/company/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#") && !href.includes("/users/") && !href.includes("?page=")) {
          const fullUrl = href.startsWith("http") ? href : `https://www.wantedly.com${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // カード要素からも取得
      $('[data-company-id], [data-company-slug], .company-card, .company-item').each((_, el) => {
        const companyId = $(el).attr("data-company-id");
        const companySlug = $(el).attr("data-company-slug");
        if (companySlug) {
          const fullUrl = `https://www.wantedly.com/companies/${companySlug}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        } else {
          const cardLink = $(el).find('a[href*="/companies/"]').attr("href");
          if (cardLink && !cardLink.includes("/search")) {
            const fullUrl = cardLink.startsWith("http") ? cardLink : `https://www.wantedly.com${cardLink}`;
            if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
          }
        }
      });
    } else if (urlLower.includes("fukuigyo-cloud.com")) {
      // 複業クラウド: 企業ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/companies/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#") && !href.includes("?page=")) {
          const fullUrl = href.startsWith("http") ? href : `https://fukuigyo-cloud.com${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // カード要素からも取得
      $('.company-card, .company-item, [data-company-id]').each((_, el) => {
        const cardLink = $(el).find('a[href*="/company/"]').attr("href");
        if (cardLink) {
          const fullUrl = cardLink.startsWith("http") ? cardLink : `https://fukuigyo-cloud.com${cardLink}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("talentio.com")) {
      // Talentio: 企業ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/companies/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#") && !href.includes("?page=")) {
          const fullUrl = href.startsWith("http") ? href : `https://talentio.com${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // カード要素からも取得
      $('.company-card, .company-item, [data-company-id]').each((_, el) => {
        const cardLink = $(el).find('a[href*="/company/"]').attr("href");
        if (cardLink) {
          const fullUrl = cardLink.startsWith("http") ? cardLink : `https://talentio.com${cardLink}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("herp.careers")) {
      // HERP: 企業ページのリンクを抽出
      $('a[href*="/companies/"], a[href*="/company/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#") && !href.includes("?page=")) {
          const fullUrl = href.startsWith("http") ? href : `https://herp.careers${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // カード要素からも取得
      $('.company-card, .company-item, [data-company-id]').each((_, el) => {
        const cardLink = $(el).find('a[href*="/company/"]').attr("href");
        if (cardLink) {
          const fullUrl = cardLink.startsWith("http") ? cardLink : `https://herp.careers${cardLink}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("offers.jp")) {
      // Offers: 企業ページのリンクを抽出
      $('a[href*="/companies/"], a[href*="/company/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#") && !href.includes("?page=")) {
          const fullUrl = href.startsWith("http") ? href : `https://offers.jp${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // カード要素からも取得
      $('.company-card, .company-item, [data-company-id]').each((_, el) => {
        const cardLink = $(el).find('a[href*="/company/"]').attr("href");
        if (cardLink) {
          const fullUrl = cardLink.startsWith("http") ? cardLink : `https://offers.jp${cardLink}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("doocyjob.com")) {
      // doocyjob: 企業ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/companies/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#") && !href.includes("?page=")) {
          const fullUrl = href.startsWith("http") ? href : `https://doocyjob.com${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // カード要素からも取得
      $('.company-card, .company-item, [data-company-id]').each((_, el) => {
        const cardLink = $(el).find('a[href*="/company/"]').attr("href");
        if (cardLink) {
          const fullUrl = cardLink.startsWith("http") ? cardLink : `https://doocyjob.com${cardLink}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("doda.jp")) {
      // doda: 企業ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/Company/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#") && !href.includes("?page=")) {
          const fullUrl = href.startsWith("http") ? href : `https://doda.jp${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // テーブル行からも取得
      $('tr, .company-row').each((_, el) => {
        const rowLink = $(el).find('a[href*="/company/"]').attr("href");
        if (rowLink) {
          const fullUrl = rowLink.startsWith("http") ? rowLink : `https://doda.jp${rowLink}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("workship.jp")) {
      // workship: 企業ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/companies/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#") && !href.includes("?page=")) {
          const fullUrl = href.startsWith("http") ? href : `https://workship.jp${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // カード要素からも取得
      $('.company-card, .company-item, [data-company-id]').each((_, el) => {
        const cardLink = $(el).find('a[href*="/company/"]').attr("href");
        if (cardLink) {
          const fullUrl = cardLink.startsWith("http") ? cardLink : `https://workship.jp${cardLink}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("type.jp")) {
      // type転職: 企業ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/companies/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#") && !href.includes("?page=")) {
          const fullUrl = href.startsWith("http") ? href : `https://type.jp${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // カード要素からも取得
      $('.company-card, .company-item, [data-company-id]').each((_, el) => {
        const cardLink = $(el).find('a[href*="/company/"]').attr("href");
        if (cardLink) {
          const fullUrl = cardLink.startsWith("http") ? cardLink : `https://type.jp${cardLink}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("mynavi.jp") || urlLower.includes("job.mynavi.jp")) {
      // マイナビ転職/マイナビ2026: 企業詳細ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/corp/"], a[href*="/detail/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : new URL(href, searchUrl).href;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("houjin.jp")) {
      // 全国法人リスト: 企業詳細ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/detail/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : new URL(href, searchUrl).href;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("alarmbox.jp")) {
      // Alarmbox: 企業詳細ページのリンクを抽出
      $('a[href*="/companyinfo/"], a[href*="/company/"], a[href*="/detail/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : new URL(href, searchUrl).href;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("g-search.or.jp") || urlLower.includes("cnavi-app")) {
      // 企業INDEXナビ: 企業詳細ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/detail/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : new URL(href, searchUrl).href;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("buffett-code.com")) {
      // バフェットコード: 企業詳細ページのリンクを抽出
      $('a[href*="/company/"], a[href*="/detail/"], a[href*="/screening/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : new URL(href, searchUrl).href;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("catr.jp")) {
      // 決算公告サイト: 検索結果から詳細ページのリンクを抽出
      $('a[href*="/detail/"], a[href*="/company/"], a[href*="/kessan/"], a[href*="/announcement/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : `https://catr.jp${href}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
      // テーブル行からも取得
      $('tr, .result-row').each((_, el) => {
        const rowLink = $(el).find('a[href*="/detail/"]').attr("href");
        if (rowLink) {
          const fullUrl = rowLink.startsWith("http") ? rowLink : `https://catr.jp${rowLink}`;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else {
      // その他のサイト: 一般的な企業ページのリンクパターンを抽出
      $('a[href*="/company/"], a[href*="/companies/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          try {
            const urlObj = new URL(href, searchUrl);
            const fullUrl = urlObj.href;
            // 会社名がURLに含まれているか、企業ページっぽいURLのみ
            if (fullUrl.toLowerCase().includes(companyName.toLowerCase().substring(0, 3)) || 
                fullUrl.match(/\/company\/|\/companies\/|\/corp\//i)) {
              if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
            }
          } catch {}
        }
      });
    }

    // 会社名でフィルタリング（より関連性の高いURLのみ）
    const filteredUrls = detailUrls.filter(url => {
      const urlLower = url.toLowerCase();
      const nameLower = companyName.toLowerCase();
      // 会社名の一部がURLに含まれているか、企業ページのパターンに一致
      return urlLower.includes(nameLower.substring(0, Math.min(5, nameLower.length))) ||
             urlLower.match(/\/company\/|\/companies\/|\/corp\//i);
    });

    // 最大10件まで（検索結果の上位のみ）
    return filteredUrls.length > 0 ? filteredUrls.slice(0, 10) : detailUrls.slice(0, 10);
  } catch (e) {
    console.warn(`[extractDetailUrlsFromSearchPage] ${searchUrl} の処理エラー:`, (e as any)?.message || e);
    return [];
  }
}

/**
 * 代替検索エンジンで企業情報を検索（Google検索の代替）
 */
async function searchWithAlternativeEngines(
  companyName: string,
  address?: string | null,
  corporateNumber?: string | null
): Promise<string[]> {
  const urls: string[] = [];
  const query = encodeURIComponent([companyName, address, corporateNumber].filter(Boolean).join(" "));

  // Bing検索（Google検索の代替）
  try {
    const bingUrl = `https://www.bing.com/search?q=${query}`;
    const res = await fetchWithRetry(bingUrl, {}, 1, 2000);
    if (res && res.ok) {
      const html = await res.text();
      const $ = cheerio.load(html);
      
      $('a[href^="http"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("bing.com") && !href.includes("microsoft.com")) {
          try {
            const urlObj = new URL(href);
            // 企業情報サイトや公式サイトっぽいURLのみ
            if (urlObj.hostname.includes(companyName.toLowerCase().substring(0, 3)) ||
                urlObj.hostname.includes("co.jp") ||
                urlObj.pathname.includes("/company") ||
                urlObj.pathname.includes("/corp")) {
              if (!urls.includes(href)) urls.push(href);
            }
          } catch {}
        }
      });
      await sleep(1000);
    }
  } catch (e) {
    console.warn(`[searchWithAlternativeEngines] Bing検索エラー:`, (e as any)?.message || e);
  }

  // Yahoo検索（追加の代替）
  try {
    const yahooUrl = `https://search.yahoo.co.jp/search?p=${query}`;
    const res = await fetchWithRetry(yahooUrl, {}, 1, 2000);
    if (res && res.ok) {
      const html = await res.text();
      const $ = cheerio.load(html);
      
      $('a[href^="http"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("yahoo.co.jp") && !href.includes("search.yahoo")) {
          try {
            const urlObj = new URL(href);
            if (urlObj.hostname.includes(companyName.toLowerCase().substring(0, 3)) ||
                urlObj.hostname.includes("co.jp")) {
              if (!urls.includes(href)) urls.push(href);
            }
          } catch {}
        }
      });
      await sleep(1000);
    }
  } catch (e) {
    console.warn(`[searchWithAlternativeEngines] Yahoo検索エラー:`, (e as any)?.message || e);
  }

  return urls.slice(0, 10); // 最大10件
}

/**
 * 単一URLから情報を取得
 * - ルール抽出 → Gemini補完 → マージ
 */
async function fetchInfoFromUrl(
  url: string,
  companyName: string
): Promise<Partial<any> | null> {
  try {
    const res = await fetchWithRetry(url, {}, 2, 2000);
    if (!res || !res.ok) return null;

    const html = await res.text();

    const ruleInfo = parseDetailedInfoFromHtml(html, url, companyName);

    // Gemini補完（USE_GEMINI=true かつ APIキーがある場合のみ）
    let aiInfo: Partial<any> | null = null;
    if (USE_GEMINI && genAI) {
      const $ = cheerio.load(html);
      const pageText = $("body").text().replace(/\s+/g, " ").trim();
      aiInfo = await extractWithGemini(pageText, url, companyName);
    }

    const merged = mergeTwoInfo(ruleInfo, aiInfo);
    return merged;
  } catch (e) {
    console.warn(`[fetchInfoFromUrl] ${url} の取得エラー:`, (e as any)?.message || e);
    return null;
  }
}

/**
 * FirestoreにまだwebInfoが無い場合に、HPやメディアから情報を取得する
 */
async function fetchWebInfoFromWeb(
  companyId: string,
  companyData: any
): Promise<any> {
  const name: string = companyData.name || "";
  const address: string | null =
    companyData.headquartersAddress || companyData.address || null;
  const corporateNumber: string | null = companyData.corporateNumber || null;

  const defaultResult: any = {
    listingStatus: "",
    securitiesCode: "",
    website: "",
    companyUrl: "",
    contactFormUrl: "",
    constructionEvaluation: "",
    capital: null,
    revenue: null,
    latestRevenue: null,
    profit: null,
    latestProfit: null,
    operatingIncome: null,
    totalAssets: null,
    totalLiabilities: null,
    netAssets: null,
    industry: "",
    licenses: [],
    banks: [],
    companyDescription: "",
    companyOverview: "",
    directors: [],
    employeeCount: null,
    officeCount: null,
    factoryCount: null,
    storeCount: null,
    contactEmail: "",
    email: "",
    contactPhone: "",
    phoneNumber: "",
    fax: "",
    sns: [],
    settlementMonth: "",
    fiscalMonth: "",
    latestFiscalYearMonth: "",
    representative: "",
    representativeName: "",
    representativeTitle: "",
    representativeKana: "",
    representativeAddress: "",
    representativeSchool: "",
    representativeBirthDate: "",
    officers: [],
    shareholders: [],
    equityRatio: null,
    postalCode: "",
    listing: "",
    sourceUrls: [],
    updatedAt: new Date().toISOString(),
    status: "failed",
    errorMessage: "",
  };

  if (!name) {
    return {
      ...defaultResult,
      errorMessage: "会社名が空のためWeb取得をスキップ",
    };
  }

  try {
    console.log(`[fetchWebInfoFromWeb] 開始: ${companyId} / ${name}`);

    const existingHomepage = companyData.hp || companyData.homepageUrl;

    // Google検索非依存: 候補URLを直接生成
    const candidateUrls = generateCandidateUrls(
      name,
      address,
      corporateNumber,
      existingHomepage
    );

    // 代替検索エンジンで追加のURLを取得（Google検索の代替）
    console.log(`[fetchWebInfoFromWeb] 代替検索エンジンで追加検索中...`);
    const alternativeUrls = await searchWithAlternativeEngines(name, address, corporateNumber);
    candidateUrls.push(...alternativeUrls);

    console.log(`[fetchWebInfoFromWeb] 候補URL数: ${candidateUrls.length} (検索エンジン含む)`);

    // 検索結果ページから企業詳細ページのURLを抽出
    const detailUrls: string[] = [];
    for (const searchUrl of candidateUrls) {
      try {
        const extractedUrls = await extractDetailUrlsFromSearchPage(searchUrl, name);
        detailUrls.push(...extractedUrls);
        await sleep(500); // 検索結果ページへのアクセス間隔
      } catch (error) {
        console.warn(`[fetchWebInfoFromWeb] 検索ページ ${searchUrl} の処理エラー:`, (error as any)?.message || error);
        // エラーでも処理継続
      }
    }

    // 既存HPがあれば追加
    if (existingHomepage) {
      detailUrls.push(existingHomepage);
    }

    // 重複除去して最大15件に制限
    const uniqueDetailUrls = Array.from(new Set(detailUrls)).slice(0, 15);
    console.log(`[fetchWebInfoFromWeb] 企業詳細ページURL数: ${uniqueDetailUrls.length}`);

    // 各詳細ページから情報を抽出
    const extractedInfos: Partial<any>[] = [];
    for (const url of uniqueDetailUrls) {
      try {
        const info = await fetchInfoFromUrl(url, name);
        if (info && info.sourceUrls && info.sourceUrls.length > 0) {
          extractedInfos.push(info);
        }
        await sleep(500); // ToS配慮: リクエスト間隔を確保
      } catch (error) {
        console.warn(`[fetchWebInfoFromWeb] URL ${url} の処理エラー:`, (error as any)?.message || error);
        // エラーでも処理継続
      }
    }

    const merged = mergeWebInfo(extractedInfos);

    const webInfo = {
      ...defaultResult,
      ...merged,
      listingStatus: merged.listingStatus || "",
      securitiesCode: merged.securitiesCode || "",
      website: merged.website || "",
      companyUrl: merged.companyUrl || merged.website || "",
      contactFormUrl: merged.contactFormUrl || "",
      constructionEvaluation: merged.constructionEvaluation || "",
      industry: merged.industry || "",
      companyDescription: merged.companyDescription || "",
      companyOverview: merged.companyOverview || "",
      contactEmail: merged.contactEmail || merged.email || "",
      email: merged.email || merged.contactEmail || "",
      contactPhone: merged.contactPhone || merged.phoneNumber || "",
      phoneNumber: merged.phoneNumber || merged.contactPhone || "",
      fax: merged.fax || "",
      settlementMonth: merged.settlementMonth || "",
      fiscalMonth: merged.fiscalMonth || merged.settlementMonth || "",
      latestFiscalYearMonth: merged.latestFiscalYearMonth || "",
      representative: merged.representative || merged.representativeName || "",
      representativeName: merged.representativeName || merged.representative || "",
      representativeTitle: merged.representativeTitle || "",
      representativeKana: merged.representativeKana || "",
      representativeAddress: merged.representativeAddress || "",
      representativeSchool: merged.representativeSchool || "",
      representativeBirthDate: merged.representativeBirthDate || "",
      postalCode: merged.postalCode || "",
      listing: merged.listing || merged.listingStatus || "",
      updatedAt: new Date().toISOString(),
    };

    // 財務情報が1つでも取れたら status を partial 以上に保つ
    const hasFinancialData =
      webInfo.totalAssets !== null ||
      webInfo.netAssets !== null ||
      webInfo.totalLiabilities !== null ||
      webInfo.revenue !== null ||
      webInfo.profit !== null ||
      webInfo.capital !== null;

    const filledFields = Object.values(webInfo).filter(
      (v) =>
        v !== null &&
        v !== undefined &&
        v !== "" &&
        (Array.isArray(v) ? v.length > 0 : true)
    ).length;
    const totalFields = Object.keys(webInfo).length - 3;

    if (filledFields >= totalFields * 0.5) {
      webInfo.status = "success";
    } else if (filledFields > 0 || hasFinancialData) {
      webInfo.status = "partial";
    } else {
      webInfo.status = "failed";
      webInfo.errorMessage = "情報を取得できませんでした";
    }

    // 財務いずれか1項目でも取れたら errorMessage は空にする
    if (hasFinancialData) {
      webInfo.errorMessage = "";
    }

    console.log(
      `[fetchWebInfoFromWeb] 完了: ${companyId} / ステータス: ${webInfo.status} / 取得項目数: ${filledFields}/${totalFields} / 財務データ: ${hasFinancialData ? "あり" : "なし"}`
    );

    return webInfo;
  } catch (e: any) {
    console.error("[fetchWebInfoFromWeb] error", e);
    return {
      ...defaultResult,
      errorMessage: String(e?.message || e),
    };
  }
}

/**
 * CSVエクスポート
 */
async function exportToCsv() {
  try {
    console.log("企業データの取得を開始...（バッチ処理＆ストリーミング出力）");

    const BATCH_SIZE = 5000;

    // companies_newコレクションの全フィールド（DBインポート用に整理）
    const headers = [
      // 📊 基本情報（14フィールド）
      "companyId",
      "name",
      "nameEn",
      "kana",
      "corporateNumber",
      "corporationType",
      "nikkeiCode",
      "badges",
      "tags",
      "createdAt",
      "updatedAt",
      "updateDate",
      "updateCount",
      "changeCount",
      "qualificationGrade",
      // 📍 所在地情報（6フィールド）
      "prefecture",
      "address",
      "headquartersAddress",
      "postalCode",
      "location",
      "departmentLocation",
      // 📞 連絡先情報（6フィールド）
      "phoneNumber",
      "contactPhoneNumber",
      "fax",
      "email",
      "companyUrl",
      "contactFormUrl",
      // 👤 代表者情報（10フィールド）
      "representativeName",
      "representativeKana",
      "representativeTitle",
      "representativeBirthDate",
      "representativePhone",
      "representativePostalCode",
      "representativeHomeAddress",
      "representativeRegisteredAddress",
      "representativeAlmaMater",
      "executives",
      // 👔 役員情報（20フィールド）
      "executiveName1", "executiveName2", "executiveName3", "executiveName4", "executiveName5",
      "executiveName6", "executiveName7", "executiveName8", "executiveName9", "executiveName10",
      "executivePosition1", "executivePosition2", "executivePosition3", "executivePosition4", "executivePosition5",
      "executivePosition6", "executivePosition7", "executivePosition8", "executivePosition9", "executivePosition10",
      // 🏢 業種情報（13フィールド）
      "industry",
      "industryLarge",
      "industryMiddle",
      "industrySmall",
      "industryDetail",
      "industries",
      "industryCategories",
      "businessDescriptions",
      "businessItems",
      "businessSummary",
      "specialties",
      "demandProducts",
      "specialNote",
      // 💰 財務情報（29フィールド）
      "capitalStock",
      "revenue",
      "latestRevenue",
      "latestProfit",
      "revenueFromStatements",
      "operatingIncome",
      "totalAssets",
      "totalLiabilities",
      "netAssets",
      "issuedShares",
      "financials",
      "listing",
      "marketSegment",
      "latestFiscalYearMonth",
      "fiscalMonth",
      "fiscalMonth1", "fiscalMonth2", "fiscalMonth3", "fiscalMonth4", "fiscalMonth5",
      "revenue1", "revenue2", "revenue3", "revenue4", "revenue5",
      "profit1", "profit2", "profit3", "profit4", "profit5",
      // 🏭 企業規模・組織（10フィールド）
      "employeeCount",
      "employeeNumber",
      "factoryCount",
      "officeCount",
      "storeCount",
      "averageAge",
      "averageYearsOfService",
      "averageOvertimeHours",
      "averagePaidLeave",
      "femaleExecutiveRatio",
      // 📅 設立・沿革（5フィールド）
      "established",
      "dateOfEstablishment",
      "founding",
      "foundingYear",
      "acquisition",
      // 🤝 取引先・関係会社（7フィールド）
      "clients",
      "suppliers",
      "subsidiaries",
      "affiliations",
      "shareholders",
      "banks",
      "bankCorporateNumber",
      // 🏢 部署・拠点情報（21フィールド）
      "departmentName1", "departmentName2", "departmentName3", "departmentName4", "departmentName5", "departmentName6", "departmentName7",
      "departmentAddress1", "departmentAddress2", "departmentAddress3", "departmentAddress4", "departmentAddress5", "departmentAddress6", "departmentAddress7",
      "departmentPhone1", "departmentPhone2", "departmentPhone3", "departmentPhone4", "departmentPhone5", "departmentPhone6", "departmentPhone7",
      // 📝 企業説明（4フィールド）
      "overview",
      "companyDescription",
      "businessDescriptions",
      "salesNotes",
      // 🌐 SNS・外部リンク（8フィールド）
      "urls",
      "profileUrl",
      "externalDetailUrl",
      "facebook",
      "linkedin",
      "wantedly",
      "youtrust",
      "metaKeywords",
    ];

    const outputPath = path.join(process.cwd(), "companies_webinfo.csv");
    
    // 既存のCSVから処理済みのcompanyIdを読み込む
    const processedIds = new Set<string>();
    let isResume = false;
    if (fs.existsSync(outputPath)) {
      try {
        const content = fs.readFileSync(outputPath, "utf8");
        const lines = content.split("\n");
        // ヘッダー行をスキップ
        for (let i = 1; i < lines.length; i++) {
          const line = lines[i].trim();
          if (!line) continue;
          // CSVの最初のカラム（companyId）を取得
          const firstComma = line.indexOf(",");
          if (firstComma > 0) {
            const companyId = line.substring(0, firstComma).trim();
            if (companyId && companyId !== "companyId") {
              processedIds.add(companyId);
            }
          }
        }
        if (processedIds.size > 0) {
          isResume = true;
          console.log(`[再開] 既存CSVから ${processedIds.size} 件の処理済み企業を検出しました。続きから処理を再開します。`);
        }
      } catch (error) {
        console.warn(`[再開] 既存CSVの読み込みエラー: ${error}`);
      }
    }
    
    // 追記モードか新規作成モードかを決定
    const writeStream = isResume 
      ? fs.createWriteStream(outputPath, { encoding: "utf8", flags: "a" }) // 追記モード
      : fs.createWriteStream(outputPath, { encoding: "utf8" }); // 新規作成モード

    // 新規作成の場合のみヘッダーを書き込む
    if (!isResume) {
      writeStream.write(headers.map(escapeCsvValue).join(",") + "\n");
    }

    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalRows = 0;

    const formatValue = (value: any): string => {
      if (value === null || value === undefined || value === "") {
        return "";
      }
      if (typeof value === "number") {
        return value.toString();
      }
      if (Array.isArray(value)) {
        return value.length > 0 ? value.join("; ") : "";
      }
      // Firestore Timestamp を ISO 文字列に変換
      if (value && typeof value.toDate === "function") {
        return value.toDate().toISOString();
      }
      // Date オブジェクトを ISO 文字列に変換
      if (value instanceof Date) {
        return value.toISOString();
      }
      return String(value);
    };

    const formatNumber = (value: any): string => {
      if (value === null || value === undefined) {
        return "";
      }
      return value.toString();
    };

    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }

      console.log(`バッチ取得: ${snapshot.size} 件`);

      for (const companyDoc of snapshot.docs) {
        const companyId = companyDoc.id;
        
        // 既に処理済みの場合はスキップ
        if (processedIds.has(companyId)) {
          console.log(`[スキップ] 既に処理済み: ${companyId}`);
          continue;
        }
        
        const companyData = companyDoc.data();

        const webInfoRef = db.collection("companies_webInfo").doc(companyId);
        const webInfoDoc = await webInfoRef.get();
        let webInfoData: any = webInfoDoc.exists ? webInfoDoc.data() : null;

        if (!webInfoData) {
          console.log(
            `[webinfo] Firestoreにデータが無いためWeb取得を実行: ${companyId} / ${companyData.name}`
          );
          webInfoData = await fetchWebInfoFromWeb(companyId, companyData);
          await webInfoRef.set(webInfoData, { merge: true });
        }

        // companies_newの全フィールドを取得（webInfoで補完）
        const getField = (fieldName: string, isNumber: boolean = false): string => {
          // まずcompanies_newから取得
          let value = companyData[fieldName];
          
          // webInfoで補完（companies_newに値がない場合のみ）
          if ((value === null || value === undefined || value === "") && webInfoData) {
            // フィールド名のマッピング
            const webInfoMapping: { [key: string]: string } = {
              "companyUrl": "website",
              "email": "contactEmail",
              "phoneNumber": "contactPhone",
              "contactPhoneNumber": "contactPhone",
              "postalCode": "postalCode",
              "representativeName": "representative",
              "representativeKana": "representativeKana",
              "representativeTitle": "representativeTitle",
              "representativeBirthDate": "representativeBirthDate",
              "representativeHomeAddress": "representativeAddress",
              "representativeAlmaMater": "representativeSchool",
              "capitalStock": "capital",
              "revenue": "revenue",
              "latestRevenue": "latestRevenue",
              "latestProfit": "latestProfit",
              "operatingIncome": "operatingIncome",
              "totalAssets": "totalAssets",
              "totalLiabilities": "totalLiabilities",
              "netAssets": "netAssets",
              "industry": "industry",
              "companyDescription": "companyDescription",
              "overview": "companyOverview",
              "businessDescriptions": "companyDescription",
              "employeeCount": "employeeCount",
              "employeeNumber": "employeeCount",
              "officeCount": "officeCount",
              "factoryCount": "factoryCount",
              "storeCount": "storeCount",
              "fiscalMonth": "fiscalMonth",
              "latestFiscalYearMonth": "latestFiscalYearMonth",
              "listing": "listingStatus",
            };
            
            const webInfoField = webInfoMapping[fieldName];
            if (webInfoField) {
              value = webInfoData[webInfoField];
            }
          }
          
          if (isNumber) {
            return formatNumber(value);
          }
          return formatValue(value);
        };

        // 配列フィールドから個別フィールドを取得（役員情報など）
        const getArrayField = (arr: any[], index: number): string => {
          if (Array.isArray(arr) && arr[index] !== undefined && arr[index] !== null) {
            return formatValue(arr[index]);
          }
          return "";
        };

        // 役員情報を配列から個別フィールドに展開
        const executives = companyData.executives || companyData.officers || webInfoData?.officers || [];
        const executiveNames = Array.isArray(executives) ? executives : [];
        const executivePositions: string[] = []; // 役職情報は別途取得が必要（現状は空配列）
        
        // 部署情報を配列から個別フィールドに展開
        const departments = companyData.departments || [];
        const departmentNames = Array.isArray(departments) ? departments.map((d: any) => d?.name || d) : [];
        const departmentAddresses = Array.isArray(departments) ? departments.map((d: any) => d?.address || "") : [];
        const departmentPhones = Array.isArray(departments) ? departments.map((d: any) => d?.phone || "") : [];

        // 株主情報を配列から文字列に変換
        const shareholders = companyData.shareholders || webInfoData?.shareholders || [];
        const shareholdersStr = Array.isArray(shareholders) ? shareholders.join("; ") : formatValue(shareholders);

        // 銀行情報を配列から文字列に変換
        const banks = companyData.banks || webInfoData?.banks || [];
        const banksStr = Array.isArray(banks) ? banks.join("; ") : formatValue(banks);

        // 取引先情報を配列から文字列に変換
        const suppliers = companyData.suppliers || [];
        const suppliersStr = Array.isArray(suppliers) ? suppliers.join("; ") : formatValue(suppliers);

        // 子会社情報を配列から文字列に変換
        const subsidiaries = companyData.subsidiaries || [];
        const subsidiariesStr = Array.isArray(subsidiaries) ? subsidiaries.join("; ") : formatValue(subsidiaries);

        // 業種情報を配列から文字列に変換
        const industries = companyData.industries || [];
        const industriesStr = Array.isArray(industries) ? industries.join("; ") : formatValue(industries);

        // 事業項目を配列から文字列に変換
        const businessItems = companyData.businessItems || [];
        const businessItemsStr = Array.isArray(businessItems) ? businessItems.join("; ") : formatValue(businessItems);

        // URL情報を配列から文字列に変換
        const urls = companyData.urls || webInfoData?.sourceUrls || [];
        const urlsStr = Array.isArray(urls) ? urls.join("; ") : formatValue(urls);

        // バッジ情報を配列から文字列に変換
        const badges = companyData.badges || [];
        const badgesStr = Array.isArray(badges) ? badges.join("; ") : formatValue(badges);

        // タグ情報を配列から文字列に変換
        const tags = companyData.tags || [];
        const tagsStr = Array.isArray(tags) ? tags.join("; ") : formatValue(tags);

        // SNS情報を配列から個別フィールドに展開
        const sns = webInfoData?.sns || [];
        const snsArray = Array.isArray(sns) ? sns : [];
        const facebook = snsArray.find((url: string) => url.includes("facebook.com")) || "";
        const linkedin = snsArray.find((url: string) => url.includes("linkedin.com")) || "";
        const wantedly = snsArray.find((url: string) => url.includes("wantedly.com")) || "";
        const youtrust = snsArray.find((url: string) => url.includes("youtrust.jp")) || "";

        const row: string[] = [
          // 基本情報（14フィールド）
          companyId,
          getField("name"),
          getField("nameEn"),
          getField("kana"),
          getField("corporateNumber"),
          getField("corporationType"),
          getField("nikkeiCode"),
          badgesStr, // badges (array)
          tagsStr, // tags (array)
          getField("createdAt"),
          getField("updatedAt"),
          getField("updateDate"),
          getField("updateCount", true),
          getField("changeCount", true),
          getField("qualificationGrade"),
          // 所在地情報（6フィールド）
          getField("prefecture"),
          getField("address"),
          getField("headquartersAddress"),
          getField("postalCode"),
          getField("location"),
          getField("departmentLocation"),
          // 連絡先情報（6フィールド）
          getField("phoneNumber"),
          getField("contactPhoneNumber"),
          getField("fax"),
          getField("email"),
          getField("companyUrl"),
          getField("contactFormUrl"),
          // 代表者情報（10フィールド）
          getField("representativeName"),
          getField("representativeKana"),
          getField("representativeTitle"),
          getField("representativeBirthDate"),
          getField("representativePhone"),
          getField("representativePostalCode"),
          getField("representativeHomeAddress"),
          getField("representativeRegisteredAddress"),
          getField("representativeAlmaMater"),
          formatValue(executives), // executives (array)
          // 役員情報（20フィールド）
          getArrayField(executiveNames, 0), getArrayField(executiveNames, 1), getArrayField(executiveNames, 2), getArrayField(executiveNames, 3), getArrayField(executiveNames, 4),
          getArrayField(executiveNames, 5), getArrayField(executiveNames, 6), getArrayField(executiveNames, 7), getArrayField(executiveNames, 8), getArrayField(executiveNames, 9),
          getArrayField(executivePositions, 0), getArrayField(executivePositions, 1), getArrayField(executivePositions, 2), getArrayField(executivePositions, 3), getArrayField(executivePositions, 4),
          getArrayField(executivePositions, 5), getArrayField(executivePositions, 6), getArrayField(executivePositions, 7), getArrayField(executivePositions, 8), getArrayField(executivePositions, 9),
          // 業種情報（13フィールド）
          getField("industry"),
          getField("industryLarge"),
          getField("industryMiddle"),
          getField("industrySmall"),
          getField("industryDetail"),
          industriesStr, // industries (array)
          getField("industryCategories"),
          getField("businessDescriptions"),
          businessItemsStr, // businessItems (array)
          getField("businessSummary"),
          getField("specialties"),
          getField("demandProducts"),
          getField("specialNote"),
          // 財務情報（29フィールド）
          getField("capitalStock", true),
          getField("revenue", true),
          getField("latestRevenue", true),
          getField("latestProfit", true),
          getField("revenueFromStatements", true),
          getField("operatingIncome", true),
          getField("totalAssets", true),
          getField("totalLiabilities", true),
          getField("netAssets", true),
          getField("issuedShares", true),
          getField("financials"),
          getField("listing"),
          getField("marketSegment"),
          getField("latestFiscalYearMonth"),
          getField("fiscalMonth"),
          getField("fiscalMonth1"), getField("fiscalMonth2"), getField("fiscalMonth3"), getField("fiscalMonth4"), getField("fiscalMonth5"),
          getField("revenue1", true), getField("revenue2", true), getField("revenue3", true), getField("revenue4", true), getField("revenue5", true),
          getField("profit1", true), getField("profit2", true), getField("profit3", true), getField("profit4", true), getField("profit5", true),
          // 企業規模・組織（10フィールド）
          getField("employeeCount", true),
          getField("employeeNumber", true),
          getField("factoryCount", true),
          getField("officeCount", true),
          getField("storeCount", true),
          getField("averageAge"),
          getField("averageYearsOfService"),
          getField("averageOvertimeHours"),
          getField("averagePaidLeave"),
          getField("femaleExecutiveRatio"),
          // 設立・沿革（5フィールド）
          getField("established"),
          getField("dateOfEstablishment"),
          getField("founding"),
          getField("foundingYear"),
          getField("acquisition"),
          // 取引先・関係会社（7フィールド）
          getField("clients"),
          suppliersStr, // suppliers (array)
          subsidiariesStr, // subsidiaries (array)
          getField("affiliations"),
          shareholdersStr, // shareholders (array)
          banksStr, // banks (array)
          getField("bankCorporateNumber"),
          // 部署・拠点情報（21フィールド）
          getArrayField(departmentNames, 0), getArrayField(departmentNames, 1), getArrayField(departmentNames, 2), getArrayField(departmentNames, 3), getArrayField(departmentNames, 4), getArrayField(departmentNames, 5), getArrayField(departmentNames, 6),
          getArrayField(departmentAddresses, 0), getArrayField(departmentAddresses, 1), getArrayField(departmentAddresses, 2), getArrayField(departmentAddresses, 3), getArrayField(departmentAddresses, 4), getArrayField(departmentAddresses, 5), getArrayField(departmentAddresses, 6),
          getArrayField(departmentPhones, 0), getArrayField(departmentPhones, 1), getArrayField(departmentPhones, 2), getArrayField(departmentPhones, 3), getArrayField(departmentPhones, 4), getArrayField(departmentPhones, 5), getArrayField(departmentPhones, 6),
          // 企業説明（4フィールド）
          getField("overview"),
          getField("companyDescription"),
          getField("businessDescriptions"),
          getField("salesNotes"),
          // SNS・外部リンク（8フィールド）
          urlsStr, // urls (array)
          getField("profileUrl"),
          getField("externalDetailUrl"),
          formatValue(facebook), // facebook (SNSから抽出)
          formatValue(linkedin), // linkedin (SNSから抽出)
          formatValue(wantedly), // wantedly (SNSから抽出)
          formatValue(youtrust), // youtrust (SNSから抽出)
          getField("metaKeywords"),
        ];

        const line = row.map(escapeCsvValue).join(",");
        writeStream.write(line + "\n");
        totalRows++;
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }

    writeStream.end();
    await new Promise<void>((resolve, reject) => {
      writeStream.on("finish", () => resolve());
      writeStream.on("error", (err) => reject(err));
    });

    console.log(`CSVファイルを出力しました: ${outputPath}`);
    console.log(`総行数: ${totalRows + 1} (ヘッダー含む)`);
  } catch (error) {
    console.error("エクスポートエラー:", error);
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
exportToCsv()
  .then(() => {
    console.log("エクスポート完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
