/* eslint-disable no-console */

/**
 * scripts/fetch_missing_fields.ts
 *
 * ✅ 目的
 * - companies_new コレクション内の企業情報で不足しているフィールドのみをWebスクレイピングで取得
 * - 1ドキュメント単位で不足している項目のみを取得していく
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
    
    if (!serviceAccountPath) {
      console.error("❌ エラー: FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません。");
      process.exit(1);
    }
    
    if (!fs.existsSync(serviceAccountPath)) {
      console.error(`❌ エラー: サービスアカウントキーファイルが存在しません: ${serviceAccountPath}`);
      process.exit(1);
    }
    
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
    process.exit(1);
  }
}

const db = admin.firestore();

// ------------------------------
// Gemini 初期化（オプション）
// ------------------------------
const USE_GEMINI = process.env.USE_GEMINI === "true";
const GEMINI_API_KEY = USE_GEMINI ? (process.env.GEMINI_API_KEY || "") : "";
const GEMINI_MODEL = process.env.GEMINI_MODEL || "gemini-1.5-flash-latest";
const GEMINI_MAX_CHARS = Number(process.env.GEMINI_MAX_CHARS || 12000);
const GEMINI_TIMEOUT_MS = Number(process.env.GEMINI_TIMEOUT_MS || 12000);

const genAI = (USE_GEMINI && GEMINI_API_KEY) ? new GoogleGenerativeAI(GEMINI_API_KEY) : null;

// ------------------------------
// フィールドマッピング
// ------------------------------

/**
 * Webスクレイピングで取得可能なフィールドのマッピング
 * (CompanyWebInfo -> companies_new)
 */
const WEBINFO_TO_COMPANY_FIELD_MAPPING: { [key: string]: string[] } = {
  listingStatus: ["listing"],
  securitiesCode: ["nikkeiCode"],
  website: ["companyUrl"],
  contactFormUrl: ["contactFormUrl"],
  capital: ["capitalStock"],
  revenue: ["revenue", "latestRevenue"],
  profit: ["latestProfit"],
  netAssets: ["netAssets"],
  totalAssets: ["totalAssets"],
  totalLiabilities: ["totalLiabilities"],
  operatingIncome: ["operatingIncome"],
  industry: ["industry"],
  companyDescription: ["companyDescription", "overview", "businessDescriptions"],
  companyOverview: ["overview", "companyDescription"],
  employeeCount: ["employeeCount", "employeeNumber"],
  officeCount: ["officeCount"],
  factoryCount: ["factoryCount"],
  storeCount: ["storeCount"],
  contactEmail: ["email"],
  contactPhone: ["phoneNumber", "contactPhoneNumber"],
  fax: ["fax"],
  settlementMonth: ["fiscalMonth", "latestFiscalYearMonth"],
  representative: ["representativeName"],
  representativeKana: ["representativeKana"],
  representativeAddress: ["representativeHomeAddress", "representativeRegisteredAddress"],
  representativeSchool: ["representativeAlmaMater"],
  representativeBirthDate: ["representativeBirthDate"],
  officers: ["executives"],
  shareholders: ["shareholders"],
  banks: ["banks"],
  sns: ["facebook", "linkedin", "wantedly", "youtrust", "urls"],
};

/**
 * 逆マッピング: companies_newフィールド -> WebInfoフィールド
 */
const COMPANY_TO_WEBINFO_FIELD_MAPPING: { [key: string]: string } = {};
for (const [webInfoField, companyFields] of Object.entries(WEBINFO_TO_COMPANY_FIELD_MAPPING)) {
  for (const companyField of companyFields) {
    if (!COMPANY_TO_WEBINFO_FIELD_MAPPING[companyField]) {
      COMPANY_TO_WEBINFO_FIELD_MAPPING[companyField] = webInfoField;
    }
  }
}

// ------------------------------
// ユーティリティ関数（export_webinfo_to_csv.tsから再利用）
// ------------------------------
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

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

      if (res.status === 429) {
        const retryAfter = res.headers.get("retry-after");
        const waitTime = retryAfter ? parseInt(retryAfter) * 1000 : retryDelay * (attempt + 1);
        console.warn(`[fetchWithRetry] 429 for ${url}, waiting ${waitTime}ms`);
        await sleep(waitTime);
        continue;
      }

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
// 不足フィールドのチェック
// ------------------------------

/**
 * フィールドが空かどうかをチェック
 */
function isEmpty(value: any): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === "string" && value.trim() === "") return true;
  if (Array.isArray(value) && value.length === 0) return true;
  if (typeof value === "object" && Object.keys(value).length === 0) return true;
  return false;
}

/**
 * 不足フィールドを特定し、Webスクレイピングで取得可能なフィールドを返す
 * 注意: フィールドが存在しない場合も、値がnull/undefined/空の場合も「不足」と判定
 */
function getMissingFieldsThatCanBeFetched(
  companyData: any,
  webInfoData: any | null
): {
  missingCompanyFields: string[];
  targetWebInfoFields: Set<string>;
} {
  const missingCompanyFields: string[] = [];
  const targetWebInfoFields = new Set<string>();

  // 各マッピングをチェック
  // フィールドが存在しない場合も、undefinedとして扱い、isEmptyでtrueになる
  for (const [companyField, webInfoField] of Object.entries(COMPANY_TO_WEBINFO_FIELD_MAPPING)) {
    // フィールドが存在しない場合はundefined、存在する場合はその値
    const companyValue = companyData[companyField];
    
    // 値がnull/undefined/空の場合は「不足」と判定
    if (isEmpty(companyValue)) {
      missingCompanyFields.push(companyField);
      
      // webInfoに既にデータがあるかチェック
      if (webInfoData) {
        const webInfoValue = webInfoData[webInfoField];
        if (isEmpty(webInfoValue)) {
          // webInfoにもない場合は、Webスクレイピングで取得が必要
          targetWebInfoFields.add(webInfoField);
        }
      } else {
        // webInfoが存在しない場合は、Webスクレイピングで取得が必要
        targetWebInfoFields.add(webInfoField);
      }
    }
  }

  return {
    missingCompanyFields,
    targetWebInfoFields,
  };
}

// ------------------------------
// Webスクレイピング関数（export_webinfo_to_csv.tsから簡略化）
// ------------------------------

/**
 * URL候補を生成（簡略版）
 */
function generateCandidateUrls(
  name: string,
  address: string | null,
  corporateNumber: string | null,
  existingHomepage: string | null
): string[] {
  const urls: string[] = [];
  
  if (existingHomepage) {
    urls.push(existingHomepage);
  }
  
  // 決算公告系
  if (corporateNumber) {
    urls.push(`https://catr.jp/s/?q=${encodeURIComponent(corporateNumber)}`);
  }
  urls.push(`https://catr.jp/s/?q=${encodeURIComponent(name)}`);
  
  // 人材/採用系（主要なもののみ）
  urls.push(`https://www.green-japan.com/search?q=${encodeURIComponent(name)}`);
  urls.push(`https://www.wantedly.com/companies?query=${encodeURIComponent(name)}`);
  
  return Array.from(new Set(urls)).slice(0, 10);
}

/**
 * HTMLから情報を抽出（必要なフィールドのみ）
 */
function parseInfoFromHtml(
  html: string,
  url: string,
  companyName: string,
  targetFields: Set<string>
): Partial<any> {
  const $ = cheerio.load(html);
  const info: Partial<any> = {};
  const text = $("body").text().replace(/\s+/g, " ").trim();

  // 必要なフィールドのみを抽出
  if (targetFields.has("listingStatus")) {
    const listingMatch = text.match(/(東証|名証|福証|札証|上場|非上場|未上場|マザーズ|グロース|スタンダード|プライム)/i);
    if (listingMatch) {
      info.listingStatus = listingMatch[1];
    }
  }

  if (targetFields.has("securitiesCode")) {
    const securitiesMatch = text.match(/証券コード[：:]\s*(\d{4})/i);
    if (securitiesMatch) {
      info.securitiesCode = securitiesMatch[1];
    }
  }

  if (targetFields.has("website") && isOfficialSite(url, companyName)) {
    info.website = url;
  }

  if (targetFields.has("capital")) {
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
  }

  if (targetFields.has("revenue")) {
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
          break;
        }
      }
    }
  }

  if (targetFields.has("profit")) {
    const profitPatterns = [
      /(当期純利益|純利益|利益)[：:]\s*([\d,]+)\s*(億円|百万円|万円|千円|円)/i,
      /(当期純利益|純利益|利益)[：:]\s*([\d,]+)\s*(億|百万|万|千)/i,
    ];
    for (const pattern of profitPatterns) {
      const match = text.match(pattern);
      if (match) {
        const value = parseFloat(match[2].replace(/,/g, ""));
        if (!isNaN(value)) {
          info.profit = normalizeToThousandYen(value, match[3] || "");
          break;
        }
      }
    }
  }

  if (targetFields.has("netAssets")) {
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
  }

  if (targetFields.has("industry")) {
    const industryMatch = text.match(/業種[：:]\s*([^\n\r]+)/i);
    if (industryMatch) {
      info.industry = industryMatch[1].trim();
    }
  }

  if (targetFields.has("companyDescription") || targetFields.has("companyOverview")) {
    const descriptionMatch = text.match(/(企業概要|会社概要|事業内容)[：:]\s*([^\n\r]{50,500})/i);
    if (descriptionMatch) {
      info.companyDescription = descriptionMatch[2].trim();
      info.companyOverview = descriptionMatch[2].trim();
    }
  }

  if (targetFields.has("employeeCount")) {
    const employeePattern = /(従業員数|社員数|従業員)[：:]\s*([\d,]+)\s*(人|名)/i;
    const employeeMatch = extractNumber(text, employeePattern);
    if (employeeMatch) {
      info.employeeCount = employeeMatch;
    }
  }

  if (targetFields.has("contactEmail")) {
    const emailMatch = text.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/i);
    if (emailMatch) {
      info.contactEmail = emailMatch[1];
    }
  }

  if (targetFields.has("contactPhone")) {
    const phoneMatch = text.match(/(電話|TEL|Tel)[：:]\s*([0-9-()]+)/i);
    if (phoneMatch) {
      info.contactPhone = phoneMatch[2].replace(/[^\d-]/g, "");
    }
  }

  if (targetFields.has("fax")) {
    const faxMatch = text.match(/(FAX|Fax|fax)[：:]\s*([0-9-()]+)/i);
    if (faxMatch) {
      info.fax = faxMatch[2].replace(/[^\d-]/g, "");
    }
  }

  if (targetFields.has("settlementMonth")) {
    const settlementMatch = text.match(/決算月[：:]\s*(\d{1,2})月/i);
    if (settlementMatch) {
      info.settlementMonth = `${settlementMatch[1]}月`;
    }
  }

  if (targetFields.has("representative")) {
    const repMatch = text.match(/(代表取締役|代表者|社長)[：:]\s*([^\n\r]+)/i);
    if (repMatch) {
      info.representative = repMatch[2].trim();
    }
  }

  if (targetFields.has("representativeKana")) {
    const repKanaMatch = text.match(/(代表取締役|代表者)[（(]([ァ-ヶー]+)[）)]/i);
    if (repKanaMatch) {
      info.representativeKana = repKanaMatch[2];
    }
  }

  if (targetFields.has("representativeAddress")) {
    const repAddressMatch = text.match(/(代表者住所|代表取締役住所)[：:]\s*([^\n\r]+)/i);
    if (repAddressMatch) {
      info.representativeAddress = repAddressMatch[2].trim();
    }
  }

  if (targetFields.has("representativeSchool")) {
    const repSchoolMatch = text.match(/(出身校|学歴)[：:]\s*([^\n\r]+)/i);
    if (repSchoolMatch) {
      info.representativeSchool = repSchoolMatch[2].trim();
    }
  }

  if (targetFields.has("representativeBirthDate")) {
    const birthMatch = text.match(/(生年月日|誕生日)[：:]\s*(\d{4}[年/-]\d{1,2}[月/-]\d{1,2})/i);
    if (birthMatch) {
      info.representativeBirthDate = birthMatch[2];
    }
  }

  if (targetFields.has("officers")) {
    const officerPatterns = [
      /(役員|取締役|監査役)[：:]\s*([^\n\r]+)/gi,
    ];
    info.officers = extractListItems(text, officerPatterns);
  }

  if (targetFields.has("shareholders")) {
    const shareholderPatterns = [
      /(主要株主|株主)[：:]\s*([^\n\r]+)/gi,
    ];
    info.shareholders = extractListItems(text, shareholderPatterns);
  }

  if (targetFields.has("banks")) {
    const bankPatterns = [
      /(取引銀行|主要取引銀行|メインバンク)[：:]\s*([^\n\r]+)/gi,
    ];
    info.banks = extractListItems(text, bankPatterns);
  }

  info.sourceUrls = [url];
  return info;
}

/**
 * 単一URLから情報を取得（必要なフィールドのみ）
 */
async function fetchInfoFromUrl(
  url: string,
  companyName: string,
  targetFields: Set<string>
): Promise<Partial<any> | null> {
  try {
    const res = await fetchWithRetry(url, {}, 2, 2000);
    if (!res || !res.ok) return null;

    const html = await res.text();
    const info = parseInfoFromHtml(html, url, companyName, targetFields);
    return info;
  } catch (e) {
    console.warn(`[fetchInfoFromUrl] ${url} の取得エラー:`, (e as any)?.message || e);
    return null;
  }
}

/**
 * 不足フィールドのみをWebスクレイピングで取得
 */
async function fetchMissingFieldsFromWeb(
  companyId: string,
  companyData: any,
  targetFields: Set<string>
): Promise<Partial<any>> {
  const name: string = companyData.name || "";
  const address: string | null = companyData.headquartersAddress || companyData.address || null;
  const corporateNumber: string | null = companyData.corporateNumber || null;
  const existingHomepage = companyData.companyUrl || companyData.hp || null;

  if (!name || targetFields.size === 0) {
    return {};
  }

  try {
    console.log(`[fetchMissingFields] 開始: ${companyId} / ${name} / 対象フィールド: ${Array.from(targetFields).join(", ")}`);

    const candidateUrls = generateCandidateUrls(name, address, corporateNumber, existingHomepage);
    console.log(`[fetchMissingFields] 候補URL数: ${candidateUrls.length}`);

    const extractedInfos: Partial<any>[] = [];
    for (const url of candidateUrls.slice(0, 5)) {
      try {
        const info = await fetchInfoFromUrl(url, name, targetFields);
        if (info && info.sourceUrls && info.sourceUrls.length > 0) {
          extractedInfos.push(info);
        }
        await sleep(500);
      } catch (error) {
        console.warn(`[fetchMissingFields] URL ${url} の処理エラー:`, (error as any)?.message || error);
      }
    }

    // 情報をマージ（最初の非null値を採用）
    const merged: Partial<any> = {};
    for (const info of extractedInfos) {
      for (const field of targetFields) {
        if (!merged[field] && info[field] !== null && info[field] !== undefined) {
          if (Array.isArray(info[field])) {
            if ((info[field] as any[]).length > 0) {
              merged[field] = info[field];
            }
          } else {
            merged[field] = info[field];
          }
        }
      }
    }

    console.log(`[fetchMissingFields] 完了: ${companyId} / 取得フィールド数: ${Object.keys(merged).length}`);
    return merged;
  } catch (e: any) {
    console.error(`[fetchMissingFields] エラー: ${companyId}`, e);
    return {};
  }
}

/**
 * WebInfoデータをcompanies_newフィールドにマッピング
 */
function mapWebInfoToCompanyFields(webInfo: Partial<any>): { [key: string]: any } {
  const companyData: { [key: string]: any } = {};

  for (const [webInfoField, companyFields] of Object.entries(WEBINFO_TO_COMPANY_FIELD_MAPPING)) {
    const webInfoValue = webInfo[webInfoField];
    if (webInfoValue !== null && webInfoValue !== undefined) {
      for (const companyField of companyFields) {
        // 既に値が設定されている場合はスキップ（既存データを優先）
        if (companyData[companyField] === undefined) {
          companyData[companyField] = webInfoValue;
        }
      }
    }
  }

  return companyData;
}

/**
 * メイン処理: 不足フィールドを取得して更新
 */
async function fetchAndUpdateMissingFields() {
  try {
    console.log("不足フィールドの取得を開始...");

    const BATCH_SIZE = 100;
    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;
    let totalUpdated = 0;

    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }

      console.log(`\nバッチ取得: ${snapshot.size} 件`);

      for (const companyDoc of snapshot.docs) {
        const companyId = companyDoc.id;
        const companyData = companyDoc.data();

        // webInfoを取得
        const webInfoRef = db.collection("companies_webInfo").doc(companyId);
        const webInfoDoc = await webInfoRef.get();
        const webInfoData = webInfoDoc.exists ? webInfoDoc.data() : null;

        // 不足フィールドを特定
        const { missingCompanyFields, targetWebInfoFields } = getMissingFieldsThatCanBeFetched(
          companyData,
          webInfoData
        );

        if (targetWebInfoFields.size === 0) {
          // 取得する必要がない場合はスキップ
          totalProcessed++;
          continue;
        }

        console.log(`\n[${companyId}] ${companyData.name || ""}`);
        console.log(`  不足フィールド: ${missingCompanyFields.length} 件`);
        console.log(`  取得対象: ${Array.from(targetWebInfoFields).join(", ")}`);

        // Webスクレイピングで不足フィールドを取得
        const fetchedWebInfo = await fetchMissingFieldsFromWeb(
          companyId,
          companyData,
          targetWebInfoFields
        );

        if (Object.keys(fetchedWebInfo).length > 0) {
          // webInfoを更新
          const updatedWebInfo = {
            ...(webInfoData || {}),
            ...fetchedWebInfo,
            updatedAt: new Date().toISOString(),
          };
          await webInfoRef.set(updatedWebInfo, { merge: true });

          // companies_newを更新（マッピングされたフィールドのみ）
          const companyUpdates = mapWebInfoToCompanyFields(fetchedWebInfo);
          if (Object.keys(companyUpdates).length > 0) {
            await companyDoc.ref.update(companyUpdates);
            console.log(`  ✅ 更新完了: ${Object.keys(companyUpdates).join(", ")}`);
            totalUpdated++;
          }
        }

        totalProcessed++;

        // レート制限対策
        await sleep(1000);
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }

    console.log(`\n✅ 処理完了`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`更新数: ${totalUpdated} 件`);
  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
fetchAndUpdateMissingFields()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });

