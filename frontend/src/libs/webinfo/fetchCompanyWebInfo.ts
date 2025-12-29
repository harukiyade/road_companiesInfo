import { chromium, Browser, Page } from "playwright";
import { db } from "@/libs/firebaseAdmin";
import { CompanyWebInfo } from "./types";

/**
 * 企業の基本情報
 */
export interface CompanyBasic {
  id: string;
  name: string;
  corporateNumber?: string;
  headquartersAddress?: string;
}

/**
 * 検索クエリを組み立てる
 */
function buildSearchQuery(company: CompanyBasic): string {
  const parts: string[] = [];
  if (company.name) parts.push(company.name);
  if (company.headquartersAddress) parts.push(company.headquartersAddress);
  if (company.corporateNumber) parts.push(`法人番号 ${company.corporateNumber}`);
  return parts.join(" ");
}

/**
 * Web検索を実行し、関連URLを取得
 */
async function searchCompanyUrls(
  page: Page,
  query: string
): Promise<string[]> {
  const urls: string[] = [];
  try {
    // Google検索を実行
    await page.goto(
      `https://www.google.com/search?q=${encodeURIComponent(query)}`,
      { waitUntil: "networkidle" }
    );

    // 検索結果からURLを抽出
    const links = await page.$$eval("a[href^='http']", (elements) => {
      return elements
        .map((el) => (el as HTMLAnchorElement).href)
        .filter((href) => {
          // Googleの内部リンクを除外
          return (
            !href.includes("google.com") &&
            !href.includes("googleusercontent.com") &&
            !href.startsWith("https://www.google.com")
          );
        })
        .slice(0, 20); // 上位20件まで
    });

    urls.push(...links);
  } catch (error) {
    console.error("検索エラー:", error);
  }

  return urls;
}

/**
 * URLが公式サイトっぽいか判定
 */
function isOfficialSite(url: string, companyName: string): boolean {
  const domain = new URL(url).hostname.toLowerCase();
  const nameLower = companyName.toLowerCase();
  // 会社名がドメインに含まれる、または一般的な企業サイトパターン
  return (
    domain.includes(nameLower.replace(/\s+/g, "")) ||
    domain.includes("corp") ||
    domain.includes("company") ||
    domain.includes("inc") ||
    domain.includes("co.jp")
  );
}

/**
 * HTMLからテキストを抽出
 */
async function extractText(page: Page, selector?: string): Promise<string> {
  try {
    if (selector) {
      const element = await page.$(selector);
      if (element) {
        return await element.textContent() || "";
      }
    }
    return await page.textContent("body") || "";
  } catch {
    return "";
  }
}

/**
 * 数値を抽出（正規表現でパターンマッチ）
 */
function extractNumber(text: string, pattern: RegExp): number | null {
  const match = text.match(pattern);
  if (match) {
    const numStr = match[1].replace(/,/g, "").replace(/[^\d.]/g, "");
    const num = parseFloat(numStr);
    return isNaN(num) ? null : num;
  }
  return null;
}

/**
 * リスト項目を抽出
 */
function extractListItems(text: string, patterns: RegExp[]): string[] {
  const items: string[] = [];
  for (const pattern of patterns) {
    const matches = text.matchAll(new RegExp(pattern, "gi"));
    for (const match of matches) {
      if (match[1] && !items.includes(match[1])) {
        items.push(match[1].trim());
      }
    }
  }
  return items;
}

/**
 * 単一URLから企業情報を抽出
 */
async function extractInfoFromUrl(
  page: Page,
  url: string,
  companyName: string
): Promise<Partial<CompanyWebInfo>> {
  const info: Partial<CompanyWebInfo> = {};
  const sourceUrls: string[] = [];

  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 10000 });
    sourceUrls.push(url);

    const bodyText = await extractText(page);

    // 上場区分・証券コード
    const listingMatch = bodyText.match(
      /(東証|名証|福証|札証|上場|非上場|未上場)/i
    );
    if (listingMatch) {
      info.listingStatus = listingMatch[1];
    }

    const securitiesMatch = bodyText.match(/証券コード[：:]\s*(\d{4})/i);
    if (securitiesMatch) {
      info.securitiesCode = securitiesMatch[1];
    }

    // HP（現在のURLが公式サイトっぽい場合）
    if (isOfficialSite(url, companyName)) {
      info.website = url;
    }

    // 問い合わせフォーム
    const contactFormMatch = bodyText.match(
      /(お問い合わせ|問い合わせ|コンタクト|contact)[^。]*?([^\s]+\.(html|php|aspx?|jsp))/i
    );
    if (contactFormMatch) {
      try {
        const baseUrl = new URL(url).origin;
        info.contactFormUrl = new URL(contactFormMatch[2], baseUrl).href;
      } catch {
        // URL構築失敗時はスキップ
      }
    }

    // 資本金
    const capitalPattern = /資本金[：:]\s*([\d,]+)\s*(千円|万円|円|百万円)/i;
    const capitalMatch = extractNumber(bodyText, capitalPattern);
    if (capitalMatch) {
      info.capital = capitalMatch;
    }

    // 売上
    const revenuePattern = /(売上高|売上)[：:]\s*([\d,]+)\s*(千円|万円|円|百万円)/i;
    const revenueMatch = extractNumber(bodyText, revenuePattern);
    if (revenueMatch) {
      info.revenue = revenueMatch;
    }

    // 利益
    const profitPattern = /(当期純利益|純利益|利益)[：:]\s*([\d,]+)\s*(千円|万円|円|百万円)/i;
    const profitMatch = extractNumber(bodyText, profitPattern);
    if (profitMatch) {
      info.profit = profitMatch;
    }

    // 純資産
    const netAssetsPattern = /(純資産|自己資本)[：:]\s*([\d,]+)\s*(千円|万円|円|百万円)/i;
    const netAssetsMatch = extractNumber(bodyText, netAssetsPattern);
    if (netAssetsMatch) {
      info.netAssets = netAssetsMatch;
    }

    // 業種
    const industryMatch = bodyText.match(/業種[：:]\s*([^\n\r]+)/i);
    if (industryMatch) {
      info.industry = industryMatch[1].trim();
    }

    // 免許/事業者登録
    const licensePatterns = [
      /(建設業許可|宅地建物取引業|古物商|旅行業|食品衛生法|薬局|運送業)[：:]\s*([^\n\r]+)/gi,
    ];
    info.licenses = extractListItems(bodyText, licensePatterns);

    // 取引先銀行
    const bankPatterns = [
      /(取引銀行|主要取引銀行|メインバンク)[：:]\s*([^\n\r]+)/gi,
    ];
    info.banks = extractListItems(bodyText, bankPatterns);

    // 企業説明・概要
    const descriptionMatch = bodyText.match(
      /(企業概要|会社概要|事業内容)[：:]\s*([^\n\r]{50,500})/i
    );
    if (descriptionMatch) {
      info.companyDescription = descriptionMatch[2].trim();
      info.companyOverview = descriptionMatch[2].trim();
    }

    // 取締役
    const directorPatterns = [
      /(代表取締役|取締役)[：:]\s*([^\n\r]+)/gi,
      /(社長|CEO|代表)[：:]\s*([^\n\r]+)/gi,
    ];
    info.directors = extractListItems(bodyText, directorPatterns);

    // 社員数
    const employeePattern = /(従業員数|社員数|従業員)[：:]\s*([\d,]+)\s*(人|名)/i;
    const employeeMatch = extractNumber(bodyText, employeePattern);
    if (employeeMatch) {
      info.employeeCount = employeeMatch;
    }

    // オフィス数・工場数・店舗数
    const officePattern = /(オフィス|事業所)[：:]\s*([\d,]+)\s*(箇所|ヶ所|件)/i;
    const officeMatch = extractNumber(bodyText, officePattern);
    if (officeMatch) {
      info.officeCount = officeMatch;
    }

    const factoryPattern = /(工場|製造所)[：:]\s*([\d,]+)\s*(箇所|ヶ所|件)/i;
    const factoryMatch = extractNumber(bodyText, factoryPattern);
    if (factoryMatch) {
      info.factoryCount = factoryMatch;
    }

    const storePattern = /(店舗|ショップ)[：:]\s*([\d,]+)\s*(箇所|ヶ所|件)/i;
    const storeMatch = extractNumber(bodyText, storePattern);
    if (storeMatch) {
      info.storeCount = storeMatch;
    }

    // メールアドレス
    const emailMatch = bodyText.match(
      /([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/i
    );
    if (emailMatch) {
      info.contactEmail = emailMatch[1];
    }

    // 電話番号
    const phoneMatch = bodyText.match(
      /(電話|TEL|Tel)[：:]\s*([0-9-()]+)/i
    );
    if (phoneMatch) {
      info.contactPhone = phoneMatch[2].replace(/[^\d-]/g, "");
    }

    // FAX
    const faxMatch = bodyText.match(/(FAX|Fax|fax)[：:]\s*([0-9-()]+)/i);
    if (faxMatch) {
      info.fax = faxMatch[2].replace(/[^\d-]/g, "");
    }

    // SNS
    const snsPatterns = [
      /(https?:\/\/[^\s]+(twitter\.com|facebook\.com|instagram\.com|linkedin\.com)[^\s]*)/gi,
    ];
    const snsMatches = bodyText.matchAll(snsPatterns[0]);
    const snsUrls: string[] = [];
    for (const match of snsMatches) {
      if (match[1] && !snsUrls.includes(match[1])) {
        snsUrls.push(match[1]);
      }
    }
    info.sns = snsUrls;

    // 決算月
    const settlementMatch = bodyText.match(/決算月[：:]\s*(\d{1,2})月/i);
    if (settlementMatch) {
      info.settlementMonth = `${settlementMatch[1]}月`;
    }

    // 代表者
    const repMatch = bodyText.match(
      /(代表取締役|代表者|社長)[：:]\s*([^\n\r]+)/i
    );
    if (repMatch) {
      info.representative = repMatch[2].trim();
    }

    // 代表者カナ
    const repKanaMatch = bodyText.match(
      /(代表取締役|代表者)[（(]([ァ-ヶー]+)[）)]/i
    );
    if (repKanaMatch) {
      info.representativeKana = repKanaMatch[2];
    }

    // 代表者住所
    const repAddressMatch = bodyText.match(
      /(代表者住所|代表取締役住所)[：:]\s*([^\n\r]+)/i
    );
    if (repAddressMatch) {
      info.representativeAddress = repAddressMatch[2].trim();
    }

    // 代表者出身校
    const repSchoolMatch = bodyText.match(
      /(出身校|学歴)[：:]\s*([^\n\r]+)/i
    );
    if (repSchoolMatch) {
      info.representativeSchool = repSchoolMatch[2].trim();
    }

    // 代表者生年月日
    const birthMatch = bodyText.match(
      /(生年月日|誕生日)[：:]\s*(\d{4}[年/-]\d{1,2}[月/-]\d{1,2})/i
    );
    if (birthMatch) {
      info.representativeBirthDate = birthMatch[2];
    }

    // 役員名
    const officerPatterns = [
      /(役員|取締役|監査役)[：:]\s*([^\n\r]+)/gi,
    ];
    info.officers = extractListItems(bodyText, officerPatterns);

    // 株主
    const shareholderPatterns = [
      /(主要株主|株主)[：:]\s*([^\n\r]+)/gi,
    ];
    info.shareholders = extractListItems(bodyText, shareholderPatterns);

    // 自己資本比率
    const equityRatioPattern = /(自己資本比率|Equity Ratio)[：:]\s*([\d.]+)\s*%/i;
    const equityRatioMatch = extractNumber(bodyText, equityRatioPattern);
    if (equityRatioMatch) {
      info.equityRatio = equityRatioMatch;
    }

    info.sourceUrls = sourceUrls;
  } catch (error) {
    console.error(`URL ${url} からの情報抽出エラー:`, error);
  }

  return info;
}

/**
 * 複数の情報をマージ
 */
function mergeWebInfo(
  infos: Partial<CompanyWebInfo>[]
): Partial<CompanyWebInfo> {
  const merged: Partial<CompanyWebInfo> = {
    sourceUrls: [],
    licenses: [],
    banks: [],
    directors: [],
    sns: [],
    officers: [],
    shareholders: [],
  };

  for (const info of infos) {
    // 単一値は最初の非null値を採用
    if (!merged.listingStatus && info.listingStatus) {
      merged.listingStatus = info.listingStatus;
    }
    if (!merged.securitiesCode && info.securitiesCode) {
      merged.securitiesCode = info.securitiesCode;
    }
    if (!merged.website && info.website) {
      merged.website = info.website;
    }
    if (!merged.contactFormUrl && info.contactFormUrl) {
      merged.contactFormUrl = info.contactFormUrl;
    }
    if (!merged.constructionEvaluation && info.constructionEvaluation) {
      merged.constructionEvaluation = info.constructionEvaluation;
    }
    if (!merged.capital && info.capital) {
      merged.capital = info.capital;
    }
    if (!merged.revenue && info.revenue) {
      merged.revenue = info.revenue;
    }
    if (!merged.profit && info.profit) {
      merged.profit = info.profit;
    }
    if (!merged.netAssets && info.netAssets) {
      merged.netAssets = info.netAssets;
    }
    if (!merged.industry && info.industry) {
      merged.industry = info.industry;
    }
    if (!merged.companyDescription && info.companyDescription) {
      merged.companyDescription = info.companyDescription;
    }
    if (!merged.companyOverview && info.companyOverview) {
      merged.companyOverview = info.companyOverview;
    }
    if (!merged.employeeCount && info.employeeCount) {
      merged.employeeCount = info.employeeCount;
    }
    if (!merged.officeCount && info.officeCount) {
      merged.officeCount = info.officeCount;
    }
    if (!merged.factoryCount && info.factoryCount) {
      merged.factoryCount = info.factoryCount;
    }
    if (!merged.storeCount && info.storeCount) {
      merged.storeCount = info.storeCount;
    }
    if (!merged.contactEmail && info.contactEmail) {
      merged.contactEmail = info.contactEmail;
    }
    if (!merged.contactPhone && info.contactPhone) {
      merged.contactPhone = info.contactPhone;
    }
    if (!merged.fax && info.fax) {
      merged.fax = info.fax;
    }
    if (!merged.settlementMonth && info.settlementMonth) {
      merged.settlementMonth = info.settlementMonth;
    }
    if (!merged.representative && info.representative) {
      merged.representative = info.representative;
    }
    if (!merged.representativeKana && info.representativeKana) {
      merged.representativeKana = info.representativeKana;
    }
    if (!merged.representativeAddress && info.representativeAddress) {
      merged.representativeAddress = info.representativeAddress;
    }
    if (!merged.representativeSchool && info.representativeSchool) {
      merged.representativeSchool = info.representativeSchool;
    }
    if (!merged.representativeBirthDate && info.representativeBirthDate) {
      merged.representativeBirthDate = info.representativeBirthDate;
    }
    if (!merged.equityRatio && info.equityRatio) {
      merged.equityRatio = info.equityRatio;
    }

    // 配列は結合（重複除去）
    if (info.licenses) {
      merged.licenses = [
        ...new Set([...(merged.licenses || []), ...info.licenses]),
      ];
    }
    if (info.banks) {
      merged.banks = [...new Set([...(merged.banks || []), ...info.banks])];
    }
    if (info.directors) {
      merged.directors = [
        ...new Set([...(merged.directors || []), ...info.directors]),
      ];
    }
    if (info.sns) {
      merged.sns = [...new Set([...(merged.sns || []), ...info.sns])];
    }
    if (info.officers) {
      merged.officers = [
        ...new Set([...(merged.officers || []), ...info.officers]),
      ];
    }
    if (info.shareholders) {
      merged.shareholders = [
        ...new Set([...(merged.shareholders || []), ...info.shareholders]),
      ];
    }
    if (info.sourceUrls) {
      merged.sourceUrls = [
        ...new Set([...(merged.sourceUrls || []), ...info.sourceUrls]),
      ];
    }
  }

  return merged;
}

/**
 * 企業のWeb情報を取得してFirestoreに保存
 */
export async function fetchCompanyWebInfo(
  company: CompanyBasic
): Promise<CompanyWebInfo> {
  let browser: Browser | null = null;
  const result: CompanyWebInfo = {
    listingStatus: null,
    securitiesCode: null,
    website: null,
    contactFormUrl: null,
    constructionEvaluation: null,
    capital: null,
    revenue: null,
    profit: null,
    netAssets: null,
    industry: null,
    licenses: [],
    banks: [],
    companyDescription: null,
    companyOverview: null,
    directors: [],
    employeeCount: null,
    officeCount: null,
    factoryCount: null,
    storeCount: null,
    contactEmail: null,
    contactPhone: null,
    fax: null,
    sns: [],
    settlementMonth: null,
    representative: null,
    representativeKana: null,
    representativeAddress: null,
    representativeSchool: null,
    representativeBirthDate: null,
    officers: [],
    shareholders: [],
    equityRatio: null,
    sourceUrls: [],
    updatedAt: new Date().toISOString(),
    status: "running",
  };

  try {
    // Playwrightブラウザを起動
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    // 検索クエリを組み立て
    const query = buildSearchQuery(company);
    console.log(`検索クエリ: ${query}`);

    // Web検索を実行
    const urls = await searchCompanyUrls(page, query);
    console.log(`取得URL数: ${urls.length}`);

    // 各URLから情報を抽出
    const extractedInfos: Partial<CompanyWebInfo>[] = [];
    for (const url of urls.slice(0, 10)) {
      // 最大10件まで処理
      try {
        const info = await extractInfoFromUrl(page, url, company.name);
        if (info.sourceUrls && info.sourceUrls.length > 0) {
          extractedInfos.push(info);
        }
        // レート制限対策で少し待機
        await page.waitForTimeout(1000);
      } catch (error) {
        console.error(`URL ${url} の処理エラー:`, error);
      }
    }

    // 情報をマージ
    const merged = mergeWebInfo(extractedInfos);
    Object.assign(result, merged);

    // 取得できた項目数をカウントしてステータスを決定
    const filledFields = Object.values(result).filter(
      (v) => v !== null && v !== undefined && (Array.isArray(v) ? v.length > 0 : true)
    ).length;
    const totalFields = Object.keys(result).length - 3; // sourceUrls, updatedAt, statusを除く

    if (filledFields >= totalFields * 0.5) {
      result.status = "success";
    } else if (filledFields > 0) {
      result.status = "partial";
    } else {
      result.status = "failed";
      result.errorMessage = "情報を取得できませんでした";
    }

    await browser.close();
  } catch (error) {
    result.status = "failed";
    result.errorMessage =
      error instanceof Error ? error.message : "不明なエラー";
    console.error("Web情報取得エラー:", error);
    if (browser) {
      await browser.close();
    }
  }

  // Firestoreに保存
  try {
    const docRef = db.collection("companies_webInfo").doc(company.id);
    await docRef.set(
      {
        ...result,
        updatedAt: new Date().toISOString(),
      },
      { merge: true }
    );
    console.log(`Firestore保存完了: ${company.id}`);
  } catch (error) {
    console.error("Firestore保存エラー:", error);
    result.errorMessage = `保存エラー: ${
      error instanceof Error ? error.message : "不明なエラー"
    }`;
  }

  return result;
}

