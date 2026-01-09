// scripts/cnavi_scrape.ts
//
// cnavi-app.g-search.or.jp から企業情報をスクレイピングしてCSVに出力するスクリプト
//
// 実行例:
//   CNAVI_LOGIN_ID=h.shiroyama@legatuscorp.com CNAVI_PASSWORD=Furapote0403/ npx tsx scripts/cnavi_scrape.ts
//
// デバッグモード（HTML保存）:
//   SAVE_HTML=1 CNAVI_LOGIN_ID=... CNAVI_PASSWORD=... npx tsx scripts/cnavi_scrape.ts
//
// バックグラウンド実行（headlessモード）:
//   HEADLESS=1 CNAVI_LOGIN_ID=... CNAVI_PASSWORD=... npx tsx scripts/cnavi_scrape.ts

import { chromium, Browser, Page } from "playwright";
import * as fs from "fs";
import * as path from "path";
import * as crypto from "crypto";
import * as readline from "readline";
import { createObjectCsvWriter } from "csv-writer";

const CNAVI_URL = "https://cnavi-app.g-search.or.jp/";
const OUTPUT_CSV = "out/cnavi_companies.csv";
const OUTPUT_LOG = "out/cnavi_scrape.log";
const HTML_OUTPUT_DIR = "out/html";
const SAVE_HTML = process.env.SAVE_HTML === "1" || process.env.SAVE_HTML === "true";

// 環境変数から認証情報を取得
const LOGIN_ID = process.env.CNAVI_LOGIN_ID;
const PASSWORD = process.env.CNAVI_PASSWORD;

if (!LOGIN_ID || !PASSWORD) {
  log("❌ エラー: 環境変数 CNAVI_LOGIN_ID と CNAVI_PASSWORD を設定してください", true);
  process.exit(1);
}

// ログファイルへの書き込み用ストリーム
let logStream: fs.WriteStream | null = null;

/**
 * ログをコンソールとファイルの両方に出力
 */
function log(message: string, error: boolean = false) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  
  // コンソールに出力
  if (error) {
    console.error(logMessage);
  } else {
    console.log(logMessage);
  }
  
  // ログファイルに出力
  if (logStream) {
    logStream.write(logMessage + "\n");
  }
}

/**
 * ログファイルを初期化
 */
function initLogFile() {
  // ログディレクトリを作成
  const logDir = path.dirname(OUTPUT_LOG);
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir, { recursive: true });
  }
  
  // ログファイルを開く（追記モード）
  logStream = fs.createWriteStream(OUTPUT_LOG, { flags: "a" });
  log(`📝 ログファイル: ${OUTPUT_LOG}`);
}

/**
 * ログファイルを閉じる
 */
function closeLogFile() {
  if (logStream) {
    logStream.end();
    logStream = null;
  }
}

// CSVヘッダー（companies_newコレクションの全フィールド、指定順序通り）
// 注意: businessDescriptions が2回登場（DB仕様に合わせるため）
const CSV_HEADERS = [
  { id: "companyId", title: "companyId" },
  { id: "name", title: "name" },
  { id: "nameEn", title: "nameEn" },
  { id: "kana", title: "kana" },
  { id: "corporateNumber", title: "corporateNumber" },
  { id: "corporationType", title: "corporationType" },
  { id: "nikkeiCode", title: "nikkeiCode" },
  { id: "badges", title: "badges" },
  { id: "tags", title: "tags" },
  { id: "createdAt", title: "createdAt" },
  { id: "updatedAt", title: "updatedAt" },
  { id: "updateDate", title: "updateDate" },
  { id: "updateCount", title: "updateCount" },
  { id: "changeCount", title: "changeCount" },
  { id: "qualificationGrade", title: "qualificationGrade" },
  { id: "prefecture", title: "prefecture" },
  { id: "address", title: "address" },
  { id: "headquartersAddress", title: "headquartersAddress" },
  { id: "postalCode", title: "postalCode" },
  { id: "location", title: "location" },
  { id: "departmentLocation", title: "departmentLocation" },
  { id: "phoneNumber", title: "phoneNumber" },
  { id: "contactPhoneNumber", title: "contactPhoneNumber" },
  { id: "fax", title: "fax" },
  { id: "email", title: "email" },
  { id: "companyUrl", title: "companyUrl" },
  { id: "contactFormUrl", title: "contactFormUrl" },
  { id: "representativeName", title: "representativeName" },
  { id: "representativeKana", title: "representativeKana" },
  { id: "representativeTitle", title: "representativeTitle" },
  { id: "representativeBirthDate", title: "representativeBirthDate" },
  { id: "representativePhone", title: "representativePhone" },
  { id: "representativePostalCode", title: "representativePostalCode" },
  { id: "representativeHomeAddress", title: "representativeHomeAddress" },
  { id: "representativeRegisteredAddress", title: "representativeRegisteredAddress" },
  { id: "representativeAlmaMater", title: "representativeAlmaMater" },
  { id: "executives", title: "executives" },
  { id: "executiveName1", title: "executiveName1" },
  { id: "executiveName2", title: "executiveName2" },
  { id: "executiveName3", title: "executiveName3" },
  { id: "executiveName4", title: "executiveName4" },
  { id: "executiveName5", title: "executiveName5" },
  { id: "executiveName6", title: "executiveName6" },
  { id: "executiveName7", title: "executiveName7" },
  { id: "executiveName8", title: "executiveName8" },
  { id: "executiveName9", title: "executiveName9" },
  { id: "executiveName10", title: "executiveName10" },
  { id: "executivePosition1", title: "executivePosition1" },
  { id: "executivePosition2", title: "executivePosition2" },
  { id: "executivePosition3", title: "executivePosition3" },
  { id: "executivePosition4", title: "executivePosition4" },
  { id: "executivePosition5", title: "executivePosition5" },
  { id: "executivePosition6", title: "executivePosition6" },
  { id: "executivePosition7", title: "executivePosition7" },
  { id: "executivePosition8", title: "executivePosition8" },
  { id: "executivePosition9", title: "executivePosition9" },
  { id: "executivePosition10", title: "executivePosition10" },
  { id: "industry", title: "industry" },
  { id: "industryLarge", title: "industryLarge" },
  { id: "industryMiddle", title: "industryMiddle" },
  { id: "industrySmall", title: "industrySmall" },
  { id: "industryDetail", title: "industryDetail" },
  { id: "industries", title: "industries" },
  { id: "industryCategories", title: "industryCategories" },
  { id: "businessDescriptions", title: "businessDescriptions" },
  { id: "businessItems", title: "businessItems" },
  { id: "businessSummary", title: "businessSummary" },
  { id: "specialties", title: "specialties" },
  { id: "demandProducts", title: "demandProducts" },
  { id: "specialNote", title: "specialNote" },
  { id: "capitalStock", title: "capitalStock" },
  { id: "revenue", title: "revenue" },
  { id: "latestRevenue", title: "latestRevenue" },
  { id: "latestProfit", title: "latestProfit" },
  { id: "revenueFromStatements", title: "revenueFromStatements" },
  { id: "operatingIncome", title: "operatingIncome" },
  { id: "totalAssets", title: "totalAssets" },
  { id: "totalLiabilities", title: "totalLiabilities" },
  { id: "netAssets", title: "netAssets" },
  { id: "issuedShares", title: "issuedShares" },
  { id: "financials", title: "financials" },
  { id: "listing", title: "listing" },
  { id: "marketSegment", title: "marketSegment" },
  { id: "latestFiscalYearMonth", title: "latestFiscalYearMonth" },
  { id: "fiscalMonth", title: "fiscalMonth" },
  { id: "fiscalMonth1", title: "fiscalMonth1" },
  { id: "fiscalMonth2", title: "fiscalMonth2" },
  { id: "fiscalMonth3", title: "fiscalMonth3" },
  { id: "fiscalMonth4", title: "fiscalMonth4" },
  { id: "fiscalMonth5", title: "fiscalMonth5" },
  { id: "revenue1", title: "revenue1" },
  { id: "revenue2", title: "revenue2" },
  { id: "revenue3", title: "revenue3" },
  { id: "revenue4", title: "revenue4" },
  { id: "revenue5", title: "revenue5" },
  { id: "profit1", title: "profit1" },
  { id: "profit2", title: "profit2" },
  { id: "profit3", title: "profit3" },
  { id: "profit4", title: "profit4" },
  { id: "profit5", title: "profit5" },
  { id: "employeeCount", title: "employeeCount" },
  { id: "employeeNumber", title: "employeeNumber" },
  { id: "factoryCount", title: "factoryCount" },
  { id: "officeCount", title: "officeCount" },
  { id: "storeCount", title: "storeCount" },
  { id: "averageAge", title: "averageAge" },
  { id: "averageYearsOfService", title: "averageYearsOfService" },
  { id: "averageOvertimeHours", title: "averageOvertimeHours" },
  { id: "averagePaidLeave", title: "averagePaidLeave" },
  { id: "femaleExecutiveRatio", title: "femaleExecutiveRatio" },
  { id: "established", title: "established" },
  { id: "dateOfEstablishment", title: "dateOfEstablishment" },
  { id: "founding", title: "founding" },
  { id: "foundingYear", title: "foundingYear" },
  { id: "acquisition", title: "acquisition" },
  { id: "clients", title: "clients" },
  { id: "suppliers", title: "suppliers" },
  { id: "subsidiaries", title: "subsidiaries" },
  { id: "affiliations", title: "affiliations" },
  { id: "shareholders", title: "shareholders" },
  { id: "banks", title: "banks" },
  { id: "bankCorporateNumber", title: "bankCorporateNumber" },
  { id: "departmentName1", title: "departmentName1" },
  { id: "departmentName2", title: "departmentName2" },
  { id: "departmentName3", title: "departmentName3" },
  { id: "departmentName4", title: "departmentName4" },
  { id: "departmentName5", title: "departmentName5" },
  { id: "departmentName6", title: "departmentName6" },
  { id: "departmentName7", title: "departmentName7" },
  { id: "departmentAddress1", title: "departmentAddress1" },
  { id: "departmentAddress2", title: "departmentAddress2" },
  { id: "departmentAddress3", title: "departmentAddress3" },
  { id: "departmentAddress4", title: "departmentAddress4" },
  { id: "departmentAddress5", title: "departmentAddress5" },
  { id: "departmentAddress6", title: "departmentAddress6" },
  { id: "departmentAddress7", title: "departmentAddress7" },
  { id: "departmentPhone1", title: "departmentPhone1" },
  { id: "departmentPhone2", title: "departmentPhone2" },
  { id: "departmentPhone3", title: "departmentPhone3" },
  { id: "departmentPhone4", title: "departmentPhone4" },
  { id: "departmentPhone5", title: "departmentPhone5" },
  { id: "departmentPhone6", title: "departmentPhone6" },
  { id: "departmentPhone7", title: "departmentPhone7" },
  { id: "overview", title: "overview" },
  { id: "companyDescription", title: "companyDescription" },
  { id: "businessDescriptions", title: "businessDescriptions" }, // 2回目（DB仕様に合わせるため）
  { id: "salesNotes", title: "salesNotes" },
  { id: "urls", title: "urls" },
  { id: "profileUrl", title: "profileUrl" },
  { id: "externalDetailUrl", title: "externalDetailUrl" },
  { id: "facebook", title: "facebook" },
  { id: "linkedin", title: "linkedin" },
  { id: "wantedly", title: "wantedly" },
  { id: "youtrust", title: "youtrust" },
  { id: "metaKeywords", title: "metaKeywords" },
];

// ユーティリティ関数

/**
 * 金額文字列を数値に変換（例: "200,000,000円" → "200000000"）
 */
function normalizeAmount(text: string | null | undefined): string {
  if (!text || text === "-" || text.trim() === "") return "";
  const cleaned = text.replace(/[^\d]/g, "");
  return cleaned;
}

/**
 * 人数文字列を数値に変換（例: "1,011人" → "1011"）
 */
function normalizeEmployeeCount(text: string | null | undefined): string {
  if (!text || text === "-" || text.trim() === "") return "";
  const cleaned = text.replace(/[^\d]/g, "");
  return cleaned;
}

/**
 * 日付文字列をYYYY-MM-DD形式に変換（例: "1949年04月05日" → "1949-04-05"）
 */
function normalizeDate(text: string | null | undefined): string {
  if (!text || text === "-" || text.trim() === "") return "";
  const match = text.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/);
  if (match) {
    const year = match[1];
    const month = match[2].padStart(2, "0");
    const day = match[3].padStart(2, "0");
    return `${year}-${month}-${day}`;
  }
  return "";
}

/**
 * companyIdを生成（法人番号があればそれ、なければhash）
 */
function generateCompanyId(corporateNumber: string | null, name: string, address: string): string {
  if (corporateNumber && corporateNumber.trim() !== "") {
    return corporateNumber.trim();
  }
  const hash = crypto.createHash("sha256").update(`${name}|${address}`).digest("hex");
  return hash.substring(0, 16); // 先頭16文字を使用
}

/**
 * Enterキー待ち
 */
function waitForEnter(): Promise<void> {
  return new Promise((resolve) => {
    const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout,
    });

    log("\n✅ 企業リストを表示してください。準備ができたら Enter キーを押してください...");
    rl.on("line", () => {
      rl.close();
      resolve();
    });
  });
}

/**
 * 空のオブジェクトを作成（全フィールドを空文字で初期化）
 */
function createEmptyRecord(): Record<string, string> {
  const record: Record<string, string> = {};
  CSV_HEADERS.forEach((header) => {
    record[header.id] = "";
  });
  return record;
}

/**
 * 企業詳細ページから情報を取得
 */
async function scrapeCompanyDetail(page: Page, companyName: string): Promise<Record<string, string>> {
  const record = createEmptyRecord();

  try {
    // セッション維持のためのアクティビティ（処理開始時）
    await maintainSession(page);

    // HTML保存（デバッグ用）
    if (SAVE_HTML) {
      const safeName = companyName.replace(/[^a-zA-Z0-9]/g, "_").substring(0, 50);
      const htmlPath = path.join(HTML_OUTPUT_DIR, `${safeName}_${Date.now()}.html`);
      const html = await page.content();
      fs.writeFileSync(htmlPath, html, "utf-8");
      log(`  💾 HTML saved: ${htmlPath}`);
    }

    // ① 詳細ページ上部のサマリー
    // 企業名
    try {
      const nameEl = page.locator("h1, .company-name, [class*='company-name']").first();
      if (await nameEl.count() > 0) {
        record.name = (await nameEl.textContent())?.trim() || "";
      }
    } catch (e) {
      // 無視
    }

    // カナ
    try {
      const kanaEl = page.locator("text=/^[ァ-ヶー]+$/").first();
      if (await kanaEl.count() > 0) {
        record.kana = (await kanaEl.textContent())?.trim() || "";
      }
    } catch (e) {
      // 無視
    }

    // 法人番号
    try {
      const corporateNumberText = await page.locator("text=/法人番号/").first().textContent();
      if (corporateNumberText) {
        const match = corporateNumberText.match(/法人番号[：:]\s*(\d{13})/);
        if (match) {
          record.corporateNumber = match[1];
        }
      }
    } catch (e) {
      // 無視
    }

    // 更新日
    try {
      const updateDateText = await page.locator("text=/更新日/").first().textContent();
      if (updateDateText) {
        const match = updateDateText.match(/更新日[：:]\s*(\d{4}年\d{1,2}月\d{1,2}日)/);
        if (match) {
          record.updateDate = normalizeDate(match[1]);
        }
      }
    } catch (e) {
      // 無視
    }

    // バッジ/タグ
    const badges: string[] = [];
    try {
      const badgeEls = page.locator("[class*='badge'], [class*='tag'], .chip");
      const count = await badgeEls.count();
      for (let i = 0; i < count; i++) {
        const text = await badgeEls.nth(i).textContent();
        if (text && text.trim()) {
          badges.push(text.trim());
        }
      }
      if (badges.length > 0) {
        record.badges = badges.join("|");
        record.tags = badges.join("|");
      }
    } catch (e) {
      // 無視
    }

    // ② 基本情報テーブル
    try {
      // 「基本情報」セクションを探す
      const basicInfoSection = page.locator("text=/基本情報/").first();
      let table;
      
      if (await basicInfoSection.count() > 0) {
        // 基本情報セクション内のテーブルを取得
        table = basicInfoSection.locator("..").locator("table").first();
        if (await table.count() === 0) {
          table = basicInfoSection.locator("..").locator("[class*='table']").first();
        }
      }
      
      // 基本情報セクションが見つからない場合は、最初のテーブルを使用
      if (!table || (await table.count()) === 0) {
        table = page.locator("table, [class*='table'], [class*='info-table']").first();
      }
      
      if (await table.count() > 0) {
        // 各行を処理
        const rows = table.locator("tr, [class*='row']");
        const rowCount = await rows.count();

        for (let i = 0; i < rowCount; i++) {
          const row = rows.nth(i);
          const label = (await row.locator("th, [class*='label'], [class*='key'], dt").first().textContent())?.trim() || "";
          const value = (await row.locator("td, [class*='value'], [class*='data'], dd").first().textContent())?.trim() || "";

          if (label.includes("企業名") && !record.name) {
            record.name = value;
          } else if (label.includes("企業名カナ") || label.includes("カナ")) {
            record.kana = value;
          } else if (label.includes("法人番号") && !record.corporateNumber) {
            record.corporateNumber = value.replace(/[^\d]/g, "");
          } else if (label.includes("本社郵便番号") || label.includes("郵便番号")) {
            record.postalCode = value.replace(/[^\d]/g, "");
          } else if (label.includes("本社住所") || label.includes("住所")) {
            record.address = value;
            record.headquartersAddress = value;
          } else if (label.includes("創業")) {
            record.founding = normalizeDate(value);
            if (record.founding) {
              const yearMatch = record.founding.match(/^(\d{4})/);
              if (yearMatch) {
                record.foundingYear = yearMatch[1];
              }
            }
          } else if (label.includes("設立")) {
            record.dateOfEstablishment = normalizeDate(value);
            record.established = normalizeDate(value);
          } else if (label.includes("資本金")) {
            record.capitalStock = normalizeAmount(value);
          } else if (label.includes("従業員数")) {
            record.employeeCount = normalizeEmployeeCount(value);
            record.employeeNumber = normalizeEmployeeCount(value);
          } else if (label.includes("上場・非上場") || label.includes("上場")) {
            // listing は「上場・非上場」の値を取得（そのまま使用）
            record.listing = value.trim();
          } else if (label.includes("ホームページ") || label.includes("URL") || label.includes("ウェブサイト")) {
            // companyUrl は「ホームページ」リンクを優先して取得
            try {
              const link = row.locator("a").first();
              if (await link.count() > 0) {
                const href = (await link.getAttribute("href")) || "";
                if (href) {
                  record.companyUrl = href;
                } else {
                  record.companyUrl = value.trim();
                }
              } else {
                record.companyUrl = value.trim();
              }
            } catch (e) {
              record.companyUrl = value.trim();
            }
          } else if (label.includes("問い合せ") || label.includes("問い合わせ") || label.includes("お問い合わせ") || label.includes("問い合わせページ")) {
            // contactFormUrl は「問い合わせページ」リンクを優先して取得
            try {
              const link = row.locator("a").first();
              if (await link.count() > 0) {
                const href = (await link.getAttribute("href")) || "";
                if (href) {
                  record.contactFormUrl = href;
                } else {
                  record.contactFormUrl = value.trim();
                }
              } else {
                record.contactFormUrl = value.trim();
              }
            } catch (e) {
              record.contactFormUrl = value.trim();
            }
          } else if (label.includes("電話番号") || label.includes("電話")) {
            record.phoneNumber = value.replace(/[^\d-]/g, "");
            record.contactPhoneNumber = value.replace(/[^\d-]/g, "");
          } else if (label.includes("代表者")) {
            record.representativeName = value;
          }
        }
      }
    } catch (e) {
      log(`  ⚠️  基本情報テーブルの取得に失敗: ${e}`, true);
    }

    // ③ 業種・事業情報
    try {
      const industrySection = page.locator("text=/業種・事業情報/").first();
      if (await industrySection.count() > 0) {
        const sectionContainer = industrySection.locator("..").first();
        
        // 業種セクション
        const industryLabel = sectionContainer.locator("text=/業種/").first();
        if (await industryLabel.count() > 0) {
          const industryContainer = industryLabel.locator("..").first();
          const industryChips: string[] = [];
          
          // チップ/バッジ形式の要素を取得
          const industryEls = industryContainer.locator("[class*='chip'], [class*='tag'], [class*='badge'], span, div").filter({ hasText: /.+/ });
          const industryCount = await industryEls.count();
          
          for (let i = 0; i < industryCount; i++) {
            const text = (await industryEls.nth(i).textContent())?.trim() || "";
            if (text && text.length > 0 && !badges.includes(text) && !text.includes("業種") && !text.includes("事業内容")) {
              industryChips.push(text);
            }
          }
          
          if (industryChips.length > 0) {
            record.industries = industryChips.join("|");
            record.industry = industryChips[0]; // 最初の1つを主要業種として
          }
        }

        // 事業内容セクション
        const businessLabel = sectionContainer.locator("text=/事業内容/").first();
        if (await businessLabel.count() > 0) {
          const businessContainer = businessLabel.locator("..").first();
          const businessChips: string[] = [];
          
          // チップ/バッジ形式の要素を取得
          const businessEls = businessContainer.locator("[class*='chip'], [class*='tag'], [class*='badge'], span, div").filter({ hasText: /.+/ });
          const businessCount = await businessEls.count();
          
          for (let i = 0; i < businessCount; i++) {
            const text = (await businessEls.nth(i).textContent())?.trim() || "";
            if (text && text.length > 0 && !text.includes("事業内容")) {
              businessChips.push(text);
            }
          }
          
          if (businessChips.length > 0) {
            record.businessItems = businessChips.join("|");
            record.businessDescriptions = businessChips.join("|");
          }
        }
      } else {
        // 業種・事業情報セクションが見つからない場合、ページ全体から業種チップを探す
        const industryChips: string[] = [];
        const industryEls = page.locator("[class*='industry'], [class*='chip'], [class*='tag']").filter({ hasText: /.+/ });
        const industryCount = await industryEls.count();
        
        for (let i = 0; i < industryCount; i++) {
          const text = (await industryEls.nth(i).textContent())?.trim() || "";
          if (text && text.length > 0 && !badges.includes(text)) {
            industryChips.push(text);
          }
        }
        
        if (industryChips.length > 0) {
          record.industries = industryChips.join("|");
          record.industry = industryChips[0];
        }
      }
    } catch (e) {
      log(`  ⚠️  業種・事業情報の取得に失敗: ${e}`, true);
    }

    // ④ 業績情報（売上、最大直近5期分）
    try {
      const revenueSection = page.locator("text=/業績情報/, text=/売上/").first();
      if (await revenueSection.count() > 0) {
        const revenueTable = page.locator("table").filter({ hasText: /決算年|売上/ }).first();
        if (await revenueTable.count() > 0) {
          // テーブルヘッダーから期を取得
          const headers = revenueTable.locator("th, thead td");
          const headerCount = await headers.count();
          const fiscalMonths: string[] = [];
          const revenues: string[] = [];

          // ヘッダー行をスキップしてデータ行を取得
          const dataRows = revenueTable.locator("tbody tr, tr").filter({ hasNotText: /決算年|売上/ });
          const rowCount = await dataRows.count();

          if (rowCount > 0) {
            const firstRow = dataRows.first();
            const cells = firstRow.locator("td");
            const cellCount = await cells.count();

            // 最初の行が決算年、2行目が売上と仮定
            for (let i = 1; i < cellCount && i <= 5; i++) {
              const cellText = (await cells.nth(i).textContent())?.trim() || "";
              if (cellText && cellText !== "-") {
                // 期の形式を正規化（例: "2024年3月" → "2024-03"）
                const fiscalMatch = cellText.match(/(\d{4})年(\d{1,2})月/);
                if (fiscalMatch) {
                  const year = fiscalMatch[1];
                  const month = fiscalMatch[2].padStart(2, "0");
                  fiscalMonths.push(`${year}-${month}`);
                }
              }
            }

            // 2行目（売上行）を取得
            if (rowCount > 1) {
              const revenueRow = dataRows.nth(1);
              const revenueCells = revenueRow.locator("td");
              const revenueCellCount = await revenueCells.count();

              for (let i = 1; i < revenueCellCount && i <= 5; i++) {
                const cellText = (await revenueCells.nth(i).textContent())?.trim() || "";
                revenues.push(normalizeAmount(cellText));
              }
            }
          }

          // より汎用的な方法：テーブル全体を解析
          if (fiscalMonths.length === 0) {
            const allRows = revenueTable.locator("tr");
            const allRowCount = await allRows.count();

            for (let rowIdx = 0; rowIdx < allRowCount; rowIdx++) {
              const row = allRows.nth(rowIdx);
              const rowText = (await row.textContent())?.trim() || "";
              if (rowText.includes("決算年") || rowText.includes("売上")) {
                const cells = row.locator("td, th");
                const cellCount = await cells.count();

                if (rowText.includes("決算年")) {
                  for (let i = 1; i < cellCount && i <= 5; i++) {
                    const cellText = (await cells.nth(i).textContent())?.trim() || "";
                    if (cellText && cellText !== "-") {
                      const fiscalMatch = cellText.match(/(\d{4})年(\d{1,2})月/);
                      if (fiscalMatch) {
                        const year = fiscalMatch[1];
                        const month = fiscalMatch[2].padStart(2, "0");
                        fiscalMonths.push(`${year}-${month}`);
                      }
                    }
                  }
                } else if (rowText.includes("売上")) {
                  for (let i = 1; i < cellCount && i <= 5; i++) {
                    const cellText = (await cells.nth(i).textContent())?.trim() || "";
                    revenues.push(normalizeAmount(cellText));
                  }
                }
              }
            }
          }

          // 結果を設定
          if (fiscalMonths.length > 0) {
            record.fiscalMonth1 = fiscalMonths[0] || "";
            record.fiscalMonth2 = fiscalMonths[1] || "";
            record.fiscalMonth3 = fiscalMonths[2] || "";
            record.fiscalMonth4 = fiscalMonths[3] || "";
            record.fiscalMonth5 = fiscalMonths[4] || "";
          }
          if (revenues.length > 0) {
            record.revenue1 = revenues[0] || "";
            record.revenue2 = revenues[1] || "";
            record.revenue3 = revenues[2] || "";
            record.revenue4 = revenues[3] || "";
            record.revenue5 = revenues[4] || "";
            // 最新の売上を設定
            for (let i = 0; i < revenues.length; i++) {
              if (revenues[i] && revenues[i] !== "") {
                record.latestRevenue = revenues[i];
                record.revenue = revenues[i];
                break;
              }
            }
          }
        }
      }
    } catch (e) {
      log(`  ⚠️  業績情報の取得に失敗: ${e}`, true);
    }

    // companyIdを設定
    record.companyId = generateCompanyId(record.corporateNumber, record.name, record.address);

    // タイムスタンプ
    const now = new Date().toISOString();
    record.createdAt = now;
    record.updatedAt = now;

    return record;
  } catch (error) {
    log(`  ❌ 企業詳細の取得中にエラー: ${error}`, true);
    // 最低限の情報を設定
    record.companyId = generateCompanyId(null, companyName, "");
    record.name = companyName;
    return record;
  }
}

/**
 * ログイン処理（再利用可能）
 */
async function performLogin(page: Page): Promise<void> {
  log("🔐 ログイン処理中...");
  
  // ログインページに遷移
  await page.goto(CNAVI_URL, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForTimeout(3000);
  
  // より柔軟なセレクタでログインフォームの要素を探す
  log("  🔍 ログインフォームを検索中...");
  
  // 複数のパターンを試す
  let emailInput = null;
  let passwordInput = null;
  let loginButton = null;
  
  // パターン1: 一般的なセレクタ
  try {
    emailInput = page.locator('input[type="email"]').first();
    if (await emailInput.count() > 0) {
      await emailInput.waitFor({ state: "visible", timeout: 5000 });
      log("  ✅ メール入力欄を発見（パターン1）");
    } else {
      emailInput = null;
    }
  } catch (e) {
    emailInput = null;
  }
  
  // パターン2: name属性で検索
  if (!emailInput) {
    try {
      emailInput = page.locator('input[name*="email"], input[name*="mail"], input[name*="login"], input[name*="user"]').first();
      if (await emailInput.count() > 0) {
        await emailInput.waitFor({ state: "visible", timeout: 5000 });
        log("  ✅ メール入力欄を発見（パターン2）");
      } else {
        emailInput = null;
      }
    } catch (e) {
      emailInput = null;
    }
  }
  
  // パターン3: id属性で検索
  if (!emailInput) {
    try {
      emailInput = page.locator('input[id*="email"], input[id*="mail"], input[id*="login"], input[id*="user"]').first();
      if (await emailInput.count() > 0) {
        await emailInput.waitFor({ state: "visible", timeout: 5000 });
        log("  ✅ メール入力欄を発見（パターン3）");
      } else {
        emailInput = null;
      }
    } catch (e) {
      emailInput = null;
    }
  }
  
  // パターン4: テキスト入力欄を全て試す
  if (!emailInput) {
    try {
      const allInputs = page.locator('input[type="text"], input:not([type])');
      const inputCount = await allInputs.count();
      log(`  🔍 テキスト入力欄を ${inputCount} 個発見`);
      if (inputCount > 0) {
        emailInput = allInputs.first();
        await emailInput.waitFor({ state: "visible", timeout: 5000 });
        log("  ✅ メール入力欄を発見（パターン4: 最初のテキスト入力欄）");
      }
    } catch (e) {
      emailInput = null;
    }
  }
  
  if (!emailInput) {
    throw new Error("ログインフォームのメール入力欄が見つかりません。");
  }
  
  // パスワード入力欄を探す
  try {
    passwordInput = page.locator('input[type="password"]').first();
    if (await passwordInput.count() > 0) {
      await passwordInput.waitFor({ state: "visible", timeout: 5000 });
      log("  ✅ パスワード入力欄を発見");
    }
  } catch (e) {
    throw new Error("パスワード入力欄が見つかりません。");
  }
  
  // ログインボタンを探す
  try {
    loginButton = page.locator('button[type="submit"], input[type="submit"], button:has-text("ログイン"), button:has-text("Login")').first();
    if (await loginButton.count() === 0) {
      loginButton = page.locator('button, [role="button"]').filter({ hasText: /ログイン|Login|Sign in/i }).first();
    }
    if (await loginButton.count() > 0) {
      await loginButton.waitFor({ state: "visible", timeout: 5000 });
      log("  ✅ ログインボタンを発見");
    }
  } catch (e) {
    throw new Error("ログインボタンが見つかりません。");
  }
  
  // ログイン情報を入力
  log("  ✍️  ログイン情報を入力中...");
  await emailInput.fill(LOGIN_ID);
  await passwordInput.fill(PASSWORD);
  await page.waitForTimeout(500);
  
  // ログインボタンをクリック
  await loginButton.click();
  await page.waitForLoadState("networkidle");
  await page.waitForTimeout(3000);
  
  log("  ✅ ログイン完了");
}

/**
 * セッションが有効かチェック（ログアウトされていないか）
 */
async function checkSessionValid(page: Page): Promise<boolean> {
  try {
    const currentUrl = page.url();
    // ログインページにリダイレクトされている場合は無効
    if (currentUrl.includes("/login") || currentUrl.includes("/signin")) {
      return false;
    }
    // ページが読み込まれているかチェック
    const body = page.locator("body");
    const bodyCount = await body.count();
    return bodyCount > 0;
  } catch (e) {
    return false;
  }
}

/**
 * セッションを維持するためのアクティビティ（定期的に呼び出す）
 */
async function maintainSession(page: Page): Promise<void> {
  try {
    // ページを少しスクロールしてアクティビティを維持
    await page.evaluate(() => {
      window.scrollBy(0, 1);
      window.scrollBy(0, -1);
    });
  } catch (e) {
    // エラーは無視
  }
}

/**
 * メイン処理
 */
async function main() {
  // ログファイルを初期化
  initLogFile();
  
  log("🚀 CNAVI スクレイピング開始\n");
  log("📌 企業リスト表示までは通常モード、その後はバックグラウンドモードで実行します\n");

  // 出力ディレクトリを作成
  if (!fs.existsSync("out")) {
    fs.mkdirSync("out", { recursive: true });
  }
  if (SAVE_HTML && !fs.existsSync(HTML_OUTPUT_DIR)) {
    fs.mkdirSync(HTML_OUTPUT_DIR, { recursive: true });
  }

  // CSVライターを初期化
  // 既存ファイルがある場合は削除して新規作成（ヘッダーを確実に含めるため）
  const fileExists = fs.existsSync(OUTPUT_CSV);
  if (fileExists) {
    log(`📝 既存のCSVファイルを削除して新規作成します: ${OUTPUT_CSV}`);
    fs.unlinkSync(OUTPUT_CSV);
  }
  
  const csvWriter = createObjectCsvWriter({
    path: OUTPUT_CSV,
    header: CSV_HEADERS,
    append: false, // 常に新規作成（ヘッダーを含める）
  });

  // 最初は通常モード（ブラウザ表示あり）で起動
  log("🌐 ブラウザを起動中（通常モード）...");
  let browser = await chromium.launch({
    headless: false,
    slowMo: 100,
  });

  let context = await browser.newContext({
    viewport: { width: 1280, height: 720 },
  });

  let page = await context.newPage();

  try {
    // ログインページに遷移
    log("📝 ログインページに遷移中...");
    await page.goto(CNAVI_URL, { waitUntil: "domcontentloaded", timeout: 60000 });
    
    // ページが完全に読み込まれるまで待機
    await page.waitForTimeout(3000);
    
    // 現在のURLを確認
    const loginPageUrl = page.url();
    log(`  📍 現在のURL: ${loginPageUrl}`);

    // ログイン処理（関数を使用）
    await performLogin(page);

    // Enterキー待ち（企業リスト表示まで手動操作）
    await waitForEnter();

    // 企業リストが表示されていることを確認
    log("⏳ 企業リストの表示を確認中...");
    
    // テーブルまたは企業リストが表示されるまで待機
    try {
      // 複数のセレクタパターンでテーブルを待機
      await Promise.race([
        page.waitForSelector('table', { timeout: 10000 }),
        page.waitForSelector('[class*="table"]', { timeout: 10000 }),
        page.waitForSelector('[role="table"]', { timeout: 10000 }),
        page.waitForSelector('tbody', { timeout: 10000 })
      ]);
      log("✅ テーブルが表示されました");
    } catch (e) {
      log("⚠️  テーブルが見つかりません。続行します...");
    }
    
    // 追加の待機時間（Reactコンポーネントの完全なレンダリングを待つ）
    await page.waitForTimeout(2000);
    
    log("✅ 準備完了\n");

    // 企業リストの取得を開始
    log("\n📊 スクレイピング開始...\n");

    let currentPage = 1;
    let totalCompanies = 0;
    let errorCount = 0;

    while (true) {
      log(`\n📄 ページ ${currentPage} を処理中...`);

      // セッション維持のためのアクティビティ
      await maintainSession(page);

      // 企業リストが表示されるまで待機（Reactアプリのレンダリングを待つ）
      await page.waitForTimeout(3000);
      
      // テーブルまたは企業リストが表示されるまで待機
      try {
        // テーブルが表示されるまで待機
        await page.waitForSelector('table, [class*="table"], [role="table"]', { timeout: 10000 });
        log("  ✅ テーブルが表示されました");
      } catch (e) {
        log("  ⚠️  テーブルが見つかりません。続行します...");
      }
      
      // 追加の待機時間（Reactコンポーネントのレンダリングを待つ）
      await page.waitForTimeout(2000);

      // 企業名リンクを取得（テーブルの企業名列のリンクを直接取得）
      log("  🔍 企業リストを検索中...");
      
      // デバッグ用: ページのHTMLを保存
      if (SAVE_HTML) {
        const listHtml = await page.content();
        const listPath = path.join(HTML_OUTPUT_DIR, `company_list_page${currentPage}_${Date.now()}.html`);
        fs.writeFileSync(listPath, listHtml, "utf-8");
        log(`  💾 企業リストページHTMLを保存: ${listPath}`);
      }
      
      // 除外するリンクテキスト
      const excludeTexts = ["こちらから", "サイトを見る", "地図で見る", "View", "見る", "詳細", "次", "前", ">>", "<<", "次へ", "前へ", "与信調査", "新聞調査", "コンプラチェック"];
      
      // validLinksArrayを保持するための変数（スコープ外で定義）
      let validLinksArray: Array<{ locator: any; text: string; href: string }> = [];
      
      // まず、企業名列のインデックスを特定
      let companyColumnIndex = -1;
      try {
        // テーブルヘッダーを探す（複数のパターンを試す）
        const headerSelectors = [
          'table thead th',
          'table thead tr th',
          'table th',
          '[class*="table"] thead th',
          '[class*="table"] th'
        ];
        
        for (const selector of headerSelectors) {
          const headers = page.locator(selector);
          const headerCount = await headers.count();
          if (headerCount > 0) {
            log(`  🔍 ヘッダーを発見（${selector}）: ${headerCount} 列`);
            for (let i = 0; i < headerCount; i++) {
              const headerText = (await headers.nth(i).textContent())?.trim() || "";
              log(`    列 ${i}: "${headerText}"`);
              if (headerText.includes("企業名") || headerText.includes("Company Name") || headerText.includes("企業")) {
                companyColumnIndex = i;
                log(`  ✅ 企業名列を発見: 列インデックス ${i} ("${headerText}")`);
                break;
              }
            }
            if (companyColumnIndex >= 0) break;
          }
        }
      } catch (e) {
        log(`  ⚠️  ヘッダーの検索に失敗: ${e}`);
      }
      
      let companyLinks;
      let linkCount = 0;
      
      // パターン1: 企業名列のインデックスが見つかった場合、その列のリンクを取得
      if (companyColumnIndex >= 0) {
        // テーブル行を取得（tbody内、またはthead以外の行）
        const rowSelectors = [
          `table tbody tr td:nth-child(${companyColumnIndex + 1}) a`,
          `table tr:not(thead tr) td:nth-child(${companyColumnIndex + 1}) a`,
          `table tr td:nth-child(${companyColumnIndex + 1}) a`,
          `[class*="table"] tbody tr td:nth-child(${companyColumnIndex + 1}) a`,
          `[class*="table"] tr td:nth-child(${companyColumnIndex + 1}) a`,
          `[role="table"] tbody tr td:nth-child(${companyColumnIndex + 1}) a`,
          `[role="table"] tr td:nth-child(${companyColumnIndex + 1}) a`
        ];
        
        for (const selector of rowSelectors) {
          companyLinks = page.locator(selector).filter({ 
            hasText: /.+/,
            hasNotText: new RegExp(excludeTexts.join("|"), "i")
          });
          linkCount = await companyLinks.count();
          if (linkCount > 0) {
            log(`  ✅ 企業名列から ${linkCount} 件のリンクを発見（パターン1: ${selector}）`);
            break;
          }
        }
      }
      
      // パターン2: テーブルの最初の列（企業名列が最初の列の場合）
      if (linkCount === 0) {
        const firstColumnSelectors = [
          'table tbody tr td:first-child a',
          'table tr:not(thead tr) td:first-child a',
          'table tr td:first-child a',
          '[class*="table"] tbody tr td:first-child a',
          '[class*="table"] tr td:first-child a',
          '[role="table"] tbody tr td:first-child a',
          '[role="table"] tr td:first-child a'
        ];
        
        for (const selector of firstColumnSelectors) {
          companyLinks = page.locator(selector).filter({ 
            hasText: /.+/,
            hasNotText: new RegExp(excludeTexts.join("|"), "i")
          });
          linkCount = await companyLinks.count();
          if (linkCount > 0) {
            log(`  ✅ 最初の列から ${linkCount} 件のリンクを発見（パターン2: ${selector}）`);
            break;
          }
        }
      }
      
      // パターン3: テーブル内のすべてのリンクから企業名らしいものを探す
      if (linkCount === 0) {
        log("  🔍 テーブル内の全リンクを検索中...");
        // Reactアプリの場合、より広範囲で検索（aタグだけでなく、クリック可能な要素も含む）
        // 企業名列のセル内の要素を探す（div, span, aなど）
        const allTableLinks = page.locator('table tbody tr td:first-child *, table tr:not(thead tr) td:first-child *, table a, [class*="table"] a, [role="table"] a');
        const allCount = await allTableLinks.count();
        log(`  🔍 テーブル内の全要素数: ${allCount}`);
        
        if (allCount > 0) {
          // 各要素をチェック
          for (let i = 0; i < allCount; i++) {
            const link = allTableLinks.nth(i);
            const text = (await link.textContent())?.trim() || "";
            const href = (await link.getAttribute("href")) || "";
            const tagName = await link.evaluate((el) => el.tagName.toLowerCase());
            
            // 除外チェック
            const shouldExclude = excludeTexts.some(exclude => text.includes(exclude)) ||
                                  href.includes("/planchange") ||
                                  href.includes("/help") ||
                                  href.startsWith("#") ||
                                  href.startsWith("javascript:") ||
                                  text.length < 2;
            
            // 企業名らしいリンクかチェック（株式会社、有限会社、合同会社などが含まれる）
            const looksLikeCompanyName = /(株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|一般財団法人|特定非営利活動法人|学校法人|医療法人|社会福祉法人|協同組合|協業組合|企業組合|事業協同組合|信用組合|信用金庫|労働金庫|農業協同組合|生活協同組合|漁業協同組合|森林組合|商工組合|商店街振興組合|中小企業等協同組合|協同組合連合会|企業|会社)/.test(text);
            
            if (!shouldExclude && looksLikeCompanyName) {
              validLinksArray.push({ locator: link, text, href });
              if (validLinksArray.length <= 10 || validLinksArray.length % 50 === 0) {
                log(`    ✅ 有効なリンク ${validLinksArray.length}: "${text}" (tag: ${tagName}, href: ${href || "なし"})`);
              }
            }
          }
          
          if (validLinksArray.length > 0) {
            log(`  ✅ ${validLinksArray.length} 件の有効な企業リンクを発見（パターン3）`);
            
            // 各リンクの行インデックスを記録
            for (let idx = 0; idx < validLinksArray.length; idx++) {
              const linkInfo = validLinksArray[idx];
              try {
                // リンクがどの行にあるかを確認
                const rowIndex = await linkInfo.locator.evaluate((el: any) => {
                  let parent = el.parentElement;
                  let rowIndex = -1;
                  while (parent) {
                    if (parent.tagName === 'TR') {
                      // テーブル内の行インデックスを取得
                      const tbody = parent.closest('tbody');
                      if (tbody) {
                        const rows = Array.from(tbody.querySelectorAll('tr'));
                        rowIndex = rows.indexOf(parent);
                      } else {
                        const table = parent.closest('table');
                        if (table) {
                          const rows = Array.from(table.querySelectorAll('tr'));
                          rowIndex = rows.indexOf(parent);
                        }
                      }
                      break;
                    }
                    parent = parent.parentElement;
                  }
                  return rowIndex;
                });
                
                // 行インデックスを保存
                (linkInfo as any).rowIndex = rowIndex;
                if (idx < 5) {
                  log(`    企業 ${idx + 1} (${linkInfo.text}): 行インデックス ${rowIndex}`);
                }
              } catch (e) {
                // 行インデックスが取得できない場合は-1を設定
                (linkInfo as any).rowIndex = -1;
              }
            }
            
            linkCount = validLinksArray.length;
            // validLinksArrayを使用するため、companyLinksは使用しない（後で直接validLinksArrayから取得）
          }
        }
      }
      
      // パターン4: ページ全体から企業名らしいテキストを含むクリック可能な要素を探す（最後の手段）
      if (linkCount === 0) {
        log("  🔍 ページ全体から企業名らしい要素を検索中（パターン4）...");
        // 企業名パターンを含むテキストを持つ要素を探す
        const companyNamePattern = /(株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|一般財団法人|特定非営利活動法人|学校法人|医療法人|社会福祉法人|協同組合|協業組合|企業組合|事業協同組合|信用組合|信用金庫|労働金庫|農業協同組合|生活協同組合|漁業協同組合|森林組合|商工組合|商店街振興組合|中小企業等協同組合|協同組合連合会)/;
        
        // テキストノードを含むクリック可能な要素を探す
        const clickableElements = page.locator('a, [role="button"], [onclick], button, [class*="click"], [class*="link"]').filter({
          hasText: companyNamePattern
        });
        const clickableCount = await clickableElements.count();
        log(`  🔍 企業名パターンを含むクリック可能要素数: ${clickableCount}`);
        
        if (clickableCount > 0) {
          // テーブル内の要素のみをフィルタ
          const tableElements: any[] = [];
          for (let i = 0; i < clickableCount; i++) {
            const elem = clickableElements.nth(i);
            const isInTable = await elem.locator('..').locator('table, [class*="table"], [role="table"]').count() > 0 ||
                             await page.locator('table, [class*="table"], [role="table"]').locator(`xpath=ancestor::*[self::table or contains(@class, "table") or @role="table"]`).count() > 0;
            
            if (isInTable) {
              const text = (await elem.textContent())?.trim() || "";
              const shouldExclude = excludeTexts.some(exclude => text.includes(exclude));
              if (!shouldExclude) {
                tableElements.push(elem);
                log(`    ✅ テーブル内の要素 ${tableElements.length}: "${text}"`);
              }
            }
          }
          
          if (tableElements.length > 0) {
            log(`  ✅ ${tableElements.length} 件の有効な企業リンクを発見（パターン4）`);
            linkCount = tableElements.length;
            // 簡易実装として、同じ条件で再取得
            companyLinks = clickableElements.filter({
              hasText: companyNamePattern,
              hasNotText: new RegExp(excludeTexts.join("|"), "i")
            });
          }
        }
      }

      if (linkCount === 0) {
        log("⚠️  企業リンクが見つかりません。30秒待機して終了します...");
        await page.waitForTimeout(30000);
        break;
      }

      log(`  📋 ${linkCount} 件の企業が見つかりました`);

      // 各企業を処理
      for (let i = 0; i < linkCount; i++) {
        try {
          // リストページに戻る（毎回確実に）
          if (i > 0) {
            await page.goBack({ waitUntil: "networkidle" });
            await page.waitForTimeout(3000); // ページが完全に読み込まれるまで待機
            
            // セッションが有効かチェック
            const sessionValid = await checkSessionValid(page);
            if (!sessionValid) {
              log(`  ⚠️  ログアウトされています。再ログインを試みます...`);
              try {
                await performLogin(page);
                // 再ログイン後、企業リストページに戻る必要がある
                // ユーザーに手動でリストページを表示してもらう必要がある
                log(`  ⚠️  再ログイン完了。企業リストページを表示してください。`);
                log(`  ⚠️  準備ができたら Enter キーを押してください...`);
                await waitForEnter();
                await page.waitForTimeout(2000);
              } catch (e) {
                log(`  ❌ 再ログインに失敗しました: ${e}`, true);
                log(`  ⚠️  企業 ${i + 1} 以降の処理をスキップします。`);
                break; // ループを抜ける
              }
            }
            
            // セッション維持のためのアクティビティ
            await maintainSession(page);
            
            // テーブルが再表示されるまで待機（最大10秒）
            let tableFound = false;
            for (let retry = 0; retry < 5; retry++) {
              try {
                await page.waitForSelector('table, [class*="table"], [role="table"]', { timeout: 2000 });
                const table = page.locator('table, [class*="table"], [role="table"]').first();
                const tableCount = await table.count();
                if (tableCount > 0) {
                  tableFound = true;
                  break;
                }
              } catch (e) {
                // リトライ
                await page.waitForTimeout(1000);
                await maintainSession(page);
              }
            }
            
            if (!tableFound) {
              log(`  ⚠️  テーブルが見つかりません。ログアウトされている可能性があります。`);
              // 再ログインを試みる
              try {
                await performLogin(page);
                log(`  ⚠️  再ログイン完了。企業リストページを表示してください。`);
                log(`  ⚠️  準備ができたら Enter キーを押してください...`);
                await waitForEnter();
                await page.waitForTimeout(2000);
              } catch (e) {
                log(`  ❌ 再ログインに失敗しました: ${e}`, true);
                log(`  ⚠️  企業 ${i + 1} 以降の処理をスキップします。`);
                break; // ループを抜ける
              }
            }
            
            await page.waitForTimeout(1000);
          }

          let currentLink;
          let companyName: string;

          // validLinksArrayが使用可能な場合（パターン3で見つかった場合）
          if (validLinksArray.length > 0 && i < validLinksArray.length) {
            const linkInfo = validLinksArray[i];
            companyName = linkInfo.text;
            const rowIndex = (linkInfo as any).rowIndex;
            
            log(`  🔍 企業名 "${companyName}" の要素を検索中... (行インデックス: ${rowIndex})`);
            
            // 最初の企業（i=0）の場合は、リストページに戻る必要がないので、直接要素を使用
            let found = false;
            
            if (i === 0) {
              // 最初の企業の場合は、保存されたlocatorを直接使用
              try {
                const linkCount = await linkInfo.locator.count();
                if (linkCount > 0) {
                  currentLink = linkInfo.locator;
                  found = true;
                  log(`  ✅ 最初の企業のリンクを直接使用`);
                }
              } catch (e) {
                log(`  ⚠️  最初の企業のリンク取得エラー: ${e}`);
              }
            }
            
            // リストページに戻った後、企業名テキストで直接要素を探す
            if (!found) {
              // パターン1: 行インデックスが記録されている場合、その行の企業名列をクリック（最も確実）
              if (rowIndex >= 0) {
                try {
                  const rows = page.locator('table tbody tr, table tr:not(thead tr)');
                  const rowCount = await rows.count();
                  
                  if (rowIndex < rowCount) {
                    const targetRow = rows.nth(rowIndex);
                    const firstCell = targetRow.locator('td:first-child');
                    const cellCount = await firstCell.count();
                    
                    if (cellCount > 0) {
                      const cell = firstCell.first();
                      const cellText = (await cell.textContent())?.trim() || "";
                      
                      // 企業名が一致するか確認
                      if (cellText === companyName || cellText.trim() === companyName.trim() || 
                          (cellText.includes(companyName) && companyName.length > 3)) {
                        log(`  ✅ 企業名セルを発見（行インデックス ${rowIndex}）: "${cellText}"`);
                        
                        // セル内のaタグを探す
                        const linkInCell = cell.locator('a').first();
                        const linkCount = await linkInCell.count();
                        
                        if (linkCount > 0) {
                          currentLink = linkInCell;
                          found = true;
                          log(`  ✅ 企業名リンクを発見（行インデックス ${rowIndex}、セル内のaタグ）`);
                        } else {
                          // aタグがない場合は、セル自体をクリック
                          currentLink = cell;
                          found = true;
                          log(`  ✅ 企業名セルを発見（行インデックス ${rowIndex}、セル自体をクリック）`);
                        }
                      }
                    }
                  }
                } catch (e) {
                  log(`  ⚠️  行インデックス検索エラー: ${e}`);
                }
              }
              
              // パターン2: テーブルの最初の列から企業名テキストを含むセルを探す
              if (!found) {
              try {
                const firstColumnCells = page.locator('table tbody tr td:first-child, table tr:not(thead tr) td:first-child');
                const cellCount = await firstColumnCells.count();
                log(`  🔍 最初の列のセル数: ${cellCount}`);
                
                for (let j = 0; j < cellCount; j++) {
                  const cell = firstColumnCells.nth(j);
                  const cellText = (await cell.textContent())?.trim() || "";
                  
                  // 企業名が完全一致、または含まれているかチェック
                  if (cellText === companyName || cellText.trim() === companyName.trim() || 
                      (cellText.includes(companyName) && companyName.length > 3)) {
                    log(`  ✅ 企業名セルを発見（行 ${j + 1}）: "${cellText}"`);
                    
                    // セル内のaタグを探す
                    const linkInCell = cell.locator('a').first();
                    const linkCount = await linkInCell.count();
                    
                    if (linkCount > 0) {
                      currentLink = linkInCell;
                      found = true;
                      log(`  ✅ 企業名リンクを発見（セル内のaタグ）`);
                      break;
                    } else {
                      // aタグがない場合は、セル自体をクリック
                      currentLink = cell;
                      found = true;
                      log(`  ✅ 企業名セルを発見（セル自体をクリック）`);
                      break;
                    }
                  }
                }
              } catch (e) {
                log(`  ⚠️  セル検索エラー: ${e}`);
              }
              }
              
              // パターン3: 企業名テキストを含むaタグを直接探す
              if (!found) {
              try {
                const textLinks = page.locator(`a`).filter({
                  hasText: companyName
                });
                const textLinkCount = await textLinks.count();
                log(`  🔍 企業名を含むaタグ数: ${textLinkCount}`);
                
                if (textLinkCount > 0) {
                  // テーブル内の要素のみを選択
                  for (let j = 0; j < textLinkCount; j++) {
                    const link = textLinks.nth(j);
                    const linkText = (await link.textContent())?.trim() || "";
                    
                    // テキストが一致するか確認
                    if (linkText === companyName || linkText.trim() === companyName.trim()) {
                      // テーブル内かどうかを確認
                      try {
                        const isInTable = await link.evaluate((el: any) => {
                          let parent = el.parentElement;
                          while (parent) {
                            if (parent.tagName === 'TABLE' || parent.classList.toString().includes('table')) {
                              return true;
                            }
                            parent = parent.parentElement;
                          }
                          return false;
                        });
                        
                        if (isInTable) {
                          currentLink = link;
                          found = true;
                          log(`  ✅ 企業名リンクを発見（パターン2: aタグ）`);
                          break;
                        }
                      } catch (e) {
                        // 次のリンクを試す
                        continue;
                      }
                    }
                  }
                }
              } catch (e) {
                log(`  ⚠️  aタグ検索エラー: ${e}`);
              }
              }
              
              // パターン4: より広範囲で企業名テキストを含む要素を探す
              if (!found) {
              try {
                // テーブル内のすべての要素から企業名を探す
                const tableElements = page.locator('table *');
                const tableElementCount = await tableElements.count();
                log(`  🔍 テーブル内の要素数: ${tableElementCount}`);
                
                for (let j = 0; j < Math.min(tableElementCount, 1000); j++) {
                  const elem = tableElements.nth(j);
                  const elemText = (await elem.textContent())?.trim() || "";
                  
                  // 企業名が完全一致する場合
                  if (elemText === companyName || elemText.trim() === companyName.trim()) {
                    const tagName = await elem.evaluate((el: any) => el.tagName.toLowerCase());
                    
                    if (tagName === 'a') {
                      currentLink = elem;
                      found = true;
                      log(`  ✅ 企業名リンクを発見（パターン3: aタグ）`);
                      break;
                    } else {
                      // aタグでない場合は、親要素のaタグを探す
                      try {
                        const parentLink = elem.locator('xpath=ancestor::a[1]').first();
                        const parentLinkCount = await parentLink.count();
                        
                        if (parentLinkCount > 0) {
                          currentLink = parentLink;
                          found = true;
                          log(`  ✅ 企業名リンクを発見（パターン3: 親要素のaタグ）`);
                          break;
                        } else {
                          // aタグがない場合は、要素自体をクリック
                          currentLink = elem;
                          found = true;
                          log(`  ✅ 企業名要素を発見（パターン3: 要素自体、${tagName}）`);
                          break;
                        }
                      } catch (e) {
                        // 要素自体をクリック
                        currentLink = elem;
                        found = true;
                        log(`  ✅ 企業名要素を発見（パターン3: 要素自体、${tagName}）`);
                        break;
                      }
                    }
                  }
                }
              } catch (e) {
                log(`  ⚠️  広範囲検索エラー: ${e}`);
              }
              }
              
              if (!found) {
                log(`  ⚠️  企業 ${i + 1} (${companyName}) の要素が見つかりません。スキップします。`, true);
                // エラーレコードを出力
                const errorRecord = createEmptyRecord();
                errorRecord.companyId = generateCompanyId(null, companyName, "");
                errorRecord.name = `${companyName} (要素が見つかりません)`;
                await csvWriter.writeRecords([errorRecord]);
                continue;
              }
            }
          } else {
            // 通常のセレクタで再取得
            const excludeTexts = ["こちらから", "サイトを見る", "地図で見る", "View", "見る", "詳細", "次", "前", ">>", "<<", "次へ", "前へ", "与信調査", "新聞調査", "コンプラチェック"];
            
            // 企業名列のインデックスを再取得
            let companyColumnIndex = -1;
            try {
              const headers = page.locator('table thead th, table thead tr th');
              const headerCount = await headers.count();
              for (let j = 0; j < headerCount; j++) {
                const headerText = (await headers.nth(j).textContent())?.trim() || "";
                if (headerText.includes("企業名") || headerText.includes("Company Name")) {
                  companyColumnIndex = j;
                  break;
                }
              }
            } catch (e) {
              // 無視
            }
            
            let links;
            let currentLinkCount = 0;
            
            // 企業名列のインデックスが見つかった場合
            if (companyColumnIndex >= 0) {
              links = page.locator(`table tbody tr td:nth-child(${companyColumnIndex + 1}) a, table tr:not(thead tr) td:nth-child(${companyColumnIndex + 1}) a`).filter({ 
                hasText: /.+/,
                hasNotText: new RegExp(excludeTexts.join("|"), "i")
              });
              currentLinkCount = await links.count();
            }
            
            // 最初の列を試す
            if (currentLinkCount === 0) {
              links = page.locator('table tbody tr td:first-child a, table tr:not(thead tr) td:first-child a').filter({ 
                hasText: /.+/,
                hasNotText: new RegExp(excludeTexts.join("|"), "i")
              });
              currentLinkCount = await links.count();
            }
            
            // その他のパターン
            if (currentLinkCount === 0) {
              links = page.locator('table [class*="company"] a, table [class*="name"] a, [class*="company-name"] a').filter({ 
                hasText: /.+/,
                hasNotText: new RegExp(excludeTexts.join("|"), "i")
              });
              currentLinkCount = await links.count();
            }
            
            if (currentLinkCount === 0) {
              links = page.locator('a[href*="/company/"], a[href*="/detail/"], a[href*="/companies/"]').filter({ 
                hasText: /.+/,
                hasNotText: new RegExp(excludeTexts.join("|"), "i")
              });
              currentLinkCount = await links.count();
            }
            
            if (i >= currentLinkCount || currentLinkCount === 0) {
              log(`  ⚠️  企業 ${i + 1} のリンクが見つかりません（リンク数: ${currentLinkCount}）。スキップします。`, true);
              // エラーレコードを出力
              const errorRecord = createEmptyRecord();
              errorRecord.companyId = generateCompanyId(null, `企業${i + 1}`, "");
              errorRecord.name = `企業${i + 1} (リンクが見つかりません)`;
              await csvWriter.writeRecords([errorRecord]);
              continue;
            }
            
            currentLink = links.nth(i);

            if ((await currentLink.count()) === 0) {
              log(`  ⚠️  企業 ${i + 1} のリンクが見つかりません。スキップします。`, true);
              // エラーレコードを出力
              const errorRecord = createEmptyRecord();
              errorRecord.companyId = generateCompanyId(null, `企業${i + 1}`, "");
              errorRecord.name = `企業${i + 1} (リンクが見つかりません)`;
              await csvWriter.writeRecords([errorRecord]);
              continue;
            }

            companyName = (await currentLink.textContent())?.trim() || `企業${i + 1}`;
          }

          log(`  [${i + 1}/${linkCount}] ${companyName} を処理中...`);

          // 企業詳細ページに遷移
          // hrefがない要素でもクリックできるようにする
          try {
            await currentLink.click({ timeout: 10000 });
          } catch (e) {
            // クリックに失敗した場合、JavaScriptでクリックイベントを発火
            log(`  ⚠️  通常のクリックに失敗。JavaScriptでクリックを試行...`);
            await currentLink.evaluate((el: any) => {
              if (el.click) {
                el.click();
              } else if (el.dispatchEvent) {
                const event = new MouseEvent('click', { bubbles: true, cancelable: true });
                el.dispatchEvent(event);
              }
            });
          }
          
          await page.waitForLoadState("networkidle");
          await page.waitForTimeout(2000); // 詳細ページが完全に読み込まれるまで待機

          // セッション維持のためのアクティビティ（詳細ページ読み込み中）
          await maintainSession(page);

          // 詳細情報を取得（タイムアウトが発生した場合の再試行処理を含む）
          let record;
          try {
            record = await scrapeCompanyDetail(page, companyName);
          } catch (error: any) {
            // タイムアウトエラーの場合、セッションをチェックして再試行
            if (error.message && error.message.includes("Timeout")) {
              log(`  ⚠️  タイムアウトが発生しました。セッションをチェックします...`);
              const sessionValid = await checkSessionValid(page);
              if (!sessionValid) {
                log(`  ⚠️  ログアウトされています。再ログインを試みます...`);
                try {
                  await performLogin(page);
                  log(`  ⚠️  再ログイン完了。企業リストページに戻ります...`);
                  // リストページに戻る
                  await page.goBack({ waitUntil: "networkidle" });
                  await page.waitForTimeout(2000);
                  // エラーレコードを返す
                  record = createEmptyRecord();
                  record.companyId = generateCompanyId(null, companyName, "");
                  record.name = `${companyName} (タイムアウト: ログアウト)`;
                } catch (loginError) {
                  log(`  ❌ 再ログインに失敗しました: ${loginError}`, true);
                  record = createEmptyRecord();
                  record.companyId = generateCompanyId(null, companyName, "");
                  record.name = `${companyName} (タイムアウト: 再ログイン失敗)`;
                }
              } else {
                // セッションは有効だがタイムアウトが発生した場合、空のレコードを返す
                record = createEmptyRecord();
                record.companyId = generateCompanyId(null, companyName, "");
                record.name = `${companyName} (タイムアウト)`;
              }
            } else {
              throw error; // タイムアウト以外のエラーは再スロー
            }
          }

          // CSVに書き込み
          await csvWriter.writeRecords([record]);
          totalCompanies++;

          log(`  ✅ 完了: ${record.name || companyName} (companyId: ${record.companyId})`);
        } catch (error) {
          errorCount++;
          log(`  ❌ エラー (企業 ${i + 1}): ${error}`, true);
          // エラー時も最低限のレコードを出力
          const errorRecord = createEmptyRecord();
          errorRecord.companyId = generateCompanyId(null, `企業${i + 1}`, "");
          errorRecord.name = `企業${i + 1} (エラー)`;
          await csvWriter.writeRecords([errorRecord]);
        }
      }

      // 次ページへ遷移
      log(`\n  🔍 次ページを探しています...`);
      try {
        const nextButton = page.locator('a:has-text("次"), button:has-text("次"), [class*="next"], [aria-label*="次"]').first();
        const nextButtonText = await nextButton.textContent();

        if (nextButtonText && (nextButtonText.includes("次") || nextButtonText.includes(">") || nextButtonText.includes("»"))) {
          const isDisabled = await nextButton.getAttribute("disabled");
          const isVisible = await nextButton.isVisible();

          if (!isDisabled && isVisible) {
            await nextButton.click();
            await page.waitForLoadState("networkidle");
            await page.waitForTimeout(2000);
            currentPage++;
          } else {
            log("  ✅ 次ページボタンが無効または非表示です。終了します。");
            break;
          }
        } else {
          // ページ番号リンクを試す
          const pageLink = page.locator(`a:has-text("${currentPage + 1}"), button:has-text("${currentPage + 1}")`).first();
          if (await pageLink.count() > 0) {
            await pageLink.click();
            await page.waitForLoadState("networkidle");
            await page.waitForTimeout(2000);
            currentPage++;
          } else {
            log("  ✅ 次ページが見つかりません。終了します。");
            break;
          }
        }
      } catch (error) {
        log(`  ⚠️  次ページへの遷移に失敗: ${error}`, true);
        log("  30秒待機して終了します...");
        await page.waitForTimeout(30000);
        break;
      }
    }

    log(`\n✅ スクレイピング完了`);
    log(`  総企業数: ${totalCompanies}`);
    log(`  エラー数: ${errorCount}`);
    log(`  出力ファイル: ${OUTPUT_CSV}`);
    log(`  ログファイル: ${OUTPUT_LOG}`);
  } catch (error) {
    log(`\n❌ エラーが発生しました: ${error}`, true);
    throw error;
  } finally {
    log("\n🔒 ブラウザを閉じます...");
    await browser.close();
    closeLogFile();
  }
}

// 実行
main().catch((error) => {
  log("致命的なエラー: " + error, true);
  closeLogFile();
  process.exit(1);
});

