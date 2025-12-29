/* eslint-disable no-console */

/**
 * scripts/scrape_edinet_reports.ts
 *
 * ✅ 目的
 * - EDINETから有価証券報告書のCSVをダウンロード
 * - CSVから財務情報（複数期）と子会社・関連会社情報を抽出
 * - Firestoreのcompanies_newコレクションに保存
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 *
 * ✅ 処理フロー
 * 1. EDINETのURLにアクセス
 * 2. 有価証券報告書のCSVを全ページダウンロード
 * 3. CSVから財務情報と子会社情報を抽出
 * 4. 企業名でcompanies_newコレクションから会社を特定
 * 5. 本体企業と子会社の関連会社情報をFirestoreに保存
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";
import { chromium, Browser, Page } from "playwright";
import { parse } from "csv-parse/sync";
import * as yauzl from "yauzl";

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
// 設定
// ------------------------------
const EDINET_SEARCH_URL = "https://disclosure2.edinet-fsa.go.jp/WEEE0030.aspx";
const DOWNLOAD_DIR = path.join(process.cwd(), "edinet_downloads");
const SLEEP_MS = 2000; // リクエスト間隔（ミリ秒）

// ログファイルの設定
const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const logDir = path.join(process.cwd(), "logs");
const logFilePath = path.join(logDir, `scrape_edinet_reports_${timestamp}.log`);
const csvFilePath = path.join(logDir, `scrape_edinet_reports_${timestamp}.csv`);

// ログディレクトリが存在しない場合は作成
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

// ダウンロードディレクトリが存在しない場合は作成
if (!fs.existsSync(DOWNLOAD_DIR)) {
  fs.mkdirSync(DOWNLOAD_DIR, { recursive: true });
}

// CSVファイルのヘッダーを書き込み
const csvHeaders = [
  "companyId",
  "companyName",
  "edinetCode",
  "documentId",
  "fiscalYear",
  "status",
  "errorMessage",
  "timestamp"
];

// 既存のログファイルがある場合は、処理済みのdocumentIdとedinetCodeを読み込む
const processedDocumentIds = new Set<string>();
const processedEdinetCodes = new Set<string>();
const existingLogFiles = fs.readdirSync(logDir).filter(f => 
  f.startsWith("scrape_edinet_reports_") && f.endsWith(".csv")
);

// ログファイルから処理済みのedinetCodeを抽出
const existingLogFiles_log = fs.readdirSync(logDir).filter(f => 
  f.startsWith("scrape_edinet_reports_") && f.endsWith(".log")
);

for (const logFile of existingLogFiles_log) {
  try {
    const logPath = path.join(logDir, logFile);
    const logContent = fs.readFileSync(logPath, "utf8");
    
    // ログから「処理中 [X/Y]: EXXXXX」のパターンを抽出
    const edinetCodeMatches = logContent.matchAll(/処理中 \[\d+\/\d+\]:\s*(E\d+)/g);
    for (const match of edinetCodeMatches) {
      if (match[1]) {
        processedEdinetCodes.add(match[1]);
      }
    }
    
    // 「✅ EXXXXX: X件のCSVファイルをダウンロードしました」のパターンも抽出
    const downloadMatches = logContent.matchAll(/✅\s*(E\d+):\s*\d+件のCSVファイルをダウンロードしました/g);
    for (const match of downloadMatches) {
      if (match[1]) {
        processedEdinetCodes.add(match[1]);
      }
    }
  } catch (error) {
    // ログファイル読み込みエラーは無視
  }
}

for (const logFile of existingLogFiles) {
  try {
    const logPath = path.join(logDir, logFile);
    const logContent = fs.readFileSync(logPath, "utf8");
    const lines = logContent.split("\n").slice(1); // ヘッダーをスキップ
    
    for (const line of lines) {
      if (!line.trim()) continue;
      const columns = line.split(",");
      if (columns.length >= 4 && columns[3]) {
        const docId = columns[3].replace(/^"|"$/g, ""); // クォートを除去
        if (docId) {
          processedDocumentIds.add(docId);
        }
      }
      // edinetCodeも抽出（2列目）
      if (columns.length >= 2 && columns[1]) {
        const edinetCode = columns[1].replace(/^"|"$/g, "");
        if (edinetCode && edinetCode.startsWith("E")) {
          processedEdinetCodes.add(edinetCode);
        }
      }
    }
  } catch (error) {
    writeLog(`⚠️ ログファイル読み込みエラー (${logFile}): ${error}`);
  }
}

if (processedDocumentIds.size > 0) {
  writeLog(`📋 処理済みドキュメントID: ${processedDocumentIds.size}件`);
}

if (processedEdinetCodes.size > 0) {
  writeLog(`📋 処理済みedinetCode: ${processedEdinetCodes.size}件`);
}

// 新しいログファイルを作成（既存のものに追記しない）
fs.writeFileSync(csvFilePath, csvHeaders.join(",") + "\n", { encoding: "utf8" });

/**
 * ログファイルに書き込み
 */
function writeLog(message: string) {
  const timestamp = new Date().toISOString();
  const logMessage = `[${timestamp}] ${message}`;
  fs.appendFileSync(logFilePath, logMessage + "\n", { encoding: "utf8" });
  console.log(message);
}

/**
 * CSVファイルに処理結果を書き込み
 */
function writeCsvRow(data: {
  companyId: string;
  companyName: string;
  edinetCode: string;
  documentId: string;
  fiscalYear: string;
  status: "success" | "failed" | "no_data";
  errorMessage?: string;
}) {
  const row = [
    data.companyId,
    `"${data.companyName.replace(/"/g, '""')}"`,
    data.edinetCode,
    data.documentId,
    data.fiscalYear,
    data.status,
    data.errorMessage ? `"${data.errorMessage.replace(/"/g, '""')}"` : "",
    new Date().toISOString()
  ];
  fs.appendFileSync(csvFilePath, row.join(",") + "\n", { encoding: "utf8" });
}

// ------------------------------
// 型定義
// ------------------------------
interface FinancialData {
  fiscalYear: string;
  revenue?: number; // 売上高（千円）
  operatingIncome?: number; // 営業利益（千円）
  ordinaryIncome?: number; // 経常利益（千円）
  netIncome?: number; // 当期純利益（千円）
  totalAssets?: number; // 総資産（千円）
  totalEquity?: number; // 純資産（千円）
  totalLiabilities?: number; // 総負債（千円）
  capital?: number; // 資本金（千円）
}

interface RelatedCompany {
  name: string;
  relationship: "子会社" | "関連会社" | "その他" | "親会社";
  capital?: number;
  equityRatio?: number; // 持株比率（%）
  address?: string;
}

interface EdinetReport {
  companyName: string;
  edinetCode: string;
  documentId: string;
  fiscalYear: string;
  financialData: FinancialData[];
  relatedCompanies: RelatedCompany[];
}

// ------------------------------
// ユーティリティ関数
// ------------------------------
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * 企業名を正規化（（株）を株式会社に変換など）
 */
function normalizeCompanyName(name: string): string {
  return name
    .replace(/（株）/g, "株式会社")
    .replace(/\(株\)/g, "株式会社")
    .replace(/㈱/g, "株式会社")
    .replace(/（有）/g, "有限会社")
    .replace(/\(有\)/g, "有限会社")
    .replace(/（合）/g, "合資会社")
    .replace(/\(合\)/g, "合資会社")
    .replace(/（名）/g, "合名会社")
    .replace(/\(名\)/g, "合名会社")
    .trim();
}

/**
 * 企業名でcompanies_newコレクションから会社を検索
 * （株）→株式会社の正規化を確実に実行
 */
async function findCompanyByName(companyName: string): Promise<string | null> {
  try {
    const normalizedName = normalizeCompanyName(companyName);
    
    if (!normalizedName) {
      return null;
    }
    
    // 完全一致で検索（正規化された名前で）
    const exactMatch = await db
      .collection("companies_new")
      .where("name", "==", normalizedName)
      .limit(1)
      .get();
    
    if (!exactMatch.empty) {
      return exactMatch.docs[0].id;
    }
    
    // 部分一致で検索（株式会社の有無を無視）
    const nameWithoutKabushiki = normalizedName.replace(/^株式会社/, "").replace(/株式会社$/, "");
    if (nameWithoutKabushiki) {
      const partialMatch = await db
        .collection("companies_new")
        .where("name", ">=", nameWithoutKabushiki)
        .where("name", "<=", nameWithoutKabushiki + "\uf8ff")
        .limit(50)
        .get();
      
      for (const doc of partialMatch.docs) {
        const docName = normalizeCompanyName(doc.data().name || "");
        const docNameWithoutKabushiki = docName.replace(/^株式会社/, "").replace(/株式会社$/, "");
        
        // 正規化後の名前で比較
        if (docName === normalizedName || docNameWithoutKabushiki === nameWithoutKabushiki) {
          return doc.id;
        }
      }
    }
    
    // さらに広範囲に検索（全件取得して正規化して比較）
    const allCompanies = await db
      .collection("companies_new")
      .limit(1000)
      .get();
    
    for (const doc of allCompanies.docs) {
      const docName = normalizeCompanyName(doc.data().name || "");
      if (docName === normalizedName) {
        return doc.id;
      }
    }
    
    return null;
  } catch (error) {
    writeLog(`❌ 企業検索エラー (${companyName}): ${error}`);
    return null;
  }
}

/**
 * listed_parentsコレクションからedinetCodeを取得
 */
async function getEdinetCodesFromListedParents(): Promise<Array<{ id: string; edinetCode: string; name?: string }>> {
  const companies: Array<{ id: string; edinetCode: string; name?: string }> = [];
  
  try {
    writeLog("📋 listed_parentsコレクションからedinetCodeを取得中...");
    const snapshot = await db.collection("listed_parents").get();
    
    writeLog(`📊 listed_parentsコレクション: ${snapshot.size}件のドキュメントを発見`);
    
    for (const doc of snapshot.docs) {
      const data = doc.data();
      const edinetCode = data.edinetCode;
      
      if (edinetCode && typeof edinetCode === "string" && edinetCode.trim()) {
        companies.push({
          id: doc.id,
          edinetCode: edinetCode.trim(),
          name: data.name || undefined
        });
      } else {
        writeLog(`⚠️ edinetCodeが存在しません: ${doc.id} (${data.name || "名前なし"})`);
      }
    }
    
    writeLog(`✅ ${companies.length}件のedinetCodeを取得しました`);
    return companies;
  } catch (error) {
    writeLog(`❌ listed_parentsコレクション取得エラー: ${error}`);
    return [];
  }
}

/**
 * EDINETでedinetCodeを検索してCSVをダウンロード
 */
async function searchAndDownloadByEdinetCode(
  page: Page,
  edinetCode: string,
  companyName?: string
): Promise<string[]> {
  const downloadedFiles: string[] = [];
  
  try {
    writeLog(`🔍 EDINET検索開始: ${edinetCode}${companyName ? ` (${companyName})` : ""}`);
    
    // EDINET検索ページにアクセス
    await page.goto(EDINET_SEARCH_URL, { waitUntil: "networkidle", timeout: 60000 });
    await sleep(SLEEP_MS);
    
    // ページが完全に読み込まれるまで待機
    await page.waitForLoadState("networkidle");
    await sleep(SLEEP_MS);
    
    // 検索フォームを探す（より具体的なセレクタを優先）
    const searchInputSelectors = [
      'input[name*="code"]',
      'input[name*="Code"]',
      'input[name*="提出者"]',
      'input[name*="発行者"]',
      'input[name*="ファンド"]',
      'input[name*="証券コード"]',
      'input[placeholder*="提出者"]',
      'input[placeholder*="発行者"]',
      'input[placeholder*="コード"]',
      'input[type="text"]',
      '#searchCode',
      '.search-code',
      'input.textbox'
    ];
    
    let searchInput = null;
    for (const selector of searchInputSelectors) {
      try {
        await page.waitForSelector(selector, { timeout: 5000, state: "visible" });
        searchInput = await page.$(selector);
        if (searchInput) {
          const isVisible = await searchInput.isVisible();
          if (isVisible) {
            writeLog(`✅ 検索入力欄を発見: ${selector}`);
            break;
          }
        }
      } catch (e) {
        continue;
      }
    }
    
    if (!searchInput) {
      writeLog(`⚠️ 検索入力欄が見つかりません: ${edinetCode}`);
      // ページのスクリーンショットを保存（デバッグ用）
      try {
        await page.screenshot({ path: `logs/edinet_search_error_${edinetCode}_${Date.now()}.png` });
      } catch (e) {
        // スクリーンショットエラーは無視
      }
      return downloadedFiles;
    }
    
    // 検索入力欄をクリアしてからedinetCodeを入力
    await searchInput.click();
    await searchInput.fill("");
    await sleep(200);
    await searchInput.fill(edinetCode);
    await sleep(500);
    
    // 検索ボタンを探す（より具体的なセレクタを優先）
    const searchButtonSelectors = [
      'input[type="submit"][value*="検索"]',
      'input[type="button"][value*="検索"]',
      'button:has-text("検索")',
      'input[value*="検索"]',
      'input[value="検索"]',
      'button[type="submit"]',
      '#searchButton',
      '#btnSearch',
      '.search-button',
      '.btn-search',
      'input.btn',
      'button.btn'
    ];
    
    let searchButton = null;
    for (const selector of searchButtonSelectors) {
      try {
        await page.waitForSelector(selector, { timeout: 5000, state: "visible" });
        searchButton = await page.$(selector);
        if (searchButton) {
          const isVisible = await searchButton.isVisible();
          if (isVisible) {
            writeLog(`✅ 検索ボタンを発見: ${selector}`);
            break;
          }
        }
      } catch (e) {
        continue;
      }
    }
    
    if (!searchButton) {
      writeLog(`⚠️ 検索ボタンが見つかりません: ${edinetCode}`);
      // ページのスクリーンショットを保存（デバッグ用）
      try {
        await page.screenshot({ path: `logs/edinet_search_button_error_${edinetCode}_${Date.now()}.png` });
      } catch (e) {
        // スクリーンショットエラーは無視
      }
      return downloadedFiles;
    }
    
    // 検索ボタンをスクロールして表示
    await searchButton.scrollIntoViewIfNeeded();
    await sleep(500);
    
    // 検索実行（より確実な方法）
    try {
      // まず通常のクリックを試行
      await searchButton.click({ timeout: 10000 });
      await page.waitForLoadState("networkidle", { timeout: 30000 });
    } catch (clickError) {
      // クリックエラーの場合、forceオプションでクリックを試行
      try {
        writeLog(`⚠️ 通常のクリックに失敗、forceオプションでクリックを試行: ${edinetCode}`);
        await searchButton.click({ force: true, timeout: 10000 });
        await page.waitForLoadState("networkidle", { timeout: 30000 });
      } catch (forceError) {
        // forceでも失敗した場合、JavaScriptでクリックを試行
        try {
          writeLog(`⚠️ forceクリックに失敗、JavaScriptでクリックを試行: ${edinetCode}`);
          
          // 見つかった検索ボタンのセレクタを使用
          let buttonSelector = null;
          for (const selector of searchButtonSelectors) {
            try {
              const el = await page.$(selector);
              if (el) {
                const isVisible = await el.isVisible();
                if (isVisible) {
                  buttonSelector = selector;
                  break;
                }
              }
            } catch {
              continue;
            }
          }
          
          if (buttonSelector) {
            await page.evaluate((selector: string) => {
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              const doc = (globalThis as any).document;
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              const button = doc?.querySelector(selector) as any;
              if (button) {
                button.click();
              }
            }, buttonSelector);
          } else {
            // フォーム送信を試行
            await page.evaluate(() => {
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              const doc = (globalThis as any).document;
              // eslint-disable-next-line @typescript-eslint/no-explicit-any
              const form = doc?.querySelector('form') as any;
              if (form) {
                form.submit();
              }
            });
          }
          await page.waitForLoadState("networkidle", { timeout: 30000 });
        } catch (jsError) {
          writeLog(`❌ 検索ボタンクリックエラー (${edinetCode}): ${jsError}`);
          return downloadedFiles;
        }
      }
    }
    
    await sleep(SLEEP_MS);
    
    // 検索結果が3000件を超えている場合のエラーメッセージをチェック
    const pageContent = await page.content();
    if (pageContent.includes("検索結果が3000件を超えています")) {
      writeLog(`⚠️ 検索結果が3000件を超えています: ${edinetCode}`);
      return downloadedFiles;
    }
    
    // テーブル行を取得するセレクタ
    const tableSelectors = [
      "table tbody tr",
      "table tr",
      "tbody tr",
      ".table tr",
      "#MainContent table tr",
      "[id*='Main'] table tr"
    ];
    
    // 複数ページを処理（全ページをダウンロード）
    let pageNum = 1;
    let hasNextPage = true;
    let totalCsvLinkCount = 0;
    
    while (hasNextPage) {
      writeLog(`📄 ${edinetCode}: ページ ${pageNum} を処理中...`);
      
      // 現在のページのテーブル行を取得
      let tableRows: any[] = [];
      for (const selector of tableSelectors) {
        tableRows = await page.$$(selector);
        if (tableRows.length > 0) {
          writeLog(`📋 テーブル行を発見: ${selector} (${tableRows.length}行)`);
          break;
        }
      }
      
      if (tableRows.length === 0) {
        writeLog(`⚠️ ページ ${pageNum} にテーブル行が見つかりません: ${edinetCode}`);
        hasNextPage = false;
        break;
      }
      
      // 各行をチェックして、有価証券報告書の行のCSV列のリンクをクリック
      let csvLinkCount = 0;
      
      for (let i = 0; i < tableRows.length; i++) {
        try {
          const row = tableRows[i];
          const rowText = await row.textContent();
          
          // 有価証券報告書かどうかを確認（提出書類列に「有価証券報告書」が含まれているか）
          if (!rowText || !rowText.includes("有価証券報告書")) {
            continue;
          }
          
          writeLog(`📋 有価証券報告書の行を発見: ${rowText.substring(0, 80)}...`);
          
          // CSV列のリンクを探す
          const cells = await row.$$("td");
          
          if (cells.length === 0) {
            continue;
          }
          
          // CSV列のリンクを探す（CSV列は通常右側にある）
          let csvLink = null;
          for (let j = cells.length - 1; j >= 0; j--) {
            const cell = cells[j];
            const links = await cell.$$("a");
            
            for (const link of links) {
              const linkText = await link.textContent();
              const linkHref = await link.getAttribute("href");
              
              // CSVリンクかどうかを確認
              if (
                (linkText && (linkText.trim() === "CSV" || linkText.includes("CSV"))) ||
                (linkHref && (linkHref.includes("csv") || linkHref.includes("CSV")))
              ) {
                csvLink = link;
                writeLog(`📥 CSVリンクを発見: ${linkText || linkHref}`);
                break;
              }
            }
            
            if (csvLink) break;
          }
          
          if (csvLink) {
            try {
              // CSVリンクをクリックしてダウンロード（実際にはZIPファイルがダウンロードされる）
              const [download] = await Promise.all([
                page.waitForEvent("download", { timeout: 120000 }),
                csvLink.click()
              ]);
              
              // ZIPファイルとして保存（EDINETのCSVリンクはZIPファイルをダウンロードする）
              const fileName = `edinet_${edinetCode}_${Date.now()}_p${pageNum}_r${i + 1}.zip`;
              const filePath = path.join(DOWNLOAD_DIR, fileName);
              await download.saveAs(filePath);
              downloadedFiles.push(filePath);
              csvLinkCount++;
              totalCsvLinkCount++;
              writeLog(`✅ ダウンロード完了（ZIPファイル）: ${fileName}`);
              await sleep(SLEEP_MS);
            } catch (downloadError) {
              writeLog(`⚠️ ダウンロードエラー (${edinetCode}, ページ${pageNum}, 行${i + 1}): ${downloadError}`);
            }
          } else {
            writeLog(`⚠️ CSVリンクが見つかりません（${edinetCode}, ページ${pageNum}, 行${i + 1}）`);
          }
        } catch (rowError) {
          writeLog(`⚠️ 行処理エラー (${edinetCode}, ページ${pageNum}, 行${i + 1}): ${rowError}`);
          continue;
        }
      }
      
      writeLog(`✅ ${edinetCode}: ページ ${pageNum} で ${csvLinkCount}件のCSVファイルをダウンロードしました`);
      
      // 次へボタンを探す
      const nextSelectors = [
        'a:has-text("次へ")',
        'input[value*="次へ"]',
        'button:has-text("次へ")',
        'a:has-text(">")',
        'a:has-text("次ページ")',
        '.pager a:last-child',
        '#nextPage',
        '.next',
        'a[title*="次"]',
        'a[aria-label*="次"]'
      ];
      
      let nextButton = null;
      for (const selector of nextSelectors) {
        try {
          nextButton = await page.$(selector);
          if (nextButton) {
            const isDisabled = await nextButton.evaluate((btn) => {
              const element = btn as any;
              return element.hasAttribute("disabled") || 
                     element.classList.contains("disabled") ||
                     element.getAttribute("aria-disabled") === "true" ||
                     element.style.display === "none";
            });
            
            if (!isDisabled) {
              writeLog(`✅ 次へボタンを発見: ${selector}`);
              break;
            } else {
              nextButton = null;
            }
          }
        } catch (e) {
          continue;
        }
      }
      
      if (nextButton) {
        try {
          writeLog(`➡️ 次のページに遷移... (${edinetCode})`);
          await nextButton.scrollIntoViewIfNeeded();
          await nextButton.click();
          await page.waitForLoadState("networkidle", { timeout: 30000 });
          await sleep(SLEEP_MS * 2);
          pageNum++;
        } catch (navError) {
          writeLog(`⚠️ ページ遷移エラー (${edinetCode}): ${navError}`);
          hasNextPage = false;
        }
      } else {
        hasNextPage = false;
        writeLog(`✅ 次へボタンが見つかりません（最後のページ）: ${edinetCode}`);
      }
    }
    
    writeLog(`✅ ${edinetCode}: 全${totalCsvLinkCount}件のCSVファイルをダウンロードしました（${pageNum}ページ）`);
    return downloadedFiles;
  } catch (error) {
    writeLog(`❌ 検索・ダウンロードエラー (${edinetCode}): ${error}`);
    return downloadedFiles;
  }
}

/**
 * EDINETのページからCSVをダウンロード（旧実装 - 使用しない）
 */
async function downloadEdinetCsvsOld(page: Page): Promise<string[]> {
  const downloadedFiles: string[] = [];
  
  try {
    writeLog("📥 EDINETページにアクセス中...");
    await page.goto(EDINET_SEARCH_URL, { waitUntil: "networkidle", timeout: 60000 });
    await sleep(SLEEP_MS);
    
    // ページが読み込まれるまで待機
    await page.waitForSelector("table, .table, #MainContent, body", { timeout: 30000 });
    
    let pageNum = 1;
    let hasNextPage = true;
    let consecutiveEmptyPages = 0;
    const maxEmptyPages = 3; // 連続で空ページが3回続いたら終了
    
    while (hasNextPage && consecutiveEmptyPages < maxEmptyPages) {
      writeLog(`📄 ページ ${pageNum} を処理中...`);
      
      // ページの内容を確認
      const pageContent = await page.content();
      if (!pageContent.includes("有価証券報告書") && !pageContent.includes("提出書類")) {
        writeLog(`⚠️ ページ ${pageNum} に有価証券報告書が見つかりません`);
        consecutiveEmptyPages++;
        if (consecutiveEmptyPages >= maxEmptyPages) {
          writeLog("✅ 連続で空ページが続いたため処理を終了します");
          break;
        }
      } else {
        consecutiveEmptyPages = 0;
      }
      
      // テーブルの行を取得（複数のパターンを試行）
      let tableRows: any[] = [];
      const tableSelectors = [
        "table tbody tr",
        "table tr",
        "tbody tr",
        ".table tr",
        "#MainContent table tr",
        "[id*='Main'] table tr"
      ];
      
      for (const selector of tableSelectors) {
        tableRows = await page.$$(selector);
        if (tableRows.length > 0) {
          writeLog(`📋 テーブル行を発見: ${selector} (${tableRows.length}行)`);
          break;
        }
      }
      
      if (tableRows.length === 0) {
        writeLog(`⚠️ テーブル行が見つかりません（ページ ${pageNum}）`);
      }
      
      // まず、ページ全体のCSVダウンロードボタンを探す（全件一括ダウンロード）
      const bulkDownloadSelectors = [
        'a[href*="csv"]:has-text("CSV")',
        'a[href*="CSV"]:has-text("CSV")',
        'input[value*="CSV"]',
        'button:has-text("CSV")',
        'a:has-text("CSVダウンロード")',
        'a:has-text("一括ダウンロード")',
        '.csv-download',
        '#csvDownload'
      ];
      
      let bulkDownloadButton = null;
      for (const selector of bulkDownloadSelectors) {
        try {
          bulkDownloadButton = await page.$(selector);
          if (bulkDownloadButton) {
            writeLog(`✅ 一括CSVダウンロードボタンを発見: ${selector}`);
            break;
          }
        } catch (e) {
          continue;
        }
      }
      
      // 一括ダウンロードボタンがある場合は、それを使用
      if (bulkDownloadButton) {
        try {
          writeLog("📥 一括CSVダウンロードを開始...");
          const [download] = await Promise.all([
            page.waitForEvent("download", { timeout: 120000 }),
            bulkDownloadButton.click()
          ]);
          
          const fileName = `edinet_page${pageNum}_bulk_${Date.now()}.csv`;
          const filePath = path.join(DOWNLOAD_DIR, fileName);
          await download.saveAs(filePath);
          downloadedFiles.push(filePath);
          writeLog(`✅ 一括ダウンロード完了: ${fileName}`);
          await sleep(SLEEP_MS * 2);
        } catch (downloadError) {
          writeLog(`⚠️ 一括ダウンロードエラー: ${downloadError}`);
        }
      }
      
      let csvLinkCount = 0;
      
      // 各行をチェックして、有価証券報告書の行のCSV列のリンクをクリック
      for (let i = 0; i < tableRows.length; i++) {
        try {
          const row = tableRows[i];
          
          // 行の内容を確認
          const rowText = await row.textContent();
          
          // 有価証券報告書かどうかを確認
          if (!rowText || !rowText.includes("有価証券報告書")) {
            continue;
          }
          
          writeLog(`📋 有価証券報告書の行を発見: ${rowText.substring(0, 50)}...`);
          
          // CSV列のリンクを探す（右側の列、最後の列から探す）
          const cells = await row.$$("td");
          
          if (cells.length === 0) {
            writeLog(`⚠️ セルが見つかりません（行${i + 1}）`);
            continue;
          }
          
          // 最後の列から順にCSVリンクを探す
          let csvLink = null;
          for (let j = cells.length - 1; j >= 0; j--) {
            const cell = cells[j];
            const links = await cell.$$("a");
            
            for (const link of links) {
              const linkText = await link.textContent();
              const linkHref = await link.getAttribute("href");
              
              // CSVリンクかどうかを確認
              if (
                (linkText && (linkText.includes("CSV") || linkText.includes("csv"))) ||
                (linkHref && (linkHref.includes("csv") || linkHref.includes("CSV") || linkHref.includes("download")))
              ) {
                csvLink = link;
                writeLog(`📥 CSVリンクを発見: ${linkText || linkHref}`);
                break;
              }
            }
            
            if (csvLink) break;
          }
          
          // CSVリンクが見つからない場合は、最後のセルの最初のリンクを使用
          if (!csvLink && cells.length > 0) {
            const lastCell = cells[cells.length - 1];
            const links = await lastCell.$$("a");
            if (links.length > 0) {
              csvLink = links[0];
              const linkText = await csvLink.textContent();
              writeLog(`📥 最後の列のリンクを使用: ${linkText || "リンク"}`);
            }
          }
          
          if (csvLink) {
            try {
              // CSVリンクをクリックしてダウンロード
              const [download] = await Promise.all([
                page.waitForEvent("download", { timeout: 120000 }),
                csvLink.click()
              ]);
              
              const fileName = `edinet_page${pageNum}_row${i + 1}_${Date.now()}.csv`;
              const filePath = path.join(DOWNLOAD_DIR, fileName);
              await download.saveAs(filePath);
              downloadedFiles.push(filePath);
              csvLinkCount++;
              writeLog(`✅ ダウンロード完了: ${fileName}`);
              await sleep(SLEEP_MS); // ダウンロード間隔
            } catch (downloadError) {
              writeLog(`⚠️ ダウンロードエラー (行${i + 1}): ${downloadError}`);
              // 個別のダウンロードエラーは無視して続行
            }
          } else {
            writeLog(`⚠️ CSVリンクが見つかりません（行${i + 1}）`);
          }
        } catch (rowError) {
          writeLog(`⚠️ 行処理エラー (行${i + 1}): ${rowError}`);
          // 個別の行エラーは無視して続行
          continue;
        }
      }
      
      writeLog(`✅ ${csvLinkCount}件のCSVファイルをダウンロードしました（ページ ${pageNum}）`);
      
      // 次へボタンを探す（複数のパターンを試行）
      const nextSelectors = [
        'a:has-text("次へ")',
        'input[value*="次へ"]',
        'button:has-text("次へ")',
        'a:has-text(">")',
        'a:has-text("次ページ")',
        '.pager a:last-child',
        '#nextPage',
        '.next'
      ];
      
      let nextButton = null;
      for (const selector of nextSelectors) {
        try {
          nextButton = await page.$(selector);
          if (nextButton) {
            const isDisabled = await nextButton.evaluate((btn) => {
              const element = btn as any;
              return element.hasAttribute("disabled") || 
                     element.classList.contains("disabled") ||
                     element.getAttribute("aria-disabled") === "true";
            });
            
            if (!isDisabled) {
              writeLog(`✅ 次へボタンを発見: ${selector}`);
              break;
            } else {
              nextButton = null;
            }
          }
        } catch (e) {
          continue;
        }
      }
      
      if (nextButton) {
        writeLog("➡️ 次のページに遷移...");
        try {
          await nextButton.scrollIntoViewIfNeeded();
          await nextButton.click();
          await page.waitForLoadState("networkidle", { timeout: 30000 });
          await sleep(SLEEP_MS);
          pageNum++;
        } catch (navError) {
          writeLog(`⚠️ ページ遷移エラー: ${navError}`);
          hasNextPage = false;
        }
      } else {
        hasNextPage = false;
        writeLog("✅ 次へボタンが見つかりません（最後のページ）");
      }
    }
    
    writeLog(`✅ 全${downloadedFiles.length}件のCSVファイルをダウンロードしました`);
    return downloadedFiles;
  } catch (error) {
    writeLog(`❌ CSVダウンロードエラー: ${error}`);
    return downloadedFiles;
  }
}

/**
 * ZIPファイルを展開してCSVファイルを取得
 */
async function extractZipFile(zipPath: string): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const extractedFiles: string[] = [];
    
    yauzl.open(zipPath, { lazyEntries: true }, (err: Error | null, zipfile: yauzl.ZipFile | undefined) => {
      if (err) {
        reject(err);
        return;
      }
      
      if (!zipfile) {
        reject(new Error("ZIPファイルを開けませんでした"));
        return;
      }
      
      zipfile.readEntry();
      
      zipfile.on("entry", (entry: yauzl.Entry) => {
        // CSVファイルのみを抽出
        if (entry.fileName.endsWith(".csv") || entry.fileName.endsWith(".CSV")) {
          zipfile.openReadStream(entry, (err: Error | null, readStream: NodeJS.ReadableStream | undefined) => {
            if (err || !readStream) {
              if (zipfile) zipfile.readEntry();
              return;
            }
            
            const extractedPath = path.join(
              path.dirname(zipPath),
              `extracted_${Date.now()}_${path.basename(entry.fileName)}`
            );
            const writeStream = fs.createWriteStream(extractedPath);
            
            readStream.pipe(writeStream);
            
            writeStream.on("close", () => {
              extractedFiles.push(extractedPath);
              writeLog(`✅ ZIPから展開: ${entry.fileName} -> ${path.basename(extractedPath)}`);
              if (zipfile) zipfile.readEntry();
            });
            
            writeStream.on("error", (err) => {
              writeLog(`⚠️ ファイル展開エラー: ${err}`);
              if (zipfile) zipfile.readEntry();
            });
          });
        } else {
          zipfile.readEntry();
        }
      });
      
      zipfile.on("end", () => {
        resolve(extractedFiles);
      });
      
      zipfile.on("error", (err: Error) => {
        reject(err);
      });
    });
  });
}

/**
 * ファイルがZIPファイルかどうかをチェック
 */
function isZipFile(filePath: string): boolean {
  try {
    const fd = fs.openSync(filePath, "r");
    const buffer = Buffer.alloc(3);
    fs.readSync(fd, buffer, 0, 3, 0);
    fs.closeSync(fd);
    // ZIPファイルのマジックナンバー: PK (0x50 0x4B)
    return buffer[0] === 0x50 && buffer[1] === 0x4B && buffer[2] === 0x03;
  } catch {
    return false;
  }
}

/**
 * CSVファイルから財務情報と子会社情報を抽出
 * EDINETのXBRL_TO_CSVファイル形式に対応
 */
async function parseEdinetCsv(filePath: string): Promise<EdinetReport[]> {
  const reports: EdinetReport[] = [];
  
  try {
    // ZIPファイルの場合はスキップ（ユーザーが手動で展開する）
    if (isZipFile(filePath)) {
      writeLog(`📦 ZIPファイルはスキップします（手動展開が必要）: ${path.basename(filePath)}`);
      return [];
    }
    
    // CSVファイルのみ処理
    const csvFilePath = filePath;
    
    // ファイルのエンコーディングを検出
    let csvContent: string;
    const buffer = fs.readFileSync(csvFilePath);
    
    // UTF-16のBOMをチェック（FF FE = UTF-16 LE, FE FF = UTF-16 BE）
    if (buffer.length >= 2) {
      const bomLE = buffer[0] === 0xFF && buffer[1] === 0xFE; // UTF-16 LE BOM
      const bomBE = buffer[0] === 0xFE && buffer[1] === 0xFF; // UTF-16 BE BOM
      
      if (bomLE) {
        // UTF-16 LEとして読み込む
        csvContent = buffer.toString("utf16le");
        writeLog(`📝 UTF-16 LEエンコーディングを検出: ${path.basename(csvFilePath)}`);
      } else if (bomBE) {
        // UTF-16 BEの場合は、バイト順を入れ替えて読み込む
        const swappedBuffer = Buffer.alloc(buffer.length);
        for (let i = 0; i < buffer.length - 1; i += 2) {
          swappedBuffer[i] = buffer[i + 1];
          swappedBuffer[i + 1] = buffer[i];
        }
        csvContent = swappedBuffer.toString("utf16le");
        writeLog(`📝 UTF-16 BEエンコーディングを検出（変換）: ${path.basename(csvFilePath)}`);
      } else {
        // BOMがない場合はUTF-8として試行
        try {
          csvContent = buffer.toString("utf8");
        } catch (error) {
          // UTF-8で読み込めない場合は、UTF-16 LEを試行
          csvContent = buffer.toString("utf16le");
          writeLog(`📝 UTF-16 LEとして読み込み（BOMなし）: ${path.basename(csvFilePath)}`);
        }
      }
    } else {
      // ファイルが小さすぎる場合はUTF-8として読み込む
      csvContent = buffer.toString("utf8");
    }
    
    // 文字エンコーディングの検出と変換
    let content = csvContent;
    if (csvContent.includes("\ufeff")) {
      content = csvContent.replace("\ufeff", ""); // BOMを除去
    }
    
    // CSVをパース（EDINETのXBRL_TO_CSVファイルはタブ区切り）
    let records: any[] = [];
    try {
      records = parse(content, {
        delimiter: "\t",
        quote: '"',
        escape: '"',
        relax_quotes: true,
        skip_empty_lines: true,
        relax_column_count: true,
        columns: true
      });
      
      writeLog(`✅ CSVパース成功: ${path.basename(filePath)} (${records.length}行)`);
    } catch (parseError) {
      writeLog(`❌ CSVパースエラー (${filePath}): ${parseError}`);
      return [];
    }
    
    if (records.length === 0) {
      writeLog(`⚠️ CSVファイルにデータがありません: ${path.basename(filePath)}`);
      return [];
    }
    
    // ヘッダーを確認
    const firstRecord = records[0];
    const headers = Object.keys(firstRecord);
    writeLog(`📋 CSVヘッダー: ${headers.slice(0, 10).join(", ")}${headers.length > 10 ? "..." : ""}`);
    
    // ファイル名から企業情報を抽出（ファイル名パターン: jpaud-aai-cc-001_E02285-000_2020-03-31_03_2025-06-20.csv）
    const fileName = path.basename(csvFilePath, ".csv");
    const fileNameParts = fileName.split("_");
    let edinetCode = "";
    let documentId = "";
    let fiscalYear = "";
    
    // ファイル名からEDINETコードを抽出（E02285のような形式）
    const edinetCodeMatch = fileName.match(/[E]\d{5}/);
    if (edinetCodeMatch) {
      edinetCode = edinetCodeMatch[0];
    }
    
    // 企業名を取得（ファイル名やCSVのメタデータから）
    let companyName = "";
    
    // 企業名を探す（会社名、企業名、名称などの項目名から）
    for (const record of records) {
      const itemName = record["項目名"] || record["要素ID"] || "";
      if (itemName.includes("会社名") || itemName.includes("企業名") || itemName.includes("名称")) {
        const value = record["値"] || "";
        if (value && value.length > 0 && !value.includes("時点") && !value.includes("期間")) {
          companyName = value.trim();
          break;
        }
      }
    }
    
    // 企業名が見つからない場合は、ファイル名から推測
    if (!companyName) {
      // listed_parentsコレクションからedinetCodeで企業名を取得
      try {
        const companyDoc = await db.collection("listed_parents")
          .where("edinetCode", "==", edinetCode)
          .limit(1)
          .get();
        
        if (!companyDoc.empty) {
          companyName = companyDoc.docs[0].data().name || "";
        }
      } catch (error) {
        writeLog(`⚠️ 企業名の取得に失敗: ${error}`);
      }
    }
    
    if (!companyName) {
      writeLog(`⚠️ 企業名が取得できませんでした: ${path.basename(filePath)}`);
      return [];
    }
    
    // 決算期を取得（相対年度、期間・時点から）
    const fiscalYearMap = new Map<string, string>(); // 相対年度 -> 決算期
    const financialDataMap = new Map<string, FinancialData>(); // 決算期 -> 財務データ
    
    // 財務情報を抽出（項目名から財務項目を特定）
    for (const record of records) {
      const itemName = record["項目名"] || "";
      const value = record["値"] || "";
      const relativeYear = record["相対年度"] || "";
      const period = record["期間・時点"] || "";
      const unit = record["単位"] || "";
      
      // 決算期を特定
      let currentFiscalYear = fiscalYear;
      if (relativeYear && relativeYear !== "提出日時点" && relativeYear !== "その他") {
        // 相対年度から決算期を推測（例: "0" = 当期, "1" = 前期）
        if (!fiscalYearMap.has(relativeYear)) {
          // 期間・時点から年度を抽出
          const yearMatch = period.match(/(\d{4})/);
          if (yearMatch) {
            fiscalYearMap.set(relativeYear, yearMatch[1]);
            currentFiscalYear = yearMatch[1];
          }
        } else {
          currentFiscalYear = fiscalYearMap.get(relativeYear) || fiscalYear;
        }
      }
      
      if (!currentFiscalYear && period) {
        const yearMatch = period.match(/(\d{4})/);
        if (yearMatch) {
          currentFiscalYear = yearMatch[1];
        }
      }
      
      // 財務項目を特定
      let financialField: keyof FinancialData | null = null;
      if (itemName.includes("売上高") || itemName.includes("売上") || itemName.includes("Revenue")) {
        financialField = "revenue";
      } else if (itemName.includes("営業利益") || itemName.includes("OperatingIncome")) {
        financialField = "operatingIncome";
      } else if (itemName.includes("経常利益") || itemName.includes("OrdinaryIncome")) {
        financialField = "ordinaryIncome";
      } else if (itemName.includes("当期純利益") || itemName.includes("NetIncome") || itemName.includes("純利益")) {
        financialField = "netIncome";
      } else if (itemName.includes("総資産") || itemName.includes("TotalAssets")) {
        financialField = "totalAssets";
      } else if (itemName.includes("純資産") || itemName.includes("TotalEquity") || itemName.includes("NetAssets")) {
        financialField = "totalEquity";
      } else if (itemName.includes("総負債") || itemName.includes("TotalLiabilities")) {
        financialField = "totalLiabilities";
      } else if (itemName.includes("資本金") || itemName.includes("Capital")) {
        financialField = "capital";
      }
      
      if (financialField && value && currentFiscalYear) {
        const parsedValue = parseFinancialValue(value);
        if (parsedValue !== undefined) {
          if (!financialDataMap.has(currentFiscalYear)) {
            financialDataMap.set(currentFiscalYear, {
              fiscalYear: currentFiscalYear
            });
          }
          
          const financialData = financialDataMap.get(currentFiscalYear)!;
          financialData[financialField] = parsedValue;
        }
      }
    }
    
    // 子会社・関連会社情報を抽出
    const relatedCompaniesMap = new Map<string, RelatedCompany>();
    
    for (const record of records) {
      const itemName = record["項目名"] || "";
      const value = record["値"] || "";
      
      // 子会社・関連会社の項目を特定
      if (itemName.includes("子会社") || itemName.includes("関連会社") || itemName.includes("Subsidiary") || itemName.includes("RelatedCompany")) {
        // 会社名を抽出
        if (itemName.includes("名称") || itemName.includes("Name") || itemName.includes("会社名")) {
          const normalizedName = normalizeCompanyName(value.trim());
          if (normalizedName && !relatedCompaniesMap.has(normalizedName)) {
            const relationship = itemName.includes("子会社") || itemName.includes("Subsidiary") ? "子会社" : "関連会社";
            relatedCompaniesMap.set(normalizedName, {
              name: normalizedName,
              relationship: relationship as "子会社" | "関連会社"
            });
          }
        }
        
        // 持株比率を抽出
        if (itemName.includes("持株比率") || itemName.includes("EquityRatio") || itemName.includes("出資比率")) {
          const ratio = parseFinancialValue(value);
          if (ratio !== undefined) {
            // 直前の会社名を探す
            for (const [name, rc] of relatedCompaniesMap.entries()) {
              if (!rc.equityRatio) {
                rc.equityRatio = ratio;
                break;
              }
            }
          }
        }
        
        // 資本金を抽出
        if (itemName.includes("資本金") || itemName.includes("Capital")) {
          const capital = parseFinancialValue(value);
          if (capital !== undefined) {
            // 直前の会社名を探す
            for (const [name, rc] of relatedCompaniesMap.entries()) {
              if (!rc.capital) {
                rc.capital = capital;
                break;
              }
            }
          }
        }
        
        // 所在地を抽出
        if (itemName.includes("所在地") || itemName.includes("Address") || itemName.includes("本店")) {
          const address = value.trim();
          if (address) {
            // 直前の会社名を探す
            for (const [name, rc] of relatedCompaniesMap.entries()) {
              if (!rc.address) {
                rc.address = address;
                break;
              }
            }
          }
        }
      }
    }
    
    // レポートを作成
    const report: EdinetReport = {
      companyName: normalizeCompanyName(companyName),
      edinetCode,
      documentId,
      fiscalYear: fiscalYear || Array.from(financialDataMap.keys())[0] || "",
      financialData: Array.from(financialDataMap.values()),
      relatedCompanies: Array.from(relatedCompaniesMap.values())
    };
    
    reports.push(report);
    writeLog(`✅ レポートを抽出しました: ${report.companyName} (財務データ: ${report.financialData.length}期, 関連会社: ${report.relatedCompanies.length}社)`);
    
    return reports;
  } catch (error) {
    writeLog(`❌ CSV解析エラー (${filePath}): ${error}`);
    return [];
  }
}

/**
 * 財務数値をパース（カンマ区切り、単位変換に対応）
 */
function parseFinancialValue(value: string | number | undefined): number | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  
  if (typeof value === "number") {
    return value;
  }
  
  if (typeof value !== "string") {
    return undefined;
  }
  
  // カンマを除去
  let cleaned = value.replace(/,/g, "").trim();
  
  // 単位を変換（億円、万円など）
  let multiplier = 1;
  if (cleaned.includes("億")) {
    multiplier = 100000; // 億円を千円に変換
    cleaned = cleaned.replace(/億[円]*/g, "");
  } else if (cleaned.includes("万円")) {
    multiplier = 10; // 万円を千円に変換
    cleaned = cleaned.replace(/万円/g, "");
  } else if (cleaned.includes("千円")) {
    multiplier = 1;
    cleaned = cleaned.replace(/千円/g, "");
  } else if (cleaned.includes("円")) {
    multiplier = 0.001; // 円を千円に変換
    cleaned = cleaned.replace(/円/g, "");
  }
  
  // 数値のみを抽出
  cleaned = cleaned.replace(/[^\d.-]/g, "");
  
  const num = parseFloat(cleaned);
  if (isNaN(num)) {
    return undefined;
  }
  
  return num * multiplier;
}

/**
 * 財務情報をFirestoreに保存
 */
async function saveFinancialData(
  companyId: string,
  companyName: string,
  financialData: FinancialData[]
): Promise<void> {
  try {
    const docRef = db.collection("companies_new").doc(companyId);
    const doc = await docRef.get();
    
    if (!doc.exists) {
      writeLog(`⚠️ 企業が見つかりません: ${companyName} (${companyId})`);
      return;
    }
    
    const currentData = doc.data() || {};
    const existingFinancialData = currentData.financialData || [];
    
    // 既存の財務データとマージ（同じ決算期の場合は上書き）
    const mergedFinancialData = [...existingFinancialData];
    
    for (const newData of financialData) {
      const existingIndex = mergedFinancialData.findIndex(
        (fd: FinancialData) => fd.fiscalYear === newData.fiscalYear
      );
      
      if (existingIndex >= 0) {
        mergedFinancialData[existingIndex] = { ...mergedFinancialData[existingIndex], ...newData };
      } else {
        mergedFinancialData.push(newData);
      }
    }
    
    // 決算期でソート
    mergedFinancialData.sort((a, b) => b.fiscalYear.localeCompare(a.fiscalYear));
    
    await docRef.update({
      financialData: mergedFinancialData,
      updatedAt: new Date().toISOString()
    });
    
    writeLog(`✅ 財務情報を保存しました: ${companyName} (${financialData.length}期)`);
  } catch (error) {
    writeLog(`❌ 財務情報保存エラー (${companyName}): ${error}`);
    throw error;
  }
}

/**
 * 関連会社情報をFirestoreに保存
 * 関連会社名は正規化（（株）→株式会社）してから保存
 */
async function saveRelatedCompanies(
  companyId: string,
  companyName: string,
  relatedCompanies: RelatedCompany[]
): Promise<void> {
  try {
    const docRef = db.collection("companies_new").doc(companyId);
    const doc = await docRef.get();
    
    if (!doc.exists) {
      writeLog(`⚠️ 企業が見つかりません: ${companyName} (${companyId})`);
      return;
    }
    
    const currentData = doc.data() || {};
    const existingRelatedCompanies = currentData.relatedCompanies || [];
    
    // 既存の関連会社情報とマージ（同じ会社名の場合は上書き）
    const mergedRelatedCompanies = [...existingRelatedCompanies];
    
    for (const newCompany of relatedCompanies) {
      // 関連会社名を正規化
      const normalizedNewName = normalizeCompanyName(newCompany.name);
      const normalizedCompany: RelatedCompany = {
        ...newCompany,
        name: normalizedNewName
      };
      
      // 既存の関連会社情報を正規化して比較
      const existingIndex = mergedRelatedCompanies.findIndex(
        (rc: RelatedCompany) => normalizeCompanyName(rc.name) === normalizedNewName
      );
      
      if (existingIndex >= 0) {
        // 既存データとマージ（undefinedの項目は既存値を保持）
        mergedRelatedCompanies[existingIndex] = {
          ...mergedRelatedCompanies[existingIndex],
          ...normalizedCompany
        };
      } else {
        mergedRelatedCompanies.push(normalizedCompany);
      }
    }
    
    await docRef.update({
      relatedCompanies: mergedRelatedCompanies,
      updatedAt: new Date().toISOString()
    });
    
    writeLog(`✅ 関連会社情報を保存しました: ${companyName} (${relatedCompanies.length}社)`);
  } catch (error) {
    writeLog(`❌ 関連会社情報保存エラー (${companyName}): ${error}`);
    throw error;
  }
}

/**
 * 子会社の関連会社情報も更新（双方向の関係を構築）
 * 子会社名は正規化（（株）→株式会社）してから検索
 */
async function updateSubsidiaryRelations(
  parentCompanyId: string,
  parentCompanyName: string,
  relatedCompanies: RelatedCompany[]
): Promise<void> {
  for (const relatedCompany of relatedCompanies) {
    try {
      // 子会社名を正規化してから検索
      const normalizedSubsidiaryName = normalizeCompanyName(relatedCompany.name);
      const subsidiaryId = await findCompanyByName(normalizedSubsidiaryName);
      
      if (subsidiaryId) {
        const subsidiaryDocRef = db.collection("companies_new").doc(subsidiaryId);
        const subsidiaryDoc = await subsidiaryDocRef.get();
        
        if (subsidiaryDoc.exists) {
          const subsidiaryData = subsidiaryDoc.data() || {};
          const existingRelatedCompanies = subsidiaryData.relatedCompanies || [];
          
          // 親会社名を正規化
          const normalizedParentName = normalizeCompanyName(parentCompanyName);
          
          // 親会社を関連会社として追加
          const parentAsRelated: RelatedCompany = {
            name: normalizedParentName,
            relationship: relatedCompany.relationship === "子会社" ? "親会社" : "関連会社",
            capital: undefined,
            equityRatio: undefined,
            address: undefined
          };
          
          // 既存の関連会社情報を正規化して比較
          const existingIndex = existingRelatedCompanies.findIndex(
            (rc: RelatedCompany) => normalizeCompanyName(rc.name) === normalizedParentName
          );
          
          if (existingIndex >= 0) {
            // 既存データとマージ（undefinedの項目は既存値を保持）
            existingRelatedCompanies[existingIndex] = {
              ...existingRelatedCompanies[existingIndex],
              ...parentAsRelated
            };
          } else {
            existingRelatedCompanies.push(parentAsRelated);
          }
          
          await subsidiaryDocRef.update({
            relatedCompanies: existingRelatedCompanies,
            updatedAt: new Date().toISOString()
          });
          
          writeLog(`✅ 子会社の関連会社情報を更新しました: ${normalizedSubsidiaryName} -> ${normalizedParentName}`);
        }
      } else {
        writeLog(`⚠️ 子会社が見つかりません: ${normalizedSubsidiaryName} (元の名前: ${relatedCompany.name})`);
      }
    } catch (error) {
      writeLog(`❌ 子会社関連会社更新エラー (${relatedCompany.name}): ${error}`);
    }
  }
}

/**
 * メイン処理
 */
async function main() {
  let browser: Browser | null = null;
  
  try {
    writeLog("🚀 EDINET有価証券報告書スクレイピングを開始します");
    
    // ブラウザを起動（headlessモードで実行）
    browser = await chromium.launch({
      headless: true, // バックグラウンドで実行
      args: ['--no-sandbox', '--disable-setuid-sandbox'] // サーバー環境での実行を安定化
    });
    
    const context = await browser.newContext({
      acceptDownloads: true
    });
    
    const page = await context.newPage();
    
    // listed_parentsコレクションからedinetCodeを取得
    const companies = await getEdinetCodesFromListedParents();
    
    if (companies.length === 0) {
      writeLog("⚠️ edinetCodeが取得できませんでした");
      return;
    }
    
    writeLog(`📋 処理対象企業: ${companies.length}件`);
    
    // 処理済みのedinetCodeをスキップ
    const companiesToProcess = companies.filter(company => 
      !processedEdinetCodes.has(company.edinetCode)
    );
    
    const skippedEdinetCodesCount = companies.length - companiesToProcess.length;
    if (skippedEdinetCodesCount > 0) {
      writeLog(`⏭️ 処理済みのedinetCodeをスキップ: ${skippedEdinetCodesCount}件`);
    }
    
    writeLog(`📋 実際の処理対象企業: ${companiesToProcess.length}件`);
    
    // 各企業のedinetCodeで検索してCSVをダウンロード
    let allDownloadedFiles: string[] = [];
    let processedCompanies = 0;
    let failedCompanies = 0;
    let skippedCompanies = 0;
    
    for (let i = 0; i < companiesToProcess.length; i++) {
      const company = companiesToProcess[i];
      
      // 既に処理済みの場合はスキップ
      if (processedEdinetCodes.has(company.edinetCode)) {
        skippedCompanies++;
        continue;
      }
      
      try {
        writeLog(`\n📊 処理中 [${i + 1}/${companiesToProcess.length}]: ${company.edinetCode}${company.name ? ` (${company.name})` : ""}`);
        
        const downloadedFiles = await searchAndDownloadByEdinetCode(
          page,
          company.edinetCode,
          company.name
        );
        
        allDownloadedFiles.push(...downloadedFiles);
        processedCompanies++;
        
        // 処理済みとしてマーク
        processedEdinetCodes.add(company.edinetCode);
        
        writeLog(`✅ ${company.edinetCode}: ${downloadedFiles.length}件のCSVファイルをダウンロードしました`);
        await sleep(SLEEP_MS);
      } catch (error) {
        writeLog(`❌ エラー (${company.edinetCode}): ${error}`);
        failedCompanies++;
        continue;
      }
    }
    
    writeLog(`\n📊 ダウンロード完了: 成功 ${processedCompanies}件、失敗 ${failedCompanies}件、合計 ${allDownloadedFiles.length}件のCSVファイル`);
    
    // 既存のダウンロードファイルも処理対象に含める（CSVファイルのみ、ZIPは手動展開後に処理）
    let filesToProcess: string[] = [...allDownloadedFiles];
    
    if (fs.existsSync(DOWNLOAD_DIR)) {
      const existingFiles = fs.readdirSync(DOWNLOAD_DIR)
        .filter(f => f.endsWith(".csv") || f.endsWith(".CSV"))
        .map(f => path.join(DOWNLOAD_DIR, f));
      
      writeLog(`📁 既存のCSVファイル: ${existingFiles.length}件（ZIPファイルは手動展開後に処理）`);
      filesToProcess.push(...existingFiles);
    }
    
    // 重複を除去
    filesToProcess = Array.from(new Set(filesToProcess));
    
    if (filesToProcess.length === 0) {
      writeLog("⚠️ 処理するファイルがありません");
      return;
    }
    
    writeLog(`📋 処理対象ファイル: ${filesToProcess.length}件`);
    
    // 各CSVファイルを処理
    let processedCount = 0;
    let skippedCount = 0;
    
    for (const filePath of filesToProcess) {
      writeLog(`📄 処理中: ${path.basename(filePath)}`);
      
      const reports = await parseEdinetCsv(filePath);
      
      if (reports.length === 0) {
        writeLog(`⚠️ レポートが見つかりません: ${path.basename(filePath)}`);
        continue;
      }
      
      for (const report of reports) {
        try {
          // 既に処理済みの場合はスキップ
          if (report.documentId && processedDocumentIds.has(report.documentId)) {
            writeLog(`⏭️ スキップ（処理済み）: ${report.companyName} (${report.documentId})`);
            skippedCount++;
            continue;
          }
          
          // 企業を検索
          const companyId = await findCompanyByName(report.companyName);
          
          if (!companyId) {
            writeLog(`⚠️ 企業が見つかりません: ${report.companyName}`);
            writeCsvRow({
              companyId: "",
              companyName: report.companyName,
              edinetCode: report.edinetCode,
              documentId: report.documentId,
              fiscalYear: report.fiscalYear,
              status: "no_data",
              errorMessage: "企業が見つかりません"
            });
            if (report.documentId) {
              processedDocumentIds.add(report.documentId);
            }
            continue;
          }
          
          // 財務情報を保存
          if (report.financialData.length > 0) {
            await saveFinancialData(companyId, report.companyName, report.financialData);
          }
          
          // 関連会社情報を保存
          if (report.relatedCompanies.length > 0) {
            await saveRelatedCompanies(companyId, report.companyName, report.relatedCompanies);
            
            // 子会社の関連会社情報も更新
            await updateSubsidiaryRelations(companyId, report.companyName, report.relatedCompanies);
          }
          
          writeCsvRow({
            companyId,
            companyName: report.companyName,
            edinetCode: report.edinetCode,
            documentId: report.documentId,
            fiscalYear: report.fiscalYear,
            status: "success"
          });
          
          if (report.documentId) {
            processedDocumentIds.add(report.documentId);
          }
          processedCount++;
          
          await sleep(SLEEP_MS);
        } catch (error) {
          writeLog(`❌ レポート処理エラー (${report.companyName}): ${error}`);
          writeCsvRow({
            companyId: "",
            companyName: report.companyName,
            edinetCode: report.edinetCode,
            documentId: report.documentId,
            fiscalYear: report.fiscalYear,
            status: "failed",
            errorMessage: error instanceof Error ? error.message : String(error)
          });
          if (report.documentId) {
            processedDocumentIds.add(report.documentId);
          }
        }
      }
    }
    
    writeLog(`✅ 処理完了: 処理済み ${processedCount}件、スキップ ${skippedCount}件`);
    
    writeLog("✅ 処理が完了しました");
  } catch (error) {
    writeLog(`❌ エラーが発生しました: ${error}`);
    throw error;
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

// スクリプト実行
main().catch((error) => {
  console.error("❌ 致命的なエラー:", error);
  process.exit(1);
});

