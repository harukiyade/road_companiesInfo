/* eslint-disable no-console */

/**
 * scripts/verify_scraping.ts
 *
 * ✅ 目的
 * - 指定したドキュメントIDの企業情報を対象サイトから取得して検証
 * - Chromiumを画面表示モードで実行して、実際の動作を確認
 * - 取得できたデータを詳細にログ出力
 *
 * ✅ 使用方法
 * COMPANY_ID="企業ID" npx ts-node scripts/verify_scraping.ts
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 * - COMPANY_ID=企業ID (検証したい企業のドキュメントID)
 */

import admin from "firebase-admin";
import * as fs from "fs";
import { chromium, Browser, Page } from "playwright";
import * as cheerio from "cheerio";

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
// 定数
// ------------------------------
const CNAVI_EMAIL = "h.shiroyama@legatuscorp.com";
const CNAVI_PASSWORD = "Furapote0403/";
const BUFFETT_EMAIL = "h.shiroyama@legatuscorp.com";
const BUFFETT_PASSWORD = "furapote0403";
const SLEEP_MS = 2000;

// ------------------------------
// ユーティリティ関数
// ------------------------------
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ------------------------------
// ログイン処理
// ------------------------------

/**
 * バフェットコードにログイン
 */
async function loginToBuffett(page: Page): Promise<boolean> {
  try {
    console.log("[Buffett] ログイン開始...");
    await page.goto("https://www.buffett-code.com/global_screening", {
      waitUntil: "networkidle",
      timeout: 30000,
    });
    await sleep(3000);

    const emailInput = await page.$('input[type="email"], input[name*="email"], input[name*="mail"]');
    if (emailInput) {
      await emailInput.fill(BUFFETT_EMAIL);
    }

    const passwordInput = await page.$('input[type="password"]');
    if (passwordInput) {
      await passwordInput.fill(BUFFETT_PASSWORD);
    }

    const loginButton = await page.$('button[type="submit"], input[type="submit"], button:has-text("ログイン")');
    if (loginButton) {
      await loginButton.click();
      await sleep(5000);
    }

    const currentUrl = page.url();
    const isLoggedIn = !currentUrl.includes("login") && !currentUrl.includes("signin");

    if (isLoggedIn) {
      console.log(`[Buffett] ✅ ログイン成功: ${currentUrl}`);
      return true;
    } else {
      console.log(`[Buffett] ❌ ログイン失敗: ${currentUrl}`);
      return false;
    }
  } catch (error) {
    console.error(`[Buffett] ログインエラー: ${(error as any)?.message}`);
    return false;
  }
}

/**
 * 企業INDEXナビにログイン
 */
async function loginToCnavi(page: Page): Promise<boolean> {
  try {
    console.log("[Cnavi] ログイン開始...");
    await page.goto("https://cnavi-app.g-search.or.jp/?team_id=c5019ad7-745a-4376-a049-7563fae74395", {
      waitUntil: "networkidle",
      timeout: 30000,
    });
    await sleep(3000);

    const emailInput = await page.$('input[name="username"]');
    if (emailInput) {
      await emailInput.fill(CNAVI_EMAIL);
      console.log("[Cnavi] メールアドレス入力完了");
    }

    const passwordInput = await page.$('input[name="password"]');
    if (passwordInput) {
      await passwordInput.fill(CNAVI_PASSWORD);
      console.log("[Cnavi] パスワード入力完了");
    }

    const loginButton = await page.$('button[name="ログイン"]');
    if (loginButton) {
      await loginButton.click();
      await sleep(5000);
    }

    const currentUrl = page.url();
    const isLoggedIn = !currentUrl.includes("login.gh.g-search.or.jp");

    if (isLoggedIn) {
      console.log(`[Cnavi] ✅ ログイン成功: ${currentUrl}`);
      return true;
    } else {
      console.log(`[Cnavi] ❌ ログイン失敗: ${currentUrl}`);
      return false;
    }
  } catch (error) {
    console.error(`[Cnavi] ログインエラー: ${(error as any)?.message}`);
    return false;
  }
}

// ------------------------------
// スクレイピング関数
// ------------------------------

/**
 * nullフィールドを取得（取得対象フィールドのみを返す）
 */
function getNullFields(companyData: any): string[] {
  const nullFields: string[] = [];
  const fieldsToCheck = [
    "phoneNumber", "contactPhoneNumber", "fax", "email", "contactFormUrl",
    "representativeHomeAddress", "representativeBirthDate",
    "industryLarge", "industryMiddle", "industrySmall", "industryDetail",
    "suppliers", "clients", "banks", "executives", "shareholders", "operatingIncome",
  ];

  for (const field of fieldsToCheck) {
    const value = companyData[field];
    if (value === null || value === undefined || 
        (typeof value === "string" && value.trim() === "") ||
        (Array.isArray(value) && value.length === 0)) {
      nullFields.push(field);
    }
  }

  return nullFields;
}

/**
 * HPから情報を取得
 */
async function scrapeFromHomepage(page: Page, homepageUrl: string, companyName: string, nullFields: string[]): Promise<any> {
  const data: any = {};
  console.log(`\n[HP] ${homepageUrl} から情報を取得中...`);

  try {
    await page.goto(homepageUrl, { waitUntil: "domcontentloaded", timeout: 15000 });
    await sleep(3000);
    console.log(`[HP] ページ読み込み完了: ${page.url()}`);

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 電話番号
    const phoneMatch = text.match(/(?:電話|TEL|Tel|電話番号)[：:\s]*([0-9-()]{10,15})/i);
    if (phoneMatch) {
      data.phoneNumber = phoneMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
      console.log(`[HP] ✅ 電話番号取得: ${data.phoneNumber}`);
    } else {
      console.log(`[HP] ❌ 電話番号が見つかりません`);
    }

    // FAX
    const faxMatch = text.match(/(?:FAX|Fax|fax|ファックス)[：:\s]*([0-9-()]{10,15})/i);
    if (faxMatch) {
      data.fax = faxMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
      console.log(`[HP] ✅ FAX取得: ${data.fax}`);
    } else {
      console.log(`[HP] ❌ FAXが見つかりません`);
    }

    // 問い合わせフォーム
    const contactFormLinks = $('a[href*="contact"], a[href*="inquiry"], a[href*="問い合わせ"], a[href*="お問い合わせ"]');
    if (contactFormLinks.length > 0) {
      const href = contactFormLinks.first().attr("href");
      if (href) {
        try {
          data.contactFormUrl = new URL(href, homepageUrl).href;
          console.log(`[HP] ✅ 問い合わせフォーム取得: ${data.contactFormUrl}`);
        } catch {
          console.log(`[HP] ❌ 問い合わせフォームURLの構築に失敗`);
        }
      }
    } else {
      console.log(`[HP] ❌ 問い合わせフォームが見つかりません`);
    }

    // 代表者住所
    const repAddressMatch = text.match(/(?:代表者住所|代表取締役住所|代表者所在)[：:\s]*([^\n\r]{10,100})/i);
    if (repAddressMatch) {
      data.representativeHomeAddress = repAddressMatch[1].trim();
      console.log(`[HP] ✅ 代表者住所取得: ${data.representativeHomeAddress}`);
    } else {
      console.log(`[HP] ❌ 代表者住所が見つかりません`);
    }

    // 代表者生年月日
    const birthMatch = text.match(/(?:代表者|代表取締役)[^\n\r]*(?:生年月日|誕生日)[：:\s]*(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)/i);
    if (birthMatch) {
      data.representativeBirthDate = birthMatch[1].replace(/[年月]/g, "-").replace(/日/g, "");
      console.log(`[HP] ✅ 代表者生年月日取得: ${data.representativeBirthDate}`);
    } else {
      console.log(`[HP] ❌ 代表者生年月日が見つかりません`);
    }

    // 業種
    const industryMatch = text.match(/(?:業種|事業内容)[：:\s]*([^\n\r]{1,200})/i);
    if (industryMatch) {
      const industryText = industryMatch[1].trim();
      const parts = industryText.split(/[>＞]/).map((p) => p.trim());
      if (parts.length >= 1) {
        data.industryLarge = parts[0];
        console.log(`[HP] ✅ 業種（大）取得: ${data.industryLarge}`);
      }
      if (parts.length >= 2) {
        data.industryMiddle = parts[1];
        console.log(`[HP] ✅ 業種（中）取得: ${data.industryMiddle}`);
      }
      if (parts.length >= 3) {
        data.industrySmall = parts[2];
        console.log(`[HP] ✅ 業種（小）取得: ${data.industrySmall}`);
      }
      if (parts.length >= 4) {
        data.industryDetail = parts[3];
        console.log(`[HP] ✅ 業種（細）取得: ${data.industryDetail}`);
      }
    } else {
      console.log(`[HP] ❌ 業種が見つかりません`);
    }

    // 仕入れ先
    const supplierMatches = text.match(/(?:仕入れ先|調達先|主要仕入先)[：:\s]*([^\n\r]{20,500})/gi);
    if (supplierMatches) {
      const suppliers: string[] = [];
      for (const match of supplierMatches) {
        const items = match.split(/[、,;；]/).map((s) => s.trim().replace(/^(?:仕入れ先|調達先|主要仕入先)[：:\s]*/i, ""));
        suppliers.push(...items.filter((s) => s.length > 0));
      }
      if (suppliers.length > 0) {
        data.suppliers = suppliers.slice(0, 20);
        console.log(`[HP] ✅ 仕入れ先取得: ${data.suppliers.length}件`);
      }
    } else {
      console.log(`[HP] ❌ 仕入れ先が見つかりません`);
    }

    // 取引先
    const clientMatches = text.match(/(?:取引先|主要取引先|主な取引先|顧客)[：:\s]*([^\n\r]{20,500})/gi);
    if (clientMatches) {
      const clients: string[] = [];
      for (const match of clientMatches) {
        const items = match.split(/[、,;；]/).map((s) => s.trim().replace(/^(?:取引先|主要取引先|主な取引先|顧客)[：:\s]*/i, ""));
        clients.push(...items.filter((s) => s.length > 0));
      }
      if (clients.length > 0) {
        data.clients = clients.slice(0, 20);
        console.log(`[HP] ✅ 取引先取得: ${data.clients.length}件`);
      }
    } else {
      console.log(`[HP] ❌ 取引先が見つかりません`);
    }

    // 取引先銀行
    const bankMatches = text.match(/(?:取引先銀行|主要取引銀行|取引銀行)[：:\s]*([^\n\r]{20,500})/gi);
    if (bankMatches) {
      const banks: string[] = [];
      for (const match of bankMatches) {
        const items = match.split(/[、,;；]/).map((s) => s.trim().replace(/^(?:取引先銀行|主要取引銀行|取引銀行)[：:\s]*/i, ""));
        banks.push(...items.filter((s) => s.length > 0));
      }
      if (banks.length > 0) {
        data.banks = banks.slice(0, 20);
        console.log(`[HP] ✅ 取引先銀行取得: ${data.banks.length}件`);
      }
    } else {
      console.log(`[HP] ❌ 取引先銀行が見つかりません`);
    }

  } catch (error) {
    console.error(`[HP] ❌ エラー: ${(error as any)?.message}`);
  }

  return data;
}

/**
 * マイナビから情報を取得（正しい検索フロー）
 */
async function scrapeFromMynavi(page: Page, companyName: string, nullFields: string[]): Promise<any> {
  const data: any = {};
  console.log(`\n[マイナビ] ${companyName} を検索中...`);

  try {
    // マイナビ転職の検索ページにアクセス
    await page.goto("https://tenshoku.mynavi.jp/company/", {
      waitUntil: "domcontentloaded",
      timeout: 15000,
    });
    await sleep(2000);

    // 検索テキストボックスに企業名を入力
    const searchInput = await page.$('input[placeholder*="企業名"], input[name*="company"], input[name*="q"]');
    if (searchInput) {
      await searchInput.fill(companyName);
      await sleep(1000);
      console.log(`[マイナビ] 企業名入力完了: ${companyName}`);
    }

    // 「企業を検索する」ボタンを押下
    const searchButton = await page.$('button:has-text("企業を検索する"), button[type="submit"]');
    if (searchButton) {
      await searchButton.click();
      await page.waitForNavigation({ waitUntil: "networkidle", timeout: 15000 }).catch(() => {});
      await sleep(3000);
      console.log(`[マイナビ] 検索ボタンクリック完了`);
    }

    // 「該当する企業はありませんでした。」をチェック
    const pageText = await page.textContent("body");
    if (pageText && (pageText.includes("該当する企業はありませんでした") || pageText.includes("検索結果がありません"))) {
      console.log(`[マイナビ] ❌ 検索結果なし: ${companyName}`);
      return data;
    }

    // 企業リストから企業名のリンクを探してクリック
    const companyLinks = await page.$$('a:has-text("' + companyName + '"), a[href*="/company/"]');
    if (companyLinks.length > 0) {
      let foundLink = false;
      for (const link of companyLinks) {
        const linkText = await link.textContent();
        if (linkText && linkText.trim().includes(companyName)) {
          await link.click();
          await page.waitForNavigation({ waitUntil: "networkidle", timeout: 15000 }).catch(() => {});
          await sleep(3000);
          foundLink = true;
          console.log(`[マイナビ] 企業詳細ページに遷移: ${page.url()}`);
          break;
        }
      }
      if (!foundLink && companyLinks.length > 0) {
        await companyLinks[0].click();
        await page.waitForNavigation({ waitUntil: "networkidle", timeout: 15000 }).catch(() => {});
        await sleep(3000);
        console.log(`[マイナビ] 最初のリンクをクリック: ${page.url()}`);
      }
    }

    console.log(`[マイナビ] ページ読み込み完了: ${page.url()}`);

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 電話番号
    const phoneMatch = text.match(/(?:電話|TEL|Tel|電話番号)[：:\s]*([0-9-()]{10,15})/i);
    if (phoneMatch) {
      data.phoneNumber = phoneMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
      console.log(`[マイナビ] ✅ 電話番号取得: ${data.phoneNumber}`);
    } else {
      console.log(`[マイナビ] ❌ 電話番号が見つかりません`);
    }

    // メール
    const emailMatch = text.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/i);
    if (emailMatch) {
      data.email = emailMatch[1].trim();
      console.log(`[マイナビ] ✅ メール取得: ${data.email}`);
    } else {
      console.log(`[マイナビ] ❌ メールが見つかりません`);
    }

    // 株主
    const shareholderMatches = text.match(/(?:株主|主要株主)[：:\s]*([^\n\r]{10,300})/gi);
    if (shareholderMatches) {
      const shareholders: string[] = [];
      for (const match of shareholderMatches) {
        const items = match.split(/[、,;；]/).map((s) => s.trim().replace(/^(?:株主|主要株主)[：:\s]*/i, ""));
        shareholders.push(...items.filter((s) => s.length > 0));
      }
      if (shareholders.length > 0) {
        data.shareholders = shareholders.slice(0, 20);
        console.log(`[マイナビ] ✅ 株主取得: ${data.shareholders.length}件`);
      }
    } else {
      console.log(`[マイナビ] ❌ 株主が見つかりません`);
    }

    // 業種
    const industryMatch = text.match(/(?:業種|事業内容)[：:\s]*([^\n\r]{1,200})/i);
    if (industryMatch) {
      const industryText = industryMatch[1].trim();
      const parts = industryText.split(/[>＞]/).map((p) => p.trim());
      if (parts.length >= 1) {
        data.industryLarge = parts[0];
        console.log(`[マイナビ] ✅ 業種（大）取得: ${data.industryLarge}`);
      }
      if (parts.length >= 2) {
        data.industryMiddle = parts[1];
        console.log(`[マイナビ] ✅ 業種（中）取得: ${data.industryMiddle}`);
      }
      if (parts.length >= 3) {
        data.industrySmall = parts[2];
        console.log(`[マイナビ] ✅ 業種（小）取得: ${data.industrySmall}`);
      }
      if (parts.length >= 4) {
        data.industryDetail = parts[3];
        console.log(`[マイナビ] ✅ 業種（細）取得: ${data.industryDetail}`);
      }
    } else {
      console.log(`[マイナビ] ❌ 業種が見つかりません`);
    }

    // 取引先
    const clientMatches = text.match(/(?:取引先|主要取引先)[：:\s]*([^\n\r]{20,500})/gi);
    if (clientMatches) {
      const clients: string[] = [];
      for (const match of clientMatches) {
        const items = match.split(/[、,;；]/).map((s) => s.trim().replace(/^(?:取引先|主要取引先)[：:\s]*/i, ""));
        clients.push(...items.filter((s) => s.length > 0));
      }
      if (clients.length > 0) {
        data.clients = clients.slice(0, 20);
        console.log(`[マイナビ] ✅ 取引先取得: ${data.clients.length}件`);
      }
    } else {
      console.log(`[マイナビ] ❌ 取引先が見つかりません`);
    }

  } catch (error) {
    console.error(`[マイナビ] ❌ エラー: ${(error as any)?.message}`);
  }

  return data;
}

// ------------------------------
// メイン処理
// ------------------------------

async function main() {
  const companyId = process.env.COMPANY_ID;
  
  if (!companyId) {
    console.error("❌ エラー: COMPANY_ID 環境変数が設定されていません。");
    console.error("使用方法: COMPANY_ID=\"企業ID\" npx ts-node scripts/verify_scraping.ts");
    process.exit(1);
  }

  console.log("=".repeat(80));
  console.log(`検証スクリプト開始: 企業ID ${companyId}`);
  console.log("=".repeat(80));

  let browser: Browser | null = null;

  try {
    // Firestoreから企業データを取得
    const companyDoc = await db.collection("companies_new").doc(companyId).get();
    
    if (!companyDoc.exists) {
      console.error(`❌ エラー: 企業ID ${companyId} が見つかりません。`);
      process.exit(1);
    }

    const companyData = companyDoc.data();
    const companyName = companyData?.name || "";
    const homepageUrl = companyData?.companyUrl || null;

    console.log(`\n企業名: ${companyName}`);
    console.log(`HP URL: ${homepageUrl || "なし"}`);
    console.log("\n" + "=".repeat(80));

    // ブラウザを起動（画面表示モード）
    console.log("\nブラウザを起動中（画面表示モード）...");
    browser = await chromium.launch({ 
      headless: false, // 画面を表示
      slowMo: 500, // 動作を遅くして確認しやすくする
    });
    const page = await browser.newPage();
    
    // 画面サイズを設定
    await page.setViewportSize({ width: 1280, height: 720 });

    // バフェットコードにログイン
    console.log("\n" + "-".repeat(80));
    await loginToBuffett(page);
    await sleep(2000);

    // 企業INDEXナビにログイン
    console.log("\n" + "-".repeat(80));
    await loginToCnavi(page);
    await sleep(2000);

    // nullフィールドを取得
    const nullFields = getNullFields(companyData);
    console.log(`\n不足フィールド: ${nullFields.length}件 (${nullFields.join(", ")})`);

    // HPから情報取得
    if (homepageUrl) {
      console.log("\n" + "-".repeat(80));
      const hpData = await scrapeFromHomepage(page, homepageUrl, companyName, nullFields);
      console.log("\n[HP] 取得結果:", JSON.stringify(hpData, null, 2));
      await sleep(3000);
    }

    // マイナビから情報取得
    console.log("\n" + "-".repeat(80));
    const mynaviData = await scrapeFromMynavi(page, companyName, nullFields);
    console.log("\n[マイナビ] 取得結果:", JSON.stringify(mynaviData, null, 2));

    console.log("\n" + "=".repeat(80));
    console.log("✅ 検証完了");
    console.log("=".repeat(80));
    console.log("\nブラウザを10秒間表示します。確認後、自動的に閉じます...");
    
    await sleep(10000);

  } catch (error) {
    console.error("❌ エラー:", error);
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

// ------------------------------
// 実行
// ------------------------------
main()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });

