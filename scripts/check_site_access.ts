#!/usr/bin/env npx ts-node
/**
 * 各サイトへのアクセス状況を確認するスクリプト
 */

import { chromium, Browser, Page } from "playwright";

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface SiteCheckResult {
  siteName: string;
  url: string;
  accessible: boolean;
  error?: string;
  details?: string;
}

async function checkSiteAccess(page: Page, siteName: string, url: string, testAction?: (page: Page) => Promise<boolean>): Promise<SiteCheckResult> {
  try {
    console.log(`\n[${siteName}] アクセス確認中: ${url}`);
    
    const response = await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 15000,
    });

    if (!response || !response.ok()) {
      return {
        siteName,
        url,
        accessible: false,
        error: `HTTP ${response?.status() || "Unknown"}`,
      };
    }

    await sleep(2000);

    // テストアクションが指定されている場合は実行
    if (testAction) {
      const actionResult = await testAction(page);
      if (!actionResult) {
        return {
          siteName,
          url,
          accessible: false,
          error: "テストアクション失敗",
        };
      }
    }

    const title = await page.title();
    const currentUrl = page.url();

    return {
      siteName,
      url,
      accessible: true,
      details: `タイトル: ${title}, URL: ${currentUrl}`,
    };
  } catch (error: any) {
    return {
      siteName,
      url,
      accessible: false,
      error: error.message || "Unknown error",
    };
  }
}

async function main() {
  console.log("==========================================");
  console.log("各サイトへのアクセス状況を確認します");
  console.log("==========================================\n");

  const browser = await chromium.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  const page = await browser.newPage();
  page.setDefaultTimeout(15000);

  const results: SiteCheckResult[] = [];

  // 1. 企業INDEXナビ（Cnavi）
  try {
    const result = await checkSiteAccess(
      page,
      "企業INDEXナビ（トップ）",
      "https://cnavi-app.g-search.or.jp/",
      async (p) => {
        await sleep(3000);
        const currentUrl = p.url();
        return currentUrl.includes("cnavi-app.g-search.or.jp") || currentUrl.includes("login");
      }
    );
    results.push(result);
  } catch (error: any) {
    results.push({
      siteName: "企業INDEXナビ（トップ）",
      url: "https://cnavi-app.g-search.or.jp/",
      accessible: false,
      error: error.message,
    });
  }

  // 2. バフェットコード
  try {
    const result = await checkSiteAccess(
      page,
      "バフェットコード",
      "https://www.buffett-code.com/global_screening",
      async (p) => {
        await sleep(2000);
        const searchInput = await p.$('input[type="text"], input[type="search"]');
        return searchInput !== null;
      }
    );
    results.push(result);
  } catch (error: any) {
    results.push({
      siteName: "バフェットコード",
      url: "https://www.buffett-code.com/global_screening",
      accessible: false,
      error: error.message,
    });
  }

  // 3. マイナビ転職
  try {
    const result = await checkSiteAccess(
      page,
      "マイナビ転職",
      "https://tenshoku.mynavi.jp/company/",
      async (p) => {
        await sleep(2000);
        const searchInput = await p.$('input[type="text"], input[type="search"]');
        return searchInput !== null;
      }
    );
    results.push(result);
  } catch (error: any) {
    results.push({
      siteName: "マイナビ転職",
      url: "https://tenshoku.mynavi.jp/company/",
      accessible: false,
      error: error.message,
    });
  }

  // 4. マイナビ2026
  try {
    const result = await checkSiteAccess(
      page,
      "マイナビ2026",
      "https://job.mynavi.jp/26/pc/search/corp.html?tab=corp",
      async (p) => {
        await sleep(3000);
        // 広告を閉じる
        try {
          const closeButtons = await p.$$('button[aria-label*="閉じる"], button[aria-label*="close"], .close, [class*="close"]');
          for (const btn of closeButtons) {
            try {
              if (await btn.isVisible()) {
                await btn.click();
                await sleep(500);
              }
            } catch {}
          }
        } catch {}
        
        const searchInput = await p.$('input[type="text"], input[type="search"], input[name*="search"]');
        return searchInput !== null;
      }
    );
    results.push(result);
  } catch (error: any) {
    results.push({
      siteName: "マイナビ2026",
      url: "https://job.mynavi.jp/26/pc/search/corp.html?tab=corp",
      accessible: false,
      error: error.message,
    });
  }

  // 5. 全国法人リスト
  try {
    const result = await checkSiteAccess(
      page,
      "全国法人リスト",
      "https://houjin.jp/",
      async (p) => {
        await sleep(2000);
        const searchInput = await p.$('input[type="text"], input[type="search"]');
        return searchInput !== null;
      }
    );
    results.push(result);
  } catch (error: any) {
    results.push({
      siteName: "全国法人リスト",
      url: "https://houjin.jp/",
      accessible: false,
      error: error.message,
    });
  }

  // 6. 官報決算データベース（catr.jp）
  try {
    const result = await checkSiteAccess(
      page,
      "官報決算データベース",
      "https://catr.jp/",
      async (p) => {
        await sleep(2000);
        const searchInput = await p.$('input[type="text"], input[type="search"]');
        return searchInput !== null;
      }
    );
    results.push(result);
  } catch (error: any) {
    results.push({
      siteName: "官報決算データベース",
      url: "https://catr.jp/",
      accessible: false,
      error: error.message,
    });
  }

  // 7. Alarmbox
  try {
    const result = await checkSiteAccess(
      page,
      "Alarmbox",
      "https://alarmbox.jp/companyinfo/",
      async (p) => {
        await sleep(2000);
        const searchInput = await p.$('input[type="text"], input[type="search"]');
        return searchInput !== null;
      }
    );
    results.push(result);
  } catch (error: any) {
    results.push({
      siteName: "Alarmbox",
      url: "https://alarmbox.jp/companyinfo/",
      accessible: false,
      error: error.message,
    });
  }

  // 8. uSonar YELLOWPAGE
  try {
    const result = await checkSiteAccess(
      page,
      "uSonar YELLOWPAGE",
      "https://yellowpage.usonar.co.jp/",
      async (p) => {
        await sleep(2000);
        const searchInput = await p.$('input[type="text"], input[type="search"]');
        return searchInput !== null;
      }
    );
    results.push(result);
  } catch (error: any) {
    results.push({
      siteName: "uSonar YELLOWPAGE",
      url: "https://yellowpage.usonar.co.jp/",
      accessible: false,
      error: error.message,
    });
  }

  await browser.close();

  // 結果を表示
  console.log("\n==========================================");
  console.log("アクセス確認結果");
  console.log("==========================================\n");

  const accessibleSites = results.filter(r => r.accessible);
  const inaccessibleSites = results.filter(r => !r.accessible);

  console.log("✅ アクセス可能なサイト:");
  accessibleSites.forEach(r => {
    console.log(`  ✓ ${r.siteName}`);
    if (r.details) {
      console.log(`    ${r.details}`);
    }
  });

  console.log("\n❌ アクセスできないサイト:");
  inaccessibleSites.forEach(r => {
    console.log(`  ✗ ${r.siteName}`);
    console.log(`    URL: ${r.url}`);
    console.log(`    エラー: ${r.error}`);
  });

  console.log(`\n合計: ${results.length}サイト中、${accessibleSites.length}サイトがアクセス可能、${inaccessibleSites.length}サイトがアクセス不可`);
}

main().catch(console.error);

