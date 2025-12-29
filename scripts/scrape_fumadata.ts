/* eslint-disable no-console */

/**
 * scripts/scrape_fumadata.ts
 *
 * ✅ 目的
 * - companydata.tsujigawa.com から企業情報をスクレイピング
 * - companies_new コレクションにない企業のみ取得
 * - 既存企業でも不足しているフィールドがあれば取得
 * - 取得内容を CSV に出力（companies_fumadata.csv）
 *
 * ✅ 使用方法
 * 1. 企業リストページのURLを指定するか、企業名のリストをCSVで提供
 * 2. スクリプトがアイコンをクリックして企業詳細ページに遷移し、情報を取得
 * 3. companies_new コレクションと比較して、新規または不足フィールドがある企業のみ処理
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 *
 * ✅ 注意点
 * - 負荷をかけないよう、リクエスト間隔を長めに設定（デフォルト: 3秒）
 * - 企業リストページの表示はユーザーが行う想定
 * - アイコンをクリックして詳細ページに遷移する処理を実装
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";
import { chromium, Browser, Page } from "playwright";
import * as cheerio from "cheerio";
import fetch from "node-fetch";

// ------------------------------
// Firebase Admin SDK 初期化
// ------------------------------
let db: admin.firestore.Firestore | null = null;
let firebaseEnabled = false;

if (!admin.apps.length) {
  try {
    const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
    
    if (serviceAccountPath && fs.existsSync(serviceAccountPath)) {
      const serviceAccount = JSON.parse(
        fs.readFileSync(serviceAccountPath, "utf8")
      );
      
      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        projectId: "albert-ma",
      });
      
      db = admin.firestore();
      firebaseEnabled = true;
      console.log("[Firebase初期化] ✅ 初期化が完了しました");
    } else {
      console.warn("[警告] FIREBASE_SERVICE_ACCOUNT_KEY が設定されていないため、Firestoreチェックをスキップします（テストモード）");
      firebaseEnabled = false;
    }
  } catch (error) {
    console.warn("[警告] Firebase初期化エラー:", error);
    console.warn("[警告] Firestoreチェックをスキップします（テストモード）");
    firebaseEnabled = false;
  }
} else {
  db = admin.firestore();
  firebaseEnabled = true;
}

// ------------------------------
// 設定
// ------------------------------
const REQUEST_DELAY_MS = 3000; // リクエスト間隔（3秒）
const TIMEOUT_MS = 30000; // タイムアウト（30秒）
const BASE_URL = "https://companydata.tsujigawa.com";

// ------------------------------
// ユーティリティ関数
// ------------------------------
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * CSVの値をエスケープ
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

// ------------------------------
// fumadata.com からの情報抽出
// ------------------------------

/**
 * グラフから数値を取得（マウスオーバーでツールチップから取得）
 */
async function extractChartData(
  page: Page,
  chartSelector: string
): Promise<number[]> {
  const values: number[] = [];
  
  try {
    // グラフ要素を探す
    let chartElement = await page.$(chartSelector);
    if (!chartElement) {
      // 代替セレクタを試行（fumadata.comの構造に合わせて）
      const altSelectors = [
        '.chart',
        '.graph',
        '[data-chart]',
        'canvas',
        'svg',
        '.revenue-chart',
        '.profit-chart',
        '.業績データ',
        '.performance-chart',
        '[class*="chart"]',
        '[class*="graph"]',
      ];
      
      for (const selector of altSelectors) {
        const element = await page.$(selector);
        if (element) {
          chartElement = element;
          break;
        }
      }
    }
    
    if (chartElement) {
      // グラフのバーやポイントを探してマウスオーバー
      const bars = await chartElement.$$('rect, circle, path, .bar, .point, [data-value], [data-year], g[class*="bar"], g[class*="point"]');
      
      // 年度順にソート（左から右へ）
      const sortedBars = [];
      for (let i = 0; i < bars.length; i++) {
        try {
          const box = await bars[i].boundingBox();
          if (box) {
            sortedBars.push({ element: bars[i], x: box.x });
          }
        } catch {}
      }
      sortedBars.sort((a, b) => a.x - b.x);
      
      for (let i = 0; i < Math.min(sortedBars.length, 5); i++) {
        try {
          await sortedBars[i].element.hover();
          await sleep(800); // ツールチップが表示されるまで待機
          
          // ツールチップから数値を取得（複数のセレクタを試行）
          const tooltipSelectors = [
            '.tooltip',
            '.chart-tooltip',
            '[role="tooltip"]',
            '.popover',
            '[class*="tooltip"]',
            '[class*="popover"]',
            '[class*="hover"]',
          ];
          
          for (const tooltipSelector of tooltipSelectors) {
            const tooltip = await page.$(tooltipSelector);
            if (tooltip) {
              const tooltipText = await tooltip.textContent();
              if (tooltipText) {
                // 数値を抽出（百万円単位、負の値も含む）
                const valueMatch = tooltipText.match(/(-?[\d,]+)\s*百万円/i);
                if (valueMatch) {
                  const value = parseFloat(valueMatch[1].replace(/,/g, ""));
                  if (!isNaN(value)) {
                    values.push(value * 1000000); // 百万円を千円に変換（100万倍）
                    break;
                  }
                }
              }
            }
          }
        } catch (e) {
          // エラーは無視して続行
        }
      }
    }
  } catch (error) {
    // エラーは無視
  }
  
  return values;
}

/**
 * companydata.tsujigawa.com の企業詳細ページから情報を抽出
 */
async function extractCompanyInfoFromFumadata(
  page: Page,
  companyName: string
): Promise<Partial<any>> {
  const info: Partial<any> = {
    companyName: companyName,
    sourceUrls: [],
    licenses: [],
    banks: [],
    directors: [],
    sns: [],
    officers: [],
    shareholders: [],
  };

  try {
    const html = await page.content();
    const $ = cheerio.load(html);
    
    // 企業データ欄を特定（業績データより上のみ）
    let companyDataSection = $('.company-data, .企業データ, [data-company-data]').first();
    if (companyDataSection.length === 0) {
      // 業績データセクションの前までを取得
      const performanceSection = $('.performance-data, .業績データ, [data-performance]').first();
      if (performanceSection.length > 0) {
        // 業績データより前の部分を取得
        companyDataSection = performanceSection.prevAll().first();
      }
    }
    
    // 企業データ欄のテキストを取得（業績データより上のみ）
    const companyDataText = companyDataSection.length > 0 
      ? companyDataSection.text().replace(/\s+/g, " ").trim()
      : "";
    
    const text = $("body").text().replace(/\s+/g, " ").trim();
    const bodyHtml = $("body").html() || "";
    const currentUrl = page.url();
    
    info.sourceUrls = [currentUrl];

    // 企業名
    const nameSelectors = [
      'h1.company-name',
      'h1',
      '.company-title',
      '[data-company-name]',
      '.company-info h1',
      '.company-info h2',
    ];
    for (const selector of nameSelectors) {
      const nameText = $(selector).first().text().trim();
      if (nameText && nameText.length > 0) {
        info.companyName = nameText;
        break;
      }
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

    // 公式サイトURL（companydata.tsujigawa.comの構造に合わせて抽出）
    const websiteSelectors = [
      'a[href^="http"]',
      'a[href*="www"]',
      '.website a',
      '.company-url a',
      '[data-website] a',
      '.homepage a',
    ];
    
    for (const selector of websiteSelectors) {
      $(selector).each((_, el) => {
        const href = $(el).attr("href");
        const linkText = $(el).text().trim().toLowerCase();
        const parentText = $(el).parent().text().trim().toLowerCase();
        
        if (href && href.startsWith("http") && !href.includes("fumadata.com")) {
          // 公式サイトっぽいリンクを探す
          if (
            linkText.includes("公式") || 
            linkText.includes("hp") || 
            linkText.includes("ホームページ") ||
            linkText.includes("website") ||
            linkText.includes("url") ||
            parentText.includes("公式") ||
            parentText.includes("ホームページ") ||
            href.includes("co.jp") ||
            href.includes(".jp")
          ) {
            try {
              const urlObj = new URL(href);
              // 企業サイトっぽいURLのみ
              if (!urlObj.hostname.includes("fumadata") && 
                  !urlObj.hostname.includes("google") &&
                  !urlObj.hostname.includes("facebook") &&
                  !urlObj.hostname.includes("twitter")) {
                info.website = urlObj.href;
                info.companyUrl = urlObj.href;
                return false; // break
              }
            } catch {}
          }
        }
      });
      if (info.website) break;
    }

    // 問い合わせフォーム
    const contactFormMatch = text.match(
      /(お問い合わせ|問い合わせ|コンタクト|contact)[^。]*?([^\s]+\.(html|php|aspx?|jsp))/i
    );
    if (contactFormMatch) {
      try {
        const baseUrl = new URL(currentUrl).origin;
        info.contactFormUrl = new URL(contactFormMatch[2], baseUrl).href;
      } catch {}
    }

    // 資本金（companydata.tsujigawa.comの企業データ欄から抽出）
    // 企業データ欄から抽出を試行
    if (companyDataSection.length > 0) {
      companyDataSection.find('td, th, li, dt, dd, .capital, [data-capital]').each((_, el) => {
        const cellText = $(el).text();
        if ((cellText.includes("資本金") || cellText.includes("資本")) && !info.capital) {
          const capitalPatterns = [
          /資本金[：:\s]*([\d,]+)\s*(百万円|億円|万円|千円|円|百万|億|万|千)/i,
          /資本[：:\s]*([\d,]+)\s*(百万円|億円|万円|千円|円|百万|億|万|千)/i,
        ];
        for (const pattern of capitalPatterns) {
          const match = cellText.match(pattern);
          if (match) {
            const value = parseFloat(match[1].replace(/,/g, ""));
            if (!isNaN(value) && value > 0) {
              // 百万円の場合は100万倍（千円単位に変換：百万円 × 1,000,000 = 千円）
              if (match[2] && (match[2].includes("百万") || match[2].includes("百万円"))) {
                info.capital = value * 1000000;
              } else if (match[2] && (match[2].includes("億") || match[2].includes("億円"))) {
                // 億円の場合は100,000倍（千円単位に変換：億円 × 100,000 = 千円）
                info.capital = value * 100000;
              } else if (match[2] && (match[2].includes("万") || match[2].includes("万円"))) {
                // 万円の場合は10倍（千円単位に変換：万円 × 10 = 千円）
                info.capital = value * 10;
              } else {
                info.capital = normalizeToThousandYen(value, match[2] || "");
              }
              return false; // break
            }
          }
        }
        }
      });
    }
    
    // テキストからも抽出を試行
    if (!info.capital) {
      const capitalPatterns = [
        /資本金[：:\s]*([\d,]+)\s*(百万円|億円|万円|千円|円|百万|億|万|千)/i,
        /資本[：:\s]*([\d,]+)\s*(百万円|億円|万円|千円|円|百万|億|万|千)/i,
      ];
      for (const pattern of capitalPatterns) {
        const match = text.match(pattern);
        if (match) {
          const value = parseFloat(match[1].replace(/,/g, ""));
          if (!isNaN(value) && value > 0) {
            // 百万円の場合は100万倍（千円単位に変換：百万円 × 1,000,000 = 千円）
            if (match[2] && (match[2].includes("百万") || match[2].includes("百万円"))) {
              info.capital = value * 1000000;
            } else if (match[2] && (match[2].includes("億") || match[2].includes("億円"))) {
              // 億円の場合は100,000倍（千円単位に変換：億円 × 100,000 = 千円）
              info.capital = value * 100000;
            } else if (match[2] && (match[2].includes("万") || match[2].includes("万円"))) {
              // 万円の場合は10倍（千円単位に変換：万円 × 10 = 千円）
              info.capital = value * 10;
            } else {
              info.capital = normalizeToThousandYen(value, match[2] || "");
            }
            break;
          }
        }
      }
    }

    // 売上高（業績データ項目から取得、全年度を取得、百万円表示なので100万倍）
    let revenueData: number[] = [];
    
    // グラフから数値を取得（マウスオーバーでツールチップから取得）
    try {
      const chartData = await extractChartData(page, '.revenue-chart, .sales-chart, [data-chart="revenue"]');
      if (chartData.length > 0) {
        revenueData = chartData;
      }
    } catch (e) {
      // エラーは無視
    }
    
    // グラフのデータ属性やSVG要素から数値を抽出
    if (revenueData.length === 0) {
      $('[data-revenue], [data-sales], .revenue-chart, .sales-chart, canvas, svg').each((_, el) => {
        const dataValue = $(el).attr('data-value') || $(el).attr('data-revenue') || $(el).attr('data-sales');
        if (dataValue) {
          const values = dataValue.split(',').map(v => parseFloat(v.replace(/,/g, ""))).filter(v => !isNaN(v) && v > 0);
          if (values.length > 0) {
            revenueData.push(...values.map(v => v * 1000000)); // 百万円を千円に変換（100万倍）
          }
        }
      });
    }
    
    // テーブルやリストから年度ごとの数値を抽出
    if (revenueData.length === 0) {
      $('td, th, li, dt, dd').each((_, el) => {
        const cellText = $(el).text();
        // 年度パターン（21/3, 22/3など）と数値の組み合わせを探す
        const yearPattern = /(\d{2})\/(\d{1,2})[：:\s]*([\d,]+)\s*百万円/i;
        const match = cellText.match(yearPattern);
        if (match) {
          const value = parseFloat(match[3].replace(/,/g, ""));
          if (!isNaN(value) && value > 0) {
            revenueData.push(value * 1000000); // 百万円を千円に変換（100万倍）
          }
        }
      });
    }
    
    // 業績データセクションから全年度の数値を抽出
    if (revenueData.length === 0) {
      // 業績データセクションを探す
      const performanceSection = $('.performance-data, .financial-data, [data-performance], [data-financial], .業績データ, .chart-container, .graph-container').text();
      const performanceHtml = $('.performance-data, .financial-data, [data-performance], [data-financial], .業績データ, .chart-container, .graph-container').html() || "";
      
      // HTMLからも抽出を試行（グラフのデータ属性など）
      const htmlDataPattern = /data-value=["']([^"']+)["']|data-revenue=["']([^"']+)["']|data-sales=["']([^"']+)["']/gi;
      let htmlMatch;
      while ((htmlMatch = htmlDataPattern.exec(performanceHtml)) !== null) {
        const dataStr = htmlMatch[1] || htmlMatch[2] || htmlMatch[3];
        if (dataStr) {
          const values = dataStr.split(',').map(v => parseFloat(v.replace(/,/g, ""))).filter(v => !isNaN(v) && v > 0);
          if (values.length > 0) {
            revenueData.push(...values.map(v => v * 1000000));
            break;
          }
        }
      }
      
      // テキストから年度パターンで数値を抽出（21/3, 22/3, 23/3, 24/3, 25/3など）
      if (revenueData.length === 0 && performanceSection) {
        const yearRevenuePattern = /(\d{2})\/(\d{1,2})[：:\s]*([\d,]+)\s*百万円/gi;
        let match;
        while ((match = yearRevenuePattern.exec(performanceSection)) !== null) {
          const value = parseFloat(match[3].replace(/,/g, ""));
          if (!isNaN(value) && value > 0) {
            revenueData.push(value * 1000000); // 百万円を千円に変換（100万倍）
          }
        }
      }
      
      // グラフのバーやポイントから数値を抽出（data属性やaria-labelなど）
      if (revenueData.length === 0) {
        $('[data-year], [data-fiscal-year], [aria-label*="売上"], [title*="売上"]').each((_, el) => {
          const year = $(el).attr('data-year') || $(el).attr('data-fiscal-year');
          const valueAttr = $(el).attr('data-value') || $(el).attr('aria-label') || $(el).attr('title');
          if (year && valueAttr) {
            const valueMatch = valueAttr.match(/([\d,]+)\s*百万円/i);
            if (valueMatch) {
              const value = parseFloat(valueMatch[1].replace(/,/g, ""));
              if (!isNaN(value) && value > 0) {
                revenueData.push(value * 1000000);
              }
            }
          }
        });
      }
    }
    
    // 最新年度の売上高を設定
    if (revenueData.length > 0) {
      info.latestRevenue = revenueData[revenueData.length - 1];
      info.revenue = info.latestRevenue;
      // 過去5年分の売上高を設定（最新が最後）
      if (revenueData.length >= 5) {
        info.revenue5 = revenueData[0];
        info.revenue4 = revenueData[1];
        info.revenue3 = revenueData[2];
        info.revenue2 = revenueData[3];
        info.revenue1 = revenueData[4];
      } else if (revenueData.length >= 1) {
        info.revenue1 = revenueData[revenueData.length - 1];
      }
    }
    
    // フォールバック: 単一の売上高値を抽出
    if (!info.revenue) {
      const millionPattern = /(?:売上高|売上|営業収益)[：:\s]*([\d,]+)\s*百万円/i;
      const millionMatch = text.match(millionPattern);
      if (millionMatch) {
        const value = parseFloat(millionMatch[1].replace(/,/g, ""));
        if (!isNaN(value) && value > 0) {
          info.revenue = value * 1000000; // 百万円を千円に変換（100万倍）
          info.latestRevenue = info.revenue;
          info.revenue1 = info.revenue;
        }
      }
    }

    // 利益（営業利益/経常利益/当期純利益）
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

    // 純利益（業績データ項目から取得、全年度を取得、百万円表示なので100万倍）
    let profitData: number[] = [];
    
    // グラフから数値を取得（マウスオーバーでツールチップから取得）
    try {
      const chartData = await extractChartData(page, '.profit-chart, .net-profit-chart, [data-chart="profit"]');
      if (chartData.length > 0) {
        profitData = chartData;
      }
    } catch (e) {
      // エラーは無視
    }
    
    // グラフのデータ属性やSVG要素から数値を抽出
    if (profitData.length === 0) {
      $('[data-profit], [data-net-profit], .profit-chart, .net-profit-chart, canvas, svg').each((_, el) => {
        const dataValue = $(el).attr('data-value') || $(el).attr('data-profit') || $(el).attr('data-net-profit');
        if (dataValue) {
          const values = dataValue.split(',').map(v => parseFloat(v.replace(/,/g, ""))).filter(v => !isNaN(v));
          if (values.length > 0) {
            profitData.push(...values.map(v => v * 1000000)); // 百万円を千円に変換（100万倍、負の値も含む）
          }
        }
      });
    }
    
    // テーブルやリストから年度ごとの数値を抽出
    if (profitData.length === 0) {
      $('td, th, li, dt, dd').each((_, el) => {
        const cellText = $(el).text();
        // 年度パターン（21/3, 22/3など）と数値の組み合わせを探す（純利益は負の値もあり得る）
        const yearPattern = /(\d{2})\/(\d{1,2})[：:\s]*(-?[\d,]+)\s*百万円/i;
        const match = cellText.match(yearPattern);
        if (match) {
          const value = parseFloat(match[3].replace(/,/g, ""));
          if (!isNaN(value)) {
            profitData.push(value * 1000000); // 百万円を千円に変換（100万倍、負の値も含む）
          }
        }
      });
    }
    
    // 業績データセクションから全年度の数値を抽出
    if (profitData.length === 0) {
      // 業績データセクションを探す
      const performanceSection = $('.performance-data, .financial-data, [data-performance], [data-financial], .業績データ, .chart-container, .graph-container').text();
      const performanceHtml = $('.performance-data, .financial-data, [data-performance], [data-financial], .業績データ, .chart-container, .graph-container').html() || "";
      
      // HTMLからも抽出を試行（グラフのデータ属性など）
      const htmlDataPattern = /data-value=["']([^"']+)["']|data-profit=["']([^"']+)["']|data-net-profit=["']([^"']+)["']/gi;
      let htmlMatch;
      while ((htmlMatch = htmlDataPattern.exec(performanceHtml)) !== null) {
        const dataStr = htmlMatch[1] || htmlMatch[2] || htmlMatch[3];
        if (dataStr) {
          const values = dataStr.split(',').map(v => parseFloat(v.replace(/,/g, ""))).filter(v => !isNaN(v));
          if (values.length > 0) {
            profitData.push(...values.map(v => v * 1000000));
            break;
          }
        }
      }
      
      // テキストから年度パターンで数値を抽出（21/3, 22/3, 23/3, 24/3, 25/3など、純利益は負の値もあり得る）
      if (profitData.length === 0 && performanceSection) {
        const yearProfitPattern = /(\d{2})\/(\d{1,2})[：:\s]*(-?[\d,]+)\s*百万円/gi;
        let match;
        while ((match = yearProfitPattern.exec(performanceSection)) !== null) {
          const value = parseFloat(match[3].replace(/,/g, ""));
          if (!isNaN(value)) {
            profitData.push(value * 1000000); // 百万円を千円に変換（100万倍、負の値も含む）
          }
        }
      }
      
      // グラフのバーやポイントから数値を抽出（data属性やaria-labelなど）
      if (profitData.length === 0) {
        $('[data-year], [data-fiscal-year], [aria-label*="純利益"], [title*="純利益"]').each((_, el) => {
          const year = $(el).attr('data-year') || $(el).attr('data-fiscal-year');
          const valueAttr = $(el).attr('data-value') || $(el).attr('aria-label') || $(el).attr('title');
          if (year && valueAttr) {
            const valueMatch = valueAttr.match(/(-?[\d,]+)\s*百万円/i);
            if (valueMatch) {
              const value = parseFloat(valueMatch[1].replace(/,/g, ""));
              if (!isNaN(value)) {
                profitData.push(value * 1000000);
              }
            }
          }
        });
      }
    }
    
    // 最新年度の純利益を設定
    if (profitData.length > 0) {
      info.latestProfit = profitData[profitData.length - 1];
      info.profit = info.latestProfit;
      // 過去5年分の純利益を設定（最新が最後）
      if (profitData.length >= 5) {
        info.profit5 = profitData[0];
        info.profit4 = profitData[1];
        info.profit3 = profitData[2];
        info.profit2 = profitData[3];
        info.profit1 = profitData[4];
      } else if (profitData.length >= 1) {
        info.profit1 = profitData[profitData.length - 1];
      }
    }
    
    // フォールバック: 単一の純利益値を抽出
    if (!info.profit) {
      const millionPattern = /(?:当期純利益|純利益)[：:\s]*(-?[\d,]+)\s*百万円/i;
      const millionMatch = text.match(millionPattern);
      if (millionMatch) {
        const value = parseFloat(millionMatch[1].replace(/,/g, ""));
        if (!isNaN(value)) {
          info.profit = value * 1000000; // 百万円を千円に変換（100万倍、負の値も含む）
          info.latestProfit = info.profit;
          info.profit1 = info.profit;
        }
      }
    }

    // 総資産
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

    // 総負債
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

    // 純資産
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

    // 業種
    const industryMatch = text.match(/業種[：:]\s*([^\n\r]+)/i);
    if (industryMatch) {
      info.industry = industryMatch[1].trim();
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

    // 企業説明・概要（不要な文字列を除去）
    const descriptionSelectors = [
      '.company-description',
      '.company-overview',
      '.company-info',
      '[data-company-description]',
    ];
    for (const selector of descriptionSelectors) {
      let desc = $(selector).first().text().trim();
      if (desc && desc.length > 20) {
        // 不要な文字列を除去
        desc = desc
          .replace(/企業データ.*$/g, "")
          .replace(/企業情報の修正.*$/g, "")
          .replace(/のご協力はこちら.*$/g, "")
          .replace(/accou.*$/g, "")
          .trim();
        if (desc && desc.length > 10) {
          info.companyDescription = desc;
          info.companyOverview = desc;
          break;
        }
      }
    }
    if (!info.companyDescription) {
      const descriptionMatch = text.match(
        /(企業概要|会社概要|事業内容)[：:]\s*([^\n\r]{50,500})/i
      );
      if (descriptionMatch) {
        let desc = descriptionMatch[2].trim();
        // 不要な文字列を除去
        desc = desc
          .replace(/企業データ.*$/g, "")
          .replace(/企業情報の修正.*$/g, "")
          .replace(/のご協力はこちら.*$/g, "")
          .replace(/accou.*$/g, "")
          .trim();
        if (desc && desc.length > 10) {
          info.companyDescription = desc;
          info.companyOverview = desc;
        }
      }
    }

    // 代表者名（companydata.tsujigawa.comの企業データ欄から抽出）
    // 企業データ欄から抽出を試行
    if (companyDataSection.length > 0) {
      companyDataSection.find('td, th, li, dt, dd, .representative, .ceo, [data-representative]').each((_, el) => {
        const cellText = $(el).text();
        if ((cellText.includes("代表") || cellText.includes("社長") || cellText.includes("CEO")) && !info.representative) {
          const repPatterns = [
            /(?:代表取締役|代表者|社長|CEO)[：:\s]*([^\n\r]+)/i,
            /([^\n\r]*代表取締役[^\n\r]*)/i,
            /([^\n\r]*代表者[^\n\r]*)/i,
          ];
          for (const pattern of repPatterns) {
            const match = cellText.match(pattern);
            if (match) {
              let repName = match[1].trim();
              // 役職名を除去
              repName = repName.replace(/^(代表取締役|取締役|社長|CEO|代表)[\s　]*/, "").trim();
              // 名前だけを抽出（カタカナや漢字の名前パターン）
              const nameMatch = repName.match(/([一-龠々ー]+[\s　]+[一-龠々ー]+|[ァ-ヶー]+[\s　]+[ァ-ヶー]+|[A-Za-z]+[\s　]+[A-Za-z]+)/);
              if (nameMatch) {
                repName = nameMatch[1].trim();
              }
              if (repName && repName.length > 1 && repName.length < 50) {
                info.representative = repName;
                info.representativeName = repName;
                return false; // break
              }
            }
          }
        }
      });
    }
    
    // テーブルやリストからも抽出を試行（企業データ欄以外も）
    $('td, th, li, dt, dd, .representative, .ceo, [data-representative]').each((_, el) => {
      const cellText = $(el).text();
      if ((cellText.includes("代表") || cellText.includes("社長") || cellText.includes("CEO")) && !info.representative) {
        const repPatterns = [
          /(?:代表取締役|代表者|社長|CEO)[：:\s]*([^\n\r]+)/i,
          /([^\n\r]*代表取締役[^\n\r]*)/i,
          /([^\n\r]*代表者[^\n\r]*)/i,
        ];
        for (const pattern of repPatterns) {
          const match = cellText.match(pattern);
          if (match) {
            let repName = match[1].trim();
            // 役職名を除去
            repName = repName.replace(/^(代表取締役|取締役|社長|CEO|代表)[\s　]*/, "").trim();
            // 名前だけを抽出（カタカナや漢字の名前パターン）
            const nameMatch = repName.match(/([一-龠々ー]+[\s　]+[一-龠々ー]+|[ァ-ヶー]+[\s　]+[ァ-ヶー]+|[A-Za-z]+[\s　]+[A-Za-z]+)/);
            if (nameMatch) {
              repName = nameMatch[1].trim();
            }
            if (repName && repName.length > 1 && repName.length < 50) {
              info.representative = repName;
              info.representativeName = repName;
              return false; // break
            }
          }
        }
      }
    });
    
    // 取締役・代表者（テキストからも抽出）
    const directorPatterns = [
      /(代表取締役|取締役)[：:\s]*([^\n\r]+)/gi,
      /(社長|CEO|代表)[：:\s]*([^\n\r]+)/gi,
      /(代表者|代表取締役社長)[：:\s]*([^\n\r]+)/gi,
    ];
    info.directors = extractListItems(text, directorPatterns);
    
    // 代表者名を個別に抽出（directorsから）
    if (!info.representative && info.directors && info.directors.length > 0) {
      const firstDirector = info.directors[0];
      const nameMatch = firstDirector.match(/(?:代表取締役|取締役|社長|CEO|代表)[\s　]*([^\s　]+[\s　]+[^\s　]+)/);
      if (nameMatch) {
        info.representative = nameMatch[1].trim();
        info.representativeName = nameMatch[1].trim();
      } else {
        const cleanName = firstDirector.replace(/^(代表取締役|取締役|社長|CEO|代表)[\s　]*/, "").trim();
        if (cleanName && cleanName.length > 1) {
          info.representative = cleanName;
          info.representativeName = cleanName;
        }
      }
    }
    
    // テキストからも抽出を試行
    if (!info.representative) {
      const repPatterns = [
        /代表者[：:\s]*([^\n\r]+)/i,
        /代表取締役[：:\s]*([^\n\r]+)/i,
        /社長[：:\s]*([^\n\r]+)/i,
      ];
      for (const pattern of repPatterns) {
        const match = text.match(pattern);
        if (match) {
          const repName = match[1].trim();
          const cleanName = repName.replace(/^(代表取締役|取締役|社長|CEO|代表)[\s　]*/, "").trim();
          if (cleanName && cleanName.length > 1 && cleanName.length < 50) {
            info.representative = cleanName;
            info.representativeName = cleanName;
            break;
          }
        }
      }
    }

    // 従業員数（companydata.tsujigawa.comの構造に合わせて抽出）
    // テーブルやリストから抽出を試行
    $('td, th, li, dt, dd, .employee, [data-employee]').each((_, el) => {
      const cellText = $(el).text();
      if ((cellText.includes("従業員") || cellText.includes("社員") || cellText.includes("職員")) && !info.employeeCount) {
        const employeePatterns = [
          /(?:従業員数|社員数|従業員|職員数)[：:\s]*([\d,]+)\s*(人|名|名様)/i,
          /([\d,]+)\s*(?:人|名)[^\d]*従業員/i,
        ];
        for (const pattern of employeePatterns) {
          const match = cellText.match(pattern);
          if (match) {
            const value = parseFloat(match[1].replace(/,/g, ""));
            if (!isNaN(value) && value > 0) {
              info.employeeCount = value;
              return false; // break
            }
          }
        }
      }
    });
    
    // テキストからも抽出を試行
    if (!info.employeeCount) {
      const employeePattern = /(?:従業員数|社員数|従業員|職員数)[：:\s]*([\d,]+)\s*(人|名|名様)/i;
      const employeeMatch = extractNumber(text, employeePattern);
      if (employeeMatch) {
        info.employeeCount = employeeMatch;
      }
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

    // FAX
    const faxMatch = text.match(/(FAX|Fax|fax)[：:]\s*([0-9-()]+)/i);
    if (faxMatch) {
      info.fax = faxMatch[2].replace(/[^\d-]/g, "");
    }

    // 決算月（companydata.tsujigawa.comの構造に合わせて抽出）
    // テーブルやリストから抽出を試行
    $('td, th, li, dt, dd, .fiscal, [data-fiscal]').each((_, el) => {
      const cellText = $(el).text();
      if ((cellText.includes("決算") || cellText.includes("決算期")) && !info.fiscalMonth) {
        const fiscalPatterns = [
          /(?:決算期|決算月)[：:\s]*(\d{1,2})月/i,
          /(\d{1,2})月決算/i,
        ];
        for (const pattern of fiscalPatterns) {
          const match = cellText.match(pattern);
          if (match) {
            const month = match[1];
            if (month && parseInt(month) >= 1 && parseInt(month) <= 12) {
              info.fiscalMonth = `${month}月`;
              info.settlementMonth = `${month}月`;
              return false; // break
            }
          }
        }
      }
    });
    
    // テキストからも抽出を試行
    if (!info.fiscalMonth) {
      const fiscalPatterns = [
        /(?:決算期|決算月)[：:\s]*(\d{1,2})月/i,
        /(\d{1,2})月決算/i,
      ];
      for (const pattern of fiscalPatterns) {
        const match = text.match(pattern);
        if (match) {
          const month = match[1] || (text.includes("3月決算") ? "3" : text.includes("6月決算") ? "6" : text.includes("9月決算") ? "9" : text.includes("12月決算") ? "12" : null);
          if (month && parseInt(month) >= 1 && parseInt(month) <= 12) {
            info.fiscalMonth = `${month}月`;
            info.settlementMonth = `${month}月`;
            break;
          }
        }
      }
    }

    // 設立年月日（companydata.tsujigawa.comの構造に合わせて抽出）
    // テーブルやリストから抽出を試行
    $('td, th, li, dt, dd, .established, .founding, [data-established]').each((_, el) => {
      const cellText = $(el).text();
      if ((cellText.includes("設立") || cellText.includes("創業") || cellText.includes("創立")) && !info.establishedDate) {
        const establishedPatterns = [
          /(?:設立|創業|創立)[：:\s]*(\d{4})年(\d{1,2})月(\d{1,2})日/i,
          /(?:設立|創業|創立)[：:\s]*(\d{4})\/(\d{1,2})\/(\d{1,2})/i,
          /(?:設立|創業|創立)[：:\s]*(\d{4})年(\d{1,2})月/i,
          /(?:設立|創業|創立)[：:\s]*(\d{4})年/i,
        ];
        for (const pattern of establishedPatterns) {
          const match = cellText.match(pattern);
          if (match) {
            const year = match[1];
            const month = match[2]?.padStart(2, "0") || "01";
            const day = match[3]?.padStart(2, "0") || "01";
            if (parseInt(year) >= 1800 && parseInt(year) <= 2100) {
              const dateStr = `${year}-${month}-${day}`;
              info.establishedDate = dateStr;
              info.establishmentDate = dateStr;
              return false; // break
            }
          }
        }
      }
    });
    
    // テキストからも抽出を試行
    if (!info.establishedDate) {
      const establishedPatterns = [
        /(?:設立|創業|創立)[：:\s]*(\d{4})年(\d{1,2})月(\d{1,2})日/i,
        /(?:設立|創業|創立)[：:\s]*(\d{4})\/(\d{1,2})\/(\d{1,2})/i,
        /(?:設立|創業|創立)[：:\s]*(\d{4})年(\d{1,2})月/i,
        /(?:設立|創業|創立)[：:\s]*(\d{4})年/i,
      ];
      for (const pattern of establishedPatterns) {
        const match = text.match(pattern);
        if (match) {
          const year = match[1];
          const month = match[2]?.padStart(2, "0") || "01";
          const day = match[3]?.padStart(2, "0") || "01";
          if (parseInt(year) >= 1800 && parseInt(year) <= 2100) {
            const dateStr = `${year}-${month}-${day}`;
            info.establishedDate = dateStr;
            info.establishmentDate = dateStr;
            break;
          }
        }
      }
    }

    // 法人番号
    const corporateNumberMatch = text.match(/法人番号[：:]\s*(\d{13})/i);
    if (corporateNumberMatch) {
      info.corporateNumber = corporateNumberMatch[1];
    }

    // 郵便番号
    const postalMatch = text.match(/(〒|郵便番号)[：:\s]*(\d{3}-?\d{4})/i);
    if (postalMatch) {
      info.postalCode = postalMatch[2].replace(/-/g, "");
    }

    // 住所（不要な文字列を除去し、企業概要を分離）
    const addressPatterns = [
      /(〒\d{3}-?\d{4})[\s　]*([^\n\r]{10,200})/i,
      /(本社|本社所在地|所在地)[：:\s]*([^\n\r]{10,200})/i,
      /(住所|所在地)[：:\s]*([^\n\r]{10,200})/i,
    ];
    for (const pattern of addressPatterns) {
      const match = text.match(pattern);
      if (match) {
        let address = match[2] || match[1];
        if (address && address.length > 5 && address.length < 300) {
          // 不要な文字列を除去
          address = address
            .replace(/地図情報\s*\(Google Mapsで開く\)/g, "")
            .replace(/地図情報/g, "")
            .replace(/\(Google Mapsで開く\)/g, "")
            .replace(/Google Maps/g, "")
            .trim();
          
          // 企業概要部分を分離（住所の後に続く説明文を抽出）
          // パターン1: 住所の後に（で始まる企業概要
          const addressEndMatch = address.match(/^([^（(]+[都道府県市区町村][^（(]*?)([（(].*)/);
          if (addressEndMatch) {
            info.headquartersAddress = addressEndMatch[1].trim();
            // 残りの部分を企業概要として保存（不要な文字列を除去）
            let overviewText = addressEndMatch[2];
            // 不要な文字列を除去
            overviewText = overviewText
              .replace(/企業データ.*$/g, "")
              .replace(/企業情報の修正.*$/g, "")
              .replace(/のご協力はこちら.*$/g, "")
              .replace(/accou.*$/g, "")
              .trim();
            if (overviewText && !info.companyDescription && overviewText.length > 10) {
              info.companyDescription = overviewText;
              info.companyOverview = overviewText;
            }
          } else {
            // パターン2: 住所と企業概要が別の行やセクションにある場合
            // 住所部分だけを抽出（都道府県から始まり、番地や建物名まで）
            const addressOnlyMatch = address.match(/^([^（(]+[都道府県市区町村][^（(]*?[0-9０-９]+[^（(]*?)([（(].*)?/);
            if (addressOnlyMatch) {
              info.headquartersAddress = addressOnlyMatch[1].trim();
              if (addressOnlyMatch[2] && !info.companyDescription) {
                let overviewText = addressOnlyMatch[2];
                // 不要な文字列を除去
                overviewText = overviewText
                  .replace(/企業データ.*$/g, "")
                  .replace(/企業情報の修正.*$/g, "")
                  .replace(/のご協力はこちら.*$/g, "")
                  .replace(/accou.*$/g, "")
                  .trim();
                if (overviewText && overviewText.length > 10) {
                  info.companyDescription = overviewText;
                  info.companyOverview = overviewText;
                }
              }
            } else {
              info.headquartersAddress = address.trim();
            }
          }
          break;
        }
      }
    }

    // 都道府県
    const prefectureMatch = text.match(/(東京都|北海道|(?:大阪|京都|神奈川|埼玉|千葉|兵庫|福岡|愛知|静岡|宮城|新潟|長野|広島|福島|群馬|栃木|茨城|岐阜|山梨|愛媛|熊本|大分|宮崎|鹿児島|沖縄|青森|岩手|秋田|山形|石川|富山|福井|滋賀|三重|和歌山|鳥取|島根|岡山|山口|徳島|香川|高知|佐賀|長崎|奈良)[県府])/);
    if (prefectureMatch) {
      info.prefecture = prefectureMatch[1];
    }

    // SNS
    const snsPatterns = [
      /https?:\/\/(www\.)?(twitter\.com|x\.com)\/[A-Za-z0-9_]+/gi,
      /https?:\/\/(www\.)?facebook\.com\/[^"'\s]+/gi,
      /https?:\/\/(www\.)?instagram\.com\/[^"'\s]+/gi,
      /https?:\/\/(www\.)?linkedin\.com\/company\/[^"'\s]+/gi,
    ];
    for (const re of snsPatterns) {
      const m = bodyHtml.match(re);
      if (m) {
        info.sns.push(...m);
      }
    }
    info.sns = Array.from(new Set(info.sns));

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
    const equityRatioPattern = /(自己資本比率|Equity Ratio)[：:]\s*([\d.]+)\s*%/i;
    const equityRatioMatch = extractNumber(text, equityRatioPattern);
    if (equityRatioMatch) {
      info.equityRatio = equityRatioMatch;
    }
  } catch (error) {
    console.error(`[extractCompanyInfoFromFumadata] エラー:`, error);
  }

  return info;
}

/**
 * 企業リストページから企業名と詳細ページURLを抽出
 */
async function extractCompanyListFromPage(
  page: Page
): Promise<Array<{ name: string; detailUrl: string }>> {
  const companies: Array<{ name: string; detailUrl: string }> = [];

  try {
    // ページが完全に読み込まれるまで待機
    await page.waitForLoadState("networkidle", { timeout: TIMEOUT_MS });
    await sleep(2000); // 追加の待機時間
    
    const html = await page.content();
    const $ = cheerio.load(html);

    // デバッグ: ページタイトルを確認
    const title = $("title").text();
    console.log(`[デバッグ] ページタイトル: ${title}`);

    // デバッグ: すべてのリンクを確認
    const totalLinks = $("a[href]").length;
    console.log(`[デバッグ] ページ内のリンク数: ${totalLinks}`);

    // companydata.tsujigawa.com の企業リストから企業情報を抽出
    // 企業名とアイコン（詳細ページへのリンク）を探す
    
    // まず、すべてのリンクから企業詳細ページのURLを抽出
    const companyPageLinks = $('a[href]');
    const companyLinks = new Map<string, { name: string; url: string }>();
    
    companyPageLinks.each((_, el) => {
      const $link = $(el);
      const href = $link.attr('href');
      const linkText = $link.text().trim();
      
      if (href) {
        try {
          const fullUrl = href.startsWith("http") ? href : new URL(href, BASE_URL).href;
          
          // 企業詳細ページのURLパターンをチェック
          if (fullUrl.includes(BASE_URL) && (fullUrl.includes("/company/") || fullUrl.match(/\/company\/[^\/]+\//))) {
            // 企業名を抽出（リンクテキストから、または親要素から）
            let companyName = linkText;
            
            // 企業名から不要な説明文を除去
            if (companyName) {
              // 「は、」や「（法人番号」などの説明文を除去
              companyName = companyName.split(/[は、]|（法人番号|（旧名称|…続きを読む/)[0].trim();
              // 括弧内の情報を除去（読み方など）
              companyName = companyName.replace(/（[^）]+）/, "").trim();
            }
            
            if (!companyName || companyName.length < 3) {
              // 親要素から企業名を探す
              const parent = $link.parent();
              const nameText = parent.find('h1, h2, h3, h4, .company-name, .name').first().text().trim();
              if (nameText) {
                companyName = nameText.split(/[は、]|（法人番号|（旧名称|…続きを読む/)[0].trim();
                companyName = companyName.replace(/（[^）]+）/, "").trim();
              } else {
                // テキストから企業名を抽出
                const text = parent.text();
                const nameMatch = text.match(/(株式会社[^\s\n\r（(]+|有限会社[^\s\n\r（(]+|合同会社[^\s\n\r（(]+|合資会社[^\s\n\r（(]+|合名会社[^\s\n\r（(]+|NPO法人[^\s\n\r（(]+|社団法人[^\s\n\r（(]+|財団法人[^\s\n\r（(]+|医療法人[^\s\n\r（(]+|学校法人[^\s\n\r（(]+|独立行政法人[^\s\n\r（(]+|社会福祉法人[^\s\n\r（(]+)/);
                if (nameMatch) {
                  companyName = nameMatch[1].trim();
                }
              }
            }
            
            // 企業名の長さを制限（説明文が含まれている場合は切り詰める）
            if (companyName && companyName.length > 100) {
              // 長すぎる場合は最初の100文字まで
              companyName = companyName.substring(0, 100).trim();
            }
            
            if (companyName && companyName.length > 3 && companyName.length < 200) {
              // 重複を避けるためにURLをキーとして使用
              if (!companyLinks.has(fullUrl)) {
                companyLinks.set(fullUrl, { name: companyName, url: fullUrl });
              }
            }
          }
        } catch {}
      }
    });
    
    // Mapから配列に変換
    companyLinks.forEach((value) => {
      companies.push({
        name: value.name,
        detailUrl: value.url,
      });
    });
    
    // 企業アイテムからも抽出を試行
    if (companies.length === 0) {
      const companyItems = $('article, .company-item, .result-item, .list-item, [class*="company"], [class*="item"], .entry, .post, .entry-content, .post-content');
      
      if (companyItems.length > 0) {
        console.log(`[デバッグ] 企業アイテム数: ${companyItems.length}`);
        
        companyItems.each((_, item) => {
          const $item = $(item);
          
          // 企業名を抽出（複数のパターンを試行）
          let companyName = $item.find('h1, h2, h3, h4, .company-name, .name, [class*="name"], .entry-title, .post-title, .entry-header h2, .post-header h2').first().text().trim();
          if (!companyName) {
            // テキストから企業名を抽出（「株式会社」で始まるなど）
            const text = $item.text();
            const nameMatch = text.match(/(株式会社[^\s\n\r（(]+|有限会社[^\s\n\r（(]+|合同会社[^\s\n\r（(]+|合資会社[^\s\n\r（(]+|合名会社[^\s\n\r（(]+|NPO法人[^\s\n\r（(]+|社団法人[^\s\n\r（(]+|財団法人[^\s\n\r（(]+|医療法人[^\s\n\r（(]+|学校法人[^\s\n\r（(]+|独立行政法人[^\s\n\r（(]+|社会福祉法人[^\s\n\r（(]+)/);
            if (nameMatch) {
              companyName = nameMatch[1].trim();
            }
          }
          
          // 詳細ページへのリンクを探す（アイコンやリンク）
          let detailUrl = null;
          
          // パターン1: 「続きを読む」などのリンク
          const readMoreLink = $item.find('a:contains("続きを読む"), a:contains("詳細"), a:contains("more"), .read-more a, .more-link').first();
          if (readMoreLink.length > 0) {
            const href = readMoreLink.attr('href');
            if (href) {
              try {
                detailUrl = href.startsWith("http") ? href : new URL(href, BASE_URL).href;
              } catch {}
            }
          }
          
          // パターン2: アイコンやボタンからリンクを取得
          if (!detailUrl) {
            const iconLink = $item.find('a[href*="/company/"], a[href*="/detail/"], a[href*="/corp/"], button[onclick*="/company/"], [class*="icon"] a, [class*="link"] a, .entry-link, .post-link').first();
            if (iconLink.length > 0) {
              const href = iconLink.attr('href');
              if (href) {
                try {
                  detailUrl = href.startsWith("http") ? href : new URL(href, BASE_URL).href;
                } catch {}
              }
            }
          }
          
          // パターン3: 企業名を含むリンクから取得
          if (!detailUrl && companyName) {
            const nameLink = $item.find(`a:contains("${companyName.substring(0, 10)}")`).first();
            if (nameLink.length > 0) {
              const href = nameLink.attr('href');
              if (href) {
                try {
                  detailUrl = href.startsWith("http") ? href : new URL(href, BASE_URL).href;
                } catch {}
              }
            }
          }
          
          // パターン4: 企業アイテム内の最初のリンク
          if (!detailUrl) {
            const firstLink = $item.find('a[href]').first();
            if (firstLink.length > 0) {
              const href = firstLink.attr('href');
              if (href && (href.includes("/company/") || href.includes("/detail/") || href.includes("/corp/") || href.match(/\/\d+\//))) {
                try {
                  detailUrl = href.startsWith("http") ? href : new URL(href, BASE_URL).href;
                } catch {}
              }
            }
          }
          
          // パターン5: 企業アイテム全体がリンクになっている場合
          if (!detailUrl) {
            const itemLink = $item.closest('a[href]');
            if (itemLink.length > 0) {
              const href = itemLink.attr('href');
              if (href) {
                try {
                  detailUrl = href.startsWith("http") ? href : new URL(href, BASE_URL).href;
                } catch {}
              }
            }
          }
          
          if (companyName && companyName.length > 0 && companyName.length < 200 && detailUrl) {
            companies.push({
              name: companyName,
              detailUrl: detailUrl,
            });
          }
        });
      }
    }
    
    // フォールバック: 一般的なリンクから企業を抽出（まだ企業が見つからない場合）
    if (companies.length === 0) {
      const listSelectors = [
        'a[href*="/company/"]',
        'a[href*="/detail/"]',
        '.company-list a',
        '.company-item a',
        '.search-result a',
        '.result-item a',
        '.list-item a',
      ];

      for (const selector of listSelectors) {
        const elements = $(selector);
        if (elements.length > 0) {
          console.log(`[デバッグ] セレクタ "${selector}" で ${elements.length} 件の要素を発見`);
        }
        
        elements.each((_, el) => {
          const href = $(el).attr("href");
          const name = $(el).text().trim();
          
          if (href && name && name.length > 0 && name.length < 100) {
            try {
              const fullUrl = href.startsWith("http") 
                ? href 
                : new URL(href, BASE_URL).href;
              
              // 詳細ページっぽいURLのみ
              if (
                fullUrl.includes("/company/") || 
                fullUrl.includes("/detail/") ||
                fullUrl.match(/\/\d+\//)
              ) {
                // 検索結果ページやページネーションリンクを除外
                if (!fullUrl.includes("?page=") && !fullUrl.includes("&page=")) {
                  companies.push({
                    name: name,
                    detailUrl: fullUrl,
                  });
                }
              }
            } catch {}
          }
        });
      }
    }

    // 重複除去
    const uniqueCompanies = Array.from(
      new Map(companies.map(c => [c.detailUrl, c])).values()
    );

    console.log(`[デバッグ] 抽出された企業数: ${uniqueCompanies.length}`);
    if (uniqueCompanies.length > 0) {
      console.log(`[デバッグ] 最初の3件の企業名: ${uniqueCompanies.slice(0, 3).map(c => c.name).join(", ")}`);
    }

    return uniqueCompanies;
  } catch (error) {
    console.error(`[extractCompanyListFromPage] エラー:`, error);
    return [];
  }
}

/**
 * 企業名から詳細ページURLを検索（リストページから見つからない場合）
 */
async function searchCompanyDetailUrl(
  page: Page,
  companyName: string
): Promise<string | null> {
  try {
    // companydata.tsujigawa.comの検索機能を使用
    const searchUrl = `${BASE_URL}/company/?q=${encodeURIComponent(companyName)}`;
    await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: TIMEOUT_MS });
    await sleep(REQUEST_DELAY_MS);

    const companies = await extractCompanyListFromPage(page);
    if (companies.length > 0) {
      // 企業名が最も一致するものを選択
      const matched = companies.find(c => 
        c.name.includes(companyName) || companyName.includes(c.name)
      );
      return matched ? matched.detailUrl : companies[0].detailUrl;
    }
  } catch (error) {
    console.error(`[searchCompanyDetailUrl] エラー:`, error);
  }
  return null;
}

// ------------------------------
// Firestore との比較・判定
// ------------------------------

/**
 * companies_new コレクションに企業が存在するかチェック
 */
async function checkCompanyExists(companyName: string, corporateNumber?: string): Promise<{ exists: boolean; docId: string | null; data: any }> {
  if (!firebaseEnabled || !db) {
    // Firebaseが無効な場合は新規企業として扱う
    return { exists: false, docId: null, data: null };
  }
  
  try {
    // 企業名で検索
    let query = db.collection("companies_new")
      .where("name", "==", companyName)
      .limit(1);
    
    let snapshot = await query.get();
    
    if (!snapshot.empty) {
      const doc = snapshot.docs[0];
      return {
        exists: true,
        docId: doc.id,
        data: doc.data(),
      };
    }

    // 法人番号で検索（ある場合）
    if (corporateNumber) {
      query = db.collection("companies_new")
        .where("corporateNumber", "==", corporateNumber)
        .limit(1);
      
      snapshot = await query.get();
      
      if (!snapshot.empty) {
        const doc = snapshot.docs[0];
        return {
          exists: true,
          docId: doc.id,
          data: doc.data(),
        };
      }
    }

    return { exists: false, docId: null, data: null };
  } catch (error) {
    console.error(`[checkCompanyExists] エラー:`, error);
    return { exists: false, docId: null, data: null };
  }
}

/**
 * 既存データと比較して、不足しているフィールドを判定
 */
function checkMissingFields(
  existingData: any,
  scrapedData: Partial<any>
): string[] {
  const missingFields: string[] = [];
  
  // 重要なフィールドのリスト（companies_new の主要フィールド）
  const importantFields = [
    "capitalStock", "capital",
    "revenue", "latestRevenue",
    "profit", "latestProfit",
    "totalAssets", "netAssets", "totalLiabilities",
    "industry",
    "employeeCount",
    "representativeName", "representative",
    "companyUrl", "website",
    "phoneNumber", "contactPhone",
    "email", "contactEmail",
    "headquartersAddress",
    "postalCode",
    "fiscalMonth", "settlementMonth",
  ];

  for (const field of importantFields) {
    const existingValue = existingData[field];
    const scrapedValue = scrapedData[field] || scrapedData[getMappedFieldName(field)];
    
    // 既存データが空またはnullで、スクレイピングデータがある場合
    if (
      (existingValue === null || existingValue === undefined || existingValue === "" || 
       (Array.isArray(existingValue) && existingValue.length === 0)) &&
      scrapedValue !== null && scrapedValue !== undefined && scrapedValue !== "" &&
      !(Array.isArray(scrapedValue) && scrapedValue.length === 0)
    ) {
      missingFields.push(field);
    }
  }

  return missingFields;
}

/**
 * フィールド名のマッピング（companies_new と webInfo の違いを吸収）
 */
function getMappedFieldName(field: string): string {
  const mapping: { [key: string]: string } = {
    "capitalStock": "capital",
    "companyUrl": "website",
    "phoneNumber": "contactPhone",
    "email": "contactEmail",
    "representativeName": "representative",
  };
  return mapping[field] || field;
}

/**
 * スクレイピングデータを既存データ形式に変換
 */
function convertToCompanyNewFormat(scrapedData: Partial<any>): any {
  // 役員情報を配列から個別フィールドに展開
  const officers = scrapedData.officers || scrapedData.directors || [];
  const officersArray = Array.isArray(officers) ? officers : [];
  
  return {
    // 基本情報
    name: scrapedData.companyName || "",
    corporateNumber: scrapedData.corporateNumber || "",
    // 財務情報
    capitalStock: scrapedData.capital || null,
    revenue: scrapedData.revenue || scrapedData.latestRevenue || null,
    latestRevenue: scrapedData.latestRevenue || scrapedData.revenue || null,
    latestProfit: scrapedData.latestProfit || scrapedData.profit || null,
    operatingIncome: scrapedData.operatingIncome || null,
    totalAssets: scrapedData.totalAssets || null,
    totalLiabilities: scrapedData.totalLiabilities || null,
    netAssets: scrapedData.netAssets || null,
    // 年度別売上高
    revenue1: scrapedData.revenue1 || null,
    revenue2: scrapedData.revenue2 || null,
    revenue3: scrapedData.revenue3 || null,
    revenue4: scrapedData.revenue4 || null,
    revenue5: scrapedData.revenue5 || null,
    // 年度別純利益
    profit1: scrapedData.profit1 || null,
    profit2: scrapedData.profit2 || null,
    profit3: scrapedData.profit3 || null,
    profit4: scrapedData.profit4 || null,
    profit5: scrapedData.profit5 || null,
    // 業種情報
    industry: scrapedData.industry || "",
    // 企業規模
    employeeCount: scrapedData.employeeCount || null,
    // 代表者情報
    representativeName: scrapedData.representative || scrapedData.representativeName || "",
    executives: officersArray,
    // 役員情報（配列から個別フィールドに展開）
    executiveName1: officersArray[0] || "",
    executiveName2: officersArray[1] || "",
    executiveName3: officersArray[2] || "",
    executiveName4: officersArray[3] || "",
    executiveName5: officersArray[4] || "",
    executiveName6: officersArray[5] || "",
    executiveName7: officersArray[6] || "",
    executiveName8: officersArray[7] || "",
    executiveName9: officersArray[8] || "",
    executiveName10: officersArray[9] || "",
    // 連絡先情報
    companyUrl: scrapedData.website || scrapedData.companyUrl || "",
    phoneNumber: scrapedData.contactPhone || scrapedData.phoneNumber || "",
    email: scrapedData.contactEmail || scrapedData.email || "",
    contactFormUrl: scrapedData.contactFormUrl || "",
    fax: scrapedData.fax || "",
    // 所在地情報
    headquartersAddress: scrapedData.headquartersAddress || "",
    postalCode: scrapedData.postalCode || "",
    prefecture: scrapedData.prefecture || "",
    // 財務情報（続き）
    fiscalMonth: scrapedData.fiscalMonth || scrapedData.settlementMonth || "",
    listing: scrapedData.listingStatus || "",
    securitiesCode: scrapedData.securitiesCode || "",
    // 企業説明
    companyDescription: scrapedData.companyDescription || scrapedData.companyOverview || "",
    overview: scrapedData.companyOverview || scrapedData.companyDescription || "",
    // 企業規模（続き）
    officeCount: scrapedData.officeCount || null,
    factoryCount: scrapedData.factoryCount || null,
    storeCount: scrapedData.storeCount || null,
    // 設立情報
    dateOfEstablishment: scrapedData.establishedDate || scrapedData.establishmentDate || "",
    established: scrapedData.establishedDate || scrapedData.establishmentDate || "",
    // 取引先・関係会社
    banks: Array.isArray(scrapedData.banks) ? scrapedData.banks.join("; ") : (scrapedData.banks || ""),
    shareholders: Array.isArray(scrapedData.shareholders) ? scrapedData.shareholders.join("; ") : (scrapedData.shareholders || ""),
    // SNS・外部リンク
    urls: Array.isArray(scrapedData.sourceUrls) ? scrapedData.sourceUrls.join("; ") : (scrapedData.sourceUrls || []).join("; "),
    // その他
    licenses: Array.isArray(scrapedData.licenses) ? scrapedData.licenses.join("; ") : (scrapedData.licenses || ""),
    sourceUrls: Array.isArray(scrapedData.sourceUrls) ? scrapedData.sourceUrls.join("; ") : (scrapedData.sourceUrls || []).join("; "),
    updatedAt: new Date().toISOString(),
  };
}

// ------------------------------
// メイン処理
// ------------------------------

/**
 * 企業リストページから企業情報を取得
 */
async function scrapeCompaniesFromListPage(
  listPageUrl: string
): Promise<void> {
  let browser: Browser | null = null;
  const results: Array<{
    companyName: string;
    corporateNumber: string;
    data: any;
    isNew: boolean;
    missingFields: string[];
    source: string;
  }> = [];

  try {
    console.log(`[開始] 企業リストページからスクレイピング: ${listPageUrl}`);
    
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    // リストページにアクセス
    await page.goto(listPageUrl, { waitUntil: "domcontentloaded", timeout: TIMEOUT_MS });
    await sleep(REQUEST_DELAY_MS);

    // 企業リストを抽出
    const companies = await extractCompanyListFromPage(page);
    console.log(`[情報] 企業リストから ${companies.length} 件の企業を検出`);

    if (companies.length === 0) {
      console.warn("[警告] 企業リストが見つかりませんでした。ページ構造を確認してください。");
      return;
    }

    // 各企業の詳細ページから情報を取得（最大5社）
    const maxCompanies = Math.min(5, companies.length);
    for (let i = 0; i < maxCompanies; i++) {
      const company = companies[i];
      console.log(`[処理中] ${i + 1}/${maxCompanies}: ${company.name}`);

      try {
        // リストページに戻る（最初の企業以外）
        if (i > 0) {
          await page.goto(listPageUrl, { waitUntil: "domcontentloaded", timeout: TIMEOUT_MS });
          await sleep(REQUEST_DELAY_MS);
        }
        
        // アイコンをクリックして詳細ページに遷移
        try {
          // 企業名を含む要素を探して、その近くのアイコンやリンクをクリック
          const companyElement = await page.locator(`text=${company.name}`).first();
          if (await companyElement.count() > 0) {
            // 企業名を含む要素の親要素からアイコンやリンクを探す
            const parent = companyElement.locator('..');
            const iconLink = parent.locator('a[href*="/company/"], a[href*="/detail/"], button, [class*="icon"], [class*="link"]').first();
            
            if (await iconLink.count() > 0) {
              await iconLink.click();
              await page.waitForLoadState("networkidle", { timeout: TIMEOUT_MS });
              await sleep(REQUEST_DELAY_MS);
            } else {
              // アイコンが見つからない場合は直接URLにアクセス
              await page.goto(company.detailUrl, { waitUntil: "domcontentloaded", timeout: TIMEOUT_MS });
              await sleep(REQUEST_DELAY_MS);
            }
          } else {
            // 企業名が見つからない場合は直接URLにアクセス
            await page.goto(company.detailUrl, { waitUntil: "domcontentloaded", timeout: TIMEOUT_MS });
            await sleep(REQUEST_DELAY_MS);
          }
        } catch (clickError) {
          // クリックに失敗した場合は直接URLにアクセス
          console.log(`  → アイコンクリックに失敗、直接URLにアクセス: ${company.name}`);
          await page.goto(company.detailUrl, { waitUntil: "domcontentloaded", timeout: TIMEOUT_MS });
          await sleep(REQUEST_DELAY_MS);
        }

        // 情報を抽出
        const scrapedData = await extractCompanyInfoFromFumadata(page, company.name);
        
        if (!scrapedData.companyName) {
          scrapedData.companyName = company.name;
        }

        // Firestore で既存データをチェック
        const { exists, docId, data: existingData } = await checkCompanyExists(
          scrapedData.companyName || company.name,
          scrapedData.corporateNumber || undefined
        );

        if (!exists) {
          // 新規企業
          console.log(`  → 新規企業として登録: ${company.name}`);
          const convertedData = convertToCompanyNewFormat(scrapedData);
          results.push({
            companyName: scrapedData.companyName || company.name,
            corporateNumber: scrapedData.corporateNumber || "",
            data: convertedData,
            isNew: true,
            missingFields: [],
            source: "companydata.tsujigawa.com",
          });
        } else {
          // 既存企業 - 不足フィールドをチェック
          const missingFields = checkMissingFields(existingData, scrapedData);
          
          if (missingFields.length > 0) {
            console.log(`  → 既存企業（不足フィールドあり）: ${company.name} (${missingFields.length}件)`);
            const convertedData = convertToCompanyNewFormat(scrapedData);
            results.push({
              companyName: scrapedData.companyName || company.name,
              corporateNumber: scrapedData.corporateNumber || "",
              data: convertedData,
              isNew: false,
              missingFields: missingFields,
              source: "companydata.tsujigawa.com",
            });
          } else {
            console.log(`  → 既存企業（情報充足）: ${company.name} - スキップ`);
          }
        }

        // リクエスト間隔を確保
        await sleep(REQUEST_DELAY_MS);
      } catch (error) {
        console.error(`  → エラー: ${company.name}`, error);
      }
    }

    // CSVに出力
    await exportToCsv(results);

    console.log(`[完了] 処理完了: ${results.length} 件の企業情報を取得`);
  } catch (error) {
    console.error("[エラー] スクレイピング処理エラー:", error);
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

/**
 * 企業名リストから企業情報を取得
 */
async function scrapeCompaniesFromNameList(
  companyNames: string[]
): Promise<void> {
  let browser: Browser | null = null;
  const results: Array<{
    companyName: string;
    corporateNumber: string;
    data: any;
    isNew: boolean;
    missingFields: string[];
    source: string;
  }> = [];

  try {
    console.log(`[開始] 企業名リストからスクレイピング: ${companyNames.length} 件`);
    
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    for (let i = 0; i < companyNames.length; i++) {
      const companyName = companyNames[i];
      console.log(`[処理中] ${i + 1}/${companyNames.length}: ${companyName}`);

      try {
        // 企業詳細ページURLを検索
        const detailUrl = await searchCompanyDetailUrl(page, companyName);
        
        if (!detailUrl) {
          console.log(`  → 詳細ページが見つかりません: ${companyName}`);
          continue;
        }

        // 詳細ページにアクセス
        await page.goto(detailUrl, { waitUntil: "domcontentloaded", timeout: TIMEOUT_MS });
        await sleep(REQUEST_DELAY_MS);

        // 情報を抽出
        const scrapedData = await extractCompanyInfoFromFumadata(page, companyName);
        scrapedData.companyName = companyName;

        // Firestore で既存データをチェック
        const { exists, docId, data: existingData } = await checkCompanyExists(
          companyName,
          scrapedData.corporateNumber || undefined
        );

        if (!exists) {
          // 新規企業
          console.log(`  → 新規企業として登録: ${companyName}`);
          const convertedData = convertToCompanyNewFormat(scrapedData);
          results.push({
            companyName: companyName,
            corporateNumber: scrapedData.corporateNumber || "",
            data: convertedData,
            isNew: true,
            missingFields: [],
            source: "companydata.tsujigawa.com",
          });
        } else {
          // 既存企業 - 不足フィールドをチェック
          const missingFields = checkMissingFields(existingData, scrapedData);
          
          if (missingFields.length > 0) {
            console.log(`  → 既存企業（不足フィールドあり）: ${companyName} (${missingFields.length}件)`);
            const convertedData = convertToCompanyNewFormat(scrapedData);
            results.push({
              companyName: companyName,
              corporateNumber: scrapedData.corporateNumber || "",
              data: convertedData,
              isNew: false,
              missingFields: missingFields,
              source: "companydata.tsujigawa.com",
            });
          } else {
            console.log(`  → 既存企業（情報充足）: ${companyName} - スキップ`);
          }
        }

        // リクエスト間隔を確保
        await sleep(REQUEST_DELAY_MS);
      } catch (error) {
        console.error(`  → エラー: ${companyName}`, error);
      }
    }

    // CSVに出力
    await exportToCsv(results);

    console.log(`[完了] 処理完了: ${results.length} 件の企業情報を取得`);
  } catch (error) {
    console.error("[エラー] スクレイピング処理エラー:", error);
  } finally {
    if (browser) {
      await browser.close();
    }
  }
}

/**
 * CSVにエクスポート
 */
async function exportToCsv(
  results: Array<{
    companyName: string;
    corporateNumber: string;
    data: any;
    isNew: boolean;
    missingFields: string[];
    source: string;
  }>
): Promise<void> {
  try {
    const outputPath = path.join(process.cwd(), "companies_fumadata.csv");
    const writeStream = fs.createWriteStream(outputPath, { encoding: "utf8" });

    // ヘッダー（companies_new コレクションの全フィールドに合わせる）
    const headers = [
      // メタ情報（fumadata用）
      "source",
      "isNew",
      "missingFields",
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
      // 追加メタ情報
      "sourceUrls",
    ];

    writeStream.write(headers.map(escapeCsvValue).join(",") + "\n");

    // データ行
    for (const result of results) {
      const data = result.data;
      
      // ヘルパー関数：フィールド値を取得
      const getField = (fieldName: string): any => {
        return data[fieldName] !== undefined ? data[fieldName] : "";
      };
      
      // 配列フィールドを文字列に変換
      const formatArray = (arr: any): string => {
        if (Array.isArray(arr)) {
          return arr.length > 0 ? arr.join("; ") : "";
        }
        return arr || "";
      };
      
      const row: string[] = [
        // メタ情報
        escapeCsvValue(result.source),
        escapeCsvValue(result.isNew ? "true" : "false"),
        escapeCsvValue(result.missingFields.join("; ")),
        // 基本情報
        escapeCsvValue(""), // companyId
        escapeCsvValue(getField("name")),
        escapeCsvValue(""), // nameEn
        escapeCsvValue(""), // kana
        escapeCsvValue(getField("corporateNumber")),
        escapeCsvValue(""), // corporationType
        escapeCsvValue(""), // nikkeiCode
        escapeCsvValue(""), // badges
        escapeCsvValue(""), // tags
        escapeCsvValue(""), // createdAt
        escapeCsvValue(getField("updatedAt")),
        escapeCsvValue(""), // updateDate
        escapeCsvValue(""), // updateCount
        escapeCsvValue(""), // changeCount
        escapeCsvValue(""), // qualificationGrade
        // 所在地情報
        escapeCsvValue(getField("prefecture")),
        escapeCsvValue(""), // address
        escapeCsvValue(getField("headquartersAddress")),
        escapeCsvValue(getField("postalCode")),
        escapeCsvValue(""), // location
        escapeCsvValue(""), // departmentLocation
        // 連絡先情報
        escapeCsvValue(getField("phoneNumber")),
        escapeCsvValue(""), // contactPhoneNumber
        escapeCsvValue(getField("fax")),
        escapeCsvValue(getField("email")),
        escapeCsvValue(getField("companyUrl")),
        escapeCsvValue(getField("contactFormUrl")),
        // 代表者情報
        escapeCsvValue(getField("representativeName")),
        escapeCsvValue(""), // representativeKana
        escapeCsvValue(""), // representativeTitle
        escapeCsvValue(""), // representativeBirthDate
        escapeCsvValue(""), // representativePhone
        escapeCsvValue(""), // representativePostalCode
        escapeCsvValue(""), // representativeHomeAddress
        escapeCsvValue(""), // representativeRegisteredAddress
        escapeCsvValue(""), // representativeAlmaMater
        escapeCsvValue(formatArray(getField("executives"))),
        // 役員情報
        escapeCsvValue(getField("executiveName1")),
        escapeCsvValue(getField("executiveName2")),
        escapeCsvValue(getField("executiveName3")),
        escapeCsvValue(getField("executiveName4")),
        escapeCsvValue(getField("executiveName5")),
        escapeCsvValue(getField("executiveName6")),
        escapeCsvValue(getField("executiveName7")),
        escapeCsvValue(getField("executiveName8")),
        escapeCsvValue(getField("executiveName9")),
        escapeCsvValue(getField("executiveName10")),
        escapeCsvValue(""), // executivePosition1-10
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        // 業種情報
        escapeCsvValue(getField("industry")),
        escapeCsvValue(""), // industryLarge
        escapeCsvValue(""), // industryMiddle
        escapeCsvValue(""), // industrySmall
        escapeCsvValue(""), // industryDetail
        escapeCsvValue(""), // industries
        escapeCsvValue(""), // industryCategories
        escapeCsvValue(getField("companyDescription")), // businessDescriptions
        escapeCsvValue(""), // businessItems
        escapeCsvValue(""), // businessSummary
        escapeCsvValue(""), // specialties
        escapeCsvValue(""), // demandProducts
        escapeCsvValue(""), // specialNote
        // 財務情報
        escapeCsvValue(getField("capitalStock")),
        escapeCsvValue(getField("revenue")),
        escapeCsvValue(getField("latestRevenue")),
        escapeCsvValue(getField("latestProfit")),
        escapeCsvValue(""), // revenueFromStatements
        escapeCsvValue(getField("operatingIncome")),
        escapeCsvValue(getField("totalAssets")),
        escapeCsvValue(getField("totalLiabilities")),
        escapeCsvValue(getField("netAssets")),
        escapeCsvValue(""), // issuedShares
        escapeCsvValue(""), // financials
        escapeCsvValue(getField("listing")),
        escapeCsvValue(""), // marketSegment
        escapeCsvValue(""), // latestFiscalYearMonth
        escapeCsvValue(getField("fiscalMonth")),
        escapeCsvValue(""), // fiscalMonth1-5
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(getField("revenue1")), // revenue1-5
        escapeCsvValue(getField("revenue2")),
        escapeCsvValue(getField("revenue3")),
        escapeCsvValue(getField("revenue4")),
        escapeCsvValue(getField("revenue5")),
        escapeCsvValue(getField("profit1")), // profit1-5
        escapeCsvValue(getField("profit2")),
        escapeCsvValue(getField("profit3")),
        escapeCsvValue(getField("profit4")),
        escapeCsvValue(getField("profit5")),
        // 企業規模・組織
        escapeCsvValue(getField("employeeCount")),
        escapeCsvValue(""), // employeeNumber
        escapeCsvValue(getField("factoryCount")),
        escapeCsvValue(getField("officeCount")),
        escapeCsvValue(getField("storeCount")),
        escapeCsvValue(""), // averageAge
        escapeCsvValue(""), // averageYearsOfService
        escapeCsvValue(""), // averageOvertimeHours
        escapeCsvValue(""), // averagePaidLeave
        escapeCsvValue(""), // femaleExecutiveRatio
        // 設立・沿革
        escapeCsvValue(getField("established")),
        escapeCsvValue(getField("dateOfEstablishment")),
        escapeCsvValue(""), // founding
        escapeCsvValue(""), // foundingYear
        escapeCsvValue(""), // acquisition
        // 取引先・関係会社
        escapeCsvValue(""), // clients
        escapeCsvValue(""), // suppliers
        escapeCsvValue(""), // subsidiaries
        escapeCsvValue(""), // affiliations
        escapeCsvValue(formatArray(getField("shareholders"))),
        escapeCsvValue(formatArray(getField("banks"))),
        escapeCsvValue(""), // bankCorporateNumber
        // 部署・拠点情報
        escapeCsvValue(""), // departmentName1-7
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""), // departmentAddress1-7
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""), // departmentPhone1-7
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        escapeCsvValue(""),
        // 企業説明
        escapeCsvValue(getField("overview")),
        escapeCsvValue(getField("companyDescription")),
        escapeCsvValue(""), // businessDescriptions (重複)
        escapeCsvValue(""), // salesNotes
        // SNS・外部リンク
        escapeCsvValue(formatArray(getField("urls"))),
        escapeCsvValue(""), // profileUrl
        escapeCsvValue(""), // externalDetailUrl
        escapeCsvValue(""), // facebook
        escapeCsvValue(""), // linkedin
        escapeCsvValue(""), // wantedly
        escapeCsvValue(""), // youtrust
        escapeCsvValue(""), // metaKeywords
        // 追加メタ情報
        escapeCsvValue(formatArray(getField("sourceUrls"))),
      ];

      writeStream.write(row.join(",") + "\n");
    }

    writeStream.end();
    await new Promise<void>((resolve, reject) => {
      writeStream.on("finish", () => resolve());
      writeStream.on("error", (err) => reject(err));
    });

    console.log(`[CSV出力] ${outputPath} に ${results.length} 件のデータを出力しました`);
  } catch (error) {
    console.error("[CSV出力エラー]:", error);
    throw error;
  }
}

// ------------------------------
// 実行
// ------------------------------

async function main() {
  // コマンドライン引数から実行モードを判定
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.log("使用方法:");
    console.log("  1. 企業リストページURLを指定:");
    console.log("     npx tsx scripts/scrape_fumadata.ts --list-url <URL>");
    console.log("");
    console.log("  2. 企業名リストを指定（カンマ区切り）:");
    console.log("     npx tsx scripts/scrape_fumadata.ts --names \"企業名1,企業名2,企業名3\"");
    console.log("");
    console.log("  例:");
    console.log("    npx tsx scripts/scrape_fumadata.ts --list-url https://fumadata.com/companies");
    console.log("    npx tsx scripts/scrape_fumadata.ts --names \"株式会社A,株式会社B\"");
    process.exit(1);
  }

  const listUrlIndex = args.indexOf("--list-url");
  const namesIndex = args.indexOf("--names");

  if (listUrlIndex !== -1 && listUrlIndex + 1 < args.length) {
    const listUrl = args[listUrlIndex + 1];
    await scrapeCompaniesFromListPage(listUrl);
  } else if (namesIndex !== -1 && namesIndex + 1 < args.length) {
    const namesStr = args[namesIndex + 1];
    const companyNames = namesStr.split(",").map(n => n.trim()).filter(n => n.length > 0);
    await scrapeCompaniesFromNameList(companyNames);
  } else {
    console.error("❌ エラー: 引数が不正です");
    process.exit(1);
  }
}

main()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });

