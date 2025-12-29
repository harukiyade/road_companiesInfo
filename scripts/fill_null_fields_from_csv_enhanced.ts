/* eslint-disable no-console */

/**
 * scripts/fill_null_fields_from_csv_enhanced.ts
 * 
 * 目的: null_fields_detailed配下のCSVファイルを読み込み、
 *       各nullフィールドに対して指定サービスから情報を取得して、
 *       CSVファイルに直接値を書き込む（高速化・並列処理対応）
 * 
 * 実行方法:
 * 1. 上から実行: START_FILE=1 END_FILE=10000 REVERSE=false
 * 2. 下から実行: START_FILE=1 END_FILE=10000 REVERSE=true
 */

import * as fs from "fs";
import * as path from "path";
import fetch from "node-fetch";
import * as cheerio from "cheerio";
import admin from "firebase-admin";

// Firebase初期化
const serviceAccountKeyPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
if (!serviceAccountKeyPath) {
  console.error("❌ FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません");
  process.exit(1);
}

try {
  const serviceAccount = require(serviceAccountKeyPath);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
  console.log("✅ Firebase初期化完了");
} catch (error: any) {
  console.error("❌ Firebase初期化エラー:", error.message);
  process.exit(1);
}

const db = admin.firestore();

// 実行パラメータ
const START_FILE = parseInt(process.env.START_FILE || "1", 10);
const END_FILE = parseInt(process.env.END_FILE || "10000", 10);
const REVERSE = process.env.REVERSE === "true";
const CONCURRENT_REQUESTS = parseInt(process.env.CONCURRENT_REQUESTS || "5", 10); // 並列リクエスト数
const CONCURRENT_FIELDS = parseInt(process.env.CONCURRENT_FIELDS || "3", 10); // 同一企業のフィールド並列処理数

/**
 * 数値を抽出
 */
function extractNumber(text: string, pattern: RegExp): number | null {
  const match = text.match(pattern);
  if (!match) return null;
  const numStr = match[1]?.replace(/,/g, "");
  if (!numStr) return null;
  const num = parseInt(numStr, 10);
  return isNaN(num) ? null : num;
}

/**
 * 金額を千円単位に正規化
 */
function normalizeToThousandYen(value: number, context: string): number {
  if (context.includes("億")) {
    return value * 100000;
  } else if (context.includes("千万")) {
    return value * 10000;
  } else if (context.includes("百万")) {
    return value * 1000;
  } else if (context.includes("万円")) {
    return value * 10;
  } else if (context.includes("千円")) {
    return value;
  } else if (context.includes("円") && !context.includes("千") && !context.includes("万") && !context.includes("億")) {
    return Math.floor(value / 1000);
  }
  return value;
}

/**
 * HTMLから全フィールドを一度に抽出（高速化のため）
 */
async function extractAllFieldsFromHtml(
  html: string,
  url: string,
  targetFields: string[]
): Promise<{ [key: string]: string | number | null }> {
  const $ = cheerio.load(html);
  const text = $.text();
  const urlLower = url.toLowerCase();
  const results: { [key: string]: string | number | null } = {};

  // HTMLからリンクを抽出（companyUrl用）- より厳格に
  if (targetFields.includes("companyUrl") || targetFields.includes("contactFormUrl")) {
    const excludeDomains = [
      'googletagmanager.com', 'google-analytics.com', 'googleapis.com', 'gstatic.com',
      'facebook.com', 'twitter.com', 'linkedin.com', 'youtube.com', 'instagram.com',
      'cdnjs.cloudflare.com', 'cdn.jsdelivr.net', 'unpkg.com', 'bootstrapcdn.com',
      'jquery.com', 'amazonaws.com', 'cloudfront.net', 'azureedge.net',
      'mynavi.jp', 'job.mynavi.jp', 'wantedly.com', 'green-japan.com',
      'doubleclick.net', 'googlesyndication.com', 'googleadservices.com',
    ];
    
    // <a>タグから企業URLを抽出（スクリプトタグ内は除外）
    $('a[href^="http"]').not('script a, style a').each((_, el) => {
      const href = $(el).attr('href');
      if (href) {
        let url = href.trim();
        // URLの末尾の不要な文字を除去
        url = url.replace(/[.,;:!?)\]}]+$/, '');
        
        const urlLower = url.toLowerCase();
        const shouldExclude = excludeDomains.some(exclude => urlLower.includes(exclude));
        
        // JSONやスクリプト内のURLを除外
        const hasJsonChars = url.includes('{') || url.includes('}') || url.includes('[') || url.includes(']');
        const isValidUrl = /^https?:\/\/[^\s]{10,200}$/i.test(url);
        
        if (!shouldExclude && !hasJsonChars && isValidUrl) {
          const linkText = $(el).text().toLowerCase().trim();
          const parentText = $(el).parent().text().toLowerCase();
          
          // 企業サイトを示すリンクテキストをチェック
          if ((linkText.includes('公式') || linkText.includes('ホームページ') || 
               linkText.includes('website') || linkText.includes('hp') ||
               parentText.includes('公式サイト') || parentText.includes('ホームページ') ||
               url.match(/\.(co\.jp|com\.jp)$/i)) &&
              !results.companyUrl) {
            results.companyUrl = url;
          }
          
          // 問い合わせフォームを示すリンクテキストをチェック（より厳格に）
          if ((linkText.includes('お問い合わせ') || linkText.includes('問い合わせ') || 
               linkText.includes('contact') || linkText.includes('inquiry') ||
               urlLower.includes('contact') || urlLower.includes('inquiry') || urlLower.includes('form')) &&
              !results.contactFormUrl) {
            results.contactFormUrl = url;
          }
        }
      }
    });
  }

  // サイト別の詳細抽出ロジック（export_webinfo_to_csv.tsから移植）
  if (urlLower.includes("mynavi.jp") || urlLower.includes("job.mynavi.jp")) {
    // マイナビ転職: 代表者名と業種を詳細に抽出
    $('.company-info, .company-detail, .company-profile, .company-data, table').each((_, el) => {
      const infoText = $(el).text();
      extractFieldsFromText(infoText, targetFields, results);
    });
    // 代表者名をテーブルから抽出
    $('th:contains("代表者"), th:contains("代表取締役"), th:contains("社長")').each((_, el) => {
      const nextTd = $(el).next('td');
      if (nextTd.length > 0) {
        const repText = nextTd.text().trim();
        if (repText && !results.representativeName) {
          const nameMatch = repText.match(/([^\s　（(]{2,20}?)(?:\s|$|（|\(|代表取締役|取締役|社長)/);
          if (nameMatch && nameMatch[1] && nameMatch[1].length >= 2) {
            results.representativeName = nameMatch[1].trim();
          }
        }
      }
    });
    // 業種をテーブルから抽出
    $('th:contains("業種"), th:contains("事業内容")').each((_, el) => {
      const nextTd = $(el).next('td');
      if (nextTd.length > 0) {
        const industryText = nextTd.text().trim();
        if (industryText && !industryText.match(/^(すべて|全て|なし|不明)$/i) && !results.industry) {
          results.industry = industryText.substring(0, 50).trim();
        }
      }
    });
  } else if (urlLower.includes("houjin.jp")) {
    $('.company-info, .company-detail, .company-data, table').each((_, el) => {
      const infoText = $(el).text();
      extractFieldsFromText(infoText, targetFields, results);
    });
    // テーブルから代表者名と業種を抽出
    $('th:contains("代表者"), th:contains("代表取締役")').each((_, el) => {
      const nextTd = $(el).next('td');
      if (nextTd.length > 0 && !results.representativeName) {
        const repText = nextTd.text().trim();
        const nameMatch = repText.match(/([^\s　（(]{2,20}?)(?:\s|$|（|\(|代表取締役|取締役|社長)/);
        if (nameMatch && nameMatch[1] && nameMatch[1].length >= 2) {
          results.representativeName = nameMatch[1].trim();
        }
      }
    });
    $('th:contains("業種"), th:contains("事業内容")').each((_, el) => {
      const nextTd = $(el).next('td');
      if (nextTd.length > 0 && !results.industry) {
        const industryText = nextTd.text().trim();
        if (industryText && !industryText.match(/^(すべて|全て|なし|不明)$/i)) {
          results.industry = industryText.substring(0, 50).trim();
        }
      }
    });
  } else if (urlLower.includes("alarmbox.jp")) {
    $('.company-info, .company-detail, .company-data, .company-profile, table').each((_, el) => {
      const infoText = $(el).text();
      extractFieldsFromText(infoText, targetFields, results);
    });
  } else if (urlLower.includes("g-search.or.jp") || urlLower.includes("cnavi-app")) {
    $('.company-info, .company-detail, .company-data, table').each((_, el) => {
      const infoText = $(el).text();
      extractFieldsFromText(infoText, targetFields, results);
    });
  } else if (urlLower.includes("buffett-code.com")) {
    $('.financial-data, .company-data, table, .data-table').each((_, el) => {
      const tableText = $(el).text();
      extractFieldsFromText(tableText, targetFields, results);
    });
  } else if (urlLower.includes("catr.jp")) {
    $('.financial-data, .kessan-data, table').each((_, el) => {
      const tableText = $(el).text();
      extractFieldsFromText(tableText, targetFields, results);
    });
  }

  // 全テキストからも抽出（フォールバック）
  extractFieldsFromText(text, targetFields, results);

  return results;
}

/**
 * テキストから指定フィールドを抽出
 */
function extractFieldsFromText(
  text: string,
  targetFields: string[],
  results: { [key: string]: string | number | null }
): void {
  for (const fieldName of targetFields) {
    if (results[fieldName] !== null && results[fieldName] !== undefined) {
      continue; // 既に取得済み
    }

    let value: string | number | null = null;

    switch (fieldName) {
      case "corporateNumber": {
        const match = text.match(/法人番号[：:]\s*(\d{13})/i);
        if (match) value = match[1];
        break;
      }
      case "prefecture": {
        const prefecture = text.match(/(東京都|北海道|(?:大阪|京都|兵庫|奈良|和歌山|滋賀|三重)府|(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|京都|大阪|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)/);
        if (prefecture) value = prefecture[1];
        break;
      }
      case "address":
      case "headquartersAddress": {
        const address = text.match(/所在地[：:]\s*([^\n]+)/i) || text.match(/(〒\d{3}-?\d{4}[\s　]*[^\n]{10,100})/);
        if (address) value = address[1].trim();
        break;
      }
      case "phoneNumber":
      case "contactPhoneNumber": {
        const phone = text.match(/電話番号[：:]\s*([0-9-()]+)/i) || text.match(/(\d{2,4}-\d{2,4}-\d{4})/);
        if (phone) value = phone[1].trim();
        break;
      }
      case "fax": {
        const fax = text.match(/(FAX|Fax|fax)[：:]\s*([0-9-()]+)/i);
        if (fax) value = fax[2].replace(/[^\d-]/g, "");
        break;
      }
      case "email": {
        const email = text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
        if (email) value = email[0];
        break;
      }
      case "companyUrl": {
        // 除外するドメインリスト（拡張版）
        const excludeDomains = [
          'googletagmanager.com', 'google-analytics.com', 'googleapis.com', 'gstatic.com',
          'facebook.com', 'twitter.com', 'linkedin.com', 'youtube.com', 'instagram.com',
          'cdnjs.cloudflare.com', 'cdn.jsdelivr.net', 'unpkg.com', 'bootstrapcdn.com',
          'jquery.com', 'amazonaws.com', 'cloudfront.net', 'azureedge.net',
          'mynavi.jp', 'job.mynavi.jp', 'wantedly.com', 'green-japan.com',
          'doubleclick.net', 'googlesyndication.com', 'googleadservices.com',
          'adservice.google', 'adservice.google.com', 'analytics.google.com',
        ];
        
        // より具体的なパターンで企業URLを抽出
        const patterns = [
          /(?:公式サイト|ホームページ|ウェブサイト|Website|HP|URL)[：:]\s*(https?:\/\/[^\s\n]+)/i,
          /<a[^>]+href=["'](https?:\/\/[^"']+\.(?:co\.jp|com\.jp|jp))["'][^>]*>/i,
        ];
        
        for (const pattern of patterns) {
          const match = text.match(pattern);
          if (match) {
            let url = match[1].trim();
            // URLの末尾の不要な文字を除去
            url = url.replace(/[.,;:!?)\]}]+$/, '');
            
            // 除外ドメインをチェック
            const urlLower = url.toLowerCase();
            const shouldExclude = excludeDomains.some(domain => urlLower.includes(domain));
            
            // JSONやスクリプト内のURLを除外
            const hasJsonChars = url.includes('{') || url.includes('}') || url.includes('[') || url.includes(']');
            
            // 周辺テキストにJSONパターンがないかチェック（より厳格に）
            const urlIndex = text.indexOf(url);
            const contextStart = Math.max(0, urlIndex - 50);
            const contextEnd = Math.min(text.length, urlIndex + url.length + 50);
            const context = text.substring(contextStart, contextEnd);
            const hasJsonPattern = /["']\s*\{|prefetch|gtag|dataLayer|script/i.test(context);
            
            // 有効なURL形式をチェック
            const isValidUrl = /^https?:\/\/[^\s]{10,200}$/i.test(url);
            
            // ドメインが有効かチェック（.co.jp, .com.jp, .jp, .com, .net, .orgなど）
            const hasValidDomain = /\.(co\.jp|com\.jp|jp|com|net|org|co\.uk|co\.kr)(?:\/|$)/i.test(url);
            
            if (!shouldExclude && !hasJsonChars && !hasJsonPattern && isValidUrl && hasValidDomain) {
              value = url;
              break;
            }
          }
        }
        break;
      }
      case "contactFormUrl": {
        // 除外するドメインリスト
        const excludeDomains = [
          'googletagmanager.com', 'google-analytics.com', 'googleapis.com', 'gstatic.com',
          'facebook.com', 'twitter.com', 'linkedin.com', 'youtube.com',
          'cdnjs.cloudflare.com', 'cdn.jsdelivr.net', 'unpkg.com',
        ];
        
        // より具体的なパターンで問い合わせフォームURLを抽出
        const patterns = [
          /(?:お問い合わせ|問い合わせ|コンタクト|Contact)[：:]\s*(https?:\/\/[^\s\n]+)/i,
          /<a[^>]+href=["'](https?:\/\/[^"']*(?:contact|inquiry|form|お問い合わせ|問い合わせ|inquiry)[^"']*)["'][^>]*>/i,
          /(?:お問い合わせ|問い合わせ)[^。]*?(https?:\/\/[^\s\n]+\.(?:html|php|aspx?|jsp|cgi))/i,
        ];
        
        for (const pattern of patterns) {
          const match = text.match(pattern);
          if (match) {
            let url = match[1].trim();
            // URLの末尾の不要な文字を除去
            url = url.replace(/[.,;:!?)\]}]+$/, '');
            
            // 除外ドメインをチェック
            const urlLower = url.toLowerCase();
            const shouldExclude = excludeDomains.some(domain => urlLower.includes(domain));
            
            // JSONやスクリプト内のURLを除外（より厳格に）
            const hasJsonChars = url.includes('{') || url.includes('}') || url.includes('[') || url.includes(']');
            
            // 周辺テキストにJSONパターンがないかチェック（より厳格に）
            const urlIndex = text.indexOf(url);
            const contextStart = Math.max(0, urlIndex - 50);
            const contextEnd = Math.min(text.length, urlIndex + url.length + 50);
            const context = text.substring(contextStart, contextEnd);
            const hasJsonPattern = /["']\s*\{|prefetch|gtag|dataLayer|script|config/i.test(context);
            
            // 有効なURL形式をチェック
            const isValidUrl = /^https?:\/\/[^\s]{10,200}$/i.test(url);
            
            // ドメインが有効かチェック
            const hasValidDomain = /\.(co\.jp|com\.jp|jp|com|net|org|co\.uk|co\.kr)(?:\/|$)/i.test(url);
            
            // 問い合わせフォームを示すキーワードを含むかチェック（必須）
            const hasContactKeyword = urlLower.includes('contact') || urlLower.includes('inquiry') || 
                                     urlLower.includes('form') || urlLower.includes('お問い合わせ') || 
                                     urlLower.includes('問い合わせ') || urlLower.includes('inquiry');
            
            if (!shouldExclude && !hasJsonChars && !hasJsonPattern && isValidUrl && hasValidDomain && hasContactKeyword) {
              value = url;
              break;
            }
          }
        }
        break;
      }
      case "representativeName": {
        // 複数のパターンで代表者名を抽出
        const patterns = [
          /代表者[：:]\s*([^\n（(]+?)(?:[（(]|$)/i,
          /代表取締役[：:]\s*([^\n（(]+?)(?:[（(]|$)/i,
          /代表取締役社長[：:]\s*([^\n（(]+?)(?:[（(]|$)/i,
          /社長[：:]\s*([^\n（(]+?)(?:[（(]|$)/i,
          /代表[：:]\s*([^\n（(]+?)(?:[（(]|$)/i,
          /(?:代表取締役|取締役社長|社長|代表)\s*([^\s　\n（(]{2,20}?)(?:\s|$|（|\(|：|:)/i,
          /([^\s　\n（(]{2,20}?)\s*(?:代表取締役|取締役社長|社長|代表)/i,
        ];
        
        for (const pattern of patterns) {
          const match = text.match(pattern);
          if (match) {
            let name = match[1].trim();
            // 役職名を除去
            name = name.replace(/^(代表取締役|取締役|社長|CEO|代表|会長|専務|常務|執行役員)[\s　]*/i, "").trim();
            name = name.replace(/[\s　]*(代表取締役|取締役|社長|CEO|代表|会長|専務|常務|執行役員)$/i, "").trim();
            // カナや括弧内の情報を除去
            name = name.replace(/[（(][^）)]+[）)]/g, "").trim();
            // 長すぎる場合は最初の部分のみ
            if (name.length > 20) {
              name = name.substring(0, 20).trim();
            }
            if (name && name.length >= 2 && name.length <= 20 && !name.match(/^(すべて|全て|なし|不明)$/i)) {
              value = name;
              break;
            }
          }
        }
        break;
      }
      case "representativeKana": {
        const kana = text.match(/代表者[（(]カナ[）)][：:]\s*([^\n]+)/i) || text.match(/代表者名[（(]カナ[）)][：:]\s*([^\n]+)/i);
        if (kana) value = kana[1].trim();
        break;
      }
      case "representativeTitle": {
        const title = text.match(/(代表取締役|取締役|社長|CEO|代表)/i);
        if (title) value = title[1];
        break;
      }
      case "representativeBirthDate": {
        const birth = text.match(/代表者生年月日[：:]\s*(\d{4})[年\/](\d{1,2})[月\/](\d{1,2})日?/i);
        if (birth) value = `${birth[1]}-${birth[2].padStart(2, "0")}-${birth[3].padStart(2, "0")}`;
        break;
      }
      case "representativePhone": {
        const repPhone = text.match(/代表者電話[：:]\s*([0-9-()]+)/i);
        if (repPhone) value = repPhone[1].trim();
        break;
      }
      case "representativePostalCode": {
        const repPostal = text.match(/代表者郵便番号[：:]\s*(\d{3}-?\d{4})/i);
        if (repPostal) value = repPostal[1].replace(/-/g, "");
        break;
      }
      case "representativeHomeAddress": {
        const repHome = text.match(/代表者自宅住所[：:]\s*([^\n]+)/i);
        if (repHome) value = repHome[1].trim();
        break;
      }
      case "representativeRegisteredAddress": {
        const repReg = text.match(/代表者登録住所[：:]\s*([^\n]+)/i);
        if (repReg) value = repReg[1].trim();
        break;
      }
      case "representativeAlmaMater": {
        const alma = text.match(/代表者出身校[：:]\s*([^\n]+)/i) || text.match(/出身校[：:]\s*([^\n]+)/i);
        if (alma) value = alma[1].trim();
        break;
      }
      case "executives": {
        const execs: string[] = [];
        const execPatterns = [
          /(?:取締役|役員)[：:]\s*([^\n]+)/gi,
          /(?:代表取締役|取締役社長)[：:]\s*([^\n]+)/gi,
        ];
        for (const pattern of execPatterns) {
          const matches = text.matchAll(pattern);
          for (const match of matches) {
            if (match[1]) execs.push(match[1].trim());
          }
        }
        if (execs.length > 0) value = execs.join("; ");
        break;
      }
      case "industry": {
        // 複数のパターンで業種を抽出（「すべて」を除外）
        const patterns = [
          /業種[：:]\s*([^\n]+)/i,
          /事業内容[：:]\s*([^\n]+)/i,
          /事業分野[：:]\s*([^\n]+)/i,
          /業界[：:]\s*([^\n]+)/i,
          /(?:IT|製造|サービス|小売|建設|不動産|金融|医療|教育|運輸|通信|エネルギー|食品|繊維|化学|機械|電気|情報|広告|人材|コンサル|不動産|卸売|飲食|宿泊|娯楽|その他)[業界|業種]/i,
        ];
        
        for (const pattern of patterns) {
          const match = text.match(pattern);
          if (match) {
            let industryValue = match[1].trim();
            // 「すべて」「全て」「なし」「不明」を除外
            if (industryValue.match(/^(すべて|全て|なし|不明|未分類|その他|その他の)$/i)) {
              continue;
            }
            // 長すぎる場合は最初の部分のみ
            if (industryValue.length > 50) {
              industryValue = industryValue.substring(0, 50).trim();
            }
            // 括弧内の情報を除去（必要に応じて）
            industryValue = industryValue.replace(/[（(][^）)]+[）)]/g, "").trim();
            if (industryValue && industryValue.length >= 2 && industryValue.length <= 50) {
              value = industryValue;
              break;
            }
          }
        }
        break;
      }
      case "industryLarge":
      case "industryMiddle":
      case "industrySmall":
      case "industryDetail": {
        const industryMap: { [key: string]: RegExp } = {
          industryLarge: /業種[（(]大分類[）)][：:]\s*([^\n]+)/i,
          industryMiddle: /業種[（(]中分類[）)][：:]\s*([^\n]+)/i,
          industrySmall: /業種[（(]小分類[）)][：:]\s*([^\n]+)/i,
          industryDetail: /業種[（(]詳細[）)][：:]\s*([^\n]+)/i,
        };
        const match = text.match(industryMap[fieldName]);
        if (match) value = match[1].trim();
        break;
      }
      case "capitalStock": {
        const capital = extractNumber(text, /資本金[：:]\s*([\d,]+)/i);
        if (capital) value = normalizeToThousandYen(capital, text);
        break;
      }
      case "revenue": {
        const revenue = extractNumber(text, /売上高[：:]\s*([\d,]+)/i);
        if (revenue) value = normalizeToThousandYen(revenue, text);
        break;
      }
      case "operatingIncome": {
        const operating = extractNumber(text, /営業利益[：:]\s*([\d,]+)/i);
        if (operating) value = normalizeToThousandYen(operating, text);
        break;
      }
      case "totalAssets": {
        const totalAssets = extractNumber(text, /総資産[：:]\s*([\d,]+)/i);
        if (totalAssets) value = normalizeToThousandYen(totalAssets, text);
        break;
      }
      case "totalLiabilities": {
        const totalLiabilities = extractNumber(text, /総負債[：:]\s*([\d,]+)/i);
        if (totalLiabilities) value = normalizeToThousandYen(totalLiabilities, text);
        break;
      }
      case "netAssets": {
        const netAssets = extractNumber(text, /純資産[：:]\s*([\d,]+)/i);
        if (netAssets) value = normalizeToThousandYen(netAssets, text);
        break;
      }
      case "listing": {
        const listing = text.match(/(東証|名証|福証|札証|上場|非上場|未上場|マザーズ|グロース|スタンダード|プライム)/i);
        if (listing) value = listing[1];
        break;
      }
      case "marketSegment": {
        const segment = text.match(/市場区分[：:]\s*([^\n]+)/i);
        if (segment) value = segment[1].trim();
        break;
      }
      case "latestFiscalYearMonth": {
        const fiscal = text.match(/最新決算[：:]\s*(\d{4})年(\d{1,2})月/i);
        if (fiscal) value = `${fiscal[1]}年${fiscal[2]}月`;
        break;
      }
      case "fiscalMonth": {
        const fiscalMonth = text.match(/決算期[：:]\s*(\d{1,2})月/i);
        if (fiscalMonth) value = `${fiscalMonth[1]}月`;
        break;
      }
      case "employeeCount": {
        const employees = extractNumber(text, /従業員数[：:]\s*(\d+)/i);
        if (employees) value = employees;
        break;
      }
      case "factoryCount": {
        const factories = extractNumber(text, /工場数[：:]\s*(\d+)/i);
        if (factories) value = factories;
        break;
      }
      case "officeCount": {
        const offices = extractNumber(text, /オフィス数[：:]\s*(\d+)/i) || extractNumber(text, /事業所数[：:]\s*(\d+)/i);
        if (offices) value = offices;
        break;
      }
      case "storeCount": {
        const stores = extractNumber(text, /店舗数[：:]\s*(\d+)/i);
        if (stores) value = stores;
        break;
      }
      case "established":
      case "dateOfEstablishment": {
        const established = text.match(/設立[：:]\s*(\d{4})年(\d{1,2})月(\d{1,2})日?/i);
        if (established) {
          if (fieldName === "dateOfEstablishment") {
            value = `${established[1]}-${established[2].padStart(2, "0")}-${(established[3] || "01").padStart(2, "0")}`;
          } else {
            value = `${established[1]}年${established[2]}月${established[3] || "1"}日`;
          }
        }
        break;
      }
      case "clients": {
        const clients = text.match(/取引先[：:]\s*([^\n]+)/i);
        if (clients) value = clients[1].trim();
        break;
      }
      case "suppliers": {
        const suppliers: string[] = [];
        const supplierPattern = /仕入先[：:]\s*([^\n]+)/gi;
        const matches = text.matchAll(supplierPattern);
        for (const match of matches) {
          if (match[1]) suppliers.push(match[1].trim());
        }
        if (suppliers.length > 0) value = suppliers.join("; ");
        break;
      }
      case "shareholders": {
        const shareholders: string[] = [];
        const shareholderPattern = /(?:主要株主|株主)[：:]\s*([^\n]+)/gi;
        const matches = text.matchAll(shareholderPattern);
        for (const match of matches) {
          if (match[1]) shareholders.push(match[1].trim());
        }
        if (shareholders.length > 0) value = shareholders.join("; ");
        break;
      }
      case "banks": {
        const banks: string[] = [];
        const bankPattern = /(?:取引銀行|主要取引銀行|メインバンク)[：:]\s*([^\n]+)/gi;
        const matches = text.matchAll(bankPattern);
        for (const match of matches) {
          if (match[1]) banks.push(match[1].trim());
        }
        if (banks.length > 0) value = banks.join("; ");
        break;
      }
    }

    if (value !== null && value !== undefined) {
      results[fieldName] = value;
    }
  }
}

/**
 * リトライ付きHTTPリクエスト
 */
async function fetchWithRetry(
  url: string,
  options: any = {},
  maxRetries: number = 2,
  retryDelay: number = 2000
): Promise<any> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      
      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
          ...options.headers,
        },
      });
      
      clearTimeout(timeout);
      
      if (response.status === 429) {
        const retryAfter = response.headers.get("retry-after");
        const delay = retryAfter ? parseInt(retryAfter, 10) * 1000 : retryDelay * (attempt + 1);
        await new Promise(resolve => setTimeout(resolve, delay));
        continue;
      }
      
      if (response.status >= 500 && attempt < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, retryDelay * (attempt + 1)));
        continue;
      }
      
      return response;
    } catch (error: any) {
      if (attempt === maxRetries) {
        return null;
      }
      if (error.name === "AbortError" || error.code === "ETIMEDOUT") {
        await new Promise(resolve => setTimeout(resolve, retryDelay * (attempt + 1)));
        continue;
      }
      return null;
    }
  }
  return null;
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

    // 各サービスの検索結果ページから企業詳細ページのURLを抽出
    if (urlLower.includes("mynavi.jp") || urlLower.includes("job.mynavi.jp")) {
      $('a[href*="/company/"], a[href*="/corp/"], a[href*="/detail/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : new URL(href, searchUrl).href;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("houjin.jp")) {
      $('a[href*="/company/"], a[href*="/detail/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : new URL(href, searchUrl).href;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("alarmbox.jp")) {
      $('a[href*="/companyinfo/"], a[href*="/company/"], a[href*="/detail/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : new URL(href, searchUrl).href;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("g-search.or.jp") || urlLower.includes("cnavi-app")) {
      $('a[href*="/company/"], a[href*="/detail/"], a[href*="/corp/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : new URL(href, searchUrl).href;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("buffett-code.com")) {
      $('a[href*="/company/"], a[href*="/detail/"], a[href*="/screening/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : new URL(href, searchUrl).href;
          if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
        }
      });
    } else if (urlLower.includes("catr.jp")) {
      $('a[href*="/detail/"], a[href*="/company/"], a[href*="/kessan/"], a[href*="/announcement/"]').each((_, el) => {
        const href = $(el).attr("href");
        if (href && !href.includes("/search") && !href.includes("#")) {
          const fullUrl = href.startsWith("http") ? href : `https://catr.jp${href}`;
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
            if (fullUrl.toLowerCase().includes(companyName.toLowerCase().substring(0, 3)) || 
                fullUrl.match(/\/company\/|\/companies\/|\/corp\//i)) {
              if (!detailUrls.includes(fullUrl)) detailUrls.push(fullUrl);
            }
          } catch {}
        }
      });
    }

    // 会社名でフィルタリング
    const filteredUrls = detailUrls.filter(url => {
      const urlLower = url.toLowerCase();
      const nameLower = companyName.toLowerCase();
      return urlLower.includes(nameLower.substring(0, Math.min(5, nameLower.length))) ||
             urlLower.match(/\/company\/|\/companies\/|\/corp\//i);
    });

    return filteredUrls.length > 0 ? filteredUrls.slice(0, 10) : detailUrls.slice(0, 10);
  } catch (e) {
    return [];
  }
}

/**
 * 指定サービスから企業情報を取得（詳細ページから抽出、並列処理対応）
 */
async function fetchCompanyInfoFromServices(
  companyName: string,
  corporateNumber: string | null,
  targetFields: string[]
): Promise<{ [key: string]: string | number | null }> {
  const searchUrls: string[] = [];

  // 指定された7つのサービスのみから取得
  // 1. 企業INDEXナビ
  searchUrls.push(`https://cnavi-app.g-search.or.jp/search?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    searchUrls.push(`https://cnavi-app.g-search.or.jp/search?q=${encodeURIComponent(corporateNumber)}`);
  }

  // 2. バフェットコード
  searchUrls.push(`https://www.buffett-code.com/global_screening?q=${encodeURIComponent(companyName)}`);

  // 3. マイナビ転職
  searchUrls.push(`https://tenshoku.mynavi.jp/company/search?q=${encodeURIComponent(companyName)}`);

  // 4. マイナビ2026
  searchUrls.push(`https://job.mynavi.jp/26/pc/search/corp.html?tab=corp&q=${encodeURIComponent(companyName)}`);

  // 5. 全国法人リスト
  searchUrls.push(`https://houjin.jp/search?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    searchUrls.push(`https://houjin.jp/search?q=${encodeURIComponent(corporateNumber)}`);
  }

  // 6. 官報決算データベース
  searchUrls.push(`https://catr.jp/s/?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    searchUrls.push(`https://catr.jp/s/?q=${encodeURIComponent(corporateNumber)}`);
  }

  // 7. Alarmbox
  searchUrls.push(`https://alarmbox.jp/companyinfo/?q=${encodeURIComponent(companyName)}`);

  const results: { [key: string]: string | number | null } = {};
  const foundFields = new Set<string>();
  const detailUrls: string[] = [];

  // ステップ1: 検索結果ページから詳細ページURLを抽出（指定された7つのサービスのみ）
  console.log(`    [${companyName}] 検索結果から詳細ページURLを抽出中...`);
  for (const searchUrl of searchUrls) { // 指定された7つのサービスの全てを使用
    try {
      const urls = await extractDetailUrlsFromSearchPage(searchUrl, companyName);
      detailUrls.push(...urls);
      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (error) {
      // エラーは無視
    }
  }

  // 重複除去
  const uniqueDetailUrls = Array.from(new Set(detailUrls)).slice(0, 10);

  if (uniqueDetailUrls.length === 0) {
    console.log(`    [${companyName}] ⚠️  詳細ページURLが見つかりませんでした`);
    return results;
  }

  console.log(`    [${companyName}] ${uniqueDetailUrls.length} 件の詳細ページURLを発見`);

  // ステップ2: 詳細ページから情報を抽出（並列処理）
  const urlBatches: string[][] = [];
  for (let i = 0; i < uniqueDetailUrls.length; i += CONCURRENT_REQUESTS) {
    urlBatches.push(uniqueDetailUrls.slice(i, i + CONCURRENT_REQUESTS));
  }

  for (const batch of urlBatches) {
    const promises = batch.map(async (url) => {
      try {
        const response = await fetchWithRetry(url, {}, 1, 2000);
        if (!response || !response.ok) return null;

        const html = await response.text();
        const extracted = await extractAllFieldsFromHtml(html, url, targetFields);
        
        // 結果をマージ
        for (const [field, value] of Object.entries(extracted)) {
          if (value !== null && value !== undefined && value !== "" && !foundFields.has(field)) {
            results[field] = value;
            foundFields.add(field);
          }
        }

        return extracted;
      } catch (error) {
        return null;
      }
    });

    await Promise.all(promises);
    
    // 全てのフィールドが取得できた場合は早期終了
    if (foundFields.size >= targetFields.length) {
      break;
    }

    // レート制限
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  return results;
}

/**
 * 並列処理用のチャンク分割
 */
function chunkArray<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

/**
 * CSVファイルを処理（高速化・並列処理対応）
 */
async function processCsvFile(csvPath: string): Promise<number> {
  console.log(`\n📄 処理中: ${path.basename(csvPath)}`);

  // CSVファイルを読み込み
  const content = fs.readFileSync(csvPath, "utf8");
  const lines = content.split("\n");
  const header = lines[0].trim();
  
  // ヘッダーに foundValue 列がなければ追加
  const headers = header.split(",");
  const hasFoundValue = headers.includes("foundValue");
  const newHeader = hasFoundValue ? header : `${header},foundValue`;

  // Firestoreから企業情報を取得（キャッシュ用）
  const companyCache: { [key: string]: { corporateNumber: string | null; name: string } } = {};

  // 企業ごとにグループ化
  const companyFields: { [key: string]: Array<{ line: string; fieldName: string; index: number }> } = {};
  const lineMap: { [key: number]: string } = {};

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const parts = line.split(",");
    if (parts.length < 4) continue;

    const companyId = parts[0];
    const companyName = parts[1];
    const nullFieldName = parts[2];

    // 既に値が取得済みの場合はスキップ
    if (hasFoundValue && parts.length > 5 && parts[5] && parts[5] !== "null" && parts[5] !== "") {
      lineMap[i] = line;
      continue;
    }

    if (!companyFields[companyId]) {
      companyFields[companyId] = [];
      companyCache[companyId] = { corporateNumber: null, name: companyName };
    }
    companyFields[companyId].push({ line, fieldName: nullFieldName, index: i });
    lineMap[i] = line;
  }

  // Firestoreから企業情報を一括取得（存在するドキュメントのみ）
  const companyIds = Object.keys(companyFields);
  const existingCompanyIds = new Set<string>();
  const batchSize = 100;
  for (let i = 0; i < companyIds.length; i += batchSize) {
    const batch = companyIds.slice(i, i + batchSize);
    const promises = batch.map(async (companyId) => {
      try {
        const companyDoc = await db.collection("companies_new").doc(companyId).get();
        if (companyDoc.exists) {
          const data = companyDoc.data();
          companyCache[companyId] = {
            corporateNumber: data?.corporateNumber || null,
            name: data?.name || companyCache[companyId]?.name || "",
          };
          existingCompanyIds.add(companyId);
        } else {
          // 存在しないドキュメントはスキップ
          console.log(`    [${companyId}] ⚠️  ドキュメントが存在しないためスキップ`);
        }
      } catch (error) {
        // エラーは無視
      }
    });
    await Promise.all(promises);
  }
  
  // 存在しないドキュメントのフィールドを除外
  const filteredCompanyFields: { [key: string]: Array<{ line: string; fieldName: string; index: number }> } = {};
  for (const [companyId, fields] of Object.entries(companyFields)) {
    if (existingCompanyIds.has(companyId)) {
      filteredCompanyFields[companyId] = fields;
    } else {
      // 存在しないドキュメントの行はそのまま保持（CSVには書き込むがFirestoreには更新しない）
      for (const field of fields) {
        lineMap[field.index] = field.line;
      }
    }
  }

  let updatedCount = 0;

  // 企業ごとに並列処理（存在するドキュメントのみ）
  const companyChunks = chunkArray(Object.entries(filteredCompanyFields), CONCURRENT_FIELDS);
  
  for (const chunk of companyChunks) {
    const promises = chunk.map(async ([companyId, fields]) => {
      const companyName = companyCache[companyId]?.name || "";
      const corporateNumber = companyCache[companyId]?.corporateNumber || null;

      if (!companyName) return;

      const targetFields = fields.map(f => f.fieldName);
      
      try {
        // サービスから情報を取得
        const fetchedInfo = await fetchCompanyInfoFromServices(
          companyName,
          corporateNumber,
          targetFields
        );

        // 取得した情報でCSV行を更新 & Firestoreに書き込み
        const firestoreUpdates: { [key: string]: any } = {};
        
        for (const field of fields) {
          const value = fetchedInfo[field.fieldName];
          if (value !== null && value !== undefined && value !== "") {
            const valueStr = typeof value === "string" ? `"${value.replace(/"/g, '""')}"` : String(value);
            const newLine = hasFoundValue 
              ? field.line.replace(/,"?null"?$/, `,${valueStr}`)
              : `${field.line},${valueStr}`;
            lineMap[field.index] = newLine;
            
            // Firestore更新用に値を保存
            firestoreUpdates[field.fieldName] = value;
            updatedCount++;
          } else {
            const newLine = hasFoundValue ? field.line : `${field.line},`;
            lineMap[field.index] = newLine;
          }
        }
        
        // Firestoreに書き込み（既存ドキュメントのみ更新）
        if (Object.keys(firestoreUpdates).length > 0) {
          try {
            const companyRef = db.collection("companies_new").doc(companyId);
            const companyDoc = await companyRef.get();
            
            // 既存ドキュメントのみ更新（新規作成はしない）
            if (companyDoc.exists) {
              await companyRef.update(firestoreUpdates);
              console.log(`    [${companyName}] ✅ Firestore更新: ${Object.keys(firestoreUpdates).join(", ")}`);
            } else {
              // 存在しないドキュメントはスキップ（CSVには書き込むがFirestoreには更新しない）
              console.log(`    [${companyName}] ⚠️  ドキュメントが存在しないためスキップ: ${companyId}`);
            }
          } catch (error: any) {
            // update()でエラーが発生した場合（ドキュメントが存在しない場合など）
            if (error.code === 'not-found' || error.message?.includes('No document to update')) {
              console.log(`    [${companyName}] ⚠️  ドキュメントが存在しないためスキップ: ${companyId}`);
            } else {
              console.warn(`    [${companyName}] ⚠️  Firestore更新エラー: ${error.message}`);
            }
          }
        }
      } catch (error) {
        // エラーは無視
      }
    });

    await Promise.all(promises);
    
    // レート制限
    await new Promise(resolve => setTimeout(resolve, 500));
  }

  // CSVファイルに書き込み
  const newLines = [newHeader];
  for (let i = 1; i < lines.length; i++) {
    if (lineMap[i]) {
      newLines.push(lineMap[i]);
    } else if (lines[i].trim()) {
      newLines.push(lines[i]);
    }
  }

  fs.writeFileSync(csvPath, newLines.join("\n"), "utf8");
  console.log(`  ✅ 完了: ${updatedCount} 件の値を取得しました`);

  return updatedCount;
}

/**
 * メイン処理
 */
async function main() {
  const csvDir = path.join(process.cwd(), "null_fields_detailed");
  
  if (!fs.existsSync(csvDir)) {
    console.error(`❌ ディレクトリが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  // CSVファイル一覧を取得
  const files = fs.readdirSync(csvDir)
    .filter(file => file.endsWith(".csv") && file.startsWith("null_fields_detailed_"))
    .sort();

  if (files.length === 0) {
    console.error(`❌ CSVファイルが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  // ファイル番号でフィルタリング
  const filteredFiles = files.filter(file => {
    const match = file.match(/null_fields_detailed_(\d+)\.csv/);
    if (!match) return false;
    const fileNum = parseInt(match[1], 10);
    return fileNum >= START_FILE && fileNum <= END_FILE;
  });

  // 逆順処理
  if (REVERSE) {
    filteredFiles.reverse();
  }

  console.log(`📁 ${filteredFiles.length} 個のCSVファイルを処理します`);
  console.log(`   開始ファイル: ${START_FILE}, 終了ファイル: ${END_FILE}`);
  console.log(`   実行方向: ${REVERSE ? "下から（逆順）" : "上から（順順）"}`);
  console.log(`   並列リクエスト数: ${CONCURRENT_REQUESTS}`);
  console.log(`   並列フィールド処理数: ${CONCURRENT_FIELDS}`);

  let totalUpdated = 0;
  for (const file of filteredFiles) {
    const csvPath = path.join(csvDir, file);
    const updated = await processCsvFile(csvPath);
    totalUpdated += updated;
  }

  console.log(`\n✅ 全処理完了`);
  console.log(`総取得数: ${totalUpdated} 件`);
}

main().catch((error) => {
  console.error("エラー:", error);
  process.exit(1);
});

