/* eslint-disable no-console */

/**
 * scripts/scrape_extended_fields.ts
 *
 * ✅ 目的
 * - unify_fields.tsで統一されたフィールドに対して、複数のWebサイトから情報を取得
 * - 要件に基づいて各フィールドを適切なWebサイトから取得
 *x
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 *
 * ✅ オプションENV
 * - START_FROM_COMPANY_ID=企業ID (指定した企業IDから処理を開始)
 * - REVERSE_ORDER=true (逆順実行モード: 企業IDを大きい順から処理)
 * - FAST_MODE=true (高速化モード: 並列数6、待機時間短縮、タイムアウト短縮)
 * - SKIP_ON_ERROR=true (エラー発生時にその企業をスキップして次の企業に進む)
 * - LIMIT=3 (テスト用: 実際にデータが取得できた企業数がこの数に達したら処理を終了、例: LIMIT=3で3件の成功まで続ける)
 * - PARALLEL_WORKERS=3 (並列処理数、デフォルト: 3、FAST_MODE時は6)
 * - SLEEP_MS=500 (リクエスト間隔、ミリ秒、デフォルト: 500ms、FAST_MODE時は300ms)
 * - PAGE_WAIT_MODE=domcontentloaded (ページ読み込み待機方法、domcontentloaded/networkidle)
 * - PAGE_TIMEOUT=10000 (ページタイムアウト、ミリ秒、デフォルト: 10000ms、FAST_MODE時は8000ms)
 * - NAVIGATION_TIMEOUT=10000 (ナビゲーションタイムアウト、ミリ秒、デフォルト: 10000ms、FAST_MODE時は8000ms)
 *
 * ✅ 途中から再開する方法
 * 1. ログファイル（logs/scrape_extended_fields_*.log）から最後に処理した企業IDを確認
 * 2. 環境変数 START_FROM_COMPANY_ID を設定して実行
 *   例: START_FROM_COMPANY_ID="12345" npx ts-node scripts/scrape_extended_fields.ts
 * 3. または、そのまま再実行すると処理済みの企業は自動的にスキップされます
 *
 * ✅ 逆順実行（もう一台のPCで使用）
 * - REVERSE_ORDER=true を設定すると、企業IDを大きい順から処理します
 *   例: REVERSE_ORDER=true npx tsx scripts/scrape_extended_fields.ts
 * - 通常実行（小さい順）と逆順実行（大きい順）を同時に実行することで、処理時間を短縮できます
 *
 * ✅ 並列処理について
 * - デフォルトで3並列処理を実行（精度を保つため）
 * - ログインが必要なサイト（企業INDEXナビ、バフェットコード）は共有ブラウザインスタンスを使用
 * - 各ワーカーで独立したページを使用して競合を回避
 * - レート制限を適切に設定してIPブロックを防止
 *
 * ✅ 対応Webサイト
 * - 企業INDEXナビ（ログイン必要）
 * - バフェットコード（ログイン必要）
 * - マイナビ転職
 * - マイナビ2026
 * - キャリタス就活
 * - 全国法人リスト
 * - 官報決算データベース
 * - 日本食糧新聞（食品関連企業）
 * - 日経コンパス
 * - 就活会議
 * - Alarmbox
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";
import { chromium, Browser, Page, ElementHandle } from "playwright";
import * as cheerio from "cheerio";
import { parse } from "csv-parse/sync";
import { parse as parseStream } from "csv-parse";
import { createReadStream } from "fs";
import fetch from "node-fetch";
import { Pool, Client } from "pg";

// ------------------------------
// Firebase Admin SDK 初期化（オプショナル - CloudSQLのみの場合は不要）
// ------------------------------
let db: admin.firestore.Firestore | null = null;

if (!admin.apps.length) {
  const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
  
  if (serviceAccountPath && fs.existsSync(serviceAccountPath)) {
    try {
      const serviceAccount = JSON.parse(
        fs.readFileSync(serviceAccountPath, "utf8")
      );

      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        projectId: "albert-ma",
      });

      db = admin.firestore();
      console.log("[Firebase初期化] ✅ 初期化が完了しました（オプショナル）");
    } catch (error) {
      console.log("[Firebase初期化] ⚠️  Firestore初期化をスキップします（CloudSQLのみ使用）");
    }
  } else {
    console.log("[Firebase初期化] ⚠️  Firestore初期化をスキップします（CloudSQLのみ使用）");
  }
} else {
  db = admin.firestore();
}

// ------------------------------
// CloudSQL (PostgreSQL) 接続設定
// ------------------------------
let pgPool: Pool | null = null;

function initPostgres(): Pool | null {
  const postgresHost = process.env.POSTGRES_HOST;
  const postgresPort = process.env.POSTGRES_PORT || "5432";
  // デフォルトはpostgresデータベース（companies_dbが存在しない場合に備える）
  const postgresDb = process.env.POSTGRES_DB || "postgres";
  const postgresUser = process.env.POSTGRES_USER || "postgres";
  const postgresPassword = process.env.POSTGRES_PASSWORD;

  // CloudSQL接続情報が設定されていない場合はエラー
  if (!postgresHost || !postgresPassword) {
    console.error("❌ エラー: CloudSQL接続情報が設定されていません。");
    console.error("  以下の環境変数を設定してください:");
    console.error("  - POSTGRES_HOST");
    console.error("  - POSTGRES_PASSWORD");
    process.exit(1);
  }

  try {
    const pool = new Pool({
      host: postgresHost,
      port: parseInt(postgresPort, 10),
      database: postgresDb,
      user: postgresUser,
      password: postgresPassword,
      max: 5, // 接続プールの最大接続数
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    });

    console.log(`[CloudSQL] ✅ PostgreSQL接続プールを初期化しました: ${postgresHost}:${postgresPort}/${postgresDb}`);
    return pool;
  } catch (error) {
    console.error(`❌ CloudSQL接続エラー: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  }
}

// ------------------------------
// 設定
// ------------------------------
// 高速化モード（環境変数 FAST_MODE=true で有効化）
const FAST_MODE = process.env.FAST_MODE === "true";

// リクエスト間隔（ミリ秒）- 環境変数で調整可能
// 精度を保ちながら最大限高速化: 最小200msを確保（精度を保つ最小値）
const SLEEP_MS = FAST_MODE 
  ? Math.max(parseInt(process.env.SLEEP_MS || "200", 10), 200) // 高速化モード: 200ms（精度を保つ最小値）
  : parseInt(process.env.SLEEP_MS || "500", 10); // 通常: 500ms

// ページ読み込み待機方法 - "domcontentloaded" (高速) または "networkidle" (確実)
// 精度を保つため、domcontentloaded + 短い待機時間を推奨
const PAGE_WAIT_MODE = (process.env.PAGE_WAIT_MODE || "domcontentloaded") as "domcontentloaded" | "networkidle";

// タイムアウト時間（ミリ秒）- 環境変数で調整可能
// 精度を保ちながら最大限高速化: 最小10秒を確保（精度を保つ最小値）
const PAGE_TIMEOUT = FAST_MODE
  ? parseInt(process.env.PAGE_TIMEOUT || "10000", 10) // 高速化モード: 10秒（精度を保つ最小値）
  : parseInt(process.env.PAGE_TIMEOUT || "15000", 10); // 通常: 15秒

const NAVIGATION_TIMEOUT = FAST_MODE
  ? parseInt(process.env.NAVIGATION_TIMEOUT || "12000", 10) // 高速化モード: 12秒（精度を保つ最小値）
  : parseInt(process.env.NAVIGATION_TIMEOUT || "20000", 10); // 通常: 20秒

// 並列処理数 - 環境変数で調整可能
// 精度を保ちながら最大限高速化: 8並列まで増やす（精度を保つ最大値）
const PARALLEL_WORKERS = FAST_MODE
  ? parseInt(process.env.PARALLEL_WORKERS || "8", 10) // 高速化モード: 8並列（精度を保つ最大値）
  : parseInt(process.env.PARALLEL_WORKERS || "3", 10); // 通常: 3並列

// 最小待機時間（高速化モード時は下げるが、精度を保つため最小値を確保）
const MIN_SLEEP_MS = FAST_MODE ? 200 : 500; // 高速化モード: 200ms（精度を保つ最小値）, 通常: 500ms
const MIN_SLEEP_MS_LONG = FAST_MODE ? 500 : 1000; // 高速化モード: 500ms（精度を保つ最小値）, 通常: 1000ms

// ログファイルの設定
const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
const logDir = path.join(process.cwd(), "logs");
const logFilePath = path.join(logDir, `scrape_extended_fields_${timestamp}.log`);
const csvFilePath = path.join(logDir, `scrape_extended_fields_${timestamp}.csv`);

// ログディレクトリが存在しない場合は作成
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

// CSVファイルのヘッダーを書き込み
const csvHeaders = [
  "companyId",
  "companyName",
  "scrapedFields",
  "fieldCount",
  "status",
  "timestamp",
  "errorMessage"
];
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
  scrapedFields: string[];
  status: "success" | "failed" | "no_data";
  errorMessage?: string;
}) {
  const row = [
    data.companyId,
    `"${data.companyName.replace(/"/g, '""')}"`,
    `"${data.scrapedFields.join("; ").replace(/"/g, '""')}"`,
    data.scrapedFields.length.toString(),
    data.status,
    new Date().toISOString(),
    data.errorMessage ? `"${data.errorMessage.replace(/"/g, '""')}"` : ""
  ];
  fs.appendFileSync(csvFilePath, row.join(",") + "\n", { encoding: "utf8" });
}

// ログイン情報
const CNAVI_EMAIL = "h.shiroyama@legatuscorp.com";
const CNAVI_PASSWORD = "Furapote0403/";
const BUFFETT_EMAIL = "h.shiroyama@legatuscorp.com";
const BUFFETT_PASSWORD = "furapote0403";

// ------------------------------
// ユーティリティ関数
// ------------------------------
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * 業種として正常な値かどうかを検証
 */
function isValidIndustry(industry: string | null | undefined): boolean {
  if (!industry || typeof industry !== "string") {
    return false;
  }

  const trimmed = industry.trim();
  
  // 空文字列は無効
  if (trimmed.length === 0) {
    return false;
  }

  // 長すぎる文字列は無効（業種名として不自然）
  if (trimmed.length > 100) {
    return false;
  }

  // JavaScriptコードが含まれている場合は無効
  const jsKeywords = [
    "function", "odtag", "window", "script", "document", "getElementById",
    "addEventListener", "querySelector", "innerHTML", "createElement",
    "setAttribute", "appendChild", "onclick", "onload", "onerror",
    "eval", "setTimeout", "setInterval", "console.log", "var ", "let ", "const ",
    "twq", "fbq", "facebook.com", "twitter.com", "ads-twitter.com",
    "fbevents.js", "uwt.js", "PageView", "noscript"
  ];
  
  const lowerIndustry = trimmed.toLowerCase();
  for (const keyword of jsKeywords) {
    if (lowerIndustry.includes(keyword.toLowerCase())) {
      return false;
    }
  }

  // HTMLタグが含まれている場合は無効
  if (/<[^>]+>/.test(trimmed)) {
    return false;
  }

  // URLが含まれている場合は無効
  if (/https?:\/\//.test(trimmed)) {
    return false;
  }

  // 特殊文字が多すぎる場合は無効（業種名として不自然）
  const specialCharCount = (trimmed.match(/[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]/g) || []).length;
  if (specialCharCount > trimmed.length * 0.3) {
    return false;
  }

  // 数字のみの場合は無効
  if (/^\d+$/.test(trimmed)) {
    return false;
  }

  // 日本語文字（ひらがな、カタカナ、漢字）が含まれていない場合は無効
  // （ただし、英語の業種名も許可するため、このチェックは緩和）
  // 日本語または英語の文字が含まれていることを確認
  if (!/[一-龠ひ-ゖァ-ヶa-zA-Z]/.test(trimmed)) {
    return false;
  }

  // 業種として一般的な形式をチェック
  // 「で検索」「すべて選択」などのUIテキストが含まれている場合は無効
  const invalidPatterns = [
    /で検索/i,
    /すべて選択/i,
    /選択解除/i,
    /主力事業/i,
    /メーカー/i,
    /限定/i,
    /検索/i,
    /選択/i,
    /解除/i,
  ];
  
  for (const pattern of invalidPatterns) {
    if (pattern.test(trimmed) && trimmed.length > 20) {
      // 短い業種名（例：「メーカー」）は許可するが、長いUIテキストは除外
      return false;
    }
  }

  return true;
}

/**
 * 業種を正規化（前後の空白を削除、不正な文字を除去）
 */
function normalizeIndustry(industry: string): string {
  let normalized = industry.trim();
  
  // 前後の特殊文字を削除
  normalized = normalized.replace(/^[、,;；\s]+|[、,;；\s]+$/g, "");
  
  // 連続する空白を1つに
  normalized = normalized.replace(/\s+/g, " ");
  
  return normalized;
}

/**
 * 電話番号が正常な値かどうかを検証
 */
function isValidPhoneNumber(phone: string | null | undefined): boolean {
  if (!phone || typeof phone !== "string") {
    return false;
  }

  const trimmed = phone.trim();
  
  // 空文字列は無効
  if (trimmed.length === 0) {
    return false;
  }

  // 電話番号は数字とハイフン、括弧のみ
  if (!/^[0-9\-()]+$/.test(trimmed)) {
    return false;
  }

  // 数字のみの場合、適切な長さか（10-15桁）
  const digitsOnly = trimmed.replace(/[^\d]/g, "");
  if (digitsOnly.length < 10 || digitsOnly.length > 15) {
    return false;
  }

  // JavaScriptコードや特殊な文字列が含まれていないか
  const invalidPatterns = ["function", "script", "eval", "window", "document"];
  const lowerPhone = trimmed.toLowerCase();
  for (const pattern of invalidPatterns) {
    if (lowerPhone.includes(pattern)) {
      return false;
    }
  }

  return true;
}

/**
 * FAXが正常な値かどうかを検証（電話番号と同じ検証）
 */
function isValidFax(fax: string | null | undefined): boolean {
  return isValidPhoneNumber(fax);
}

/**
 * メールアドレスが正常な値かどうかを検証
 */
function isValidEmail(email: string | null | undefined): boolean {
  if (!email || typeof email !== "string") {
    return false;
  }

  const trimmed = email.trim();
  
  // 空文字列は無効
  if (trimmed.length === 0) {
    return false;
  }

  // 基本的なメールアドレス形式チェック
  const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
  if (!emailRegex.test(trimmed)) {
    return false;
  }

  // 長すぎる場合は無効
  if (trimmed.length > 255) {
    return false;
  }

  // JavaScriptコードが含まれていないか
  const invalidPatterns = ["function", "script", "eval", "window", "document", "<script"];
  const lowerEmail = trimmed.toLowerCase();
  for (const pattern of invalidPatterns) {
    if (lowerEmail.includes(pattern)) {
      return false;
    }
  }

  return true;
}

/**
 * URLが正常な値かどうかを検証
 */
function isValidUrl(url: string | null | undefined): boolean {
  if (!url || typeof url !== "string") {
    return false;
  }

  const trimmed = url.trim();
  
  // 空文字列は無効
  if (trimmed.length === 0) {
    return false;
  }

  try {
    const urlObj = new URL(trimmed);
    // httpまたはhttpsスキームのみ許可
    if (urlObj.protocol !== "http:" && urlObj.protocol !== "https:") {
      return false;
    }
    return true;
  } catch {
    // URLとして解析できない場合は無効
    return false;
  }
}

// ------------------------------
// 保存直前のクリーニング（URL/住所）
// ------------------------------
/**
 * "http" から始まるURL部分だけを抽出する（日本語などの混入テキストを除外）
 * 例: "http://example.com電話番号-設立..." -> "http://example.com"
 */
const HTTP_URL_EXTRACT_REGEX =
  /https?:\/\/[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+/;

function extractFirstHttpUrl(input: string): string | null {
  const m = input.match(HTTP_URL_EXTRACT_REGEX);
  return m ? m[0] : null;
}

function cleanUrlBeforeSave(url: string | null | undefined): string | null {
  if (!url || typeof url !== "string") return null;
  const trimmed = url.trim();
  if (trimmed.length === 0) return null;

  const extracted = extractFirstHttpUrl(trimmed);
  return extracted || trimmed;
}

/**
 * 住所から「/地図」「Googleマップで表示」等の不要文言を削除
 */
function cleanAddressBeforeSave(address: string | null | undefined): string | null {
  if (!address || typeof address !== "string") return null;
  let s = address.replace(/\s+/g, " ").trim();
  if (s.length === 0) return null;

  // 代表的な混入パターンを末尾から除去（住所 + 余計な案内文が連結されるケースに対応）
  const trailingCutPatterns: RegExp[] = [
    /\/地図.*$/u,
    /Google\s*マップで表示.*$/u,
    /Google\s*マップ.*$/u,
    /Google\s*Maps?.*$/iu,
    /地図を表示.*$/u,
    /地図を見る.*$/u,
  ];
  for (const p of trailingCutPatterns) {
    s = s.replace(p, "");
  }

  // 単独で混入することがある文言も削除
  s = s
    .replace(/Google\s*マップで表示/gu, "")
    .replace(/Google\s*マップ/gu, "")
    .replace(/\/地図/gu, "")
    .trim();

  // 余計な空白を再度整理
  s = s.replace(/\s+/g, " ").trim();
  return s.length === 0 ? null : s;
}

function cleanUrlArrayBeforeSave(urls: unknown): string[] | null {
  if (!Array.isArray(urls)) return null;
  const cleaned = urls
    .map((u) => (typeof u === "string" ? cleanUrlBeforeSave(u) : null))
    .filter((u): u is string => !!u && u.trim().length > 0);
  return cleaned.length > 0 ? Array.from(new Set(cleaned)) : null;
}

/**
 * 保存処理の直前に、URL/住所系フィールドをクリーニングして返す
 */
function sanitizeScrapedDataForSave(scrapedData: Partial<ScrapedData>): Partial<ScrapedData> {
  const data: Partial<ScrapedData> = { ...scrapedData };

  // URL類
  if (data.companyUrl) data.companyUrl = cleanUrlBeforeSave(data.companyUrl) || data.companyUrl;
  if (data.contactFormUrl) data.contactFormUrl = cleanUrlBeforeSave(data.contactFormUrl) || data.contactFormUrl;
  if ((data as any).profileUrl) (data as any).profileUrl = cleanUrlBeforeSave((data as any).profileUrl) || (data as any).profileUrl;

  // SNS/urls配列（存在する場合）
  const cleanedUrls = cleanUrlArrayBeforeSave((data as any).urls);
  if (cleanedUrls) (data as any).urls = cleanedUrls;
  const cleanedSns = cleanUrlArrayBeforeSave((data as any).sns);
  if (cleanedSns) (data as any).sns = cleanedSns;

  // 住所類
  if (data.address) data.address = cleanAddressBeforeSave(data.address) || data.address;
  if (data.headquartersAddress) data.headquartersAddress = cleanAddressBeforeSave(data.headquartersAddress) || data.headquartersAddress;
  if (data.representativeHomeAddress) data.representativeHomeAddress = cleanAddressBeforeSave(data.representativeHomeAddress) || data.representativeHomeAddress;
  if (data.representativeRegisteredAddress) data.representativeRegisteredAddress = cleanAddressBeforeSave(data.representativeRegisteredAddress) || data.representativeRegisteredAddress;

  return data;
}

/**
 * 生年月日が正常な値かどうかを検証
 */
function isValidBirthDate(date: string | null | undefined): boolean {
  if (!date || typeof date !== "string") {
    return false;
  }

  const trimmed = date.trim();
  
  // 空文字列は無効
  if (trimmed.length === 0) {
    return false;
  }

  // 日付形式のパターン（YYYY-MM-DD, YYYY/MM/DD, YYYY年MM月DD日など）
  const datePatterns = [
    /^\d{4}[-\/]\d{1,2}[-\/]\d{1,2}$/, // YYYY-MM-DD or YYYY/MM/DD
    /^\d{4}年\d{1,2}月\d{1,2}日?$/, // YYYY年MM月DD日
  ];

  let isValidFormat = false;
  for (const pattern of datePatterns) {
    if (pattern.test(trimmed)) {
      isValidFormat = true;
      break;
    }
  }

  if (!isValidFormat) {
    return false;
  }

  // 実際の日付として有効かチェック
  try {
    const normalized = trimmed.replace(/年/g, "-").replace(/月/g, "-").replace(/日/g, "");
    const dateObj = new Date(normalized);
    if (isNaN(dateObj.getTime())) {
      return false;
    }
    
    // 1900年から2100年の範囲内か
    const year = dateObj.getFullYear();
    if (year < 1900 || year > 2100) {
      return false;
    }
    
    return true;
  } catch {
    return false;
  }
}

/**
 * 数値が正常な値かどうかを検証
 */
function isValidNumber(value: number | null | undefined): boolean {
  if (value === null || value === undefined) {
    return false;
  }

  if (typeof value !== "number") {
    return false;
  }

  // NaNや無限大は無効
  if (isNaN(value) || !isFinite(value)) {
    return false;
  }

  // 負の数値も許可（損益などで必要）
  return true;
}

/**
 * 企業名（取引先）が正常な値かどうかを検証
 */
function isValidClientName(clientName: string | null | undefined): boolean {
  if (!clientName || typeof clientName !== "string") {
    return false;
  }

  const trimmed = clientName.trim();
  
  // 空文字列は無効
  if (trimmed.length === 0) {
    return false;
  }

  // 長すぎる場合は無効（企業名として不自然）
  if (trimmed.length > 100) {
    return false;
  }

  // 企業名らしくない文字列を除外
  const invalidPatterns = [
    /^(の業務|生産性|コスト|など|業務プロセス|大幅|効率化|管理システム|登録企業数|社以上|で解決|お困り対応|サポート|詳しく見る|最新ニュース|年末年始|休業|お知らせ|夏季休業|期間|ゴールデンウィーク|新年|ご挨拶|ニュース一覧|HOME|とは|事業内容|会社案内|採用情報|お問合せ|資料請求|PAGE TOP|Copyright|All Rights Reserved|jQuery|document|ready|function|window|scroll|index_box_content|contentPosition|offset|top|windowHeight)/i,
    /^(http|https|www\.|\.co\.jp|\.com|\.jp)/i, // URLを含む
    /(jQuery|function|var|document|window|offset|top|height)/i, // JavaScriptコードを含む
    /(登録企業数|社以上|で解決|お困り対応|サポート)/i, // 説明文を含む
    /(詳しく見る|最新ニュース|年末年始|休業|お知らせ)/i, // ナビゲーション要素を含む
    /(HOME|とは|事業内容|会社案内|採用情報|お問合せ|資料請求|PAGE TOP)/i, // ナビゲーション要素を含む
    /(Copyright|All Rights Reserved)/i, // コピーライトを含む
  ];

  for (const pattern of invalidPatterns) {
    if (pattern.test(trimmed)) {
      return false;
    }
  }

  // 企業名らしい形式（株式会社、有限会社、合同会社など）を含むか、または適切な長さの文字列
  const companyPatterns = [
    /(株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|一般財団法人|特定非営利活動法人|医療法人|学校法人|宗教法人|社会福祉法人|協同組合|協業組合|企業組合|協同組合連合会)/,
    /^[ぁ-んァ-ヶ一-龠々ー]+$/, // 日本語のみ（企業名の可能性）
  ];

  // 企業名らしい形式を含む、または適切な長さ（2文字以上50文字以下）の日本語文字列
  const hasCompanyPattern = companyPatterns.some(pattern => pattern.test(trimmed));
  const isValidLength = trimmed.length >= 2 && trimmed.length <= 50;
  const isJapanese = /^[ぁ-んァ-ヶ一-龠々ー\s]+$/.test(trimmed);

  return (hasCompanyPattern || (isValidLength && isJapanese)) && !trimmed.includes('—') && !trimmed.includes('ー');
}

/**
 * 役員名（氏名）が正常な値かどうかを検証
 */
function isValidExecutiveName(executiveName: string | null | undefined): boolean {
  if (!executiveName || typeof executiveName !== "string") {
    return false;
  }

  let trimmed = executiveName.trim();
  
  // 空文字列は無効
  if (trimmed.length === 0) {
    return false;
  }

  // "代表者"、"代表取締役"などの接頭辞を除去
  trimmed = trimmed.replace(/^(代表者|代表取締役|社長|取締役|監査役|執行役員|役員)[：:\s]*/i, "").trim();

  // 接頭辞除去後も空文字列の場合は無効
  if (trimmed.length === 0) {
    return false;
  }

  // 長すぎる場合は無効（氏名として不自然）
  if (trimmed.length > 50) {
    return false;
  }

  // 短すぎる場合も無効（氏名として不自然）
  if (trimmed.length < 2) {
    return false;
  }

  // 氏名らしくない文字列を除外
  const invalidPatterns = [
    /^(代表者|代表取締役|社長|取締役|監査役|執行役員|役員)/i, // 接頭辞が残っている
    /(http|https|www\.|\.co\.jp|\.com|\.jp)/i, // URLを含む
    /(jQuery|function|var|document|window|offset|top|height)/i, // JavaScriptコードを含む
    /(登録企業数|社以上|で解決|お困り対応|サポート)/i, // 説明文を含む
    /(詳しく見る|最新ニュース|年末年始|休業|お知らせ)/i, // ナビゲーション要素を含む
    /(HOME|とは|事業内容|会社案内|採用情報|お問合せ|資料請求|PAGE TOP)/i, // ナビゲーション要素を含む
    /(Copyright|All Rights Reserved)/i, // コピーライトを含む
    /[0-9]/, // 数字を含む（氏名として不自然）
    /[a-zA-Z]{3,}/, // 3文字以上の英字を含む（氏名として不自然）
  ];

  for (const pattern of invalidPatterns) {
    if (pattern.test(trimmed)) {
      return false;
    }
  }

  // 氏名らしい形式（漢字、ひらがな、カタカナのみ）
  const namePattern = /^[ぁ-んァ-ヶ一-龠々ー]+$/;
  if (!namePattern.test(trimmed)) {
    return false;
  }

  return true;
}

/**
 * 金額文字列を千円単位の数値に正規化
 * 例: "1億円" -> 100000, "500万円" -> 5000, "1,000,000円" -> 1000
 */
function normalizeToThousandYen(value: string, unit: string): number | null {
  if (!value || typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim().replace(/,/g, "");
  
  // 億、万、千の単位を処理
  let result = 0;
  let currentValue = 0;
  
  // 億の処理
  const okuMatch = trimmed.match(/([\d.]+)億/);
  if (okuMatch) {
    result += parseFloat(okuMatch[1]) * 100000; // 億 = 100000千円
  }
  
  // 万の処理
  const manMatch = trimmed.match(/([\d.]+)万/);
  if (manMatch) {
    result += parseFloat(manMatch[1]) * 10; // 万 = 10千円
  }
  
  // 千の処理
  const senMatch = trimmed.match(/([\d.]+)千/);
  if (senMatch) {
    result += parseFloat(senMatch[1]); // 千 = 1千円
  }
  
  // 数値のみの場合（円単位と仮定）
  if (result === 0) {
    const numMatch = trimmed.match(/([\d.]+)/);
    if (numMatch) {
      const num = parseFloat(numMatch[1]);
      // 数値が大きい場合は億単位、中程度は万単位、小さい場合は千円単位と仮定
      if (num >= 100000000) {
        result = num / 1000; // 億円単位を千円に変換
      } else if (num >= 10000) {
        result = num / 1000; // 万円単位を千円に変換
      } else {
        result = num; // 既に千円単位
      }
    }
  }
  
  if (result > 0 && isFinite(result)) {
    return Math.round(result);
  }
  
  return null;
}

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

// ------------------------------
// ログイン処理
// ------------------------------

/**
 * 企業INDEXナビにログイン
 */
async function loginToCnavi(page: Page): Promise<boolean> {
  try {
    writeLog("[Cnavi] ログイン開始...");
    
    // 企業INDEXナビのトップページにアクセス
    await page.goto("https://cnavi-app.g-search.or.jp/", {
      waitUntil: "networkidle",
      timeout: PAGE_TIMEOUT * 2, // ログインページの読み込みに時間がかかる場合があるため
    });

    // ログインページにリダイレクトされるまで待機（より長く待つ）
    await sleep(Math.max(SLEEP_MS * 5, MIN_SLEEP_MS_LONG * 3)); // ログインページの読み込みを待つ
    
    // 現在のURLを確認
    const currentUrl = page.url();
    writeLog(`[Cnavi] 現在のURL: ${currentUrl}`);
    
    // ログインページにリダイレクトされていない場合、ログインページを探す
    if (!currentUrl.includes("login.gh.g-search.or.jp") && !currentUrl.includes("login")) {
      writeLog(`[Cnavi] ログインページへのリダイレクト待機中...`);
      // ログインボタンやリンクを探してクリック
      try {
        const loginLink = await page.$('a[href*="login"], a:has-text("ログイン"), button:has-text("ログイン")');
        if (loginLink) {
          await loginLink.click({ force: true }).catch(() => {});
          await page.waitForNavigation({ waitUntil: "networkidle", timeout: NAVIGATION_TIMEOUT * 2 });
          await sleep(Math.max(SLEEP_MS * 3, MIN_SLEEP_MS_LONG * 2)); // 最小待機時間（高速化モード対応）
        }
      } catch (e) {
        // ログインリンクが見つからない場合は続行
        writeLog(`[Cnavi] ログインリンクが見つかりませんでした: ${(e as any)?.message}`);
      }
    }

    // 再度URLを確認
    const cnaviFinalUrl = page.url();
    writeLog(`[Cnavi] 最終URL: ${cnaviFinalUrl}`);

    // すべてのフレーム（iframe含む）を確認
    const frames = page.frames();
    let targetFrame = page.mainFrame();
    
    // iframe内にログインフォームがある可能性があるため、すべてのフレームを確認
    for (const frame of frames) {
      try {
        const emailInFrame = await frame.$('input[name="username"], input[type="email"], input[type="text"]');
        if (emailInFrame) {
          targetFrame = frame;
          writeLog(`[Cnavi] ログインフォームをiframe内で発見`);
          break;
        }
      } catch (e) {
        continue;
      }
    }

    // メールアドレス入力フィールドを探す（より広範囲のセレクターを使用）
    const emailSelectors = [
      'input[name="username"]',
      'input[type="email"]',
      'input[type="text"]', // より一般的なセレクター
      'input[name*="email"]',
      'input[name*="mail"]',
      'input[name*="Email"]',
      'input[name*="Mail"]',
      'input[name*="username"]',
      'input[id*="email"]',
      'input[id*="mail"]',
      'input[id*="username"]',
      'input[placeholder*="メール"]',
      'input[placeholder*="email"]',
    ];

    let emailInput: ElementHandle | null = null;
    for (const selector of emailSelectors) {
      try {
        // まずターゲットフレームで探す
        emailInput = await targetFrame.$(selector);
        if (!emailInput) {
          // ターゲットフレームで見つからない場合は、すべてのフレームで探す
          for (const frame of frames) {
            try {
              emailInput = await frame.$(selector);
              if (emailInput) {
                const isVisible = await emailInput.isVisible().catch(() => false);
                if (isVisible) {
                  targetFrame = frame;
                  break;
                }
              }
            } catch (e) {
              continue;
            }
          }
        } else {
          // 見つかった要素が表示されているか確認
          const isVisible = await emailInput.isVisible().catch(() => false);
          if (!isVisible) {
            emailInput = null;
            continue;
          }
        }
        
        if (emailInput) {
          // 要素が表示されるまで待つ
          await sleep(500); // より長い待機時間
          // 既に値が入っている場合はクリアしてから入力
          await emailInput.click({ clickCount: 3, force: true }).catch(() => {}); // 全選択
          await emailInput.fill(CNAVI_EMAIL);
          writeLog(`[Cnavi] メールアドレス入力成功: ${selector}`);
          break;
        }
      } catch (e) {
        continue;
      }
    }

    if (!emailInput) {
      writeLog("[Cnavi] メールアドレス入力フィールドが見つかりません");
      // ページのHTMLを一部取得してデバッグ情報を出力
      try {
        const pageContent = await page.content();
        const inputCount = (pageContent.match(/<input/gi) || []).length;
        writeLog(`[Cnavi] ページ内のinput要素数: ${inputCount}`);
        await page.screenshot({ path: 'cnavi_login_debug_email.png', fullPage: true });
        writeLog(`[Cnavi] スクリーンショットを保存: cnavi_login_debug_email.png`);
      } catch (e) {
        writeLog(`[Cnavi] デバッグ情報の取得に失敗: ${(e as any)?.message}`);
      }
      // ログイン失敗しても処理は続行する
      writeLog("[Cnavi] ⚠️  ログイン失敗しましたが、処理を続行します");
      return false;
    }

    // パスワード入力フィールドを探す（name="password"が実際の属性）
    const passwordSelectors = [
      'input[name="password"]', // 実際の属性
      'input[type="password"]',
      'input[name*="password"]',
      'input[name*="Password"]',
      'input[id*="password"]',
      'input[id*="Password"]',
    ];

    let passwordInput: ElementHandle | null = null;
    for (const selector of passwordSelectors) {
      try {
        // まずターゲットフレームで探す
        passwordInput = await targetFrame.$(selector);
        if (!passwordInput) {
          // ターゲットフレームで見つからない場合は、すべてのフレームで探す
          for (const frame of frames) {
            try {
              passwordInput = await frame.$(selector);
              if (passwordInput) {
                targetFrame = frame;
                break;
              }
            } catch (e) {
              continue;
            }
          }
        }
        
        if (passwordInput) {
          // 要素が表示されるまで待つ（要素が見つかった時点で表示されていると仮定）
          // 必要に応じて短い待機時間を追加
          await sleep(100);
          await passwordInput.fill(CNAVI_PASSWORD);
          writeLog(`[Cnavi] パスワード入力成功: ${selector}`);
          break;
        }
      } catch (e) {
        continue;
      }
    }

    if (!passwordInput) {
      writeLog("[Cnavi] パスワード入力フィールドが見つかりません");
      await page.screenshot({ path: 'cnavi_login_debug_password.png', fullPage: true });
      writeLog("[Cnavi] ⚠️  ログイン失敗しましたが、処理を続行します");
      return false;
    }

    // ログインボタンをクリック（name="ログイン"が実際の属性）
    const loginButtonSelectors = [
      'button[name="ログイン"]', // 実際の属性
      'button:has-text("ログイン")',
      'button[type="submit"]',
      'button.btn-primary',
      'input[type="submit"]',
    ];

    let loginButton: ElementHandle | null = null;
    for (const selector of loginButtonSelectors) {
      try {
        // まずターゲットフレームで探す
        loginButton = await targetFrame.$(selector);
        if (!loginButton) {
          // ターゲットフレームで見つからない場合は、すべてのフレームで探す
          for (const frame of frames) {
            try {
              loginButton = await frame.$(selector);
              if (loginButton) {
                targetFrame = frame;
                break;
              }
            } catch (e) {
              continue;
            }
          }
        }
        
        if (loginButton) {
          // 要素が表示されるまで待つ（要素が見つかった時点で表示されていると仮定）
          // 必要に応じて短い待機時間を追加
          await sleep(100);
          await loginButton.click({ force: true }).catch(() => {});
          writeLog(`[Cnavi] ログインボタンクリック成功: ${selector}`);
          break;
        }
      } catch (e) {
        continue;
      }
    }

    if (!loginButton) {
      writeLog("[Cnavi] ログインボタンが見つからないため、Enterキーを試行");
      try {
        await passwordInput.press("Enter");
      } catch (e) {
        writeLog(`[Cnavi] Enterキーの送信に失敗: ${(e as any)?.message}`);
      }
    }

    // ログイン完了を待つ（リダイレクトまたはページ遷移を待機）
    try {
      await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT });
    } catch (e) {
      // ナビゲーション待機がタイムアウトした場合は続行
      writeLog("[Cnavi] ナビゲーション待機タイムアウト、続行します");
    }
    
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）

    // ログイン成功を確認（URLや要素で判定）
    const cnaviLoginFinalUrl = page.url();
    const pageContent = await page.content();
    
    writeLog(`[Cnavi] ログイン後のURL: ${cnaviLoginFinalUrl}`);
    
    // ログイン成功の判定（ログインページ以外に遷移した、またはcnavi-appのページにいる）
    const isLoggedIn = !cnaviLoginFinalUrl.includes("login.gh.g-search.or.jp") && 
                       (cnaviLoginFinalUrl.includes("cnavi-app.g-search.or.jp") || 
                        !pageContent.includes("メールアドレスとパスワードを入力してください"));

    if (isLoggedIn) {
      writeLog(`[Cnavi] ログイン成功: ${cnaviLoginFinalUrl}`);
      return true;
    } else {
      writeLog(`[Cnavi] ログイン失敗: ${cnaviLoginFinalUrl}`);
      await page.screenshot({ path: 'cnavi_login_failed.png' });
      return false;
    }
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    writeLog(`[Cnavi] ログインエラー: ${errorMsg}`);
    await page.screenshot({ path: 'cnavi_login_error.png' }).catch(() => {});
    return false;
  }
}

/**
 * 企業INDEXナビから情報を取得（法人番号がある場合は直接アクセス）
 */
async function scrapeFromCnavi(
  page: Page,
  companyName: string,
  headquartersAddress: string,
  nullFields: string[],
  corporateNumber?: string | null
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // 法人番号がある場合は直接企業詳細ページにアクセス
    if (corporateNumber) {
      const detailUrl = `https://cnavi-app.g-search.or.jp/corporatedetail?corporateNumber=${corporateNumber}`;
      writeLog(`  [Cnavi] 法人番号で直接アクセス: ${detailUrl}`);
      
      await page.goto(detailUrl, {
        waitUntil: PAGE_WAIT_MODE,
        timeout: PAGE_TIMEOUT,
      });
      await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG * 2)); // 詳細ページの読み込みを待つ
      
      // ページが存在するかチェック（404エラーなど）
      const pageText = await page.textContent("body");
      if (pageText && (pageText.includes("ページが見つかりません") || pageText.includes("404"))) {
        writeLog(`  [Cnavi] 企業詳細ページが見つかりません: ${companyName} (法人番号: ${corporateNumber})`);
        // 法人番号での直接アクセスが失敗した場合は検索にフォールバック
      } else {
        // 詳細ページから情報を取得
        return await scrapeFromCnaviDetailPage(page, companyName, nullFields);
      }
    }

    // 法人番号がない場合、または直接アクセスが失敗した場合は検索フローを使用
    // 企業INDEXナビの検索ページにアクセス（ログイン済みであることを前提）
    await page.goto("https://cnavi-app.g-search.or.jp/", {
      waitUntil: PAGE_WAIT_MODE,
      timeout: PAGE_TIMEOUT,
    });
    await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG * 2)); // ページ読み込みを待つ

    // ポップアップ・広告を閉じる（強化版）
    try {
      // モーダル、オーバーレイ、ポップアップを閉じる
      const popupSelectors = [
        'button:has-text("×")',
        'button[aria-label*="閉じる"]',
        'button[aria-label*="close"]',
        '.close',
        '[class*="close"]',
        'button.close',
        '[class*="ad-close"]',
        '[class*="modal-close"]',
        '[class*="popup-close"]',
        '[class*="overlay-close"]',
        'button[class*="close"]',
        '[id*="close"]',
        '[data-dismiss="modal"]',
        '[data-close]'
      ];
      
      for (const selector of popupSelectors) {
        try {
          await page.waitForSelector(selector, { timeout: 2000, state: 'visible' }).catch(() => {});
          const closeButtons = await page.$$(selector);
          for (const closeBtn of closeButtons) {
            try {
              const isVisible = await closeBtn.isVisible();
              if (isVisible) {
                await closeBtn.click({ timeout: 3000, force: true }).catch(() => {});
                await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300));
              }
            } catch {
              // 個別のボタンクリックエラーは無視
            }
          }
        } catch {
          // セレクターが見つからない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    // 「企業名で探す」のテキストボックスを探す（waitForSelectorで待機）
    let companyNameInput: ElementHandle | null = null;
    const searchSelectors = [
      'input[placeholder*="企業名"]',
      'input[placeholder*="法人格"]',
      'input[placeholder*="法人格の除いた企業名"]',
      'input[name*="company"]',
      'input[name*="q"]',
      'input[name*="keyword"]',
      'input[type="text"][class*="search"]',
      'input[type="text"]'
    ];
    
    for (const selector of searchSelectors) {
      try {
        // 要素が表示されるまで待機
        await page.waitForSelector(selector, { timeout: 5000, state: 'visible' }).catch(() => {});
        companyNameInput = await page.$(selector);
        if (companyNameInput) {
          const isVisible = await companyNameInput.isVisible();
          if (isVisible) {
            break;
          }
        }
      } catch {
        continue;
      }
    }

    if (companyNameInput) {
      // ポップアップ/広告で入力が阻害されることがあるため、forceクリック→入力
      await companyNameInput.click({ force: true, timeout: 3000 }).catch(() => {});
      try {
        await companyNameInput.fill(companyName);
      } catch {
        await page.keyboard.type(companyName, { delay: 10 }).catch(() => {});
      }
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
    } else {
      writeLog(`  [Cnavi] 検索ボックスが見つかりません: ${companyName}`);
      return data;
    }

    // 「検索する」ボタンを押下（waitForSelectorで待機）
    try {
      await page.waitForSelector('button:has-text("検索する"), button[type="submit"]', { timeout: 5000, state: 'visible' }).catch(() => {});
    } catch {
      // ボタンが見つからない場合は続行
    }
    
    const searchButton = await page.$('button:has-text("検索する"), button[type="submit"]');
    if (searchButton) {
      // クリックと同時に遷移するため、Promise.allで待機（遷移取りこぼし防止）
      await Promise.all([
        page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {}),
        searchButton.click({ force: true }).catch(() => {}),
      ]);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    }

    // 「指定された検索条件に一致する結果がありませんでした。」をチェック
    const pageText = await page.textContent("body");
    if (pageText && pageText.includes("指定された検索条件に一致する結果がありませんでした")) {
      writeLog(`  [Cnavi] 検索結果なし: ${companyName}`);
      return data;
    }

    // 企業リストから企業名+住所がマッチする「企業名」列を探してクリック
    const companyRows = await page.$$('tr, .company-row, [class*="company"]');
    let foundCompany = false;
    
    for (const row of companyRows) {
      const rowText = await row.textContent();
      if (rowText && rowText.includes(companyName)) {
        // 住所もチェック（あれば）
        if (!headquartersAddress || rowText.includes(headquartersAddress.substring(0, 5))) {
          const companyLink = await row.$('a, [href*="/company/"], [href*="/detail/"]');
          if (companyLink) {
            await Promise.all([
              page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {}),
              companyLink.click({ force: true }).catch(() => {}),
            ]);
            await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
            foundCompany = true;
            break;
          }
        }
      }
    }

    if (!foundCompany) {
      writeLog(`  [Cnavi] 企業が見つかりません: ${companyName}`);
      return data;
    }

    // 企業詳細画面から情報を取得
    return await scrapeFromCnaviDetailPage(page, companyName, nullFields);

  } catch (error) {
    writeLog(`  [Cnavi] エラー: ${(error as any)?.message}`);
  }

  return data;
}

/**
 * 企業INDEXナビの詳細ページから情報を取得
 */
async function scrapeFromCnaviDetailPage(
  page: Page,
  companyName: string,
  nullFields: string[]
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // ページ読み込みを待つ
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG));

    // 広告を閉じる（右上の×ボタンなど）
    try {
      const closeAdButtons = await page.$$('button:has-text("×"), button[aria-label*="閉じる"], .close, [class*="close"], button.close, [class*="ad-close"], [class*="modal-close"]');
      for (const closeBtn of closeAdButtons) {
        try {
          const isVisible = await closeBtn.isVisible();
          if (isVisible) {
            await closeBtn.click();
            await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300));
          }
        } catch {
          // 広告が存在しない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 「ホームページ」の「サイトを見る」リンクを取得
    if (nullFields.includes("companyUrl") && !data.companyUrl) {
      const homepageSection = $('*:contains("ホームページ")').closest('tr, div, td, th');
      if (homepageSection.length > 0) {
        const homepageLink = homepageSection.find('a[href^="http"], a[target="_blank"]').first();
        if (homepageLink.length > 0) {
          const href = homepageLink.attr('href');
          if (href && isValidUrl(href)) {
            data.companyUrl = href;
            writeLog(`  [Cnavi] ホームページ取得: ${href}`);
          }
        }
      }
      // 別のパターン: テーブル内の「ホームページ」行を探す
      if (!data.companyUrl) {
        const rows = $('tr, div');
        for (let i = 0; i < rows.length; i++) {
          const rowText = $(rows[i]).text();
          if (rowText.includes('ホームページ')) {
            const link = $(rows[i]).find('a[href^="http"]').first();
            if (link.length > 0) {
              const href = link.attr('href');
              if (href && isValidUrl(href)) {
                data.companyUrl = href;
                writeLog(`  [Cnavi] ホームページ取得: ${href}`);
                break;
              }
            }
          }
        }
      }
    }

    // 「問い合せページ」の「サイトを見る」リンクを取得
    if (nullFields.includes("contactFormUrl") && !data.contactFormUrl) {
      const inquirySection = $('*:contains("問い合せ"), *:contains("問い合わせ"), *:contains("お問い合わせ")').closest('tr, div, td, th');
      if (inquirySection.length > 0) {
        const inquiryLink = inquirySection.find('a[href^="http"], a[target="_blank"]').first();
        if (inquiryLink.length > 0) {
          const href = inquiryLink.attr('href');
          if (href && isValidUrl(href)) {
            data.contactFormUrl = href;
            writeLog(`  [Cnavi] 問い合せページ取得: ${href}`);
          }
        }
      }
      // 別のパターン: テーブル内の「問い合せ」行を探す
      if (!data.contactFormUrl) {
        const rows = $('tr, div');
        for (let i = 0; i < rows.length; i++) {
          const rowText = $(rows[i]).text();
          if (rowText.includes('問い合せ') || rowText.includes('問い合わせ') || rowText.includes('お問い合わせ')) {
            const link = $(rows[i]).find('a[href^="http"]').first();
            if (link.length > 0) {
              const href = link.attr('href');
              if (href && isValidUrl(href)) {
                data.contactFormUrl = href;
                writeLog(`  [Cnavi] 問い合せページ取得: ${href}`);
                break;
              }
            }
          }
        }
      }
    }

    // 電話番号
    if (nullFields.includes("phoneNumber") && !data.phoneNumber) {
      const phoneMatch = text.match(/(?:電話番号|電話|TEL)[：:\s]*([0-9-()]{10,15})/i);
      if (phoneMatch) {
        const phone = phoneMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
        if (isValidPhoneNumber(phone)) {
          data.phoneNumber = phone;
          writeLog(`  [Cnavi] 電話番号取得: ${phone}`);
        }
      }
    }

    // FAX
    if (nullFields.includes("fax") && !data.fax) {
      const faxMatch = text.match(/(?:FAX|Fax|fax|ファックス)[：:\s]*([0-9-()]{10,15})/i);
      if (faxMatch) {
        const fax = faxMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
        if (isValidFax(fax)) {
          data.fax = fax;
          writeLog(`  [Cnavi] FAX取得: ${fax}`);
        }
      }
    }

    // 代表者名
    if (nullFields.includes("representativeName") && !data.representativeName) {
      const repMatch = text.match(/(?:代表者|代表取締役|社長)[：:\s]*([^\n\r]{1,50})/i);
      if (repMatch) {
        const repName = repMatch[1].trim();
        if (repName && repName.length > 0 && repName.length < 50 && !repName.includes('—') && !repName.includes('ー')) {
          data.representativeName = repName;
          writeLog(`  [Cnavi] 代表者名取得: ${repName}`);
        }
      }
    }

    // 売上高
    if (nullFields.includes("revenue") && !data.revenue) {
      const revenueMatch = text.match(/(?:売上|売上高)[：:\s]*([\d,億万千]+円?)/i);
      if (revenueMatch) {
        const revenueValue = normalizeToThousandYen(revenueMatch[1], "");
        if (revenueValue !== null && isValidNumber(revenueValue)) {
          data.revenue = revenueValue;
          writeLog(`  [Cnavi] 売上高取得: ${revenueValue.toLocaleString()}千円`);
        }
      }
    }

    // 資本金
    if (nullFields.includes("capitalStock") && !data.capitalStock) {
      const capitalMatch = text.match(/(?:資本金)[：:\s]*([\d,億万千]+円?)/i);
      if (capitalMatch) {
        const capitalValue = normalizeToThousandYen(capitalMatch[1], "");
        if (capitalValue !== null && isValidNumber(capitalValue)) {
          data.capitalStock = capitalValue;
          writeLog(`  [Cnavi] 資本金取得: ${capitalValue.toLocaleString()}千円`);
        }
      }
    }

    // 従業員数
    if (nullFields.includes("employeeCount") && !data.employeeCount) {
      const employeeMatch = text.match(/(?:従業員数|従業員)[：:\s]*([\d,]+人?)/i);
      if (employeeMatch) {
        const employeeValue = parseInt(employeeMatch[1].replace(/[^\d]/g, ""), 10);
        if (!isNaN(employeeValue) && employeeValue > 0) {
          data.employeeCount = employeeValue;
          writeLog(`  [Cnavi] 従業員数取得: ${employeeValue}人`);
        }
      }
    }

    // 設立日
    if (nullFields.includes("established") && !data.established) {
      const establishedMatch = text.match(/(?:設立)[：:\s]*(\d{4}年\d{1,2}月\d{1,2}日?)/i);
      if (establishedMatch) {
        data.established = establishedMatch[1];
        writeLog(`  [Cnavi] 設立日取得: ${establishedMatch[1]}`);
      }
    }

    // 取引先（より厳密な抽出とバリデーション）
    if (nullFields.includes("clients") && !data.clients) {
      const clientsSection = $('*:contains("取引先")').closest('tr, div, td, th');
      if (clientsSection.length > 0) {
        const clientsText = clientsSection.text();
        const clientMatches = clientsText.match(/(?:取引先)[：:\s]*([^\n\r]{10,200})/i);
        if (clientMatches) {
          let clientText = clientMatches[1].trim();
          // 長すぎる場合は最初の部分のみ
          if (clientText.length > 200) {
            clientText = clientText.substring(0, 200);
          }
          const clientsList = clientText.split(/[、,;；\n\r]/).map(c => c.trim()).filter(c => c.length > 0);
          const validClients = clientsList.filter(c => isValidClientName(c));
          if (validClients.length > 0) {
            data.clients = [...new Set(validClients)].slice(0, 20);
            writeLog(`  [Cnavi] 取引先取得: ${validClients.length}件`);
          }
        }
      }
    }

    // 上場区分
    if (nullFields.includes("listing") && !data.listing) {
      const listingMatch = text.match(/(?:上場[・・]非上場)[：:\s]*([^\n\r]{1,20})/i);
      if (listingMatch) {
        const listing = listingMatch[1].trim();
        if (listing && !listing.includes('—')) {
          data.listing = listing;
          writeLog(`  [Cnavi] 上場区分取得: ${listing}`);
        }
      }
    }

    // 本社住所
    if (nullFields.includes("headquartersAddress") && !data.headquartersAddress) {
      const addressMatch = text.match(/(?:本社住所|本社)[：:\s]*([^\n\r]{10,100})/i);
      if (addressMatch) {
        const address = addressMatch[1].trim();
        if (address && address.length > 5 && address.length < 200 && !address.includes('—')) {
          data.headquartersAddress = address;
          writeLog(`  [Cnavi] 本社住所取得: ${address}`);
        }
      }
    }

  } catch (error) {
    writeLog(`  [Cnavi] 詳細ページ取得エラー: ${(error as any)?.message}`);
  }

  return data;
}

/**
 * バフェットコードにログイン
 */
async function loginToBuffett(page: Page): Promise<boolean> {
  try {
    writeLog("[Buffett] ログイン開始...");
    
    // バフェットコードのトップページにアクセス
    await page.goto("https://www.buffett-code.com/", {
      waitUntil: PAGE_WAIT_MODE,
      timeout: PAGE_TIMEOUT,
    });

    await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）

    const currentUrl = page.url();
    writeLog(`[Buffett] 現在のURL: ${currentUrl}`);

    // ログイン画面が表示されているか確認
    const isLoginPage = currentUrl.includes("login") || 
                        (await page.$('button:has-text("Googleでログイン"), button:has-text("Xでログイン"), input[type="email"]') !== null);
    
    if (isLoginPage) {
      writeLog("[Buffett] ログイン画面を検出、Googleログインを試行...");
      
      // Googleログインボタンを探す
      const googleLoginButton = await page.$('button:has-text("Googleでログイン"), button:has-text("G Googleでログイン"), a:has-text("Googleでログイン")');
      if (googleLoginButton) {
        await googleLoginButton.click();
        await sleep(Math.max(SLEEP_MS * 3, MIN_SLEEP_MS_LONG * 2));
        
        // Googleログインページでメールアドレスとパスワードを入力
        try {
          const googleEmailInput = await page.$('input[type="email"], input[name="identifier"]');
          if (googleEmailInput) {
            await googleEmailInput.fill(BUFFETT_EMAIL);
            await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS));
            
            const nextButton = await page.$('button:has-text("次へ"), button[id="identifierNext"]');
            if (nextButton) {
              await nextButton.click();
              await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG));
            }
            
            const googlePasswordInput = await page.$('input[type="password"], input[name="password"]');
            if (googlePasswordInput) {
              await googlePasswordInput.fill(BUFFETT_PASSWORD);
              await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS));
              
              const passwordNextButton = await page.$('button:has-text("次へ"), button[id="passwordNext"]');
              if (passwordNextButton) {
                await passwordNextButton.click();
                await sleep(Math.max(SLEEP_MS * 3, MIN_SLEEP_MS_LONG * 2));
              }
            }
          }
        } catch (e) {
          writeLog(`[Buffett] Googleログイン処理中にエラー: ${(e as any)?.message}`);
        }
      } else {
        // Googleログインボタンが見つからない場合は、通常のメール/パスワードログインを試行
        writeLog("[Buffett] Googleログインボタンが見つからないため、通常ログインを試行...");
        const emailInput = await page.$('input[type="email"], input[name*="email"], input[name*="mail"]');
        if (emailInput) {
          await emailInput.fill(BUFFETT_EMAIL);
        }

        const passwordInput = await page.$('input[type="password"]');
        if (passwordInput) {
          await passwordInput.fill(BUFFETT_PASSWORD);
        }

        const loginButton = await page.$('button[type="submit"], button:has-text("ログイン"), input[type="submit"]');
        if (loginButton) {
          await loginButton.click();
          await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
        }
      }
    }

    // ログイン成功を確認
    await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG));
    const finalUrl = page.url();
    writeLog(`[Buffett] ログイン後のURL: ${finalUrl}`);
    
    if (!finalUrl.includes("login") && !finalUrl.includes("signin")) {
      writeLog("[Buffett] ログイン成功");
      return true;
    }

    writeLog("[Buffett] ログイン失敗");
    return false;
  } catch (error) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    writeLog(`[Buffett] ログインエラー: ${errorMsg}`);
    return false;
  }
}

// ------------------------------
// フィールド別スクレイピング関数
// ------------------------------

interface ScrapedData {
  // 基本情報
  corporateNumber?: string;
  prefecture?: string;
  address?: string;
  phoneNumber?: string;
  fax?: string;
  email?: string;
  companyUrl?: string;
  contactFormUrl?: string;
  sns?: string[];
  
  // 代表者情報
  representativeName?: string;
  representativeKana?: string;
  representativeTitle?: string;
  representativeBirthDate?: string;
  representativePhone?: string;
  representativePostalCode?: string;
  representativeHomeAddress?: string;
  representativeRegisteredAddress?: string;
  representativeAlmaMater?: string;
  
  // 役員・組織情報
  executives?: string[];
  
  // 業種情報
  industry?: string;
  industryLarge?: string;
  industryMiddle?: string;
  industrySmall?: string;
  industryDetail?: string;
  
  // 財務情報
  capitalStock?: number;
  revenue?: number;
  operatingIncome?: number;
  totalAssets?: number;
  totalLiabilities?: number;
  netAssets?: number;
  
  // 上場情報
  listing?: string;
  marketSegment?: string;
  latestFiscalYearMonth?: string;
  fiscalMonth?: string;
  
  // 規模情報
  employeeCount?: number;
  factoryCount?: number;
  officeCount?: number;
  storeCount?: number;
  established?: string;
  
  // 取引先情報
  clients?: string[];
  suppliers?: string[];
  shareholders?: string[];
  banks?: string[];
  
  // その他
  headquartersAddress?: string;
}

/**
 * HPから情報を取得
 */
async function scrapeFromHomepage(
  page: Page,
  homepageUrl: string,
  companyName: string
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    await page.goto(homepageUrl, { waitUntil: PAGE_WAIT_MODE, timeout: PAGE_TIMEOUT });
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 電話番号
    const phoneMatch = text.match(/(?:電話|TEL|Tel|電話番号)[：:\s]*([0-9-()]{10,15})/i);
    if (phoneMatch) {
      const phone = phoneMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
      if (isValidPhoneNumber(phone)) {
        data.phoneNumber = phone;
      }
    }

    // FAX
    const faxMatch = text.match(/(?:FAX|Fax|fax|ファックス)[：:\s]*([0-9-()]{10,15})/i);
    if (faxMatch) {
      const fax = faxMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
      if (isValidFax(fax)) {
        data.fax = fax;
      }
    }

    // 問い合わせフォーム
    const contactFormLinks = $('a[href*="contact"], a[href*="inquiry"], a[href*="問い合わせ"], a[href*="お問い合わせ"]');
    if (contactFormLinks.length > 0) {
      const href = contactFormLinks.first().attr("href");
      if (href) {
        try {
          const url = new URL(href, homepageUrl).href;
          if (isValidUrl(url)) {
            data.contactFormUrl = url;
            // 問い合わせフォームURLからルートURL（HPのURL）を抽出
            try {
              const contactUrlObj = new URL(url);
              const rootUrl = `${contactUrlObj.protocol}//${contactUrlObj.host}`;
              if (isValidUrl(rootUrl)) {
                data.companyUrl = rootUrl;
              }
            } catch {
              // URL解析エラー時はスキップ
            }
          }
        } catch {
          const url = href.startsWith("http") ? href : `${homepageUrl}${href}`;
          if (isValidUrl(url)) {
            data.contactFormUrl = url;
            // 問い合わせフォームURLからルートURL（HPのURL）を抽出
            try {
              const contactUrlObj = new URL(url);
              const rootUrl = `${contactUrlObj.protocol}//${contactUrlObj.host}`;
              if (isValidUrl(rootUrl)) {
                data.companyUrl = rootUrl;
              }
            } catch {
              // URL解析エラー時はスキップ
            }
          }
        }
      }
    }
    
    // HPのURLがまだ取得できていない場合、現在のページURLからルートURLを抽出
    if (!data.companyUrl) {
      try {
        const currentUrl = page.url();
        const currentUrlObj = new URL(currentUrl);
        const rootUrl = `${currentUrlObj.protocol}//${currentUrlObj.host}`;
        if (isValidUrl(rootUrl)) {
          data.companyUrl = rootUrl;
        }
      } catch {
        // URL解析エラー時はスキップ
      }
    }

    // 代表者住所
    const repAddressMatch = text.match(/(?:代表者住所|代表取締役住所|代表者所在)[：:\s]*([^\n\r]{10,100})/i);
    if (repAddressMatch) {
      data.representativeHomeAddress = repAddressMatch[1].trim();
    }

    // 代表者生年月日
    const birthMatch = text.match(/(?:代表者|代表取締役)[^\n\r]*(?:生年月日|誕生日)[：:\s]*(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)/i);
    if (birthMatch) {
      const birthDate = birthMatch[1].replace(/[年月]/g, "-").replace(/日/g, "");
      if (isValidBirthDate(birthDate)) {
        data.representativeBirthDate = birthDate;
      }
    }

    // 業種（大・中・小・細）
    const industryMatch = text.match(/(?:業種|事業内容)[：:\s]*([^\n\r]{1,200})/i);
    if (industryMatch) {
      const industryText = industryMatch[1].trim();
      // 業種を分割（例: "大分類 > 中分類 > 小分類" の形式を想定）
      const parts = industryText.split(/[>＞]/).map((p) => normalizeIndustry(p)).filter(p => p.length > 0);
      
      if (parts.length >= 1 && isValidIndustry(parts[0])) {
        data.industryLarge = parts[0];
      }
      if (parts.length >= 2 && isValidIndustry(parts[1])) {
        data.industryMiddle = parts[1];
      }
      if (parts.length >= 3 && isValidIndustry(parts[2])) {
        data.industrySmall = parts[2];
      }
      if (parts.length >= 4 && isValidIndustry(parts[3])) {
        data.industryDetail = parts[3];
      }
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
        data.suppliers = suppliers.slice(0, 20); // 最大20件
      }
    }

    // 取引先（より厳密な抽出とバリデーション）
    const clientMatches = text.match(/(?:取引先|主要取引先|主な取引先|顧客)[：:\s]*([^\n\r]{10,200})/gi);
    if (clientMatches) {
      const clients: string[] = [];
      for (const match of clientMatches) {
        // 接頭辞を除去
        let clientText = match.replace(/^(?:取引先|主要取引先|主な取引先|顧客)[：:\s]*/i, "").trim();
        
        // 企業名らしい部分のみを抽出（長すぎる場合は最初の部分のみ）
        if (clientText.length > 200) {
          // 最初の200文字までで、区切り文字で分割
          clientText = clientText.substring(0, 200);
        }
        
        // 区切り文字で分割
        const items = clientText.split(/[、,;；\n\r]/).map((s) => s.trim()).filter((s) => s.length > 0);
        
        // 各項目をバリデーション
        for (const item of items) {
          if (isValidClientName(item)) {
            clients.push(item);
          }
        }
      }
      if (clients.length > 0) {
        data.clients = [...new Set(clients)].slice(0, 20); // 重複除去、最大20件
      }
    }

    // 取引先銀行
    const bankMatches = text.match(/(?:取引銀行|主要取引銀行|メインバンク|取引金融機関)[：:\s]*([^\n\r]{10,200})/gi);
    if (bankMatches) {
      const banks: string[] = [];
      for (const match of bankMatches) {
        const items = match.split(/[、,;；]/).map((s) => s.trim().replace(/^(?:取引銀行|主要取引銀行|メインバンク|取引金融機関)[：:\s]*/i, ""));
        banks.push(...items.filter((s) => s.length > 0));
      }
      if (banks.length > 0) {
        data.banks = banks.slice(0, 10); // 最大10件
      }
    }

    // 役員（より厳密な抽出とバリデーション）
    const officerMatches = text.match(/(?:役員|取締役|監査役|執行役員|代表者|代表取締役|社長)[：:\s]*([^\n\r]{1,100})/gi);
    if (officerMatches) {
      const officers: string[] = [];
      for (const match of officerMatches) {
        // 接頭辞を除去
        let name = match.replace(/^(?:役員|取締役|監査役|執行役員|代表者|代表取締役|社長)[：:\s]*/i, "").trim();
        
        // 区切り文字で分割（複数の役員が列挙されている場合）
        const names = name.split(/[、,;；\n\r]/).map((n) => n.trim()).filter((n) => n.length > 0);
        
        // 各氏名をバリデーション
        for (const n of names) {
          if (isValidExecutiveName(n)) {
            // バリデーション関数内で接頭辞が除去されるので、その結果を使用
            const validatedName = n.replace(/^(代表者|代表取締役|社長|取締役|監査役|執行役員|役員)[：:\s]*/i, "").trim();
            if (validatedName.length > 0) {
              officers.push(validatedName);
            }
          }
        }
      }
      if (officers.length > 0) {
        data.executives = [...new Set(officers)].slice(0, 20); // 重複除去、最大20件
      }
    }

    // SNS（Twitter）
    const twitterLinks = $('a[href*="twitter.com"], a[href*="x.com"]');
    const sns: string[] = [];
    twitterLinks.each((_, el) => {
      const href = $(el).attr("href");
      if (href && (href.includes("twitter.com") || href.includes("x.com"))) {
        sns.push(href);
      }
    });
    if (sns.length > 0) {
      data.sns = [...new Set(sns)];
    }

  } catch (error) {
    console.warn(`[HP] ${homepageUrl} からの取得エラー:`, (error as any)?.message);
  }

  return data;
}

/**
 * マイナビから情報を取得（正しい検索フロー）
 */
async function scrapeFromMynavi(
  page: Page,
  companyName: string,
  nullFields: string[]
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // マイナビ転職の検索ページにアクセス
    await page.goto("https://tenshoku.mynavi.jp/company/", {
      waitUntil: PAGE_WAIT_MODE,
      timeout: PAGE_TIMEOUT,
    });
    await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG * 2)); // ページ読み込みを待つ

    // ポップアップ・広告を閉じる（強化版）
    try {
      // モーダル、オーバーレイ、ポップアップを閉じる
      const popupSelectors = [
        'button:has-text("×")',
        'button[aria-label*="閉じる"]',
        'button[aria-label*="close"]',
        '.close',
        '[class*="close"]',
        'button.close',
        '[class*="ad-close"]',
        '[class*="modal-close"]',
        '[class*="popup-close"]',
        '[class*="overlay-close"]',
        'button[class*="close"]',
        '[id*="close"]',
        '[data-dismiss="modal"]',
        '[data-close]'
      ];
      
      for (const selector of popupSelectors) {
        try {
          await page.waitForSelector(selector, { timeout: 2000, state: 'visible' }).catch(() => {});
          const closeButtons = await page.$$(selector);
          for (const closeBtn of closeButtons) {
            try {
              const isVisible = await closeBtn.isVisible();
              if (isVisible) {
                await closeBtn.click({ timeout: 3000, force: true }).catch(() => {});
                await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300));
              }
            } catch {
              // 個別のボタンクリックエラーは無視
            }
          }
        } catch {
          // セレクターが見つからない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    // 検索テキストボックスを探す（waitForSelectorで待機）
    let searchInput: ElementHandle | null = null;
    const searchSelectors = [
      'input[placeholder*="企業名"]',
      'input[placeholder*="企業名で検索"]',
      'input[name*="company"]',
      'input[name*="q"]',
      'input[name*="keyword"]',
      'input[type="text"][class*="search"]',
      'input[type="text"]'
    ];
    
    for (const selector of searchSelectors) {
      try {
        // 要素が表示されるまで待機
        await page.waitForSelector(selector, { timeout: 5000, state: 'visible' }).catch(() => {});
        searchInput = await page.$(selector);
        if (searchInput) {
          const isVisible = await searchInput.isVisible();
          if (isVisible) {
            break;
          }
        }
      } catch {
        continue;
      }
    }

    if (searchInput) {
      await searchInput.fill(companyName);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
    } else {
      writeLog(`  [マイナビ] 検索ボックスが見つかりません: ${companyName}`);
      return data;
    }

    // 「企業を検索する」ボタンを押下
    const searchButton = await page.$('button:has-text("企業を検索する"), button[type="submit"]');
    if (searchButton) {
      await searchButton.click();
      await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    }

    // 「該当する企業はありませんでした。」をチェック
    const pageText = await page.textContent("body");
    if (pageText && (pageText.includes("該当する企業はありませんでした") || pageText.includes("検索結果がありません"))) {
      writeLog(`  [マイナビ] 検索結果なし: ${companyName}`);
      return data;
    }

    // 企業リストから企業名のリンクを探してクリック
    const companyLinks = await page.$$('a:has-text("' + companyName + '"), a[href*="/company/"]');
    if (companyLinks.length > 0) {
      // 企業名が完全一致するリンクを探す
      let foundLink = false;
      for (const link of companyLinks) {
        const linkText = await link.textContent();
        if (linkText && linkText.trim().includes(companyName)) {
          await link.click();
          await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
          await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
          foundLink = true;
          break;
        }
      }
      if (!foundLink && companyLinks.length > 0) {
        // 完全一致が見つからない場合は最初のリンクをクリック
        await companyLinks[0].click();
        await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
      }
    }

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 電話番号
    const phoneMatch = text.match(/(?:電話|TEL|Tel|電話番号)[：:\s]*([0-9-()]{10,15})/i);
    if (phoneMatch && !data.phoneNumber) {
      const phone = phoneMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
      if (isValidPhoneNumber(phone)) {
        data.phoneNumber = phone;
      }
    }

    // メール
    const emailMatch = text.match(/([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})/i);
    if (emailMatch) {
      const email = emailMatch[1].trim();
      if (isValidEmail(email)) {
        data.email = email;
      }
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
      }
    }

    // 業種
    const industryMatch = text.match(/(?:業種|事業内容)[：:\s]*([^\n\r]{1,200})/i);
    if (industryMatch) {
      const industryText = industryMatch[1].trim();
      const parts = industryText.split(/[>＞]/).map((p) => normalizeIndustry(p)).filter(p => p.length > 0);
      
      if (parts.length >= 1 && isValidIndustry(parts[0]) && !data.industryLarge) {
        data.industryLarge = parts[0];
      }
      if (parts.length >= 2 && isValidIndustry(parts[1]) && !data.industryMiddle) {
        data.industryMiddle = parts[1];
      }
      if (parts.length >= 3 && isValidIndustry(parts[2]) && !data.industrySmall) {
        data.industrySmall = parts[2];
      }
      if (parts.length >= 4 && isValidIndustry(parts[3]) && !data.industryDetail) {
        data.industryDetail = parts[3];
      }
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
        data.clients = [...(data.clients || []), ...clients].slice(0, 20);
      }
    }

  } catch (error) {
    console.warn(`[Mynavi] ${companyName} からの取得エラー:`, (error as any)?.message);
  }

  return data;
}

/**
 * マイナビ2026から情報を取得（正しい検索フロー）
 */
async function scrapeFromMynavi2026(
  page: Page,
  companyName: string,
  nullFields: string[]
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // マイナビ2026の検索ページにアクセス（指定されたURLを使用）
    await page.goto("https://job.mynavi.jp/26/pc/?gig_actions=sso.login", {
      waitUntil: PAGE_WAIT_MODE,
      timeout: PAGE_TIMEOUT,
    });
    await sleep(Math.max(SLEEP_MS * 3, MIN_SLEEP_MS_LONG * 2)); // ページ読み込みを待つ

    // ポップアップ・広告を閉じる（強化版）
    try {
      // モーダル、オーバーレイ、ポップアップを閉じる
      const popupSelectors = [
        'button:has-text("×")',
        'button[aria-label*="閉じる"]',
        'button[aria-label*="close"]',
        '.close',
        '[class*="close"]',
        'button.close',
        '[class*="ad-close"]',
        '[class*="modal-close"]',
        '[class*="popup-close"]',
        '[class*="overlay-close"]',
        'button[class*="close"]',
        '[id*="close"]',
        '[data-dismiss="modal"]',
        '[data-close]'
      ];
      
      for (const selector of popupSelectors) {
        try {
          await page.waitForSelector(selector, { timeout: 2000, state: 'visible' }).catch(() => {});
          const closeButtons = await page.$$(selector);
          for (const closeBtn of closeButtons) {
            try {
              const isVisible = await closeBtn.isVisible();
              if (isVisible) {
                await closeBtn.click({ timeout: 3000 }).catch(() => {});
                await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300));
              }
            } catch {
              // 個別のボタンクリックエラーは無視
            }
          }
        } catch {
          // セレクターが見つからない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    // 検索テキストボックスを探す（waitForSelectorで待機）
    let searchInput: ElementHandle | null = null;
    const searchSelectors = [
      'input[placeholder*="企業名"]',
      'input[placeholder*="キーワード"]',
      'input[placeholder*="企業名や事業内容"]',
      'input[name*="q"]',
      'input[name*="keyword"]',
      'input[type="text"][class*="search"]',
      'input[type="text"]'
    ];
    
    for (const selector of searchSelectors) {
      try {
        // 要素が表示されるまで待機
        await page.waitForSelector(selector, { timeout: 5000, state: 'visible' }).catch(() => {});
        searchInput = await page.$(selector);
        if (searchInput) {
          const isVisible = await searchInput.isVisible();
          if (isVisible) {
            break;
          }
        }
      } catch {
        continue;
      }
    }

    if (searchInput) {
      // ポップアップ/広告で入力が阻害されることがあるため、forceクリック→入力
      await searchInput.click({ force: true, timeout: 3000 }).catch(() => {});
      try {
        await searchInput.fill(companyName);
      } catch {
        // fillが失敗する場合はキーボード入力にフォールバック
        await page.keyboard.type(companyName, { delay: 10 }).catch(() => {});
      }
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
    } else {
      writeLog(`  [マイナビ2026] 検索ボックスが見つかりません: ${companyName}`);
      return data;
    }

    // 「検索」ボタンを押下（waitForSelectorで待機）
    try {
      await page.waitForSelector('button:has-text("検索"), button[type="submit"]', { timeout: 5000, state: 'visible' }).catch(() => {});
    } catch {
      // ボタンが見つからない場合は続行
    }
    
    const searchButton = await page.$('button:has-text("検索"), button[type="submit"]');
    if (searchButton) {
      const isDisabled = await searchButton.isDisabled();
      if (isDisabled) {
        writeLog(`  [マイナビ2026] 検索ボタンが無効のため取得不可: ${companyName}`);
        return data;
      }
      // クリックと同時に遷移するため、Promise.allで待機（遷移取りこぼし防止）
      await Promise.all([
        page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {}),
        searchButton.click({ force: true }).catch(() => {}),
      ]);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    }

    // 企業リストから企業名のリンクを探してクリック
    const companyLinks = await page.$$('a:has-text("' + companyName + '"), a[href*="/corp/"]');
    if (companyLinks.length > 0) {
      let foundLink = false;
      for (const link of companyLinks) {
        const linkText = await link.textContent();
        if (linkText && linkText.trim().includes(companyName)) {
          await Promise.all([
            page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {}),
            link.click({ force: true }).catch(() => {}),
          ]);
          await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
          foundLink = true;
          break;
        }
      }
      if (!foundLink && companyLinks.length > 0) {
        await Promise.all([
          page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {}),
          companyLinks[0].click({ force: true }).catch(() => {}),
        ]);
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
      }
    }

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 業種
    const industryMatch = text.match(/(?:業種|事業内容)[：:\s]*([^\n\r]{1,200})/i);
    if (industryMatch) {
      const industryText = industryMatch[1].trim();
      const parts = industryText.split(/[>＞]/).map((p) => normalizeIndustry(p)).filter(p => p.length > 0);
      
      if (parts.length >= 1 && isValidIndustry(parts[0]) && !data.industryLarge) {
        data.industryLarge = parts[0];
      }
      if (parts.length >= 2 && isValidIndustry(parts[1]) && !data.industryMiddle) {
        data.industryMiddle = parts[1];
      }
      if (parts.length >= 3 && isValidIndustry(parts[2]) && !data.industrySmall) {
        data.industrySmall = parts[2];
      }
      if (parts.length >= 4 && isValidIndustry(parts[3]) && !data.industryDetail) {
        data.industryDetail = parts[3];
      }
    }

    // 取引先（より詳細）
    const clientMatches = text.match(/(?:取引先|主要取引先|主な取引先)[：:\s]*([^\n\r]{20,500})/gi);
    if (clientMatches) {
      const clients: string[] = [];
      for (const match of clientMatches) {
        const items = match.split(/[、,;；]/).map((s) => s.trim().replace(/^(?:取引先|主要取引先|主な取引先)[：:\s]*/i, ""));
        clients.push(...items.filter((s) => s.length > 0));
      }
      if (clients.length > 0) {
        data.clients = [...(data.clients || []), ...clients].slice(0, 20);
      }
    }

  } catch (error) {
    console.warn(`[Mynavi2026] ${companyName} からの取得エラー:`, (error as any)?.message);
  }

  return data;
}

/**
 * キャリタス就活から情報を取得
 * 注意: 現在このサイトは利用できないため、関数を無効化しています
 */
async function scrapeFromCareeritas(
  page: Page,
  companyName: string,
  nullFields: string[]
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // キャリタス就活の検索ページにアクセス
    const searchUrls = [
      "https://careeritas.jp/",
      "https://careeritas.jp/search",
    ];

    let searchUrl = searchUrls[0];
    for (const url of searchUrls) {
      try {
        await page.goto(url, { waitUntil: PAGE_WAIT_MODE, timeout: PAGE_TIMEOUT });
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
        searchUrl = url;
        break;
      } catch {
        continue;
      }
    }

    // 検索テキストボックスに企業名を入力
    const searchInput = await page.$('input[type="search"], input[placeholder*="企業名"], input[name*="q"]');
    if (searchInput) {
      await searchInput.fill(companyName);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
    }

    // 検索ボタンを押下（Promise.allでナビゲーション待機）
    const searchButton = await page.$('button:has-text("検索"), button[type="submit"]');
    if (searchButton) {
      await Promise.all([
        page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {}),
        searchButton.click({ force: true }).catch(() => {})
      ]);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    }

    // 企業リストから企業名のリンクを探してクリック（Promise.allでナビゲーション待機）
    const companyLinks = await page.$$('a:has-text("' + companyName + '"), a[href*="/company/"]');
    if (companyLinks.length > 0) {
      let foundLink = false;
      for (const link of companyLinks) {
        let linkText: string | null = null;
        try {
          linkText = await link.textContent();
        } catch {
          // SPA遷移等でコンテキストが破棄される場合があるためスキップ
          continue;
        }
        if (linkText && linkText.trim().includes(companyName)) {
          await Promise.all([
            page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {}),
            link.click({ force: true }).catch(() => {})
          ]);
          await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
          foundLink = true;
          break;
        }
      }
      if (!foundLink && companyLinks.length > 0) {
        await Promise.all([
          page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {}),
          companyLinks[0].click({ force: true }).catch(() => {})
        ]);
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
      }
    }

    // 遷移直後はコンテキスト破棄が起きやすいので、ロード状態を待つ
    await page.waitForLoadState(PAGE_WAIT_MODE, { timeout: NAVIGATION_TIMEOUT }).catch(() => {});

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 電話番号
    if (nullFields.includes("phoneNumber")) {
      const phoneMatch = text.match(/(?:電話|TEL|Tel|電話番号)[：:\s]*([0-9-()]{10,15})/i);
      if (phoneMatch && !data.phoneNumber) {
        const phone = phoneMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
        if (isValidPhoneNumber(phone)) {
          data.phoneNumber = phone;
        }
      }
    }

    // 業種
    if (nullFields.some(f => ["industryLarge", "industryMiddle", "industrySmall", "industryDetail"].includes(f))) {
      const industryMatch = text.match(/(?:業種|事業内容)[：:\s]*([^\n\r]{1,200})/i);
      if (industryMatch) {
        const industryText = industryMatch[1].trim();
        const parts = industryText.split(/[>＞]/).map((p) => normalizeIndustry(p)).filter(p => p.length > 0);
        
        if (nullFields.includes("industryLarge") && parts.length >= 1 && isValidIndustry(parts[0]) && !data.industryLarge) {
          data.industryLarge = parts[0];
        }
        if (nullFields.includes("industryMiddle") && parts.length >= 2 && isValidIndustry(parts[1]) && !data.industryMiddle) {
          data.industryMiddle = parts[1];
        }
        if (nullFields.includes("industrySmall") && parts.length >= 3 && isValidIndustry(parts[2]) && !data.industrySmall) {
          data.industrySmall = parts[2];
        }
        if (nullFields.includes("industryDetail") && parts.length >= 4 && isValidIndustry(parts[3]) && !data.industryDetail) {
          data.industryDetail = parts[3];
        }
      }
    }

  } catch (error) {
    const errorMsg = (error as any)?.message || String(error);
    // クリックと同時遷移で発生することがあるエラーは、警告のみでスキップ
    if (errorMsg.includes("Execution context was destroyed")) {
      writeLog(`  [Careeritas] 遷移中のコンテキスト破棄のためスキップ: ${companyName}`);
      return {};
    }
    // DNSエラーやネットワークエラーは警告のみ（処理は続行）
    if (errorMsg.includes("ERR_NAME_NOT_RESOLVED") || errorMsg.includes("net::")) {
      // サイトが存在しない場合は静かにスキップ
      return {};
    }
    writeLog(`  [Careeritas] ${companyName} からの取得エラー: ${errorMsg}`);
  }

  return data;
}

/**
 * バフェットコードから営業利益を取得（正しい検索フロー）
 * N/Aやレンジ表示は除外し、数値のみを取得
 */
async function scrapeOperatingIncomeFromBuffett(
  page: Page,
  companyName: string,
  nikkeiCode?: string
): Promise<number | null> {
  try {
    // バフェットコードのトップページにアクセス（ログイン後は検索ページに遷移）
    await page.goto("https://www.buffett-code.com/", {
      waitUntil: PAGE_WAIT_MODE,
      timeout: PAGE_TIMEOUT,
    });
    await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    
    // ログイン済みでない場合は、検索ページに遷移
    const currentUrl = page.url();
    if (!currentUrl.includes("global_screening") && !currentUrl.includes("company") && !currentUrl.includes("stock")) {
      // 検索ページに遷移
      await page.goto("https://www.buffett-code.com/global_screening", {
        waitUntil: PAGE_WAIT_MODE,
        timeout: PAGE_TIMEOUT,
      });
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG));
    }

    // 検索ボックスに企業名を入力
    const searchInput = await page.$('input[placeholder*="会社名"], input[placeholder*="銘"], input[type="search"], input[name*="q"]');
    if (searchInput) {
      const searchQuery = nikkeiCode || companyName;
      await searchInput.fill(searchQuery);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
      
      // Enterキーを押下
      await searchInput.press("Enter");
      await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    }

    // 「検索結果はありませんでした。」をチェック
    const pageText = await page.textContent("body");
    if (pageText && pageText.includes("検索結果はありませんでした")) {
      writeLog(`  [Buffett] 検索結果なし: ${companyName}`);
      return null;
    }

    // 企業リストから企業名のリンクを探してクリック
    const companyLinks = await page.$$('a:has-text("' + companyName + '"), a[href*="/company/"], a[href*="/stock/"]');
    if (companyLinks.length > 0) {
      let foundLink = false;
      for (const link of companyLinks) {
        const linkText = await link.textContent();
        if (linkText && linkText.trim().includes(companyName)) {
          await link.click();
          await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
          await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
          foundLink = true;
          break;
        }
      }
      if (!foundLink && companyLinks.length > 0) {
        await companyLinks[0].click();
        await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
      }
    }

    // 「企業概要」「業績」「大株主」「役員」タブから情報を取得
    // まず「業績」タブをクリック
    const performanceTab = await page.$('a:has-text("業績"), button:has-text("業績"), [data-tab="業績"]');
    if (performanceTab) {
      await performanceTab.click();
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
    }

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 営業利益を探す（複数のパターンに対応）
    const patterns = [
      /(?:営業利益|営業損益)[：:\s]*([^\n\r]{0,50})/i,
    ];

    for (const pattern of patterns) {
      const match = text.match(pattern);
      if (match) {
        const rawValue = match[1].trim();
        
        // N/A、レンジ表示（-、～、〜）、その他の非数値表現を除外
        const excludePatterns = [
          /N\/A/i,
          /-/, // ハイフン（レンジ表示）
          /～/, // 全角チルダ
          /〜/, // 半角チルダ
          /非公開/i,
          /なし/i,
          /不明/i,
          /該当なし/i,
        ];

        // 除外パターンに該当する場合はスキップ
        if (excludePatterns.some((p) => p.test(rawValue))) {
          console.log(`  [Buffett] 営業利益が非数値表示のためスキップ: ${rawValue}`);
          continue;
        }

        // 数値のみを抽出（カンマや単位を含む可能性がある）
        const numberMatch = rawValue.match(/([\d,]+)\s*(億円|百万円|万円|千円|円|億|百万|万|千)?/);
        if (numberMatch) {
          const valueStr = numberMatch[1].replace(/,/g, "");
          const value = parseFloat(valueStr);
          
          // 有効な数値であることを確認
          if (!isNaN(value) && isFinite(value) && value > 0) {
            // 単位を千円に変換
            let valueInThousandYen = value;
            const unit = numberMatch[2] || "";
            
            if (unit.includes("億")) {
              valueInThousandYen = value * 100000;
            } else if (unit.includes("百万")) {
              valueInThousandYen = value * 1000;
            } else if (unit.includes("万") && !unit.includes("百万")) {
              valueInThousandYen = value * 10;
            } else if (unit.includes("円") && !unit.includes("千") && !unit.includes("万") && !unit.includes("百万") && !unit.includes("億")) {
              valueInThousandYen = value / 1000;
            }
            
            // マイナス値や異常に大きな値も除外
            if (valueInThousandYen > 0 && valueInThousandYen < 1e15) {
              return valueInThousandYen;
            }
          }
        }
      }
    }

  } catch (error) {
    console.warn(`[Buffett] ${companyName} の営業利益取得エラー:`, (error as any)?.message);
  }

  return null;
}

/**
 * 官報決算DBから営業利益を取得（正しい検索フロー）
 */
async function scrapeOperatingIncomeFromCatr(
  page: Page,
  companyName: string,
  corporateNumber?: string
): Promise<number | null> {
  try {
    // 官報決算DBの検索ページにアクセス
    await page.goto("https://catr.jp/", {
      waitUntil: PAGE_WAIT_MODE,
      timeout: PAGE_TIMEOUT,
    });
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）

    // 画面上部の検索テキストボックスに企業名を入力
    const searchInput = await page.$('input[placeholder*="社名"], input[placeholder*="代表者名"], input[type="search"], input[name*="q"]');
    if (searchInput) {
      const searchQuery = corporateNumber || companyName;
      await searchInput.fill(searchQuery);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
    }

    // 検索ボタンを押下
    const searchButton = await page.$('button:has-text("検索"), button[type="submit"]');
    if (searchButton) {
      await searchButton.click();
      await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    }

    // 広告を閉じる（右上の×ボタン）
    const closeAdButtons = await page.$$('button:has-text("×"), button[aria-label*="閉じる"], .close, [class*="close"]');
    for (const closeBtn of closeAdButtons) {
      try {
        await closeBtn.click();
          await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300)); // 最小待機時間（高速化モード対応）
      } catch {
        // 広告が存在しない場合はスキップ
      }
    }

    // 「上場企業の関連会社から探す」または「{企業名}の検索結果(最大100件)」の企業名リンクをクリック
    const companyLinks = await page.$$('a:has-text("' + companyName + '"), a[href*="/company/"]');
    if (companyLinks.length > 0) {
      let foundLink = false;
      for (const link of companyLinks) {
        const linkText = await link.textContent();
        if (linkText && linkText.trim().includes(companyName)) {
          await link.click();
          await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
          await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
          foundLink = true;
          break;
        }
      }
      if (!foundLink && companyLinks.length > 0) {
        await companyLinks[0].click();
        await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
      }
    }

    // 結果が表示されない場合は取得不可
    const pageText = await page.textContent("body");
    if (!pageText || pageText.trim().length < 100) {
      writeLog(`  [CATR] 検索結果なし: ${companyName}`);
      return null;
    }

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 営業利益を探す
    const patterns = [
      /(?:営業利益|営業損益)[：:\s]*([\d,]+)\s*(?:億円|百万円|万円|千円|円)/i,
      /(?:営業利益|営業損益)[：:\s]*([\d,]+)/i,
    ];

    for (const pattern of patterns) {
      const match = text.match(pattern);
      if (match) {
        const value = parseFloat(match[1].replace(/,/g, ""));
        if (!isNaN(value)) {
          let valueInThousandYen = value;
          if (match[0].includes("億円")) {
            valueInThousandYen = value * 100000;
          } else if (match[0].includes("百万円")) {
            valueInThousandYen = value * 1000;
          } else if (match[0].includes("万円")) {
            valueInThousandYen = value * 10;
          } else if (match[0].includes("円") && !match[0].includes("千")) {
            valueInThousandYen = value / 1000;
          }
          return valueInThousandYen;
        }
      }
    }

  } catch (error) {
    console.warn(`[CATR] ${companyName} の営業利益取得エラー:`, (error as any)?.message);
  }

  return null;
}

/**
 * 就活会議から営業利益を取得
 */
async function scrapeOperatingIncomeFromShukatsu(
  page: Page,
  companyName: string
): Promise<number | null> {
  try {
    // 就活会議の検索URL（複数のURLパターンを試行）
    const searchUrls = [
      `https://shukatsu-db.com/search?q=${encodeURIComponent(companyName)}`,
      `https://www.shukatsu-db.com/search?q=${encodeURIComponent(companyName)}`,
      `https://shukatsu-db.com/company/search?q=${encodeURIComponent(companyName)}`,
      `https://shukatsu-kaigi.com/search?q=${encodeURIComponent(companyName)}`,
      `https://www.shukatsu-kaigi.com/search?q=${encodeURIComponent(companyName)}`,
    ];

    let html = "";
    for (const searchUrl of searchUrls) {
      try {
        await page.goto(searchUrl, { waitUntil: PAGE_WAIT_MODE, timeout: PAGE_TIMEOUT });
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
        html = await page.content();
        
        // ページが正常に読み込まれたか確認（404エラーなどでないか）
        if (html && html.length > 1000) {
          break;
        }
      } catch (urlError) {
        // このURLではアクセスできない場合は次のURLを試す
        continue;
      }
    }

    if (!html) {
      return null;
    }

    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 営業利益を探す
    const patterns = [
      /(?:営業利益|営業損益)[：:\s]*([\d,]+)\s*(?:億円|百万円|万円|千円|円)/i,
      /(?:営業利益|営業損益)[：:\s]*([\d,]+)/i,
    ];

    for (const pattern of patterns) {
      const match = text.match(pattern);
      if (match) {
        const value = parseFloat(match[1].replace(/,/g, ""));
        if (!isNaN(value) && value > 0) {
          let valueInThousandYen = value;
          if (match[0].includes("億円")) {
            valueInThousandYen = value * 100000;
          } else if (match[0].includes("百万円")) {
            valueInThousandYen = value * 1000;
          } else if (match[0].includes("万円")) {
            valueInThousandYen = value * 10;
          } else if (match[0].includes("円") && !match[0].includes("千")) {
            valueInThousandYen = value / 1000;
          }
          return valueInThousandYen;
        }
      }
    }

  } catch (error) {
    const errorMsg = (error as any)?.message || String(error);
    // DNSエラーやネットワークエラーは警告のみ（処理は続行）
    if (!errorMsg.includes("ERR_NAME_NOT_RESOLVED") && !errorMsg.includes("net::ERR_NAME_NOT_RESOLVED")) {
      writeLog(`  ⚠️  [Shukatsu] ${companyName} の営業利益取得エラー: ${errorMsg}`);
    }
  }

  return null;
}

/**
 * 代表者生年月日を検索＋ニュース記事から取得
 */
async function scrapeRepresentativeBirthDateFromNews(
  page: Page,
  representativeName: string,
  companyName: string
): Promise<string | null> {
  try {
    // Google検索で代表者名 + 企業名 + 生年月日を検索
    const searchQuery = `${representativeName} ${companyName} 生年月日`;
    const searchUrl = `https://www.google.com/search?q=${encodeURIComponent(searchQuery)}`;
    
    await page.goto(searchUrl, { waitUntil: PAGE_WAIT_MODE, timeout: PAGE_TIMEOUT });
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 生年月日を探す（複数のパターンに対応）
    const patterns = [
      /(?:生年月日|誕生日)[：:\s]*(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)/i,
      /(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)\s*(?:生まれ|誕生)/i,
      /(?:昭和|平成|令和)\s*(\d{1,2})年\s*(\d{1,2})月\s*(\d{1,2})日/i,
    ];

    for (const pattern of patterns) {
      const match = text.match(pattern);
      if (match) {
        let birthDate = "";
        if (match[0].includes("昭和") || match[0].includes("平成") || match[0].includes("令和")) {
          // 和暦を西暦に変換（簡易版）
          const era = match[0].includes("昭和") ? 1925 : match[0].includes("平成") ? 1988 : 2018;
          const year = parseInt(match[1]) + era;
          birthDate = `${year}-${match[2]}-${match[3]}`;
        } else {
          birthDate = match[1].replace(/[年月]/g, "-").replace(/日/g, "");
        }
        
        // 日付形式の検証
        if (birthDate.match(/\d{4}[-\/]\d{1,2}[-\/]\d{1,2}/)) {
          return birthDate;
        }
      }
    }

  } catch (error) {
    console.warn(`[News] ${representativeName} の生年月日取得エラー:`, (error as any)?.message);
  }

  return null;
}

/**
 * 日本食糧新聞から情報を取得（食品関連企業の場合）
 */
async function scrapeFromNihonShokuryo(
  page: Page,
  companyName: string,
  representativeName?: string
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // 日本食糧新聞の検索URL（実際のURL構造に合わせて調整が必要）
    const searchUrl = `https://www.nissyoku.co.jp/search?q=${encodeURIComponent(companyName)}`;
    await page.goto(searchUrl, { waitUntil: PAGE_WAIT_MODE, timeout: PAGE_TIMEOUT });
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 役員情報
    if (representativeName) {
      const officerMatches = text.match(new RegExp(`(${representativeName}[^\n\r]{0,50})`, "gi"));
      if (officerMatches) {
        const officers: string[] = [];
        for (const match of officerMatches) {
          if (match.length > 0 && match.length < 100) {
            officers.push(match.trim());
          }
        }
        if (officers.length > 0) {
          data.executives = [...new Set(officers)].slice(0, 10);
        }
      }
    }

    // 代表者生年月日
    const birthMatch = text.match(/(?:生年月日|誕生日)[：:\s]*(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)/i);
    if (birthMatch) {
      const birthDate = birthMatch[1].replace(/[年月]/g, "-").replace(/日/g, "");
      if (isValidBirthDate(birthDate)) {
        data.representativeBirthDate = birthDate;
      }
    }

  } catch (error) {
    console.warn(`[NihonShokuryo] ${companyName} からの取得エラー:`, (error as any)?.message);
  }

  return data;
}

/**
 * 日経コンパスから役員情報を取得
 */
async function scrapeOfficersFromNikkeiCompass(
  page: Page,
  companyName: string
): Promise<string[] | null> {
  try {
    // 日経コンパスの検索URL（実際のURL構造に合わせて調整が必要）
    const searchUrl = `https://compass.nikkei.com/search?q=${encodeURIComponent(companyName)}`;
    await page.goto(searchUrl, { waitUntil: PAGE_WAIT_MODE, timeout: PAGE_TIMEOUT });
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 役員情報
    const officerMatches = text.match(/(?:役員|取締役|監査役|執行役員)[：:\s]*([^\n\r]+)/gi);
    if (officerMatches) {
      const officers: string[] = [];
      for (const match of officerMatches) {
        const name = match.replace(/^(?:役員|取締役|監査役|執行役員)[：:\s]*/i, "").trim();
        if (name.length > 0 && name.length < 50) {
          officers.push(name);
        }
      }
      if (officers.length > 0) {
        return [...new Set(officers)].slice(0, 20);
      }
    }

  } catch (error) {
    console.warn(`[NikkeiCompass] ${companyName} の役員取得エラー:`, (error as any)?.message);
  }

  return null;
}

/**
 * 全国法人リストから企業情報を取得（法人番号がある場合は直接アクセス）
 */
async function scrapeFromHoujin(
  page: Page,
  companyName: string,
  nullFields: string[],
  corporateNumber?: string | null
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // 法人番号がある場合は直接企業詳細ページにアクセス
    if (corporateNumber) {
      const detailUrl = `https://houjin.jp/c/${corporateNumber}`;
      writeLog(`  [全国法人リスト] 法人番号で直接アクセス: ${detailUrl}`);
      
      await page.goto(detailUrl, {
        waitUntil: PAGE_WAIT_MODE,
        timeout: PAGE_TIMEOUT,
      });
      await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG * 2)); // 詳細ページの読み込みを待つ
      
      // ページが存在するかチェック（404エラーなど）
      const pageText = await page.textContent("body");
      if (pageText && (pageText.includes("ページが見つかりません") || pageText.includes("404"))) {
        writeLog(`  [全国法人リスト] 企業詳細ページが見つかりません: ${companyName} (法人番号: ${corporateNumber})`);
        // 法人番号での直接アクセスが失敗した場合は検索にフォールバック
      } else {
        // 詳細ページから情報を取得
        return await scrapeFromHoujinDetailPage(page, companyName, nullFields);
      }
    }

    // 法人番号がない場合、または直接アクセスが失敗した場合は検索フローを使用
    await page.goto("https://houjin.jp/", {
      waitUntil: PAGE_WAIT_MODE,
      timeout: PAGE_TIMEOUT,
    });
    await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG * 2)); // ページ読み込みを待つ

    // 広告を閉じる（右上の×ボタンなど）
    try {
      const closeAdButtons = await page.$$('button:has-text("×"), button[aria-label*="閉じる"], .close, [class*="close"], button.close, [class*="ad-close"], [class*="modal-close"]');
      for (const closeBtn of closeAdButtons) {
        try {
          const isVisible = await closeBtn.isVisible();
          if (isVisible) {
            await closeBtn.click();
            await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300));
          }
        } catch {
          // 広告が存在しない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    // 検索テキストボックスを探す（複数のセレクターを試行）
    let searchInput: ElementHandle | null = null;
    const searchSelectors = [
      'input[placeholder*="企業名"]',
      'input[placeholder*="法人番号"]',
      'input[placeholder*="企業名、法人番号"]',
      'input[name*="q"]',
      'input[name*="keyword"]',
      'input[type="text"][class*="search"]',
      'input[type="text"]'
    ];
    
    for (const selector of searchSelectors) {
      try {
        searchInput = await page.$(selector);
        if (searchInput) {
          const isVisible = await searchInput.isVisible();
          if (isVisible) {
            break;
          }
        }
      } catch {
        continue;
      }
    }

    if (searchInput) {
      await searchInput.fill(companyName);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
    } else {
      writeLog(`  [全国法人リスト] 検索ボックスが見つかりません: ${companyName}`);
      return data;
    }

    // 検索ボタンを押下
    const searchButton = await page.$('button:has-text("検索"), button[type="submit"], button:has(svg[class*="search"])');
    if (searchButton) {
      await searchButton.click();
      await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    }

    // 「検索後0件を表示/全0件」をチェック
    const pageText = await page.textContent("body");
    if (pageText && (pageText.includes("0件") || pageText.includes("全0件") || pageText.includes("検索結果がありません"))) {
      writeLog(`  [全国法人リスト] 検索結果なし: ${companyName}`);
      return data;
    }

    // 広告を閉じる（検索結果ページでも）
    try {
      const closeAdButtons = await page.$$('button:has-text("×"), button[aria-label*="閉じる"], .close, [class*="close"], button.close, [class*="ad-close"], [class*="modal-close"]');
      for (const closeBtn of closeAdButtons) {
        try {
          const isVisible = await closeBtn.isVisible();
          if (isVisible) {
            await closeBtn.click();
            await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300)); // 最小待機時間（高速化モード対応）
          }
        } catch {
          // 広告が存在しない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    // 企業リストから企業名のリンクを探してクリック
    const companyLinks = await page.$$('a:has-text("' + companyName + '"), a[href*="/c/"]');
    if (companyLinks.length > 0) {
      let foundLink = false;
      for (const link of companyLinks) {
        const linkText = await link.textContent();
        if (linkText && linkText.trim().includes(companyName)) {
          await link.click();
          await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
          await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
          foundLink = true;
          break;
        }
      }
      if (!foundLink && companyLinks.length > 0) {
        // 完全一致が見つからない場合は最初のリンクをクリック
        await companyLinks[0].click();
        await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
      }
    } else {
      writeLog(`  [全国法人リスト] 企業リンクが見つかりません: ${companyName}`);
      return data;
    }

    // 企業詳細画面から情報を取得
    return await scrapeFromHoujinDetailPage(page, companyName, nullFields);

  } catch (error) {
    writeLog(`  [全国法人リスト] エラー: ${(error as any)?.message}`);
  }

  return data;
}

/**
 * 全国法人リストの詳細ページから情報を取得
 */
async function scrapeFromHoujinDetailPage(
  page: Page,
  companyName: string,
  nullFields: string[]
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // ページ読み込みを待つ
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG));

    // 広告を閉じる（右上の×ボタン）
    try {
      const closeAdButtons = await page.$$('button:has-text("×"), button[aria-label*="閉じる"], .close, [class*="close"], button.close, [class*="ad-close"], [class*="modal-close"]');
      for (const closeBtn of closeAdButtons) {
        try {
          const isVisible = await closeBtn.isVisible();
          if (isVisible) {
            await closeBtn.click();
            await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300)); // 最小待機時間（高速化モード対応）
          }
        } catch {
          // 広告が存在しない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    // 「基本情報」タブをクリック（既に表示されている場合はスキップ）
    try {
      const basicInfoTab = await page.$('a:has-text("基本情報"), button:has-text("基本情報"), [data-tab*="basic"]');
      if (basicInfoTab) {
        await basicInfoTab.click();
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
      }
    } catch {
      // タブが見つからない場合は続行（既に基本情報が表示されている可能性）
    }

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 電話番号
    if (nullFields.includes("phoneNumber") && !data.phoneNumber) {
      const phoneMatch = text.match(/(?:電話番号|電話|TEL)[：:\s]*([0-9-()]{10,15})/i);
      if (phoneMatch) {
        const phone = phoneMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
        if (isValidPhoneNumber(phone)) {
          data.phoneNumber = phone;
          writeLog(`  [全国法人リスト] 電話番号取得: ${phone}`);
        }
      }
    }

    // URL（companyUrl）
    if (nullFields.includes("companyUrl") && !data.companyUrl) {
      const urlMatch = text.match(/(?:URL)[：:\s]*(https?:\/\/[^\s\n\r]+)/i);
      if (urlMatch) {
        const url = urlMatch[1].trim();
        if (isValidUrl(url)) {
          data.companyUrl = url;
          writeLog(`  [全国法人リスト] URL取得: ${url}`);
        }
      }
      // テーブル内のURLセルを探す
      if (!data.companyUrl) {
        const urlCell = $('td:contains("URL"), th:contains("URL")').next('td');
        if (urlCell.length > 0) {
          const urlLink = urlCell.find('a[href^="http"]').first();
          if (urlLink.length > 0) {
            const href = urlLink.attr('href');
            if (href && isValidUrl(href)) {
              data.companyUrl = href;
              writeLog(`  [全国法人リスト] URL取得: ${href}`);
            }
          }
        }
      }
    }

    // 代表者名
    if (nullFields.includes("representativeName") && !data.representativeName) {
      const repMatch = text.match(/(?:社長|代表者|代表取締役)[：:\s]*([^\n\r]{1,50})/i);
      if (repMatch) {
        const repName = repMatch[1].trim();
        if (repName && repName.length > 0 && repName.length < 50 && !repName.includes('—') && !repName.includes('-')) {
          data.representativeName = repName;
          writeLog(`  [全国法人リスト] 代表者名取得: ${repName}`);
        }
      }
      // テーブル内の代表者セルを探す
      if (!data.representativeName) {
        const repCell = $('td:contains("社長"), td:contains("代表者"), th:contains("代表者")').next('td');
        if (repCell.length > 0) {
          const repText = repCell.text().trim();
          if (repText && repText.length > 0 && repText.length < 50 && !repText.includes('—') && !repText.includes('-')) {
            data.representativeName = repText;
            writeLog(`  [全国法人リスト] 代表者名取得: ${repText}`);
          }
        }
      }
    }

    // 設立日
    if (nullFields.includes("established") && !data.established) {
      const establishedMatch = text.match(/(?:設立)[：:\s]*(\d{4}年\d{1,2}月\d{1,2}日?)/i);
      if (establishedMatch) {
        data.established = establishedMatch[1];
        writeLog(`  [全国法人リスト] 設立日取得: ${establishedMatch[1]}`);
      }
      // テーブル内の設立セルを探す
      if (!data.established) {
        const establishedCell = $('td:contains("設立"), th:contains("設立")').next('td');
        if (establishedCell.length > 0) {
          const establishedText = establishedCell.text().trim();
          if (establishedText && establishedText.match(/\d{4}年\d{1,2}月\d{1,2}日?/)) {
            data.established = establishedText;
            writeLog(`  [全国法人リスト] 設立日取得: ${establishedText}`);
          }
        }
      }
    }

    // 住所
    if (nullFields.includes("address") || nullFields.includes("headquartersAddress")) {
      const addressMatch = text.match(/住所[：:\s]*([^\n\r]{10,100})/i);
      if (addressMatch) {
        const address = addressMatch[1].trim();
        if (address && address.length > 5 && address.length < 200 && !address.includes('—')) {
          if (nullFields.includes("address") && !data.address) {
            data.address = address;
            writeLog(`  [全国法人リスト] 住所取得: ${address}`);
          }
          if (nullFields.includes("headquartersAddress") && !data.headquartersAddress) {
            data.headquartersAddress = address;
            writeLog(`  [全国法人リスト] 本社住所取得: ${address}`);
          }
        }
      }
      // テーブル内の住所セルを探す
      if ((nullFields.includes("address") && !data.address) || (nullFields.includes("headquartersAddress") && !data.headquartersAddress)) {
        const addressCell = $('td:contains("住所"), th:contains("住所")').next('td');
        if (addressCell.length > 0) {
          const addressText = addressCell.text().trim();
          if (addressText && addressText.length > 5 && addressText.length < 200 && !addressText.includes('—')) {
            if (nullFields.includes("address") && !data.address) {
              data.address = addressText;
              writeLog(`  [全国法人リスト] 住所取得: ${addressText}`);
            }
            if (nullFields.includes("headquartersAddress") && !data.headquartersAddress) {
              data.headquartersAddress = addressText;
              writeLog(`  [全国法人リスト] 本社住所取得: ${addressText}`);
            }
          }
        }
      }
    }

    // 業種
    if (nullFields.includes("industry") && !data.industry) {
      const industryMatch = text.match(/(?:業種)[：:\s]*([^\n\r]{1,100})/i);
      if (industryMatch) {
        const industry = industryMatch[1].trim();
        if (industry && industry.length > 0 && industry.length < 100 && !industry.includes('—')) {
          data.industry = industry;
          writeLog(`  [全国法人リスト] 業種取得: ${industry}`);
        }
      }
    }

    // 「決算情報」タブをクリックして決算情報を取得
    try {
      const financialInfoTab = await page.$('a:has-text("決算情報"), button:has-text("決算情報"), [data-tab*="financial"]');
      if (financialInfoTab) {
        await financialInfoTab.click();
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
        
        const financialHtml = await page.content();
        const $financial = cheerio.load(financialHtml);
        const financialText = $financial("body").text().replace(/\s+/g, " ");

        // 売上高
        if (nullFields.includes("revenue") && !data.revenue) {
          const revenueMatch = financialText.match(/(?:売上高|売上)[：:\s]*([\d,億万千]+円?)/i);
          if (revenueMatch) {
            const revenueValue = normalizeToThousandYen(revenueMatch[1], "");
            if (revenueValue !== null && isValidNumber(revenueValue)) {
              data.revenue = revenueValue;
              writeLog(`  [全国法人リスト] 売上高取得: ${revenueValue.toLocaleString()}千円`);
            }
          }
        }

        // 純利益
        if (nullFields.includes("netAssets") && !data.netAssets) {
          const netProfitMatch = financialText.match(/(?:純利益)[：:\s]*([\d,億万千]+円?)/i);
          if (netProfitMatch) {
            const netProfitValue = normalizeToThousandYen(netProfitMatch[1], "");
            if (netProfitValue !== null && isValidNumber(netProfitValue)) {
              // 純利益と純資産は異なるが、純利益が取得できた場合は記録
              // 純資産は別途取得が必要
            }
          }
        }
      }
    } catch {
      // 決算情報タブが見つからない場合は続行
    }

  } catch (error) {
    writeLog(`  [全国法人リスト] 詳細ページ取得エラー: ${(error as any)?.message}`);
  }

  return data;
}

/**
 * 住所を正規化して比較（都道府県、市区町村、番地などを比較）
 */
function normalizeAddress(address: string | null | undefined): string {
  if (!address) return "";
  // 都道府県、市区町村、番地などの主要部分を抽出
  return address
    .replace(/[〒０-９]/g, "") // 郵便番号と全角数字を除去
    .replace(/[都道府県市区町村]/g, "") // 都道府県市区町村を除去
    .replace(/\s+/g, "") // 空白を除去
    .trim();
}

/**
 * 住所が一致するかチェック（部分一致も許可）
 */
function isAddressMatch(address1: string | null | undefined, address2: string | null | undefined): boolean {
  if (!address1 || !address2) return false;
  const normalized1 = normalizeAddress(address1);
  const normalized2 = normalizeAddress(address2);
  if (normalized1.length === 0 || normalized2.length === 0) return false;
  
  // 完全一致または部分一致（どちらかがもう一方を含む）
  return normalized1.includes(normalized2) || normalized2.includes(normalized1);
}

/**
 * Alarmboxから企業情報を取得（法人番号がある場合は直接アクセス、ない場合は検索＋住所照合）
 */
async function scrapeFromAlarmbox(
  page: Page,
  companyName: string,
  nullFields: string[],
  corporateNumber?: string | null,
  headquartersAddress?: string | null,
  address?: string | null
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // 法人番号がある場合のみ直接企業詳細ページにアクセス
    if (corporateNumber) {
      const detailUrl = `https://alarmbox.jp/companyinfo/entities/${corporateNumber}`;
      writeLog(`  [Alarmbox] 法人番号で直接アクセス: ${detailUrl}`);
      
      await page.goto(detailUrl, {
        waitUntil: PAGE_WAIT_MODE,
        timeout: PAGE_TIMEOUT,
      });
      await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG * 2)); // 詳細ページの読み込みを待つ
      
      // ページが存在するかチェック（404エラーなど）
      const pageText = await page.textContent("body");
      if (pageText && (pageText.includes("ページが見つかりません") || pageText.includes("404") || pageText.includes("Not Found"))) {
        writeLog(`  [Alarmbox] 企業詳細ページが見つかりません: ${companyName} (法人番号: ${corporateNumber})`);
        // 法人番号での直接アクセスが失敗した場合は検索にフォールバック
      } else {
        // 詳細ページから情報を取得
        return await scrapeFromAlarmboxDetailPage(page, companyName, nullFields);
      }
    }
    
    // 法人番号がない場合、または直接アクセスが失敗した場合は検索フローを使用
    if (!companyName || companyName.trim().length === 0) {
      writeLog(`  [Alarmbox] 企業名が空のため検索をスキップ`);
      return data;
    }

    // 法人番号がない場合、または直接アクセスが失敗した場合は検索フローを使用
    await page.goto("https://alarmbox.jp/companyinfo/", {
      waitUntil: PAGE_WAIT_MODE,
      timeout: PAGE_TIMEOUT,
    });
    await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG * 2)); // ページ読み込みを待つ

    // 広告を閉じる（右上の×ボタンなど）
    try {
      const closeAdButtons = await page.$$('button:has-text("×"), button[aria-label*="閉じる"], .close, [class*="close"], button.close, [class*="ad-close"], [class*="modal-close"]');
      for (const closeBtn of closeAdButtons) {
        try {
          const isVisible = await closeBtn.isVisible();
          if (isVisible) {
            await closeBtn.click();
            await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300));
          }
        } catch {
          // 広告が存在しない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    // 検索テキストボックスを探す（複数のセレクターを試行）
    let searchInput: ElementHandle | null = null;
    const searchSelectors = [
      'input[aria-label*="法人番号または企業名"]',
      'input[aria-label*="企業名"]',
      'input[aria-label*="法人番号"]',
      'input[placeholder*="法人番号または企業名"]',
      'input[placeholder*="企業名"]',
      'input[placeholder*="法人番号"]',
      'input[name*="q"]',
      'input[name*="keyword"]',
      'input[type="text"][class*="search"]',
      'input[type="text"]',
      'input[type="search"]',
      '[role="textbox"]',
      'input'
    ];
    
    for (const selector of searchSelectors) {
      try {
        const elements = await page.$$(selector);
        for (const element of elements) {
          const isVisible = await element.isVisible();
          if (isVisible) {
            // 検索ボックスらしい要素か確認（フォーム内、または検索関連のテキストを含む）
            const ariaLabel = await element.getAttribute('aria-label');
            const placeholder = await element.getAttribute('placeholder');
            const name = await element.getAttribute('name');
            if (ariaLabel?.includes('法人番号') || ariaLabel?.includes('企業名') ||
                placeholder?.includes('法人番号') || placeholder?.includes('企業名') ||
                name?.includes('q') || name?.includes('keyword') ||
                selector === 'input[type="text"]' || selector === 'input[type="search"]' || selector === '[role="textbox"]') {
              searchInput = element;
              break;
            }
          }
        }
        if (searchInput) {
          break;
        }
      } catch {
        continue;
      }
    }

    if (searchInput) {
      // 企業名または法人番号を入力（法人番号がある場合は法人番号を優先）
      const searchQuery = corporateNumber || companyName;
      await searchInput.fill(searchQuery);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
    } else {
      writeLog(`  [Alarmbox] 検索ボックスが見つかりません: ${companyName}`);
      return data;
    }

    // 検索ボタンを押下（虫眼鏡マークの検索ボタンを優先）
    let searchButton: ElementHandle | null = null;
    const buttonSelectors = [
      'button:has(svg[class*="search"])', // 虫眼鏡マークの検索ボタンを優先
      'button:has(svg[aria-label*="検索"])',
      'button:has(svg[aria-label*="Search"])',
      'button[type="submit"]',
      'button:has-text("検索")',
      'button[aria-label*="検索"]',
      '[role="button"]:has-text("検索")',
      'form button',
      'form button[type="submit"]'
    ];
    
    for (const selector of buttonSelectors) {
      try {
        const buttons = await page.$$(selector);
        for (const btn of buttons) {
          const isVisible = await btn.isVisible();
          if (isVisible) {
            // 虫眼鏡マークの検索ボタンを優先
            const hasSearchIcon = await btn.$('svg[class*="search"], svg[aria-label*="検索"], svg[aria-label*="Search"]');
            const text = await btn.textContent();
            const ariaLabel = await btn.getAttribute('aria-label');
            if (hasSearchIcon || text?.includes('検索') || text?.includes('Search') || 
                ariaLabel?.includes('検索') || ariaLabel?.includes('Search') ||
                selector.includes('submit')) {
              searchButton = btn;
              break;
            }
          }
        }
        if (searchButton) {
          break;
        }
      } catch {
        continue;
      }
    }
    
    if (searchButton) {
      await searchButton.click();
      await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    } else {
      // 検索ボタンが見つからない場合はEnterキーで検索
      if (searchInput) {
        await searchInput.press('Enter');
        await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG));
      }
    }

    // 検索結果ページのHTMLを取得
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 検索結果の読み込みを待つ
    const html = await page.content();
    const $ = cheerio.load(html);
    
    // 企業リストから企業名と住所を照合して対象企業を特定
    let targetLink: { element: any; address: string } | null = null;
    const companyLinks = await page.$$(`a[href*="/entities/"], a[href*="/company/"]`);
    
    if (companyLinks.length > 0) {
      // 住所情報がある場合は照合、ない場合は企業名のみで判定
      const hasAddressInfo = (headquartersAddress && headquartersAddress.trim().length > 0) || 
                            (address && address.trim().length > 0);
      
      writeLog(`  [Alarmbox] 検索結果: ${companyLinks.length}件 (住所照合: ${hasAddressInfo ? "あり" : "なし"})`);
      
      for (const link of companyLinks) {
        try {
          const linkText = await link.textContent();
          
          // 企業名が一致するかチェック
          if (linkText && linkText.trim().includes(companyName)) {
            // 住所情報がある場合は照合
            if (hasAddressInfo) {
              // リンクの親要素から住所情報を取得
              const parentElement = await link.evaluateHandle((el: any) => {
                // 親要素を探す（div, li, tr, td, article, sectionなど）
                let parent = el.parentElement;
                let depth = 0;
                while (parent && depth < 5) {
                  if (parent.tagName === 'DIV' || parent.tagName === 'LI' || 
                      parent.tagName === 'TR' || parent.tagName === 'TD' ||
                      parent.tagName === 'ARTICLE' || parent.tagName === 'SECTION') {
                    return parent;
                  }
                  parent = parent.parentElement;
                  depth++;
                }
                return null;
              });
              
              if (parentElement && parentElement.asElement()) {
                const parentText = await parentElement.asElement()!.textContent();
                
                // 住所らしいテキストを探す（都道府県、市区町村、番地など）
                const addressPatterns = [
                  /([都道府県][^\s\n]+[市区町村][^\s\n]+[0-9０-９\-ー]+)/,
                  /([都道府県][^\s\n]+[市区町村][^\s\n]+)/,
                  /([都道府県][^\s\n]+)/,
                ];
                
                let foundAddress = "";
                if (parentText) {
                  for (const pattern of addressPatterns) {
                    const match = parentText.match(pattern);
                    if (match) {
                      foundAddress = match[1];
                      break;
                    }
                  }
                }
                
                // 住所が一致するかチェック
                if (foundAddress && (
                  isAddressMatch(foundAddress, headquartersAddress) ||
                  isAddressMatch(foundAddress, address)
                )) {
                  targetLink = { element: link, address: foundAddress };
                  writeLog(`  [Alarmbox] 企業名と住所が一致: ${companyName} (住所: ${foundAddress})`);
                  break;
                }
              }
            } else {
              // 住所情報がない場合は最初に企業名が一致したリンクを使用
              if (!targetLink) {
                targetLink = { element: link, address: "" };
                writeLog(`  [Alarmbox] 企業名で一致: ${companyName} (住所照合なし)`);
              }
            }
          }
        } catch (error) {
          // 個別のリンク処理エラーは無視して続行
          continue;
        }
      }
      
      // 住所照合で見つからなかった場合、企業名のみで最初の一致を選択
      if (!targetLink && companyLinks.length > 0) {
        for (const link of companyLinks) {
          const linkText = await link.textContent();
          if (linkText && linkText.trim().includes(companyName)) {
            targetLink = { element: link, address: "" };
            writeLog(`  [Alarmbox] 企業名のみで一致: ${companyName} (住所照合なし)`);
            break;
          }
        }
      }
    }
    
    if (targetLink) {
      await targetLink.element.click();
      await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    } else {
      writeLog(`  [Alarmbox] 企業リンクが見つかりません: ${companyName}`);
      return data;
    }

    // 企業詳細画面から情報を取得
    return await scrapeFromAlarmboxDetailPage(page, companyName, nullFields);

  } catch (error) {
    writeLog(`  [Alarmbox] エラー: ${(error as any)?.message}`);
  }

  return data;
}

/**
 * Alarmboxの詳細ページから情報を取得
 */
async function scrapeFromAlarmboxDetailPage(
  page: Page,
  companyName: string,
  nullFields: string[]
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // ページ読み込みを待つ
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG));

    // 広告を閉じる（右上の×ボタンなど）
    try {
      const closeAdButtons = await page.$$('button:has-text("×"), button[aria-label*="閉じる"], .close, [class*="close"], button.close, [class*="ad-close"], [class*="modal-close"]');
      for (const closeBtn of closeAdButtons) {
        try {
          const isVisible = await closeBtn.isVisible();
          if (isVisible) {
            await closeBtn.click();
            await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300));
          }
        } catch {
          // 広告が存在しない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 「{企業名}の企業情報」セクションから情報を取得
    const companyInfoSection = $(`section:contains("${companyName}"), div:contains("${companyName}"), .company-info, .entity-info`).first();
    const sectionText = companyInfoSection.length > 0 ? companyInfoSection.text() : text;

    // 電話番号
    if (nullFields.includes("phoneNumber") && !data.phoneNumber) {
      const phoneMatch = sectionText.match(/(?:電話番号|電話|TEL)[：:\s]*([0-9-()]{10,15})/i);
      if (phoneMatch) {
        const phone = phoneMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
        if (isValidPhoneNumber(phone)) {
          data.phoneNumber = phone;
          writeLog(`  [Alarmbox] 電話番号取得: ${phone}`);
        }
      }
    }

    // FAX
    if (nullFields.includes("fax") && !data.fax) {
      const faxMatch = sectionText.match(/(?:FAX|Fax|fax|ファックス)[：:\s]*([0-9-()]{10,15})/i);
      if (faxMatch) {
        const fax = faxMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
        if (isValidFax(fax)) {
          data.fax = fax;
          writeLog(`  [Alarmbox] FAX取得: ${fax}`);
        }
      }
    }

    // 代表者名
    if (nullFields.includes("representativeName") && !data.representativeName) {
      const repMatch = sectionText.match(/(?:代表者|代表取締役|社長)[：:\s]*([^\n\r]{1,50})/i);
      if (repMatch) {
        const repName = repMatch[1].trim();
        if (repName && repName.length > 0 && repName.length < 50 && !repName.includes('—') && !repName.includes('-')) {
          data.representativeName = repName;
          writeLog(`  [Alarmbox] 代表者名取得: ${repName}`);
        }
      }
    }

    // 住所
    if (nullFields.includes("address") || nullFields.includes("headquartersAddress")) {
      const addressMatch = sectionText.match(/(?:住所|所在地|本社)[：:\s]*([^\n\r]{10,100})/i);
      if (addressMatch) {
        const address = addressMatch[1].trim();
        if (address && address.length > 5 && address.length < 200 && !address.includes('—')) {
          if (nullFields.includes("address") && !data.address) {
            data.address = address;
            writeLog(`  [Alarmbox] 住所取得: ${address}`);
          }
          if (nullFields.includes("headquartersAddress") && !data.headquartersAddress) {
            data.headquartersAddress = address;
            writeLog(`  [Alarmbox] 本社住所取得: ${address}`);
          }
        }
      }
    }

    // URL
    if (nullFields.includes("companyUrl") && !data.companyUrl) {
      const urlMatch = sectionText.match(/(?:URL|ホームページ|ウェブサイト)[：:\s]*(https?:\/\/[^\s\n\r]+)/i);
      if (urlMatch) {
        const url = urlMatch[1].trim();
        if (isValidUrl(url)) {
          data.companyUrl = url;
          writeLog(`  [Alarmbox] URL取得: ${url}`);
        }
      }
      // リンク要素からも取得
      if (!data.companyUrl) {
        const urlLink = $('a[href^="http"]').not('a[href*="alarmbox"]').first();
        if (urlLink.length > 0) {
          const href = urlLink.attr('href');
          if (href && isValidUrl(href)) {
            data.companyUrl = href;
            writeLog(`  [Alarmbox] URL取得: ${href}`);
          }
        }
      }
    }

    // 資本金
    if (nullFields.includes("capitalStock") && !data.capitalStock) {
      const capitalMatch = sectionText.match(/(?:資本金)[：:\s]*([\d,億万千]+円?)/i);
      if (capitalMatch) {
        const capitalValue = normalizeToThousandYen(capitalMatch[1], "");
        if (capitalValue !== null && isValidNumber(capitalValue)) {
          data.capitalStock = capitalValue;
          writeLog(`  [Alarmbox] 資本金取得: ${capitalValue.toLocaleString()}千円`);
        }
      }
    }

    // 売上高
    if (nullFields.includes("revenue") && !data.revenue) {
      const revenueMatch = sectionText.match(/(?:売上高|売上)[：:\s]*([\d,億万千]+円?)/i);
      if (revenueMatch) {
        const revenueValue = normalizeToThousandYen(revenueMatch[1], "");
        if (revenueValue !== null && isValidNumber(revenueValue)) {
          data.revenue = revenueValue;
          writeLog(`  [Alarmbox] 売上高取得: ${revenueValue.toLocaleString()}千円`);
        }
      }
    }

    // 従業員数
    if (nullFields.includes("employeeCount") && !data.employeeCount) {
      const employeeMatch = sectionText.match(/(?:従業員数|従業員)[：:\s]*([\d,]+人?)/i);
      if (employeeMatch) {
        const employeeValue = parseInt(employeeMatch[1].replace(/[^\d]/g, ""), 10);
        if (!isNaN(employeeValue) && employeeValue > 0) {
          data.employeeCount = employeeValue;
          writeLog(`  [Alarmbox] 従業員数取得: ${employeeValue}人`);
        }
      }
    }

    // 設立日
    if (nullFields.includes("established") && !data.established) {
      const establishedMatch = sectionText.match(/(?:設立)[：:\s]*(\d{4}年\d{1,2}月\d{1,2}日?)/i);
      if (establishedMatch) {
        data.established = establishedMatch[1];
        writeLog(`  [Alarmbox] 設立日取得: ${establishedMatch[1]}`);
      }
    }

    // 業種
    if (nullFields.includes("industry") && !data.industry) {
      const industryMatch = sectionText.match(/(?:業種|事業内容)[：:\s]*([^\n\r]{1,100})/i);
      if (industryMatch) {
        const industry = industryMatch[1].trim();
        if (industry && industry.length > 0 && industry.length < 100 && !industry.includes('—')) {
          data.industry = industry;
          writeLog(`  [Alarmbox] 業種取得: ${industry}`);
        }
      }
    }

  } catch (error) {
    writeLog(`  [Alarmbox] 詳細ページ取得エラー: ${(error as any)?.message}`);
  }

  return data;
}

/**
 * 企業名から「株式会社」「有限会社」などの接頭辞を除去
 */
function removeCompanyPrefix(companyName: string): string {
  return companyName
    .replace(/^(株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|一般財団法人|特定非営利活動法人|医療法人|学校法人|宗教法人|社会福祉法人|協同組合|協業組合|企業組合|協同組合連合会)\s*/i, "")
    .trim();
}

/**
 * 経審（Ullet経審）から企業情報を取得
 */
async function scrapeFromUlletKeishin(
  page: Page,
  companyName: string,
  nullFields: string[],
  headquartersAddress?: string | null,
  address?: string | null
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // 企業名が空の場合はスキップ
    if (!companyName || companyName.trim().length === 0) {
      writeLog(`  [経審] 企業名が空のため検索をスキップ`);
      return data;
    }

    // 経審の検索ページにアクセス（正しいURLに修正）
    await page.goto("https://ullet.com/keishin/", {
      waitUntil: PAGE_WAIT_MODE,
      timeout: PAGE_TIMEOUT,
    });
    await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG * 2)); // ページ読み込みを待つ

    // ポップアップ・広告を閉じる（強化版）
    try {
      // モーダル、オーバーレイ、ポップアップを閉じる
      const popupSelectors = [
        'button:has-text("×")',
        'button[aria-label*="閉じる"]',
        'button[aria-label*="close"]',
        '.close',
        '[class*="close"]',
        'button.close',
        '[class*="ad-close"]',
        '[class*="modal-close"]',
        '[class*="popup-close"]',
        '[class*="overlay-close"]',
        'button[class*="close"]',
        '[id*="close"]',
        '[data-dismiss="modal"]',
        '[data-close]'
      ];
      
      for (const selector of popupSelectors) {
        try {
          await page.waitForSelector(selector, { timeout: 2000, state: 'visible' }).catch(() => {});
          const closeButtons = await page.$$(selector);
          for (const closeBtn of closeButtons) {
            try {
              const isVisible = await closeBtn.isVisible();
              if (isVisible) {
                await closeBtn.click({ timeout: 3000 }).catch(() => {});
                await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300));
              }
            } catch {
              // 個別のボタンクリックエラーは無視
            }
          }
        } catch {
          // セレクターが見つからない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    // 検索テキストボックスを探す（waitForSelectorで待機）
    let searchInput: ElementHandle | null = null;
    const searchSelectors = [
      'input[placeholder*="企業名"]',
      'input[placeholder*="許可番号"]',
      'input[placeholder*="企業名・許可番号"]',
      'input[name*="q"]',
      'input[name*="keyword"]',
      'input[name*="company"]',
      'input[type="text"][class*="search"]',
      'input[type="text"]',
      'input[type="search"]',
      '[role="textbox"]',
      'input'
    ];
    
    for (const selector of searchSelectors) {
      try {
        // 要素が表示されるまで待機
        await page.waitForSelector(selector, { timeout: 5000, state: 'visible' }).catch(() => {});
        const elements = await page.$$(selector);
        for (const element of elements) {
          const isVisible = await element.isVisible();
          if (isVisible) {
            const ariaLabel = await element.getAttribute('aria-label');
            const placeholder = await element.getAttribute('placeholder');
            const name = await element.getAttribute('name');
            if (ariaLabel?.includes('企業名') || ariaLabel?.includes('許可番号') ||
                placeholder?.includes('企業名') || placeholder?.includes('許可番号') ||
                name?.includes('q') || name?.includes('keyword') || name?.includes('company') ||
                selector === 'input[type="text"]' || selector === 'input[type="search"]' || selector === '[role="textbox"]') {
              searchInput = element;
              break;
            }
          }
        }
        if (searchInput) {
          break;
        }
      } catch {
        continue;
      }
    }

    if (searchInput) {
      // 企業名から「株式会社」などを除去して入力
      const searchQuery = removeCompanyPrefix(companyName);
      await searchInput.fill(searchQuery);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
    } else {
      writeLog(`  [経審] 検索ボックスが見つかりません: ${companyName}`);
      return data;
    }

    // 「検索」ボタンを押下（waitForSelectorで待機）
    let searchButton: ElementHandle | null = null;
    const buttonSelectors = [
      'button:has-text("検索")',
      'button[type="submit"]',
      'button[aria-label*="検索"]',
      'form button',
      'form button[type="submit"]',
      'input[type="submit"]'
    ];
    
    // 検索ボタンが表示されるまで待機
    try {
      await page.waitForSelector('button:has-text("検索"), button[type="submit"]', { timeout: 5000, state: 'visible' }).catch(() => {});
    } catch {
      // ボタンが見つからない場合は続行
    }
    
    for (const selector of buttonSelectors) {
      try {
        const buttons = await page.$$(selector);
        for (const btn of buttons) {
          const isVisible = await btn.isVisible();
          if (isVisible) {
            const text = await btn.textContent();
            const ariaLabel = await btn.getAttribute('aria-label');
            if (text?.includes('検索') || text?.includes('Search') || 
                ariaLabel?.includes('検索') || ariaLabel?.includes('Search') ||
                selector.includes('submit')) {
              searchButton = btn;
              break;
            }
          }
        }
        if (searchButton) {
          break;
        }
      } catch {
        continue;
      }
    }
    
    if (searchButton) {
      await searchButton.click();
      await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    } else {
      // 検索ボタンが見つからない場合はEnterキーで検索
      if (searchInput) {
        await searchInput.press('Enter');
        await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG));
      }
    }

    // 検索結果ページのHTMLを取得
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 検索結果の読み込みを待つ
    const html = await page.content();
    const $ = cheerio.load(html);
    
    // 企業リストから企業名と住所を照合して対象企業を特定
    let targetLink: { element: any; address: string } | null = null;
    const companyLinks = await page.$$(`a[href*="/company/"], a[href*="/keishin/"], a:has-text("${companyName}")`);
    
    if (companyLinks.length > 0) {
      // 住所情報がある場合は照合、ない場合は企業名のみで判定
      const hasAddressInfo = (headquartersAddress && headquartersAddress.trim().length > 0) || 
                            (address && address.trim().length > 0);
      
      writeLog(`  [経審] 検索結果: ${companyLinks.length}件 (住所照合: ${hasAddressInfo ? "あり" : "なし"})`);
      
      for (const link of companyLinks) {
        try {
          const linkText = await link.textContent();
          const searchQuery = removeCompanyPrefix(companyName);
          
          // 企業名が一致するかチェック（接頭辞を除去した名前で比較）
          if (linkText && (linkText.trim().includes(companyName) || linkText.trim().includes(searchQuery))) {
            // 住所情報がある場合は照合
            if (hasAddressInfo) {
              // リンクの親要素から住所情報を取得
              const parentElement = await link.evaluateHandle((el) => {
                let parent = el.parentElement;
                let depth = 0;
                while (parent && depth < 5) {
                  if (parent.tagName === 'DIV' || parent.tagName === 'LI' || 
                      parent.tagName === 'TR' || parent.tagName === 'TD' ||
                      parent.tagName === 'ARTICLE' || parent.tagName === 'SECTION') {
                    return parent;
                  }
                  parent = parent.parentElement;
                  depth++;
                }
                return null;
              });
              
              if (parentElement && parentElement.asElement()) {
                const parentText = await parentElement.asElement()!.textContent();
                
                // 住所らしいテキストを探す（都道府県、市区町村、番地など）
                const addressPatterns = [
                  /([都道府県][^\s\n]+[市区町村][^\s\n]+[0-9０-９\-ー]+)/,
                  /([都道府県][^\s\n]+[市区町村][^\s\n]+)/,
                  /([都道府県][^\s\n]+)/,
                ];
                
                let foundAddress = "";
                if (parentText) {
                  for (const pattern of addressPatterns) {
                    const match = parentText.match(pattern);
                    if (match) {
                      foundAddress = match[1];
                      break;
                    }
                  }
                }
                
                // 住所が一致するかチェック
                if (foundAddress && (
                  isAddressMatch(foundAddress, headquartersAddress) ||
                  isAddressMatch(foundAddress, address)
                )) {
                  targetLink = { element: link, address: foundAddress };
                  writeLog(`  [経審] 企業名と住所が一致: ${companyName} (住所: ${foundAddress})`);
                  break;
                }
              }
            } else {
              // 住所情報がない場合は最初に企業名が一致したリンクを使用
              if (!targetLink) {
                targetLink = { element: link, address: "" };
                writeLog(`  [経審] 企業名で一致: ${companyName} (住所照合なし)`);
              }
            }
          }
        } catch (error) {
          // 個別のリンク処理エラーは無視して続行
          continue;
        }
      }
      
      // 住所照合で見つからなかった場合、企業名のみで最初の一致を選択
      if (!targetLink && companyLinks.length > 0) {
        for (const link of companyLinks) {
          const linkText = await link.textContent();
          const searchQuery = removeCompanyPrefix(companyName);
          if (linkText && (linkText.trim().includes(companyName) || linkText.trim().includes(searchQuery))) {
            targetLink = { element: link, address: "" };
            writeLog(`  [経審] 企業名のみで一致: ${companyName} (住所照合なし)`);
            break;
          }
        }
      }
    }
    
    if (targetLink) {
      await targetLink.element.click();
      await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
    } else {
      writeLog(`  [経審] 企業リンクが見つかりません: ${companyName}`);
      return data;
    }

    // 企業詳細画面から情報を取得
    return await scrapeFromUlletKeishinDetailPage(page, companyName, nullFields);

  } catch (error) {
    writeLog(`  [経審] エラー: ${(error as any)?.message}`);
  }

  return data;
}

/**
 * 経審（Ullet経審）の詳細ページから情報を取得
 */
async function scrapeFromUlletKeishinDetailPage(
  page: Page,
  companyName: string,
  nullFields: string[]
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // ページ読み込みを待つ
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG));

    // 広告を閉じる（右上の×ボタンなど）
    try {
      const closeAdButtons = await page.$$('button:has-text("×"), button[aria-label*="閉じる"], .close, [class*="close"], button.close, [class*="ad-close"], [class*="modal-close"]');
      for (const closeBtn of closeAdButtons) {
        try {
          const isVisible = await closeBtn.isVisible();
          if (isVisible) {
            await closeBtn.click();
            await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300));
          }
        } catch {
          // 広告が存在しない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 財務情報を取得（経審は財務情報が豊富）
    // 売上高
    if (nullFields.includes("revenue") && !data.revenue) {
      const revenueMatch = text.match(/(?:売上高|売上)[：:\s]*([\d,]+)\s*(千円|万円|億円|円)/i);
      if (revenueMatch) {
        const revenueValue = parseFloat(revenueMatch[1].replace(/,/g, ""));
        const unit = revenueMatch[2];
        let revenue = revenueValue;
        if (unit.includes("億")) revenue = revenueValue * 100000;
        else if (unit.includes("万")) revenue = revenueValue * 10;
        else if (unit.includes("円") && !unit.includes("千")) revenue = revenueValue / 1000;
        if (revenue > 0) {
          data.revenue = Math.round(revenue);
          writeLog(`  [経審] 売上高取得: ${data.revenue.toLocaleString()}千円`);
        }
      }
    }

    // 資本金
    if (nullFields.includes("capitalStock") && !data.capitalStock) {
      const capitalMatch = text.match(/(?:資本金|資本)[：:\s]*([\d,]+)\s*(千円|万円|億円|円)/i);
      if (capitalMatch) {
        const capitalValue = parseFloat(capitalMatch[1].replace(/,/g, ""));
        const unit = capitalMatch[2];
        let capital = capitalValue;
        if (unit.includes("億")) capital = capitalValue * 100000;
        else if (unit.includes("万")) capital = capitalValue * 10;
        else if (unit.includes("円") && !unit.includes("千")) capital = capitalValue / 1000;
        if (capital > 0) {
          data.capitalStock = Math.round(capital);
          writeLog(`  [経審] 資本金取得: ${data.capitalStock.toLocaleString()}千円`);
        }
      }
    }

    // 総資産
    if (nullFields.includes("totalAssets") && !data.totalAssets) {
      const totalAssetsMatch = text.match(/(?:総資産)[：:\s]*([\d,]+)\s*(千円|万円|億円|円)/i);
      if (totalAssetsMatch) {
        const totalAssetsValue = parseFloat(totalAssetsMatch[1].replace(/,/g, ""));
        const unit = totalAssetsMatch[2];
        let totalAssets = totalAssetsValue;
        if (unit.includes("億")) totalAssets = totalAssetsValue * 100000;
        else if (unit.includes("万")) totalAssets = totalAssetsValue * 10;
        else if (unit.includes("円") && !unit.includes("千")) totalAssets = totalAssetsValue / 1000;
        if (totalAssets > 0) {
          data.totalAssets = Math.round(totalAssets);
          writeLog(`  [経審] 総資産取得: ${data.totalAssets.toLocaleString()}千円`);
        }
      }
    }

    // 総負債
    if (nullFields.includes("totalLiabilities") && !data.totalLiabilities) {
      const totalLiabilitiesMatch = text.match(/(?:総負債)[：:\s]*([\d,]+)\s*(千円|万円|億円|円)/i);
      if (totalLiabilitiesMatch) {
        const totalLiabilitiesValue = parseFloat(totalLiabilitiesMatch[1].replace(/,/g, ""));
        const unit = totalLiabilitiesMatch[2];
        let totalLiabilities = totalLiabilitiesValue;
        if (unit.includes("億")) totalLiabilities = totalLiabilitiesValue * 100000;
        else if (unit.includes("万")) totalLiabilities = totalLiabilitiesValue * 10;
        else if (unit.includes("円") && !unit.includes("千")) totalLiabilities = totalLiabilitiesValue / 1000;
        if (totalLiabilities > 0) {
          data.totalLiabilities = Math.round(totalLiabilities);
          writeLog(`  [経審] 総負債取得: ${data.totalLiabilities.toLocaleString()}千円`);
        }
      }
    }

    // 純資産
    if (nullFields.includes("netAssets") && !data.netAssets) {
      const netAssetsMatch = text.match(/(?:純資産)[：:\s]*([\d,]+)\s*(千円|万円|億円|円)/i);
      if (netAssetsMatch) {
        const netAssetsValue = parseFloat(netAssetsMatch[1].replace(/,/g, ""));
        const unit = netAssetsMatch[2];
        let netAssets = netAssetsValue;
        if (unit.includes("億")) netAssets = netAssetsValue * 100000;
        else if (unit.includes("万")) netAssets = netAssetsValue * 10;
        else if (unit.includes("円") && !unit.includes("千")) netAssets = netAssetsValue / 1000;
        if (netAssets > 0) {
          data.netAssets = Math.round(netAssets);
          writeLog(`  [経審] 純資産取得: ${data.netAssets.toLocaleString()}千円`);
        }
      }
    }

    // 営業利益
    if (nullFields.includes("operatingIncome") && !data.operatingIncome) {
      const operatingIncomeMatch = text.match(/(?:営業利益)[：:\s]*([\d,]+)\s*(千円|万円|億円|円)/i);
      if (operatingIncomeMatch) {
        const operatingIncomeValue = parseFloat(operatingIncomeMatch[1].replace(/,/g, ""));
        const unit = operatingIncomeMatch[2];
        let operatingIncome = operatingIncomeValue;
        if (unit.includes("億")) operatingIncome = operatingIncomeValue * 100000;
        else if (unit.includes("万")) operatingIncome = operatingIncomeValue * 10;
        else if (unit.includes("円") && !unit.includes("千")) operatingIncome = operatingIncomeValue / 1000;
        if (operatingIncome !== 0) {
          data.operatingIncome = Math.round(operatingIncome);
          writeLog(`  [経審] 営業利益取得: ${data.operatingIncome.toLocaleString()}千円`);
        }
      }
    }

    // 従業員数
    if (nullFields.includes("employeeCount") && !data.employeeCount) {
      const employeeMatch = text.match(/(?:従業員数|社員数|従業員)[：:\s]*([\d,]+)\s*(人|名)/i);
      if (employeeMatch) {
        const employeeValue = parseInt(employeeMatch[1].replace(/,/g, ""), 10);
        if (employeeValue > 0) {
          data.employeeCount = employeeValue;
          writeLog(`  [経審] 従業員数取得: ${employeeValue}人`);
        }
      }
    }

    // 業種
    if (nullFields.includes("industry") && !data.industry) {
      const industryMatch = text.match(/(?:業種|業界)[：:\s]*([^\n\r]{1,100})/i);
      if (industryMatch) {
        const industry = industryMatch[1].trim();
        if (industry && industry.length > 0 && industry.length < 100) {
          data.industry = industry;
          writeLog(`  [経審] 業種取得: ${industry}`);
        }
      }
    }

    // 設立日
    if (nullFields.includes("established") && !data.established) {
      const establishedMatch = text.match(/(?:設立|設立日)[：:\s]*(\d{4})[年/-](\d{1,2})[月/-](\d{1,2})?[日]?/i);
      if (establishedMatch) {
        const year = establishedMatch[1];
        const month = establishedMatch[2].padStart(2, "0");
        const day = (establishedMatch[3] || "01").padStart(2, "0");
        data.established = `${year}-${month}-${day}`;
        writeLog(`  [経審] 設立日取得: ${data.established}`);
      }
    }

  } catch (error) {
    writeLog(`  [経審] 詳細ページ取得エラー: ${(error as any)?.message}`);
  }

  return data;
}

/**
 * uSonar YELLOWPAGEから企業情報を取得
 */
async function scrapeFromUsonarYellowpage(
  page: Page,
  companyName: string,
  nullFields: string[]
): Promise<Partial<ScrapedData>> {
  const data: Partial<ScrapedData> = {};

  try {
    // uSonar YELLOWPAGEの検索ページにアクセス
    await page.goto("https://yellowpage.usonar.co.jp/", {
      waitUntil: PAGE_WAIT_MODE,
      timeout: PAGE_TIMEOUT,
    });
    await sleep(Math.max(SLEEP_MS * 2, MIN_SLEEP_MS_LONG * 2)); // ページ読み込みを待つ

    // 広告を閉じる（右上の×ボタンなど）
    try {
      const closeAdButtons = await page.$$('button:has-text("×"), button[aria-label*="閉じる"], .close, [class*="close"], button.close, [class*="ad-close"]');
      for (const closeBtn of closeAdButtons) {
        try {
          const isVisible = await closeBtn.isVisible();
          if (isVisible) {
            await closeBtn.click();
            await sleep(Math.max(SLEEP_MS, FAST_MODE ? 200 : 300));
          }
        } catch {
          // 広告が存在しない場合はスキップ
        }
      }
    } catch {
      // 広告閉じ処理のエラーは無視
    }

    // 検索テキストボックスを探す（複数のセレクターを試行）
    let searchInput: ElementHandle | null = null;
    const searchSelectors = [
      'input[placeholder*="会社名、役員名"]',
      'input[placeholder*="企業名"]',
      'input[placeholder*="会社名"]',
      'input[type="text"][class*="search"]',
      'input[type="text"]',
      'input[name*="q"]',
      'input[name*="keyword"]',
      'input'
    ];
    
    for (const selector of searchSelectors) {
      try {
        searchInput = await page.$(selector);
        if (searchInput) {
          const isVisible = await searchInput.isVisible();
          if (isVisible) {
            break;
          }
        }
      } catch {
        continue;
      }
    }

    if (searchInput) {
      await searchInput.fill(companyName);
      await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）
    } else {
      writeLog(`  [uSonar YELLOWPAGE] 検索ボックスが見つかりません: ${companyName}`);
      return data;
    }

    // 虫眼鏡マーク（検索ボタン）を押下
    // まず検索ボタンを探す
    let searchButton = await page.$('button[type="submit"], button:has(svg), button.search-button, [aria-label*="検索"]');
    if (!searchButton) {
      // 虫眼鏡アイコンを探す（検索ボックスの近く）
      const searchIcon = await page.$('svg[class*="search"], [class*="search-icon"], [class*="magnifying"], button:has-text("検索")');
      if (searchIcon) {
        await searchIcon.click();
      } else {
        // Enterキーで検索
        await page.keyboard.press("Enter");
      }
    } else {
      await searchButton.click();
    }
    
    await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）

    // 検索結果がない場合のチェック
    const pageText = await page.textContent("body");
    if (pageText && (pageText.includes("検索結果がありません") || pageText.includes("該当する企業はありません"))) {
      writeLog(`  [uSonar YELLOWPAGE] 検索結果なし: ${companyName}`);
      return data;
    }

    // 企業リストから企業名のリンクを探してクリック
    const companyLinks = await page.$$(`a:has-text("${companyName}"), a[href*="/company/"], a[href*="/detail/"]`);
    if (companyLinks.length > 0) {
      // 企業名が完全一致または部分一致するリンクを探す
      let foundLink = false;
      for (const link of companyLinks) {
        const linkText = await link.textContent();
        if (linkText && (linkText.trim().includes(companyName) || companyName.includes(linkText.trim()))) {
          await link.click();
          await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
          await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
          foundLink = true;
          break;
        }
      }
      if (!foundLink && companyLinks.length > 0) {
        // 完全一致が見つからない場合は最初のリンクをクリック
        await companyLinks[0].click();
        await page.waitForNavigation({ waitUntil: PAGE_WAIT_MODE, timeout: NAVIGATION_TIMEOUT }).catch(() => {});
        await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS_LONG)); // 最小待機時間（高速化モード対応）
      }
    } else {
      writeLog(`  [uSonar YELLOWPAGE] 企業リンクが見つかりません: ${companyName}`);
      return data;
    }

    const html = await page.content();
    const $ = cheerio.load(html);
    const text = $("body").text().replace(/\s+/g, " ");

    // 「本社情報」セクションから情報を取得
    const companyInfoSection = $('section:contains("本社情報"), div:contains("本社情報"), .company-info, .head-office-info').first();
    const sectionText = companyInfoSection.length > 0 ? companyInfoSection.text() : text;

    // 電話番号（nullフィールドに含まれている場合のみ）
    if (nullFields.includes("phoneNumber") && !data.phoneNumber) {
      // まず「本社情報」セクション内を探す
      const phoneInSection = companyInfoSection.find('*:contains("電話番号"), *:contains("電話")').first();
      if (phoneInSection.length > 0) {
        const phoneText = phoneInSection.text();
        const phoneMatch = phoneText.match(/(?:電話番号|電話|TEL|Tel)[：:\s]*([0-9-()]{10,15})/i);
        if (phoneMatch) {
          const phone = phoneMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
          if (isValidPhoneNumber(phone)) {
            data.phoneNumber = phone;
          }
        }
      }
      // セクション内で見つからない場合は全体から検索
      if (!data.phoneNumber) {
        const phoneMatch = sectionText.match(/(?:電話番号|電話|TEL|Tel)[：:\s]*([0-9-()]{10,15})/i);
        if (phoneMatch) {
          const phone = phoneMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
          if (isValidPhoneNumber(phone)) {
            data.phoneNumber = phone;
          }
        }
      }
    }

    // FAX（nullフィールドに含まれている場合のみ）
    if (nullFields.includes("fax") && !data.fax) {
      const faxMatch = sectionText.match(/(?:FAX|Fax|fax|ファックス)[：:\s]*([0-9-()]{10,15})/i);
      if (faxMatch) {
        const fax = faxMatch[1].replace(/[^\d-]/g, "").replace(/-/g, "-");
        if (isValidFax(fax)) {
          data.fax = fax;
        }
      }
    }

    // 代表者名（nullフィールドに含まれている場合のみ）
    if (nullFields.includes("representativeName") || nullFields.includes("executives")) {
      // 「代表者名」のラベルを探す
      const representativeInSection = companyInfoSection.find('*:contains("代表者名"), *:contains("代表者"), *:contains("代表取締役")').first();
      if (representativeInSection.length > 0) {
        const repText = representativeInSection.text();
        const representativeMatch = repText.match(/(?:代表者名|代表者|代表取締役|社長)[：:\s]*([^\n\r]{1,50})/i);
        if (representativeMatch) {
          const representativeName = representativeMatch[1].trim();
          // バリデーション付きでexecutivesに追加
          if (isValidExecutiveName(representativeName)) {
            // 接頭辞を除去
            const cleanName = representativeName.replace(/^(代表者|代表取締役|社長|取締役|監査役|執行役員|役員)[：:\s]*/i, "").trim();
            if (cleanName.length > 0) {
              if (!data.executives) {
                data.executives = [];
              }
              if (!data.executives.includes(cleanName)) {
                data.executives.push(cleanName);
              }
            }
          }
        }
      }
      // セクション内で見つからない場合は全体から検索
      if (!data.executives || data.executives.length === 0) {
        const representativeMatch = sectionText.match(/(?:代表者名|代表者|代表取締役|社長)[：:\s]*([^\n\r]{1,50})/i);
        if (representativeMatch) {
          const representativeName = representativeMatch[1].trim();
          // バリデーション付きでexecutivesに追加
          if (isValidExecutiveName(representativeName)) {
            // 接頭辞を除去
            const cleanName = representativeName.replace(/^(代表者|代表取締役|社長|取締役|監査役|執行役員|役員)[：:\s]*/i, "").trim();
            if (cleanName.length > 0) {
              if (!data.executives) {
                data.executives = [];
              }
              if (!data.executives.includes(cleanName)) {
                data.executives.push(cleanName);
              }
            }
          }
        }
      }
    }

    // 所在地（郵便番号、住所）
    if (nullFields.includes("headquartersAddress") || nullFields.includes("postalCode")) {
      // 「所在地」のラベルを探す
      const addressInSection = companyInfoSection.find('*:contains("所在地"), *:contains("住所")').first();
      if (addressInSection.length > 0) {
        const addressText = addressInSection.text();
        // 郵便番号
        const postalCodeMatch = addressText.match(/〒\s*(\d{3}-?\d{4})/);
        // 住所
        const addressMatch = addressText.match(/(?:所在地|住所)[：:\s]*([^\n\r]{10,100})/i);
        if (addressMatch) {
          const address = addressMatch[1].trim();
          if (address && address.length > 5 && address.length < 200) {
            if (nullFields.includes("headquartersAddress")) {
              data.headquartersAddress = address;
            }
          }
        }
      }
      // セクション内で見つからない場合は全体から検索
      if (!data.headquartersAddress) {
        const postalCodeMatch = sectionText.match(/〒\s*(\d{3}-?\d{4})/);
        const addressMatch = sectionText.match(/(?:所在地|住所|本社)[：:\s]*([^\n\r]{10,100})/i);
        if (addressMatch) {
          const address = addressMatch[1].trim();
          if (address && address.length > 5 && address.length < 200) {
            if (nullFields.includes("headquartersAddress")) {
              data.headquartersAddress = address;
            }
          }
        }
      }
    }

    // URL（companyUrl）
    const urlMatch = text.match(/(?:URL|ホームページ|ウェブサイト)[：:\s]*(https?:\/\/[^\s\n\r]+)/i);
    if (urlMatch) {
      const url = urlMatch[1].trim();
      if (isValidUrl(url)) {
        // companyUrlは直接保存しないが、必要に応じて保存可能
      }
    }

    // 資本金
    const capitalMatch = text.match(/(?:資本金)[：:\s]*([\d,億万千]+円?)/i);
    if (capitalMatch) {
      // 資本金は数値に変換して保存（必要に応じて）
      // 今回は取得のみ
    }

    // 設立
    const establishmentMatch = text.match(/(?:設立)[：:\s]*(\d{4}年\d{1,2}月\d{1,2}日?)/i);
    if (establishmentMatch) {
      // 設立日は取得のみ
    }

  } catch (error) {
    writeLog(`  [uSonar YELLOWPAGE] ${companyName} からの取得エラー: ${(error as any)?.message}`);
  }

  return data;
}

// ------------------------------
// メイン処理
// ------------------------------

/**
 * nullフィールドを取得（取得対象フィールドのみを返す）
 * CSVファイルから読み込んだnullフィールドリストを使用する
 */
function getNullFields(companyData: any, csvNullFields?: string[]): string[] {
  // CSVから読み込んだnullフィールドリストがある場合
  if (csvNullFields && csvNullFields.length > 0) {
    // CSVのnullフィールドリストと実際のFirestoreデータを照合
    // 既に値が入っているフィールドは除外する
    const actualNullFields: string[] = [];
    for (const field of csvNullFields) {
      const value = companyData[field];
      // 値がnull、undefined、空文字列、空配列の場合はnullフィールドとして扱う
      if (value === null || value === undefined || 
          (typeof value === "string" && value.trim() === "") ||
          (Array.isArray(value) && value.length === 0)) {
        actualNullFields.push(field);
      }
    }
    return actualNullFields;
  }

  // CSVがない場合は、従来通りDBからnullフィールドをチェック
  const nullFields: string[] = [];
  const fieldsToCheck = [
    "corporateNumber", "prefecture", "address", "phoneNumber", "fax", "email",
    "companyUrl", "contactFormUrl", "representativeName", "representativeKana",
    "representativeTitle", "representativeBirthDate", "representativePhone",
    "representativePostalCode", "representativeHomeAddress", "representativeRegisteredAddress",
    "representativeAlmaMater", "executives", "industry", "industryLarge",
    "industryMiddle", "industrySmall", "industryDetail", "capitalStock", "revenue",
    "operatingIncome", "totalAssets", "totalLiabilities", "netAssets", "listing",
    "marketSegment", "latestFiscalYearMonth", "fiscalMonth", "employeeCount",
    "factoryCount", "officeCount", "storeCount", "established", "clients",
    "suppliers", "shareholders", "banks",
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
 * null_fields_detailed配下のCSVファイルを読み込んで、企業IDごとにnullフィールドをグループ化
 */
interface CompanyNullFields {
  companyId: string;
  companyName: string;
  nullFields: string[];
}

async function loadNullFieldsFromCsv(): Promise<Map<string, CompanyNullFields>> {
  const nullFieldsMap = new Map<string, CompanyNullFields>();
  const csvDir = path.join(__dirname, "..", "null_fields_detailed");

  // CSVディレクトリが存在しない場合は空のMapを返す
  if (!fs.existsSync(csvDir)) {
    writeLog(`⚠️  CSVディレクトリが存在しません: ${csvDir}`);
    return nullFieldsMap;
  }

  // CSVファイルを取得
  const allCsvFiles = fs.readdirSync(csvDir)
    .filter(file => file.endsWith(".csv"))
    .map(file => ({
      name: file,
      path: path.join(csvDir, file),
      stats: fs.statSync(path.join(csvDir, file))
    }));
  
  // タイムスタンプ形式のファイル（null_fields_detailed_YYYY-MM-DDTHH-MM-SS.csv）を優先
  // 最新のファイルを最初に読み込む
  const timestampFiles = allCsvFiles.filter(f => /^null_fields_detailed_\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.csv$/.test(f.name));
  const otherFiles = allCsvFiles.filter(f => !/^null_fields_detailed_\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}\.csv$/.test(f.name));
  
  // タイムスタンプファイルを新しい順にソート（最新のファイルを最初に）
  timestampFiles.sort((a, b) => b.name.localeCompare(a.name));
  
  // その他のファイルをソート（0001, 0002, ... の順）
  otherFiles.sort((a, b) => a.name.localeCompare(b.name));
  
  // 全てのCSVファイルを読み込む（タイムスタンプファイルとその他ファイルの両方）
  let csvFiles: string[];
  if (timestampFiles.length > 0) {
    // 全てのタイムスタンプファイルを読み込む
    csvFiles = [...timestampFiles.map(f => f.name), ...otherFiles.map(f => f.name)];
    writeLog(`タイムスタンプファイル ${timestampFiles.length}件 とその他ファイル ${otherFiles.length}件 の合計 ${csvFiles.length}件 を読み込みます`);
  } else {
    // タイムスタンプファイルがない場合は従来通り全て読み込む
    csvFiles = [...timestampFiles.map(f => f.name), ...otherFiles.map(f => f.name)];
    writeLog(`その他ファイル ${otherFiles.length}件 を読み込みます`);
  }
  
  // 逆順実行オプション（環境変数 REVERSE_ORDER=true で有効化）
  const REVERSE_ORDER = process.env.REVERSE_ORDER === "true";
  if (REVERSE_ORDER) {
    csvFiles.reverse(); // ファイルを逆順に
    writeLog(`⚠️  逆順実行モード: CSVファイルを逆順で読み込みます`);
  }
  
  writeLog(`CSVファイル数: ${csvFiles.length}件`);

  // スクレイピング対象フィールドのマッピング（CSVのフィールド名 → スクレイピング対象フィールド名）
  // 全てのフィールドをそのまま使用（マッピング不要）
  const fieldMapping: { [key: string]: string } = {
    // 基本情報
    "corporateNumber": "corporateNumber",
    "prefecture": "prefecture",
    "address": "address",
    "phoneNumber": "phoneNumber",
    "contactPhoneNumber": "phoneNumber", // contactPhoneNumberもphoneNumberとして扱う
    "fax": "fax",
    "email": "email",
    "companyUrl": "companyUrl",
    "contactFormUrl": "contactFormUrl",
    // 代表者情報
    "representativeName": "representativeName",
    "representativeKana": "representativeKana",
    "representativeTitle": "representativeTitle",
    "representativeBirthDate": "representativeBirthDate",
    "representativePhone": "representativePhone",
    "representativePostalCode": "representativePostalCode",
    "representativeHomeAddress": "representativeHomeAddress",
    "representativeRegisteredAddress": "representativeRegisteredAddress",
    "representativeAlmaMater": "representativeAlmaMater",
    // 役員・組織情報
    "executives": "executives",
    // 業種情報
    "industry": "industry",
    "industryLarge": "industryLarge",
    "industryMiddle": "industryMiddle",
    "industrySmall": "industrySmall",
    "industryDetail": "industryDetail",
    // 財務情報
    "capitalStock": "capitalStock",
    "revenue": "revenue",
    "operatingIncome": "operatingIncome",
    "totalAssets": "totalAssets",
    "totalLiabilities": "totalLiabilities",
    "netAssets": "netAssets",
    // 上場情報
    "listing": "listing",
    "marketSegment": "marketSegment",
    "latestFiscalYearMonth": "latestFiscalYearMonth",
    "fiscalMonth": "fiscalMonth",
    // 規模情報
    "employeeCount": "employeeCount",
    "factoryCount": "factoryCount",
    "officeCount": "officeCount",
    "storeCount": "storeCount",
    "established": "established",
    // 取引先情報
    "clients": "clients",
    "suppliers": "suppliers",
    "shareholders": "shareholders",
    "banks": "banks",
  };

  let totalRows = 0;
  const totalFiles = csvFiles.length;
  const progressInterval = Math.max(1, Math.floor(totalFiles / 100)); // 1%ごと、または100ファイルごとに進捗を表示
  
  for (let i = 0; i < csvFiles.length; i++) {
    const csvFile = csvFiles[i];
    const csvPath = path.join(csvDir, csvFile);
    
    // 進捗を表示（100ファイルごと、または1%ごと）
    if (i % progressInterval === 0 || i === csvFiles.length - 1) {
      const progress = ((i + 1) / totalFiles * 100).toFixed(1);
      writeLog(`CSV読み込み進捗: ${i + 1}/${totalFiles} ファイル (${progress}%) - 現在: ${csvFile}`);
    }
    
    try {
      // LIMITが設定されている場合、実際にデータが取得できた企業数がLIMITに達するまで続けるため、
      // より多くの企業から選択できるように、LIMITの1000倍（1企業あたり約20フィールドと仮定）の行数を読み込む
      const LIMIT_COMPANIES = process.env.LIMIT ? parseInt(process.env.LIMIT, 10) : null;
      const maxRows = LIMIT_COMPANIES ? LIMIT_COMPANIES * 1000 : null; // 1000倍に増やして、より多くの企業から選択できるようにする
      
      // 大きなファイルをストリーミングで読み込む（メモリエラーを回避）
      // 配列に保存せず、ストリーミング中に直接処理することでメモリ使用量を削減
      const fileStream = createReadStream(csvPath, { encoding: "utf-8" });
      const parser = parseStream({
        columns: true,
        skip_empty_lines: true,
      });
      
      let rowCount = 0;
      
      // ストリーミングでCSVをパースし、即座に処理（メモリに保存しない）
      for await (const record of fileStream.pipe(parser)) {
        // LIMITが設定されている場合、指定行数まで読み込む
        if (maxRows && rowCount >= maxRows) {
          break;
        }
        
        totalRows++;
        const companyId = (record as { companyId: string; companyName: string; nullFieldName: string }).companyId;
        const companyName = (record as { companyId: string; companyName: string; nullFieldName: string }).companyName || "";
        const nullFieldName = (record as { companyId: string; companyName: string; nullFieldName: string }).nullFieldName;
        
        // スクレイピング対象フィールドかどうかをチェック
        const targetField = fieldMapping[nullFieldName];
        if (!targetField) {
          // スクレイピング対象外のフィールドはスキップ
          rowCount++;
          // 進捗表示（10万行ごと）
          if (rowCount % 100000 === 0) {
            writeLog(`  CSV読み込み中: ${rowCount.toLocaleString()}行読み込み済み (${csvFile})`);
          }
          continue;
        }

        if (!nullFieldsMap.has(companyId)) {
          nullFieldsMap.set(companyId, {
            companyId,
            companyName,
            nullFields: [],
          });
        }

        const companyNullFields = nullFieldsMap.get(companyId)!;
        // 重複を避ける
        if (!companyNullFields.nullFields.includes(targetField)) {
          companyNullFields.nullFields.push(targetField);
        }
        
        rowCount++;
        
        // 進捗表示（10万行ごと）
        if (rowCount % 100000 === 0) {
          writeLog(`  CSV読み込み中: ${rowCount.toLocaleString()}行読み込み済み (${csvFile})`);
        }
      }
      
      writeLog(`  CSV読み込み完了: ${rowCount.toLocaleString()}行読み込み (${csvFile})`);
    } catch (error) {
      writeLog(`⚠️  CSVファイル読み込みエラー (${csvFile}): ${(error as any)?.message}`);
      // エラーが発生しても次のファイルを処理する
    }
  }

  writeLog(`CSVから読み込んだ企業数: ${nullFieldsMap.size}件`);
  writeLog(`CSV総行数: ${totalRows}行`);
  return nullFieldsMap;
}

/**
 * 企業情報を収集
 */
async function collectCompanyData(
  browser: Browser,
  companyId: string,
  companyData: any,
  csvNullFields?: string[]
): Promise<Partial<ScrapedData>> {
  const page = await browser.newPage();
  const result: Partial<ScrapedData> = {};

  try {
    const companyName = companyData.name || "";
    const homepageUrl = companyData.companyUrl || null;
    const corporateNumber = companyData.corporateNumber || null;
    const nikkeiCode = companyData.nikkeiCode || null;
    const representativeName = companyData.representativeName || null;
    const industry = companyData.industry || "";
    const headquartersAddress = companyData.headquartersAddress || "";

    // nullフィールドを取得（CSVから読み込んだリストを使用、既に取得済みのフィールドは除外）
    const nullFields = getNullFields(companyData, csvNullFields);
    writeLog(`\n[${companyId}] ${companyName} の情報を収集中...`);
    
    // CSVから読み込んだnullフィールド数と実際に取得が必要なフィールド数を比較
    if (csvNullFields && csvNullFields.length > 0) {
      const skippedCount = csvNullFields.length - nullFields.length;
      if (skippedCount > 0) {
        writeLog(`  → CSVのnullフィールド: ${csvNullFields.length}件 → 既に取得済み: ${skippedCount}件 → 取得必要: ${nullFields.length}件`);
      } else {
        writeLog(`  → 不足フィールド: ${nullFields.length}件 (${nullFields.join(", ")})`);
      }
    } else {
      writeLog(`  → 不足フィールド: ${nullFields.length}件 (${nullFields.join(", ")})`);
    }
    
    // nullフィールドがない場合はスキップ
    if (nullFields.length === 0) {
      writeLog(`  → 取得対象フィールドがないためスキップ（既に全て取得済み）`);
      return result;
    }

    // 1. HPから情報取得
    if (homepageUrl) {
      writeLog(`  → HPから取得: ${homepageUrl}`);
      const hpData = await scrapeFromHomepage(page, homepageUrl, companyName);
      Object.assign(result, hpData);
      await sleep(SLEEP_MS);
    }

    // 2. 電話番号が未取得の場合、キャリタス就活 or マイナビから取得（nullフィールドのみ）
    if (nullFields.includes("phoneNumber") && !result.phoneNumber) {
      writeLog(`  → 電話番号をキャリタス就活から取得`);
      const careeritasData = await scrapeFromCareeritas(page, companyName, nullFields);
      if (careeritasData.phoneNumber) {
        result.phoneNumber = careeritasData.phoneNumber;
      }
      await sleep(SLEEP_MS);

      if (!result.phoneNumber) {
        writeLog(`  → 電話番号をマイナビから取得`);
        const mynaviData = await scrapeFromMynavi(page, companyName, nullFields);
        if (mynaviData.phoneNumber) {
          result.phoneNumber = mynaviData.phoneNumber;
        }
        await sleep(SLEEP_MS);
      }
    }

    // 3. マイナビからメール、株主、業種、取引先を取得（nullフィールドのみ）
    const needsFromMynavi = nullFields.some(f => 
      ["email", "shareholders", "industryLarge", "industryMiddle", "industrySmall", "industryDetail", "clients"].includes(f)
    );
    if (needsFromMynavi) {
      writeLog(`  → マイナビから取得`);
      const mynaviData = await scrapeFromMynavi(page, companyName, nullFields);
      if (nullFields.includes("email") && mynaviData.email && !result.email) result.email = mynaviData.email;
      if (nullFields.includes("shareholders") && mynaviData.shareholders) result.shareholders = mynaviData.shareholders;
      if (nullFields.includes("industryLarge") && mynaviData.industryLarge && !result.industryLarge) result.industryLarge = mynaviData.industryLarge;
      if (nullFields.includes("industryMiddle") && mynaviData.industryMiddle && !result.industryMiddle) result.industryMiddle = mynaviData.industryMiddle;
      if (nullFields.includes("industrySmall") && mynaviData.industrySmall && !result.industrySmall) result.industrySmall = mynaviData.industrySmall;
      if (nullFields.includes("industryDetail") && mynaviData.industryDetail && !result.industryDetail) result.industryDetail = mynaviData.industryDetail;
      if (nullFields.includes("clients") && mynaviData.clients) {
        result.clients = [...(result.clients || []), ...mynaviData.clients].slice(0, 20);
      }
      await sleep(SLEEP_MS);
    }

    // 4. マイナビ2026から業種、取引先（より詳細）を取得（nullフィールドのみ）
    const needsFromMynavi2026 = nullFields.some(f => 
      ["industryLarge", "industryMiddle", "industrySmall", "industryDetail", "clients"].includes(f)
    );
    if (needsFromMynavi2026) {
      writeLog(`  → マイナビ2026から取得`);
      const mynavi2026Data = await scrapeFromMynavi2026(page, companyName, nullFields);
      if (nullFields.includes("industryLarge") && mynavi2026Data.industryLarge && !result.industryLarge) result.industryLarge = mynavi2026Data.industryLarge;
      if (nullFields.includes("industryMiddle") && mynavi2026Data.industryMiddle && !result.industryMiddle) result.industryMiddle = mynavi2026Data.industryMiddle;
      if (nullFields.includes("industrySmall") && mynavi2026Data.industrySmall && !result.industrySmall) result.industrySmall = mynavi2026Data.industrySmall;
      if (nullFields.includes("industryDetail") && mynavi2026Data.industryDetail && !result.industryDetail) result.industryDetail = mynavi2026Data.industryDetail;
      if (nullFields.includes("clients") && mynavi2026Data.clients) {
        result.clients = [...(result.clients || []), ...mynavi2026Data.clients].slice(0, 20);
      }
      await sleep(SLEEP_MS);
    }

    // 5. 業種が食品関連の場合、日本食糧新聞から役員・代表者生年月日を取得
    if (industry.includes("食品") || industry.includes("飲食") || industry.includes("フード")) {
      writeLog(`  → 日本食糧新聞から取得（食品関連）`);
      const nihonShokuryoData = await scrapeFromNihonShokuryo(page, companyName, representativeName || undefined);
      if (nihonShokuryoData.executives) {
        result.executives = [...(result.executives || []), ...nihonShokuryoData.executives].slice(0, 20);
      }
      if (nihonShokuryoData.representativeBirthDate && !result.representativeBirthDate) {
        result.representativeBirthDate = nihonShokuryoData.representativeBirthDate;
      }
      await sleep(SLEEP_MS);
    }

    // 6. 役員が未取得の場合、uSonar YELLOWPAGE → 日経コンパスから取得
    const needsExecutives = nullFields.includes("executives") || nullFields.includes("representativeName");
    if (needsExecutives && (!result.executives || result.executives.length === 0)) {
      writeLog(`  → 役員をuSonar YELLOWPAGEから取得`);
      const usonarData = await scrapeFromUsonarYellowpage(page, companyName, nullFields);
      if (usonarData.executives && usonarData.executives.length > 0) {
        result.executives = [...(result.executives || []), ...usonarData.executives].slice(0, 20);
      }
      await sleep(SLEEP_MS);
    }

    if (needsExecutives && (!result.executives || result.executives.length === 0)) {
      writeLog(`  → 日経コンパスから役員取得`);
      const officers = await scrapeOfficersFromNikkeiCompass(page, companyName);
      if (officers) {
        result.executives = officers;
      }
      await sleep(SLEEP_MS);
    }

    // 7. 営業利益を取得（就活会議 → バフェットコード → 官報決算DBの順）（nullフィールドのみ）
    if (nullFields.includes("operatingIncome") && !result.operatingIncome) {
      writeLog(`  → 就活会議から営業利益取得`);
      const operatingIncome1 = await scrapeOperatingIncomeFromShukatsu(page, companyName);
      if (operatingIncome1 !== null) {
        result.operatingIncome = operatingIncome1;
      }
      await sleep(SLEEP_MS);
    }

    if (nullFields.includes("operatingIncome") && !result.operatingIncome) {
      writeLog(`  → バフェットコードから営業利益取得`);
      const operatingIncome2 = await scrapeOperatingIncomeFromBuffett(page, companyName, nikkeiCode || undefined);
      if (operatingIncome2 !== null) {
        result.operatingIncome = operatingIncome2;
      }
      await sleep(SLEEP_MS);
    }

    if (!result.operatingIncome && nullFields.includes("operatingIncome")) {
      writeLog(`  → 官報決算DBから営業利益取得`);
      const operatingIncome3 = await scrapeOperatingIncomeFromCatr(page, companyName, corporateNumber || undefined);
      if (operatingIncome3 !== null) {
        result.operatingIncome = operatingIncome3;
      }
      await sleep(SLEEP_MS);
    }

    // 8. 企業INDEXナビから情報を取得（法人番号がある場合は直接アクセス）
    const needsFromCnavi = nullFields.some(f => 
      ["companyUrl", "contactFormUrl", "phoneNumber", "fax", "representativeName", "revenue", "capitalStock", 
       "employeeCount", "established", "clients", "listing", "headquartersAddress"].includes(f)
    );
    if (needsFromCnavi) {
      writeLog(`  → 企業INDEXナビから取得`);
      const cnaviData = await scrapeFromCnavi(page, companyName, headquartersAddress, nullFields, corporateNumber);
      if (nullFields.includes("companyUrl") && cnaviData.companyUrl && !result.companyUrl) result.companyUrl = cnaviData.companyUrl;
      if (nullFields.includes("contactFormUrl") && cnaviData.contactFormUrl && !result.contactFormUrl) result.contactFormUrl = cnaviData.contactFormUrl;
      if (nullFields.includes("phoneNumber") && cnaviData.phoneNumber && !result.phoneNumber) result.phoneNumber = cnaviData.phoneNumber;
      if (nullFields.includes("fax") && cnaviData.fax && !result.fax) result.fax = cnaviData.fax;
      if (nullFields.includes("representativeName") && cnaviData.representativeName && !result.representativeName) result.representativeName = cnaviData.representativeName;
      if (nullFields.includes("revenue") && cnaviData.revenue && !result.revenue) result.revenue = cnaviData.revenue;
      if (nullFields.includes("capitalStock") && cnaviData.capitalStock && !result.capitalStock) result.capitalStock = cnaviData.capitalStock;
      if (nullFields.includes("employeeCount") && cnaviData.employeeCount && !result.employeeCount) result.employeeCount = cnaviData.employeeCount;
      if (nullFields.includes("established") && cnaviData.established && !result.established) result.established = cnaviData.established;
      if (nullFields.includes("clients") && cnaviData.clients) {
        result.clients = [...(result.clients || []), ...cnaviData.clients].slice(0, 20);
      }
      if (nullFields.includes("listing") && cnaviData.listing && !result.listing) result.listing = cnaviData.listing;
      if (nullFields.includes("headquartersAddress") && cnaviData.headquartersAddress && !result.headquartersAddress) result.headquartersAddress = cnaviData.headquartersAddress;
      await sleep(SLEEP_MS);
    }

    // 9. 全国法人リストから情報を取得（法人番号がある場合は直接アクセス）
    const needsFromHoujin = nullFields.some(f => 
      ["phoneNumber", "companyUrl", "representativeName", "established", "address", "headquartersAddress", "industry", "revenue"].includes(f)
    );
    if (needsFromHoujin) {
      writeLog(`  → 全国法人リストから取得`);
      const houjinData = await scrapeFromHoujin(page, companyName, nullFields, corporateNumber);
      if (nullFields.includes("phoneNumber") && houjinData.phoneNumber && !result.phoneNumber) result.phoneNumber = houjinData.phoneNumber;
      if (nullFields.includes("companyUrl") && houjinData.companyUrl && !result.companyUrl) result.companyUrl = houjinData.companyUrl;
      if (nullFields.includes("representativeName") && houjinData.representativeName && !result.representativeName) result.representativeName = houjinData.representativeName;
      if (nullFields.includes("established") && houjinData.established && !result.established) result.established = houjinData.established;
      if (nullFields.includes("address") && houjinData.address && !result.address) result.address = houjinData.address;
      if (nullFields.includes("headquartersAddress") && houjinData.headquartersAddress && !result.headquartersAddress) result.headquartersAddress = houjinData.headquartersAddress;
      if (nullFields.includes("industry") && houjinData.industry && !result.industry) result.industry = houjinData.industry;
      if (nullFields.includes("revenue") && houjinData.revenue && !result.revenue) result.revenue = houjinData.revenue;
      await sleep(SLEEP_MS);
    }

    // 10. Alarmboxから情報を取得（法人番号がある場合は直接アクセス、ない場合は検索＋住所照合）
    const needsFromAlarmbox = nullFields.some(f => 
      ["phoneNumber", "fax", "companyUrl", "representativeName", "address", "headquartersAddress", "capitalStock", "revenue", "employeeCount", "established", "industry"].includes(f)
    );
    if (needsFromAlarmbox) {
      writeLog(`  → Alarmboxから取得`);
      const alarmboxData = await scrapeFromAlarmbox(page, companyName, nullFields, corporateNumber, companyData.headquartersAddress, companyData.address);
      if (nullFields.includes("phoneNumber") && alarmboxData.phoneNumber && !result.phoneNumber) result.phoneNumber = alarmboxData.phoneNumber;
      if (nullFields.includes("fax") && alarmboxData.fax && !result.fax) result.fax = alarmboxData.fax;
      if (nullFields.includes("companyUrl") && alarmboxData.companyUrl && !result.companyUrl) result.companyUrl = alarmboxData.companyUrl;
      if (nullFields.includes("representativeName") && alarmboxData.representativeName && !result.representativeName) result.representativeName = alarmboxData.representativeName;
      if (nullFields.includes("address") && alarmboxData.address && !result.address) result.address = alarmboxData.address;
      if (nullFields.includes("headquartersAddress") && alarmboxData.headquartersAddress && !result.headquartersAddress) result.headquartersAddress = alarmboxData.headquartersAddress;
      if (nullFields.includes("capitalStock") && alarmboxData.capitalStock && !result.capitalStock) result.capitalStock = alarmboxData.capitalStock;
      if (nullFields.includes("revenue") && alarmboxData.revenue && !result.revenue) result.revenue = alarmboxData.revenue;
      if (nullFields.includes("employeeCount") && alarmboxData.employeeCount && !result.employeeCount) result.employeeCount = alarmboxData.employeeCount;
      if (nullFields.includes("established") && alarmboxData.established && !result.established) result.established = alarmboxData.established;
      if (nullFields.includes("industry") && alarmboxData.industry && !result.industry) result.industry = alarmboxData.industry;
      await sleep(SLEEP_MS);
    }

    // 11. 経審（Ullet経審）から情報を取得（財務情報が豊富）
    const needsFromUlletKeishin = nullFields.some(f => 
      ["revenue", "capitalStock", "totalAssets", "totalLiabilities", "netAssets", "operatingIncome", "employeeCount", "industry", "established"].includes(f)
    );
    if (needsFromUlletKeishin) {
      writeLog(`  → 経審（Ullet経審）から取得`);
      const ulletKeishinData = await scrapeFromUlletKeishin(page, companyName, nullFields, companyData.headquartersAddress, companyData.address);
      if (nullFields.includes("revenue") && ulletKeishinData.revenue && !result.revenue) result.revenue = ulletKeishinData.revenue;
      if (nullFields.includes("capitalStock") && ulletKeishinData.capitalStock && !result.capitalStock) result.capitalStock = ulletKeishinData.capitalStock;
      if (nullFields.includes("totalAssets") && ulletKeishinData.totalAssets && !result.totalAssets) result.totalAssets = ulletKeishinData.totalAssets;
      if (nullFields.includes("totalLiabilities") && ulletKeishinData.totalLiabilities && !result.totalLiabilities) result.totalLiabilities = ulletKeishinData.totalLiabilities;
      if (nullFields.includes("netAssets") && ulletKeishinData.netAssets && !result.netAssets) result.netAssets = ulletKeishinData.netAssets;
      if (nullFields.includes("operatingIncome") && ulletKeishinData.operatingIncome && !result.operatingIncome) result.operatingIncome = ulletKeishinData.operatingIncome;
      if (nullFields.includes("employeeCount") && ulletKeishinData.employeeCount && !result.employeeCount) result.employeeCount = ulletKeishinData.employeeCount;
      if (nullFields.includes("industry") && ulletKeishinData.industry && !result.industry) result.industry = ulletKeishinData.industry;
      if (nullFields.includes("established") && ulletKeishinData.established && !result.established) result.established = ulletKeishinData.established;
      await sleep(SLEEP_MS);
    }

    // 12. 代表者生年月日が未取得の場合、検索＋ニュース記事から取得
    if (!result.representativeBirthDate && representativeName) {
      writeLog(`  → ニュース記事から代表者生年月日取得`);
      const birthDate = await scrapeRepresentativeBirthDateFromNews(page, representativeName, companyName);
      if (birthDate) {
        result.representativeBirthDate = birthDate;
      }
      await sleep(SLEEP_MS);
    }

    // 取得できたフィールド数をカウント（null/undefined/空配列を除く）
    const validFields = Object.entries(result).filter(([key, value]) => {
      if (value === null || value === undefined) return false;
      if (Array.isArray(value) && value.length === 0) return false;
      if (typeof value === "string" && value.trim() === "") return false;
      return true;
    });
    
    writeLog(`  ✅ 収集完了: ${validFields.length} フィールド取得`);
    
    // 1フィールドでも取得できた場合は詳細をログに出力（コードの正常性確認のため）
    if (validFields.length > 0) {
      writeLog(`  📋 [${companyId}] スクレイピングで取得したデータ（${validFields.length}フィールド）:`);
      for (const [key, value] of validFields) {
        let logValue: string;
        if (Array.isArray(value)) {
          // 配列の場合は、全要素を表示（最大20件）
          const displayItems = value.slice(0, 20);
          logValue = `[${value.length}件] ${displayItems.join(", ")}${value.length > 20 ? "..." : ""}`;
        } else if (typeof value === "string" && value.length > 200) {
          // 長い文字列の場合は200文字まで表示
          logValue = `${value.substring(0, 200)}... (長さ: ${value.length}文字)`;
        } else if (typeof value === "number") {
          // 数値の場合はカンマ区切りで表示
          logValue = value.toLocaleString();
        } else {
          logValue = String(value);
        }
        writeLog(`    ✓ ${key}: ${logValue}`);
      }
    } else {
      writeLog(`  ⚠️  [${companyId}] スクレイピングで取得できたデータはありませんでした`);
    }

  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    writeLog(`  ❌ [${companyId}] エラー: ${errorMessage}`);
  } finally {
    await page.close();
  }

  return result;
}

/**
 * データをCloudSQLに保存
 */
async function saveToCloudSQL(
  companyId: string,
  updates: { [key: string]: any }
): Promise<void> {
  if (!pgPool) {
    return;
  }

  try {
    // フィールド名をスネークケースに変換するマッピング
    const fieldMapping: { [key: string]: string } = {
      companyUrl: "company_url",
      contactFormUrl: "contact_form_url",
      phoneNumber: "phone_number",
      contactPhoneNumber: "contact_phone_number",
      representativeName: "representative_name",
      representativeKana: "representative_kana",
      representativeTitle: "representative_title",
      representativeBirthDate: "representative_birth_date",
      representativePhone: "representative_phone",
      representativePostalCode: "representative_postal_code",
      representativeHomeAddress: "representative_home_address",
      representativeRegisteredAddress: "representative_registered_address",
      representativeAlmaMater: "representative_alma_mater",
      industryLarge: "industry_large",
      industryMiddle: "industry_middle",
      industrySmall: "industry_small",
      industryDetail: "industry_detail",
      capitalStock: "capital_stock",
      revenue: "revenue",
      operatingIncome: "operating_income",
      totalAssets: "total_assets",
      totalLiabilities: "total_liabilities",
      netAssets: "net_assets",
      employeeCount: "employee_count",
      factoryCount: "factory_count",
      officeCount: "office_count",
      storeCount: "store_count",
      headquartersAddress: "headquarters_address",
    };

    // 更新用のSQLを構築
    const setClauses: string[] = [];
    const values: any[] = [];
    let paramIndex = 1;

    for (const [fieldName, fieldValue] of Object.entries(updates)) {
      const dbFieldName = fieldMapping[fieldName] || fieldName;
      
      // 配列フィールドの処理
      if (Array.isArray(fieldValue)) {
        setClauses.push(`${dbFieldName} = $${paramIndex}`);
        values.push(fieldValue);
        paramIndex++;
      }
      // JSONBフィールドの処理
      else if (typeof fieldValue === "object" && fieldValue !== null) {
        setClauses.push(`${dbFieldName} = $${paramIndex}`);
        values.push(JSON.stringify(fieldValue));
        paramIndex++;
      }
      // 通常の値
      else {
        setClauses.push(`${dbFieldName} = $${paramIndex}`);
        values.push(fieldValue);
        paramIndex++;
      }
    }

    if (setClauses.length === 0) {
      return;
    }

    // UPDATE文を実行
    const updateQuery = `
      UPDATE companies 
      SET ${setClauses.join(", ")}, updated_at = NOW()
      WHERE id = $${paramIndex}
    `;
    values.push(companyId);

    await pgPool.query(updateQuery, values);
    writeLog(`  ✅ [${companyId}] CloudSQL保存完了: ${setClauses.length} フィールド`);
  } catch (error) {
    writeLog(`  ❌ [${companyId}] CloudSQL保存エラー: ${error instanceof Error ? error.message : String(error)}`);
    throw error;
  }
}

/**
 * データをCloudSQLに保存
 */
async function saveToFirestore(
  companyId: string,
  companyName: string,
  scrapedData: Partial<ScrapedData>
): Promise<{ updatedFields: string[]; status: "success" | "failed" | "no_data"; errorMessage?: string }> {
  const updatedFields: string[] = [];
  let status: "success" | "failed" | "no_data" = "no_data";
  let errorMessage: string | undefined;

  try {
    // 保存直前のURL/住所クリーニング（最優先）
    scrapedData = sanitizeScrapedDataForSave(scrapedData);

    const updates: { [key: string]: any } = {};

    // フィールドマッピング（検証付き）
    if (scrapedData.phoneNumber && isValidPhoneNumber(scrapedData.phoneNumber)) {
      updates.phoneNumber = scrapedData.phoneNumber;
      updates.contactPhoneNumber = scrapedData.phoneNumber;
      updatedFields.push("phoneNumber", "contactPhoneNumber");
      writeLog(`  ✅ [${companyId}] 電話番号取得: ${scrapedData.phoneNumber}`);
    } else if (scrapedData.phoneNumber) {
      writeLog(`  ⚠️  [${companyId}] 不正な電話番号をスキップ: ${scrapedData.phoneNumber}`);
    }
    
    if (scrapedData.fax && isValidFax(scrapedData.fax)) {
      updates.fax = scrapedData.fax;
      updatedFields.push("fax");
      writeLog(`  ✅ [${companyId}] FAX取得: ${scrapedData.fax}`);
    } else if (scrapedData.fax) {
      writeLog(`  ⚠️  [${companyId}] 不正なFAXをスキップ: ${scrapedData.fax}`);
    }
    
    if (scrapedData.companyUrl && isValidUrl(scrapedData.companyUrl)) {
      updates.companyUrl = scrapedData.companyUrl;
      updatedFields.push("companyUrl");
      writeLog(`  ✅ [${companyId}] HPのURL取得: ${scrapedData.companyUrl}`);
    } else if (scrapedData.companyUrl) {
      writeLog(`  ⚠️  [${companyId}] 不正なHPのURLをスキップ: ${scrapedData.companyUrl}`);
    }
    
    if (scrapedData.contactFormUrl && isValidUrl(scrapedData.contactFormUrl)) {
      updates.contactFormUrl = scrapedData.contactFormUrl;
      updatedFields.push("contactFormUrl");
      writeLog(`  ✅ [${companyId}] 問い合わせフォームURL取得: ${scrapedData.contactFormUrl}`);
    } else if (scrapedData.contactFormUrl) {
      writeLog(`  ⚠️  [${companyId}] 不正な問い合わせフォームURLをスキップ: ${scrapedData.contactFormUrl}`);
    }
    
    if (scrapedData.email && isValidEmail(scrapedData.email)) {
      updates.email = scrapedData.email;
      updatedFields.push("email");
      writeLog(`  ✅ [${companyId}] メールアドレス取得: ${scrapedData.email}`);
    } else if (scrapedData.email) {
      writeLog(`  ⚠️  [${companyId}] 不正なメールアドレスをスキップ: ${scrapedData.email}`);
    }
    if (scrapedData.sns && scrapedData.sns.length > 0) {
      // SNSをURLsフィールドに追加
      if (pgPool) {
        try {
          const result = await pgPool.query('SELECT urls FROM companies WHERE id = $1', [companyId]);
          const existingUrls = result.rows[0]?.urls || [];
          updates.urls = [...new Set([...existingUrls, ...scrapedData.sns])];
          updatedFields.push("urls");
        } catch (error) {
          // エラー時は新しいSNSのみを保存
          updates.urls = scrapedData.sns;
          updatedFields.push("urls");
        }
      } else {
        updates.urls = scrapedData.sns;
        updatedFields.push("urls");
      }
    }
    if (scrapedData.executives && scrapedData.executives.length > 0) {
      updates.executives = scrapedData.executives;
      updatedFields.push("executives");
      writeLog(`  ✅ [${companyId}] 役員取得: ${scrapedData.executives.length}件 (${scrapedData.executives.slice(0, 3).join(", ")}${scrapedData.executives.length > 3 ? "..." : ""})`);
    }
    if (scrapedData.shareholders && scrapedData.shareholders.length > 0) {
      updates.shareholders = scrapedData.shareholders;
      updatedFields.push("shareholders");
      writeLog(`  ✅ [${companyId}] 株主取得: ${scrapedData.shareholders.length}件 (${scrapedData.shareholders.slice(0, 3).join(", ")}${scrapedData.shareholders.length > 3 ? "..." : ""})`);
    }
    if (scrapedData.representativeHomeAddress) {
      updates.representativeHomeAddress = scrapedData.representativeHomeAddress;
      updatedFields.push("representativeHomeAddress");
      writeLog(`  ✅ [${companyId}] 代表者住所取得: ${scrapedData.representativeHomeAddress}`);
    }
    if (scrapedData.representativeBirthDate && isValidBirthDate(scrapedData.representativeBirthDate)) {
      updates.representativeBirthDate = scrapedData.representativeBirthDate;
      updatedFields.push("representativeBirthDate");
      writeLog(`  ✅ [${companyId}] 代表者生年月日取得: ${scrapedData.representativeBirthDate}`);
    } else if (scrapedData.representativeBirthDate) {
      writeLog(`  ⚠️  [${companyId}] 不正な代表者生年月日をスキップ: ${scrapedData.representativeBirthDate}`);
    }
    // 業種情報は検証してから保存
    if (scrapedData.industryLarge && isValidIndustry(scrapedData.industryLarge)) {
      updates.industryLarge = normalizeIndustry(scrapedData.industryLarge);
      updatedFields.push("industryLarge");
      writeLog(`  ✅ [${companyId}] 業種（大）取得: ${scrapedData.industryLarge}`);
    } else if (scrapedData.industryLarge) {
      writeLog(`  ⚠️  [${companyId}] 不正な業種（大）をスキップ: ${scrapedData.industryLarge.substring(0, 50)}...`);
    }
    
    if (scrapedData.industryMiddle && isValidIndustry(scrapedData.industryMiddle)) {
      updates.industryMiddle = normalizeIndustry(scrapedData.industryMiddle);
      updatedFields.push("industryMiddle");
      writeLog(`  ✅ [${companyId}] 業種（中）取得: ${scrapedData.industryMiddle}`);
    } else if (scrapedData.industryMiddle) {
      writeLog(`  ⚠️  [${companyId}] 不正な業種（中）をスキップ: ${scrapedData.industryMiddle.substring(0, 50)}...`);
    }
    
    if (scrapedData.industrySmall && isValidIndustry(scrapedData.industrySmall)) {
      updates.industrySmall = normalizeIndustry(scrapedData.industrySmall);
      updatedFields.push("industrySmall");
      writeLog(`  ✅ [${companyId}] 業種（小）取得: ${scrapedData.industrySmall}`);
    } else if (scrapedData.industrySmall) {
      writeLog(`  ⚠️  [${companyId}] 不正な業種（小）をスキップ: ${scrapedData.industrySmall.substring(0, 50)}...`);
    }
    
    if (scrapedData.industryDetail && isValidIndustry(scrapedData.industryDetail)) {
      updates.industryDetail = normalizeIndustry(scrapedData.industryDetail);
      updatedFields.push("industryDetail");
      writeLog(`  ✅ [${companyId}] 業種（細）取得: ${scrapedData.industryDetail}`);
    } else if (scrapedData.industryDetail) {
      writeLog(`  ⚠️  [${companyId}] 不正な業種（細）をスキップ: ${scrapedData.industryDetail.substring(0, 50)}...`);
    }
    if (scrapedData.suppliers && scrapedData.suppliers.length > 0) {
      updates.suppliers = scrapedData.suppliers;
      updatedFields.push("suppliers");
      writeLog(`  ✅ [${companyId}] 仕入れ先取得: ${scrapedData.suppliers.length}件 (${scrapedData.suppliers.slice(0, 3).join(", ")}${scrapedData.suppliers.length > 3 ? "..." : ""})`);
    }
    if (scrapedData.clients && scrapedData.clients.length > 0) {
      updates.clients = scrapedData.clients;
      updatedFields.push("clients");
      writeLog(`  ✅ [${companyId}] 取引先取得: ${scrapedData.clients.length}件 (${scrapedData.clients.slice(0, 3).join(", ")}${scrapedData.clients.length > 3 ? "..." : ""})`);
    }
    if (scrapedData.banks && scrapedData.banks.length > 0) {
      updates.banks = scrapedData.banks;
      updatedFields.push("banks");
      writeLog(`  ✅ [${companyId}] 取引先銀行取得: ${scrapedData.banks.length}件 (${scrapedData.banks.slice(0, 3).join(", ")}${scrapedData.banks.length > 3 ? "..." : ""})`);
    }
    if (scrapedData.operatingIncome !== undefined && scrapedData.operatingIncome !== null) {
      if (isValidNumber(scrapedData.operatingIncome)) {
        updates.operatingIncome = scrapedData.operatingIncome;
        updatedFields.push("operatingIncome");
        writeLog(`  ✅ [${companyId}] 営業利益取得: ${scrapedData.operatingIncome.toLocaleString()}千円`);
      } else {
        writeLog(`  ⚠️  [${companyId}] 不正な営業利益をスキップ: ${scrapedData.operatingIncome}`);
      }
    }

    // 基本情報（検証なしで保存）
    if (scrapedData.corporateNumber) {
      updates.corporateNumber = scrapedData.corporateNumber;
      updatedFields.push("corporateNumber");
      writeLog(`  ✅ [${companyId}] 法人番号取得: ${scrapedData.corporateNumber}`);
    }
    if (scrapedData.prefecture) {
      updates.prefecture = scrapedData.prefecture;
      updatedFields.push("prefecture");
      writeLog(`  ✅ [${companyId}] 都道府県取得: ${scrapedData.prefecture}`);
    }
    if (scrapedData.address) {
      updates.address = scrapedData.address;
      updatedFields.push("address");
      writeLog(`  ✅ [${companyId}] 住所取得: ${scrapedData.address}`);
    }
    if (scrapedData.companyUrl && isValidUrl(scrapedData.companyUrl)) {
      updates.companyUrl = scrapedData.companyUrl;
      updatedFields.push("companyUrl");
      writeLog(`  ✅ [${companyId}] 企業URL取得: ${scrapedData.companyUrl}`);
    }

    // 代表者情報（検証なしで保存）
    if (scrapedData.representativeName) {
      updates.representativeName = scrapedData.representativeName;
      updatedFields.push("representativeName");
      writeLog(`  ✅ [${companyId}] 代表者名取得: ${scrapedData.representativeName}`);
    }
    if (scrapedData.representativeKana) {
      updates.representativeKana = scrapedData.representativeKana;
      updatedFields.push("representativeKana");
      writeLog(`  ✅ [${companyId}] 代表者名（カナ）取得: ${scrapedData.representativeKana}`);
    }
    if (scrapedData.representativeTitle) {
      updates.representativeTitle = scrapedData.representativeTitle;
      updatedFields.push("representativeTitle");
      writeLog(`  ✅ [${companyId}] 代表者役職取得: ${scrapedData.representativeTitle}`);
    }
    if (scrapedData.representativePhone && isValidPhoneNumber(scrapedData.representativePhone)) {
      updates.representativePhone = scrapedData.representativePhone;
      updatedFields.push("representativePhone");
      writeLog(`  ✅ [${companyId}] 代表者電話番号取得: ${scrapedData.representativePhone}`);
    }
    if (scrapedData.representativePostalCode) {
      updates.representativePostalCode = scrapedData.representativePostalCode;
      updatedFields.push("representativePostalCode");
      writeLog(`  ✅ [${companyId}] 代表者郵便番号取得: ${scrapedData.representativePostalCode}`);
    }
    if (scrapedData.representativeRegisteredAddress) {
      updates.representativeRegisteredAddress = scrapedData.representativeRegisteredAddress;
      updatedFields.push("representativeRegisteredAddress");
      writeLog(`  ✅ [${companyId}] 代表者登録住所取得: ${scrapedData.representativeRegisteredAddress}`);
    }
    if (scrapedData.representativeAlmaMater) {
      updates.representativeAlmaMater = scrapedData.representativeAlmaMater;
      updatedFields.push("representativeAlmaMater");
      writeLog(`  ✅ [${companyId}] 代表者出身校取得: ${scrapedData.representativeAlmaMater}`);
    }

    // 業種情報
    if (scrapedData.industry) {
      updates.industry = scrapedData.industry;
      updatedFields.push("industry");
      writeLog(`  ✅ [${companyId}] 業種取得: ${scrapedData.industry}`);
    }

    // 財務情報（数値検証あり）
    if (scrapedData.capitalStock !== undefined && scrapedData.capitalStock !== null && isValidNumber(scrapedData.capitalStock)) {
      updates.capitalStock = scrapedData.capitalStock;
      updatedFields.push("capitalStock");
      writeLog(`  ✅ [${companyId}] 資本金取得: ${scrapedData.capitalStock.toLocaleString()}千円`);
    }
    if (scrapedData.revenue !== undefined && scrapedData.revenue !== null && isValidNumber(scrapedData.revenue)) {
      updates.revenue = scrapedData.revenue;
      updatedFields.push("revenue");
      writeLog(`  ✅ [${companyId}] 売上高取得: ${scrapedData.revenue.toLocaleString()}千円`);
    }
    if (scrapedData.totalAssets !== undefined && scrapedData.totalAssets !== null && isValidNumber(scrapedData.totalAssets)) {
      updates.totalAssets = scrapedData.totalAssets;
      updatedFields.push("totalAssets");
      writeLog(`  ✅ [${companyId}] 総資産取得: ${scrapedData.totalAssets.toLocaleString()}千円`);
    }
    if (scrapedData.totalLiabilities !== undefined && scrapedData.totalLiabilities !== null && isValidNumber(scrapedData.totalLiabilities)) {
      updates.totalLiabilities = scrapedData.totalLiabilities;
      updatedFields.push("totalLiabilities");
      writeLog(`  ✅ [${companyId}] 総負債取得: ${scrapedData.totalLiabilities.toLocaleString()}千円`);
    }
    if (scrapedData.netAssets !== undefined && scrapedData.netAssets !== null && isValidNumber(scrapedData.netAssets)) {
      updates.netAssets = scrapedData.netAssets;
      updatedFields.push("netAssets");
      writeLog(`  ✅ [${companyId}] 純資産取得: ${scrapedData.netAssets.toLocaleString()}千円`);
    }

    // 上場情報
    if (scrapedData.listing) {
      updates.listing = scrapedData.listing;
      updatedFields.push("listing");
      writeLog(`  ✅ [${companyId}] 上場区分取得: ${scrapedData.listing}`);
    }
    if (scrapedData.marketSegment) {
      updates.marketSegment = scrapedData.marketSegment;
      updatedFields.push("marketSegment");
      writeLog(`  ✅ [${companyId}] 市場区分取得: ${scrapedData.marketSegment}`);
    }
    if (scrapedData.latestFiscalYearMonth) {
      updates.latestFiscalYearMonth = scrapedData.latestFiscalYearMonth;
      updatedFields.push("latestFiscalYearMonth");
      writeLog(`  ✅ [${companyId}] 最新決算年月取得: ${scrapedData.latestFiscalYearMonth}`);
    }
    if (scrapedData.fiscalMonth) {
      updates.fiscalMonth = scrapedData.fiscalMonth;
      updatedFields.push("fiscalMonth");
      writeLog(`  ✅ [${companyId}] 決算月取得: ${scrapedData.fiscalMonth}`);
    }

    // 規模情報（数値検証あり）
    if (scrapedData.employeeCount !== undefined && scrapedData.employeeCount !== null && isValidNumber(scrapedData.employeeCount)) {
      updates.employeeCount = scrapedData.employeeCount;
      updatedFields.push("employeeCount");
      writeLog(`  ✅ [${companyId}] 従業員数取得: ${scrapedData.employeeCount}`);
    }
    if (scrapedData.factoryCount !== undefined && scrapedData.factoryCount !== null && isValidNumber(scrapedData.factoryCount)) {
      updates.factoryCount = scrapedData.factoryCount;
      updatedFields.push("factoryCount");
      writeLog(`  ✅ [${companyId}] 工場数取得: ${scrapedData.factoryCount}`);
    }
    if (scrapedData.officeCount !== undefined && scrapedData.officeCount !== null && isValidNumber(scrapedData.officeCount)) {
      updates.officeCount = scrapedData.officeCount;
      updatedFields.push("officeCount");
      writeLog(`  ✅ [${companyId}] オフィス数取得: ${scrapedData.officeCount}`);
    }
    if (scrapedData.storeCount !== undefined && scrapedData.storeCount !== null && isValidNumber(scrapedData.storeCount)) {
      updates.storeCount = scrapedData.storeCount;
      updatedFields.push("storeCount");
      writeLog(`  ✅ [${companyId}] 店舗数取得: ${scrapedData.storeCount}`);
    }
    if (scrapedData.established) {
      updates.established = scrapedData.established;
      updatedFields.push("established");
      writeLog(`  ✅ [${companyId}] 設立日取得: ${scrapedData.established}`);
    }

    // 本社住所
    if (scrapedData.headquartersAddress) {
      updates.headquartersAddress = scrapedData.headquartersAddress;
      updatedFields.push("headquartersAddress");
      writeLog(`  ✅ [${companyId}] 本社住所取得: ${scrapedData.headquartersAddress}`);
    }

    if (Object.keys(updates).length > 0) {
      // 保存前のログ出力（保存される値の詳細を記録）
      writeLog(`  💾 [${companyId}] CloudSQL保存開始: ${Object.keys(updates).length} フィールド`);
      writeLog(`  📝 [${companyId}] 保存されるフィールドと値:`);
      
      // データをログに出力
      for (const [fieldName, fieldValue] of Object.entries(updates)) {
        let logValue: string;
        if (Array.isArray(fieldValue)) {
          // 配列の場合は、全要素を表示（最大20件）
          const displayItems = fieldValue.slice(0, 20);
          logValue = `[${fieldValue.length}件] ${displayItems.join(", ")}${fieldValue.length > 20 ? "..." : ""}`;
        } else if (typeof fieldValue === "string" && fieldValue.length > 200) {
          // 長い文字列の場合は200文字まで表示
          logValue = `${fieldValue.substring(0, 200)}... (長さ: ${fieldValue.length}文字)`;
        } else if (typeof fieldValue === "number") {
          // 数値の場合はカンマ区切りで表示
          logValue = fieldValue.toLocaleString();
        } else {
          logValue = String(fieldValue);
        }
        writeLog(`    - ${fieldName}: ${logValue}`);
      }
      
      // CloudSQLに保存
      if (pgPool) {
        try {
          await saveToCloudSQL(companyId, updates);
          status = "success";
          writeLog(`  ✅ [${companyId}] CloudSQL保存完了: ${Object.keys(updates).length} フィールド (${updatedFields.join(", ")})`);
        } catch (error) {
          status = "failed";
          errorMessage = error instanceof Error ? error.message : String(error);
          writeLog(`  ❌ [${companyId}] CloudSQL保存エラー: ${errorMessage}`);
        }
      } else {
        status = "failed";
        errorMessage = "CloudSQL接続が初期化されていません";
        writeLog(`  ❌ [${companyId}] ${errorMessage}`);
      }
    } else {
      // データが取得できなかった場合はフラグを設定しない（次回も処理対象になる）
      status = "no_data";
      writeLog(`  ⚠️  [${companyId}] 保存するデータがありません（スクレイピングで取得できたデータも保存対象外でした）`);
    }

  } catch (error) {
    status = "failed";
    errorMessage = error instanceof Error ? error.message : String(error);
    writeLog(`  ❌ [${companyId}] Firestore保存エラー: ${errorMessage}`);
  }

  return { updatedFields, status, errorMessage };
}

/**
 * 必要なフィールドがすべて取得できているかチェック
 * 要件に基づく必須フィールドを定義
 */
const REQUIRED_FIELDS = [
  // 連絡先情報
  "phoneNumber", // 電話番号
  "fax", // FAX
  "contactFormUrl", // 問い合わせフォーム
  "email", // メール
  // 代表者情報
  "representativeHomeAddress", // 代表者住所
  "representativeBirthDate", // 代表者生年月日
  // 業種情報
  "industryLarge", // 業種（大）
  "industryMiddle", // 業種（中）
  "industrySmall", // 業種（小）
  "industryDetail", // 業種（細）
  // 関係会社情報
  "suppliers", // 仕入れ先
  "clients", // 取引先
  "banks", // 取引先銀行
  // その他
  "executives", // 役員
  "shareholders", // 株主
  "operatingIncome", // 営業利益
  // SNSは任意（取得できれば良い）
];

/**
 * フィールドが有効な値を持っているかチェック
 */
function hasValidValue(fieldName: string, value: any): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === "string" && value.trim() === "") return false;
  if (Array.isArray(value) && value.length === 0) return false;
  if (typeof value === "number" && isNaN(value)) return false;
  return true;
}

/**
 * スクレイピング処理済みかどうかを判定
 * 完全にDBに正しい値が入っている場合のみ処理済みとする
 */
function isAlreadyScraped(companyData: any): boolean {
  // extendedFieldsScrapedAtフラグがない場合は未処理
  if (!companyData.extendedFieldsScrapedAt) {
    return false;
  }

  // フラグがあっても、必要なフィールドが全て揃っているかチェック
  // ただし、営業利益など「数値がない場合はスキップ」とされているものは除外
  const criticalFields = REQUIRED_FIELDS.filter(field => field !== "operatingIncome");
  
  for (const field of criticalFields) {
    if (!hasValidValue(field, companyData[field])) {
      // 必要なフィールドが欠けている場合は再処理が必要
      return false;
    }
  }

  // 全ての重要なフィールドが揃っている場合のみ処理済み
  return true;
}

/**
 * CloudSQLから企業情報を取得
 */
function snakeToCamelKey(key: string): string {
  return key.replace(/_([a-z])/g, (_, c: string) => c.toUpperCase());
}

function normalizeCloudSqlRowToCamelCase<T extends Record<string, any>>(row: T): any {
  const out: Record<string, any> = {};
  for (const [k, v] of Object.entries(row)) {
    out[snakeToCamelKey(k)] = v;
  }
  return out;
}

async function getCompaniesFromCloudSQL(
  limit?: number,
  offset?: number
): Promise<Array<{ id: string; name: string; corporateNumber: string | null }>> {
  if (!pgPool) {
    throw new Error("PostgreSQL接続が初期化されていません");
  }

  try {
    // まずデータベースが存在するか確認し、存在しない場合はpostgresデータベースを使用
    let query = `
      SELECT id, name, corporate_number as "corporateNumber"
      FROM companies
      WHERE name IS NOT NULL AND name != ''
        AND (
          company_url IS NULL OR company_url = '' OR
          headquarters_address IS NULL OR headquarters_address = '' OR
          address IS NULL OR address = ''
        )
      ORDER BY id
    `;
    
    const params: any[] = [];
    if (limit) {
      query += ` LIMIT $${params.length + 1}`;
      params.push(limit);
    }
    if (offset) {
      query += ` OFFSET $${params.length + 1}`;
      params.push(offset);
    }

    const result = await pgPool.query(query, params);
    return result.rows;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    writeLog(`  ❌ CloudSQLから企業取得エラー: ${errorMessage}`);
    
    // データベースが存在しないエラーの場合、ヒントを表示
    if (errorMessage.includes("does not exist") && errorMessage.includes("database")) {
      writeLog(`  💡 ヒント: データベースが存在しない可能性があります。`);
      writeLog(`     環境変数 POSTGRES_DB を確認するか、デフォルトの "postgres" データベースを使用してください。`);
    }
    
    throw error;
  }
}

/**
 * CloudSQLから企業名と法人番号で企業情報を取得
 */
async function getCompanyFromCloudSQLByNameAndCorporateNumber(
  name: string,
  corporateNumber: string | null
): Promise<any | null> {
  if (!pgPool) {
    throw new Error("PostgreSQL接続が初期化されていません");
  }

  try {
    let query = `
      SELECT *
      FROM companies
      WHERE name = $1
    `;
    const params: any[] = [name];

    if (corporateNumber) {
      query += ` AND corporate_number = $2`;
      params.push(corporateNumber);
    } else {
      query += ` AND (corporate_number IS NULL OR corporate_number = '')`;
    }

    query += ` LIMIT 1`;

    const result = await pgPool.query(query, params);
    if (result.rows.length === 0) {
      return null;
    }
    // CloudSQLのsnake_caseカラムをcamelCaseに正規化して、以降の処理（getNullFields等）と整合させる
    return normalizeCloudSqlRowToCamelCase(result.rows[0]);
  } catch (error) {
    writeLog(`  ❌ CloudSQLから企業取得エラー (${name}, ${corporateNumber}): ${error instanceof Error ? error.message : String(error)}`);
    throw error;
  }
}

/**
 * メイン処理: 全企業の情報を収集（未処理の企業のみ）
 */
async function main() {
  let browser: Browser | null = null;

  try {
    writeLog("=".repeat(80));
    writeLog("Webスクレイピングを開始...");
    writeLog(`（CloudSQLから企業情報を取得して処理します）`);
    writeLog(`ログファイル: ${logFilePath}`);
    writeLog(`CSVファイル: ${csvFilePath}`);
    writeLog(`速度設定: SLEEP_MS=${SLEEP_MS}ms, PAGE_WAIT_MODE=${PAGE_WAIT_MODE}, PAGE_TIMEOUT=${PAGE_TIMEOUT}ms, NAVIGATION_TIMEOUT=${NAVIGATION_TIMEOUT}ms`);
    writeLog(`並列処理数: ${PARALLEL_WORKERS}並列${FAST_MODE ? "（高速化モード）" : "（精度を保つため）"}`);
    if (FAST_MODE) {
      writeLog(`⚠️  高速化モード: 待機時間とタイムアウトを短縮しています`);
    }
    
    // 開始位置を指定するオプション（環境変数から取得）
    const START_FROM_ID = process.env.START_FROM_COMPANY_ID || null;
    // エラー発生時にスキップするかどうか（環境変数から取得）
    const SKIP_ON_ERROR = process.env.SKIP_ON_ERROR === "true";
    // テスト用: 処理する企業数の上限（環境変数から取得）
    const LIMIT = process.env.LIMIT ? parseInt(process.env.LIMIT, 10) : null;
    if (START_FROM_ID) {
      writeLog(`指定された企業IDから開始: ${START_FROM_ID}`);
      if (SKIP_ON_ERROR) {
        writeLog(`エラー発生時はスキップして次の企業に進みます`);
      }
    }
    if (LIMIT) {
      writeLog(`⚠️  テストモード: ${LIMIT}件のみ処理します`);
    }
    
    writeLog("=".repeat(80));
    console.log("");

    // CloudSQL接続を初期化
    pgPool = initPostgres();
    
    if (!pgPool) {
      writeLog("❌ CloudSQL接続が初期化できませんでした。環境変数を確認してください。");
      process.exit(1);
    }

    // ブラウザを起動（メインブラウザ - ログインが必要なサイト用）
    browser = await chromium.launch({ headless: true });
    const loginPage = await browser.newPage();

    // バフェットコードにログイン（必要に応じて）
    writeLog("バフェットコードにログイン中...");
    await loginToBuffett(loginPage);
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）

    // 企業INDEXナビにログイン（必要に応じて）
    writeLog("企業INDEXナビにログイン中...");
    const cnaviLoginSuccess = await loginToCnavi(loginPage);
    if (!cnaviLoginSuccess) {
      writeLog("⚠️  企業INDEXナビのログインに失敗しましたが、処理を続行します");
    }
    await sleep(Math.max(SLEEP_MS, MIN_SLEEP_MS)); // 最小待機時間（高速化モード対応）

    await loginPage.close();

    // CloudSQLから企業を取得して処理
    writeLog("\nCloudSQLから企業を取得中...");
    
    let totalProcessed = 0;
    let totalSkipped = 0;
    let totalUpdated = 0;
    
    // CloudSQLから企業を取得（企業名と法人番号がある企業のみ）
    const BATCH_SIZE = 100;
    let offset = 0;
    let hasMore = true;
    
    while (hasMore) {
      const companies = await getCompaniesFromCloudSQL(BATCH_SIZE, offset);
      
      if (companies.length === 0) {
        hasMore = false;
        break;
      }
      
      writeLog(`\n処理中: ${offset + 1}〜${offset + companies.length}件目（合計 ${companies.length}件）`);
      
      // 各企業を処理
      for (const company of companies) {
        const companyId = company.id;
        const companyName = company.name || "";
        const corporateNumber = company.corporateNumber || null;
        
        try {
          // CloudSQLから企業情報を再取得（企業名+法人番号で特定）
          const companyData = await getCompanyFromCloudSQLByNameAndCorporateNumber(companyName, corporateNumber);
          
          if (!companyData) {
            writeLog(`  ⚠️  [${companyId}] 企業が見つかりません: ${companyName} / ${corporateNumber}`);
            totalSkipped++;
            continue;
          }
          
          // 既に処理済みの場合はスキップ
          if (isAlreadyScraped(companyData)) {
            totalSkipped++;
            continue;
          }
          
          // スクレイピングで情報を取得
          const scrapedData = await collectCompanyData(browser, companyId, companyData, undefined);
          
          // CloudSQLに保存
          const saveResult = await saveToFirestore(companyId, companyName, scrapedData);
          
          // CSVに記録
          writeCsvRow({
            companyId,
            companyName,
            scrapedFields: saveResult.updatedFields,
            status: saveResult.status,
            errorMessage: saveResult.errorMessage,
          });
          
          if (saveResult.status === "success") {
            totalUpdated++;
          }
          
          totalProcessed++;
          await sleep(SLEEP_MS);
          
          // LIMITが設定されている場合、成功数がLIMITに達したら終了
          if (LIMIT && totalUpdated >= LIMIT) {
            writeLog(`  ✅ 成功カウント制限に達しました（${totalUpdated}件）。処理を終了します。`);
            hasMore = false;
            break;
          }
          
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          writeLog(`  ❌ [${companyId}] 処理エラー: ${errorMessage}`);
          writeCsvRow({
            companyId,
            companyName,
            scrapedFields: [],
            status: "failed",
            errorMessage,
          });
          totalProcessed++;
          
          if (SKIP_ON_ERROR) {
            continue;
          } else {
            throw error;
          }
        }
      }
      
      // 次のバッチに進む
      offset += companies.length;
      
      // レート制限対策
      await sleep(SLEEP_MS * 2);
      
      // 進捗を表示
      writeLog(`  進捗: 処理済み ${totalProcessed} 件 / スキップ ${totalSkipped} 件 / 更新 ${totalUpdated} 件`);
    }
    
    // CSVファイルから読み込む場合はスキップ
    const nullFieldsMap = await loadNullFieldsFromCsv();
    if (nullFieldsMap.size > 0 && false) {
      let companyIds = Array.from(nullFieldsMap.keys());
      
      // 逆順実行オプション（環境変数 REVERSE_ORDER=true で有効化）
      const REVERSE_ORDER = process.env.REVERSE_ORDER === "true";
      if (REVERSE_ORDER) {
        // companyIdを数値として比較してソート（逆順）
        companyIds.sort((a, b) => {
          const numA = parseInt(a, 10);
          const numB = parseInt(b, 10);
          if (isNaN(numA) || isNaN(numB)) {
            // 数値でない場合は文字列比較
            return b.localeCompare(a);
          }
          return numB - numA; // 逆順（大きい順）
        });
        writeLog(`⚠️  逆順実行モード: 企業IDを逆順で処理します`);
      } else {
        // 通常は数値順（小さい順）でソート
        companyIds.sort((a, b) => {
          const numA = parseInt(a, 10);
          const numB = parseInt(b, 10);
          if (isNaN(numA) || isNaN(numB)) {
            // 数値でない場合は文字列比較
            return a.localeCompare(b);
          }
          return numA - numB; // 通常順（小さい順）
        });
      }
      
      // 開始位置が指定されている場合
      if (START_FROM_ID !== null && START_FROM_ID !== undefined) {
        const startId: string = START_FROM_ID as string; // 型アサーション
        if (REVERSE_ORDER) {
          // 逆順の場合は、指定ID以下の最大のIDから開始
          const startIndex = companyIds.findIndex((id: string) => id <= startId);
          if (startIndex >= 0) {
            writeLog(`指定された企業IDから開始（逆順）: ${startId} (${startIndex + 1}件目)`);
            companyIds.splice(0, startIndex);
          }
        } else {
          // 通常順の場合は、指定ID以上の最小のIDから開始
          const startIndex = companyIds.findIndex((id: string) => id >= startId);
          if (startIndex >= 0) {
            writeLog(`指定された企業IDから開始: ${startId} (${startIndex + 1}件目)`);
            companyIds.splice(0, startIndex);
          }
        }
      }

      // LIMITが設定されている場合、最低限その数の企業を処理できるように企業リストを準備
      // ただし、実際にデータが取得できた企業数がLIMITに達するまで続けるため、
      // 企業リストは制限せず、処理ループ内で制御する
      if (LIMIT) {
        writeLog(`⚠️  成功カウント制限モード: 実際にデータが取得できた企業が${LIMIT}件になるまで処理を続けます`);
      }

      writeLog(`\n処理対象企業数: ${companyIds.length}件`);

      // 並列処理用のキューを作成
      const processingQueue: Array<{ companyId: string; companyNullFields: CompanyNullFields }> = [];
      
      // nullフィールドがない企業を事前にフィルタリング
      const companiesToCheck = companyIds.filter(companyId => {
        const companyNullFields = nullFieldsMap.get(companyId)!;
        const csvNullFields = companyNullFields.nullFields;
        if (csvNullFields.length === 0) {
          totalSkipped++;
          return false;
        }
        return true;
      });

      writeLog(`\nnullフィールドがある企業数: ${companiesToCheck.length}件（並列でFirestoreから取得中...）`);

      // 並列処理でFirestoreからデータを取得（チャンク単位で処理、高速化）
      const BATCH_SIZE = 500; // 一度に処理する企業数（100→500に増加）
      const CONCURRENT_BATCHES = 20; // 同時に処理するバッチ数（10→20に増加）
      
      for (let i = 0; i < companiesToCheck.length; i += BATCH_SIZE * CONCURRENT_BATCHES) {
        const batchPromises: Promise<void>[] = [];
        
        for (let j = 0; j < CONCURRENT_BATCHES && i + j * BATCH_SIZE < companiesToCheck.length; j++) {
          const batchStart = i + j * BATCH_SIZE;
          const batchEnd = Math.min(batchStart + BATCH_SIZE, companiesToCheck.length);
          const batch = companiesToCheck.slice(batchStart, batchEnd);
          
          const batchPromise = (async () => {
            // FirestoreのgetAll()を使用してバッチ取得（高速化）
            // getAll()は最大500件までなので、500件を超える場合は分割
            const MAX_GETALL_SIZE = 500;
            
            for (let chunkStart = 0; chunkStart < batch.length; chunkStart += MAX_GETALL_SIZE) {
              const chunk = batch.slice(chunkStart, chunkStart + MAX_GETALL_SIZE);
              
              if (!db) {
                // Firestoreが初期化されていない場合はスキップ
                continue;
              }
              
              const docRefs = chunk.map(companyId => db.collection("companies_new").doc(companyId));
              
              try {
                // バッチで一括取得（最大500件まで）
                const docs = await db.getAll(...docRefs);
                
                for (let k = 0; k < docs.length; k++) {
                  const companyDoc = docs[k];
                  const companyId = chunk[k];
                  const companyNullFields = nullFieldsMap.get(companyId)!;
                  
                  if (!companyDoc.exists) {
                    totalSkipped++;
                    continue;
                  }

                  const companyData = companyDoc.data()!;
                  const companyName = companyData.name || companyNullFields.companyName || "";

                  // 既に処理済みの場合はスキップ
                  if (isAlreadyScraped(companyData)) {
                    totalSkipped++;
                    continue;
                  }

                  processingQueue.push({ companyId, companyNullFields });
                }
              } catch (error) {
                // getAll()が失敗した場合は個別取得にフォールバック
                // ログは最初の1回のみ出力（パフォーマンス向上）
                if (chunkStart === 0 && Math.random() < 0.1) {
                  writeLog(`  ⚠️  バッチ取得エラー、個別取得に切り替え: ${(error as any)?.message}`);
                }
                
                // 個別取得にフォールバック
                for (const companyId of chunk) {
                  const companyNullFields = nullFieldsMap.get(companyId)!;
                  
                  try {
                    if (!db) {
                      totalSkipped++;
                      continue;
                    }
                    const companyDoc = await db.collection("companies_new").doc(companyId).get();
                    
                    if (!companyDoc.exists) {
                      totalSkipped++;
                      continue;
                    }

                    const companyData = companyDoc.data()!;
                    const companyName = companyData.name || companyNullFields.companyName || "";

                    if (isAlreadyScraped(companyData)) {
                      totalSkipped++;
                      continue;
                    }

                    processingQueue.push({ companyId, companyNullFields });
                  } catch (err) {
                    totalSkipped++;
                  }
                }
              }
            }
          })();
          
          batchPromises.push(batchPromise);
        }
        
        await Promise.all(batchPromises);
        
        // 進捗を表示（5%ごと、より頻繁に表示）
        const processed = Math.min(i + BATCH_SIZE * CONCURRENT_BATCHES, companiesToCheck.length);
        const progress = (processed / companiesToCheck.length) * 100;
        const lastProgress = Math.floor((i / companiesToCheck.length) * 100 / 5);
        const currentProgress = Math.floor(progress / 5);
        if (currentProgress !== lastProgress || processed === companiesToCheck.length) {
          writeLog(`  進捗: ${progress.toFixed(1)}% (${processed.toLocaleString()}/${companiesToCheck.length.toLocaleString()}件) - 処理対象: ${processingQueue.length.toLocaleString()}件`);
        }
      }

      writeLog(`\n実際の処理対象企業数: ${processingQueue.length}件（並列処理: ${PARALLEL_WORKERS}並列）`);

      // LIMITが設定されている場合、実際にデータが取得できた企業数がLIMITに達したら終了
      const LIMIT_SUCCESS = LIMIT ? LIMIT : null;
      if (LIMIT_SUCCESS) {
        writeLog(`⚠️  成功カウント制限: 実際にデータが取得できた企業が${LIMIT_SUCCESS}件になるまで処理を続けます`);
      }

      // 並列処理を実行
      let queueIndex = 0;
      const processCompany = async (workerId: number): Promise<void> => {
        while (queueIndex < processingQueue.length) {
          // LIMIT_SUCCESSが設定されている場合、既に必要な成功数に達しているかチェック
          if (LIMIT_SUCCESS && totalUpdated >= LIMIT_SUCCESS) {
            writeLog(`  ✅ [Worker ${workerId}] 成功カウント制限に達しました（${totalUpdated}件）。処理を終了します。`);
            break;
          }

          const currentIndex = queueIndex++;
          if (currentIndex >= processingQueue.length) break;

          const { companyId, companyNullFields } = processingQueue[currentIndex];
          const csvNullFields = companyNullFields.nullFields;

          try {
            // Firestoreから企業データを取得（CSVファイルからの処理の場合のみ）
            if (!db) {
              totalSkipped++;
              continue;
            }
            const companyDoc = await db.collection("companies_new").doc(companyId).get();
            
            if (!companyDoc.exists) {
              writeLog(`  ⚠️  [Worker ${workerId}] [${companyId}] 企業データが存在しません`);
              totalSkipped++;
              continue;
            }

            const companyData = companyDoc.data()!;
            const companyName = companyData.name || companyNullFields.companyName || "";

            writeLog(`\n[Worker ${workerId}] [${companyId}] ${companyName}`);
            writeLog(`  → CSVから読み込んだnullフィールド: ${csvNullFields.join(", ")}`);

            // 情報を収集（CSVから読み込んだnullフィールドリストを渡す）
            // 各ワーカーで独立したページを使用（ログイン済みブラウザを共有）
            if (!browser) {
              throw new Error("ブラウザが初期化されていません");
            }
            const scrapedData = await collectCompanyData(browser, companyId, companyData, csvNullFields);

            // Firestoreに保存
            const saveResult = await saveToFirestore(companyId, companyName, scrapedData);

            // CSVに記録
            writeCsvRow({
              companyId,
              companyName,
              scrapedFields: saveResult.updatedFields,
              status: saveResult.status,
              errorMessage: saveResult.errorMessage,
            });

            // スクレイピングで1フィールドでも取得できた場合は成功としてカウント（コードの正常性確認のため）
            const scrapedFieldsCount = Object.keys(scrapedData).filter(key => {
              const value = scrapedData[key as keyof typeof scrapedData];
              if (value === null || value === undefined) return false;
              if (Array.isArray(value) && value.length === 0) return false;
              if (typeof value === "string" && value.trim() === "") return false;
              return true;
            }).length;
            
            if (saveResult.status === "success") {
              totalUpdated++;
              writeLog(`  ✅ [Worker ${workerId}] [${companyId}] データ取得成功！ 現在の成功数: ${totalUpdated}件`);
              
              // LIMIT_SUCCESSに達したら処理を終了
              if (LIMIT_SUCCESS && totalUpdated >= LIMIT_SUCCESS) {
                writeLog(`  ✅ 成功カウント制限（${LIMIT_SUCCESS}件）に達しました。処理を終了します。`);
                break;
              }
            } else if (scrapedFieldsCount > 0) {
              // スクレイピングで取得できたが、Firestoreに保存対象がなかった場合
              // コードの正常性確認のため、取得できたことをログに記録
              writeLog(`  📊 [Worker ${workerId}] [${companyId}] スクレイピングで${scrapedFieldsCount}フィールド取得できましたが、Firestore保存対象外でした`);
            }

            totalProcessed++;

            // レート制限（並列処理時は少し長めに）
            await sleep(Math.max(SLEEP_MS * (FAST_MODE ? 1.2 : 1.5), FAST_MODE ? 400 : 750));

          } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            writeLog(`  ❌ [Worker ${workerId}] [${companyId}] エラー: ${errorMessage}`);
            writeCsvRow({
              companyId,
              companyName: companyNullFields.companyName || "",
              scrapedFields: [],
              status: "failed",
              errorMessage,
            });
            totalProcessed++;
            
            // エラー発生時にスキップする設定の場合、次の企業に進む
            if (SKIP_ON_ERROR) {
              writeLog(`  ⏭️  [Worker ${workerId}] エラー発生のためスキップして次の企業に進みます`);
              continue;
            }
            
            // エラーが発生しても処理を続行（デフォルト動作）
            if (errorMessage.includes("browser") || errorMessage.includes("context") || errorMessage.includes("Target closed")) {
              writeLog(`  ⚠️  [Worker ${workerId}] 重大なエラーのため、処理を続行しますが注意が必要です`);
            }
          }
        }
      };

      // 並列ワーカーを起動
      const workers = Array.from({ length: PARALLEL_WORKERS }, (_, i) => processCompany(i + 1));
      await Promise.allSettled(workers);
    } else {
      // CSVがない場合は従来の方法（Firestoreから全企業を取得）
      // 注意: CloudSQLのみ使用する場合、この処理はスキップされます
      if (!db) {
        writeLog("\n⚠️  Firestoreが初期化されていないため、CSVファイルなしの処理はスキップされます");
        writeLog("CloudSQLからの直接取得のみがサポートされています");
        return;
      }
      
      writeLog("\n従来の方法（Firestoreから全企業を取得）で処理を続行します...");
      
      const BATCH_SIZE = 50;
      const companiesCollection = db
        .collection("companies_new")
        .orderBy(admin.firestore.FieldPath.documentId());

      let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
      let startedFromId = false;

      while (true) {
        let query = companiesCollection.limit(BATCH_SIZE);
        if (lastDoc) {
          query = query.startAfter(lastDoc);
        }

        const snapshot = await query.get();
        if (snapshot.empty) {
          break;
        }

        if (START_FROM_ID && !startedFromId) {
          const foundStartDoc = snapshot.docs.find(doc => doc.id >= START_FROM_ID);
          if (foundStartDoc) {
            writeLog(`指定された企業IDに到達: ${START_FROM_ID}`);
            lastDoc = foundStartDoc;
            startedFromId = true;
            query = companiesCollection.limit(BATCH_SIZE).startAfter(foundStartDoc);
            continue;
          } else {
            lastDoc = snapshot.docs[snapshot.docs.length - 1];
            continue;
          }
        }

        writeLog(`\nバッチ取得: ${snapshot.size} 件`);

        const unprocessedCompanies = snapshot.docs.filter((doc) => {
          const data = doc.data();
          return !isAlreadyScraped(data);
        });

        if (unprocessedCompanies.length === 0) {
          writeLog(`  → このバッチは全て処理済みのためスキップ`);
          totalSkipped += snapshot.size;
          lastDoc = snapshot.docs[snapshot.docs.length - 1];
          continue;
        }

        writeLog(`  → 未処理: ${unprocessedCompanies.length} 件`);

        for (const companyDoc of unprocessedCompanies) {
          const companyId = companyDoc.id;
          const companyData = companyDoc.data();
          const companyName = companyData.name || "";

          try {
            // nullフィールドを取得
            const nullFields = getNullFields(companyData);
            
            // 情報を収集（nullフィールドリストを渡す）
            const scrapedData = await collectCompanyData(browser, companyId, companyData, nullFields);

            // Firestoreに保存
            const saveResult = await saveToFirestore(companyId, companyName, scrapedData);

            // CSVに記録
            writeCsvRow({
              companyId,
              companyName,
              scrapedFields: saveResult.updatedFields,
              status: saveResult.status,
              errorMessage: saveResult.errorMessage,
            });

            if (saveResult.status === "success") {
              totalUpdated++;
            }

            totalProcessed++;
            await sleep(SLEEP_MS);

          } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            writeLog(`  ❌ [${companyId}] 処理エラー: ${errorMessage}`);
            writeCsvRow({
              companyId,
              companyName,
              scrapedFields: [],
              status: "failed",
              errorMessage,
            });
            totalProcessed++;
          }
        }

        // レート制限対策
        await sleep(SLEEP_MS * (FAST_MODE ? 1.5 : 2));
        
        totalSkipped += snapshot.size - unprocessedCompanies.length;
        lastDoc = snapshot.docs[snapshot.docs.length - 1];

        // 進捗を表示
        writeLog(`  進捗: 処理済み ${totalProcessed} 件 / スキップ ${totalSkipped} 件 / 更新 ${totalUpdated} 件`);
      }
    }

    // 処理完了
    writeLog("\n" + "=".repeat(80));
    writeLog("✅ 処理完了");
    writeLog(`処理企業数: ${totalProcessed} 件`);
    writeLog(`スキップ数: ${totalSkipped} 件`);
    writeLog(`更新数: ${totalUpdated} 件`);
    writeLog(`ログファイル: ${logFilePath}`);
    writeLog(`CSVファイル: ${csvFilePath}`);
    writeLog("=".repeat(80));

  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  } finally {
    if (browser) {
      await browser.close();
    }
    // PostgreSQL接続を閉じる
    if (pgPool) {
      await pgPool.end();
      writeLog("[CloudSQL] PostgreSQL接続を閉じました");
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

