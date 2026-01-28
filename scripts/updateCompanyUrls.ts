/* eslint-disable no-console */

/**
 * scripts/updateCompanyUrls.ts
 *
 * ✅ 目的
 * - Firestoreの companies_new コレクションから companyUrl が null のドキュメントを取得
 * - Playwrightを使用してブラウザでWeb検索を実行（DuckDuckGoまたはBing）
 * - 検索結果から企業HPのURLを取得
 * - 企業HPにアクセスして問い合わせフォームのURLを特定
 * - 取得した情報をFirestoreに更新
 *
 * ✅ 使用方法
 * FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json npx ts-node scripts/updateCompanyUrls.ts
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 *
 * ✅ 注意点
 * - スクレイピング対策のため、各検索の間に3-10秒のランダムな待機時間を設けています
 * - 検索エンジンはDuckDuckGoを使用（IPブロックのリスクが低い）
 * - エラーが発生した場合はログを出力して次の企業に進みます
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
// 設定
// ------------------------------
// 高速化モード: 待機時間を短縮（精度は維持）
const MIN_DELAY_MS = 2000; // 最小待機時間（2秒）
const MAX_DELAY_MS = 5000; // 最大待機時間（5秒）
const PAGE_TIMEOUT_MS = 45000; // ページ読み込みタイムアウト（45秒）
const SEARCH_ENGINE: "duckduckgo" | "bing" = "bing"; // "duckduckgo" または "bing" (Bing推奨: DuckDuckGoがブロックされている場合)
const BATCH_SIZE = 100; // バッチ処理サイズ（50 → 100に増加）
const MAX_RETRIES = 2; // 最大リトライ回数（3 → 2に削減、高速化）
const ERROR_BACKOFF_MS = 20000; // 連続エラー時の待機時間（30秒 → 20秒）
const PAGE_LOAD_WAIT_MS = 2000; // ページ読み込み後の待機時間（最適化）
const SEARCH_RESULT_WAIT_MS = 1500; // 検索結果抽出前の待機時間（最適化）

// ------------------------------
// ユーティリティ関数
// ------------------------------

/**
 * ランダムな待機時間を生成
 */
function getRandomDelay(): number {
  return Math.floor(Math.random() * (MAX_DELAY_MS - MIN_DELAY_MS + 1)) + MIN_DELAY_MS;
}

/**
 * 指定時間待機
 */
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * URLが有効かチェック
 */
function isValidUrl(url: string | null): boolean {
  if (!url) return false;
  try {
    const parsed = new URL(url);
    return parsed.protocol === "http:" || parsed.protocol === "https:";
  } catch {
    return false;
  }
}

/**
 * 検索URLを生成
 */
function getSearchUrl(query: string, engine: string = SEARCH_ENGINE): string {
  const encodedQuery = encodeURIComponent(query);
  if (engine === "bing") {
    return `https://www.bing.com/search?q=${encodedQuery}`;
  } else {
    // DuckDuckGo（通常の検索ページを使用、JavaScript版）
    return `https://duckduckgo.com/?q=${encodedQuery}`;
  }
}

// ------------------------------
// Web検索とURL取得
// ------------------------------

/**
 * DuckDuckGoの検索結果から企業HPのURLを取得（高速化版）
 */
async function extractCompanyUrlFromDuckDuckGo(page: Page, companyName: string): Promise<string | null> {
  try {
    // 検索結果が読み込まれるまで少し待機（最適化: 短縮）
    await sleep(SEARCH_RESULT_WAIT_MS);

    // 複数のセレクタを試行（DuckDuckGoの通常検索ページとHTML検索ページの両方に対応）
    const selectors = [
      "a[data-testid='result-title-a']", // 通常検索ページのセレクタ
      "a.result__a", // HTML検索ページの標準的なセレクタ
      "a.result-link", // 代替セレクタ
      ".result a", // より広範囲なセレクタ
      ".web-result a", // 別のパターン
      "a[class*='result']", // クラス名にresultを含むリンク
      ".result-link__a", // 別のパターン
    ];

    let resultLinks: Array<{ url: string; text: string }> = [];

    for (const selector of selectors) {
      try {
        // セレクタが存在するか確認
        const element = await page.$(selector);
        if (!element) continue;

        const links = await page.$$eval(selector, (links) => {
          return links
            .map((link) => {
              const href = link.getAttribute("href");
              // DuckDuckGoのリダイレクトURLを処理
              let url = href;
              if (href && href.startsWith("/l/?kh=")) {
                // DuckDuckGoのリダイレクトリンクから実際のURLを抽出
                const uParam = new URLSearchParams(href.split("?")[1] || "");
                url = uParam.get("uddg") || href;
              }
              return {
                url: url,
                text: link.textContent?.trim() || "",
              };
            })
            .filter((item) => item.url && !item.url.includes("duckduckgo.com"));
        });

        if (links.length > 0) {
          resultLinks = links as Array<{ url: string; text: string }>;
          break;
        }
      } catch {
        // セレクタが見つからない場合は次のセレクタを試行
        continue;
      }
    }

    // セレクタで見つからない場合は、ページ全体からリンクを抽出
    if (resultLinks.length === 0) {
      try {
        const html = await page.content();
        const $ = cheerio.load(html);
        $("a").each((_, element) => {
          let href = $(element).attr("href");
          const text = $(element).text().trim();
          
          // DuckDuckGoのリダイレクトURLを処理
          if (href && href.startsWith("/l/?kh=")) {
            const uParam = new URLSearchParams(href.split("?")[1] || "");
            href = uParam.get("uddg") || href;
          }
          
          if (href && !href.includes("duckduckgo.com") && isValidUrl(href)) {
            resultLinks.push({ url: href, text });
          }
        });
      } catch {
        // エラーは無視
      }
    }

    // 最初の有効なURLを返す
    for (const link of resultLinks) {
      if (link.url && isValidUrl(link.url)) {
        // 企業名が含まれているかチェック（簡易的な判定）
        const urlLower = link.url.toLowerCase();
        const nameLower = companyName.toLowerCase();
        // ドメイン名やパスに企業名が含まれている可能性を考慮
        if (
          urlLower.includes(nameLower.substring(0, 3)) ||
          link.text.includes(companyName) ||
          urlLower.includes(nameLower.replace(/\s+/g, "").substring(0, 5))
        ) {
          return link.url;
        }
      }
    }

    // 企業名が含まれていなくても、最初の有効なURLを返す
    if (resultLinks.length > 0 && resultLinks[0].url && isValidUrl(resultLinks[0].url)) {
      return resultLinks[0].url;
    }

    return null;
  } catch (error) {
    console.error(`[DuckDuckGo検索結果抽出エラー] ${companyName}:`, error);
    return null;
  }
}

/**
 * Bingの検索結果から企業HPのURLを取得（改善版）
 */
async function extractCompanyUrlFromBing(page: Page, companyName: string): Promise<string | null> {
  try {
    // 検索結果が読み込まれるまで少し待機
    await sleep(SEARCH_RESULT_WAIT_MS);

    // 複数のセレクタを試行（Bingの検索結果ページの構造に対応）
    const selectors = [
      "ol#b_results li h2 a", // 標準的なセレクタ
      "#b_results h2 a", // より広範囲なセレクタ
      ".b_algo h2 a", // 別のパターン
      "li.b_algo h2 a", // より具体的なセレクタ
      "a[href^='http']", // すべての外部リンク（フォールバック）
    ];

    let resultLinks: Array<{ url: string; text: string }> = [];

    for (const selector of selectors) {
      try {
        // セレクタが存在するか確認
        const element = await page.$(selector);
        if (!element) continue;

        const links = await page.$$eval(selector, (links) => {
          return links
            .map((link) => ({
              url: link.getAttribute("href"),
              text: link.textContent?.trim() || "",
            }))
            .filter((item) => item.url && !item.url.includes("bing.com"));
        });

        if (links.length > 0) {
          resultLinks = links as Array<{ url: string; text: string }>;
          break;
        }
      } catch {
        // セレクタが見つからない場合は次のセレクタを試行
        continue;
      }
    }

    // セレクタで見つからない場合は、ページ全体からリンクを抽出
    if (resultLinks.length === 0) {
      try {
        const html = await page.content();
        const $ = cheerio.load(html);
        $("a").each((_, element) => {
          const href = $(element).attr("href");
          const text = $(element).text().trim();
          if (href && !href.includes("bing.com") && isValidUrl(href)) {
            resultLinks.push({ url: href, text });
          }
        });
      } catch {
        // エラーは無視
      }
    }

    // 最初の有効なURLを返す
    for (const link of resultLinks) {
      if (link.url && isValidUrl(link.url)) {
        const urlLower = link.url.toLowerCase();
        const nameLower = companyName.toLowerCase();
        if (
          urlLower.includes(nameLower.substring(0, 3)) ||
          link.text.includes(companyName) ||
          urlLower.includes(nameLower.replace(/\s+/g, "").substring(0, 5))
        ) {
          return link.url;
        }
      }
    }

    // 企業名が含まれていなくても、最初の有効なURLを返す
    if (resultLinks.length > 0 && resultLinks[0].url && isValidUrl(resultLinks[0].url)) {
      return resultLinks[0].url;
    }

    return null;
  } catch (error) {
    console.error(`[Bing検索結果抽出エラー] ${companyName}:`, error);
    return null;
  }
}

/**
 * 検索エンジンで企業HPのURLを検索（リトライ機能付き、高速化版）
 */
async function searchCompanyUrl(
  page: Page,
  companyName: string,
  corporateNumber: string | null
): Promise<string | null> {
  try {
    // 検索キーワードを生成（優先度順、最初に見つかれば即終了）
    const searchQueries = [
      `${companyName} ${corporateNumber || ""}`.trim(),
      `${companyName} 公式サイト`,
      `${companyName} コーポレートサイト`,
    ].filter((q) => q.trim()); // 空のクエリを除外

    for (const query of searchQueries) {
      const searchUrl = getSearchUrl(query, SEARCH_ENGINE);
      console.log(`  🔍 検索中: ${query}`);

      // リトライロジック（最適化）
      for (let retry = 0; retry < MAX_RETRIES; retry++) {
        try {
          if (retry > 0) {
            console.log(`  🔄 リトライ ${retry}/${MAX_RETRIES - 1}...`);
            // リトライ時は待機時間を短縮（高速化）
            await sleep(getRandomDelay() * (retry + 0.5));
          }

          // ページ読み込み（最適化: networkidleは時間がかかるため、domcontentloadedに変更）
          const waitUntil = "domcontentloaded"; // networkidleから変更（高速化）
          await page.goto(searchUrl, {
            waitUntil: waitUntil as any,
            timeout: PAGE_TIMEOUT_MS,
          });

          // ページ読み込み待機（最適化: 短縮）
          await sleep(SEARCH_RESULT_WAIT_MS);

          // 検索結果からURLを抽出
          let companyUrl: string | null = null;
          if (SEARCH_ENGINE === "bing") {
            companyUrl = await extractCompanyUrlFromBing(page, companyName);
          } else {
            companyUrl = await extractCompanyUrlFromDuckDuckGo(page, companyName);
          }

          if (companyUrl && isValidUrl(companyUrl)) {
            console.log(`  ✅ 企業HPを発見: ${companyUrl}`);
            return companyUrl; // 見つかったら即終了（高速化）
          }

          // URLが見つからなかった場合は次の検索クエリに進む（リトライしない）
          break;
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          
          // タイムアウトや接続エラーの場合のみリトライ
          if (
            errorMessage.includes("Timeout") ||
            errorMessage.includes("ERR_CONNECTION") ||
            errorMessage.includes("net::")
          ) {
            if (retry < MAX_RETRIES - 1) {
              console.log(`  ⚠️  接続エラー (リトライ可能): ${errorMessage.substring(0, 80)}`);
              continue;
            } else {
              console.error(`  ❌ 検索エラー (リトライ上限): ${errorMessage.substring(0, 80)}`);
              break; // リトライ上限に達したら次のクエリへ
            }
          } else {
            // その他のエラーはリトライしない（高速化）
            console.error(`  ⚠️  検索エラー: ${errorMessage.substring(0, 80)}`);
            break;
          }
        }
      }

      // 次の検索前に短い待機（高速化: 短縮）
      if (searchQueries.indexOf(query) < searchQueries.length - 1) {
        await sleep(Math.floor(getRandomDelay() * 0.6)); // 待機時間を40%削減
      }
    }

    return null;
  } catch (error) {
    console.error(`[企業URL検索エラー] ${companyName}:`, error);
    return null;
  }
}

/**
 * 企業HPから問い合わせフォームのURLを検索（高速化版）
 */
async function findContactFormUrl(page: Page, companyUrl: string): Promise<string | null> {
  try {
    console.log(`  📄 企業HPにアクセス: ${companyUrl}`);

    await page.goto(companyUrl, {
      waitUntil: "domcontentloaded",
      timeout: PAGE_TIMEOUT_MS,
    });

    // ページ読み込み待機（最適化: 短縮）
    await sleep(PAGE_LOAD_WAIT_MS);

    // ページのHTMLを取得
    const html = await page.content();
    const $ = cheerio.load(html);

    // 問い合わせ関連のキーワード
    const contactKeywords = [
      "お問い合わせ",
      "問い合わせ",
      "お問合せ",
      "問合せ",
      "Contact",
      "contact",
      "Inquiry",
      "inquiry",
      "お問い合わせフォーム",
      "問い合わせフォーム",
      "Contact Form",
      "contact-form",
      "inquiry-form",
    ];

    // リンクを検索
    const contactLinks: Array<{ url: string; text: string }> = [];

    $("a").each((_, element) => {
      const href = $(element).attr("href");
      const text = $(element).text().trim();
      const lowerText = text.toLowerCase();

      if (!href) return;

      // キーワードが含まれているかチェック
      const hasKeyword = contactKeywords.some((keyword) =>
        lowerText.includes(keyword.toLowerCase())
      );

      if (hasKeyword) {
        try {
          // 相対URLを絶対URLに変換
          const absoluteUrl = new URL(href, companyUrl).href;
          contactLinks.push({ url: absoluteUrl, text });
        } catch {
          // URL変換エラーは無視
        }
      }
    });

    // 最初に見つかった問い合わせリンクを返す
    if (contactLinks.length > 0) {
      const contactUrl = contactLinks[0].url;
      console.log(`  ✅ 問い合わせフォームを発見: ${contactUrl}`);
      return contactUrl;
    }

    // リンクテキストにキーワードがなくても、URLパスに含まれている可能性をチェック
    $("a").each((_, element) => {
      const href = $(element).attr("href");
      if (!href) return;

      const lowerHref = href.toLowerCase();
      if (
        lowerHref.includes("contact") ||
        lowerHref.includes("inquiry") ||
        lowerHref.includes("問い合わせ") ||
        lowerHref.includes("お問い合わせ")
      ) {
        try {
          const absoluteUrl = new URL(href, companyUrl).href;
          if (!contactLinks.some((link) => link.url === absoluteUrl)) {
            contactLinks.push({ url: absoluteUrl, text: $(element).text().trim() });
          }
        } catch {
          // URL変換エラーは無視
        }
      }
    });

    if (contactLinks.length > 0) {
      const contactUrl = contactLinks[0].url;
      console.log(`  ✅ 問い合わせフォームを発見（URLパスから）: ${contactUrl}`);
      return contactUrl;
    }

    console.log(`  ⚠️  問い合わせフォームが見つかりませんでした`);
    return null;
  } catch (error) {
    console.error(`  ⚠️  問い合わせフォーム検索エラー:`, error);
    return null;
  }
}

// ------------------------------
// Firestore操作
// ------------------------------

/**
 * 企業データを更新
 */
async function updateCompanyUrls(
  companyId: string,
  companyUrl: string | null,
  contactFormUrl: string | null
): Promise<void> {
  try {
    const updates: { [key: string]: any } = {
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    if (companyUrl) {
      updates.companyUrl = companyUrl;
    }

    if (contactFormUrl) {
      updates.contactFormUrl = contactFormUrl;
    }

    await db.collection("companies_new").doc(companyId).update(updates);
    console.log(`  💾 Firestore更新完了: ${companyId}`);
  } catch (error) {
    console.error(`  ❌ Firestore更新エラー (${companyId}):`, error);
    throw error;
  }
}

// ------------------------------
// メイン処理
// ------------------------------

/**
 * メイン処理: companyUrlがnullの企業を検索して更新
 */
async function main() {
  let browser: Browser | null = null;
  let page: Page | null = null;

  try {
    // コマンドライン引数を解析
    const args = process.argv.slice(2);
    let startOffset = 0;

    // --offset オプションをチェック
    const offsetIndex = args.indexOf("--offset");
    if (offsetIndex !== -1 && offsetIndex + 1 < args.length) {
      startOffset = parseInt(args[offsetIndex + 1], 10);
      if (isNaN(startOffset) || startOffset < 0) {
        console.error("❌ エラー: --offset には0以上の数値を指定してください");
        process.exit(1);
      }
    }

    // --help オプションをチェック
    if (args.includes("--help") || args.includes("-h")) {
      console.log("使用方法:");
      console.log("  npx ts-node scripts/updateCompanyUrls.ts [オプション]");
      console.log("");
      console.log("オプション:");
      console.log("  --offset <数値>  処理を開始する位置を指定（0から開始）");
      console.log("                   例: --offset 25 で26件目から処理を開始");
      console.log("  --help, -h        このヘルプを表示");
      console.log("");
      console.log("環境変数:");
      console.log("  FIREBASE_SERVICE_ACCOUNT_KEY  必須: Firebaseサービスアカウントキーのパス");
      console.log("");
      console.log("実行例:");
      console.log("  # 最初から実行");
      console.log("  FIREBASE_SERVICE_ACCOUNT_KEY=/path/to/key.json npx ts-node scripts/updateCompanyUrls.ts");
      console.log("");
      console.log("  # 26件目から再開");
      console.log("  FIREBASE_SERVICE_ACCOUNT_KEY=/path/to/key.json npx ts-node scripts/updateCompanyUrls.ts --offset 25");
      process.exit(0);
    }

    console.log("🚀 企業URL補完スクリプトを開始します...\n");
    if (startOffset > 0) {
      console.log(`📌 開始位置: ${startOffset}件目から処理を再開します\n`);
    }

    // ブラウザを起動
    console.log("🌐 ブラウザを起動中...");
    browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });

    const context = await browser.newContext({
      userAgent:
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    });

    page = await context.newPage();

    // companyUrlがnullのドキュメントを取得
    console.log("📊 Firestoreからデータを取得中...");
    let query = db
      .collection("companies_new")
      .where("companyUrl", "==", null)
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(BATCH_SIZE);

    // オフセットがある場合は、その位置までスキップ
    if (startOffset > 0) {
      // オフセット分のドキュメントを取得して、最後のドキュメントIDを取得
      const offsetSnapshot = await db
        .collection("companies_new")
        .where("companyUrl", "==", null)
        .orderBy(admin.firestore.FieldPath.documentId())
        .limit(startOffset)
        .get();

      if (offsetSnapshot.empty || offsetSnapshot.docs.length < startOffset) {
        console.log(`⚠️  警告: 指定されたオフセット（${startOffset}）が取得可能な件数を超えています`);
        console.log(`📋 取得可能な件数: ${offsetSnapshot.docs.length}件`);
        return;
      }

      const lastDoc = offsetSnapshot.docs[offsetSnapshot.docs.length - 1];
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();

    if (snapshot.empty) {
      console.log("✅ companyUrlがnullのドキュメントはありませんでした。");
      return;
    }

    console.log(`📋 ${snapshot.size} 件の企業を処理します。\n`);

    let successCount = 0;
    let errorCount = 0;
    let skippedCount = 0;
    let consecutiveErrors = 0; // 連続エラーカウント

    for (let i = 0; i < snapshot.docs.length; i++) {
      const doc = snapshot.docs[i];
      const companyId = doc.id;
      const companyData = doc.data();
      const companyName = companyData.name || "";
      const corporateNumber = companyData.corporateNumber || null;

      if (!companyName) {
        console.log(`\n[${i + 1}/${snapshot.size}] ⚠️  企業名が空のためスキップ: ${companyId}`);
        skippedCount++;
        continue;
      }

      console.log(`\n[${i + 1}/${snapshot.size}] 📌 処理中: ${companyName} (${companyId})`);

      // 連続エラーが発生している場合は長めに待機（高速化: 短縮）
      if (consecutiveErrors >= 3) {
        console.log(`  ⚠️  連続エラーが${consecutiveErrors}回発生しています。${ERROR_BACKOFF_MS / 1000}秒待機します...`);
        await sleep(ERROR_BACKOFF_MS);
        consecutiveErrors = 0; // リセット
      }

      try {
        // 企業HPのURLを検索
        const companyUrl = await searchCompanyUrl(page, companyName, corporateNumber);

        if (!companyUrl) {
          console.log(`  ⚠️  企業HPが見つかりませんでした`);
          errorCount++;
          consecutiveErrors++;
          // 次の企業に進む前に待機（高速化: 短縮）
          await sleep(Math.floor(getRandomDelay() * 0.7)); // 待機時間を30%削減
          continue;
        }

        // 問い合わせフォームのURLを検索
        let contactFormUrl: string | null = null;
        try {
          contactFormUrl = await findContactFormUrl(page, companyUrl);
        } catch (error) {
          console.error(`  ⚠️  問い合わせフォーム検索でエラー:`, error);
          // 問い合わせフォームが見つからなくても、企業HPは更新する
        }

        // Firestoreを更新
        await updateCompanyUrls(companyId, companyUrl, contactFormUrl);

        successCount++;
        consecutiveErrors = 0; // 成功したらリセット

        // 次の企業に進む前に待機（スクレイピング対策、高速化: 短縮）
        if (i < snapshot.docs.length - 1) {
          const delay = getRandomDelay();
          console.log(`  ⏳ ${(delay / 1000).toFixed(1)}秒待機中...`);
          await sleep(delay);
        }
      } catch (error) {
        console.error(`  ❌ 処理エラー:`, error);
        errorCount++;
        consecutiveErrors++;
        // エラーが発生しても次の企業に進む（高速化: 短縮）
        await sleep(Math.floor(getRandomDelay() * 0.7)); // 待機時間を30%削減
      }
    }

    console.log("\n" + "=".repeat(60));
    console.log("📊 処理結果");
    console.log("=".repeat(60));
    console.log(`✅ 成功: ${successCount} 件`);
    console.log(`❌ エラー: ${errorCount} 件`);
    console.log(`⚠️  スキップ: ${skippedCount} 件`);
    console.log(`📋 合計: ${snapshot.size} 件`);
    console.log("=".repeat(60));
  } catch (error) {
    console.error("❌ メイン処理エラー:", error);
    process.exit(1);
  } finally {
    if (page) {
      await page.close();
    }
    if (browser) {
      await browser.close();
      console.log("\n🌐 ブラウザを終了しました");
    }
  }
}

// スクリプト実行
main()
  .then(() => {
    console.log("\n✅ スクリプトが正常に完了しました");
    process.exit(0);
  })
  .catch((error) => {
    console.error("\n❌ スクリプト実行エラー:", error);
    process.exit(1);
  });
