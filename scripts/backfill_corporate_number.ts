/* 
  法人番号がnullのドキュメントに対して、内部データと外部ソースから法人番号を補完するバッチ
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/backfill_corporate_number.ts [オプション]
    
  オプション:
    --dry-run: 実際には更新せず、更新予定の内容を表示
    --limit=N: 処理するドキュメント数を制限（テスト用）
    --start-after=DOC_ID: 指定したドキュメントIDから処理を開始
    --batch-size=N: バッチサイズ（デフォルト: 500）
    --analyze-only: 内訳集計のみ実行（更新は行わない）
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference, DocumentReference, WriteBatch } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";
import * as csv from "csv-writer";

const COLLECTION_NAME = "companies_new";

// パフォーマンス設定（環境変数で上書き可能）
const BATCH_SIZE = process.env.BATCH_SIZE ? parseInt(process.env.BATCH_SIZE) : 500;
const CONCURRENT_QUERIES = process.env.CONCURRENT_QUERIES ? parseInt(process.env.CONCURRENT_QUERIES) : 40;

// ==============================
// Firebase初期化
// ==============================

let db: Firestore;
let companiesCol: CollectionReference;

function initAdmin() {
  if (admin.apps.length > 0) {
    db = admin.firestore();
    companiesCol = db.collection(COLLECTION_NAME);
    return;
  }

  const serviceAccountPath =
    process.env.GOOGLE_APPLICATION_CREDENTIALS ||
    path.join(__dirname, "../serviceAccountKey.json");

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(`❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });

  db = admin.firestore();
  companiesCol = db.collection(COLLECTION_NAME);
}

// ==============================
// ログ関数
// ==============================

function log(message: string) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
}

// ==============================
// オプション解析
// ==============================

interface Options {
  dryRun: boolean;
  limit: number | null;
  startAfter: string | null;
  batchSize: number;
  analyzeOnly: boolean;
}

function parseOptions(): Options {
  const args = process.argv.slice(2);
  const options: Options = {
    dryRun: args.includes("--dry-run"),
    limit: null,
    startAfter: null,
    batchSize: BATCH_SIZE,
    analyzeOnly: args.includes("--analyze-only"),
  };

  for (const arg of args) {
    if (arg.startsWith("--limit=")) {
      options.limit = parseInt(arg.split("=")[1]);
    } else if (arg.startsWith("--start-after=")) {
      options.startAfter = arg.split("=")[1];
    } else if (arg.startsWith("--batch-size=")) {
      options.batchSize = parseInt(arg.split("=")[1]);
    }
  }

  return options;
}

// ==============================
// 正規化関数
// ==============================

/**
 * 会社名を正規化
 * - 法人格表記の統一（株式会社/(株)/（株）等）
 * - 空白除去
 * - 全角半角統一（英数字・記号を半角に）
 * - 記号除去（一部）
 * - カナ統一（全角カナに）
 */
function normalizeCompanyName(name: string | null | undefined): string | null {
  if (!name || name.trim() === "") return null;

  let normalized = name.trim();

  // 法人格表記の統一
  const corporateTypes = [
    { pattern: /\(株\)|（株）|㈱/g, replacement: "株式会社" },
    { pattern: /\(有\)|（有）|㈲/g, replacement: "有限会社" },
    { pattern: /\(合\)|（合）|㈱合/g, replacement: "合同会社" },
    { pattern: /\(医\)|（医）/g, replacement: "医療法人" },
    { pattern: /\(学\)|（学）/g, replacement: "学校法人" },
    { pattern: /\(福\)|（福）/g, replacement: "社会福祉法人" },
    { pattern: /\(宗\)|（宗）/g, replacement: "宗教法人" },
    { pattern: /\(社\)|（社）/g, replacement: "一般社団法人" },
    { pattern: /\(財\)|（財）/g, replacement: "一般財団法人" },
    { pattern: /\(特\)|（特）/g, replacement: "特定非営利活動法人" },
  ];

  for (const { pattern, replacement } of corporateTypes) {
    normalized = normalized.replace(pattern, replacement);
  }

  // 空白除去
  normalized = normalized.replace(/\s+/g, "");

  // 全角英数字・記号を半角に
  normalized = normalized.replace(/[Ａ-Ｚａ-ｚ０-９]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) - 0xFEE0);
  });

  // 全角カナを統一（全角カナに）
  normalized = normalized.replace(/[ァ-ヶ]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) + 0x60);
  });

  return normalized;
}

/**
 * 住所を正規化（都道府県市区町村まで）
 * - 都道府県表記の統一
 * - 丁目/番地の揺れを除去
 * - 建物名を除去
 * - 空白除去
 */
function normalizeAddress(address: string | null | undefined): string | null {
  if (!address || address.trim() === "") return null;

  let normalized = address.trim();

  // 都道府県表記の統一
  const prefectures = [
    { pattern: /^北海道|^ほっかいどう/gi, replacement: "北海道" },
    { pattern: /^青森県|^あおもりけん/gi, replacement: "青森県" },
    { pattern: /^岩手県|^いわてけん/gi, replacement: "岩手県" },
    { pattern: /^宮城県|^みやぎけん/gi, replacement: "宮城県" },
    { pattern: /^秋田県|^あきたけん/gi, replacement: "秋田県" },
    { pattern: /^山形県|^やまがたけん/gi, replacement: "山形県" },
    { pattern: /^福島県|^ふくしまけん/gi, replacement: "福島県" },
    { pattern: /^茨城県|^いばらきけん/gi, replacement: "茨城県" },
    { pattern: /^栃木県|^とちぎけん/gi, replacement: "栃木県" },
    { pattern: /^群馬県|^ぐんまけん/gi, replacement: "群馬県" },
    { pattern: /^埼玉県|^さいたまけん/gi, replacement: "埼玉県" },
    { pattern: /^千葉県|^ちばけん/gi, replacement: "千葉県" },
    { pattern: /^東京都|^とうきょうと/gi, replacement: "東京都" },
    { pattern: /^神奈川県|^かながわけん/gi, replacement: "神奈川県" },
    { pattern: /^新潟県|^にいがたけん/gi, replacement: "新潟県" },
    { pattern: /^富山県|^とやまけん/gi, replacement: "富山県" },
    { pattern: /^石川県|^いしかわけん/gi, replacement: "石川県" },
    { pattern: /^福井県|^ふくいけん/gi, replacement: "福井県" },
    { pattern: /^山梨県|^やまなしけん/gi, replacement: "山梨県" },
    { pattern: /^長野県|^ながのけん/gi, replacement: "長野県" },
    { pattern: /^岐阜県|^ぎふけん/gi, replacement: "岐阜県" },
    { pattern: /^静岡県|^しずおかけん/gi, replacement: "静岡県" },
    { pattern: /^愛知県|^あいちけん/gi, replacement: "愛知県" },
    { pattern: /^三重県|^みえけん/gi, replacement: "三重県" },
    { pattern: /^滋賀県|^しがけん/gi, replacement: "滋賀県" },
    { pattern: /^京都府|^きょうとふ/gi, replacement: "京都府" },
    { pattern: /^大阪府|^おおさかふ/gi, replacement: "大阪府" },
    { pattern: /^兵庫県|^ひょうごけん/gi, replacement: "兵庫県" },
    { pattern: /^奈良県|^ならけん/gi, replacement: "奈良県" },
    { pattern: /^和歌山県|^わかやまけん/gi, replacement: "和歌山県" },
    { pattern: /^鳥取県|^とっとりけん/gi, replacement: "鳥取県" },
    { pattern: /^島根県|^しまねけん/gi, replacement: "島根県" },
    { pattern: /^岡山県|^おかやまけん/gi, replacement: "岡山県" },
    { pattern: /^広島県|^ひろしまけん/gi, replacement: "広島県" },
    { pattern: /^山口県|^やまぐちけん/gi, replacement: "山口県" },
    { pattern: /^徳島県|^とくしまけん/gi, replacement: "徳島県" },
    { pattern: /^香川県|^かがわけん/gi, replacement: "香川県" },
    { pattern: /^愛媛県|^えひめけん/gi, replacement: "愛媛県" },
    { pattern: /^高知県|^こうちけん/gi, replacement: "高知県" },
    { pattern: /^福岡県|^ふくおかけん/gi, replacement: "福岡県" },
    { pattern: /^佐賀県|^さがけん/gi, replacement: "佐賀県" },
    { pattern: /^長崎県|^ながさきけん/gi, replacement: "長崎県" },
    { pattern: /^熊本県|^くまもとけん/gi, replacement: "熊本県" },
    { pattern: /^大分県|^おおいたけん/gi, replacement: "大分県" },
    { pattern: /^宮崎県|^みやざきけん/gi, replacement: "宮崎県" },
    { pattern: /^鹿児島県|^かごしまけん/gi, replacement: "鹿児島県" },
    { pattern: /^沖縄県|^おきなわけん/gi, replacement: "沖縄県" },
  ];

  for (const { pattern, replacement } of prefectures) {
    if (pattern.test(normalized)) {
      normalized = normalized.replace(pattern, replacement);
      break;
    }
  }

  // 市区町村まで抽出（丁目/番地/建物名を除去）
  // 都道府県 + 市区町村のパターンにマッチ
  const cityMatch = normalized.match(/^(.+?[都道府県])(.+?[市区町村])/);
  if (cityMatch) {
    normalized = cityMatch[1] + cityMatch[2];
  }

  // 空白除去
  normalized = normalized.replace(/\s+/g, "");

  return normalized;
}

/**
 * 電話番号を正規化
 * - ハイフン除去
 * - 全角→半角
 * - 先頭0保持
 */
function normalizePhoneNumber(phone: string | null | undefined): string | null {
  if (!phone || phone.trim() === "") return null;

  let normalized = phone.trim();

  // 全角数字を半角に
  normalized = normalized.replace(/[０-９]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) - 0xFEE0);
  });

  // ハイフン・括弧・空白を除去
  normalized = normalized.replace(/[-()（）\s]/g, "");

  // 先頭の0を保持（日本の電話番号は0から始まる）
  if (!normalized.startsWith("0") && normalized.length >= 10) {
    normalized = "0" + normalized;
  }

  return normalized;
}

/**
 * URLを正規化
 * - プロトコル除去
 * - www.除去
 * - 末尾スラッシュ除去
 * - 小文字化
 */
function normalizeUrl(url: string | null | undefined): string | null {
  if (!url || url.trim() === "") return null;

  let normalized = url.trim().toLowerCase();

  // プロトコル除去
  normalized = normalized.replace(/^https?:\/\//, "");

  // www.除去
  normalized = normalized.replace(/^www\./, "");

  // 末尾スラッシュ除去
  normalized = normalized.replace(/\/$/, "");

  return normalized;
}

// ==============================
// 内訳集計
// ==============================

interface AnalysisResult {
  total: number;
  hasCorporateType: number;
  hasAddress: number;
  hasPhone: number;
  hasUrl: number;
  hasCommonName: number; // 同名企業が多そうな名称
  corporateTypes: Map<string, number>;
  prefectures: Map<string, number>;
}

/**
 * 法人格っぽい名称が含まれるかチェック
 */
function hasCorporateType(name: string | null | undefined): boolean {
  if (!name) return false;
  const corporatePatterns = [
    /株式会社|有限会社|合同会社|医療法人|学校法人|社会福祉法人|宗教法人|一般社団|一般財団|NPO|特定非営利活動法人|協同組合|相互会社|合資会社|合名会社|有限責任事業組合|投資法人|資産運用会社/i,
  ];
  return corporatePatterns.some(pattern => pattern.test(name));
}

/**
 * 同名企業が多そうな名称かチェック（例：〇〇商事、〇〇商店）
 */
function hasCommonName(name: string | null | undefined): boolean {
  if (!name) return false;
  const commonPatterns = [
    /商事|商店|物産|貿易|興業|産業|工業|製作所|製作|製造|建設|工務店|不動産|運輸|運送|倉庫|卸|問屋/i,
  ];
  return commonPatterns.some(pattern => pattern.test(name));
}

async function analyzeNullCorporateNumbers(options: Options): Promise<AnalysisResult> {
  log("📊 法人番号がnullのドキュメントの内訳集計を開始...");

  const result: AnalysisResult = {
    total: 0,
    hasCorporateType: 0,
    hasAddress: 0,
    hasPhone: 0,
    hasUrl: 0,
    hasCommonName: 0,
    corporateTypes: new Map(),
    prefectures: new Map(),
  };

  const PAGE_SIZE = 1000;
  let lastDoc: any = null;
  let totalProcessed = 0;

  while (true) {
    if (options.limit && result.total >= options.limit) break;

    let batchQuery = companiesCol.orderBy("__name__").limit(PAGE_SIZE);
    if (lastDoc) {
      batchQuery = batchQuery.startAfter(lastDoc);
    } else if (options.startAfter) {
      const startDoc = await companiesCol.doc(options.startAfter).get();
      if (startDoc.exists) {
        batchQuery = batchQuery.startAfter(startDoc);
      }
    }

    const batchSnapshot = await batchQuery.get();
    if (batchSnapshot.empty) break;

    for (const doc of batchSnapshot.docs) {
      const data = doc.data();
      const corporateNumber = data.corporateNumber;

      // 法人番号がnull/undefined/空のもののみ
      const isNull = corporateNumber === null || 
          corporateNumber === undefined || 
          corporateNumber === "" ||
          !("corporateNumber" in data);

      if (!isNull) continue;

      result.total++;

      // 法人格チェック
      const name = data.name;
      if (hasCorporateType(name)) {
        result.hasCorporateType++;
        
        // 法人格の種類を集計
        const normalizedName = normalizeCompanyName(name) || "";
        if (normalizedName.includes("株式会社")) {
          result.corporateTypes.set("株式会社", (result.corporateTypes.get("株式会社") || 0) + 1);
        } else if (normalizedName.includes("有限会社")) {
          result.corporateTypes.set("有限会社", (result.corporateTypes.get("有限会社") || 0) + 1);
        } else if (normalizedName.includes("合同会社")) {
          result.corporateTypes.set("合同会社", (result.corporateTypes.get("合同会社") || 0) + 1);
        } else if (normalizedName.includes("医療法人")) {
          result.corporateTypes.set("医療法人", (result.corporateTypes.get("医療法人") || 0) + 1);
        } else if (normalizedName.includes("学校法人")) {
          result.corporateTypes.set("学校法人", (result.corporateTypes.get("学校法人") || 0) + 1);
        } else if (normalizedName.includes("社会福祉法人")) {
          result.corporateTypes.set("社会福祉法人", (result.corporateTypes.get("社会福祉法人") || 0) + 1);
        } else if (normalizedName.includes("宗教法人")) {
          result.corporateTypes.set("宗教法人", (result.corporateTypes.get("宗教法人") || 0) + 1);
        } else if (normalizedName.includes("一般社団")) {
          result.corporateTypes.set("一般社団法人", (result.corporateTypes.get("一般社団法人") || 0) + 1);
        } else if (normalizedName.includes("一般財団")) {
          result.corporateTypes.set("一般財団法人", (result.corporateTypes.get("一般財団法人") || 0) + 1);
        } else if (normalizedName.includes("NPO") || normalizedName.includes("特定非営利活動法人")) {
          result.corporateTypes.set("NPO法人", (result.corporateTypes.get("NPO法人") || 0) + 1);
        }
      }

      // 同名企業が多そうな名称
      if (hasCommonName(name)) {
        result.hasCommonName++;
      }

      // 住所チェック
      const address = data.address || data.headquartersAddress;
      if (address && address.trim() !== "") {
        result.hasAddress++;
        
        // 都道府県を抽出
        const prefectureMatch = address.match(/^(.+?[都道府県])/);
        if (prefectureMatch) {
          const prefecture = prefectureMatch[1];
          result.prefectures.set(prefecture, (result.prefectures.get(prefecture) || 0) + 1);
        }
      }

      // 電話番号チェック
      const phone = data.phoneNumber || data.contactPhoneNumber;
      if (phone && phone.trim() !== "") {
        result.hasPhone++;
      }

      // URLチェック
      const urls = [
        data.companyUrl,
        data.profileUrl,
        data.externalDetailUrl,
      ].filter(url => url && url.trim() !== "");
      if (urls.length > 0) {
        result.hasUrl++;
      }

      totalProcessed++;
    }

    if (result.total % 10000 === 0) {
      log(`  📊 処理中: ${totalProcessed.toLocaleString()} 件スキャン、法人番号null: ${result.total.toLocaleString()} 件`);
    }

    if (options.limit && result.total >= options.limit) break;

    lastDoc = batchSnapshot.docs[batchSnapshot.docs.length - 1];
    if (batchSnapshot.size < PAGE_SIZE) break;
  }

  return result;
}

// ==============================
// 内部照合（試算版）
// ==============================

interface MatchCandidate {
  docId: string;
  corporateNumber: string;
  name: string;
  address: string | null;
  phone: string | null;
  url: string | null;
  score: number;
  matchType: "url" | "phone" | "name_address" | "name_postal";
}

/**
 * 内部データからマッチング候補を検索（試算版）
 */
async function findInternalMatches(
  nullDoc: { ref: DocumentReference; data: Record<string, any> },
  options: Options
): Promise<MatchCandidate[]> {
  const candidates: MatchCandidate[] = [];
  const data = nullDoc.data;

  // URL一致を最優先で検索
  const urls = [
    data.companyUrl,
    data.profileUrl,
    data.externalDetailUrl,
  ].filter(url => url && url.trim() !== "");

  for (const url of urls) {
    const normalizedUrl = normalizeUrl(url);
    if (!normalizedUrl) continue;

    // URLで検索（companyUrl, profileUrl, externalDetailUrlのいずれかと一致）
    const urlQueries = [
      companiesCol.where("companyUrl", "!=", null).limit(100),
      companiesCol.where("profileUrl", "!=", null).limit(100),
      companiesCol.where("externalDetailUrl", "!=", null).limit(100),
    ];

    // 注意: Firestoreのwhere("!=", null)は効率的ではないため、実際の実装では別のアプローチが必要
    // ここでは試算のため、簡易的に実装
    // 実際には、URLのインデックスを作成するか、別の方法で検索する必要がある
  }

  // 電話番号一致
  const phone = data.phoneNumber || data.contactPhoneNumber;
  if (phone) {
    const normalizedPhone = normalizePhoneNumber(phone);
    if (normalizedPhone) {
      // 電話番号で検索
      const phoneQuery = companiesCol
        .where("phoneNumber", "==", normalizedPhone)
        .limit(10)
        .get();
      
      // 実際の実装では、正規化された電話番号で検索する必要がある
      // ここでは試算のため、スキップ
    }
  }

  // name正規化 + 住所正規化
  const name = normalizeCompanyName(data.name);
  const address = normalizeAddress(data.address || data.headquartersAddress);

  if (name && address) {
    // 社名で検索して、住所でフィルタリング
    const nameQuery = companiesCol
      .where("name", ">=", name)
      .where("name", "<=", name + "\uf8ff")
      .limit(100)
      .get();
    
    // 実際の実装では、正規化された社名と住所で照合する必要がある
    // ここでは試算のため、スキップ
  }

  // name正規化 + 郵便番号
  const postalCode = data.postalCode;
  if (name && postalCode) {
    // 郵便番号で検索して、社名でフィルタリング
    const postalQuery = companiesCol
      .where("postalCode", "==", postalCode)
      .limit(100)
      .get();
    
    // 実際の実装では、正規化された社名と郵便番号で照合する必要がある
    // ここでは試算のため、スキップ
  }

  return candidates;
}

/**
 * 内部照合の試算を実行
 */
async function estimateInternalMatches(options: Options): Promise<{
  total: number;
  estimatedMatches: number;
  matchBreakdown: {
    url: number;
    phone: number;
    nameAddress: number;
    namePostal: number;
  };
}> {
  log("🔍 内部照合の試算を開始...");

  const result = {
    total: 0,
    estimatedMatches: 0,
    matchBreakdown: {
      url: 0,
      phone: 0,
      nameAddress: 0,
      namePostal: 0,
    },
  };

  // 実際の実装では、nullドキュメントを取得して内部データと照合する
  // ここでは試算のため、簡易的に実装
  // 実際の実装では、内部データ（402万件）を効率的に検索する必要がある

  return result;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  initAdmin();
  const options = parseOptions();

  log("============================================================");
  log("法人番号補完バッチ処理");
  log("============================================================");
  log(`オプション: ${JSON.stringify(options, null, 2)}`);
  log("");

  // 1. 内訳集計
  const analysis = await analyzeNullCorporateNumbers(options);

  log("\n============================================================");
  log("📊 内訳集計結果");
  log("============================================================");
  log(`総数: ${analysis.total.toLocaleString()} 社`);
  log(`法人格っぽい名称: ${analysis.hasCorporateType.toLocaleString()} 社 (${((analysis.hasCorporateType / analysis.total) * 100).toFixed(2)}%)`);
  log(`住所あり: ${analysis.hasAddress.toLocaleString()} 社 (${((analysis.hasAddress / analysis.total) * 100).toFixed(2)}%)`);
  log(`電話番号あり: ${analysis.hasPhone.toLocaleString()} 社 (${((analysis.hasPhone / analysis.total) * 100).toFixed(2)}%)`);
  log(`URLあり: ${analysis.hasUrl.toLocaleString()} 社 (${((analysis.hasUrl / analysis.total) * 100).toFixed(2)}%)`);
  log(`同名企業が多そうな名称: ${analysis.hasCommonName.toLocaleString()} 社 (${((analysis.hasCommonName / analysis.total) * 100).toFixed(2)}%)`);

  if (analysis.corporateTypes.size > 0) {
    log("\n法人格の内訳:");
    const sortedTypes = Array.from(analysis.corporateTypes.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    for (const [type, count] of sortedTypes) {
      log(`  - ${type}: ${count.toLocaleString()} 社`);
    }
  }

  if (analysis.prefectures.size > 0) {
    log("\n都道府県の内訳（上位10件）:");
    const sortedPrefectures = Array.from(analysis.prefectures.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10);
    for (const [prefecture, count] of sortedPrefectures) {
      log(`  - ${prefecture}: ${count.toLocaleString()} 社`);
    }
  }

  if (options.analyzeOnly) {
    log("\n✅ 内訳集計のみ完了（--analyze-only）");
    return;
  }

  // 2. 内部照合の試算
  const estimate = await estimateInternalMatches(options);

  log("\n============================================================");
  log("🔍 内部照合試算結果");
  log("============================================================");
  log(`総数: ${estimate.total.toLocaleString()} 社`);
  log(`推定マッチ数: ${estimate.estimatedMatches.toLocaleString()} 社`);
  log(`  - URL一致: ${estimate.matchBreakdown.url.toLocaleString()} 社`);
  log(`  - 電話番号一致: ${estimate.matchBreakdown.phone.toLocaleString()} 社`);
  log(`  - 社名+住所一致: ${estimate.matchBreakdown.nameAddress.toLocaleString()} 社`);
  log(`  - 社名+郵便番号一致: ${estimate.matchBreakdown.namePostal.toLocaleString()} 社`);

  log("\n✅ 処理完了");
}

// 実行
main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
