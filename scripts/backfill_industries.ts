/* eslint-disable no-console */

/**
 * scripts/backfill_industries.ts
 *
 * ✅ 目的
 * - industries.csv（正規マスタ）を読み込み
 * - companies_new コレクションの業種4階層フィールドを更新
 * - industryLarge, industryMiddle, industrySmall, industryDetail を必ず埋める
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 * - DRY_RUN=1 (任意: 1の場合はFirestoreを更新せずレポートのみ出力)
 * - LIMIT=1000 (任意: 処理件数上限)
 * - START_AFTER_ID=xxx (任意: 途中から再開)
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

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
// 型定義
// ------------------------------

interface IndustryTree {
  large: string;
  middle: string;
  small: string;
  normalizedLarge?: string;
  normalizedMiddle?: string;
  normalizedSmall?: string;
}

interface IndustryMatch {
  large: string;
  middle: string;
  small: string;
  detail?: string;
  method: "exact" | "normalized" | "fuzzy" | "manual-needed";
  confidence: "high" | "medium" | "low";
  candidates?: IndustryTree[];
}

interface CompanyData {
  docId: string;
  corporateNumber?: string;
  name?: string;
  industryLarge?: string | null;
  industryMiddle?: string | null;
  industrySmall?: string | null;
  industryDetail?: string | null;
  industry?: string | null;
  industries?: string[] | string | null;
  industryName?: string | null;
  [key: string]: any;
}

interface BackfillResult {
  docId: string;
  corporateNumber: string;
  name: string;
  before: {
    large: string;
    middle: string;
    small: string;
    detail: string;
  };
  after: {
    large: string;
    middle: string;
    small: string;
    detail: string;
  };
  method: string;
  confidence: string;
  unresolved?: string;
  candidates?: string;
}

// ------------------------------
// industries.csv の読み込みとツリー構築
// ------------------------------

function loadIndustryMaster(csvPath: string): {
  tree: IndustryTree[];
  treeByLarge: Map<string, IndustryTree[]>;
  treeByMiddle: Map<string, IndustryTree[]>;
  treeBySmall: Map<string, IndustryTree[]>;
  normalizedTreeByLarge: Map<string, IndustryTree[]>;
  normalizedTreeByMiddle: Map<string, IndustryTree[]>;
  normalizedTreeBySmall: Map<string, IndustryTree[]>;
  normalizedTreeKeyMap: Map<string, IndustryTree>;
} {
  const csvContent = fs.readFileSync(csvPath, "utf-8");
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
  }) as Array<{
    industryLarge: string;
    industryMiddle: string;
    industrySmall: string;
  }>;

  const tree: IndustryTree[] = [];
  const treeByLarge = new Map<string, IndustryTree[]>();
  const treeByMiddle = new Map<string, IndustryTree[]>();
  const treeBySmall = new Map<string, IndustryTree[]>();
  const normalizedTreeByLarge = new Map<string, IndustryTree[]>();
  const normalizedTreeByMiddle = new Map<string, IndustryTree[]>();
  const normalizedTreeBySmall = new Map<string, IndustryTree[]>();
  const normalizedTreeKeyMap = new Map<string, IndustryTree>();

  for (const record of records) {
    const large = (record.industryLarge || "").trim();
    const middle = (record.industryMiddle || "").trim();
    const small = (record.industrySmall || "").trim();

    if (!large || !middle || !small) {
      continue; // 不完全な行はスキップ
    }

    // 正規化を事前計算
    const normalizedLarge = normalizeText(large);
    const normalizedMiddle = normalizeText(middle);
    const normalizedSmall = normalizeText(small);
    const normalizedKey = `${normalizedLarge}|${normalizedMiddle}|${normalizedSmall}`;

    const item: IndustryTree = {
      large,
      middle,
      small,
      normalizedLarge,
      normalizedMiddle,
      normalizedSmall,
    };
    tree.push(item);

    // 元の値でのインデックス構築
    if (!treeByLarge.has(large)) {
      treeByLarge.set(large, []);
    }
    treeByLarge.get(large)!.push(item);

    if (!treeByMiddle.has(middle)) {
      treeByMiddle.set(middle, []);
    }
    treeByMiddle.get(middle)!.push(item);

    if (!treeBySmall.has(small)) {
      treeBySmall.set(small, []);
    }
    treeBySmall.get(small)!.push(item);

    // 正規化値でのインデックス構築
    if (!normalizedTreeByLarge.has(normalizedLarge)) {
      normalizedTreeByLarge.set(normalizedLarge, []);
    }
    normalizedTreeByLarge.get(normalizedLarge)!.push(item);

    if (!normalizedTreeByMiddle.has(normalizedMiddle)) {
      normalizedTreeByMiddle.set(normalizedMiddle, []);
    }
    normalizedTreeByMiddle.get(normalizedMiddle)!.push(item);

    if (!normalizedTreeBySmall.has(normalizedSmall)) {
      normalizedTreeBySmall.set(normalizedSmall, []);
    }
    normalizedTreeBySmall.get(normalizedSmall)!.push(item);

    // 正規化キーマップ（完全一致検索用）
    normalizedTreeKeyMap.set(normalizedKey, item);
  }

  console.log(`[マスタ読み込み] ✅ ${tree.length} 件の業種分類を読み込みました（正規化済みインデックスも構築）`);
  console.log(`  大分類数: ${treeByLarge.size}`);
  console.log(`  中分類数: ${treeByMiddle.size}`);
  console.log(`  小分類数: ${treeBySmall.size}`);

  return {
    tree,
    treeByLarge,
    treeByMiddle,
    treeBySmall,
    normalizedTreeByLarge,
    normalizedTreeByMiddle,
    normalizedTreeBySmall,
    normalizedTreeKeyMap,
  };
}

// ------------------------------
// 文字列正規化
// ------------------------------

function normalizeText(text: string | null | undefined): string {
  if (!text || typeof text !== "string") {
    return "";
  }

  return text
    .trim()
    .replace(/[（(].*?[）)]/g, "") // 括弧内を削除
    .replace(/[：:].*$/, "") // コロン以降を削除
    .replace(/\s+/g, "") // 空白を削除
    .replace(/[０-９]/g, (s) => String.fromCharCode(s.charCodeAt(0) - 0xfee0)) // 全角数字→半角
    .replace(/[Ａ-Ｚａ-ｚ]/g, (s) => String.fromCharCode(s.charCodeAt(0) - 0xfee0)) // 全角英字→半角
    .normalize("NFKC"); // NFKC正規化
}

// ------------------------------
// 業種マッチング
// ------------------------------

function findIndustryMatch(
  companyData: CompanyData,
  industryMaster: {
    tree: IndustryTree[];
    treeByLarge: Map<string, IndustryTree[]>;
    treeByMiddle: Map<string, IndustryTree[]>;
    treeBySmall: Map<string, IndustryTree[]>;
    normalizedTreeByLarge: Map<string, IndustryTree[]>;
    normalizedTreeByMiddle: Map<string, IndustryTree[]>;
    normalizedTreeBySmall: Map<string, IndustryTree[]>;
    normalizedTreeKeyMap: Map<string, IndustryTree>;
  }
): IndustryMatch | null {
  // 既存の業種情報を収集（一度だけ正規化）
  const existingFields = {
    large: normalizeText(companyData.industryLarge),
    middle: normalizeText(companyData.industryMiddle),
    small: normalizeText(companyData.industrySmall),
    detail: normalizeText(companyData.industryDetail),
    industry: normalizeText(companyData.industry),
    industries: Array.isArray(companyData.industries)
      ? (companyData.industries as string[]).map(normalizeText).filter((s) => s)
      : [normalizeText(companyData.industries)],
    industryName: normalizeText(companyData.industryName),
  };

  const searchTexts: string[] = [];
  if (existingFields.detail) searchTexts.push(existingFields.detail);
  if (existingFields.small) searchTexts.push(existingFields.small);
  if (existingFields.middle) searchTexts.push(existingFields.middle);
  if (existingFields.large) searchTexts.push(existingFields.large);
  if (existingFields.industry) searchTexts.push(existingFields.industry);
  if (existingFields.industryName) searchTexts.push(existingFields.industryName);
  searchTexts.push(...existingFields.industries.filter((s) => s));

  // 優先度1: 既存の完全な階層がindustries.csvに存在するかチェック（正規化キーマップを使用）
  if (
    existingFields.large &&
    existingFields.middle &&
    existingFields.small
  ) {
    const normalizedKey = `${existingFields.large}|${existingFields.middle}|${existingFields.small}`;
    const matchedItem = industryMaster.normalizedTreeKeyMap.get(normalizedKey);
    if (matchedItem) {
      return {
        large: matchedItem.large,
        middle: matchedItem.middle,
        small: matchedItem.small,
        detail: existingFields.detail || matchedItem.small,
        method: "exact",
        confidence: "high",
      };
    }
  }

  // 優先度2: industryDetailから小分類を逆引き
  if (existingFields.detail) {
    const matches = findMatchesByText(
      existingFields.detail,
      industryMaster,
      "small"
    );
    if (matches.length === 1) {
      const match = matches[0];
      return {
        large: match.large,
        middle: match.middle,
        small: match.small,
        detail: existingFields.detail,
        method: "normalized",
        confidence: "high",
      };
    } else if (matches.length > 1) {
      return {
        large: matches[0].large,
        middle: matches[0].middle,
        small: matches[0].small,
        detail: existingFields.detail,
        method: "manual-needed",
        confidence: "low",
        candidates: matches,
      };
    }
  }

  // 優先度3: industrySmallから中分類・大分類を逆引き（正規化インデックスを使用）
  if (existingFields.small) {
    const matches = industryMaster.normalizedTreeBySmall.get(existingFields.small) || [];
    const uniqueMatches = Array.from(
      new Map(matches.map((m) => [`${m.large}|${m.middle}|${m.small}`, m])).values()
    );

    if (uniqueMatches.length === 1) {
      return {
        large: uniqueMatches[0].large,
        middle: uniqueMatches[0].middle,
        small: uniqueMatches[0].small,
        detail: existingFields.detail || uniqueMatches[0].small,
        method: "normalized",
        confidence: "high",
      };
    } else if (uniqueMatches.length > 1) {
      return {
        large: uniqueMatches[0].large,
        middle: uniqueMatches[0].middle,
        small: uniqueMatches[0].small,
        detail: existingFields.detail || uniqueMatches[0].small,
        method: "manual-needed",
        confidence: "medium",
        candidates: uniqueMatches,
      };
    }
  }

  // 優先度4: テキストマッチング（部分一致）
  for (const searchText of searchTexts) {
    if (!searchText) continue;

    const matches = findMatchesByText(searchText, industryMaster, "all");
    if (matches.length === 1) {
      return {
        large: matches[0].large,
        middle: matches[0].middle,
        small: matches[0].small,
        detail: existingFields.detail || matches[0].small,
        method: "fuzzy",
        confidence: "medium",
      };
    } else if (matches.length > 1) {
      // 複数候補がある場合は最初の1つを採用（要確認フラグ付き）
      return {
        large: matches[0].large,
        middle: matches[0].middle,
        small: matches[0].small,
        detail: existingFields.detail || matches[0].small,
        method: "manual-needed",
        confidence: "low",
        candidates: matches,
      };
    }
  }

  return null;
}

function findMatchesByText(
  text: string,
  industryMaster: {
    tree: IndustryTree[];
    treeByLarge: Map<string, IndustryTree[]>;
    treeByMiddle: Map<string, IndustryTree[]>;
    treeBySmall: Map<string, IndustryTree[]>;
    normalizedTreeByLarge: Map<string, IndustryTree[]>;
    normalizedTreeByMiddle: Map<string, IndustryTree[]>;
    normalizedTreeBySmall: Map<string, IndustryTree[]>;
    normalizedTreeKeyMap: Map<string, IndustryTree>;
  },
  target: "large" | "middle" | "small" | "all"
): IndustryTree[] {
  const normalizedText = normalizeText(text);
  const matches: IndustryTree[] = [];
  const seen = new Set<string>();

  // インデックスベースで高速検索
  if (target === "all" || target === "large") {
    // 正規化インデックスから完全一致または部分一致を検索
    for (const [normalizedKey, items] of industryMaster.normalizedTreeByLarge.entries()) {
      if (normalizedKey.includes(normalizedText) || normalizedText.includes(normalizedKey)) {
        for (const item of items) {
          const key = `${item.large}|${item.middle}|${item.small}`;
          if (!seen.has(key)) {
            matches.push(item);
            seen.add(key);
          }
        }
      }
    }
  }

  if (target === "all" || target === "middle") {
    for (const [normalizedKey, items] of industryMaster.normalizedTreeByMiddle.entries()) {
      if (normalizedKey.includes(normalizedText) || normalizedText.includes(normalizedKey)) {
        for (const item of items) {
          const key = `${item.large}|${item.middle}|${item.small}`;
          if (!seen.has(key)) {
            matches.push(item);
            seen.add(key);
          }
        }
      }
    }
  }

  if (target === "all" || target === "small") {
    for (const [normalizedKey, items] of industryMaster.normalizedTreeBySmall.entries()) {
      if (normalizedKey.includes(normalizedText) || normalizedText.includes(normalizedKey)) {
        for (const item of items) {
          const key = `${item.large}|${item.middle}|${item.small}`;
          if (!seen.has(key)) {
            matches.push(item);
            seen.add(key);
          }
        }
      }
    }
  }

  return matches;
}

// ------------------------------
// メイン処理
// ------------------------------

// ------------------------------
// 並列処理用のヘルパー関数
// ------------------------------

/**
 * チャンク配列に分割
 */
function chunkArray<T>(array: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += size) {
    chunks.push(array.slice(i, i + size));
  }
  return chunks;
}

/**
 * 1つのドキュメントを処理
 */
async function processDocument(
  doc: admin.firestore.QueryDocumentSnapshot,
  industryMaster: {
    tree: IndustryTree[];
    treeByLarge: Map<string, IndustryTree[]>;
    treeByMiddle: Map<string, IndustryTree[]>;
    treeBySmall: Map<string, IndustryTree[]>;
    normalizedTreeByLarge: Map<string, IndustryTree[]>;
    normalizedTreeByMiddle: Map<string, IndustryTree[]>;
    normalizedTreeBySmall: Map<string, IndustryTree[]>;
    normalizedTreeKeyMap: Map<string, IndustryTree>;
  },
  dryRun: boolean
): Promise<{
  result: BackfillResult | null;
  needsUpdate: boolean;
  finalAfter: { large: string; middle: string; small: string; detail: string } | null;
  error: string | null;
}> {
  try {
    const companyData: CompanyData = {
      docId: doc.id,
      ...doc.data(),
    };

    const before = {
      large: companyData.industryLarge || "",
      middle: companyData.industryMiddle || "",
      small: companyData.industrySmall || "",
      detail: companyData.industryDetail || "",
    };

    // 高速化: 既に完全にマッチしている場合は早期チェック
    let skipMatching = false;
    if (before.large && before.middle && before.small) {
      const normalizedKey = `${normalizeText(before.large)}|${normalizeText(before.middle)}|${normalizeText(before.small)}`;
      if (industryMaster.normalizedTreeKeyMap.has(normalizedKey)) {
        skipMatching = true;
      }
    }

    // 業種マッチング
    const match = skipMatching
      ? {
          large: before.large,
          middle: before.middle,
          small: before.small,
          detail: before.detail || before.small,
          method: "exact" as const,
          confidence: "high" as const,
        }
      : findIndustryMatch(companyData, industryMaster);

    // matchに基づいて最終的な値を決定
    let finalAfter: {
      large: string;
      middle: string;
      small: string;
      detail: string;
    };

    if (!match) {
      finalAfter = {
        large: before.large || "未確定",
        middle: before.middle || "未確定",
        small: before.small || "未確定",
        detail: before.detail || before.small || "未確定",
      };
    } else {
      finalAfter = {
        large: match.large,
        middle: match.middle,
        small: match.small,
        detail: match.detail || match.small,
      };
    }

    const result: BackfillResult = {
      docId: companyData.docId,
      corporateNumber: companyData.corporateNumber || "",
      name: companyData.name || "",
      before,
      after: finalAfter,
      method: match?.method || "unresolved",
      confidence: match?.confidence || "low",
      unresolved: match ? undefined : "マッチする業種が見つかりませんでした",
      candidates: match?.candidates
        ? match.candidates.map((c) => `${c.large}/${c.middle}/${c.small}`).join("; ")
        : undefined,
    };

    // 更新が必要な場合
    const needsUpdate: boolean =
      !dryRun &&
      !!match &&
      match.method !== "manual-needed" &&
      match.confidence !== "low" &&
      finalAfter.large !== "未確定" &&
      finalAfter.middle !== "未確定" &&
      finalAfter.small !== "未確定" &&
      (before.large !== finalAfter.large ||
        before.middle !== finalAfter.middle ||
        before.small !== finalAfter.small ||
        before.detail !== finalAfter.detail) &&
      !skipMatching;

    return { result, needsUpdate, finalAfter, error: null };
  } catch (error: any) {
    return {
      result: null,
      needsUpdate: false,
      finalAfter: null,
      error: `データ処理エラー: ${error.message}`,
    };
  }
}

async function backfillIndustries() {
  try {
    const dryRun = process.env.DRY_RUN === "1";
    const limit = process.env.LIMIT ? parseInt(process.env.LIMIT, 10) : undefined;
    const startAfterId = process.env.START_AFTER_ID;
    const parallelWorkers = process.env.PARALLEL_WORKERS ? parseInt(process.env.PARALLEL_WORKERS, 10) : 16;

    console.log("業種バックフィル処理を開始...");
    if (dryRun) {
      console.log("⚠️  DRY_RUNモード: Firestoreは更新しません");
    }
    if (limit) {
      console.log(`📊 処理件数上限: ${limit} 件`);
    }
    if (startAfterId) {
      console.log(`📍 開始ID: ${startAfterId}`);
    }
    console.log(`⚡ 並列処理数: ${parallelWorkers} 並列`);

    // industries.csv を読み込み
    const csvPath = path.join(process.cwd(), "scripts", "industries.csv");
    if (!fs.existsSync(csvPath)) {
      console.error(`❌ エラー: industries.csv が見つかりません: ${csvPath}`);
      process.exit(1);
    }

    const industryMaster = loadIndustryMaster(csvPath);

    // 出力ディレクトリを作成
    const outDir = path.join(process.cwd(), "out");
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    // ログファイルパス
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const logFilePath = path.join(outDir, `industry_backfill_${timestamp}.log`);
    const updatedLogPath = path.join(outDir, `industry_backfill_updated_${timestamp}.log`);
    const errorLogPath = path.join(outDir, `industry_backfill_errors_${timestamp}.log`);

    // ログストリーム（追記モード）
    const logStream = fs.createWriteStream(logFilePath, { encoding: "utf8", flags: "w" });
    const updatedLogStream = fs.createWriteStream(updatedLogPath, { encoding: "utf8", flags: "w" });
    const errorLogStream = fs.createWriteStream(errorLogPath, { encoding: "utf8", flags: "w" });

    // ログヘッダー
    logStream.write(`# 業種バックフィル処理ログ\n`);
    logStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    logStream.write(`# DRY_RUN: ${dryRun}\n`);
    logStream.write(`# LIMIT: ${limit || "なし"}\n`);
    logStream.write(`# START_AFTER_ID: ${startAfterId || "なし"}\n`);
    logStream.write(`# PARALLEL_WORKERS: ${parallelWorkers}\n`);
    logStream.write(`#\n`);

    updatedLogStream.write(`# 更新されたドキュメント一覧\n`);
    updatedLogStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    updatedLogStream.write(`# フォーマット: docId,corporateNumber,name,large,middle,small,detail\n`);
    updatedLogStream.write(`#\n`);

    errorLogStream.write(`# エラー発生ドキュメント一覧\n`);
    errorLogStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    errorLogStream.write(`# フォーマット: docId,corporateNumber,name,error\n`);
    errorLogStream.write(`#\n`);

    // レポート用データ
    const reportRows: BackfillResult[] = [];
    const unresolvedRows: BackfillResult[] = [];

    let totalProcessed = 0;
    let totalUpdated = 0;
    let totalUnresolved = 0;
    let totalErrors = 0;
    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;

    // companies_new を取得（orderByで効率的なページネーション）
    // タイムアウト対策のため、バッチサイズを1000に設定
    const BATCH_SIZE = 1000;
    const MAX_BATCH_COMMIT_SIZE = 500; // Firestoreのバッチ制限は500
    const MAX_RETRIES = 3; // クエリリトライ回数
    const RETRY_DELAY = 5000; // リトライ待機時間（ミリ秒）
    
    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    /**
     * リトライ付きクエリ実行
     */
    async function executeQueryWithRetry(
      query: admin.firestore.Query,
      retryCount: number = 0
    ): Promise<admin.firestore.QuerySnapshot> {
      try {
        return await query.get();
      } catch (error: any) {
        // タイムアウトエラーまたは一時的なエラーの場合、リトライ
        if (
          (error.code === 14 || error.code === 4 || error.code === 13) &&
          retryCount < MAX_RETRIES
        ) {
          const delay = RETRY_DELAY * (retryCount + 1);
          console.warn(
            `⚠️  クエリエラー (code: ${error.code}), ${delay}ms後にリトライします (${retryCount + 1}/${MAX_RETRIES})...`
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
          return executeQueryWithRetry(query, retryCount + 1);
        }
        throw error;
      }
    }

    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (startAfterId && !lastDoc) {
        // 初回のみSTART_AFTER_IDで開始
        const startDoc = await db.collection("companies_new").doc(startAfterId).get();
        if (startDoc.exists) {
          query = query.startAfter(startDoc);
        }
      } else if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      // リトライ付きクエリ実行
      let snapshot: admin.firestore.QuerySnapshot;
      try {
        snapshot = await executeQueryWithRetry(query);
      } catch (error: any) {
        // 最終的なエラー処理
        console.error(`❌ クエリエラー（リトライ後も失敗）:`, error.message);
        console.error(`   最後に処理したdocId: ${lastDoc?.id || "なし"}`);
        console.error(`   このdocIdをSTART_AFTER_IDに指定して再開できます`);
        
        // エラーログに記録
        const fatalErrorLogPath = path.join(
          outDir,
          `industry_backfill_fatal_error_${timestamp}.log`
        );
        fs.writeFileSync(
          fatalErrorLogPath,
          `# 重大エラー発生\n` +
          `# 時刻: ${new Date().toISOString()}\n` +
          `# エラー: ${error.message}\n` +
          `# コード: ${error.code}\n` +
          `# 最後に処理したdocId: ${lastDoc?.id || "なし"}\n` +
          `# 処理済み: ${totalProcessed} 件\n` +
          `# 更新: ${totalUpdated} 件\n` +
          `#\n` +
          `# 再開コマンド:\n` +
          `# export START_AFTER_ID='${lastDoc?.id || ""}'\n` +
          `# npx ts-node scripts/backfill_industries.ts\n`
        );
        console.error(`📁 エラーログ: ${fatalErrorLogPath}`);
        throw error;
      }
      
      if (snapshot.empty || (limit && totalProcessed >= limit)) {
        break;
      }

      console.log(`\nバッチ取得: ${snapshot.size} 件`);

      // 並列処理用にチャンクに分割
      const docs: admin.firestore.QueryDocumentSnapshot[] = snapshot.docs;
      const chunks = chunkArray(docs, parallelWorkers);

      let batch = db.batch();
      let batchCount = 0;

      // すべてのチャンクを並列で処理（処理速度向上）
      const chunkPromises = chunks.map((chunk) =>
        Promise.all(chunk.map((doc) => processDocument(doc, industryMaster, dryRun)))
      );

      const chunkResults = await Promise.all(chunkPromises);

      // チャンクごとの結果を順次処理
      for (let chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
        const chunk = chunks[chunkIndex];
        const results = chunkResults[chunkIndex];

        for (let i = 0; i < results.length; i++) {
          const { result, needsUpdate, finalAfter, error } = results[i];
          const doc: admin.firestore.QueryDocumentSnapshot = chunk[i];

          totalProcessed++;

          if (limit && totalProcessed > limit) {
            break;
          }

          // エラーハンドリング
          if (error) {
            totalErrors++;
            console.error(`  [エラー] ${doc.id}: ${error}`);
            errorLogStream.write(`${doc.id},"","","${error}"\n`);
            logStream.write(`ERROR: ${doc.id} - ${error}\n`);
            continue;
          }

          if (!result || !finalAfter) {
            totalErrors++;
            const errorMsg = "処理結果がnull";
            console.error(`  [エラー] ${doc.id}: ${errorMsg}`);
            errorLogStream.write(`${doc.id},"","","${errorMsg}"\n`);
            logStream.write(`ERROR: ${doc.id} - ${errorMsg}\n`);
            continue;
          }

          reportRows.push(result);

          // 未確定または要確認の場合は unresolved に追加
          const isUnresolved =
            !result.method ||
            result.method === "manual-needed" ||
            result.confidence === "low" ||
            finalAfter.large === "未確定" ||
            finalAfter.middle === "未確定" ||
            finalAfter.small === "未確定";

          if (isUnresolved) {
            unresolvedRows.push(result);
            totalUnresolved++;
          }

          // 更新が必要な場合
          if (needsUpdate && finalAfter) {
            try {
              batch.update(doc.ref, {
                industryLarge: finalAfter.large,
                industryMiddle: finalAfter.middle,
                industrySmall: finalAfter.small,
                industryDetail: finalAfter.detail,
              });
              batchCount++;
              totalUpdated++;

              // 更新ログに記録（バッファリングでパフォーマンス向上）
              updatedLogStream.write(`${doc.id},"${result.corporateNumber || ""}","${result.name || ""}","${finalAfter.large}","${finalAfter.middle}","${finalAfter.small}","${finalAfter.detail}"\n`);
              // 詳細ログは必要最小限に（パフォーマンス向上のため）
              // logStream.write(`UPDATED: ${doc.id} - ${result.name || ""} - ${finalAfter.large}/${finalAfter.middle}/${finalAfter.small}\n`);

              if (batchCount >= MAX_BATCH_COMMIT_SIZE) {
                await batch.commit();
                console.log(`  バッチコミット完了: ${batchCount} 件`);
                logStream.write(`# バッチコミット: ${batchCount} 件 at ${new Date().toISOString()}\n`);
                updatedLogStream.write(`# バッチコミット: ${batchCount} 件 at ${new Date().toISOString()}\n`);
                batch = db.batch();
                batchCount = 0;
              }
            } catch (error: any) {
              totalErrors++;
              const errorMsg = `更新エラー: ${error.message}`;
              console.error(`  [エラー] ${doc.id}: ${errorMsg}`);
              errorLogStream.write(`${doc.id},"${result.corporateNumber || ""}","${result.name || ""}","${errorMsg}"\n`);
              logStream.write(`ERROR: ${doc.id} - ${errorMsg}\n`);
            }
          }
        }

        if (limit && totalProcessed >= limit) {
          break;
        }
      }

      if (batchCount > 0 && !dryRun) {
        try {
          await batch.commit();
          console.log(`  バッチコミット完了: ${batchCount} 件`);
          logStream.write(`# バッチコミット: ${batchCount} 件 at ${new Date().toISOString()}\n`);
          updatedLogStream.write(`# バッチコミット: ${batchCount} 件 at ${new Date().toISOString()}\n`);
        } catch (error: any) {
          totalErrors++;
          const errorMsg = `バッチコミットエラー: ${error.message}`;
          console.error(`  [エラー] バッチコミット失敗: ${errorMsg}`);
          errorLogStream.write(`BATCH_COMMIT_ERROR,"","","${errorMsg}"\n`);
          logStream.write(`ERROR: バッチコミット - ${errorMsg}\n`);
        }
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
      const progressMsg = `処理済み: ${totalProcessed} 件 / 更新: ${totalUpdated} 件 / 未確定: ${totalUnresolved} 件 / エラー: ${totalErrors} 件`;
      console.log(progressMsg);
      logStream.write(`# Progress: ${progressMsg} at ${new Date().toISOString()}\n`);

      if (limit && totalProcessed >= limit) {
        break;
      }
    }

    // ログストリームを閉じる
    logStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    logStream.end();
    updatedLogStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    updatedLogStream.end();
    errorLogStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    errorLogStream.end();

    // レポート出力
    const reportPath = path.join(outDir, "industry_backfill_report.csv");
    const unresolvedPath = path.join(outDir, "industry_unresolved.csv");

    // CSV出力関数
    function writeCSV(filePath: string, rows: BackfillResult[]) {
      const headers = [
        "docId",
        "corporateNumber",
        "name",
        "beforeLarge",
        "beforeMiddle",
        "beforeSmall",
        "beforeDetail",
        "afterLarge",
        "afterMiddle",
        "afterSmall",
        "afterDetail",
        "method",
        "confidence",
        "unresolved",
        "candidates",
      ];

      const csvRows = rows.map((row) => [
        row.docId,
        row.corporateNumber,
        row.name,
        row.before.large,
        row.before.middle,
        row.before.small,
        row.before.detail,
        row.after.large,
        row.after.middle,
        row.after.small,
        row.after.detail,
        row.method,
        row.confidence,
        row.unresolved || "",
        row.candidates || "",
      ]);

      const lines = [headers.join(",")];
      for (const row of csvRows) {
        const escaped = row.map((cell) => {
          const str = String(cell || "");
          if (str.includes(",") || str.includes('"') || str.includes("\n")) {
            return `"${str.replace(/"/g, '""')}"`;
          }
          return str;
        });
        lines.push(escaped.join(","));
      }

      fs.writeFileSync(filePath, lines.join("\n"), "utf8");
    }

    writeCSV(reportPath, reportRows);
    writeCSV(unresolvedPath, unresolvedRows);

    console.log(`\n✅ 処理完了`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`更新数: ${totalUpdated} 件`);
    console.log(`未確定数: ${totalUnresolved} 件`);
    console.log(`エラー数: ${totalErrors} 件`);
    console.log(`\n📁 出力ファイル:`);
    console.log(`  - ${reportPath}`);
    console.log(`  - ${unresolvedPath}`);
    console.log(`  - ${logFilePath} (処理ログ)`);
    console.log(`  - ${updatedLogPath} (更新されたドキュメント一覧)`);
    if (totalErrors > 0) {
      console.log(`  - ${errorLogPath} (エラーログ)`);
    }

  } catch (error: any) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error("❌ 重大エラー:", errorMsg);
    console.error(error);
    
    // エラーログにも記録（可能な場合）
    try {
      const outDir = path.join(process.cwd(), "out");
      const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
      const errorLogPath = path.join(outDir, `industry_backfill_fatal_error_${timestamp}.log`);
      fs.writeFileSync(
        errorLogPath,
        `# 重大エラー発生\n` +
        `時刻: ${new Date().toISOString()}\n` +
        `エラー: ${errorMsg}\n` +
        `スタックトレース:\n${error instanceof Error ? error.stack : String(error)}\n`,
        "utf8"
      );
      console.error(`\n📁 エラーログ: ${errorLogPath}`);
    } catch (logError) {
      // ログ出力に失敗しても処理は継続
    }
    
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
backfillIndustries()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });

