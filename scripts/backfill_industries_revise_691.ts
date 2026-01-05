/* eslint-disable no-console */

/**
 * scripts/backfill_industries_revise_691.ts
 *
 * ✅ 目的
 * - 先ほど更新された691件のドキュメントに対して再度修正
 * - industryフィールドとindustryCategoriesフィールドを優先的に参照
 * - scripts/industries.csvから最適な値を判断してindustryLarge, industryMiddle, industrySmallを更新
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 * - DRY_RUN=1 (任意: 1の場合はFirestoreを更新せずレポートのみ出力)
 * - TARGET_DOC_IDS_FILE=/path/to/doc_ids.txt (任意: 対象ドキュメントIDリストファイル)
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
  industryCategories?: string | null;
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
// 業種マッチング（industryとindustryCategoriesを優先）
// ------------------------------

function findIndustryMatchByIndustryAndCategories(
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
  // industryとindustryCategoriesを優先的に使用
  const priorityTexts: string[] = [];
  
  if (companyData.industry && typeof companyData.industry === "string") {
    priorityTexts.push(companyData.industry);
  }
  
  if (companyData.industryCategories && typeof companyData.industryCategories === "string") {
    priorityTexts.push(companyData.industryCategories);
  }
  
  // 優先度1: industryとindustryCategoriesから完全一致を探す
  for (const text of priorityTexts) {
    if (!text || typeof text !== "string" || text.trim() === "") continue;
    
    const normalizedText = normalizeText(text);
    if (!normalizedText) continue;
    
    // 小分類から検索（最も具体的）
    const matchesSmall = industryMaster.normalizedTreeBySmall.get(normalizedText) || [];
    if (matchesSmall.length === 1) {
      return {
        large: matchesSmall[0].large,
        middle: matchesSmall[0].middle,
        small: matchesSmall[0].small,
        method: "exact",
        confidence: "high",
      };
    } else if (matchesSmall.length > 1) {
      return {
        large: matchesSmall[0].large,
        middle: matchesSmall[0].middle,
        small: matchesSmall[0].small,
        method: "manual-needed",
        confidence: "medium",
        candidates: matchesSmall,
      };
    }
    
    // 中分類から検索
    const matchesMiddle = industryMaster.normalizedTreeByMiddle.get(normalizedText) || [];
    if (matchesMiddle.length > 0) {
      // 中分類が複数ある場合は、最初の1つを採用
      const uniqueMatches = Array.from(
        new Map(matchesMiddle.map((m) => [`${m.large}|${m.middle}|${m.small}`, m])).values()
      );
      if (uniqueMatches.length === 1) {
        return {
          large: uniqueMatches[0].large,
          middle: uniqueMatches[0].middle,
          small: uniqueMatches[0].small,
          method: "normalized",
          confidence: "high",
        };
      } else if (uniqueMatches.length > 1) {
        return {
          large: uniqueMatches[0].large,
          middle: uniqueMatches[0].middle,
          small: uniqueMatches[0].small,
          method: "manual-needed",
          confidence: "medium",
          candidates: uniqueMatches,
        };
      }
    }
    
    // 大分類から検索
    const matchesLarge = industryMaster.normalizedTreeByLarge.get(normalizedText) || [];
    if (matchesLarge.length > 0) {
      const uniqueMatches = Array.from(
        new Map(matchesLarge.map((m) => [`${m.large}|${m.middle}|${m.small}`, m])).values()
      );
      if (uniqueMatches.length === 1) {
        return {
          large: uniqueMatches[0].large,
          middle: uniqueMatches[0].middle,
          small: uniqueMatches[0].small,
          method: "normalized",
          confidence: "medium",
        };
      } else if (uniqueMatches.length > 1) {
        return {
          large: uniqueMatches[0].large,
          middle: uniqueMatches[0].middle,
          small: uniqueMatches[0].small,
          method: "manual-needed",
          confidence: "low",
          candidates: uniqueMatches,
        };
      }
    }
  }
  
  // 優先度2: industryとindustryCategoriesから部分一致を探す
  for (const text of priorityTexts) {
    if (!text || typeof text !== "string" || text.trim() === "") continue;
    
    const normalizedText = normalizeText(text);
    if (!normalizedText) continue;
    
    // 部分一致で検索
    const matches = findMatchesByText(text, industryMaster, "all");
    if (matches.length === 1) {
      return {
        large: matches[0].large,
        middle: matches[0].middle,
        small: matches[0].small,
        method: "fuzzy",
        confidence: "medium",
      };
    } else if (matches.length > 1) {
      // 複数候補がある場合は最初の1つを採用
      return {
        large: matches[0].large,
        middle: matches[0].middle,
        small: matches[0].small,
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

/**
 * 1つのドキュメントを処理
 */
async function processDocument(
  doc: admin.firestore.DocumentSnapshot,
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

    // industryとindustryCategoriesを優先的に使用してマッチング
    const match = findIndustryMatchByIndustryAndCategories(companyData, industryMaster);

    // matchに基づいて最終的な値を決定
    let finalAfter: {
      large: string;
      middle: string;
      small: string;
      detail: string;
    };

    if (!match) {
      // マッチしない場合は既存値を保持
      finalAfter = {
        large: before.large,
        middle: before.middle,
        small: before.small,
        detail: before.detail,
      };
    } else {
      // マッチした場合: industries.csvの値を使用
      finalAfter = {
        large: match.large,
        middle: match.middle,
        small: match.small,
        detail: before.detail || match.small, // detailは既存値があれば保持、なければsmallを使用
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
      unresolved: match ? undefined : "industry/industryCategoriesからマッチする業種が見つかりませんでした",
      candidates: match?.candidates
        ? match.candidates.map((c) => `${c.large}/${c.middle}/${c.small}`).join("; ")
        : undefined,
    };

    // 値に変更があるかチェック
    const hasValueChanges = 
      before.large !== finalAfter.large ||
      before.middle !== finalAfter.middle ||
      before.small !== finalAfter.small;
    
    // 更新条件: 値が変更された場合は更新（detailは変更しない）
    const needsUpdate: boolean =
      !dryRun &&
      hasValueChanges;

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

async function backfillIndustriesRevise691() {
  try {
    const dryRun = process.env.DRY_RUN === "1";
    const targetDocIdsFile = process.env.TARGET_DOC_IDS_FILE || "/tmp/target_doc_ids.txt";

    console.log("業種修正処理（691件）を開始...");
    if (dryRun) {
      console.log("⚠️  DRY_RUNモード: Firestoreは更新しません");
    }

    // industries.csv を読み込み
    const csvPath = path.join(process.cwd(), "scripts", "industries.csv");
    if (!fs.existsSync(csvPath)) {
      console.error(`❌ エラー: industries.csv が見つかりません: ${csvPath}`);
      process.exit(1);
    }

    const industryMaster = loadIndustryMaster(csvPath);

    // 対象ドキュメントIDリストを読み込み
    if (!fs.existsSync(targetDocIdsFile)) {
      console.error(`❌ エラー: 対象ドキュメントIDファイルが存在しません: ${targetDocIdsFile}`);
      process.exit(1);
    }

    const docIds = fs.readFileSync(targetDocIdsFile, "utf-8")
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0);

    console.log(`📋 対象ドキュメント数: ${docIds.length} 件`);

    // 出力ディレクトリを作成
    const outDir = path.join(process.cwd(), "out");
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    // ログファイルパス
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const logFilePath = path.join(outDir, `industry_revise_691_${timestamp}.log`);
    const updatedLogPath = path.join(outDir, `industry_revise_691_updated_${timestamp}.log`);
    const errorLogPath = path.join(outDir, `industry_revise_691_errors_${timestamp}.log`);
    const reportPath = path.join(outDir, `industry_revise_691_report_${timestamp}.csv`);

    // ログストリーム
    const logStream = fs.createWriteStream(logFilePath, { encoding: "utf8", flags: "w" });
    const updatedLogStream = fs.createWriteStream(updatedLogPath, { encoding: "utf8", flags: "w" });
    const errorLogStream = fs.createWriteStream(errorLogPath, { encoding: "utf8", flags: "w" });
    const reportStream = fs.createWriteStream(reportPath, { encoding: "utf8", flags: "w" });

    // ログヘッダー
    logStream.write(`# 業種修正処理ログ（691件）\n`);
    logStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    logStream.write(`# DRY_RUN: ${dryRun}\n`);
    logStream.write(`#\n`);

    updatedLogStream.write(`# 更新されたドキュメント一覧\n`);
    updatedLogStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    updatedLogStream.write(`# フォーマット: docId,corporateNumber,name,large,middle,small,detail\n`);
    updatedLogStream.write(`#\n`);

    errorLogStream.write(`# エラー発生ドキュメント一覧\n`);
    errorLogStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    errorLogStream.write(`# フォーマット: docId,corporateNumber,name,error\n`);
    errorLogStream.write(`#\n`);

    // CSVヘッダー
    const csvHeaders = [
      "docId",
      "corporateNumber",
      "name",
      "industry",
      "industryCategories",
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
    reportStream.write(csvHeaders.join(",") + "\n");

    // CSV行をエスケープして書き込む関数
    function writeCSVRow(stream: NodeJS.WritableStream, result: BackfillResult, companyData: CompanyData) {
      const row = [
        result.docId,
        result.corporateNumber,
        result.name,
        companyData.industry || "",
        companyData.industryCategories || "",
        result.before.large,
        result.before.middle,
        result.before.small,
        result.before.detail,
        result.after.large,
        result.after.middle,
        result.after.small,
        result.after.detail,
        result.method,
        result.confidence,
        result.unresolved || "",
        result.candidates || "",
      ];
      
      const escaped = row.map((cell) => {
        const str = String(cell || "");
        if (str.includes(",") || str.includes('"') || str.includes("\n")) {
          return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
      });
      
      stream.write(escaped.join(",") + "\n");
    }

    let totalProcessed = 0;
    let totalUpdated = 0;
    let totalErrors = 0;
    const MAX_BATCH_COMMIT_SIZE = 300;

    // ドキュメントを取得して処理（並列処理で高速化）
    const BATCH_SIZE = 100;
    let batch = db.batch();
    let batchCount = 0;

    for (let i = 0; i < docIds.length; i += BATCH_SIZE) {
      const batchDocIds = docIds.slice(i, i + BATCH_SIZE);
      
      // 並列でドキュメントを取得
      const docPromises = batchDocIds.map((docId) =>
        db.collection("companies_new").doc(docId).get()
      );
      const docs = await Promise.all(docPromises);

      // 並列で処理
      const processPromises = docs.map((doc) =>
        processDocument(doc, industryMaster, dryRun)
      );
      const results = await Promise.all(processPromises);

      for (let j = 0; j < results.length; j++) {
        const { result, needsUpdate, finalAfter, error } = results[j];
        const doc = docs[j];

        if (!doc.exists) {
          totalErrors++;
          const errorMsg = "ドキュメントが存在しません";
          console.error(`  [エラー] ${doc.id}: ${errorMsg}`);
          errorLogStream.write(`${doc.id},"","","${errorMsg}"\n`);
          continue;
        }

        totalProcessed++;

        // エラーハンドリング
        if (error) {
          totalErrors++;
          console.error(`  [エラー] ${doc.id}: ${error}`);
          errorLogStream.write(`${doc.id},"","","${error}"\n`);
          continue;
        }

        if (!result || !finalAfter) {
          totalErrors++;
          const errorMsg = "処理結果がnull";
          console.error(`  [エラー] ${doc.id}: ${errorMsg}`);
          errorLogStream.write(`${doc.id},"","","${errorMsg}"\n`);
          continue;
        }

        const companyData: CompanyData = {
          docId: doc.id,
          ...doc.data(),
        };

        // 結果をCSVに書き込み
        writeCSVRow(reportStream, result, companyData);

        // 更新が必要な場合
        if (needsUpdate && finalAfter) {
          try {
            // バッチサイズチェック
            if (batchCount >= MAX_BATCH_COMMIT_SIZE) {
              await batch.commit();
              console.log(`  バッチコミット完了: ${batchCount} 件`);
              logStream.write(`# バッチコミット: ${batchCount} 件 at ${new Date().toISOString()}\n`);
              updatedLogStream.write(`# バッチコミット: ${batchCount} 件 at ${new Date().toISOString()}\n`);
              batch = db.batch();
              batchCount = 0;
            }

            batch.update(doc.ref, {
              industryLarge: finalAfter.large,
              industryMiddle: finalAfter.middle,
              industrySmall: finalAfter.small,
              // detailは更新しない（既存値を保持）
            });
            batchCount++;
            totalUpdated++;

            updatedLogStream.write(`${doc.id},"${result.corporateNumber || ""}","${result.name || ""}","${finalAfter.large}","${finalAfter.middle}","${finalAfter.small}","${finalAfter.detail}"\n`);
          } catch (error: any) {
            totalErrors++;
            const errorMsg = `更新エラー: ${error.message}`;
            console.error(`  [エラー] ${doc.id}: ${errorMsg}`);
            errorLogStream.write(`${doc.id},"${result.corporateNumber || ""}","${result.name || ""}","${errorMsg}"\n`);
            
            if (error.message.includes("WriteBatch") || error.message.includes("Transaction too big")) {
              try {
                batch = db.batch();
                batchCount = 0;
              } catch (resetError) {
                // リセットエラーは無視
              }
            }
          }
        }
      }

      const progressMsg = `処理済み: ${totalProcessed} 件 / 更新: ${totalUpdated} 件 / エラー: ${totalErrors} 件`;
      console.log(progressMsg);
      logStream.write(`# Progress: ${progressMsg} at ${new Date().toISOString()}\n`);
    }

    // 残りのバッチをコミット
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
      }
    }

    // ログストリームを閉じる
    logStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    logStream.end();
    updatedLogStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    updatedLogStream.end();
    errorLogStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    errorLogStream.end();
    reportStream.end();

    console.log(`\n✅ 処理完了`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`更新数: ${totalUpdated} 件`);
    console.log(`エラー数: ${totalErrors} 件`);
    console.log(`\n📁 出力ファイル:`);
    console.log(`  - ${reportPath}`);
    console.log(`  - ${logFilePath} (処理ログ)`);
    console.log(`  - ${updatedLogPath} (更新されたドキュメント一覧)`);
    if (totalErrors > 0) {
      console.log(`  - ${errorLogPath} (エラーログ)`);
    }

  } catch (error: any) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error("❌ 重大エラー:", errorMsg);
    console.error(error);
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
backfillIndustriesRevise691()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
