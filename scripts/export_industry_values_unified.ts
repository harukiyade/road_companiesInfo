/* eslint-disable no-console */

/**
 * scripts/export_industry_values_unified.ts
 *
 * ✅ 目的
 * - companies_newコレクションの業種フィールド（industryLarge, industryMiddle, industrySmall, industryDetail）の値一覧を抽出
 * - 類似した表記を統一（例：「ホテル」と「ホテル業」を統一）
 * - 各フィールドの値とその出現回数をCSVに出力
 * - 検索可能なデータベースとして使用
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

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

interface IndustryValueCount {
  value: string;
  normalizedValue: string;
  count: number;
  originalValues: Set<string>; // 統一前の元の値の集合
}

// ------------------------------
// 文字列正規化（表記統一用）
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

/**
 * 表記を統一する（「業」の有無を考慮）
 * 正規化後に同じ値になるものは、出現回数の多い方を優先して統一
 */
function unifySimilarValues(values: Map<string, number>): Map<string, IndustryValueCount> {
  const normalizedToOriginal = new Map<string, Map<string, number>>(); // normalized -> original values
  const unified = new Map<string, IndustryValueCount>();

  // 正規化してグループ化
  for (const [originalValue, count] of values.entries()) {
    const normalized = normalizeText(originalValue);
    if (!normalized) continue;

    if (!normalizedToOriginal.has(normalized)) {
      normalizedToOriginal.set(normalized, new Map());
    }
    const originalMap = normalizedToOriginal.get(normalized)!;
    originalMap.set(originalValue, count);
  }

  // 各正規化グループから代表値を選定（出現回数の多い方を優先）
  for (const [normalized, originalMap] of normalizedToOriginal.entries()) {
    // 出現回数順にソート
    const sorted = Array.from(originalMap.entries()).sort((a, b) => b[1] - a[1]);
    const representativeValue = sorted[0][0]; // 出現回数の多い方を代表値に
    const totalCount = Array.from(originalMap.values()).reduce((sum, count) => sum + count, 0);
    const originalValues = new Set(originalMap.keys());

    unified.set(representativeValue, {
      value: representativeValue,
      normalizedValue: normalized,
      count: totalCount,
      originalValues,
    });
  }

  return unified;
}

// ------------------------------
// メイン処理
// ------------------------------

async function exportIndustryValuesUnified() {
  try {
    console.log("業種フィールドの値一覧を抽出開始（表記統一版）...");

    // 値の集計用マップ（統一前）
    const largeValues = new Map<string, number>();
    const middleValues = new Map<string, number>();
    const smallValues = new Map<string, number>();
    const detailValues = new Map<string, number>();

    // 出力ディレクトリを作成
    const outDir = path.join(process.cwd(), "out");
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const logFilePath = path.join(outDir, `export_industry_values_unified_${timestamp}.log`);
    const logStream = fs.createWriteStream(logFilePath, { encoding: "utf8", flags: "w" });

    logStream.write(`# 業種フィールド値抽出ログ（表記統一版）\n`);
    logStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    logStream.write(`#\n`);

    const BATCH_SIZE = 1000;
    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;

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
        if (
          (error.code === 14 || error.code === 4 || error.code === 13) &&
          retryCount < 3
        ) {
          const delay = 5000 * (retryCount + 1);
          console.warn(
            `⚠️  クエリエラー (code: ${error.code}), ${delay}ms後にリトライします (${retryCount + 1}/3)...`
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
          return executeQueryWithRetry(query, retryCount + 1);
        }
        throw error;
      }
    }

    console.log("データ収集を開始...");
    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      let snapshot: admin.firestore.QuerySnapshot;
      try {
        snapshot = await executeQueryWithRetry(query);
      } catch (error: any) {
        console.error(`❌ クエリエラー:`, error.message);
        logStream.write(`ERROR: ${error.message}\n`);
        break;
      }

      if (snapshot.empty) {
        break;
      }

      // 進捗表示（10000件ごと）
      if (totalProcessed % 10000 === 0 && totalProcessed > 0) {
        console.log(`処理済み: ${totalProcessed} 件`);
      }

      // 各ドキュメントの業種フィールドを集計
      for (const doc of snapshot.docs) {
        const data = doc.data();

        // industryLarge
        if (data.industryLarge && typeof data.industryLarge === "string") {
          const value = data.industryLarge.trim();
          if (value) {
            largeValues.set(value, (largeValues.get(value) || 0) + 1);
          }
        }

        // industryMiddle
        if (data.industryMiddle && typeof data.industryMiddle === "string") {
          const value = data.industryMiddle.trim();
          if (value) {
            middleValues.set(value, (middleValues.get(value) || 0) + 1);
          }
        }

        // industrySmall
        if (data.industrySmall && typeof data.industrySmall === "string") {
          const value = data.industrySmall.trim();
          if (value) {
            smallValues.set(value, (smallValues.get(value) || 0) + 1);
          }
        }

        // industryDetail
        if (data.industryDetail && typeof data.industryDetail === "string") {
          const value = data.industryDetail.trim();
          if (value) {
            detailValues.set(value, (detailValues.get(value) || 0) + 1);
          }
        }

        totalProcessed++;
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
      logStream.write(`# Progress: 処理済み: ${totalProcessed} 件 at ${new Date().toISOString()}\n`);
    }

    console.log(`\nデータ収集完了: ${totalProcessed} 件`);
    console.log("表記の統一処理を開始...");

    // 表記を統一
    const largeUnified = unifySimilarValues(largeValues);
    const middleUnified = unifySimilarValues(middleValues);
    const smallUnified = unifySimilarValues(smallValues);
    const detailUnified = unifySimilarValues(detailValues);

    console.log("統一前の種類数:");
    console.log(`  industryLarge: ${largeValues.size} → 統一後: ${largeUnified.size}`);
    console.log(`  industryMiddle: ${middleValues.size} → 統一後: ${middleUnified.size}`);
    console.log(`  industrySmall: ${smallValues.size} → 統一後: ${smallUnified.size}`);
    console.log(`  industryDetail: ${detailValues.size} → 統一後: ${detailUnified.size}`);

    // 結果をソート（出現回数の降順、値の昇順）
    const sortUnified = (map: Map<string, IndustryValueCount>): IndustryValueCount[] => {
      return Array.from(map.values())
        .sort((a, b) => {
          // まず出現回数で降順ソート
          if (b.count !== a.count) {
            return b.count - a.count;
          }
          // 出現回数が同じ場合は値で昇順ソート
          return a.value.localeCompare(b.value, "ja");
        });
    };

    const largeSorted = sortUnified(largeUnified);
    const middleSorted = sortUnified(middleUnified);
    const smallSorted = sortUnified(smallUnified);
    const detailSorted = sortUnified(detailUnified);

    // CSVファイルに出力（統合版）
    const csvPath = path.join(outDir, `industry_values_unified_${timestamp}.csv`);
    const csvStream = fs.createWriteStream(csvPath, { encoding: "utf8", flags: "w" });

    // CSVヘッダー
    csvStream.write("フィールド,統一後の値,正規化値,出現回数,統一前の値一覧\n");

    // データを書き込み
    const writeUnifiedRow = (stream: fs.WriteStream, fieldName: string, item: IndustryValueCount) => {
      const originalValuesList = Array.from(item.originalValues).join(" | ");
      stream.write(
        `${fieldName},"${item.value.replace(/"/g, '""')}","${item.normalizedValue.replace(/"/g, '""')}",${item.count},"${originalValuesList.replace(/"/g, '""')}"\n`
      );
    };

    for (const item of largeSorted) {
      writeUnifiedRow(csvStream, "industryLarge", item);
    }
    for (const item of middleSorted) {
      writeUnifiedRow(csvStream, "industryMiddle", item);
    }
    for (const item of smallSorted) {
      writeUnifiedRow(csvStream, "industrySmall", item);
    }
    for (const item of detailSorted) {
      writeUnifiedRow(csvStream, "industryDetail", item);
    }

    csvStream.end();

    // 各フィールドごとのファイルも作成（検索しやすくするため）
    const largeCsvPath = path.join(outDir, `industryLarge_values_unified_${timestamp}.csv`);
    const middleCsvPath = path.join(outDir, `industryMiddle_values_unified_${timestamp}.csv`);
    const smallCsvPath = path.join(outDir, `industrySmall_values_unified_${timestamp}.csv`);
    const detailCsvPath = path.join(outDir, `industryDetail_values_unified_${timestamp}.csv`);

    const writeFieldCsv = (filePath: string, items: IndustryValueCount[], fieldName: string) => {
      const stream = fs.createWriteStream(filePath, { encoding: "utf8", flags: "w" });
      stream.write(`${fieldName},正規化値,出現回数,統一前の値一覧\n`);
      for (const item of items) {
        const originalValuesList = Array.from(item.originalValues).join(" | ");
        stream.write(
          `"${item.value.replace(/"/g, '""')}","${item.normalizedValue.replace(/"/g, '""')}",${item.count},"${originalValuesList.replace(/"/g, '""')}"\n`
        );
      }
      stream.end();
    };

    writeFieldCsv(largeCsvPath, largeSorted, "industryLarge");
    writeFieldCsv(middleCsvPath, middleSorted, "industryMiddle");
    writeFieldCsv(smallCsvPath, smallSorted, "industrySmall");
    writeFieldCsv(detailCsvPath, detailSorted, "industryDetail");

    logStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    logStream.end();

    // サマリー出力
    console.log(`\n✅ 処理完了`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`\n📊 集計結果:`);
    console.log(`  industryLarge: ${largeUnified.size} 種類（統一前: ${largeValues.size}）`);
    console.log(`  industryMiddle: ${middleUnified.size} 種類（統一前: ${middleValues.size}）`);
    console.log(`  industrySmall: ${smallUnified.size} 種類（統一前: ${smallValues.size}）`);
    console.log(`  industryDetail: ${detailUnified.size} 種類（統一前: ${detailValues.size}）`);
    console.log(`\n📁 出力ファイル:`);
    console.log(`  - ${csvPath} (全フィールド統合CSV)`);
    console.log(`  - ${largeCsvPath} (industryLarge専用)`);
    console.log(`  - ${middleCsvPath} (industryMiddle専用)`);
    console.log(`  - ${smallCsvPath} (industrySmall専用)`);
    console.log(`  - ${detailCsvPath} (industryDetail専用)`);
    console.log(`  - ${logFilePath} (処理ログ)`);

    // トップ10を表示
    console.log(`\n📈 トップ10（出現回数順）:`);
    console.log(`\n【industryLarge】`);
    largeSorted.slice(0, 10).forEach((item, index) => {
      console.log(`  ${index + 1}. ${item.value} (${item.count}件)`);
    });
    console.log(`\n【industryMiddle】`);
    middleSorted.slice(0, 10).forEach((item, index) => {
      console.log(`  ${index + 1}. ${item.value} (${item.count}件)`);
    });
    console.log(`\n【industrySmall】`);
    smallSorted.slice(0, 10).forEach((item, index) => {
      console.log(`  ${index + 1}. ${item.value} (${item.count}件)`);
    });
    console.log(`\n【industryDetail】`);
    detailSorted.slice(0, 10).forEach((item, index) => {
      console.log(`  ${index + 1}. ${item.value} (${item.count}件)`);
    });

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
exportIndustryValuesUnified()
  .then(() => {
    console.log("\n処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
