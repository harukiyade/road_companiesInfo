/* eslint-disable no-console */

/**
 * scripts/export_industry_values.ts
 *
 * ✅ 目的
 * - companies_newコレクションの業種フィールド（industryLarge, industryMiddle, industrySmall, industryDetail）の値一覧を抽出
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
  count: number;
}

// ------------------------------
// メイン処理
// ------------------------------

async function exportIndustryValues() {
  try {
    console.log("業種フィールドの値一覧を抽出開始...");

    // 値の集計用マップ
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
    const logFilePath = path.join(outDir, `export_industry_values_${timestamp}.log`);
    const logStream = fs.createWriteStream(logFilePath, { encoding: "utf8", flags: "w" });

    logStream.write(`# 業種フィールド値抽出ログ\n`);
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

      console.log(`バッチ取得: ${snapshot.size} 件`);

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
      const progressMsg = `処理済み: ${totalProcessed} 件`;
      console.log(progressMsg);
      logStream.write(`# Progress: ${progressMsg} at ${new Date().toISOString()}\n`);
    }

    logStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    logStream.end();

    // 結果をソート（出現回数の降順、値の昇順）
    const sortValues = (map: Map<string, number>): IndustryValueCount[] => {
      return Array.from(map.entries())
        .map(([value, count]) => ({ value, count }))
        .sort((a, b) => {
          // まず出現回数で降順ソート
          if (b.count !== a.count) {
            return b.count - a.count;
          }
          // 出現回数が同じ場合は値で昇順ソート
          return a.value.localeCompare(b.value, "ja");
        });
    };

    const largeSorted = sortValues(largeValues);
    const middleSorted = sortValues(middleValues);
    const smallSorted = sortValues(smallValues);
    const detailSorted = sortValues(detailValues);

    // CSVファイルに出力
    const csvPath = path.join(outDir, `industry_values_${timestamp}.csv`);
    const csvStream = fs.createWriteStream(csvPath, { encoding: "utf8", flags: "w" });

    // CSVヘッダー
    csvStream.write("フィールド,値,出現回数\n");

    // データを書き込み
    for (const item of largeSorted) {
      csvStream.write(`industryLarge,"${item.value.replace(/"/g, '""')}",${item.count}\n`);
    }
    for (const item of middleSorted) {
      csvStream.write(`industryMiddle,"${item.value.replace(/"/g, '""')}",${item.count}\n`);
    }
    for (const item of smallSorted) {
      csvStream.write(`industrySmall,"${item.value.replace(/"/g, '""')}",${item.count}\n`);
    }
    for (const item of detailSorted) {
      csvStream.write(`industryDetail,"${item.value.replace(/"/g, '""')}",${item.count}\n`);
    }

    csvStream.end();

    // 各フィールドごとのファイルも作成（検索しやすくするため）
    const largeCsvPath = path.join(outDir, `industryLarge_values_${timestamp}.csv`);
    const middleCsvPath = path.join(outDir, `industryMiddle_values_${timestamp}.csv`);
    const smallCsvPath = path.join(outDir, `industrySmall_values_${timestamp}.csv`);
    const detailCsvPath = path.join(outDir, `industryDetail_values_${timestamp}.csv`);

    const writeFieldCsv = (filePath: string, items: IndustryValueCount[], fieldName: string) => {
      const stream = fs.createWriteStream(filePath, { encoding: "utf8", flags: "w" });
      stream.write(`${fieldName},出現回数\n`);
      for (const item of items) {
        stream.write(`"${item.value.replace(/"/g, '""')}",${item.count}\n`);
      }
      stream.end();
    };

    writeFieldCsv(largeCsvPath, largeSorted, "industryLarge");
    writeFieldCsv(middleCsvPath, middleSorted, "industryMiddle");
    writeFieldCsv(smallCsvPath, smallSorted, "industrySmall");
    writeFieldCsv(detailCsvPath, detailSorted, "industryDetail");

    // サマリー出力
    console.log(`\n✅ 処理完了`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`\n📊 集計結果:`);
    console.log(`  industryLarge: ${largeValues.size} 種類`);
    console.log(`  industryMiddle: ${middleValues.size} 種類`);
    console.log(`  industrySmall: ${smallValues.size} 種類`);
    console.log(`  industryDetail: ${detailValues.size} 種類`);
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
exportIndustryValues()
  .then(() => {
    console.log("\n処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
