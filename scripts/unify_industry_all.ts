/* eslint-disable no-console */

/**
 * scripts/unify_industry_all.ts
 *
 * ✅ 目的
 * - 統一ルールCSVファイルを読み込んで、companies_newコレクションの業種フィールドを更新
 * - 意味的重複と法人種別の統一を一括実行
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 * - DRY_RUN=1 (任意: 1の場合はFirestoreを更新せずレポートのみ出力)
 * - UNIFICATION_RULES_CSV=/path/to/unification_rules.csv (任意: デフォルトは最新のファイル)
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
// メイン処理
// ------------------------------

async function unifyIndustryAll() {
  try {
    const dryRun = process.env.DRY_RUN === "1";
    
    // 統一ルールCSVファイルを取得
    let unificationRulesCsv = process.env.UNIFICATION_RULES_CSV;
    if (!unificationRulesCsv) {
      // 最新のunification_rulesファイルを探す
      const outDir = path.join(process.cwd(), "out");
      const files = fs.readdirSync(outDir)
        .filter((f) => f.startsWith("unification_rules_") && f.endsWith(".csv"))
        .sort()
        .reverse();
      
      if (files.length === 0) {
        console.error("❌ エラー: 統一ルールCSVファイルが見つかりません。");
        console.error("   先に scripts/generate_unification_rules.ts を実行してください。");
        process.exit(1);
      }
      
      unificationRulesCsv = path.join(outDir, files[0]);
    }

    if (!fs.existsSync(unificationRulesCsv)) {
      console.error(`❌ エラー: 統一ルールCSVファイルが見つかりません: ${unificationRulesCsv}`);
      process.exit(1);
    }

    console.log(`統一ルールCSVファイルを読み込み中: ${path.basename(unificationRulesCsv)}`);
    const csvContent = fs.readFileSync(unificationRulesCsv, "utf-8");
    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }) as Array<{
      フィールド: string;
      統一前の値: string;
      統一後の値: string;
      理由: string;
      [key: string]: any;
    }>;

    // 統一ルールを作成
    const unificationRules = new Map<string, {
      field: string;
      oldValue: string;
      newValue: string;
      reason: string;
    }>();

    for (const record of records) {
      const field = record.フィールド || record["フィールド"] || "";
      const oldValue = record.統一前の値 || record["統一前の値"] || "";
      const newValue = record.統一後の値 || record["統一後の値"] || "";
      const reason = record.理由 || record["理由"] || "";

      if (!field || !oldValue || !newValue) continue;

      const key = `${field}|${oldValue}`;
      unificationRules.set(key, {
        field,
        oldValue,
        newValue,
        reason,
      });
    }

    console.log(`\n📊 統一ルール数: ${unificationRules.size} 件`);

    // 理由別の集計
    const byReason = new Map<string, number>();
    for (const rule of unificationRules.values()) {
      byReason.set(rule.reason, (byReason.get(rule.reason) || 0) + 1);
    }

    console.log(`\n📈 理由別の内訳:`);
    for (const [reason, count] of Array.from(byReason.entries()).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${reason}: ${count} 件`);
    }

    // 出力ディレクトリを作成
    const outDir = path.join(process.cwd(), "out");
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const logFilePath = path.join(outDir, `unify_industry_all_${timestamp}.log`);
    const updatedLogPath = path.join(outDir, `unify_industry_all_updated_${timestamp}.log`);
    const reportPath = path.join(outDir, `unify_industry_all_report_${timestamp}.csv`);

    const logStream = fs.createWriteStream(logFilePath, { encoding: "utf8", flags: "w" });
    const updatedLogStream = fs.createWriteStream(updatedLogPath, { encoding: "utf8", flags: "w" });
    const reportStream = fs.createWriteStream(reportPath, { encoding: "utf8", flags: "w" });

    logStream.write(`# 業種統一処理ログ（全統一ルール適用）\n`);
    logStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    logStream.write(`# DRY_RUN: ${dryRun}\n`);
    logStream.write(`# 統一ルール数: ${unificationRules.size} 件\n`);
    logStream.write(`#\n`);

    updatedLogStream.write(`# 更新されたドキュメント一覧\n`);
    updatedLogStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    updatedLogStream.write(`# フォーマット: docId,corporateNumber,name,field,oldValue,newValue,reason\n`);
    updatedLogStream.write(`#\n`);

    reportStream.write("docId,corporateNumber,name,field,oldValue,newValue,reason\n");

    // 並列処理の設定
    const PARALLEL_WORKERS = parseInt(process.env.PARALLEL_WORKERS || "32", 10);
    const QUERY_BATCH_SIZE = 2000; // クエリで取得するバッチサイズ
    const MAX_BATCH_COMMIT_SIZE = 300; // Firestoreバッチコミットの最大サイズ
    const CHUNK_SIZE = 500; // 並列処理するチャンクサイズ

    const fields = ["industryLarge", "industryMiddle", "industrySmall", "industryDetail"];

    console.log(`\n⚡ 高速化設定:`);
    console.log(`  並列ワーカー数: ${PARALLEL_WORKERS}`);
    console.log(`  クエリバッチサイズ: ${QUERY_BATCH_SIZE}`);
    console.log(`  チャンクサイズ: ${CHUNK_SIZE}`);
    console.log(`  最大バッチコミットサイズ: ${MAX_BATCH_COMMIT_SIZE}`);

    // チャンク配列に分割するヘルパー関数
    function chunkArray<T>(array: T[], size: number): T[][] {
      const chunks: T[][] = [];
      for (let i = 0; i < array.length; i += size) {
        chunks.push(array.slice(i, i + size));
      }
      return chunks;
    }

    // リトライ付きクエリ実行
    async function executeQueryWithRetry(
      query: admin.firestore.Query,
      maxRetries: number = 3
    ): Promise<admin.firestore.QuerySnapshot> {
      let lastError: Error | null = null;
      for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
          return await query.get();
        } catch (error: any) {
          lastError = error;
          if (error.code === 14 || error.message?.includes("timeout") || error.message?.includes("UNAVAILABLE")) {
            const delay = Math.pow(2, attempt) * 1000; // 指数バックオフ
            console.log(`  クエリタイムアウト、${delay}ms後にリトライ... (試行 ${attempt + 1}/${maxRetries})`);
            await new Promise((resolve) => setTimeout(resolve, delay));
            continue;
          }
          throw error;
        }
      }
      throw lastError || new Error("クエリ実行失敗");
    }

    // 1つのドキュメントを処理
    function processDocument(
      doc: admin.firestore.QueryDocumentSnapshot,
      unificationRules: Map<string, { field: string; oldValue: string; newValue: string; reason: string }>
    ): {
      docId: string;
      updates: { [key: string]: string };
      reportRows: string[];
      logRows: string[];
    } {
      const data = doc.data();
      const updates: { [key: string]: string } = {};
      const reportRows: string[] = [];
      const logRows: string[] = [];

      // 各フィールドをチェック
      for (const field of fields) {
        const value = data[field];
        if (!value || typeof value !== "string") continue;

        // 統一ルールをチェック
        const key = `${field}|${value}`;
        if (unificationRules.has(key)) {
          const rule = unificationRules.get(key)!;
          if (value !== rule.newValue) {
            updates[field] = rule.newValue;

            // レポート行を生成
            reportRows.push(
              `${doc.id},"${(data.corporateNumber || "").replace(/"/g, '""')}","${(data.name || "").replace(/"/g, '""')}",${field},"${value.replace(/"/g, '""')}","${rule.newValue.replace(/"/g, '""')}","${rule.reason.replace(/"/g, '""')}"\n`
            );

            // ログ行を生成
            logRows.push(
              `${doc.id},"${(data.corporateNumber || "").replace(/"/g, '""')}","${(data.name || "").replace(/"/g, '""')}",${field},"${value.replace(/"/g, '""')}","${rule.newValue.replace(/"/g, '""')}","${rule.reason.replace(/"/g, '""')}"\n`
            );
          }
        }
      }

      return {
        docId: doc.id,
        updates,
        reportRows,
        logRows,
      };
    }

    // チャンクを並列処理
    async function processChunk(
      docs: admin.firestore.QueryDocumentSnapshot[],
      unificationRules: Map<string, { field: string; oldValue: string; newValue: string; reason: string }>,
      dryRun: boolean
    ): Promise<{
      processed: number;
      updated: number;
      reportRows: string[];
      logRows: string[];
      batchUpdates: Array<{ docRef: admin.firestore.DocumentReference; updates: { [key: string]: string } }>;
    }> {
      const reportRows: string[] = [];
      const logRows: string[] = [];
      const batchUpdates: Array<{ docRef: admin.firestore.DocumentReference; updates: { [key: string]: string } }> = [];
      let updated = 0;

      for (const doc of docs) {
        const result = processDocument(doc, unificationRules);
        
        if (result.reportRows.length > 0) {
          reportRows.push(...result.reportRows);
          logRows.push(...result.logRows);
          
          if (!dryRun && Object.keys(result.updates).length > 0) {
            batchUpdates.push({
              docRef: doc.ref,
              updates: result.updates,
            });
            updated++;
          } else if (dryRun) {
            updated++;
          }
        }
      }

      return {
        processed: docs.length,
        updated,
        reportRows,
        logRows,
        batchUpdates,
      };
    }

    console.log("\nドキュメントをスキャン・処理中...");

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;
    let totalUpdated = 0;
    let allBatchUpdates: Array<{ docRef: admin.firestore.DocumentReference; updates: { [key: string]: string } }> = [];

    // ドキュメントを取得してチャンクに分割
    const allDocs: admin.firestore.QueryDocumentSnapshot[] = [];
    
    while (true) {
      let query = db
        .collection("companies_new")
        .orderBy(admin.firestore.FieldPath.documentId())
        .limit(QUERY_BATCH_SIZE);
      
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await executeQueryWithRetry(query);
      if (snapshot.empty) {
        break;
      }

      allDocs.push(...snapshot.docs);
      lastDoc = snapshot.docs[snapshot.docs.length - 1];

      if (allDocs.length % 10000 === 0 && allDocs.length > 0) {
        console.log(`  取得済み: ${allDocs.length} 件`);
      }
    }

    console.log(`\n総ドキュメント数: ${allDocs.length} 件`);
    console.log(`並列処理を開始します...`);

    // チャンクに分割して並列処理
    const chunks = chunkArray(allDocs, CHUNK_SIZE);
    const totalChunks = chunks.length;
    console.log(`チャンク数: ${totalChunks} チャンク`);

    // 並列処理（ワーカー数制限付き）
    const semaphore = new Array(PARALLEL_WORKERS).fill(null);
    let currentChunkIndex = 0;
    let activeWorkers = 0;

    const processNextChunk = async (): Promise<void> => {
      if (currentChunkIndex >= totalChunks) {
        return;
      }

      const chunkIndex = currentChunkIndex++;
      const chunk = chunks[chunkIndex];
      activeWorkers++;

      try {
        const result = await processChunk(chunk, unificationRules, dryRun);
        
        totalProcessed += result.processed;
        totalUpdated += result.updated;
        allBatchUpdates.push(...result.batchUpdates);

        // レポートとログに書き込み
        for (const row of result.reportRows) {
          reportStream.write(row);
        }
        for (const row of result.logRows) {
          updatedLogStream.write(row);
        }

        if ((chunkIndex + 1) % 10 === 0 || chunkIndex === totalChunks - 1) {
          const progress = ((chunkIndex + 1) / totalChunks * 100).toFixed(1);
          console.log(`  進捗: ${chunkIndex + 1}/${totalChunks} チャンク (${progress}%) - 処理済み: ${totalProcessed} 件 / 更新: ${totalUpdated} 件`);
          logStream.write(`# Progress: ${chunkIndex + 1}/${totalChunks} チャンク - 処理済み: ${totalProcessed} 件 / 更新: ${totalUpdated} 件 at ${new Date().toISOString()}\n`);
        }
      } catch (error: any) {
        console.error(`  [エラー] チャンク ${chunkIndex}: ${error.message}`);
        logStream.write(`ERROR: チャンク ${chunkIndex} - ${error.message}\n`);
      } finally {
        activeWorkers--;
        // 次のチャンクを処理
        if (currentChunkIndex < totalChunks) {
          await processNextChunk();
        }
      }
    };

    // 初期ワーカーを起動
    const workerPromises = semaphore.map(() => processNextChunk());
    await Promise.all(workerPromises);

    console.log(`\n✅ ドキュメント処理完了: ${totalProcessed} 件 / 更新対象: ${totalUpdated} 件`);

    // バッチ更新を実行
    if (!dryRun && allBatchUpdates.length > 0) {
      console.log(`\nバッチ更新を実行中... (${allBatchUpdates.length} 件)`);
      
      const updateChunks = chunkArray(allBatchUpdates, MAX_BATCH_COMMIT_SIZE);
      let committedCount = 0;

      for (let i = 0; i < updateChunks.length; i++) {
        const updateChunk = updateChunks[i];
        const batch = db.batch();

        for (const { docRef, updates } of updateChunk) {
          batch.update(docRef, updates);
        }

        try {
          await batch.commit();
          committedCount += updateChunk.length;
          
          if ((i + 1) % 10 === 0 || i === updateChunks.length - 1) {
            const progress = ((i + 1) / updateChunks.length * 100).toFixed(1);
            console.log(`  バッチコミット進捗: ${i + 1}/${updateChunks.length} (${progress}%) - ${committedCount} 件`);
            logStream.write(`# バッチコミット: ${i + 1}/${updateChunks.length} - ${committedCount} 件 at ${new Date().toISOString()}\n`);
            updatedLogStream.write(`# バッチコミット: ${i + 1}/${updateChunks.length} - ${committedCount} 件 at ${new Date().toISOString()}\n`);
          }
        } catch (error: any) {
          console.error(`  [エラー] バッチコミット ${i + 1}: ${error.message}`);
          logStream.write(`ERROR: バッチコミット ${i + 1} - ${error.message}\n`);
          
          // エラーが発生した場合、個別にリトライ
          if (error.message.includes("Transaction too big") || error.message.includes("WriteBatch")) {
            console.log(`  バッチサイズを減らしてリトライ...`);
            // より小さなバッチでリトライ
            const smallerChunks = chunkArray(updateChunk, Math.floor(MAX_BATCH_COMMIT_SIZE / 2));
            for (const smallerChunk of smallerChunks) {
              const retryBatch = db.batch();
              for (const { docRef, updates } of smallerChunk) {
                retryBatch.update(docRef, updates);
              }
              try {
                await retryBatch.commit();
                committedCount += smallerChunk.length;
              } catch (retryError: any) {
                console.error(`  [エラー] リトライバッチコミット失敗: ${retryError.message}`);
                logStream.write(`ERROR: リトライバッチコミット - ${retryError.message}\n`);
              }
            }
          }
        }
      }

      console.log(`\n✅ バッチ更新完了: ${committedCount} 件`);
    }

    logStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    logStream.end();
    updatedLogStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    updatedLogStream.end();
    reportStream.end();

    console.log(`\n✅ 処理完了`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`更新数: ${totalUpdated} 件`);
    console.log(`\n📁 出力ファイル:`);
    console.log(`  - ${reportPath} (更新レポート)`);
    console.log(`  - ${logFilePath} (処理ログ)`);
    console.log(`  - ${updatedLogPath} (更新されたドキュメント一覧)`);

  } catch (error: any) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error("❌ 重大エラー:", errorMsg);
    console.error(error);
    process.exit(1);
  }
}

unifyIndustryAll()
  .then(() => {
    console.log("\n処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
