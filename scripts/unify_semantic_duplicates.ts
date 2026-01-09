/* eslint-disable no-console */

/**
 * scripts/unify_semantic_duplicates.ts
 *
 * ✅ 目的
 * - 意味的に重複している業種を統一
 * - 出現回数の多い方、またはより具体的な表記を優先
 * - DBを更新
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 * - DRY_RUN=1 (任意: 1の場合はFirestoreを更新せずレポートのみ出力)
 * - SEMANTIC_DUPLICATES_CSV=/path/to/semantic_duplicates.csv (任意: デフォルトは最新のファイル)
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

interface UnificationRule {
  field: string;
  oldValue: string;
  newValue: string;
  similarity: number;
}

// ------------------------------
// メイン処理
// ------------------------------

async function unifySemanticDuplicates() {
  try {
    const dryRun = process.env.DRY_RUN === "1";
    
    // 意味的重複CSVファイルを取得
    let semanticDuplicatesCsv = process.env.SEMANTIC_DUPLICATES_CSV;
    if (!semanticDuplicatesCsv) {
      // 最新のsemantic_duplicatesファイルを探す
      const outDir = path.join(process.cwd(), "out");
      const files = fs.readdirSync(outDir)
        .filter((f) => f.startsWith("semantic_duplicates_") && f.endsWith(".csv"))
        .sort()
        .reverse();
      
      if (files.length === 0) {
        console.error("❌ エラー: 意味的重複分析CSVファイルが見つかりません。");
        console.error("   先に scripts/analyze_semantic_duplicates.ts を実行してください。");
        process.exit(1);
      }
      
      semanticDuplicatesCsv = path.join(outDir, files[0]);
    }

    if (!fs.existsSync(semanticDuplicatesCsv)) {
      console.error(`❌ エラー: 意味的重複分析CSVファイルが見つかりません: ${semanticDuplicatesCsv}`);
      process.exit(1);
    }

    console.log(`意味的重複分析CSVファイルを読み込み中: ${semanticDuplicatesCsv}`);
    const csvContent = fs.readFileSync(semanticDuplicatesCsv, "utf-8");
    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }) as Array<{
      フィールド: string;
      値1: string;
      値2: string;
      類似度: string;
      値1の出現回数: string;
      値2の出現回数: string;
      推奨統一値: string;
    }>;

    // 統一ルールを作成（類似度が0.7以上、かつ出現回数の差が大きいもののみ）
    const MIN_SIMILARITY = 0.7; // 類似度の閾値
    const unificationRules = new Map<string, string>(); // oldValue -> newValue

    for (const record of records) {
      const similarity = parseFloat(record.類似度);
      const count1 = parseInt(record.値1の出現回数, 10);
      const count2 = parseInt(record.値2の出現回数, 10);
      const recommendedValue = record.推奨統一値;

      // 類似度が閾値以上の場合のみ統一
      if (similarity >= MIN_SIMILARITY) {
        // 値1を統一値に変更
        if (record.値1 !== recommendedValue) {
          const key = `${record.フィールド}|${record.値1}`;
          if (!unificationRules.has(key) || count1 < count2) {
            unificationRules.set(key, recommendedValue);
          }
        }

        // 値2を統一値に変更
        if (record.値2 !== recommendedValue) {
          const key = `${record.フィールド}|${record.値2}`;
          if (!unificationRules.has(key) || count2 < count1) {
            unificationRules.set(key, recommendedValue);
          }
        }
      }
    }

    console.log(`\n📊 統一ルール数: ${unificationRules.size} 件`);
    console.log(`   類似度閾値: ${MIN_SIMILARITY}以上`);

    // 出力ディレクトリを作成
    const outDir = path.join(process.cwd(), "out");
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const logFilePath = path.join(outDir, `unify_semantic_duplicates_${timestamp}.log`);
    const updatedLogPath = path.join(outDir, `unify_semantic_duplicates_updated_${timestamp}.log`);
    const reportPath = path.join(outDir, `unify_semantic_duplicates_report_${timestamp}.csv`);

    const logStream = fs.createWriteStream(logFilePath, { encoding: "utf8", flags: "w" });
    const updatedLogStream = fs.createWriteStream(updatedLogPath, { encoding: "utf8", flags: "w" });
    const reportStream = fs.createWriteStream(reportPath, { encoding: "utf8", flags: "w" });

    logStream.write(`# 意味的重複統一処理ログ\n`);
    logStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    logStream.write(`# DRY_RUN: ${dryRun}\n`);
    logStream.write(`# 統一ルール数: ${unificationRules.size} 件\n`);
    logStream.write(`#\n`);

    updatedLogStream.write(`# 更新されたドキュメント一覧\n`);
    updatedLogStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    updatedLogStream.write(`# フォーマット: docId,corporateNumber,name,field,oldValue,newValue\n`);
    updatedLogStream.write(`#\n`);

    reportStream.write("docId,corporateNumber,name,field,oldValue,newValue\n");

    const BATCH_SIZE = 1000;
    const MAX_BATCH_COMMIT_SIZE = 300;
    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;
    let totalUpdated = 0;
    let batch = db.batch();
    let batchCount = 0;

    const fields = ["industryLarge", "industryMiddle", "industrySmall", "industryDetail"];

    console.log("ドキュメントをスキャン中...");

    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }

      if (totalProcessed % 10000 === 0 && totalProcessed > 0) {
        console.log(`処理済み: ${totalProcessed} 件 / 更新: ${totalUpdated} 件`);
        logStream.write(`# Progress: 処理済み: ${totalProcessed} 件 / 更新: ${totalUpdated} 件 at ${new Date().toISOString()}\n`);
      }

      for (const doc of snapshot.docs) {
        totalProcessed++;
        const data = doc.data();
        const updates: { [key: string]: string } = {};
        let hasUpdates = false;

        // 各フィールドをチェック
        for (const field of fields) {
          const value = data[field];
          if (!value || typeof value !== "string") continue;

          // 統一ルールをチェック
          const key = `${field}|${value}`;
          if (unificationRules.has(key)) {
            const newValue = unificationRules.get(key)!;
            if (value !== newValue) {
              updates[field] = newValue;
              hasUpdates = true;

              // レポートに記録
              reportStream.write(
                `${doc.id},"${(data.corporateNumber || "").replace(/"/g, '""')}","${(data.name || "").replace(/"/g, '""')}",${field},"${value.replace(/"/g, '""')}","${newValue.replace(/"/g, '""')}"\n`
              );
            }
          }
        }

        // 更新が必要な場合
        if (hasUpdates && !dryRun) {
          try {
            if (batchCount >= MAX_BATCH_COMMIT_SIZE) {
              await batch.commit();
              console.log(`  バッチコミット完了: ${batchCount} 件`);
              logStream.write(`# バッチコミット: ${batchCount} 件 at ${new Date().toISOString()}\n`);
              updatedLogStream.write(`# バッチコミット: ${batchCount} 件 at ${new Date().toISOString()}\n`);
              batch = db.batch();
              batchCount = 0;
            }

            batch.update(doc.ref, updates);
            batchCount++;
            totalUpdated++;

            // 更新ログに記録
            for (const [field, newValue] of Object.entries(updates)) {
              const oldValue = data[field] || "";
              updatedLogStream.write(
                `${doc.id},"${(data.corporateNumber || "").replace(/"/g, '""')}","${(data.name || "").replace(/"/g, '""')}",${field},"${oldValue.replace(/"/g, '""')}","${newValue.replace(/"/g, '""')}"\n`
              );
            }
          } catch (error: any) {
            totalUpdated--;
            console.error(`  [エラー] ${doc.id}: ${error.message}`);
            logStream.write(`ERROR: ${doc.id} - ${error.message}\n`);
            
            if (error.message.includes("WriteBatch") || error.message.includes("Transaction too big")) {
              try {
                batch = db.batch();
                batchCount = 0;
              } catch (resetError) {
                // リセットエラーは無視
              }
            }
          }
        } else if (hasUpdates && dryRun) {
          // DRY_RUNモードでも更新数をカウント
          totalUpdated++;
        }
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }

    // 残りのバッチをコミット
    if (batchCount > 0 && !dryRun) {
      try {
        await batch.commit();
        console.log(`  バッチコミット完了: ${batchCount} 件`);
        logStream.write(`# バッチコミット: ${batchCount} 件 at ${new Date().toISOString()}\n`);
        updatedLogStream.write(`# バッチコミット: ${batchCount} 件 at ${new Date().toISOString()}\n`);
      } catch (error: any) {
        console.error(`  [エラー] バッチコミット失敗: ${error.message}`);
        logStream.write(`ERROR: バッチコミット - ${error.message}\n`);
      }
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

unifySemanticDuplicates()
  .then(() => {
    console.log("\n処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
