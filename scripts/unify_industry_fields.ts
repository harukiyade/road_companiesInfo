/* eslint-disable no-console */

/**
 * scripts/unify_industry_fields.ts
 *
 * ✅ 目的
 * - industryLarge, industryMiddle, industrySmall, industryDetailの各フィールド間で
 *   同じ値が存在する場合、統一する
 * - 統一ルール: より上位のフィールド（Large > Middle > Small > Detail）の値を優先
 * - DBを更新
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 * - DRY_RUN=1 (任意: 1の場合はFirestoreを更新せずレポートのみ出力)
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

interface OverlapRecord {
  value: string;
  normalizedValue: string;
  fields: string[];
  counts: { [field: string]: number };
}

// フィールドの優先順位（上位のフィールドの値を優先）
const FIELD_PRIORITY: { [field: string]: number } = {
  industryLarge: 1,
  industryMiddle: 2,
  industrySmall: 3,
  industryDetail: 4,
};

// ------------------------------
// メイン処理
// ------------------------------

async function unifyIndustryFields() {
  try {
    const dryRun = process.env.DRY_RUN === "1";
    const overlapCsvPath = path.join(process.cwd(), "out", "industry_overlap_analysis_2026-01-05T13-33-59-042Z.csv");

    if (!fs.existsSync(overlapCsvPath)) {
      console.error(`❌ エラー: 重複分析CSVファイルが見つかりません: ${overlapCsvPath}`);
      process.exit(1);
    }

    console.log("重複分析CSVファイルを読み込み中...");
    const csvContent = fs.readFileSync(overlapCsvPath, "utf-8");
    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }) as Array<{
      統一後の値: string;
      正規化値: string;
      出現フィールド: string;
      industryLarge件数: string;
      industryMiddle件数: string;
      industrySmall件数: string;
      industryDetail件数: string;
    }>;

    // 統一マッピングを作成
    // キー: 統一前の値, 値: 統一後の値（より上位のフィールドの値）
    const unificationMap = new Map<string, string>();

    for (const record of records) {
      const value = record.統一後の値;
      const fields = record.出現フィールド.split(" | ");
      
      // より上位のフィールドを特定
      let targetField = fields[0];
      let minPriority = FIELD_PRIORITY[targetField] || 999;

      for (const field of fields) {
        const priority = FIELD_PRIORITY[field] || 999;
        if (priority < minPriority) {
          minPriority = priority;
          targetField = field;
        }
      }

      // 各フィールドの値を統一後の値にマッピング
      // ただし、targetField以外のフィールドの値のみ統一する
      for (const field of fields) {
        if (field !== targetField) {
          // このフィールドの値は、targetFieldの値に統一する
          // ただし、実際の値は同じなので、マッピングは不要
          // ここでは、より上位のフィールドに存在する値で統一することを記録
        }
      }
    }

    // 実際の統一処理: より上位のフィールドの値で統一
    // 例: industryLargeとindustryDetailに同じ値がある場合、industryLargeの値を優先
    const fieldUnificationMap = new Map<string, Map<string, string>>();
    
    for (const record of records) {
      const value = record.統一後の値;
      const fields = record.出現フィールド.split(" | ");
      
      if (fields.length <= 1) continue; // 重複がない場合はスキップ

      // より上位のフィールドを特定
      let targetField = fields[0];
      let minPriority = FIELD_PRIORITY[targetField] || 999;

      for (const field of fields) {
        const priority = FIELD_PRIORITY[field] || 999;
        if (priority < minPriority) {
          minPriority = priority;
          targetField = field;
        }
      }

      // 下位のフィールドの値を、上位のフィールドの値に統一
      for (const field of fields) {
        if (field !== targetField) {
          if (!fieldUnificationMap.has(field)) {
            fieldUnificationMap.set(field, new Map());
          }
          // 下位フィールドの値 → 上位フィールドの値（同じ値だが、階層的に統一）
          fieldUnificationMap.get(field)!.set(value, value);
        }
      }
    }

    console.log(`\n📊 統一対象:`);
    console.log(`  重複レコード数: ${records.length} 件`);

    // 出力ディレクトリを作成
    const outDir = path.join(process.cwd(), "out");
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const logFilePath = path.join(outDir, `unify_industry_fields_${timestamp}.log`);
    const updatedLogPath = path.join(outDir, `unify_industry_fields_updated_${timestamp}.log`);
    const reportPath = path.join(outDir, `unify_industry_fields_report_${timestamp}.csv`);

    const logStream = fs.createWriteStream(logFilePath, { encoding: "utf8", flags: "w" });
    const updatedLogStream = fs.createWriteStream(updatedLogPath, { encoding: "utf8", flags: "w" });
    const reportStream = fs.createWriteStream(reportPath, { encoding: "utf8", flags: "w" });

    logStream.write(`# 業種フィールド統一処理ログ\n`);
    logStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    logStream.write(`# DRY_RUN: ${dryRun}\n`);
    logStream.write(`#\n`);

    updatedLogStream.write(`# 更新されたドキュメント一覧\n`);
    updatedLogStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    updatedLogStream.write(`# フォーマット: docId,corporateNumber,name,field,oldValue,newValue\n`);
    updatedLogStream.write(`#\n`);

    reportStream.write("docId,corporateNumber,name,field,oldValue,newValue,updated\n");

    // 実際には、同じ値が複数のフィールドに存在することは問題ない場合が多い
    // しかし、データの整合性を保つため、より上位のフィールドの値を優先する
    // この処理は、主にデータの整合性チェックとレポート生成が目的

    console.log(`\n⚠️  注意: 同じ値が複数のフィールドに存在することは、階層構造上問題ない場合があります。`);
    console.log(`   このスクリプトは、データの整合性チェックとレポート生成が主な目的です。`);
    console.log(`   実際の統一処理が必要な場合は、個別に判断してください。\n`);

    // 全ドキュメントをスキャンして、統一が必要なケースを検出
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
      }

      for (const doc of snapshot.docs) {
        totalProcessed++;
        const data = doc.data();
        const updates: { [key: string]: string } = {};

        // 各フィールドをチェック
        const fields = ["industryLarge", "industryMiddle", "industrySmall", "industryDetail"];
        
        for (const field of fields) {
          const value = data[field];
          if (!value || typeof value !== "string") continue;

          // 重複レコードをチェック
          for (const record of records) {
            const recordValue = record.統一後の値;
            const recordFields = record.出現フィールド.split(" | ");
            
            if (recordValue === value && recordFields.includes(field)) {
              // より上位のフィールドを特定
              let targetField = recordFields[0];
              let minPriority = FIELD_PRIORITY[targetField] || 999;

              for (const f of recordFields) {
                const priority = FIELD_PRIORITY[f] || 999;
                if (priority < minPriority) {
                  minPriority = priority;
                  targetField = f;
                }
              }

              // 現在のフィールドが下位で、より上位のフィールドに同じ値がある場合
              // 実際には値は同じなので、更新は不要
              // ただし、データの整合性チェックとして記録
              if (field !== targetField && FIELD_PRIORITY[field] > FIELD_PRIORITY[targetField]) {
                // このケースは、下位フィールドに上位フィールドと同じ値が入っている
                // これは問題ないが、レポートに記録
                reportStream.write(
                  `${doc.id},"${(data.corporateNumber || "").replace(/"/g, '""')}","${(data.name || "").replace(/"/g, '""')}",${field},"${value.replace(/"/g, '""')}","${value.replace(/"/g, '""')}",false\n`
                );
              }
            }
          }
        }

        // 更新が必要な場合（実際には値が同じなので、更新は不要）
        if (Object.keys(updates).length > 0 && !dryRun) {
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
          } catch (error: any) {
            console.error(`  [エラー] ${doc.id}: ${error.message}`);
            logStream.write(`ERROR: ${doc.id} - ${error.message}\n`);
          }
        }
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }

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
    console.log(`  - ${reportPath}`);
    console.log(`  - ${logFilePath} (処理ログ)`);
    console.log(`  - ${updatedLogPath} (更新されたドキュメント一覧)`);

  } catch (error: any) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error("❌ 重大エラー:", errorMsg);
    console.error(error);
    process.exit(1);
  }
}

unifyIndustryFields()
  .then(() => {
    console.log("\n処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
