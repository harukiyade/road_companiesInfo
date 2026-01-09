/* eslint-disable no-console */

/**
 * scripts/unify_industry_remove_corporation_types.ts
 *
 * ✅ 目的
 * - 法人種別が業種として入っているものを「その他」に統一
 * - industryLarge, industryMiddle, industrySmall, industryDetailフィールドを更新
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 * - DRY_RUN=1 (任意: 1の場合はFirestoreを更新せずレポートのみ出力)
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
// 法人種別のリスト（業種として不適切なもの）
// ------------------------------

const CORPORATION_TYPES = [
  // NPO・NGO関連
  "NPO",
  "NGO",
  "NPO法人",
  "NGO法人",
  "NPO・NGO",
  "NPO・NGO・公益法人",
  "公益法人",
  "一般社団法人",
  "一般財団法人",
  "公益社団法人",
  "公益財団法人",
  "特定非営利活動法人",
  
  // その他の法人種別
  "株式会社",
  "有限会社",
  "合資会社",
  "合名会社",
  "合同会社",
  "医療法人",
  "学校法人",
  "宗教法人",
  "社会福祉法人",
  "協同組合",
  "農業協同組合",
  "生活協同組合",
  "信用組合",
  "信用金庫",
  "相互会社",
  "特殊会社",
  "独立行政法人",
  "地方独立行政法人",
  "認可法人",
  "財団法人",
  "社団法人",
];

/**
 * 値が法人種別かどうかを判定
 */
function isCorporationType(value: string): boolean {
  if (!value || typeof value !== "string") {
    return false;
  }

  const normalizedValue = value.trim();
  
  // 完全一致
  if (CORPORATION_TYPES.some((type) => normalizedValue === type)) {
    return true;
  }

  // 部分一致（法人種別を含む）
  for (const type of CORPORATION_TYPES) {
    if (normalizedValue.includes(type)) {
      return true;
    }
  }

  // 「法人」で終わる場合（一部例外を除く）
  if (normalizedValue.endsWith("法人") && normalizedValue.length <= 10) {
    // 業種として適切なものは除外
    const validIndustryWith法人 = [
      "医療法人",
      "学校法人",
      "宗教法人",
      "社会福祉法人",
    ];
    
    if (!validIndustryWith法人.includes(normalizedValue)) {
      // 「法人」で終わり、短い場合は法人種別の可能性が高い
      return true;
    }
  }

  return false;
}

// ------------------------------
// メイン処理
// ------------------------------

async function unifyIndustryRemoveCorporationTypes() {
  try {
    const dryRun = process.env.DRY_RUN === "1";

    console.log("業種フィールドから法人種別を除去して「その他」に統一開始...");
    if (dryRun) {
      console.log("⚠️  DRY_RUNモード: Firestoreは更新しません");
    }

    // 出力ディレクトリを作成
    const outDir = path.join(process.cwd(), "out");
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const logFilePath = path.join(outDir, `unify_industry_remove_corp_types_${timestamp}.log`);
    const updatedLogPath = path.join(outDir, `unify_industry_remove_corp_types_updated_${timestamp}.log`);
    const reportPath = path.join(outDir, `unify_industry_remove_corp_types_report_${timestamp}.csv`);

    const logStream = fs.createWriteStream(logFilePath, { encoding: "utf8", flags: "w" });
    const updatedLogStream = fs.createWriteStream(updatedLogPath, { encoding: "utf8", flags: "w" });
    const reportStream = fs.createWriteStream(reportPath, { encoding: "utf8", flags: "w" });

    logStream.write(`# 業種フィールド統一処理ログ（法人種別除去）\n`);
    logStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    logStream.write(`# DRY_RUN: ${dryRun}\n`);
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

          // 法人種別かどうかを判定
          if (isCorporationType(value)) {
            // 「その他」に統一
            updates[field] = "その他";
            hasUpdates = true;

            // レポートに記録
            reportStream.write(
              `${doc.id},"${(data.corporateNumber || "").replace(/"/g, '""')}","${(data.name || "").replace(/"/g, '""')}",${field},"${value.replace(/"/g, '""')}","その他"\n`
            );
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

unifyIndustryRemoveCorporationTypes()
  .then(() => {
    console.log("\n処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
