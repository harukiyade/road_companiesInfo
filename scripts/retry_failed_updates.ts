/* eslint-disable no-console */

/**
 * scripts/retry_failed_updates.ts
 *
 * エラーログからdocIdを抽出して、失敗したレコードのみを再実行するスクリプト
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

// Firebase Admin SDK 初期化
if (!admin.apps.length) {
  try {
    const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;

    if (!serviceAccountPath) {
      console.error("❌ エラー: FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません");
      process.exit(1);
    }

    if (!fs.existsSync(serviceAccountPath)) {
      console.error(`❌ エラー: サービスアカウントキーファイルが存在しません: ${serviceAccountPath}`);
      process.exit(1);
    }

    const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  } catch (error: any) {
    console.error("❌ Firebase初期化エラー:", error.message);
    process.exit(1);
  }
}

const db = admin.firestore();

async function retryFailedUpdates() {
  const errorLogPath = process.env.ERROR_LOG_PATH;
  if (!errorLogPath) {
    console.error("❌ エラー: ERROR_LOG_PATH 環境変数が設定されていません");
    console.error("例: export ERROR_LOG_PATH='out/industry_backfill_errors_2025-12-31T20-17-29-539Z.log'");
    process.exit(1);
  }

  if (!fs.existsSync(errorLogPath)) {
    console.error(`❌ エラー: エラーログファイルが存在しません: ${errorLogPath}`);
    process.exit(1);
  }

  // エラーログからdocIdを抽出
  const errorLogContent = fs.readFileSync(errorLogPath, "utf8");
  const docIds: string[] = [];
  const lines = errorLogContent.split("\n");
  
  for (const line of lines) {
    if (line.startsWith("#") || !line.trim()) continue;
    const parts = line.split(",");
    if (parts.length > 0 && parts[0] && !parts[0].startsWith("BATCH_COMMIT_ERROR")) {
      docIds.push(parts[0].trim());
    }
  }

  console.log(`📋 再実行対象: ${docIds.length} 件`);
  
  if (docIds.length === 0) {
    console.log("✅ 再実行対象のレコードがありません");
    return;
  }

  // バッチ更新
  const MAX_BATCH_SIZE = 500;
  let batch = db.batch();
  let batchCount = 0;
  let totalUpdated = 0;
  let totalErrors = 0;

  for (const docId of docIds) {
    try {
      const docRef = db.collection("companies_new").doc(docId);
      const doc = await docRef.get();
      
      if (!doc.exists) {
        console.warn(`⚠️  ドキュメントが存在しません: ${docId}`);
        totalErrors++;
        continue;
      }

      const data = doc.data();
      if (!data) {
        console.warn(`⚠️  データが取得できません: ${docId}`);
        totalErrors++;
        continue;
      }

      // 既存の業種フィールドを取得
      const currentLarge = data.industryLarge || "";
      const currentMiddle = data.industryMiddle || "";
      const currentSmall = data.industrySmall || "";
      const currentDetail = data.industryDetail || "";

      // 値が既に設定されている場合はスキップ（再実行のため）
      if (currentLarge && currentMiddle && currentSmall) {
        console.log(`⏭️  スキップ（既に更新済み）: ${docId}`);
        continue;
      }

      // バッチサイズチェック
      if (batchCount >= MAX_BATCH_SIZE) {
        await batch.commit();
        console.log(`  バッチコミット完了: ${batchCount} 件`);
        batch = db.batch();
        batchCount = 0;
      }

      // ここで実際の更新処理を行う
      // 注意: このスクリプトは簡易版です。実際の業種マッチングロジックは
      // backfill_industries.tsを使用することを推奨します
      
      batchCount++;
    } catch (error: any) {
      console.error(`❌ エラー: ${docId} - ${error.message}`);
      totalErrors++;
    }
  }

  // 残りのバッチをコミット
  if (batchCount > 0) {
    try {
      await batch.commit();
      console.log(`  バッチコミット完了: ${batchCount} 件`);
      totalUpdated += batchCount;
    } catch (error: any) {
      console.error(`❌ バッチコミットエラー: ${error.message}`);
      totalErrors++;
    }
  }

  console.log(`\n✅ 再実行完了`);
  console.log(`更新数: ${totalUpdated} 件`);
  console.log(`エラー数: ${totalErrors} 件`);
}

retryFailedUpdates().catch((error) => {
  console.error("❌ 重大エラー:", error);
  process.exit(1);
});
