/**
 * 以下の2つの処理を実行:
 * 1. listing="上場"のドキュメントでtransactionTypeが「譲受企業」でないものを更新
 * 2. csv/yuzuriからインポートした企業（最近作成されたドキュメント）にtransactionType="譲受企業"を設定
 * 
 * 使い方:
 *   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
 *   npx ts-node scripts/update_transaction_type_for_listed_and_yuzuri.ts
 */

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// Firebase 初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
    ];
    for (const p of defaultPaths) {
      const resolved = path.resolve(p);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolved}`);
        break;
      }
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ サービスアカウント JSON のパスを指定してください");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId =
    serviceAccount.project_id ||
    process.env.GCLOUD_PROJECT ||
    process.env.GCP_PROJECT;

  if (!projectId) {
    console.error("❌ Project ID が取得できませんでした");
    process.exit(1);
  }

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });
  console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

async function main() {
  console.log("🔄 transactionTypeフィールドを更新します\n");

  // csv/yuzuriからインポートしたドキュメントのIDリスト（このチャットでインポートしたもの）
  // 数値IDで1766735で始まるものは今回インポートしたものと推測
  const YUZURI_IMPORT_TIMESTAMP_START = 1766735000000000000; // 2024年12月26日頃
  const YUZURI_IMPORT_TIMESTAMP_END = 1766737000000000000; // 少し余裕を持たせる

  let listedUpdated = 0;
  let yuzuriUpdated = 0;
  const BATCH_LIMIT = 500;
  const CONCURRENT_BATCHES = 10;
  const pendingBatches: Array<{ promise: Promise<void>; id: number }> = [];
  let batchIdCounter = 0;

  // 1. listing="上場"でtransactionTypeが「譲受企業」でないものを更新
  console.log("📋 処理1: listing='上場'のドキュメントをチェック中...");
  
  const listedQuery = await companiesCol
    .where("listing", "==", "上場")
    .get();

  console.log(`   見つかったドキュメント数: ${listedQuery.size} 件`);

  const updatesToProcess: Array<{ ref: any; name: string; oldType: string }> = [];

  for (const doc of listedQuery.docs) {
    const data = doc.data();
    const transactionType = data.transactionType;

    if (transactionType !== "譲受企業") {
      const name = data.name || data.companyName || doc.id;
      updatesToProcess.push({
        ref: doc.ref,
        name: name,
        oldType: transactionType || "null",
      });
    }
  }

  console.log(`   更新対象: ${updatesToProcess.length} 件`);

  // バッチ更新を並列実行
  for (let i = 0; i < updatesToProcess.length; i += BATCH_LIMIT) {
    const batch = db.batch();
    const batchUpdates = updatesToProcess.slice(i, i + BATCH_LIMIT);
    
    for (const update of batchUpdates) {
      batch.update(update.ref, {
        transactionType: "譲受企業",
      });
    }

    // 並列実行数の制限をチェック
    while (pendingBatches.length >= CONCURRENT_BATCHES) {
      // 最も古いバッチの完了を待つ
      const completed = await Promise.race(
        pendingBatches.map(b => b.promise.then(() => b.id).catch(() => b.id))
      );
      // 完了したバッチを削除
      const index = pendingBatches.findIndex(b => b.id === completed);
      if (index !== -1) {
        pendingBatches.splice(index, 1);
      }
    }

    // バッチを並列実行キューに追加
    const batchIndex = Math.floor(i / BATCH_LIMIT) + 1;
    const totalBatches = Math.ceil(updatesToProcess.length / BATCH_LIMIT);
    const currentBatchId = ++batchIdCounter;
    const batchPromise = (async () => {
      try {
        await batch.commit();
        if (batchIndex % 10 === 0 || batchIndex === totalBatches) {
          console.log(`  💾 バッチコミット (${batchUpdates.length} 件) - ${batchIndex}/${totalBatches} バッチ完了`);
        }
      } catch (error) {
        console.error(`❌ バッチコミットエラー: ${error}`);
        throw error;
      }
    })();

    pendingBatches.push({ promise: batchPromise, id: currentBatchId });
    listedUpdated += batchUpdates.length;

    if (i < 5 * BATCH_LIMIT) {
      // 最初の5バッチの最初の数件をログ出力
      for (let j = 0; j < Math.min(5, batchUpdates.length); j++) {
        console.log(`  ✅ 更新: ${batchUpdates[j].name} - transactionType: ${batchUpdates[j].oldType} → 譲受企業`);
      }
    }
  }

  // 残りのバッチの完了を待つ
  if (pendingBatches.length > 0) {
    console.log(`⏳ 残りのバッチの完了を待機中... (${pendingBatches.length} バッチ)`);
    await Promise.all(pendingBatches.map(b => b.promise));
    pendingBatches.length = 0;
  }

  console.log(`\n✅ 処理1完了: ${listedUpdated} 件更新`);

  // 2. csv/yuzuriからインポートした企業を特定して更新
  console.log("\n📋 処理2: csv/yuzuriからインポートした企業をチェック中...");
  
  // 数値IDで特定（1766735で始まるもの）
  // 全ドキュメントをスキャンして、最近作成された数値IDのドキュメントを特定
  const FETCH_BATCH_SIZE = 1000;
  let lastDoc: any = null;
  let totalFetched = 0;

  const yuzuriUpdatesToProcess: Array<{ ref: any; name: string; oldType: string; docId: string }> = [];

  while (true) {
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(FETCH_BATCH_SIZE);
    
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();

    if (snapshot.empty) {
      break;
    }

    totalFetched += snapshot.size;
    if (totalFetched <= FETCH_BATCH_SIZE || totalFetched % 5000 === 0) {
      console.log(`   取得中... (${totalFetched} 件)`);
    }

    for (const doc of snapshot.docs) {
      const docId = doc.id;
      const data = doc.data();
      const transactionType = data.transactionType;

      // 数値IDで、今回インポートした範囲内かチェック
      if (/^[0-9]+$/.test(docId)) {
        const docIdNum = BigInt(docId);
        if (
          docIdNum >= BigInt(YUZURI_IMPORT_TIMESTAMP_START) &&
          docIdNum <= BigInt(YUZURI_IMPORT_TIMESTAMP_END)
        ) {
          // csv/yuzuriからインポートしたドキュメント
          if (transactionType !== "譲受企業") {
            const name = data.name || data.companyName || doc.id;
            yuzuriUpdatesToProcess.push({
              ref: doc.ref,
              name: name,
              oldType: transactionType || "null",
              docId: docId,
            });
          }
        }
      }

      lastDoc = doc;
    }
  }

  console.log(`   更新対象: ${yuzuriUpdatesToProcess.length} 件`);

  // バッチ更新を並列実行
  for (let i = 0; i < yuzuriUpdatesToProcess.length; i += BATCH_LIMIT) {
    const batch = db.batch();
    const batchUpdates = yuzuriUpdatesToProcess.slice(i, i + BATCH_LIMIT);
    
    for (const update of batchUpdates) {
      batch.update(update.ref, {
        transactionType: "譲受企業",
      });
    }

    // 並列実行数の制限をチェック
    while (pendingBatches.length >= CONCURRENT_BATCHES) {
      const completed = await Promise.race(
        pendingBatches.map(b => b.promise.then(() => b.id).catch(() => b.id))
      );
      const index = pendingBatches.findIndex(b => b.id === completed);
      if (index !== -1) {
        pendingBatches.splice(index, 1);
      }
    }

    const batchIndex = Math.floor(i / BATCH_LIMIT) + 1;
    const totalBatches = Math.ceil(yuzuriUpdatesToProcess.length / BATCH_LIMIT);
    const currentBatchId = ++batchIdCounter;
    const batchPromise = (async () => {
      try {
        await batch.commit();
        if (batchIndex % 10 === 0 || batchIndex === totalBatches) {
          console.log(`  💾 バッチコミット (${batchUpdates.length} 件) - ${batchIndex}/${totalBatches} バッチ完了`);
        }
      } catch (error) {
        console.error(`❌ バッチコミットエラー: ${error}`);
        throw error;
      }
    })();

    pendingBatches.push({ promise: batchPromise, id: currentBatchId });
    yuzuriUpdated += batchUpdates.length;

    if (i < 5 * BATCH_LIMIT) {
      for (let j = 0; j < Math.min(5, batchUpdates.length); j++) {
        console.log(`  ✅ 更新: ${batchUpdates[j].name} (${batchUpdates[j].docId}) - transactionType: ${batchUpdates[j].oldType} → 譲受企業`);
      }
    }
  }

  // 残りのバッチの完了を待つ
  if (pendingBatches.length > 0) {
    console.log(`⏳ 残りのバッチの完了を待機中... (${pendingBatches.length} バッチ)`);
    await Promise.all(pendingBatches.map(b => b.promise));
  }

  console.log(`\n✅ 処理2完了: ${yuzuriUpdated} 件更新`);

  console.log("\n" + "=".repeat(80));
  console.log("✅ 処理完了");
  console.log("=".repeat(80));
  console.log(`  listing='上場'で更新: ${listedUpdated} 件`);
  console.log(`  csv/yuzuriからインポートした企業で更新: ${yuzuriUpdated} 件`);
  console.log(`  合計更新: ${listedUpdated + yuzuriUpdated} 件`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

