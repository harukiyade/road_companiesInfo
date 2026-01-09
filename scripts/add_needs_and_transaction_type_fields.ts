/**
 * companies_new の全ドキュメントに以下を追加するスクリプト:
 * 1. needs フィールド（null で初期化）
 * 2. transactionType フィールド（取引種別: 譲受企業・譲渡企業・契約済み・なし）
 *    - listing="上場" のドキュメントは transactionType="譲受企業" に設定
 *    - それ以外は transactionType=null に設定
 *
 * 使い方:
 *   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
 *   npx ts-node scripts/add_needs_and_transaction_type_fields.ts [--dry-run]
 */

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  WriteBatch,
  DocumentSnapshot,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// ドライランモード（--dry-run フラグで有効化）
const DRY_RUN = process.argv.includes("--dry-run");

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
    console.error("   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json");
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
const col: CollectionReference = db.collection(COLLECTION_NAME);

async function main() {
  if (DRY_RUN) {
    console.log("🔍 ドライランモード: 実際の更新は行いません\n");
  }

  console.log("🔎 companies_new 全件をスキャンします…");
  
  // ページングで全件取得
  const FETCH_BATCH_SIZE = 1000;
  let lastDoc: DocumentSnapshot | null = null;
  let totalFetched = 0;
  let totalUpdated = 0;
  let needsAdded = 0;
  let transactionTypeAdded = 0;
  let transactionTypeSetToAcquirer = 0; // listing="上場"で「譲受企業」に設定した件数

  // 高速化のための設定
  const BATCH_LIMIT = 500; // Firestoreの上限（400→500に増加）
  const CONCURRENT_BATCHES = 10; // 並列実行するバッチ数
  const pendingBatches: Array<{ promise: Promise<void>; id: number }> = []; // 実行中のバッチを保持
  let batchIdCounter = 0;

  while (true) {
    let query = col.orderBy(admin.firestore.FieldPath.documentId()).limit(FETCH_BATCH_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();

    if (snapshot.empty) {
      break;
    }

    totalFetched += snapshot.size;
    if (totalFetched <= FETCH_BATCH_SIZE || totalFetched % 5000 === 0) {
      console.log(`📦 取得中... (${totalFetched} 件)`);
    }

    // 更新が必要なドキュメントを収集
    const updates: Array<{ ref: any; data: Record<string, any> }> = [];

    for (const doc of snapshot.docs) {
      const data = doc.data() || {};
      const updateData: Record<string, any> = {};
      let needsUpdate = false;

      // 1. needs フィールドの追加（存在しない場合）
      if (!("needs" in data)) {
        updateData.needs = null;
        needsUpdate = true;
        needsAdded++;
      }

      // 2. transactionType フィールドの追加・更新
      const listing = data.listing;
      const isListed = listing === "上場";

      if (!("transactionType" in data)) {
        // フィールドが存在しない場合
        if (isListed) {
          updateData.transactionType = "譲受企業";
          transactionTypeSetToAcquirer++;
        } else {
          updateData.transactionType = null;
        }
        needsUpdate = true;
        transactionTypeAdded++;
      } else if (isListed && data.transactionType !== "譲受企業") {
        // listing="上場" だが transactionType が「譲受企業」でない場合
        updateData.transactionType = "譲受企業";
        needsUpdate = true;
        transactionTypeSetToAcquirer++;
      }

      if (needsUpdate) {
        if (DRY_RUN) {
          const name = data.name || data.companyName || doc.id;
          console.log(`  [DRY-RUN] ${doc.id}: ${name}`);
          if (updateData.needs !== undefined) {
            console.log(`    needs: ${updateData.needs === null ? "null (追加)" : updateData.needs}`);
          }
          if (updateData.transactionType !== undefined) {
            console.log(`    transactionType: ${updateData.transactionType === null ? "null (追加)" : updateData.transactionType}`);
          }
        } else {
          updates.push({ ref: doc.ref, data: updateData });
          totalUpdated++;
        }
      }

      lastDoc = doc;
    }

    // バッチ更新を並列実行
    if (!DRY_RUN && updates.length > 0) {
      for (let i = 0; i < updates.length; i += BATCH_LIMIT) {
        const batch = db.batch();
        const batchUpdates = updates.slice(i, i + BATCH_LIMIT);
        
        for (const update of batchUpdates) {
          batch.update(update.ref, update.data);
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
        const totalBatches = Math.ceil(updates.length / BATCH_LIMIT);
        const currentBatchId = ++batchIdCounter;
        const batchPromise = (async () => {
          try {
            await batch.commit();
            if (batchIndex % 10 === 0 || batchIndex === totalBatches) {
              console.log(`💾 バッチコミット (${batchUpdates.length} 件) - ${batchIndex}/${totalBatches} バッチ完了`);
            }
          } catch (error) {
            console.error(`❌ バッチコミットエラー: ${error}`);
            throw error;
          }
        })();

        pendingBatches.push({ promise: batchPromise, id: currentBatchId });
      }
    }
  }

  // 残りのバッチの完了を待つ
  if (!DRY_RUN && pendingBatches.length > 0) {
    console.log(`⏳ 残りのバッチの完了を待機中... (${pendingBatches.length} バッチ)`);
    await Promise.all(pendingBatches.map(b => b.promise));
  }

  console.log("\n✅ 処理完了");
  console.log(`  取得したドキュメント数: ${totalFetched} 件`);
  if (DRY_RUN) {
    console.log(`  [DRY-RUN] 更新対象: ${totalUpdated} 件`);
  } else {
    console.log(`  更新したドキュメント数: ${totalUpdated} 件`);
  }
  console.log(`  needs フィールドを追加: ${needsAdded} 件`);
  console.log(`  transactionType フィールドを追加: ${transactionTypeAdded} 件`);
  console.log(`  listing="上場" で transactionType="譲受企業" に設定: ${transactionTypeSetToAcquirer} 件`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

