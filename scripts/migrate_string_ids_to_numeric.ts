/**
 * 文字列IDのドキュメントを数値IDに変更するスクリプト
 * 
 * 使い方:
 *   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
 *   npx ts-node scripts/migrate_string_ids_to_numeric.ts
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

// 変更対象の文字列IDリスト（テストで作成されたもの）
const STRING_IDS_TO_MIGRATE = [
  "C8FIj0rIozWM2rgOs1mb",
  "770yMPVzLUAwQZ1U0UF5",
  "3IUIKhiLbHnCWiL96QQ4",
  "NYf3zdgSz5uXnjRTDUv4",
  "dgTGEY3KdGx8Ei6Ucbtc",
  "GZNVQOTTVTUxs0kjHRhU",
  "mbDE7yE8uyMRRUvBODAp",
  "ffbtu9z29hCL0BUNoyzP",
  "AUUMlYvW0iCTrhoGq7mm",
  "EaR8zCGoxRuMT0jWv0aW",
];

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

// 数値IDを生成
function generateNumericDocId(
  corporateNumber: string | null,
  index: number
): string {
  // corporateNumberが存在し、数字のみの場合 → そのまま使用
  if (corporateNumber && /^[0-9]+$/.test(corporateNumber.trim())) {
    return corporateNumber.trim();
  }
  
  // それ以外の場合 → Date.now() + インデックスから数字のみの一意IDを生成
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

async function main() {
  console.log("🔄 文字列IDのドキュメントを数値IDに変更します\n");
  console.log(`対象ID数: ${STRING_IDS_TO_MIGRATE.length} 件\n`);

  let migratedCount = 0;
  let notFoundCount = 0;
  let skippedCount = 0;
  const migratedIds: Array<{ oldId: string; newId: string; name: string }> = [];

  let batch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 400;

  for (let i = 0; i < STRING_IDS_TO_MIGRATE.length; i++) {
    const oldId = STRING_IDS_TO_MIGRATE[i];
    const oldRef = companiesCol.doc(oldId);

    try {
      const oldDoc = await oldRef.get();

      if (!oldDoc.exists) {
        console.log(`⚠️  [${i + 1}/${STRING_IDS_TO_MIGRATE.length}] ドキュメントが見つかりません: ${oldId}`);
        notFoundCount++;
        continue;
      }

      const data = oldDoc.data();
      if (!data) {
        console.log(`⚠️  [${i + 1}/${STRING_IDS_TO_MIGRATE.length}] データが空です: ${oldId}`);
        skippedCount++;
        continue;
      }

      const name = data.name || data.companyName || oldId;
      const corporateNumber = data.corporateNumber || null;

      // 新しい数値IDを生成
      const newId = generateNumericDocId(corporateNumber, i);
      const newRef = companiesCol.doc(newId);

      // 新しいIDが既に存在するかチェック
      const newDoc = await newRef.get();
      if (newDoc.exists) {
        console.log(`⚠️  [${i + 1}/${STRING_IDS_TO_MIGRATE.length}] 新しいIDが既に存在します: ${oldId} → ${newId}`);
        // 別のIDを生成（タイムスタンプ + より大きなインデックス）
        const timestamp = Date.now();
        const paddedIndex = String(i + 10000).padStart(6, "0");
        const alternativeId = `${timestamp}${paddedIndex}`;
        const altRef = companiesCol.doc(alternativeId);
        const altDoc = await altRef.get();
        if (altDoc.exists) {
          console.log(`  ⚠️  代替IDも存在します。スキップします: ${alternativeId}`);
          skippedCount++;
          continue;
        }
        // 代替IDを使用
        batch.set(altRef, data);
        batch.delete(oldRef);
        migratedIds.push({ oldId, newId: alternativeId, name });
        console.log(`  ✅ 代替IDで移行: ${oldId} → ${alternativeId} (${name})`);
      } else {
        // 新しいIDでドキュメントを作成
        batch.set(newRef, data);
        batch.delete(oldRef);
        migratedIds.push({ oldId, newId, name });
        console.log(`  ✅ 移行: ${oldId} → ${newId} (${name})`);
      }

      batchCount++;
      migratedCount++;

      if (batchCount >= BATCH_LIMIT) {
        console.log(`💾 バッチコミット (${batchCount} 件)…`);
        await batch.commit();
        batch = db.batch();
        batchCount = 0;
      }
    } catch (error: any) {
      console.error(`❌ [${i + 1}/${STRING_IDS_TO_MIGRATE.length}] エラー (${oldId}): ${error.message}`);
      skippedCount++;
    }
  }

  // 残りのバッチをコミット
  if (batchCount > 0) {
    console.log(`💾 最終バッチコミット (${batchCount} 件)…`);
    await batch.commit();
  }

  console.log("\n" + "=".repeat(80));
  console.log("✅ 処理完了");
  console.log("=".repeat(80));
  console.log(`  移行成功: ${migratedCount} 件`);
  console.log(`  見つからなかった: ${notFoundCount} 件`);
  console.log(`  スキップ: ${skippedCount} 件`);

  if (migratedIds.length > 0) {
    console.log("\n📋 移行したドキュメント一覧:");
    for (const item of migratedIds) {
      console.log(`  ${item.oldId} → ${item.newId} (${item.name})`);
    }
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

