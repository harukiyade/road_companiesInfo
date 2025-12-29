/*
  companies_newコレクションのドキュメントIDを数値のみに統一するスクリプト
  
  英字を含むIDを数値IDに変換します。
  
  使い方:
    npx ts-node scripts/fix_doc_ids.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
    ];

    for (const pth of defaultPaths) {
      const resolved = path.resolve(pth);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        break;
      }
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: serviceAccount.project_id,
  });
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// ==============================
// 数値ID生成
// ==============================

function generateNumericDocId(
  corporateNumber: string | null | undefined,
  index: number,
  existingDocId?: string
): string {
  // corporateNumberが存在し、数字のみの場合 → そのまま使用
  if (
    corporateNumber &&
    typeof corporateNumber === "string" &&
    /^[0-9]+$/.test(corporateNumber.trim())
  ) {
    return corporateNumber.trim();
  }

  // 既存のdocIdが数字のみの場合 → そのまま使用
  if (existingDocId && /^[0-9]+$/.test(existingDocId)) {
    return existingDocId;
  }

  // それ以外の場合 → Date.now() + インデックスから数字のみの一意IDを生成
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");

  let processedCount = 0;
  let idChangedCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 200;
  const FETCH_BATCH_SIZE = 1000;

  const docsToDelete: DocumentReference[] = [];
  const newDocIds = new Set<string>();
  let lastDoc: any = null;
  let globalIndex = 0;

  console.log("📊 非数値IDのドキュメントを検索中...");

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

    for (const doc of snapshot.docs) {
      const oldId = doc.id;
      const data = doc.data();

      // 数値IDでない場合のみ処理
      if (!/^[0-9]+$/.test(oldId)) {
        const corporateNumber = data.corporateNumber || null;
        let newId = generateNumericDocId(corporateNumber, globalIndex, oldId);

        // 新しいIDが既に使用されている場合、一意のIDを生成
        let retryCount = 0;
        while (newDocIds.has(newId) && retryCount < 10) {
          newId = generateNumericDocId(null, globalIndex + retryCount * 10000, undefined);
          retryCount++;
        }
        newDocIds.add(newId);

        idChangedCount++;

        if (!DRY_RUN) {
          // 新しいIDでドキュメントを作成
          const newRef = companiesCol.doc(newId);
          batch.set(newRef, data, { merge: true });
          batchCount++;

          // 古いドキュメントを削除リストに追加
          docsToDelete.push(doc.ref);
        }

        if (idChangedCount <= 20) {
          console.log(`🔄 ID変更: "${oldId}" → "${newId}"`);
        }
      }

      processedCount++;
      globalIndex++;

      // バッチコミット
      if (batchCount >= BATCH_LIMIT) {
        if (!DRY_RUN) {
          console.log(`💾 バッチコミット (${batchCount} 件) ...`);
          await batch.commit();
          batch = db.batch();
          batchCount = 0;
        }
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];

    if (processedCount % 10000 === 0) {
      console.log(`  📊 処理済み: ${processedCount} 件 (ID変更: ${idChangedCount} 件)`);
    }
  }

  // 最後のバッチをコミット
  if (!DRY_RUN && batchCount > 0) {
    console.log(`💾 最後のバッチコミット (${batchCount} 件) ...`);
    await batch.commit();
  }

  // 古いドキュメントを削除
  if (!DRY_RUN && docsToDelete.length > 0) {
    console.log(`\n🗑️  古いドキュメントを削除中 (${docsToDelete.length} 件)...`);
    const DELETE_BATCH_LIMIT = 200;
    for (let i = 0; i < docsToDelete.length; i += DELETE_BATCH_LIMIT) {
      const batchToDelete = docsToDelete.slice(i, i + DELETE_BATCH_LIMIT);
      const deleteBatch = db.batch();
      for (const ref of batchToDelete) {
        deleteBatch.delete(ref);
      }
      await deleteBatch.commit();
      console.log(`  💾 削除バッチコミット (${batchToDelete.length} 件) ...`);
    }
  }

  console.log("\n✅ ID変換完了");
  console.log(`  📊 処理件数: ${processedCount} 件`);
  console.log(`  🔄 ID変更: ${idChangedCount} 件`);

  if (DRY_RUN) {
    console.log("\n💡 実際にIDを変換するには、--dry-run フラグを外してください");
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

