/* 
  テスト用ドキュメント削除スクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/delete_test_docs.ts <doc_ids_file>
*/

import "dotenv/config";
import fs from "fs";
import admin from "firebase-admin";

// Firebase初期化
function initAdmin() {
  if (admin.apps.length) return;
  try {
    admin.initializeApp({
      credential: admin.credential.applicationDefault(),
    });
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", (error as Error).message);
    throw error;
  }
}

const COLLECTION_NAME = "companies_new";

async function main() {
  const docIdsFile = process.argv[2];
  
  if (!docIdsFile) {
    console.error("❌ ドキュメントIDファイルを指定してください");
    console.error("使い方: npx tsx scripts/delete_test_docs.ts <doc_ids_file>");
    process.exit(1);
  }
  
  if (!fs.existsSync(docIdsFile)) {
    console.error(`❌ ファイルが見つかりません: ${docIdsFile}`);
    process.exit(1);
  }
  
  const docIds = fs.readFileSync(docIdsFile, "utf8")
    .split("\n")
    .map(id => id.trim())
    .filter(id => id.length > 0);
  
  if (docIds.length === 0) {
    console.log("⚠️  削除するドキュメントIDがありません");
    return;
  }
  
  console.log(`📋 削除対象: ${docIds.length} 件`);
  
  initAdmin();
  const db = admin.firestore();
  const companiesCol = db.collection(COLLECTION_NAME);
  
  let deleted = 0;
  let notFound = 0;
  let errors = 0;
  
  // バッチ削除（Firestoreの制限は500件まで）
  const BATCH_SIZE = 400;
  
  for (let i = 0; i < docIds.length; i += BATCH_SIZE) {
    const batch = db.batch();
    const batchIds = docIds.slice(i, i + BATCH_SIZE);
    let batchCount = 0;
    
    for (const docId of batchIds) {
      try {
        const docRef = companiesCol.doc(docId);
        batch.delete(docRef);
        batchCount++;
      } catch (error) {
        console.error(`  ❌ エラー (ID: ${docId}):`, (error as Error).message);
        errors++;
      }
    }
    
    if (batchCount > 0) {
      try {
        await batch.commit();
        deleted += batchCount;
        console.log(`  ✅ 削除完了: ${batchCount} 件 (合計: ${deleted} 件)`);
      } catch (error) {
        console.error(`  ❌ バッチコミットエラー:`, (error as Error).message);
        errors += batchCount;
      }
    }
  }
  
  console.log("\n" + "=".repeat(60));
  console.log("📊 削除結果サマリー");
  console.log("=".repeat(60));
  console.log(`  削除: ${deleted} 件`);
  console.log(`  エラー: ${errors} 件`);
  console.log("\n✅ 処理完了");
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});












