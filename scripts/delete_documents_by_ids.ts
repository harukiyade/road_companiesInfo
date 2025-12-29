/* 
  ドキュメントIDのリストファイルを読み込んでFirestoreから削除するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/delete_documents_by_ids.ts <doc_ids_file.txt>
*/

import "dotenv/config";
import fs from "fs";
import path from "path";
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
    console.error("❌ エラー: ドキュメントIDファイルのパスを指定してください");
    console.error("使い方: npx tsx scripts/delete_documents_by_ids.ts <doc_ids_file.txt>");
    process.exit(1);
  }
  
  const fullPath = path.resolve(process.cwd(), docIdsFile);
  
  if (!fs.existsSync(fullPath)) {
    console.error(`❌ エラー: ファイルが見つかりません: ${fullPath}`);
    process.exit(1);
  }
  
  const docIds = fs.readFileSync(fullPath, "utf8")
    .split("\n")
    .map(line => line.trim())
    .filter(line => line.length > 0);
  
  console.log(`📄 読み込んだドキュメントID数: ${docIds.length}`);
  
  if (docIds.length === 0) {
    console.log("⚠️  削除するドキュメントがありません");
    return;
  }
  
  initAdmin();
  const db = admin.firestore();
  const companiesCol = db.collection(COLLECTION_NAME);
  
  const BATCH_SIZE = 500; // Firestoreのバッチ制限
  let deleted = 0;
  let notFound = 0;
  
  for (let i = 0; i < docIds.length; i += BATCH_SIZE) {
    const batch = db.batch();
    const batchIds = docIds.slice(i, i + BATCH_SIZE);
    let batchCount = 0;
    
    for (const docId of batchIds) {
      const docRef = companiesCol.doc(docId);
      const doc = await docRef.get();
      
      if (doc.exists) {
        batch.delete(docRef);
        batchCount++;
      } else {
        notFound++;
        console.log(`  ⚠️  ドキュメントが見つかりません: ${docId}`);
      }
    }
    
    if (batchCount > 0) {
      await batch.commit();
      deleted += batchCount;
      console.log(`  ✅ バッチ削除完了: ${batchCount} 件 (合計: ${deleted} 件)`);
    }
  }
  
  console.log("\n" + "=".repeat(60));
  console.log("📊 削除結果サマリー");
  console.log("=".repeat(60));
  console.log(`削除: ${deleted} 件`);
  console.log(`見つからなかった: ${notFound} 件`);
  console.log(`合計: ${docIds.length} 件`);
  console.log("\n✅ 処理完了");
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});












