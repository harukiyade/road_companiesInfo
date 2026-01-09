import "dotenv/config";
import admin from "firebase-admin";
import fs from "fs";
import path from "path";

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
  initAdmin();
  const db = admin.firestore();
  const companiesCol = db.collection(COLLECTION_NAME);
  
  // 削除するドキュメントIDのリスト
  const docIds = [
    "17655532220122936",
    "17655532223309711",
    "17655532224040695",
    "17655532225065580",
    "17655532225858358",
  ];
  
  console.log(`🗑️  削除対象: ${docIds.length} 件`);
  
  let deleted = 0;
  let notFound = 0;
  
  for (const docId of docIds) {
    try {
      const docRef = companiesCol.doc(docId);
      const doc = await docRef.get();
      
      if (doc.exists) {
        await docRef.delete();
        console.log(`  ✅ 削除: ${docId}`);
        deleted++;
      } else {
        console.log(`  ⚠️  見つかりません: ${docId}`);
        notFound++;
      }
    } catch (error) {
      console.error(`  ❌ エラー (${docId}): ${(error as Error).message}`);
    }
  }
  
  console.log(`\n✅ 削除完了`);
  console.log(`  削除: ${deleted} 件`);
  console.log(`  見つからない: ${notFound} 件`);
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
