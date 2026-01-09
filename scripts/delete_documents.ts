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

async function deleteDocuments(docIds: string[]) {
  initAdmin();
  const db = admin.firestore();
  const companiesCol = db.collection(COLLECTION_NAME);
  
  let deleted = 0;
  let errors = 0;
  
  for (const docId of docIds) {
    try {
      await companiesCol.doc(docId).delete();
      console.log(`  ✅ 削除: ${docId}`);
      deleted++;
    } catch (error) {
      console.error(`  ❌ 削除エラー (${docId}): ${(error as Error).message}`);
      errors++;
    }
  }
  
  console.log(`\n📊 削除結果: 成功 ${deleted}件, エラー ${errors}件`);
}

async function main() {
  const filePath = process.argv[2];
  
  if (!filePath) {
    console.error("❌ 使用方法: npx tsx scripts/delete_documents.ts <doc_ids_file>");
    process.exit(1);
  }
  
  const fullPath = path.resolve(process.cwd(), filePath);
  
  if (!fs.existsSync(fullPath)) {
    console.error(`❌ ファイルが見つかりません: ${fullPath}`);
    process.exit(1);
  }
  
  const content = fs.readFileSync(fullPath, "utf8");
  const docIds = content
    .split("\n")
    .map(line => line.trim())
    .filter(line => line.length > 0);
  
  console.log(`📁 削除対象: ${docIds.length}件\n`);
  
  await deleteDocuments(docIds);
  
  console.log("\n✅ 処理完了");
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});

