/* 
  最近更新されたドキュメントIDを表示するスクリプト
*/

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const serviceAccountPath =
  process.env.GOOGLE_APPLICATION_CREDENTIALS ||
  "/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json";

const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });

async function main() {
  const db = admin.firestore();
  const companiesCol = db.collection("companies_new");

  // 法人番号が設定されているドキュメントをランダムに取得して、更新日時を確認
  console.log("🔍 法人番号が設定されているドキュメントを検索中...");
  
  const results: Array<{ docId: string; corporateNumber: string; name: string; updatedAt: string }> = [];
  
  // 全件スキャンして、法人番号が設定されているドキュメントを取得
  const PAGE_SIZE = 1000;
  let lastDoc: any = null;
  let totalProcessed = 0;
  
  while (results.length < 100) {
    let batchQuery = companiesCol.orderBy("__name__").limit(PAGE_SIZE);
    if (lastDoc) {
      batchQuery = batchQuery.startAfter(lastDoc);
    }
    
    const batchSnapshot = await batchQuery.get();
    
    if (batchSnapshot.empty) break;
    
    for (const doc of batchSnapshot.docs) {
      const data = doc.data();
      const corporateNumber = data.corporateNumber;
      const updatedAt = data.updatedAt;
      
      // 法人番号が設定されているか確認
      if (corporateNumber && corporateNumber !== "" && corporateNumber !== null && /^\d{13}$/.test(corporateNumber)) {
        // 更新日時が最近（24時間以内）のものを優先
        const updatedAtDate = updatedAt ? (updatedAt.toDate ? updatedAt.toDate() : new Date(updatedAt)) : null;
        const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
        
        if (updatedAtDate && updatedAtDate >= oneDayAgo) {
          results.push({
            docId: doc.id,
            corporateNumber: corporateNumber,
            name: data.name || "(社名なし)",
            updatedAt: updatedAtDate.toISOString(),
          });
        }
      }
      
      totalProcessed++;
      if (totalProcessed % 10000 === 0) {
        console.log(`  📊 処理中: ${totalProcessed.toLocaleString()} 件、見つかった: ${results.length.toLocaleString()} 社`);
      }
      
      if (results.length >= 100) break;
    }
    
    if (results.length >= 100) break;
    
    lastDoc = batchSnapshot.docs[batchSnapshot.docs.length - 1];
    
    if (batchSnapshot.size < PAGE_SIZE) break;
  }
  
  // 更新日時でソート
  results.sort((a, b) => new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime());
  
  console.log(`\n📊 最近24時間以内に更新されたドキュメント（法人番号あり）: ${results.length} 件\n`);
  console.log("📋 更新されたドキュメントID（最初の50件）:");
  results.slice(0, 50).forEach((item, index) => {
    console.log(`   ${index + 1}. docId: ${item.docId}, 法人番号: ${item.corporateNumber}, 社名: ${item.name.substring(0, 40)}`);
  });

  process.exit(0);
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
