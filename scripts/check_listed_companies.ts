/* eslint-disable no-console */
import admin from "firebase-admin";

// Firebase初期化
const serviceAccountKeyPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
if (!serviceAccountKeyPath) {
  console.error("❌ FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません");
  process.exit(1);
}

try {
  const serviceAccount = require(serviceAccountKeyPath);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
  console.log("✅ Firebase初期化完了");
} catch (error: any) {
  console.error("❌ Firebase初期化エラー:", error.message);
  process.exit(1);
}

const db = admin.firestore();

async function checkListedCompanies() {
  try {
    console.log("companies_newコレクションでlisting='上場'のドキュメント数を確認中...");
    
    // まずwhereクエリで試してみる
    try {
      const querySnapshot = await db
        .collection("companies_new")
        .where("listing", "==", "上場")
        .get();
      
      console.log(`\n✅ listing='上場'のドキュメント数: ${querySnapshot.size} 件`);
      process.exit(0);
    } catch (queryError: any) {
      // インデックスが設定されていない場合は、全件取得してフィルタリング
      if (queryError.code === 9 || queryError.message?.includes("index")) {
        console.log("⚠️  インデックスが設定されていないため、全件取得してフィルタリングします...");
        
        let listedCount = 0;
        let totalCount = 0;
        let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
        const BATCH_SIZE = 5000;
        
        while (true) {
          let query = db.collection("companies_new").limit(BATCH_SIZE);
          if (lastDoc) {
            query = query.startAfter(lastDoc);
          }
          
          const snapshot = await query.get();
          if (snapshot.empty) {
            break;
          }
          
          for (const doc of snapshot.docs) {
            totalCount++;
            const data = doc.data();
            if (data.listing === "上場") {
              listedCount++;
            }
          }
          
          console.log(`処理中... 総数: ${totalCount} 件, 上場: ${listedCount} 件`);
          
          lastDoc = snapshot.docs[snapshot.docs.length - 1];
        }
        
        console.log(`\n✅ listing='上場'のドキュメント数: ${listedCount} 件`);
        console.log(`   総ドキュメント数: ${totalCount} 件`);
        process.exit(0);
      } else {
        throw queryError;
      }
    }
  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  }
}

checkListedCompanies();
