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

async function checkRelatedCompanies() {
  try {
    console.log("companies_newコレクションでrelatedCompaniesフィールドに値があるドキュメント数を確認中...\n");
    
    let totalCount = 0;
    let hasRelatedCompaniesCount = 0;
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
        const relatedCompanies = data.relatedCompanies;
        
        // relatedCompaniesフィールドが存在し、値があるかチェック
        if (relatedCompanies !== undefined && relatedCompanies !== null) {
          // 配列の場合は要素があるか、オブジェクトの場合はキーがあるかチェック
          if (Array.isArray(relatedCompanies)) {
            if (relatedCompanies.length > 0) {
              hasRelatedCompaniesCount++;
            }
          } else if (typeof relatedCompanies === 'object') {
            if (Object.keys(relatedCompanies).length > 0) {
              hasRelatedCompaniesCount++;
            }
          } else if (relatedCompanies !== '') {
            // 文字列やその他の型の場合、空でなければカウント
            hasRelatedCompaniesCount++;
          }
        }
      }
      
      if (totalCount % 10000 === 0) {
        console.log(`処理中... 総数: ${totalCount.toLocaleString()} 件, relatedCompaniesあり: ${hasRelatedCompaniesCount.toLocaleString()} 件`);
      }
      
      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }
    
    console.log(`\n✅ 統計結果:`);
    console.log(`総ドキュメント数: ${totalCount.toLocaleString()} 件`);
    console.log(`relatedCompaniesフィールドに値があるドキュメント数: ${hasRelatedCompaniesCount.toLocaleString()} 件`);
    console.log(`割合: ${((hasRelatedCompaniesCount / totalCount) * 100).toFixed(2)}%`);
    
    process.exit(0);
  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  }
}

checkRelatedCompanies();
