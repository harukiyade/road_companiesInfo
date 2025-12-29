/* eslint-disable no-console */
import admin from "firebase-admin";
import * as path from "path";

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

async function checkTotalCount() {
  try {
    console.log("companies_newコレクションの総件数を確認中...");
    
    // 全件数を取得（バッチ処理で）
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
      
      totalCount += snapshot.size;
      console.log(`現在のカウント: ${totalCount} 件`);
      
      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }
    
    console.log(`\n✅ companies_newコレクションの総件数: ${totalCount} 件`);
    
    // CSVファイルの行数を確認
    const csvPath = path.join(process.cwd(), "companies_webinfo.csv");
    const fs = require("fs");
    if (fs.existsSync(csvPath)) {
      const content = fs.readFileSync(csvPath, "utf8");
      const lines = content.split("\n").filter((line: string) => line.trim().length > 0);
      const csvRowCount = lines.length - 1; // ヘッダーを除く
      console.log(`✅ CSVファイルの行数（ヘッダー除く）: ${csvRowCount} 件`);
      console.log(`\n進捗: ${csvRowCount} / ${totalCount} (${((csvRowCount / totalCount) * 100).toFixed(2)}%)`);
      
      if (csvRowCount >= totalCount) {
        console.log("\n✅ 全企業の処理が完了しています！");
      } else {
        console.log(`\n⏳ まだ ${totalCount - csvRowCount} 件の企業が処理待ちです。`);
      }
    } else {
      console.log("⚠️  CSVファイルが見つかりません");
    }
    
    process.exit(0);
  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  }
}

checkTotalCount();




