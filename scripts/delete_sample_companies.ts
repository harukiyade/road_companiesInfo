/* 
  先ほど新規追加したタイプA・B・C・D・Gのサンプルドキュメントを削除
*/

import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// 削除対象のドキュメントID
const SAMPLE_DOC_IDS = {
  "A": "9KRppSYSifIgppXwCN9I",
  "B": "RfbBjCdNQhMkYZBUvPIv",
  "C": "6u81SaY6K53O1QWjEXUS",
  "D": "ZW5QRJK8CbuDbPtqQNTh",
  "G": "Jep7juUztAKFH0uZRH9Q",
};

// Firebase初期化
if (!admin.apps.length) {
  const serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || 
    path.join(__dirname, "../albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
  
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccountPath)
  });
}

const db: Firestore = admin.firestore();

async function main() {
  console.log("================================================================================");
  console.log("サンプルドキュメントの削除");
  console.log("================================================================================");
  console.log();

  let successCount = 0;
  let failCount = 0;

  for (const [type, docId] of Object.entries(SAMPLE_DOC_IDS)) {
    try {
      const docRef = db.collection(COLLECTION_NAME).doc(docId);
      const docSnap = await docRef.get();

      if (docSnap.exists) {
        const data = docSnap.data();
        console.log(`タイプ${type}: ${data?.name} (ID: ${docId})`);
        await docRef.delete();
        console.log(`  ✓ 削除完了`);
        successCount++;
      } else {
        console.log(`タイプ${type}: ドキュメントが見つかりません (ID: ${docId})`);
      }
    } catch (error: any) {
      console.error(`タイプ${type}: 削除エラー - ${error.message}`);
      failCount++;
    }
  }

  console.log();
  console.log("================================================================================");
  console.log(`削除完了: ${successCount}件成功, ${failCount}件失敗`);
  console.log("================================================================================");
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

