/* 
  追加した10社をcompanies_newコレクションから削除
*/

import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// 削除対象のドキュメントID
const ADDED_COMPANIES: Record<string, string> = {
  "A": "wnPspUkcfFcb3Qz7zjuB",
  "B": "8QYZZEMVp2THCO9wNpEY",
  "C": "o5DoyvVwxfnI227rg52Y",
  "D": "hCbGuFYwMzyZlwCrfj1T",
  "E": "mFu0zOpOk63POUirjGIs",
  "F": "KmgKFCRYgBHAO4aBEnyu",
  "G": "yAdIfuyx3OmCkqGWjOIs",
  "H": "GGlcAaYbxBJYfRvK1HhN",
  "I": "YJ8wLD9dIbkqXSR5VMxm",
  "J": "QtAp1FMaDaFZYEMLPcuj",
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
  console.log("追加した10社のドキュメントを削除");
  console.log("================================================================================");
  console.log();

  let successCount = 0;
  let failCount = 0;

  for (const [type, docId] of Object.entries(ADDED_COMPANIES)) {
    try {
      const docRef = db.collection(COLLECTION_NAME).doc(docId);
      const docSnap = await docRef.get();

      if (!docSnap.exists) {
        console.log(`⚠️  タイプ${type}: ドキュメントが既に存在しません (ID: ${docId})`);
        continue;
      }

      const data = docSnap.data();
      const companyName = data?.name || "(名前なし)";

      await docRef.delete();
      console.log(`✓ タイプ${type}: 削除完了 - ${companyName} (ID: ${docId})`);
      successCount++;

    } catch (error: any) {
      console.error(`❌ タイプ${type}: 削除エラー - ${error.message} (ID: ${docId})`);
      failCount++;
    }
  }

  console.log("\n================================================================================");
  console.log("削除結果");
  console.log("================================================================================");
  console.log(`✓ 削除成功: ${successCount}件`);
  console.log(`❌ 削除失敗: ${failCount}件`);
  console.log("================================================================================");
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

