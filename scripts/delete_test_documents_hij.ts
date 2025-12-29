/* 
  タイプH・I・Jの新規追加したテストドキュメントを削除
*/

import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// 削除対象のドキュメントID
const DOC_IDS_TO_DELETE = {
  H: "GGlcAaYbxBJYfRvK1HhN",
  I: "rfODM79w8VPGnadmd8yy",
  J: "FVCBXMICk0bzVEkZzxZv",
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
  console.log("タイプH・I・Jのテストドキュメント削除");
  console.log("================================================================================");
  console.log();

  for (const [type, docId] of Object.entries(DOC_IDS_TO_DELETE)) {
    console.log(`【タイプ${type}】削除中... (ID: ${docId})`);
    try {
      const docRef = db.collection(COLLECTION_NAME).doc(docId);
      const docSnap = await docRef.get();

      if (docSnap.exists) {
        const data = docSnap.data();
        console.log(`  削除対象: ${data?.name || "(名前なし)"}`);
        await docRef.delete();
        console.log(`  ✓ 削除完了`);
      } else {
        console.log(`  ⚠️  ドキュメントが見つかりません`);
      }
    } catch (error: any) {
      console.error(`  ❌ 削除エラー: ${error.message}`);
    }
    console.log();
  }

  console.log("================================================================================");
  console.log("削除完了");
  console.log("================================================================================");
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

