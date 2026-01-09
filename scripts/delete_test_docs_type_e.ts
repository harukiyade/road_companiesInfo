/*
  タイプEのテストドキュメントを削除するスクリプト
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const TEST_DOC_IDS = ["test_row_11", "test_row_12", "test_row_17"];

// Firebase初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: serviceAccount.project_id,
  });

  console.log(`✅ Firebase 初期化完了`);
}

const db: Firestore = admin.firestore();
const companiesCol = db.collection(COLLECTION_NAME);

async function main() {
  console.log("🗑️  テストドキュメントを削除中...\n");

  for (const docId of TEST_DOC_IDS) {
    const docRef = companiesCol.doc(docId);
    const docSnap = await docRef.get();

    if (docSnap.exists) {
      await docRef.delete();
      console.log(`  ✅ 削除完了: ${docId}`);
    } else {
      console.log(`  ⚠️  見つかりません: ${docId}`);
    }
  }

  console.log("\n✅ 削除処理完了");
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

