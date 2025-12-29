/*
  特定のドキュメントIDの存在を確認するスクリプト
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DOC_IDS_TO_CHECK = [
  "2010000000000",
  "7010000000000",
  "5010000000000",
  "2011400000000"
];

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
  console.log("🔍 特定のドキュメントIDを確認中...\n");

  for (const docId of DOC_IDS_TO_CHECK) {
    const docRef = companiesCol.doc(docId);
    const docSnap = await docRef.get();

    if (docSnap.exists) {
      const data = docSnap.data();
      console.log(`✅ docId: ${docId}`);
      console.log(`   会社名: ${data?.name || '(null)'}`);
      console.log(`   法人番号: ${data?.corporateNumber || '(null)'}`);
      console.log(`   住所: ${data?.address || '(null)'}`);
      console.log();
    } else {
      console.log(`❌ docId: ${docId} - 存在しません\n`);
    }
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

