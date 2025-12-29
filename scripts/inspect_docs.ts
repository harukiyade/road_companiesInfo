/*
  特定ドキュメントの内容を確認するスクリプト
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const TARGET_DOC_IDS = [
  "3430001051236",
  "5430001089258",
  "5430001094489",
  "6450001013611"
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
}

const db: Firestore = admin.firestore();

async function main() {
  for (const docId of TARGET_DOC_IDS) {
    console.log(`\n📄 ドキュメントID: ${docId}`);
    const docRef = db.collection(COLLECTION_NAME).doc(docId);
    const docSnap = await docRef.get();

    if (!docSnap.exists) {
      console.warn(`  ⚠️  ドキュメントが見つかりません`);
      continue;
    }

    const data = docSnap.data();
    if (!data) {
      console.warn(`  ⚠️  ドキュメントデータが空です`);
      continue;
    }

    console.log(`  name: ${data.name}`);
    console.log(`  corporateNumber: ${data.corporateNumber}`);
    
    // JSON形式のフィールドを確認
    for (const [field, value] of Object.entries(data)) {
      if (typeof value === "string" && (value.startsWith("{") || value.startsWith("["))) {
        console.log(`\n  📝 ${field} (JSON形式):`);
        try {
          const parsed = JSON.parse(value);
          console.log(`    ${JSON.stringify(parsed, null, 2).substring(0, 500)}...`);
        } catch (e) {
          console.log(`    (パースエラー)`);
        }
      } else if (typeof value === "object" && value !== null && !Array.isArray(value)) {
        console.log(`\n  📝 ${field} (オブジェクト):`);
        console.log(`    ${JSON.stringify(value, null, 2).substring(0, 500)}...`);
      }
    }
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

