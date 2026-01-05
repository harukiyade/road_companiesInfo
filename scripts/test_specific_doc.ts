/* eslint-disable no-console */

/**
 * 特定のdocIdをテストするスクリプト
 */

import admin from "firebase-admin";
import * as fs from "fs";

// Firebase Admin SDK 初期化
if (!admin.apps.length) {
  try {
    const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;

    if (!serviceAccountPath) {
      console.error("❌ エラー: FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません");
      process.exit(1);
    }

    if (!fs.existsSync(serviceAccountPath)) {
      console.error(`❌ エラー: サービスアカウントキーファイルが存在しません: ${serviceAccountPath}`);
      process.exit(1);
    }

    const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  } catch (error: any) {
    console.error("❌ Firebase初期化エラー:", error.message);
    process.exit(1);
  }
}

const db = admin.firestore();

async function testSpecificDoc() {
  const docId = "1764469650024004469";
  
  try {
    const docRef = db.collection("companies_new").doc(docId);
    const doc = await docRef.get();
    
    if (!doc.exists) {
      console.log(`❌ ドキュメントが存在しません: ${docId}`);
      return;
    }

    const data = doc.data();
    console.log(`✅ ドキュメントID: ${docId}`);
    console.log(`名前: ${data?.name || "N/A"}`);
    console.log(`\n現在の値:`);
    console.log(`  industryLarge: "${data?.industryLarge || ""}"`);
    console.log(`  industryMiddle: "${data?.industryMiddle || ""}"`);
    console.log(`  industrySmall: "${data?.industrySmall || ""}"`);
    console.log(`  industryDetail: "${data?.industryDetail || ""}"`);
  } catch (error: any) {
    console.error(`❌ エラー: ${error.message}`);
  }
}

testSpecificDoc().catch((error) => {
  console.error("❌ 重大エラー:", error);
  process.exit(1);
});
