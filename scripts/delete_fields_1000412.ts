/* eslint-disable no-console */
/**
 * 1000412のclientsとexecutivesフィールドを削除するスクリプト
 */

import admin from "firebase-admin";
import * as path from "path";

// Firebase Admin SDK 初期化
const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
if (!serviceAccountPath) {
  console.error("❌ FIREBASE_SERVICE_ACCOUNT_KEY環境変数が設定されていません");
  process.exit(1);
}

try {
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccountPath),
  });
  console.log("[Firebase初期化] ✅ 初期化が完了しました");
} catch (error) {
  console.error("[Firebase初期化] ❌ エラー:", (error as any)?.message);
  process.exit(1);
}

const db = admin.firestore();

async function deleteFields() {
  const companyId = "1000412";
  
  try {
    const docRef = db.collection("companies_new").doc(companyId);
    const doc = await docRef.get();
    
    if (!doc.exists) {
      console.log(`❌ ドキュメント ${companyId} が見つかりません`);
      return;
    }
    
    const data = doc.data();
    console.log(`📋 現在のデータ:`);
    console.log(`  clients: ${data?.clients ? JSON.stringify(data.clients) : "なし"}`);
    console.log(`  executives: ${data?.executives ? JSON.stringify(data.executives) : "なし"}`);
    
    // clientsとexecutivesフィールドを削除
    await docRef.update({
      clients: admin.firestore.FieldValue.delete(),
      executives: admin.firestore.FieldValue.delete(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });
    
    console.log(`✅ ドキュメント ${companyId} のclientsとexecutivesフィールドを削除しました`);
    
    // 確認
    const updatedDoc = await docRef.get();
    const updatedData = updatedDoc.data();
    console.log(`📋 削除後のデータ:`);
    console.log(`  clients: ${updatedData?.clients ? JSON.stringify(updatedData.clients) : "なし"}`);
    console.log(`  executives: ${updatedData?.executives ? JSON.stringify(updatedData.executives) : "なし"}`);
    
  } catch (error) {
    console.error(`❌ エラー:`, (error as any)?.message);
  }
}

deleteFields()
  .then(() => {
    console.log("✅ 処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("❌ エラー:", error);
    process.exit(1);
  });

