/**
 * 巨大ドキュメントの問題フィールドをリセットするスクリプト
 */

import * as admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const DOC_ID = "3030001094218"; // 問題のドキュメントID

// Firebase初期化
function initializeFirebase() {
  const projectRoot = process.cwd();
  const defaultPaths = [
    "./serviceAccountKey.json",
    "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
    path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
  ];

  let serviceAccountPath: string | null = null;
  for (const p of defaultPaths) {
    if (fs.existsSync(p)) {
      serviceAccountPath = p;
      break;
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ エラー: サービスアカウントキーが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  
  if (!admin.apps.length) {
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  }
  
  return admin.firestore();
}

async function fixLargeDoc() {
  const db = initializeFirebase();
  const docRef = db.collection("companies_new").doc(DOC_ID);
  
  const doc = await docRef.get();
  if (!doc.exists) {
    console.log(`❌ ドキュメント ${DOC_ID} が見つかりません`);
    return;
  }
  
  const data = doc.data() as Record<string, any>;
  
  console.log(`📊 ドキュメント ${DOC_ID} のフィールドサイズ:`);
  
  const fieldSizes: { field: string; size: number }[] = [];
  
  for (const [field, value] of Object.entries(data)) {
    if (value === null || value === undefined) continue;
    const size = JSON.stringify(value).length;
    fieldSizes.push({ field, size });
  }
  
  // サイズ順にソート
  fieldSizes.sort((a, b) => b.size - a.size);
  
  // 上位10フィールドを表示
  console.log("\n📈 サイズ上位フィールド:");
  for (const { field, size } of fieldSizes.slice(0, 10)) {
    const sizeKB = (size / 1024).toFixed(1);
    console.log(`  ${field}: ${sizeKB} KB`);
  }
  
  // 100KB以上のフィールドをリセット
  const fieldsToReset: string[] = [];
  for (const { field, size } of fieldSizes) {
    if (size > 100 * 1024) { // 100KB以上
      fieldsToReset.push(field);
    }
  }
  
  if (fieldsToReset.length > 0) {
    console.log(`\n🔄 リセット対象フィールド: ${fieldsToReset.join(", ")}`);
    
    const updateData: Record<string, any> = {};
    for (const field of fieldsToReset) {
      updateData[field] = null;
    }
    
    await docRef.update(updateData);
    console.log("✅ リセット完了");
  } else {
    console.log("\n✅ 100KB以上のフィールドはありません");
  }
}

fixLargeDoc().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

