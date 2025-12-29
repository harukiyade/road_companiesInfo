/* 
  既存のcompanies_newコレクションのフィールド構造を確認
*/

import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

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
  console.log("companies_newコレクション 既存フィールド構造の確認");
  console.log("================================================================================");
  console.log();

  // 最初の5件を取得してフィールド構造を確認
  const snapshot = await db.collection(COLLECTION_NAME).limit(5).get();

  if (snapshot.empty) {
    console.log("⚠️  コレクションにドキュメントが存在しません");
    return;
  }

  console.log(`取得件数: ${snapshot.size}件\n`);

  // 全フィールドを集約
  const allFields = new Set<string>();
  const fieldTypes: Record<string, Set<string>> = {};
  const sampleValues: Record<string, any[]> = {};

  snapshot.forEach((doc) => {
    const data = doc.data();
    Object.keys(data).forEach(field => {
      allFields.add(field);
      
      const value = data[field];
      const type = Array.isArray(value) ? "array" : value === null ? "null" : typeof value;
      
      if (!fieldTypes[field]) {
        fieldTypes[field] = new Set();
      }
      fieldTypes[field].add(type);
      
      if (!sampleValues[field]) {
        sampleValues[field] = [];
      }
      if (sampleValues[field].length < 3 && value !== null && value !== undefined) {
        sampleValues[field].push(value);
      }
    });
  });

  console.log("================================================================================");
  console.log(`発見されたフィールド: ${allFields.size}個`);
  console.log("================================================================================\n");

  // フィールドをアルファベット順にソート
  const sortedFields = Array.from(allFields).sort();

  console.log("フィールド名 | 型 | サンプル値");
  console.log("-------------|----|-----------");

  for (const field of sortedFields) {
    const types = Array.from(fieldTypes[field]).join(", ");
    const samples = sampleValues[field] || [];
    let sampleStr = "";
    
    if (samples.length > 0) {
      const firstSample = samples[0];
      if (typeof firstSample === "string") {
        sampleStr = firstSample.length > 30 ? firstSample.substring(0, 30) + "..." : firstSample;
      } else if (Array.isArray(firstSample)) {
        sampleStr = `[${firstSample.length}件]`;
      } else if (typeof firstSample === "object") {
        sampleStr = JSON.stringify(firstSample).substring(0, 30) + "...";
      } else {
        sampleStr = String(firstSample);
      }
    } else {
      sampleStr = "(値なし)";
    }
    
    console.log(`${field} | ${types} | ${sampleStr}`);
  }

  console.log("\n================================================================================");
  console.log("サンプルドキュメント（1件目）の完全な構造");
  console.log("================================================================================\n");

  const firstDoc = snapshot.docs[0];
  console.log(`ドキュメントID: ${firstDoc.id}\n`);
  
  const firstData = firstDoc.data();
  const sortedEntries = Object.entries(firstData).sort(([a], [b]) => a.localeCompare(b));
  
  for (const [key, value] of sortedEntries) {
    let displayValue: string;
    
    if (value === null || value === undefined) {
      displayValue = "null";
    } else if (Array.isArray(value)) {
      displayValue = `[${value.length}件] ${value.length > 0 ? JSON.stringify(value[0]) : ""}`;
    } else if (typeof value === "object") {
      displayValue = JSON.stringify(value).substring(0, 100);
    } else if (typeof value === "string") {
      displayValue = value.length > 100 ? value.substring(0, 100) + "..." : value;
    } else {
      displayValue = String(value);
    }
    
    console.log(`  ${key}: ${displayValue}`);
  }

  console.log("\n================================================================================");
  console.log("完了");
  console.log("================================================================================");
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

