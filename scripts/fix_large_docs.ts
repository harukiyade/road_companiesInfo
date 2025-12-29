/**
 * 特定の巨大ドキュメントを修復するスクリプト
 */

import * as admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

// 問題のドキュメントIDリスト
const PROBLEM_DOC_IDS = [
  "3030001094218",
  "1010901037923",
];

const MAX_FIELD_SIZE = 50 * 1024; // 50KBを超えるフィールドをリセット

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
  
  console.log("✅ Firebase 初期化完了");
  return admin.firestore();
}

async function fixLargeDocs() {
  const db = initializeFirebase();
  const companiesCol = db.collection("companies_new");
  
  console.log(`\n🔧 ${PROBLEM_DOC_IDS.length} 件のドキュメントを修復中...\n`);
  
  let fixedCount = 0;
  
  for (const docId of PROBLEM_DOC_IDS) {
    const docRef = companiesCol.doc(docId);
    const doc = await docRef.get();
    
    if (!doc.exists) {
      console.log(`⚠️  ${docId}: 見つかりません`);
      continue;
    }
    
    const data = doc.data() as Record<string, any>;
    const docSize = JSON.stringify(data).length;
    console.log(`📄 ${docId}: ${(docSize / 1024).toFixed(0)} KB`);
    
    // 巨大フィールドを特定
    const fieldsToReset: string[] = [];
    for (const [field, value] of Object.entries(data)) {
      if (value === null || value === undefined) continue;
      const fieldSize = JSON.stringify(value).length;
      if (fieldSize > MAX_FIELD_SIZE) {
        console.log(`   └─ ${field}: ${(fieldSize / 1024).toFixed(0)} KB → リセット`);
        fieldsToReset.push(field);
      }
    }
    
    if (fieldsToReset.length > 0) {
      const updateData: Record<string, any> = {};
      for (const field of fieldsToReset) {
        updateData[field] = null;
      }
      
      await docRef.update(updateData);
      fixedCount++;
      console.log(`   ✅ 修復完了\n`);
    } else {
      console.log(`   ℹ️  50KB以上のフィールドなし\n`);
    }
  }
  
  console.log(`========================================`);
  console.log(`✅ 修復完了: ${fixedCount} 件`);
}

fixLargeDocs().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
