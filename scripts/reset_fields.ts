/**
 * 特定のフィールドをリセットするスクリプト
 * ドキュメント自体は削除せず、指定したフィールドのみを削除します
 */

import * as admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

// リセット対象のフィールド
const FIELDS_TO_RESET = [
  "shareholders",
  "executives",
];

const DRY_RUN = process.argv.includes("--dry-run");

// Firebase初期化
function initializeFirebase() {
  const projectRoot = process.cwd();
  const defaultPaths = [
    "./serviceAccountKey.json",
    "./service-account-key.json",
    "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
    path.join(projectRoot, "serviceAccountKey.json"),
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
  
  console.log(`✅ Firebase 初期化完了`);
  return admin.firestore();
}

async function resetFields() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log(`🔄 リセット対象フィールド: ${FIELDS_TO_RESET.join(", ")}\n`);
  
  const db = initializeFirebase();
  const companiesCol = db.collection("companies_new");
  
  // 全ドキュメントを取得
  const snapshot = await companiesCol.get();
  const totalDocs = snapshot.size;
  
  console.log(`📊 companies_new: ${totalDocs} ドキュメント\n`);
  
  let resetCount = 0;
  let batchCount = 0;
  const batchSize = 500;
  let batch = db.batch();
  
  for (const doc of snapshot.docs) {
    const data = doc.data();
    const updateData: Record<string, any> = {};
    let needsUpdate = false;
    
    for (const field of FIELDS_TO_RESET) {
      const value = data[field];
      
      // 配列で複数の値が入っている場合、または不正な形式の場合にリセット
      if (Array.isArray(value) && value.length > 1) {
        // 配列に複数の値が入っている = 不正なマージが発生している
        updateData[field] = admin.firestore.FieldValue.delete();
        needsUpdate = true;
      } else if (Array.isArray(value) && value.length === 1) {
        // 配列に1つの値が入っている = 文字列に変換
        updateData[field] = value[0];
        needsUpdate = true;
      }
    }
    
    if (needsUpdate) {
      if (!DRY_RUN) {
        batch.update(doc.ref, updateData);
        batchCount++;
        
        if (batchCount >= batchSize) {
          await batch.commit();
          batch = db.batch();
          batchCount = 0;
        }
      }
      resetCount++;
    }
  }
  
  // 残りのバッチをコミット
  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
  }
  
  console.log(`\n✅ 完了`);
  console.log(`  📊 リセット対象: ${resetCount} ドキュメント`);
  
  if (DRY_RUN) {
    console.log(`\n💡 --dry-run を外すと実際にリセットします`);
  }
}

resetFields().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

