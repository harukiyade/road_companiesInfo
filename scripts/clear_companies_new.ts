import * as admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

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

async function clearCollection() {
  const DRY_RUN = process.argv.includes("--dry-run");
  
  console.log(DRY_RUN ? "🔍 DRY_RUN モード（削除しません）\n" : "⚠️  本番モード（削除します）\n");
  
  const db = initializeFirebase();
  const companiesCol = db.collection("companies_new");
  
  // 全ドキュメントを取得
  const snapshot = await companiesCol.get();
  const totalDocs = snapshot.size;
  
  console.log(`📊 companies_new コレクション: ${totalDocs} ドキュメント`);
  
  if (DRY_RUN) {
    console.log(`\n💡 --dry-run を外すと実際に削除します`);
    return;
  }
  
  // 確認プロンプト
  if (!process.argv.includes("--yes")) {
    console.log(`\n⚠️  ${totalDocs} 件のドキュメントを削除しますか？`);
    console.log(`   確認済みの場合は --yes オプションを付けて再実行してください`);
    console.log(`   npx ts-node scripts/clear_companies_new.ts --yes`);
    return;
  }
  
  console.log(`\n🗑️  削除開始...`);
  
  // バッチ削除
  const batchSize = 500;
  let deletedCount = 0;
  
  const docs = snapshot.docs;
  for (let i = 0; i < docs.length; i += batchSize) {
    const batch = db.batch();
    const chunk = docs.slice(i, i + batchSize);
    
    for (const doc of chunk) {
      batch.delete(doc.ref);
    }
    
    await batch.commit();
    deletedCount += chunk.length;
    console.log(`  ✅ 削除済み: ${deletedCount} / ${totalDocs}`);
  }
  
  console.log(`\n✅ 完了: ${deletedCount} 件削除しました`);
}

clearCollection().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

