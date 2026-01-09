/* 
  「丹羽興業株式会社」のドキュメントを削除するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_niwa_kogyo.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

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
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  
  const companyName = "丹羽興業株式会社";
  const corporateNumber = "9180000000000"; // 9.18E+12を正規化
  
  console.log(`🔍 検索条件:`);
  console.log(`  企業名: ${companyName}`);
  console.log(`  法人番号: ${corporateNumber}`);
  console.log(`  指定ドキュメントID: 17654801411612238, 17654800952160303, 17654800949084104\n`);
  
  const docIdsToDelete: string[] = [];
  
  // 1. 指定されたドキュメントIDを確認
  const specifiedIds = ["17654801411612238", "17654800952160303", "17654800949084104"];
  for (const docId of specifiedIds) {
    const docRef = companiesCol.doc(docId);
    const doc = await docRef.get();
    if (doc.exists) {
      const data = doc.data();
      if (data && (data.name === companyName || data.corporateNumber === corporateNumber)) {
        docIdsToDelete.push(docId);
        console.log(`  ✅ 指定IDで見つかりました: ${docId}`);
        console.log(`     企業名: ${data.name || '(なし)'}`);
        console.log(`     法人番号: ${data.corporateNumber || '(なし)'}`);
      } else {
        console.log(`  ⚠️  指定IDのドキュメントは存在しますが、企業名/法人番号が一致しません: ${docId}`);
        console.log(`     企業名: ${data?.name || '(なし)'}`);
        console.log(`     法人番号: ${data?.corporateNumber || '(なし)'}`);
      }
    } else {
      console.log(`  ⚠️  指定IDのドキュメントが見つかりません: ${docId}`);
    }
  }
  
  // 2. 法人番号で検索
  console.log(`\n🔍 法人番号で検索中...`);
  const snapByCorp = await companiesCol
    .where("corporateNumber", "==", corporateNumber)
    .get();
  
  for (const doc of snapByCorp.docs) {
    if (!docIdsToDelete.includes(doc.id)) {
      docIdsToDelete.push(doc.id);
      console.log(`  ✅ 法人番号で見つかりました: ${doc.id}`);
    }
  }
  
  // 3. 企業名で検索
  console.log(`\n🔍 企業名で検索中...`);
  const snapByName = await companiesCol
    .where("name", "==", companyName)
    .limit(10)
    .get();
  
  for (const doc of snapByName.docs) {
    if (!docIdsToDelete.includes(doc.id)) {
      docIdsToDelete.push(doc.id);
      console.log(`  ✅ 企業名で見つかりました: ${doc.id}`);
    }
  }
  
  // 4. 法人番号をdocIdとして直接確認
  const directDocRef = companiesCol.doc(corporateNumber);
  const directDoc = await directDocRef.get();
  if (directDoc.exists) {
    if (!docIdsToDelete.includes(corporateNumber)) {
      docIdsToDelete.push(corporateNumber);
      console.log(`  ✅ 法人番号をdocIdとして見つかりました: ${corporateNumber}`);
    }
  }
  
  // 5. 削除実行
  console.log(`\n${"=".repeat(60)}`);
  console.log(`📊 削除対象: ${docIdsToDelete.length} 件`);
  console.log(`${"=".repeat(60)}`);
  
  if (docIdsToDelete.length === 0) {
    console.log(`⚠️  削除対象のドキュメントが見つかりませんでした`);
    return;
  }
  
  for (const docId of docIdsToDelete) {
    const docRef = companiesCol.doc(docId);
    if (DRY_RUN) {
      const doc = await docRef.get();
      const data = doc.data();
      console.log(`  🔍 (DRY_RUN) 削除予定: ${docId}`);
      console.log(`     企業名: ${data?.name || '(なし)'}`);
      console.log(`     法人番号: ${data?.corporateNumber || '(なし)'}`);
      console.log(`     住所: ${data?.address || '(なし)'}`);
    } else {
      await docRef.delete();
      console.log(`  ✅ 削除完了: ${docId}`);
    }
  }
  
  console.log(`\n✅ 処理完了`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に削除するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
