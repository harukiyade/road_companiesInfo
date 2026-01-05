/* eslint-disable no-console */
/**
 * companies_newコレクションでnameフィールドに値がないドキュメントの数を確認するスクリプト
 */

import admin from "firebase-admin";

// Firebase Admin SDK 初期化
const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
if (!serviceAccountPath) {
  console.error("❌ FIREBASE_SERVICE_ACCOUNT_KEY環境変数が設定されていません");
  process.exit(1);
}

try {
  const serviceAccount = require(serviceAccountPath);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
  console.log("✅ Firebase初期化完了");
} catch (error: any) {
  console.error("❌ Firebase初期化エラー:", error.message);
  process.exit(1);
}

const db = admin.firestore();

/**
 * nameフィールドに値がないかチェック
 */
function hasNoName(data: admin.firestore.DocumentData | undefined): boolean {
  if (!data) return true;
  const name = data.name;
  return name === null || name === undefined || name === "";
}

/**
 * メイン処理: nameフィールドに値がないドキュメントの数を確認
 */
async function checkDocumentsWithoutName() {
  try {
    console.log("companies_newコレクションでnameフィールドに値がないドキュメントを検索中...");

    const BATCH_SIZE = 500;
    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;
    let totalWithoutName = 0;
    const sampleIds: string[] = [];

    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }

      // nameフィールドに値がないドキュメントをフィルタリング
      const docsWithoutName = snapshot.docs.filter((doc) => {
        const data = doc.data();
        return hasNoName(data);
      });

      totalWithoutName += docsWithoutName.length;
      
      // サンプルIDを収集（最大10件）
      if (sampleIds.length < 10 && docsWithoutName.length > 0) {
        docsWithoutName.slice(0, 10 - sampleIds.length).forEach((doc) => {
          sampleIds.push(doc.id);
        });
      }

      totalProcessed += snapshot.size;
      lastDoc = snapshot.docs[snapshot.docs.length - 1];

      if (totalProcessed % 5000 === 0) {
        console.log(`  処理中: ${totalProcessed} 件, nameなし: ${totalWithoutName} 件`);
      }
    }

    console.log("\n" + "=".repeat(60));
    console.log("✅ 確認完了");
    console.log(`  総処理件数: ${totalProcessed} 件`);
    console.log(`  nameフィールドに値がないドキュメント: ${totalWithoutName} 件`);
    console.log("=".repeat(60));

    if (totalWithoutName > 0 && sampleIds.length > 0) {
      console.log("\nサンプルID（最初の10件）:");
      sampleIds.forEach((id) => {
        console.log(`  - ${id}`);
      });
    }

    if (totalWithoutName === 0) {
      console.log("\n✅ nameフィールドに値がないドキュメントはありません");
    } else {
      console.log(`\n⚠️  nameフィールドに値がないドキュメントが ${totalWithoutName} 件残っています`);
      console.log("削除スクリプトを実行してください:");
      console.log("  export FIREBASE_SERVICE_ACCOUNT_KEY='...' && npx ts-node scripts/delete_documents_without_name.ts");
    }

    process.exit(0);
  } catch (error: any) {
    console.error("❌ エラー:", error.message);
    console.error(error);
    process.exit(1);
  }
}

checkDocumentsWithoutName();
