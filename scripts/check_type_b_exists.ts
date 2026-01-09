/* 
  タイプBのドキュメント（RfbBjCdNQhMkYZBUvPIv）が存在するか確認
*/

import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const TYPE_B_DOC_ID = "RfbBjCdNQhMkYZBUvPIv";

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
  console.log("タイプBのドキュメント存在確認");
  console.log("================================================================================");
  console.log();
  console.log(`コレクション: ${COLLECTION_NAME}`);
  console.log(`ドキュメントID: ${TYPE_B_DOC_ID}`);
  console.log();

  try {
    // 1. ドキュメントIDで直接取得を試みる
    console.log("【方法1】ドキュメントIDで直接取得...");
    const docRef = db.collection(COLLECTION_NAME).doc(TYPE_B_DOC_ID);
    const docSnap = await docRef.get();

    if (docSnap.exists) {
      console.log("✅ ドキュメントが見つかりました！");
      const data = docSnap.data();
      console.log(`  企業名: ${data?.name}`);
      console.log(`  法人番号: ${data?.corporateNumber}`);
      console.log(`  住所: ${data?.address}`);
    } else {
      console.log("❌ ドキュメントが見つかりません");
    }

    console.log();

    // 2. companies_newコレクション全体を検索
    console.log("【方法2】companies_newコレクション全体を検索...");
    console.log("最近追加された10件を表示:");
    
    const recentDocs = await db.collection(COLLECTION_NAME)
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(10)
      .get();

    if (recentDocs.empty) {
      console.log("❌ コレクションが空です");
    } else {
      console.log(`✅ ${recentDocs.size}件のドキュメントを発見`);
      console.log();
      let index = 0;
      recentDocs.forEach((doc) => {
        index++;
        const data = doc.data();
        console.log(`${index}. ID: ${doc.id}`);
        console.log(`   企業名: ${data.name || "(なし)"}`);
        console.log(`   法人番号: ${data.corporateNumber || "(なし)"}`);
      });
    }

    console.log();

    // 3. 企業名で検索してみる
    console.log("【方法3】企業名「セクダム株式会社」で検索...");
    const nameQuery = await db.collection(COLLECTION_NAME)
      .where("name", "==", "セクダム株式会社")
      .limit(5)
      .get();

    if (nameQuery.empty) {
      console.log("❌ 企業名での検索結果なし");
    } else {
      console.log(`✅ ${nameQuery.size}件見つかりました`);
      nameQuery.forEach((doc) => {
        const data = doc.data();
        console.log(`  ドキュメントID: ${doc.id}`);
        console.log(`  企業名: ${data.name}`);
        console.log(`  法人番号: ${data.corporateNumber}`);
      });
    }

    console.log();

    // 4. 法人番号で検索してみる
    console.log("【方法4】法人番号「7180001017077」で検索...");
    const corpNumQuery = await db.collection(COLLECTION_NAME)
      .where("corporateNumber", "==", "7180001017077")
      .limit(5)
      .get();

    if (corpNumQuery.empty) {
      console.log("❌ 法人番号での検索結果なし");
    } else {
      console.log(`✅ ${corpNumQuery.size}件見つかりました`);
      corpNumQuery.forEach((doc) => {
        const data = doc.data();
        console.log(`  ドキュメントID: ${doc.id}`);
        console.log(`  企業名: ${data.name}`);
        console.log(`  法人番号: ${data.corporateNumber}`);
      });
    }

    console.log();

    // 5. タイプBのドキュメントIDが含まれるか確認
    console.log("【方法5】「RfbB」で始まるドキュメントIDを検索...");
    const allDocs = await db.collection(COLLECTION_NAME)
      .orderBy(admin.firestore.FieldPath.documentId())
      .startAt("RfbB")
      .endAt("RfbB\uf8ff")
      .get();

    if (allDocs.empty) {
      console.log("❌ 「RfbB」で始まるドキュメントが見つかりません");
    } else {
      console.log(`✅ ${allDocs.size}件見つかりました`);
      allDocs.forEach((doc) => {
        const data = doc.data();
        console.log(`  ドキュメントID: ${doc.id}`);
        console.log(`  企業名: ${data.name || "(なし)"}`);
      });
    }

  } catch (error: any) {
    console.error(`❌ エラー: ${error.message}`);
    console.error(error);
  }

  console.log();
  console.log("================================================================================");
  console.log("確認完了");
  console.log("================================================================================");
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

