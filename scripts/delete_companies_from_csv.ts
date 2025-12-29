/* 
  指定したCSVファイルからインポートされたドキュメントを削除するスクリプト
  
  使い方:
    DRY_RUN=1 npx tsx scripts/delete_companies_from_csv.ts 53.csv  # 削除せず候補だけログ
    npx tsx scripts/delete_companies_from_csv.ts 53.csv             # 実際に削除
*/

import "dotenv/config";
import admin from "firebase-admin";
import type { Firestore, CollectionReference, WriteBatch } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";
const BATCH_DELETE_SIZE = 400; // Firestoreのバッチ削除上限（500未満）

// ==============================
// Firebase 初期化
// ==============================
function initFirebaseAdmin(): Firestore {
  if (admin.apps.length) {
    return admin.firestore();
  }

  try {
    admin.initializeApp({
      credential: admin.credential.applicationDefault(),
    });
    console.log("✅ Firebase 初期化完了");
    return admin.firestore();
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", (error as Error).message);
    console.error("   環境変数 GOOGLE_APPLICATION_CREDENTIALS が正しく設定されているか確認してください");
    throw error;
  }
}

const db: Firestore = initFirebaseAdmin();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// ==============================
// メイン処理
// ==============================

async function main() {
  const csvFileName = process.argv[2];
  if (!csvFileName) {
    console.error("使用方法: npx tsx scripts/delete_companies_from_csv.ts <csvFileName>");
    console.error("例: npx tsx scripts/delete_companies_from_csv.ts 53.csv");
    process.exit(1);
  }

  if (DRY_RUN) {
    console.log("🔍 DRY_RUN モード: Firestore は削除しません\n");
  } else {
    console.log(`⚠️  本番モード: ${csvFileName} からインポートされたドキュメントを削除します\n`);
  }

  console.log(`🔍 ${csvFileName} からインポートされたドキュメントを検索中...`);

  let lastDoc: any = null;
  let totalScanned = 0;
  const deleteCandidates: Array<{ docId: string; name: string; source: string }> = [];

  while (true) {
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(1000);
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snapshot = await query.get();
    if (snapshot.empty) break;

    for (const doc of snapshot.docs) {
      totalScanned++;
      const data = doc.data();
      
      // ソース情報を取得
      const sourceFile = data.source?.file || data.lastImportSource?.file || "";
      
      // 指定したCSVファイルからインポートされたかチェック
      if (sourceFile === csvFileName || sourceFile.includes(csvFileName)) {
        const name = data.name || data.companyName || "(名前なし)";
        deleteCandidates.push({
          docId: doc.id,
          name: String(name),
          source: sourceFile,
        });
      }

      lastDoc = doc;
    }

    if (totalScanned % 10000 === 0) {
      console.log(`  📊 スキャン中... (${totalScanned} 件)`);
    }
  }

  console.log(`\n📊 スキャン完了: ${totalScanned} 件のドキュメントを確認`);
  console.log(`📊 削除候補: ${deleteCandidates.length} 件のドキュメント\n`);

  if (deleteCandidates.length === 0) {
    console.log(`✅ ${csvFileName} からインポートされたドキュメントは見つかりませんでした`);
    process.exit(0);
  }

  // 最初の10件を表示
  console.log("削除候補の例（最初の10件）:");
  for (let i = 0; i < Math.min(10, deleteCandidates.length); i++) {
    const candidate = deleteCandidates[i];
    console.log(`  ${i + 1}. ${candidate.name} (docId: ${candidate.docId}, source: ${candidate.source})`);
  }
  if (deleteCandidates.length > 10) {
    console.log(`  ... 他 ${deleteCandidates.length - 10} 件`);
  }
  console.log();

  // 削除を実行
  if (DRY_RUN) {
    console.log("💡 DRY_RUN モードのため、削除は実行されませんでした");
    console.log(`   削除予定: ${deleteCandidates.length} 件`);
  } else {
    console.log(`🗑️  ドキュメントを削除中...`);

    let batch: WriteBatch = db.batch();
    let batchCount = 0;
    let deletedCount = 0;

    for (const candidate of deleteCandidates) {
      batch.delete(companiesCol.doc(candidate.docId));
      batchCount++;

      if (batchCount >= BATCH_DELETE_SIZE) {
        await batch.commit();
        deletedCount += batchCount;
        console.log(`  💾 削除バッチコミット: ${batchCount} 件 (合計: ${deletedCount} 件)`);
        batch = db.batch();
        batchCount = 0;
      }
    }

    // 最後のバッチをコミット
    if (batchCount > 0) {
      await batch.commit();
      deletedCount += batchCount;
      console.log(`  💾 最後の削除バッチコミット: ${batchCount} 件 (合計: ${deletedCount} 件)`);
    }

    console.log(`\n✅ 削除完了: ${deletedCount} 件のドキュメントを削除しました`);
  }

  process.exit(0);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
