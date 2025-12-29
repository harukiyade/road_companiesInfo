/* 
  テストインポートで作成されたドキュメントを削除するスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_test_imported_companies.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const BATCH_SIZE = 500; // Firestoreのバッチ制限

function initFirebase() {
  if (admin.apps.length === 0) {
    const serviceAccountPath =
      process.env.GOOGLE_APPLICATION_CREDENTIALS ||
      './serviceAccountKey.json';

    if (!fs.existsSync(serviceAccountPath)) {
      console.error(
        '❌ エラー: サービスアカウントキーファイルが見つかりません'
      );
      console.error(`   パス: ${serviceAccountPath}`);
      process.exit(1);
    }

    const serviceAccount = JSON.parse(
      fs.readFileSync(serviceAccountPath, 'utf8')
    );
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  }
}

function loadTestDocIds(): string[] {
  const docIds = new Set<string>();
  const cwd = process.cwd();
  
  // created_test_companies_*.txt ファイルを検索
  const files = fs.readdirSync(cwd)
    .filter(f => f.startsWith('created_test_companies_') && f.endsWith('.txt'))
    .sort();
  
  console.log(`📋 テストインポートログファイルを検索中...`);
  console.log(`   見つかったファイル: ${files.length}個`);
  
  for (const file of files) {
    const filePath = path.join(cwd, file);
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(line => line.trim());
    
    for (const line of lines) {
      const docId = line.trim();
      if (docId) {
        docIds.add(docId);
      }
    }
    
    console.log(`   - ${file}: ${lines.length}件のdocId`);
  }
  
  return Array.from(docIds);
}

async function deleteTestCompanies(
  db: Firestore,
  docIds: string[]
): Promise<void> {
  const companiesCol = db.collection(COLLECTION_NAME);
  
  console.log(`\n🗑️  削除を開始します...`);
  console.log(`   対象: ${docIds.length}件`);
  console.log(`   モード: ${DRY_RUN ? 'DRY RUN (実際には削除しません)' : '実際に削除'}\n`);

  let deletedCount = 0;
  let notFoundCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;

  for (let i = 0; i < docIds.length; i++) {
    const docId = docIds[i];
    const docRef = companiesCol.doc(docId);
    
    // ドキュメントの存在確認（DRY_RUN時のみ）
    if (DRY_RUN) {
      const doc = await docRef.get();
      if (doc.exists) {
        const data = doc.data();
        console.log(`  [DRY RUN] 削除予定: ${docId} - ${(data?.name || '(空)').substring(0, 60)}`);
      } else {
        console.log(`  [DRY RUN] 見つからない: ${docId}`);
        notFoundCount++;
      }
    } else {
      batch.delete(docRef);
      batchCount++;
      deletedCount++;

      // バッチ制限に達したらコミット
      if (batchCount >= BATCH_SIZE) {
        await batch.commit();
        console.log(`  ✅ 削除済み: ${deletedCount}/${docIds.length}件`);
        batch = db.batch();
        batchCount = 0;
      }
    }
    
    if ((i + 1) % 1000 === 0) {
      console.log(`  処理中: ${i + 1}/${docIds.length}件`);
    }
  }

  // 残りのバッチをコミット
  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 最終バッチ削除済み: ${batchCount}件`);
  }

  if (DRY_RUN) {
    console.log(`\n⚠️  DRY RUNモード: 実際には削除していません`);
    console.log(`   削除対象: ${docIds.length}件`);
    console.log(`   見つからない: ${notFoundCount}件`);
    console.log(`   実際に削除するには --dry-run フラグを外してください`);
  } else {
    console.log(`\n✅ 削除完了: ${deletedCount}件のドキュメントを削除しました`);
  }
}

async function main() {
  initFirebase();
  const db = admin.firestore();

  const docIds = loadTestDocIds();
  
  if (docIds.length === 0) {
    console.log('⚠️  削除対象のドキュメントIDが見つかりませんでした。');
    return;
  }

  await deleteTestCompanies(db, docIds);
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
