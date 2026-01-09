/* 
  テストインポートで追加したドキュメントを削除するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_test_imported_docs.ts
*/

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const COLLECTION_NAME = "companies_new";

// 削除するドキュメントIDリスト
const DOC_IDS_TO_DELETE = [
  "1766599059055000001",
  "1766599059471000003",
  "1766599059806000004",
];

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
    ];

    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        break;
      }
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ エラー: サービスアカウントキーファイルのパスが指定されていません");
    process.exit(1);
  }

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(`❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
    process.exit(1);
  }

  try {
    const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
    const projectId = serviceAccount.project_id || process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT;

    if (!projectId) {
      console.error("❌ エラー: Project ID を検出できませんでした");
      process.exit(1);
    }

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: projectId,
    });

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
  } catch (err: any) {
    console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
    console.error(`   詳細: ${err.message}`);
    process.exit(1);
  }
}

const db = admin.firestore();
const companiesCol = db.collection(COLLECTION_NAME);

async function main() {
  console.log(`🗑️  削除対象ドキュメントID: ${DOC_IDS_TO_DELETE.length} 件\n`);

  const batch = db.batch();
  let foundCount = 0;
  let notFoundCount = 0;

  for (const docId of DOC_IDS_TO_DELETE) {
    const docRef = companiesCol.doc(docId);
    const doc = await docRef.get();

    if (doc.exists) {
      foundCount++;
      const data = doc.data();
      console.log(`✅ 発見: ${docId} - ${data?.name || "(名前なし)"}`);
      batch.delete(docRef);
    } else {
      notFoundCount++;
      console.log(`⚠️  見つかりませんでした: ${docId}`);
    }
  }

  if (foundCount > 0) {
    console.log(`\n💾 削除を実行中...`);
    await batch.commit();
    console.log(`\n✅ 削除完了！ ${foundCount} 件のドキュメントを削除しました。`);
  } else {
    console.log(`\n⚠️  削除するドキュメントが見つかりませんでした。`);
  }

  if (notFoundCount > 0) {
    console.log(`   見つからなかったドキュメント: ${notFoundCount} 件`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
