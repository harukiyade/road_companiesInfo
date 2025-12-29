/* 
  最新にインポートされたドキュメントIDを取得するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/get_latest_imported_docs.ts [件数]
*/

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const COLLECTION_NAME = "companies_new";
const LIMIT = parseInt(process.argv[2]) || 10;

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

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})\n`);
  } catch (err: any) {
    console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
    console.error(`   詳細: ${err.message}`);
    process.exit(1);
  }
}

const db = admin.firestore();
const companiesCol = db.collection(COLLECTION_NAME);

async function main() {
  console.log(`📊 最新のドキュメントを取得中（作成日時順、上位${LIMIT}件）...\n`);

  try {
    // createdAtフィールドでソートして最新のものを取得
    const snapshot = await companiesCol
      .orderBy("createdAt", "desc")
      .limit(LIMIT)
      .get();

    if (snapshot.empty) {
      console.log("⚠️  ドキュメントが見つかりませんでした");
      return;
    }

    console.log(`✅ ${snapshot.size} 件のドキュメントを取得しました\n`);

    snapshot.docs.forEach((doc, index) => {
      const data = doc.data();
      const createdAt = data.createdAt?.toDate?.() || data.createdAt || "(日時不明)";
      console.log(`${index + 1}. ドキュメントID: ${doc.id}`);
      console.log(`   会社名: ${data.name || "(未設定)"}`);
      console.log(`   法人番号: ${data.corporateNumber || "(未設定)"}`);
      console.log(`   作成日時: ${createdAt}`);
      console.log("");
    });

    // 最初の3件のドキュメントIDのみを出力
    console.log("📋 新規インポートされたドキュメントID（最初の3件）:");
    snapshot.docs.slice(0, 3).forEach((doc, index) => {
      console.log(`   ${index + 1}. ${doc.id}`);
    });

  } catch (error: any) {
    if (error.code === 9) {
      // createdAtフィールドでソートできない場合、ドキュメントIDでソート
      console.log("⚠️  createdAtフィールドでソートできないため、ドキュメントIDでソートします...\n");
      
      const snapshot = await companiesCol
        .orderBy(admin.firestore.FieldPath.documentId(), "desc")
        .limit(LIMIT)
        .get();

      if (snapshot.empty) {
        console.log("⚠️  ドキュメントが見つかりませんでした");
        return;
      }

      console.log(`✅ ${snapshot.size} 件のドキュメントを取得しました\n`);

      snapshot.docs.forEach((doc, index) => {
        const data = doc.data();
        const createdAt = data.createdAt?.toDate?.() || data.createdAt || "(日時不明)";
        console.log(`${index + 1}. ドキュメントID: ${doc.id}`);
        console.log(`   会社名: ${data.name || "(未設定)"}`);
        console.log(`   法人番号: ${data.corporateNumber || "(未設定)"}`);
        console.log(`   作成日時: ${createdAt}`);
        console.log("");
      });

      console.log("📋 新規インポートされたドキュメントID（最初の3件）:");
      snapshot.docs.slice(0, 3).forEach((doc, index) => {
        console.log(`   ${index + 1}. ${doc.id}`);
      });
    } else {
      console.error("❌ エラー:", error);
      throw error;
    }
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
