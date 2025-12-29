/* 
  ドキュメントIDを指定して詳細を取得するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/get_doc_details_by_id.ts [ドキュメントID1] [ドキュメントID2] ...
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
      path.join(projectRoot, "config", "serviceAccountKey.json"),
      path.join(projectRoot, ".config", "serviceAccountKey.json"),
    ];

    for (const pth of defaultPaths) {
      const resolved = path.resolve(pth);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolved}`);
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
    const projectId =
      serviceAccount.project_id ||
      process.env.GCLOUD_PROJECT ||
      process.env.GCP_PROJECT;

    if (!projectId) {
      console.error("❌ エラー: Project ID を検出できませんでした");
      process.exit(1);
    }

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId,
    });

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
  } catch (err: any) {
    console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
    console.error(`   詳細: ${err.message}`);
    process.exit(1);
  }
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// メイン処理
async function main() {
  const docIds = process.argv.slice(2);

  if (docIds.length === 0) {
    console.error("❌ エラー: ドキュメントIDを指定してください");
    console.error("使い方: npx ts-node scripts/get_doc_details_by_id.ts [ドキュメントID1] [ドキュメントID2] ...");
    process.exit(1);
  }

  console.log(`🔍 ${docIds.length}件のドキュメントを取得します\n`);

  for (const docId of docIds) {
    try {
      const doc = await companiesCol.doc(docId).get();
      
      if (!doc.exists) {
        console.log(`\n❌ ドキュメントID: ${docId} - 存在しません`);
        continue;
      }

      const data = doc.data();
      console.log(`\n📄 ドキュメントID: ${docId}`);
      console.log("=".repeat(80));
      console.log(`会社名: ${data?.name || "(未設定)"}`);
      console.log(`法人番号: ${data?.corporateNumber || "(未設定)"}`);
      console.log(`都道府県: ${data?.prefecture || "(未設定)"}`);
      console.log(`郵便番号: ${data?.postalCode || "(未設定)"}`);
      console.log(`住所: ${data?.address || "(未設定)"}`);
      console.log(`代表者名: ${data?.representativeName || "(未設定)"}`);
      console.log(`電話番号: ${data?.phoneNumber || "(未設定)"}`);
      console.log(`企業URL: ${data?.companyUrl || "(未設定)"}`);
      console.log(`業種: ${data?.industry || "(未設定)"}`);
      console.log(`業種配列: ${data?.industries ? JSON.stringify(data.industries) : "(未設定)"}`);
      console.log(`設立: ${data?.established || "(未設定)"}`);
      console.log(`資本金: ${data?.capitalStock || "(未設定)"}`);
      console.log(`更新日時: ${data?.updatedAt ? data.updatedAt.toDate().toLocaleString("ja-JP") : "(未設定)"}`);
    } catch (err: any) {
      console.log(`\n❌ ドキュメントID: ${docId} - エラー: ${err.message}`);
    }
  }
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

