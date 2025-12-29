/* 
  タイプIの財務情報を確認するスクリプト
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
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
    ];

    for (const pth of defaultPaths) {
      const resolved = path.resolve(pth);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        break;
      }
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId = serviceAccount.project_id || process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT;

  if (!projectId) {
    console.error("❌ エラー: Project ID を検出できませんでした");
    process.exit(1);
  }

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

async function main() {
  // サンプル企業の値を確認
  const sampleCorpNums = ['8010401179128', '5180001155721', '8011001152623', '1240001061074', '6150001026156'];
  
  console.log("📊 タイプIの財務情報確認:\n");
  
  for (const corpNum of sampleCorpNums) {
    try {
      const doc = await companiesCol.doc(corpNum).get();
      if (doc.exists) {
        const data = doc.data();
        console.log(`法人番号: ${corpNum}`);
        console.log(`  資本金: ${data?.capitalStock ?? "null"}`);
        console.log(`  売上: ${data?.revenue ?? "null"}`);
        console.log(`  利益: ${data?.latestProfit ?? "null"}`);
        console.log(`  決算月: ${data?.fiscalMonth ?? "null"}`);
        console.log('');
      } else {
        console.log(`法人番号: ${corpNum} - ドキュメントが見つかりません`);
        console.log('');
      }
    } catch (error: any) {
      console.error(`❌ エラー (${corpNum}): ${error.message}`);
    }
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

