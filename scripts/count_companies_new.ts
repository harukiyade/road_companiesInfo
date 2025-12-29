/* 
  companies_newコレクションの件数をカウントするスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/count_companies_new.ts
*/

import admin from "firebase-admin";
import * as fs from "fs";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";
const PAGE_SIZE = 1000;

function initFirebaseAdmin() {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    console.error(
      "❌ エラー: 環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されていません"
    );
    process.exit(1);
  }

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(
      `❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`
    );
    process.exit(1);
  }

  const serviceAccount = JSON.parse(
    fs.readFileSync(serviceAccountPath, "utf8")
  );

  const projectId =
    serviceAccount.project_id ||
    process.env.GCLOUD_PROJECT ||
    process.env.GCP_PROJECT ||
    PROJECT_ID;

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });
  console.log(`✅ Firebase Admin initialized (Project ID: ${projectId})`);

  return admin.firestore();
}

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  console.log(`\n📊 ${COLLECTION_NAME}コレクションの件数をカウント中...\n`);

  let totalCount = 0;
  let lastDoc: FirebaseFirestore.QueryDocumentSnapshot | null = null;

  while (true) {
    let query = colRef
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(PAGE_SIZE);
    
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();

    if (snapshot.empty) {
      break;
    }

    totalCount += snapshot.size;
    lastDoc = snapshot.docs[snapshot.docs.length - 1];

    // 10,000件ごとにログ出力（パフォーマンス向上）
    if (totalCount % 10000 === 0 || totalCount < 10000) {
      console.log(`  カウント中... ${totalCount.toLocaleString()} 件`);
    }
  }

  console.log(`\n${"=".repeat(60)}`);
  console.log(`📈 総件数: ${totalCount.toLocaleString()} 件`);
  console.log(`${"=".repeat(60)}\n`);
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});

