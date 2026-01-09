// scripts/cleanup_companies_without_keys.ts
//
// companies_new コレクションのうち、
//   - name が空 or 未定義
//   - corporateNumber が空 or 未定義
//   - companyUrl は存在する（=URLだけのゴミdoc）
// を検出し、削除するメンテ用スクリプトです。
//
// 実行例:
//   DRY_RUN=1 npx ts-node scripts/cleanup_companies_without_keys.ts   // 削除せず候補だけログ
//   npx ts-node scripts/cleanup_companies_without_keys.ts             // 実際に削除
//
// Firestore 認証:
//   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/serviceAccountKey.json"

import admin from "firebase-admin";
import * as fs from "fs";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// 1 回のクエリで読む件数
const PAGE_SIZE = 1000;
// 1 バッチで削除する件数（Firestore の上限 500 未満にする）
const BATCH_DELETE_SIZE = 400;

// DRY_RUN=1 のときは削除せずログだけ出す
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";

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

function normalizeString(v: unknown): string {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

async function main() {
  const db = initFirebaseAdmin();

  const colRef = db.collection(COLLECTION_NAME);

  let lastDoc: FirebaseFirestore.QueryDocumentSnapshot | null = null;

  let scanned = 0;
  let candidates = 0;
  let deleted = 0;

  let batch = db.batch();
  let batchCount = 0;

  console.log(
    `🔍 Scan start: collection="${COLLECTION_NAME}", pageSize=${PAGE_SIZE}, batchDeleteSize=${BATCH_DELETE_SIZE}, DRY_RUN=${DRY_RUN}`,
  );

  while (true) {
    let query = colRef.orderBy(admin.firestore.FieldPath.documentId()).limit(
      PAGE_SIZE,
    );
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snap = await query.get();
    if (snap.empty) {
      break;
    }

    for (const doc of snap.docs) {
      scanned += 1;

      const data = doc.data();
      const name = normalizeString((data as any).name);
      const corporateNumber = normalizeString((data as any).corporateNumber);
      const companyUrl = normalizeString(
        (data as any).companyUrl ??
          (data as any).companyurl ??
          (data as any).url,
      );

      const hasKey = !!name || !!corporateNumber;
      const hasUrl = !!companyUrl;

      // 「name も corporateNumber も無いのに URL だけある」ものをゴミdocとみなす
      if (!hasKey && hasUrl) {
        candidates += 1;

        if (DRY_RUN) {
          console.log(
            `🗑️ [candidate] docId=${doc.id} (name/corporateNumber missing, companyUrl=${companyUrl})`,
          );
        } else {
          batch.delete(doc.ref);
          batchCount += 1;

          if (batchCount >= BATCH_DELETE_SIZE) {
            await batch.commit();
            deleted += batchCount;
            console.log(
              `💾 Committed delete batch: ${batchCount} docs (total deleted: ${deleted}, scanned: ${scanned})`,
            );
            batch = db.batch();
            batchCount = 0;
          }
        }
      }

      if (scanned % 10000 === 0) {
        console.log(
          `📦 scanning... scanned=${scanned}, candidates=${candidates}, deleted=${deleted}`,
        );
      }
    }

    lastDoc = snap.docs[snap.docs.length - 1];
  }

  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    deleted += batchCount;
    console.log(
      `💾 Committed final delete batch: ${batchCount} docs (total deleted: ${deleted})`,
    );
  }

  console.log("✅ Cleanup finished");
  console.log(`  🔍 scanned docs : ${scanned}`);
  console.log(`  🗑️ candidates   : ${candidates}`);
  console.log(`  ❌ deleted      : ${deleted} (DRY_RUN=${DRY_RUN})`);
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});