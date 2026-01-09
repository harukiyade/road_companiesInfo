/**
 * companies_new のうち、
 * - name が空（null / undefined / ""）
 * - companyName が入っている
 * ドキュメントに対して name = companyName をセットするパッチ。
 *
 * 使い方:
 *   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
 *   npx ts-node scripts/patch_companies_name_from_companyName.ts
 */

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  WriteBatch,
  DocumentSnapshot,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// シンプルな空判定
function isEmpty(v: any): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  return false;
}

// Firebase 初期化（import_companies_from_csv.ts と同じロジックを使ってOKですが、
// ここでは簡略版にしています。必要ならそちらと揃えてください）
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
    for (const p of defaultPaths) {
      const resolved = path.resolve(p);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolved}`);
        break;
      }
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ サービスアカウント JSON のパスを指定してください");
    console.error("   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId =
    serviceAccount.project_id ||
    process.env.GCLOUD_PROJECT ||
    process.env.GCP_PROJECT;

  if (!projectId) {
    console.error("❌ Project ID が取得できませんでした");
    process.exit(1);
  }

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });
  console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
}

const db: Firestore = admin.firestore();
const col: CollectionReference = db.collection(COLLECTION_NAME);

async function main() {
  console.log("🔎 companies_new 全件をスキャンします…");
  const snap = await col.get();
  console.log(`docs: ${snap.size} 件`);

  let patched = 0;
  let skipped = 0;

  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 400;

  const docs: DocumentSnapshot[] = snap.docs;

  for (let i = 0; i < docs.length; i++) {
    const doc = docs[i];
    const data = doc.data() || {};

    const hasName = !isEmpty(data.name);
    const hasCompanyName = !isEmpty(data.companyName);

    // name が空で companyName がある doc だけ修正対象
    if (!hasName && hasCompanyName) {
      batch.update(doc.ref, {
        name: data.companyName,
      });
      patched++;
      batchCount++;

      if (batchCount >= BATCH_LIMIT) {
        console.log(`💾 バッチコミット (${batchCount} 件)…`);
        await batch.commit();
        batch = db.batch();
        batchCount = 0;
      }
    } else {
      skipped++;
    }
  }

  if (batchCount > 0) {
    console.log(`💾 最終バッチコミット (${batchCount} 件)…`);
    await batch.commit();
  }

  console.log("✅ パッチ完了");
  console.log(`  name を補完したドキュメント: ${patched} 件`);
  console.log(`  変更不要だったドキュメント: ${skipped} 件`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});