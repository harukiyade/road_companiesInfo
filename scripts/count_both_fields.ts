// scripts/count_both_fields.ts
//
// overview と companyDescription の両方に値が入っているドキュメント数をカウント

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";
const PAGE_SIZE = 1000;

function initFirebaseAdmin() {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
      path.join(projectRoot, "config", "serviceAccountKey.json"),
      path.join(projectRoot, ".config", "serviceAccountKey.json"),
      "/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
    ];
    
    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolvedPath}`);
        break;
      }
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
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

function norm(s: string | null | undefined): string | null {
  if (!s) return null;
  const trimmed = s.toString().trim();
  return trimmed === "" ? null : trimmed;
}

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  let scanned = 0;
  let countBoth = 0;
  let countOverviewOnly = 0;
  let countCompanyDescriptionOnly = 0;
  let countNeither = 0;
  let lastDoc: FirebaseFirestore.QueryDocumentSnapshot | null = null;

  console.log("🔍 overview/companyDescriptionフィールドの状態をカウントします...\n");

  while (true) {
    let query = colRef.orderBy(admin.firestore.FieldPath.documentId()).limit(PAGE_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snap = await query.get();
    if (snap.empty) break;

    for (const doc of snap.docs) {
      scanned += 1;

      const data = doc.data();
      const overview = norm((data as any).overview);
      const companyDescription = norm((data as any).companyDescription);

      if (overview !== null && companyDescription !== null) {
        countBoth += 1;
      } else if (overview !== null) {
        countOverviewOnly += 1;
      } else if (companyDescription !== null) {
        countCompanyDescriptionOnly += 1;
      } else {
        countNeither += 1;
      }
    }

    lastDoc = snap.docs[snap.docs.length - 1];

    if (scanned % 10000 === 0) {
      console.log(`📦 scanning... scanned=${scanned}, both=${countBoth}, overviewOnly=${countOverviewOnly}, companyDescriptionOnly=${countCompanyDescriptionOnly}, neither=${countNeither}`);
    }
  }

  console.log("\n✅ カウント完了");
  console.log(`  🔍 総スキャン数: ${scanned}`);
  console.log(`  📊 両方に値がある: ${countBoth}`);
  console.log(`  📊 overviewのみ: ${countOverviewOnly}`);
  console.log(`  📊 companyDescriptionのみ: ${countCompanyDescriptionOnly}`);
  console.log(`  📊 両方とも空: ${countNeither}`);
}

main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
