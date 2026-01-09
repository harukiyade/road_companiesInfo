import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const DOC_ID = "1764468991341001563";

function initAdmin() {
  if (admin.apps.length > 0) {
    return;
  }

  const serviceAccountPath =
    process.env.GOOGLE_APPLICATION_CREDENTIALS ||
    path.join(__dirname, "../serviceAccountKey.json");

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(`❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: serviceAccount.project_id,
  });
}

async function main() {
  initAdmin();
  const db = admin.firestore();
  const doc = await db.collection("companies_new").doc(DOC_ID).get();

  if (doc.exists) {
    const data = doc.data();
    console.log("企業名:", data?.name);
    console.log("住所:", data?.address);
    console.log("郵便番号:", data?.postalCode);
    console.log("法人番号:", data?.corporateNumber);
    console.log("\n検索用情報:");
    console.log(`企業名: ${data?.name || ""}`);
  } else {
    console.log("ドキュメントが見つかりません");
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
