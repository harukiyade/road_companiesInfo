/**
 * 上場企業のフィールド構成を確認するスクリプト
 */

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";

const COLLECTION_NAME = "companies_new";

// Firebase 初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      "./serviceAccountKey.json",
      "./service-account-key.json",
    ];
    for (const p of defaultPaths) {
      const resolved = path.resolve(p);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        break;
      }
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ サービスアカウント JSON のパスを指定してください");
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
}

const db = admin.firestore();

async function main() {
  console.log("🔍 上場企業のフィールド構成を確認します...\n");

  // 上場企業を取得
  const listedQuery = await db
    .collection(COLLECTION_NAME)
    .where("listing", "==", "上場")
    .limit(20)
    .get();

  console.log(`📊 上場企業: ${listedQuery.size} 件取得\n`);

  const allFields = new Set<string>();
  const fieldValues: Record<string, Set<string>> = {};

  console.log("=".repeat(80));
  console.log("上場企業のサンプル:");
  console.log("=".repeat(80));

  for (const doc of listedQuery.docs) {
    const data = doc.data();
    Object.keys(data).forEach((key) => allFields.add(key));

    // transactionType, needs, securityCode の値を記録
    ["transactionType", "needs", "securityCode", "listing", "name"].forEach((field) => {
      if (!fieldValues[field]) {
        fieldValues[field] = new Set();
      }
      const value = data[field];
      if (value !== null && value !== undefined) {
        fieldValues[field].add(String(value));
      }
    });

    console.log(`\nDoc ID: ${doc.id}`);
    console.log(`  name: ${data.name || "null"}`);
    console.log(`  transactionType: ${data.transactionType || "null"}`);
    console.log(`  needs: ${data.needs || "null"}`);
    console.log(`  securityCode: ${data.securityCode || "null"}`);
    console.log(`  listing: ${data.listing || "null"}`);
  }

  console.log("\n" + "=".repeat(80));
  console.log("フィールド値の分布:");
  console.log("=".repeat(80));
  for (const [field, values] of Object.entries(fieldValues)) {
    console.log(`\n${field}:`);
    if (values.size === 0) {
      console.log("  (全て null)");
    } else {
      Array.from(values).slice(0, 10).forEach((v) => console.log(`  - ${v}`));
      if (values.size > 10) {
        console.log(`  ... 他 ${values.size - 10} 件`);
      }
    }
  }

  console.log("\n" + "=".repeat(80));
  console.log(`全フィールド一覧 (${allFields.size} フィールド):`);
  console.log("=".repeat(80));
  Array.from(allFields)
    .sort()
    .forEach((field) => {
      const isNewField = ["transactionType", "needs", "securityCode"].includes(field);
      const prefix = isNewField ? "✨ " : "   ";
      console.log(`${prefix}${field}`);
    });
}

main()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("❌ エラー:", err);
    process.exit(1);
  });

