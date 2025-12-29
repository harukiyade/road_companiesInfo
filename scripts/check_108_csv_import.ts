/* 
  108.csvからインポートしたデータの住所フィールドを確認するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/check_108_csv_import.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_FILE = path.join(process.cwd(), "csv", "108.csv");

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

    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolvedPath}`);
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
const companiesCol = db.collection(COLLECTION_NAME);

// メイン処理
async function main() {
  console.log("📄 108.csvからインポートしたデータの住所フィールドを確認します\n");

  if (!fs.existsSync(CSV_FILE)) {
    console.error(`❌ エラー: ${CSV_FILE} が見つかりません`);
    process.exit(1);
  }

  const content = fs.readFileSync(CSV_FILE, "utf8");
  const records: Record<string, string>[] = parse(content, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_records_with_error: true,
  });

  if (records.length === 0) {
    console.log("❌ CSVに有効なレコードがありません");
    return;
  }

  console.log(`📋 レコード数: ${records.length}\n`);

  // 最初の10件を確認
  const checkCount = Math.min(10, records.length);

  for (let i = 0; i < checkCount; i++) {
    const row = records[i];
    const rowNumber = i + 2;
    const companyName = row["会社名"]?.trim() || "";

    if (!companyName) {
      continue;
    }

    try {
      const snap = await companiesCol
        .where("name", "==", companyName)
        .limit(1)
        .get();

      if (!snap.empty) {
        const doc = snap.docs[0];
        const data = doc.data();
        const csvAddress = row["住所"]?.trim() || "";
        const docAddress = (data.address || data.headquartersAddress || "").trim();

        console.log(`\n${"=".repeat(80)}`);
        console.log(`[行${rowNumber}] ${companyName}`);
        console.log(`docId: ${doc.id}`);
        console.log(`\nCSVの住所: ${csvAddress}`);
        console.log(`Firestoreのaddress: ${docAddress}`);
        console.log(`FirestoreのheadquartersAddress: ${data.headquartersAddress || ""}`);
        
        // 住所が正しく処理されているか確認
        if (csvAddress && docAddress) {
          if (docAddress === csvAddress) {
            console.log(`✅ 住所は正しく処理されています`);
          } else if (docAddress.includes(csvAddress)) {
            console.log(`⚠️  住所に追加の情報が含まれています`);
            console.log(`   追加部分: ${docAddress.substring(csvAddress.length)}`);
          } else {
            console.log(`❌ 住所が一致しません`);
          }
        }
      } else {
        console.log(`\n${"=".repeat(80)}`);
        console.log(`[行${rowNumber}] ${companyName} → 見つかりませんでした`);
      }
    } catch (err: any) {
      console.error(`⚠️  [行${rowNumber}] エラー: ${err.message}`);
    }
  }

  console.log(`\n${"=".repeat(80)}`);
  console.log("✅ 確認完了");
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});
