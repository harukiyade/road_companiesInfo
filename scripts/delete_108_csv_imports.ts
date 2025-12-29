/* 
  108.csvからインポートしたデータを削除するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_108_csv_imports.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import { Firestore, WriteBatch } from "firebase-admin/firestore";

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
  console.log("📄 108.csvからインポートしたデータを削除します\n");

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

  let deletedCount = 0;
  let notFoundCount = 0;
  const processed = new Set<string>();

  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 400;

  for (let i = 0; i < records.length; i++) {
    const row = records[i];
    const rowNumber = i + 2;
    const companyName = row["会社名"]?.trim() || "";

    if (!companyName) {
      continue;
    }

    const key = companyName;
    if (processed.has(key)) {
      continue;
    }
    processed.add(key);

    try {
      // 企業名で検索
      const snap = await companiesCol
        .where("name", "==", companyName)
        .limit(10)
        .get();

      if (!snap.empty) {
        for (const doc of snap.docs) {
          const data = doc.data();
          const address = row["住所"]?.trim() || "";
          const docAddress = (data.address || data.headquartersAddress || "").trim();

          // 住所が一致するか、または住所が空の場合は削除
          if (!address || !docAddress || docAddress.includes(address) || address.includes(docAddress)) {
            batch.delete(doc.ref);
            batchCount++;
            deletedCount++;

            if (deletedCount <= 10) {
              console.log(`🗑️  [行${rowNumber}] 削除予定: ${companyName} (docId: ${doc.id})`);
            }

            if (batchCount >= BATCH_LIMIT) {
              await batch.commit();
              console.log(`  ✅ バッチ削除: ${BATCH_LIMIT}件 (累計: ${deletedCount}件)`);
              batch = db.batch();
              batchCount = 0;
            }
            break; // 最初の一致するドキュメントのみ削除
          }
        }
      } else {
        notFoundCount++;
        if (notFoundCount <= 10) {
          console.log(`⚠️  [行${rowNumber}] 見つかりませんでした: ${companyName}`);
        }
      }
    } catch (err: any) {
      console.error(`⚠️  [行${rowNumber}] エラー: ${err.message}`);
    }
  }

  // 残りのバッチをコミット
  if (batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 最終バッチ削除: ${batchCount}件`);
  }

  console.log("\n" + "=".repeat(80));
  console.log("✅ 削除完了");
  console.log(`   削除: ${deletedCount}件`);
  console.log(`   見つからなかった: ${notFoundCount}件`);
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});
