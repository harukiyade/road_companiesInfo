/* 
  最近作成されたドキュメントIDを取得するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/get_recently_created_doc_ids.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const TARGET_FILES = [
  "csv/38.csv",
  "csv/107.csv",
  "csv/108.csv",
  "csv/109.csv",
  "csv/110.csv",
  "csv/111.csv",
  "csv/112.csv",
  "csv/113.csv",
  "csv/114.csv",
  "csv/115.csv",
  "csv/116.csv",
  "csv/117.csv",
  "csv/118.csv",
  "csv/119.csv",
  "csv/120.csv",
  "csv/121.csv",
  "csv/122.csv",
  "csv/123.csv",
  "csv/124.csv",
  "csv/125.csv",
];

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
  console.log("🔍 最近作成されたドキュメントIDを取得します\n");
  
  const allDocIds: { csvFile: string; rowNum: number; docId: string; companyName: string }[] = [];
  
  // 各CSVファイルの最初の5行の会社名を取得して検索
  for (const filePath of TARGET_FILES) {
    const fileName = path.basename(filePath);
    console.log(`📄 ${fileName} を処理中...`);
    
    try {
      if (!fs.existsSync(filePath)) {
        console.log(`  ⚠️  ファイルが見つかりません: ${filePath}`);
        continue;
      }
      
      const csvContent = fs.readFileSync(filePath, "utf8");
      const records: Record<string, string>[] = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        relax_column_count: true,
        relax_quotes: true,
        skip_records_with_error: true,
      });
      
      for (let i = 0; i < Math.min(5, records.length); i++) {
        const companyName = records[i]["会社名"] || records[i]["企業名"] || records[i]["name"] || "";
        if (!companyName || !companyName.trim()) continue;
        
        const trimmedName = companyName.trim();
        
        // 会社名で検索（インデックス不要の方法）
        const snapshot = await companiesCol
          .where("name", "==", trimmedName)
          .limit(10)
          .get();
        
        if (!snapshot.empty) {
          // 最新のcreatedAtを持つドキュメントを選択
          let latestDoc = snapshot.docs[0];
          let latestTime = latestDoc.data().createdAt?.toMillis() || 0;
          
          for (const doc of snapshot.docs) {
            const data = doc.data();
            const createdAt = data.createdAt?.toMillis() || 0;
            if (createdAt > latestTime) {
              latestTime = createdAt;
              latestDoc = doc;
            }
          }
          
          allDocIds.push({
            csvFile: fileName,
            rowNum: i + 1,
            docId: latestDoc.id,
            companyName: trimmedName,
          });
          console.log(`  ✅ 行${i + 1}: ${latestDoc.id} (${trimmedName})`);
        } else {
          console.log(`  ⚠️  行${i + 1}: ドキュメントが見つかりませんでした (${trimmedName})`);
        }
      }
    } catch (err: any) {
      console.error(`  ❌ エラー (${fileName}): ${err.message}`);
    }
  }
  
  // 結果を表示
  console.log("\n" + "=".repeat(80));
  console.log("📋 作成されたドキュメントID一覧");
  console.log("=".repeat(80));
  console.log();
  
  // CSVファイルごとにグループ化
  const groupedByFile = new Map<string, typeof allDocIds>();
  for (const item of allDocIds) {
    if (!groupedByFile.has(item.csvFile)) {
      groupedByFile.set(item.csvFile, []);
    }
    groupedByFile.get(item.csvFile)!.push(item);
  }
  
  for (const [file, items] of Array.from(groupedByFile.entries()).sort()) {
    console.log(`📄 ${file} (${items.length}件)`);
    console.log("-".repeat(80));
    for (const item of items) {
      console.log(`  行${item.rowNum}: ${item.docId} (${item.companyName})`);
    }
    console.log();
  }
  
  // 結果をファイルに保存
  const timestamp = Date.now();
  const outputFile = `created_doc_ids_${timestamp}.txt`;
  const outputContent = allDocIds
    .map(item => `${item.csvFile} - 行${item.rowNum}: ${item.docId} (${item.companyName})`)
    .join("\n");
  fs.writeFileSync(outputFile, outputContent, "utf8");
  
  console.log("=".repeat(80));
  console.log(`✅ 合計: ${allDocIds.length}件のドキュメントIDを取得しました`);
  console.log(`📄 結果ファイル: ${outputFile}`);
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

