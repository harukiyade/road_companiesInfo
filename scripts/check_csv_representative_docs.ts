/* 
  各CSVファイルの代表的なドキュメントを取得するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/check_csv_representative_docs.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";

// 処理対象のファイル
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

// CSVファイルから企業情報を読み込んで、対応するドキュメントを取得
async function getRepresentativeDocsForCSV(csvFilePath: string): Promise<void> {
  const csvFileName = path.basename(csvFilePath);
  console.log(`\n📄 ${csvFileName}`);
  console.log("=".repeat(80));

  try {
    const resolvedPath = path.resolve(csvFilePath);
    
    if (!fs.existsSync(resolvedPath)) {
      console.log(`  ⚠️  ファイルが見つかりません: ${csvFilePath}`);
      return;
    }

    // CSVファイルを読み込む
    const content = fs.readFileSync(resolvedPath, "utf8");
    const records: string[][] = parse(content, {
      columns: false,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      skip_records_with_error: true,
    });

    if (records.length === 0) {
      console.log("  ⚠️  CSVファイルにデータがありません");
      return;
    }

    const headers = records[0];
    console.log(`  📊 CSVヘッダー数: ${headers.length}, データ行数: ${records.length - 1}`);

    // ヘッダーから列インデックスを取得
    const nameIndex = headers.findIndex(h => h === "会社名" || h === "企業名");
    const corporateNumberIndex = headers.findIndex(h => h === "法人番号");
    const prefectureIndex = headers.findIndex(h => h === "都道府県");

    if (nameIndex === -1) {
      console.log("  ⚠️  「会社名」列が見つかりません");
      return;
    }

    // CSVから最初の5行の企業情報を取得
    const sampleCompanies: Array<{ name: string; corporateNumber?: string; prefecture?: string }> = [];
    for (let i = 1; i < Math.min(6, records.length); i++) {
      const row = records[i];
      const name = row[nameIndex] ? String(row[nameIndex]).trim() : "";
      if (name) {
        sampleCompanies.push({
          name,
          corporateNumber: corporateNumberIndex >= 0 && row[corporateNumberIndex] 
            ? String(row[corporateNumberIndex]).trim().replace(/\D/g, "") 
            : undefined,
          prefecture: prefectureIndex >= 0 && row[prefectureIndex] 
            ? String(row[prefectureIndex]).trim() 
            : undefined,
        });
      }
    }

    if (sampleCompanies.length === 0) {
      console.log("  ⚠️  サンプル企業が見つかりません");
      return;
    }

    console.log(`  📊 サンプル企業: ${sampleCompanies.length}件\n`);

    // 各企業のドキュメントを検索
    let foundCount = 0;
    for (let i = 0; i < sampleCompanies.length && foundCount < 5; i++) {
      const company = sampleCompanies[i];
      
      try {
        let snapshot;
        
        // 法人番号で検索（優先）
        if (company.corporateNumber && company.corporateNumber.length === 13) {
          snapshot = await companiesCol
            .where("corporateNumber", "==", company.corporateNumber)
            .limit(1)
            .get();
        }
        
        // 法人番号で見つからない場合、会社名と都道府県で検索
        if (!snapshot || snapshot.empty) {
          if (company.prefecture) {
            snapshot = await companiesCol
              .where("name", "==", company.name)
              .where("prefecture", "==", company.prefecture)
              .limit(1)
              .get();
          }
        }
        
        // それでも見つからない場合、会社名のみで検索
        if (!snapshot || snapshot.empty) {
          snapshot = await companiesCol
            .where("name", "==", company.name)
            .limit(1)
            .get();
        }

        if (snapshot && !snapshot.empty) {
          const doc = snapshot.docs[0];
          const data = doc.data();
          
          console.log(`  [${foundCount + 1}] ドキュメントID: ${doc.id}`);
          console.log(`      CSV会社名: ${company.name}`);
          console.log(`      DB会社名: ${data.name || "(未設定)"}`);
          console.log(`      法人番号: ${data.corporateNumber || "(未設定)"}`);
          console.log(`      都道府県: ${data.prefecture || "(未設定)"}`);
          console.log(`      住所: ${data.address ? data.address.substring(0, 50) + "..." : "(未設定)"}`);
          console.log(`      郵便番号: ${data.postalCode || "(未設定)"}`);
          console.log(`      代表者名: ${data.representativeName || "(未設定)"}`);
          console.log(`      業種: ${data.industry || "(未設定)"}`);
          console.log(`      更新日時: ${data.updatedAt ? data.updatedAt.toDate().toLocaleString("ja-JP") : "(未設定)"}`);
          console.log("");
          foundCount++;
        } else {
          console.log(`  [${i + 1}] CSV会社名: ${company.name} - ❌ ドキュメントが見つかりません`);
        }
      } catch (err: any) {
        console.log(`  [${i + 1}] CSV会社名: ${company.name} - ❌ エラー: ${err.message}`);
      }
    }

    if (foundCount === 0) {
      console.log("  ⚠️  該当するドキュメントが見つかりませんでした");
    }
  } catch (err: any) {
    console.log(`  ❌ エラー: ${err.message}`);
  }
}

// メイン処理
async function main() {
  console.log("🔍 各CSVファイルの代表的なドキュメントを確認します\n");

  for (const filePath of TARGET_FILES) {
    await getRepresentativeDocsForCSV(filePath);
  }

  console.log("\n" + "=".repeat(80));
  console.log("✅ 確認完了");
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

