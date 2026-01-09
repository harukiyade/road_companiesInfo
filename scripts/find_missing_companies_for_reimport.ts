/* 
  CSV配下の各ファイルで、companies_newコレクションに存在しない企業を特定するスクリプト
  
  照合は企業名と住所で行います。
  全件チェックを実行して、再インポートが必要なCSVファイルを特定します。
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/find_missing_companies_for_reimport.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_DIR = path.join(process.cwd(), "csv");

// ==============================
// Firebase 初期化
// ==============================
function initFirebase() {
  if (admin.apps.length > 0) {
    return admin.firestore();
  }

  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS
    ? process.env.GOOGLE_APPLICATION_CREDENTIALS.trim().replace(/\n/g, "").replace(/\r/g, "")
    : null;

  if (serviceAccountPath && !fs.existsSync(serviceAccountPath)) {
    console.error(`❌ エラー: 指定されたサービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
    serviceAccountPath = null;
  }

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
      "/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
    ];

    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        console.log(`ℹ️  サービスアカウントキーを使用: ${resolvedPath}`);
        break;
      }
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    console.error("   環境変数 GOOGLE_APPLICATION_CREDENTIALS を設定してください");
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
      projectId: projectId,
    });

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
  } catch (err: any) {
    console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
    console.error(`   詳細: ${err.message}`);
    process.exit(1);
  }

  return admin.firestore();
}

// ==============================
// 文字列正規化
// ==============================
function normalizeString(str: string | null | undefined): string {
  if (!str) return "";
  return str
    .trim()
    .replace(/\s+/g, " ")
    .replace(/[　]/g, " "); // 全角スペースを半角に
}

// ==============================
// CSVファイルから企業情報を読み込む
// ==============================
function readCompaniesFromCsv(csvPath: string): Array<{ name: string; address: string; rowIndex: number }> {
  try {
    const content = fs.readFileSync(csvPath, "utf-8");
    const records = parse(content, {
      columns: true,
      skip_empty_lines: true,
      bom: true,
      relax_column_count: true,
      relax_quotes: true,
      skip_records_with_error: true,
    }) as Array<Record<string, string>>;

    const companies: Array<{ name: string; address: string; rowIndex: number }> = [];

    for (let i = 0; i < records.length; i++) {
      const record = records[i];
      const name = normalizeString(
        record["会社名"] ||
        record["企業名"] ||
        record["name"] ||
        record["Name"] ||
        record["companyName"] ||
        record["CompanyName"]
      );
      
      const address = normalizeString(
        record["会社住所"] ||
        record["住所"] ||
        record["address"] ||
        record["Address"] ||
        record["本社住所"] ||
        record["所在地"]
      );

      if (name && address) {
        companies.push({ name, address, rowIndex: i + 2 }); // +2 because header is row 1, and 0-indexed
      }
    }

    return companies;
  } catch (err: any) {
    console.error(`❌ CSV読み込みエラー (${csvPath}): ${err.message}`);
    return [];
  }
}

// ==============================
// Firestoreで企業名と住所で検索
// ==============================
async function findCompanyInFirestore(
  db: Firestore,
  name: string,
  address: string
): Promise<boolean> {
  try {
    const normalizedName = normalizeString(name);
    const normalizedAddress = normalizeString(address);

    // 企業名で検索
    const nameQuery = db
      .collection(COLLECTION_NAME)
      .where("name", "==", normalizedName)
      .limit(100);

    const nameSnapshot = await nameQuery.get();

    if (nameSnapshot.empty) {
      return false;
    }

    // 住所も一致するものを探す
    for (const doc of nameSnapshot.docs) {
      const data = doc.data();
      const docAddress = normalizeString(data.address);
      
      if (docAddress === normalizedAddress) {
        return true;
      }
    }

    return false;
  } catch (err: any) {
    console.error(`❌ Firestore検索エラー: ${err.message}`);
    return false;
  }
}

// ==============================
// メイン処理
// ==============================
async function main() {
  console.log("🔍 再インポートが必要な企業を特定します...\n");

  const db = initFirebase();

  // CSVディレクトリ内のファイル一覧を取得
  const csvFiles = fs
    .readdirSync(CSV_DIR)
    .filter((file) => file.endsWith(".csv") && !file.startsWith("old") && file !== "128.csv" && file !== "129.csv")
    .sort();

  console.log(`📁 検出されたCSVファイル数: ${csvFiles.length}\n`);

  const filesNeedingReimport: Array<{
    file: string;
    missingCount: number;
    totalCount: number;
    missingCompanies: Array<{ name: string; address: string; rowIndex: number }>;
  }> = [];

  for (let i = 0; i < csvFiles.length; i++) {
    const csvFile = csvFiles[i];
    const csvPath = path.join(CSV_DIR, csvFile);

    console.log(`[${i + 1}/${csvFiles.length}] 処理中: ${csvFile}`);

    const companies = readCompaniesFromCsv(csvPath);

    if (companies.length === 0) {
      console.log(`  ⚠️  企業データが見つかりませんでした\n`);
      continue;
    }

    console.log(`  全件チェック中... (${companies.length}件)`);

    const missingCompanies: Array<{ name: string; address: string; rowIndex: number }> = [];
    let foundCount = 0;

    // 全件チェック
    for (let j = 0; j < companies.length; j++) {
      const company = companies[j];
      const found = await findCompanyInFirestore(db, company.name, company.address);
      
      if (found) {
        foundCount++;
      } else {
        missingCompanies.push(company);
      }

      // 進捗表示（100件ごと）
      if ((j + 1) % 100 === 0 || j === companies.length - 1) {
        process.stdout.write(`\r  進捗: ${j + 1}/${companies.length}件 (見つかった: ${foundCount}件, 見つからない: ${missingCompanies.length}件)`);
      }
    }
    console.log(); // 改行

    if (missingCompanies.length > 0) {
      console.log(`  ⚠️  ${missingCompanies.length}/${companies.length}件が見つかりませんでした\n`);
      filesNeedingReimport.push({
        file: csvFile,
        missingCount: missingCompanies.length,
        totalCount: companies.length,
        missingCompanies: missingCompanies.slice(0, 10), // 最初の10件のみ保存（サンプル）
      });
    } else {
      console.log(`  ✅ すべての企業が見つかりました\n`);
    }
  }

  // 結果サマリー
  console.log("\n" + "=".repeat(80));
  console.log("📊 再インポートが必要なCSVファイル一覧");
  console.log("=".repeat(80));

  if (filesNeedingReimport.length === 0) {
    console.log("✅ すべてのCSVファイルの企業がcompanies_newコレクションに存在します");
  } else {
    console.log(`\n⚠️  再インポートが必要なCSVファイル: ${filesNeedingReimport.length}件\n`);

    // CSVファイルごとにインポート方法を表示
    for (const item of filesNeedingReimport) {
      const percentage = ((item.missingCount / item.totalCount) * 100).toFixed(1);
      console.log(`📄 ${item.file}`);
      console.log(`   見つからない企業: ${item.missingCount}/${item.totalCount}件 (${percentage}%)`);
      console.log(`   インポートコマンド:`);
      console.log(`   GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \\`);
      console.log(`   npx ts-node scripts/import_companies_from_csv.ts ./csv/${item.file}`);
      console.log();
    }
  }

  // 結果をファイルに出力
  const timestamp = Date.now();
  const resultFile = path.join(process.cwd(), `missing_companies_for_reimport_${timestamp}.json`);
  const result = {
    timestamp: new Date().toISOString(),
    filesNeedingReimport: filesNeedingReimport.map(item => ({
      file: item.file,
      missingCount: item.missingCount,
      totalCount: item.totalCount,
      missingPercentage: ((item.missingCount / item.totalCount) * 100).toFixed(1),
      sampleMissingCompanies: item.missingCompanies,
    })),
    totalFiles: csvFiles.length,
  };
  
  fs.writeFileSync(resultFile, JSON.stringify(result, null, 2), "utf-8");
  console.log(`\n📄 詳細結果をファイルに保存しました: ${resultFile}`);
  console.log("\n" + "=".repeat(80));
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
