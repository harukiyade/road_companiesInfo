/* 
  CSV配下の各ファイルがcompanies_newコレクションに作成されているか確認するスクリプト
  
  照合は企業名と住所で行います。
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/verify_csv_in_companies_new.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  QuerySnapshot,
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

  // 環境変数から取得（改行や空白をトリム）
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS
    ? process.env.GOOGLE_APPLICATION_CREDENTIALS.trim().replace(/\n/g, "").replace(/\r/g, "")
    : null;

  // 指定されたパスが存在するか確認
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
      // ユーザーが指定したパスも試す
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
    console.error("   例: export GOOGLE_APPLICATION_CREDENTIALS='/path/to/serviceAccountKey.json'");
    console.error(`   現在の値: ${process.env.GOOGLE_APPLICATION_CREDENTIALS || "(未設定)"}`);
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
function readCompaniesFromCsv(csvPath: string): Array<{ name: string; address: string }> {
  try {
    const content = fs.readFileSync(csvPath, "utf-8");
    const records = parse(content, {
      columns: true,
      skip_empty_lines: true,
      bom: true,
      relax_column_count: true, // カラム数の不一致を許容
      relax_quotes: true,
      skip_records_with_error: true, // エラーのある行をスキップ
    }) as Array<Record<string, string>>;

    const companies: Array<{ name: string; address: string }> = [];

    for (const record of records) {
      // 様々なカラム名に対応
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
        companies.push({ name, address });
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
  console.log("🔍 CSVファイルとcompanies_newコレクションの照合を開始します...\n");

  const db = initFirebase();
  const companiesCol = db.collection(COLLECTION_NAME);

  // CSVディレクトリ内のファイル一覧を取得
  const csvFiles = fs
    .readdirSync(CSV_DIR)
    .filter((file) => file.endsWith(".csv") && !file.startsWith("old"))
    .sort();

  console.log(`📁 検出されたCSVファイル数: ${csvFiles.length}\n`);

  const missingFiles: string[] = [];
  const partiallyMissingFiles: Array<{ file: string; missingCount: number; totalCount: number; foundCount: number }> = [];
  const csvReadErrors: string[] = [];

  for (let i = 0; i < csvFiles.length; i++) {
    const csvFile = csvFiles[i];
    const csvPath = path.join(CSV_DIR, csvFile);

    console.log(`[${i + 1}/${csvFiles.length}] 処理中: ${csvFile}`);

    const companies = readCompaniesFromCsv(csvPath);

    if (companies.length === 0) {
      console.log(`  ⚠️  企業データが見つかりませんでした\n`);
      csvReadErrors.push(csvFile);
      continue;
    }

    let foundCount = 0;
    let notFoundCount = 0;

    // サンプルとして最初の10件をチェック（全件チェックは時間がかかるため）
    const sampleSize = Math.min(10, companies.length);
    const sampleCompanies = companies.slice(0, sampleSize);

    for (const company of sampleCompanies) {
      const found = await findCompanyInFirestore(db, company.name, company.address);
      if (found) {
        foundCount++;
      } else {
        notFoundCount++;
      }
    }

    console.log(`  サンプルチェック: ${foundCount}/${sampleSize} 件が見つかりました`);

    // サンプルで1件も見つからない場合は、全件チェック
    if (foundCount === 0 && sampleSize > 0) {
      console.log(`  ⚠️  サンプルで見つからなかったため、全件チェックを実行します...`);
      
      foundCount = 0;
      notFoundCount = 0;

      for (const company of companies) {
        const found = await findCompanyInFirestore(db, company.name, company.address);
        if (found) {
          foundCount++;
        } else {
          notFoundCount++;
        }
      }

      console.log(`  全件チェック結果: ${foundCount}/${companies.length} 件が見つかりました`);
    }

    if (foundCount === 0 && companies.length > 0) {
      console.log(`  ❌ このCSVファイルの企業は1件も見つかりませんでした\n`);
      missingFiles.push(csvFile);
    } else if (notFoundCount > 0) {
      console.log(`  ⚠️  一部の企業が見つかりませんでした (見つからない: ${notFoundCount}件)\n`);
      partiallyMissingFiles.push({
        file: csvFile,
        missingCount: notFoundCount,
        totalCount: companies.length,
        foundCount: foundCount,
      });
    } else {
      console.log(`  ✅ すべての企業が見つかりました\n`);
    }
  }

  // 結果サマリー
  console.log("\n" + "=".repeat(60));
  console.log("📊 照合結果サマリー");
  console.log("=".repeat(60));

  // CSV読み込みエラーがある場合
  if (csvReadErrors.length > 0) {
    console.log(`\n❌ CSV読み込みエラーが発生したファイル (${csvReadErrors.length}件):`);
    csvReadErrors.forEach((file) => {
      console.log(`  - ${file}`);
    });
  }

  // 1件も見つからなかったファイル
  if (missingFiles.length > 0) {
    console.log(`\n❌ 1件も見つからなかったCSVファイル (${missingFiles.length}件):`);
    missingFiles.forEach((file) => {
      console.log(`  - ${file}`);
    });
  }

  // 一部が見つからなかったファイル
  if (partiallyMissingFiles.length > 0) {
    console.log(`\n⚠️  一部が見つからなかったCSVファイル (${partiallyMissingFiles.length}件):`);
    partiallyMissingFiles.forEach((item) => {
      const percentage = ((item.foundCount / item.totalCount) * 100).toFixed(1);
      console.log(`  - ${item.file} (見つかった: ${item.foundCount}/${item.totalCount}件, ${percentage}%)`);
    });
  }

  // すべて見つかった場合
  if (missingFiles.length === 0 && partiallyMissingFiles.length === 0 && csvReadErrors.length === 0) {
    console.log("✅ すべてのCSVファイルの企業がcompanies_newコレクションに存在します");
  }

  // 結果をファイルに出力
  const timestamp = Date.now();
  const resultFile = path.join(process.cwd(), `csv_verification_result_${timestamp}.json`);
  const result = {
    timestamp: new Date().toISOString(),
    csvReadErrors,
    missingFiles,
    partiallyMissingFiles,
    totalFiles: csvFiles.length,
  };
  
  fs.writeFileSync(resultFile, JSON.stringify(result, null, 2), "utf-8");
  console.log(`\n📄 詳細結果をファイルに保存しました: ${resultFile}`);

  console.log("\n" + "=".repeat(60));
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
