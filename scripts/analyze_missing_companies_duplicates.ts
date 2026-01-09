/* 
  グループ6-30の未インポート企業が重複（既存）なのか、本当に未インポートなのかを調査するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
    npx ts-node scripts/analyze_missing_companies_duplicates.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_DIR = path.join(process.cwd(), "csv");

// グループ1-5のファイル（除外対象）
const GROUP1_FILES = ["1.csv", "103.csv", "126.csv", "2.csv", "53.csv"];
const GROUP2_FILES = ["3.csv", "4.csv", "5.csv", "6.csv"];
const GROUP3_FILES = [
  "10.csv", "101.csv", "11.csv", "12.csv", "13.csv", "14.csv", "15.csv", "16.csv", "17.csv", "18.csv",
  "19.csv", "20.csv", "21.csv", "22.csv", "25.csv", "26.csv", "27.csv", "28.csv", "29.csv", "30.csv",
  "31.csv", "32.csv", "33.csv", "34.csv", "35.csv", "39.csv", "52.csv", "54.csv", "55.csv", "56.csv",
  "57.csv", "58.csv", "59.csv", "60.csv", "61.csv", "62.csv", "63.csv", "64.csv", "65.csv", "66.csv",
  "67.csv", "68.csv", "69.csv", "7.csv", "70.csv", "71.csv", "72.csv", "73.csv", "74.csv", "75.csv",
  "76.csv", "77.csv", "8.csv", "9.csv"
];
const GROUP4_FILES = [
  "102.csv", "23.csv", "78.csv", "79.csv", "80.csv", "81.csv", "82.csv", "83.csv", "84.csv", "85.csv",
  "86.csv", "87.csv", "88.csv", "89.csv", "90.csv", "91.csv", "92.csv", "93.csv", "94.csv", "95.csv",
  "96.csv", "97.csv", "98.csv", "99.csv"
];
const GROUP5_FILES = ["133.csv", "134.csv", "24.csv", "40.csv", "41.csv"];

const EXCLUDED_FILES = new Set([
  ...GROUP1_FILES,
  ...GROUP2_FILES,
  ...GROUP3_FILES,
  ...GROUP4_FILES,
  ...GROUP5_FILES,
  "128.csv",
  "129.csv"
]);

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
        break;
      }
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
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
// ヘルパー関数
// ==============================
function normalizeString(str: string | null | undefined): string {
  if (!str) return "";
  return str.trim().replace(/\s+/g, " ").replace(/[　]/g, " ");
}

function normalizeForSearch(str: string): string {
  return normalizeString(str)
    .replace(/株式会社/g, "")
    .replace(/有限会社/g, "")
    .replace(/合資会社/g, "")
    .replace(/合名会社/g, "")
    .replace(/合同会社/g, "")
    .replace(/（/g, "")
    .replace(/）/g, "")
    .replace(/\(/g, "")
    .replace(/\)/g, "")
    .replace(/[都道府県市区町村]/g, "")
    .trim();
}

// ==============================
// CSVファイルから企業情報を読み込む
// ==============================
function readCompaniesFromCsv(csvPath: string): Array<{ 
  name: string; 
  address: string;
  corporateNumber?: string;
}> {
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

    const companies: Array<{ name: string; address: string; corporateNumber?: string }> = [];

    for (const record of records) {
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

      const corporateNumber = normalizeString(
        record["法人番号"] ||
        record["corporateNumber"] ||
        record["CorporateNumber"]
      );

      if (name && address) {
        companies.push({ 
          name, 
          address,
          corporateNumber: corporateNumber || undefined
        });
      }
    }

    return companies;
  } catch (err: any) {
    console.error(`❌ CSV読み込みエラー (${csvPath}): ${err.message}`);
    return [];
  }
}

// ==============================
// Firestoreで企業を検索（複数の方法で）
// ==============================
async function findCompanyInFirestore(
  db: Firestore,
  name: string,
  address: string,
  corporateNumber?: string
): Promise<{
  found: boolean;
  matchType: "exact" | "name_only" | "corporate_number" | "normalized_name" | "none";
}> {
  try {
    const normalizedName = normalizeString(name);
    const normalizedAddress = normalizeString(address);
    const normalizedNameForSearch = normalizeForSearch(name);

    // 1. 法人番号で検索
    if (corporateNumber) {
      const corpNum = corporateNumber.replace(/[^0-9]/g, "");
      if (corpNum.length === 13) {
        const corpQuery = db
          .collection(COLLECTION_NAME)
          .where("corporateNumber", "==", corpNum)
          .limit(1);
        const corpSnapshot = await corpQuery.get();
        if (!corpSnapshot.empty) {
          return { found: true, matchType: "corporate_number" };
        }
      }
    }

    // 2. 企業名と住所の完全一致
    const nameQuery = db
      .collection(COLLECTION_NAME)
      .where("name", "==", normalizedName)
      .limit(100);

    const nameSnapshot = await nameQuery.get();

    if (!nameSnapshot.empty) {
      for (const doc of nameSnapshot.docs) {
        const data = doc.data();
        const docAddress = normalizeString(data.address);
        
        if (docAddress === normalizedAddress) {
          return { found: true, matchType: "exact" };
        }
      }

      // 3. 企業名のみ一致（住所が異なる可能性）
      return { found: true, matchType: "name_only" };
    }

    // 4. 正規化した企業名で検索（部分一致的な検索）
    const allDocs = await db.collection(COLLECTION_NAME).limit(1000).get();
    for (const doc of allDocs.docs) {
      const data = doc.data();
      const docName = normalizeString(data.name);
      const docNameForSearch = normalizeForSearch(docName);
      
      if (docNameForSearch === normalizedNameForSearch && normalizedNameForSearch.length > 3) {
        return { found: true, matchType: "normalized_name" };
      }
    }

    return { found: false, matchType: "none" };
  } catch (err: any) {
    return { found: false, matchType: "none" };
  }
}

// ==============================
// メイン処理
// ==============================
async function main() {
  console.log("🔍 グループ6-30の未インポート企業が重複（既存）なのかを調査します...\n");

  const db = initFirebase();

  // CSVディレクトリ内のファイル一覧を取得
  const allCsvFiles = fs
    .readdirSync(CSV_DIR)
    .filter((file) => file.endsWith(".csv") && !file.startsWith("old"))
    .sort();

  // グループ6-30のファイルを抽出（グループ1-5と128,129を除外）
  const groups6to30Files = allCsvFiles.filter(
    (file) => !EXCLUDED_FILES.has(file)
  );

  console.log(`📁 グループ6-30のCSVファイル数: ${groups6to30Files.length}\n`);

  const results: Array<{
    file: string;
    totalCount: number;
    exactMatch: number;
    nameOnlyMatch: number;
    corporateNumberMatch: number;
    normalizedNameMatch: number;
    trulyMissing: number;
    sampleSize: number;
  }> = [];

  for (let i = 0; i < groups6to30Files.length; i++) {
    const csvFile = groups6to30Files[i];
    const csvPath = path.join(CSV_DIR, csvFile);

    if (!fs.existsSync(csvPath)) {
      continue;
    }

    console.log(`[${i + 1}/${groups6to30Files.length}] 処理中: ${csvFile}`);

    const companies = readCompaniesFromCsv(csvPath);

    if (companies.length === 0) {
      console.log(`   ⚠️  企業データが見つかりませんでした\n`);
      results.push({
        file: csvFile,
        totalCount: 0,
        exactMatch: 0,
        nameOnlyMatch: 0,
        corporateNumberMatch: 0,
        normalizedNameMatch: 0,
        trulyMissing: 0,
        sampleSize: 0,
      });
      continue;
    }

    // サンプルチェック（最大200件、または全体の10%）
    const sampleSize = Math.min(200, Math.max(50, Math.floor(companies.length * 0.1)));
    const sampleCompanies = companies.slice(0, sampleSize);

    let exactMatch = 0;
    let nameOnlyMatch = 0;
    let corporateNumberMatch = 0;
    let normalizedNameMatch = 0;
    let trulyMissing = 0;

    for (const company of sampleCompanies) {
      const result = await findCompanyInFirestore(
        db,
        company.name,
        company.address,
        company.corporateNumber
      );

      if (result.found) {
        switch (result.matchType) {
          case "exact":
            exactMatch++;
            break;
          case "name_only":
            nameOnlyMatch++;
            break;
          case "corporate_number":
            corporateNumberMatch++;
            break;
          case "normalized_name":
            normalizedNameMatch++;
            break;
        }
      } else {
        trulyMissing++;
      }
    }

    // 全体への推定
    const exactMatchRatio = exactMatch / sampleSize;
    const nameOnlyMatchRatio = nameOnlyMatch / sampleSize;
    const corporateNumberMatchRatio = corporateNumberMatch / sampleSize;
    const normalizedNameMatchRatio = normalizedNameMatch / sampleSize;
    const trulyMissingRatio = trulyMissing / sampleSize;

    const estimatedExactMatch = Math.round(exactMatchRatio * companies.length);
    const estimatedNameOnlyMatch = Math.round(nameOnlyMatchRatio * companies.length);
    const estimatedCorporateNumberMatch = Math.round(corporateNumberMatchRatio * companies.length);
    const estimatedNormalizedNameMatch = Math.round(normalizedNameMatchRatio * companies.length);
    const estimatedTrulyMissing = Math.round(trulyMissingRatio * companies.length);

    console.log(`   サンプルチェック結果 (${sampleSize}件):`);
    console.log(`     完全一致（企業名+住所）: ${exactMatch}件`);
    console.log(`     企業名のみ一致: ${nameOnlyMatch}件`);
    console.log(`     法人番号一致: ${corporateNumberMatch}件`);
    console.log(`     正規化企業名一致: ${normalizedNameMatch}件`);
    console.log(`     本当に未インポート: ${trulyMissing}件`);
    console.log(`   推定（全体 ${companies.length}件）:`);
    console.log(`     既存（重複）: ${estimatedExactMatch + estimatedNameOnlyMatch + estimatedCorporateNumberMatch + estimatedNormalizedNameMatch}件`);
    console.log(`     未インポート: ${estimatedTrulyMissing}件\n`);

    results.push({
      file: csvFile,
      totalCount: companies.length,
      exactMatch: estimatedExactMatch,
      nameOnlyMatch: estimatedNameOnlyMatch,
      corporateNumberMatch: estimatedCorporateNumberMatch,
      normalizedNameMatch: estimatedNormalizedNameMatch,
      trulyMissing: estimatedTrulyMissing,
      sampleSize,
    });
  }

  // 結果サマリー
  console.log("\n" + "=".repeat(80));
  console.log("📊 重複分析サマリー");
  console.log("=".repeat(80));

  const totalCompanies = results.reduce((sum, r) => sum + r.totalCount, 0);
  const totalExactMatch = results.reduce((sum, r) => sum + r.exactMatch, 0);
  const totalNameOnlyMatch = results.reduce((sum, r) => sum + r.nameOnlyMatch, 0);
  const totalCorporateNumberMatch = results.reduce((sum, r) => sum + r.corporateNumberMatch, 0);
  const totalNormalizedNameMatch = results.reduce((sum, r) => sum + r.normalizedNameMatch, 0);
  const totalTrulyMissing = results.reduce((sum, r) => sum + r.trulyMissing, 0);
  const totalExisting = totalExactMatch + totalNameOnlyMatch + totalCorporateNumberMatch + totalNormalizedNameMatch;

  console.log(`\n📁 対象ファイル数: ${results.length}件`);
  console.log(`📊 総企業数: ${totalCompanies.toLocaleString()}件`);
  console.log(`\n✅ 既存（重複）の可能性:`);
  console.log(`   完全一致（企業名+住所）: ${totalExactMatch.toLocaleString()}件`);
  console.log(`   企業名のみ一致: ${totalNameOnlyMatch.toLocaleString()}件`);
  console.log(`   法人番号一致: ${totalCorporateNumberMatch.toLocaleString()}件`);
  console.log(`   正規化企業名一致: ${totalNormalizedNameMatch.toLocaleString()}件`);
  console.log(`   合計（既存）: ${totalExisting.toLocaleString()}件 (${((totalExisting / totalCompanies) * 100).toFixed(1)}%)`);
  console.log(`\n❌ 本当に未インポート: ${totalTrulyMissing.toLocaleString()}件 (${((totalTrulyMissing / totalCompanies) * 100).toFixed(1)}%)`);

  // 結果をJSONファイルに保存
  const timestamp = Date.now();
  const resultFile = path.join(process.cwd(), `duplicate_analysis_groups_6_to_30_${timestamp}.json`);
  fs.writeFileSync(resultFile, JSON.stringify({
    timestamp: new Date().toISOString(),
    summary: {
      totalFiles: results.length,
      totalCompanies,
      totalExisting,
      totalTrulyMissing,
      existingPercentage: (totalExisting / totalCompanies) * 100,
      trulyMissingPercentage: (totalTrulyMissing / totalCompanies) * 100,
    },
    breakdown: {
      exactMatch: totalExactMatch,
      nameOnlyMatch: totalNameOnlyMatch,
      corporateNumberMatch: totalCorporateNumberMatch,
      normalizedNameMatch: totalNormalizedNameMatch,
    },
    files: results,
  }, null, 2), "utf-8");
  
  console.log(`\n📄 詳細結果をファイルに保存しました: ${resultFile}`);
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
