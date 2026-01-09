// scripts/enrich_restore_data_from_csv.ts
//
// 削除されたドキュメントの復元データをCSVファイルから取得して充実させるスクリプト
//
// 使い方:
//   INPUT_FILE=deleted_documents_from_csv_import.json \
//   OUTPUT_FILE=enriched_restore_data.json \
//   CSV_DIR=csv \
//   npx ts-node scripts/enrich_restore_data_from_csv.ts
//
// Firestore 認証:
//   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/serviceAccountKey.json"

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// 入力ファイル（削除されたドキュメントのリスト）
const INPUT_FILE = process.env.INPUT_FILE || "deleted_documents_from_csv_import.json";

// 出力ファイル（充実させた復元データ）
const OUTPUT_FILE = process.env.OUTPUT_FILE || "enriched_restore_data.json";

// CSVディレクトリ
const CSV_DIR = process.env.CSV_DIR || "csv";

function initFirebaseAdmin() {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  // デフォルトのパスを試す
  if (!serviceAccountPath) {
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

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));

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

// CSVファイルから法人番号でデータを検索
function findDataInCsv(docId: string, csvDir: string): Record<string, any> | null {
  const csvPath = path.resolve(csvDir);
  
  if (!fs.existsSync(csvPath) || !fs.statSync(csvPath).isDirectory()) {
    return null;
  }

  const csvFiles = fs.readdirSync(csvPath).filter((f) => f.endsWith(".csv"));

  // ドキュメントIDが13桁の数値の場合、法人番号として扱う
  const isCorporateNumber = /^\d{13}$/.test(docId);

  for (const csvFile of csvFiles) {
    try {
      const filePath = path.join(csvPath, csvFile);
      const content = fs.readFileSync(filePath, "utf8");
      const records: Record<string, string>[] = parse(content, {
        columns: true,
        skip_empty_lines: true,
        relax_column_count: true,
        relax_quotes: true,
        skip_records_with_error: true,
      });

      for (const record of records) {
        // 法人番号で検索
        if (isCorporateNumber) {
          const corporateNumber = 
            record["法人番号"] || 
            record["corporateNumber"] || 
            record["corporate_number"] ||
            record["法人番号（13桁）"];
          
          if (corporateNumber && corporateNumber.trim().replace(/\D/g, "") === docId) {
            // データをマッピング
            return mapCsvRecordToCompanyData(record);
          }
        }

        // ドキュメントIDが数値の場合、行番号としても検索（最初の数件のみ）
        // これは最後の手段として使用
      }
    } catch (error: any) {
      // CSVファイルの読み込みエラーは無視
      continue;
    }
  }

  return null;
}

// CSVレコードを会社データにマッピング
function mapCsvRecordToCompanyData(record: Record<string, string>): Record<string, any> {
  const data: Record<string, any> = {};

  // 基本的なフィールドマッピング
  const fieldMapping: Record<string, string[]> = {
    name: ["会社名", "companyName", "company_name", "name", "企業名"],
    corporateNumber: ["法人番号", "corporateNumber", "corporate_number", "法人番号（13桁）"],
    companyUrl: ["URL", "url", "companyUrl", "company_url", "企業URL", "HP", "hp"],
    address: ["住所", "address", "本社住所", "所在地"],
    prefecture: ["都道府県", "prefecture", "都道府県名"],
    postalCode: ["郵便番号", "postalCode", "postal_code", "郵便番号（7桁）"],
    phone: ["電話番号", "phone", "phoneNumber", "phone_number", "TEL", "tel"],
    fax: ["FAX", "fax", "faxNumber", "fax_number"],
    email: ["メールアドレス", "email", "e-mail", "Email"],
    representativeName: ["代表者名", "representativeName", "representative_name", "代表取締役"],
    established: ["設立年月日", "established", "設立日"],
    capitalStock: ["資本金", "capitalStock", "capital_stock"],
    employeeCount: ["従業員数", "employeeCount", "employee_count", "従業員数（人）"],
    industry: ["業種", "industry", "業種分類"],
  };

  for (const [targetField, sourceFields] of Object.entries(fieldMapping)) {
    for (const sourceField of sourceFields) {
      if (record[sourceField] && record[sourceField].trim()) {
        data[targetField] = record[sourceField].trim();
        break;
      }
    }
  }

  return data;
}

async function main() {
  console.log(`\n🔍 CSVファイルから削除されたドキュメントのデータを取得中...`);
  console.log(`   入力ファイル: ${INPUT_FILE}`);
  console.log(`   出力ファイル: ${OUTPUT_FILE}`);
  console.log(`   CSVディレクトリ: ${CSV_DIR}\n`);

  // 入力ファイルを読み込む
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`❌ エラー: 入力ファイルが見つかりません: ${INPUT_FILE}`);
    process.exit(1);
  }

  const inputData = JSON.parse(fs.readFileSync(INPUT_FILE, "utf8"));
  const deletedDocIds = inputData.deletedDocIds || [];
  const existingRestoreData = inputData.restoreData || [];

  console.log(`📊 削除されたドキュメントID: ${deletedDocIds.length} 件\n`);

  // 既存の復元データをマップに変換（docIdをキーに）
  const restoreDataMap = new Map<string, any>();
  for (const item of existingRestoreData) {
    if (item.docId) {
      restoreDataMap.set(item.docId, item.data || {});
    }
  }

  let enrichedCount = 0;
  let notFoundCount = 0;

  // 各ドキュメントIDについてCSVからデータを検索
  for (let i = 0; i < deletedDocIds.length; i++) {
    const docId = deletedDocIds[i];
    
    if ((i + 1) % 100 === 0) {
      console.log(`  📦 処理中... ${i + 1}/${deletedDocIds.length} (充実: ${enrichedCount}, 未発見: ${notFoundCount})`);
    }

    const existingData = restoreDataMap.get(docId) || {};
    
    // 既にデータがある場合はスキップ（null以外の値がある場合）
    const hasData = Object.values(existingData).some((v) => v !== null && v !== undefined && v !== "");
    
    if (hasData) {
      continue;
    }

    // CSVからデータを検索
    const csvData = findDataInCsv(docId, CSV_DIR);
    
    if (csvData && Object.keys(csvData).length > 0) {
      // 既存のデータとマージ（CSVデータを優先）
      const mergedData = { ...existingData, ...csvData };
      restoreDataMap.set(docId, mergedData);
      enrichedCount++;
    } else {
      notFoundCount++;
    }
  }

  // 復元データリストを再構築
  const enrichedRestoreData = deletedDocIds.map((docId: string) => ({
    docId,
    data: restoreDataMap.get(docId) || {},
  }));

  // 出力データを作成
  const outputData = {
    ...inputData,
    restoreData: enrichedRestoreData,
    enrichedAt: new Date().toISOString(),
    enrichedCount,
    notFoundCount,
  };

  // 出力ファイルに保存
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(outputData, null, 2), "utf8");

  console.log(`\n✅ 処理完了`);
  console.log(`  📊 総件数: ${deletedDocIds.length} 件`);
  console.log(`  ✅ データを充実させた件数: ${enrichedCount} 件`);
  console.log(`  ❌ CSVで見つからなかった件数: ${notFoundCount} 件`);
  console.log(`\n💾 出力ファイル: ${OUTPUT_FILE}`);
  console.log(`\n💡 次のステップ:`);
  console.log(`   RESTORE_DATA_FILE=${OUTPUT_FILE} npx ts-node scripts/restore_deleted_documents.ts`);
}

main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
