/* 
  重複しているCSVファイルから企業情報を削除するスクリプト
  
  36.csv, 37.csv, 38.csv, 42.csv, 107.csv, 108.csv, 109.csv, 110.csvは全て同じ企業情報
  108.csvを正として、他のCSVから同じ企業情報を削除します
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/delete_duplicate_companies.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference, WriteBatch } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";

// 正とするCSVファイル（108.csv）
const SOURCE_CSV = "108.csv";

// 削除対象のCSVファイル
const TARGET_CSV_FILES = ["36.csv", "37.csv", "38.csv", "42.csv", "107.csv", "109.csv", "110.csv"];

const DRY_RUN = process.argv.includes("--dry-run");

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

const BATCH_LIMIT = 500;

// CSVから企業情報を抽出（企業名、法人番号、住所、電話番号）
interface CompanyInfo {
  name: string;
  corporateNumber?: string;
  address?: string;
  phoneNumber?: string;
}

function extractCompaniesFromCSV(filePath: string): CompanyInfo[] {
  const companies: CompanyInfo[] = [];
  
  try {
    const csvContent = fs.readFileSync(filePath, "utf8");
    const records: Record<string, string>[] = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
      relax_quotes: true,
      skip_records_with_error: true,
    });

    for (const record of records) {
      const name = record["会社名"] || record["企業名"] || record["name"] || "";
      if (!name || !name.trim()) continue;

      const corporateNumber = record["法人番号"] || record["corporatenumber"] || record["corporate_number"] || "";
      const address = record["住所"] || record["会社住所"] || record["本社住所"] || record["address"] || "";
      const phoneNumber = record["電話番号(窓口)"] || record["電話番号"] || record["phone"] || record["phoneNumber"] || "";

      companies.push({
        name: name.trim(),
        corporateNumber: corporateNumber.trim() || undefined,
        address: address.trim() || undefined,
        phoneNumber: phoneNumber.trim() || undefined,
      });
    }
  } catch (err: any) {
    console.error(`  ❌ CSV読み込みエラー (${filePath}): ${err.message}`);
  }
  
  return companies;
}

// 企業情報でドキュメントを検索して削除
async function deleteCompaniesByInfo(
  companies: CompanyInfo[]
): Promise<{ deletedCount: number; batch: WriteBatch; batchCount: number }> {
  let deletedCount = 0;
  const processed = new Set<string>();
  let batch: WriteBatch = db.batch();
  let batchCount = 0;

  for (const company of companies) {
    const key = `${company.name}|${company.corporateNumber || ""}|${company.address || ""}`;
    if (processed.has(key)) continue;
    processed.add(key);

    try {
      // 企業名で検索
      let query = companiesCol.where("name", "==", company.name) as any;

      const snapshot = await query.get();

      if (!snapshot.empty) {
        for (const doc of snapshot.docs) {
          const data = doc.data();
          
          // 法人番号で確認
          let matches = false;
          if (company.corporateNumber) {
            const docCorpNum = data.corporateNumber;
            // 文字列と数値の両方をチェック
            const csvCorpNum = company.corporateNumber.trim();
            if (docCorpNum && (
              String(docCorpNum).trim() === csvCorpNum ||
              String(docCorpNum).trim() === csvCorpNum.replace(/^0+/, "") ||
              String(docCorpNum).trim().replace(/^0+/, "") === csvCorpNum
            )) {
              matches = true;
            }
          } else if (company.address) {
            // 法人番号がない場合、住所で確認
            const docAddress = (data.address || data.headquartersAddress || "").trim();
            const csvAddress = company.address.trim();
            if (docAddress && csvAddress && 
                docAddress.replace(/\s+/g, "") === csvAddress.replace(/\s+/g, "")) {
              matches = true;
            }
          } else {
            // 法人番号も住所もない場合、企業名のみでマッチ
            matches = true;
          }

          if (matches) {
            if (DRY_RUN) {
              console.log(`  [DRY RUN] 削除予定: ${doc.id} - ${company.name}`);
            } else {
              batch.delete(doc.ref);
              batchCount++;
              deletedCount++;

              if (batchCount >= BATCH_LIMIT) {
                await batch.commit();
                console.log(`  ✅ バッチ削除: ${BATCH_LIMIT}件 (累計: ${deletedCount}件)`);
                batch = db.batch();
                batchCount = 0;
              }
            }
          }
        }
      }
    } catch (err: any) {
      console.error(`  ⚠️  削除エラー (${company.name}): ${err.message}`);
    }
  }

  return { deletedCount, batch, batchCount };
}

// メイン処理
async function main() {
  console.log("🗑️  重複しているCSVファイルから企業情報を削除します\n");
  console.log(`📋 正とするCSV: ${SOURCE_CSV}`);
  console.log(`📋 削除対象CSV: ${TARGET_CSV_FILES.join(", ")}\n`);

  if (DRY_RUN) {
    console.log("⚠️  DRY RUN モード: 実際の削除は行いません\n");
  }

  // 1. 削除対象CSVファイルから企業情報を抽出
  const allCompanies: CompanyInfo[] = [];
  
  for (const fileName of TARGET_CSV_FILES) {
    const filePath = path.join(process.cwd(), "csv", fileName);
    
    if (!fs.existsSync(filePath)) {
      console.log(`⚠️  ${fileName} が見つかりません。スキップします。`);
      continue;
    }

    console.log(`📄 ${fileName} から企業情報を抽出中...`);
    
    const companies = extractCompaniesFromCSV(filePath);
    allCompanies.push(...companies);
    
    console.log(`  ✅ ${companies.length}件の企業情報を抽出`);
  }

  console.log(`\n📊 合計: ${allCompanies.length}件の企業情報を抽出しました\n`);

  if (allCompanies.length === 0) {
    console.log("⚠️  削除対象の企業が見つかりませんでした");
    return;
  }

  // 2. 確認
  console.log("=".repeat(80));
  console.log("⚠️  以下の企業情報を削除します:");
  console.log("=".repeat(80));
  const companyArray = allCompanies.slice(0, 20);
  companyArray.forEach((company, i) => {
    console.log(`  ${i + 1}. ${company.name}${company.corporateNumber ? ` (法人番号: ${company.corporateNumber})` : ""}`);
  });
  if (allCompanies.length > 20) {
    console.log(`  ... 他 ${allCompanies.length - 20}件`);
  }
  console.log("=".repeat(80));
  console.log(`\n合計: ${allCompanies.length}件の企業情報に対応するドキュメントを削除します\n`);

  // 3. 削除実行
  console.log("\n🗑️  削除を開始します...\n");

  const result = await deleteCompaniesByInfo(allCompanies);

  // 残りのバッチをコミット
  if (!DRY_RUN && result.batchCount > 0) {
    await result.batch.commit();
    console.log(`  ✅ 最終バッチ削除: ${result.batchCount}件`);
  }

  console.log("\n" + "=".repeat(80));
  if (DRY_RUN) {
    console.log(`✅ [DRY RUN] 削除予定: ${result.deletedCount}件のドキュメント`);
  } else {
    console.log(`✅ 削除完了: ${result.deletedCount}件のドキュメントを削除しました`);
  }
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});
