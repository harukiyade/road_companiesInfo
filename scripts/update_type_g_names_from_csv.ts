/*
  Type Gの会社名をCSVファイル（127.csv、128.csv）から更新するスクリプト
  
  - CSVファイルの会社名欄（「（株）」形式）を読み込み
  - 「（株）」を「株式会社」に正規化
  - 法人番号でマッチングしてDBのnameフィールドを更新
  
  使い方:
    # DRY RUN
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/update_type_g_names_from_csv.ts --dry-run
    
    # 実行
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/update_type_g_names_from_csv.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, WriteBatch } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const CSV_FILES = ["csv/127.csv", "csv/128.csv"];

// Firebase初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: serviceAccount.project_id,
  });

  console.log(`✅ Firebase 初期化完了`);
}

const db: Firestore = admin.firestore();

// 「（株）」を「株式会社」に変換（前株・後株を判定）
function normalizeCompanyNameFormat(name: string | null | undefined): string | null {
  if (!name) return null;
  const trimmed = String(name).trim();
  if (!trimmed) return null;

  // 「（株）」を検出
  if (trimmed.includes("（株）")) {
    // 前株: 「（株）○○」→ 「株式会社○○」
    if (trimmed.startsWith("（株）")) {
      return "株式会社" + trimmed.substring(3).trim();
    }
    // 後株: 「○○（株）」→ 「○○株式会社」
    if (trimmed.endsWith("（株）")) {
      return trimmed.substring(0, trimmed.length - 3).trim() + "株式会社";
    }
    // 中間にある場合も後株として処理
    const index = trimmed.indexOf("（株）");
    if (index > 0) {
      return trimmed.substring(0, index).trim() + "株式会社" + trimmed.substring(index + 3).trim();
    }
  }

  // 既に「株式会社」が含まれている場合はそのまま
  return trimmed;
}

async function processCSVFile(filePath: string): Promise<{ updated: number; notFound: number }> {
  console.log(`\n📄 処理中: ${filePath}`);

  if (!fs.existsSync(filePath)) {
    console.error(`  ❌ ファイルが見つかりません: ${filePath}`);
    return { updated: 0, notFound: 0 };
  }

  const content = fs.readFileSync(filePath, "utf8");
  const records: Record<string, string>[] = parse(content, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_records_with_error: true,
  });

  console.log(`  📊 レコード数: ${records.length} 件`);

  // 会社名と法人番号の列名を探す
  const companyNameKey = Object.keys(records[0]).find(
    key => key === "会社名" || key.toLowerCase() === "companyname" || key.toLowerCase() === "company_name"
  );
  const corporateNumberKey = Object.keys(records[0]).find(
    key => key === "法人番号" || key.toLowerCase() === "corporatenumber" || key.toLowerCase() === "corporate_number"
  );

  if (!companyNameKey || !corporateNumberKey) {
    console.error(`  ❌ 「会社名」または「法人番号」列が見つかりません`);
    return { updated: 0, notFound: 0 };
  }

  console.log(`  🔍 会社名列: "${companyNameKey}"`);
  console.log(`  🔍 法人番号列: "${corporateNumberKey}"`);

  let updatedCount = 0;
  let notFoundCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 400;

  for (let i = 0; i < records.length; i++) {
    const row = records[i];
    const corporateNumber = row[corporateNumberKey]?.trim();
    const companyName = row[companyNameKey]?.trim();

    if (!corporateNumber || !companyName) {
      continue;
    }

    // 「日経バリューサーチ」はスキップ
    if (companyName === "日経バリューサーチ" || companyName.includes("日経バリューサーチ")) {
      continue;
    }

    // 「（株）」を「株式会社」に正規化
    const normalizedName = normalizeCompanyNameFormat(companyName);
    if (!normalizedName) {
      continue;
    }

    // 法人番号でドキュメントを検索
    try {
      const docRef = db.collection(COLLECTION_NAME).doc(corporateNumber);
      const docSnap = await docRef.get();

      if (!docSnap.exists) {
        // 法人番号で直接見つからない場合は、corporateNumberフィールドで検索
        const querySnap = await db.collection(COLLECTION_NAME)
          .where("corporateNumber", "==", corporateNumber)
          .where("csvType", "==", "type_g")
          .limit(1)
          .get();

        if (querySnap.empty) {
          notFoundCount++;
          if (notFoundCount <= 10) {
            console.log(`  ⚠️  法人番号 ${corporateNumber} のドキュメントが見つかりませんでした`);
          }
          continue;
        }

        const doc = querySnap.docs[0];
        const currentName = doc.data().name;

        if (currentName !== normalizedName) {
          if (DRY_RUN) {
            if (updatedCount < 20) {
              console.log(`  📝 [${doc.id}] 更新予定: "${currentName}" → "${normalizedName}"`);
            }
          } else {
            batch.update(doc.ref, {
              name: normalizedName,
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            });
            batchCount++;
          }
          updatedCount++;

          if (batchCount >= BATCH_LIMIT && !DRY_RUN) {
            console.log(`  💾 バッチコミット (${batchCount} 件)…`);
            await batch.commit();
            batch = db.batch();
            batchCount = 0;
          }
        }
      } else {
        // ドキュメントが存在する場合
        const data = docSnap.data();
        if (data?.csvType !== "type_g") {
          continue;
        }

        const currentName = data.name;
        if (currentName !== normalizedName) {
          if (DRY_RUN) {
            if (updatedCount < 20) {
              console.log(`  📝 [${docSnap.id}] 更新予定: "${currentName}" → "${normalizedName}"`);
            }
          } else {
            batch.update(docRef, {
              name: normalizedName,
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            });
            batchCount++;
          }
          updatedCount++;

          if (batchCount >= BATCH_LIMIT && !DRY_RUN) {
            console.log(`  💾 バッチコミット (${batchCount} 件)…`);
            await batch.commit();
            batch = db.batch();
            batchCount = 0;
          }
        }
      }
    } catch (error) {
      console.error(`  ❌ エラー (法人番号: ${corporateNumber}):`, error);
    }

    if ((i + 1) % 100 === 0) {
      console.log(`  📊 処理中: ${i + 1} / ${records.length} 件`);
    }
  }

  // 残りのバッチをコミット
  if (batchCount > 0 && !DRY_RUN) {
    console.log(`  💾 最終バッチコミット (${batchCount} 件)…`);
    await batch.commit();
  }

  console.log(`  ✅ 処理完了: 更新 ${updatedCount} 件、見つからなかった ${notFoundCount} 件`);

  return { updated: updatedCount, notFound: notFoundCount };
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log("📊 CSVファイルからType Gの会社名を更新します\n");

  let totalUpdated = 0;
  let totalNotFound = 0;

  for (const csvFile of CSV_FILES) {
    const result = await processCSVFile(csvFile);
    totalUpdated += result.updated;
    totalNotFound += result.notFound;
  }

  console.log(`\n✅ 全ファイルの処理完了`);
  console.log(`  - 更新: ${totalUpdated} 件`);
  console.log(`  - 見つからなかった: ${totalNotFound} 件`);

  if (DRY_RUN) {
    console.log(`\n💡 実際に保存するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

