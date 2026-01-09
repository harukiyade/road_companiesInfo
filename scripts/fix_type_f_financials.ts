/* 
  タイプFのcompanies_newコレクションの財務情報フィールドをCSVから再読み込みして更新するスクリプト
  
  対象フィールド:
    - capitalStock (資本金)
    - latestFiscalYearMonth (直近決算年月) / fiscalMonth
    - latestRevenue (直近売上)
    - latestProfit (直近利益)
  
  CSVの財務情報はすでに千単位になっているので、1000倍して実値に変換します。
  ヘッダーと内容がずれていることがあるので、行単位で判断します。
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/fix_type_f_financials.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const BATCH_LIMIT = 500; // Firestoreのバッチ制限

// タイプFのCSVファイルを識別
function isTypeFCSV(filePath: string): boolean {
  const typeFFiles = ["csv/124.csv", "csv/125.csv", "csv/126.csv"];
  return typeFFiles.some(f => filePath.endsWith(f));
}

// タイプFのCSVを行配列として読み込む（列インデックスベース）
function loadTypeFCSVByIndex(csvFilePath: string): Array<Array<string>> {
  const buf = fs.readFileSync(csvFilePath);
  try {
    const records: Array<Array<string>> = parse(buf, {
      columns: false,  // ヘッダーを無視して配列として読み込む
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      skip_records_with_error: true,
    });
    console.log(`  📄 ${path.basename(csvFilePath)}: ${records.length} 行（タイプF: 列順序ベース）`);
    return records;
  } catch (err: any) {
    console.warn(`  ⚠️ ${path.basename(csvFilePath)}: CSVパースエラー - ${err.code || err.message}`);
    return [];
  }
}

// 文字列をトリム
function trim(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim();
}

// 値が数値かどうかを判定（郵便番号判定用）
function isNumericValue(value: string | null | undefined): boolean {
  if (!value) return false;
  const trimmed = String(value).trim();
  if (!trimmed) return false;
  
  // 数値のみ（カンマやハイフンを含む可能性がある）
  const cleaned = trimmed.replace(/[,\-\s]/g, "");
  return /^\d+$/.test(cleaned) && cleaned.length > 0;
}

// 郵便番号を検証（7桁の数値でない場合はnull）
function validatePostalCode(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  
  // 郵便番号形式（XXX-XXXX）を検証
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 7) {
    // 7桁の数字の場合、XXX-XXXX形式に変換
    return digits.replace(/(\d{3})(\d{4})/, "$1-$2");
  }
  
  // 7桁でない場合はnull
  return null;
}

// 数値をパース（カンマや空白を除去）
function parseNumeric(v: string | null | undefined): number | null {
  if (!v) return null;
  const cleaned = String(v).replace(/[,，\s]/g, "");
  if (!cleaned) return null;
  const num = Number(cleaned);
  if (!Number.isFinite(num)) return null;
  return num;
}

// 財務数値を実値に変換（千単位から実値へ）
function parseFinancialNumeric(
  v: string,
  fieldName: string
): number | null {
  const num = parseNumeric(v);
  if (num === null) return null;
  // タイプFは千単位なので1000倍
  return num * 1000;
}

// タイプFの行データから財務情報のみを抽出（mapTypeFRowByIndexと同じロジック）
// 構造: 会社名(0),都道府県(1),代表者名(2),取引種別(3),SBフラグ(4),NDA(5),AD(6),ステータス(7),備考(8),URL(9),業種1(10),業種2(11),業種3(12),郵便番号(13),住所(14),設立(15),電話番号(窓口)(16),代表者郵便番号(17),代表者住所(18),代表者誕生日(19),資本金(20),上場(21),直近決算年月(22),直近売上(23),直近利益(24),...
function extractTypeFRowFinancials(row: Array<string>, filePath: string = ""): {
  companyName: string;
  prefecture: string;
  representativeName: string;
  capitalStock: number | null;
  latestFiscalYearMonth: string | null;
  latestRevenue: number | null;
  latestProfit: number | null;
} {
  let colIndex = 0;
  
  // 0. 会社名
  const companyName = trim(row[colIndex] || "");
  colIndex++;
  
  // 1. 都道府県
  const prefecture = trim(row[colIndex] || "");
  colIndex++;
  
  // 2. 代表者名
  const representativeName = trim(row[colIndex] || "");
  colIndex++;
  
  // 3-8. 取引種別・SBフラグ・NDA・AD・ステータス・備考（スキップ）
  colIndex += 6;
  
  // 9. URL
  colIndex++;
  
  // 10. 業種1
  colIndex++;
  
  // 11. 業種2
  colIndex++;
  
  // 12. 業種3
  colIndex++;
  
  // 13以降: 業種4〜7の処理（動的判定）
  // 業種4の位置をチェック
  const industry4Value = row[colIndex] ? trim(row[colIndex]) : null;
  
  if (industry4Value && isNumericValue(industry4Value)) {
    // 業種4の位置に数値が来た = 業種4〜7はない、これは郵便番号
    validatePostalCode(industry4Value);
    colIndex++;
  } else {
    // 業種4がある（非数値）
    if (industry4Value) {
      // industryDetailとして処理（スキップ）
    }
    colIndex++;
    
    // 業種5の位置をチェック
    const industry5Value = row[colIndex] ? trim(row[colIndex]) : null;
    if (industry5Value && isNumericValue(industry5Value)) {
      // 業種5の位置に数値が来た = 業種5〜7はない、これは郵便番号
      validatePostalCode(industry5Value);
      colIndex++;
    } else {
      // 業種5がある（非数値）
      if (industry5Value) {
        // industryCategoriesとして処理（スキップ）
      }
      colIndex++;
      
      // 業種6の位置をチェック
      const industry6Value = row[colIndex] ? trim(row[colIndex]) : null;
      if (industry6Value && isNumericValue(industry6Value)) {
        // 業種6の位置に郵便番号が来た（業種6〜7はない）
        validatePostalCode(industry6Value);
        colIndex++;
      } else {
        // 業種6がある（非数値）
        if (industry6Value) {
          // industryCategoriesとして処理（スキップ）
        }
        colIndex++;
        
        // 業種7の位置をチェック
        const industry7Value = row[colIndex] ? trim(row[colIndex]) : null;
        if (industry7Value && isNumericValue(industry7Value)) {
          // 業種7の位置に郵便番号が来た（業種7はない）
          validatePostalCode(industry7Value);
          colIndex++;
        } else {
          // 業種7がある（非数値）
          if (industry7Value) {
            // industryCategoriesとして処理（スキップ）
          }
          colIndex++;
          
          // 次の位置が郵便番号
          if (row[colIndex]) {
            validatePostalCode(row[colIndex]);
            colIndex++;
          } else {
            colIndex++;
          }
        }
      }
    }
  }
  
  // 郵便番号がまだ処理されていない場合、現在の位置を確認
  if (row[colIndex]) {
    const postalCode = validatePostalCode(row[colIndex]);
    if (postalCode) {
      colIndex++;
    } else {
      colIndex++;
    }
  } else {
    colIndex++;
  }
  
  // 住所
  colIndex++;
  
  // 設立
  colIndex++;
  
  // 電話番号(窓口)
  colIndex++;
  
  // 代表者郵便番号
  colIndex++;
  
  // 代表者住所
  colIndex++;
  
  // 代表者誕生日
  colIndex++;
  
  // 資本金 (colIndexがこの時点で資本金の位置)
  const capitalStock = row[colIndex] 
    ? parseFinancialNumeric(row[colIndex], "capitalStock")
    : null;
  colIndex++;
  
  // 上場
  colIndex++;
  
  // 直近決算年月 (colIndexがこの時点で直近決算年月の位置)
  const latestFiscalYearMonth = row[colIndex] ? trim(row[colIndex]) : null;
  colIndex++;
  
  // 直近売上 (colIndexがこの時点で直近売上の位置)
  const latestRevenue = row[colIndex]
    ? parseFinancialNumeric(row[colIndex], "latestRevenue")
    : null;
  colIndex++;
  
  // 直近利益 (colIndexがこの時点で直近利益の位置)
  const latestProfit = row[colIndex]
    ? parseFinancialNumeric(row[colIndex], "latestProfit")
    : null;
  colIndex++;
  
  return {
    companyName,
    prefecture,
    representativeName,
    capitalStock,
    latestFiscalYearMonth,
    latestRevenue,
    latestProfit,
  };
}

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

// 企業名を正規化
function normalizeCompanyName(name: string): string {
  return name
    .trim()
    .replace(/[（(].*?[）)]/g, "") // 括弧内を削除
    .replace(/株式会社/g, "")
    .replace(/有限会社/g, "")
    .replace(/合資会社/g, "")
    .replace(/合名会社/g, "")
    .replace(/合同会社/g, "")
    .replace(/\s+/g, "")
    .toLowerCase();
}

// 都道府県を正規化
function normalizePrefecture(pref: string): string {
  return pref.trim().replace(/[都道府県]/g, "");
}

// 文字列を正規化
function normalizeStr(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "");
}

// タイプF用: 会社名・都道府県・代表者名で企業を特定
async function findCompanyByTypeF(
  companyName: string,
  prefecture: string,
  representativeName: string
): Promise<DocumentReference | null> {
  const normName = normalizeCompanyName(companyName);
  const normPref = normalizePrefecture(prefecture);
  const normRep = normalizeCompanyName(representativeName);

  // 1. 会社名で検索
  const nameQuery = await companiesCol
    .where("name", "==", companyName)
    .limit(50)
    .get();

  if (nameQuery.empty) {
    return null;
  }

  // 2. 都道府県と代表者名で絞り込み
  for (const doc of nameQuery.docs) {
    const data = doc.data();
    const docPref = data.prefecture ? normalizePrefecture(String(data.prefecture)) : "";
    const docRep = data.representativeName ? normalizeCompanyName(String(data.representativeName)) : "";

    // 都道府県と代表者名が一致する場合
    if (docPref === normPref && docRep === normRep) {
      return doc.ref;
    }

    // 都道府県のみ一致する場合（代表者名が空の場合）
    if (docPref === normPref && !docRep && normRep) {
      return doc.ref;
    }
  }

  // 3. 会社名のみで一致する場合（最初の候補を返す）
  if (nameQuery.docs.length === 1) {
    return nameQuery.docs[0].ref;
  }

  return null;
}

async function main() {
  console.log("⚠️  本番モード: Firestore を書き換えます\n");

  // タイプFのCSVファイルを読み込み
  console.log("📖 タイプFのCSVファイルを読み込み中...");
  const typeFFiles = ["csv/124.csv", "csv/125.csv", "csv/126.csv"];
  const allRows: Array<{
    file: string;
    rowIndex: number;
    financials: ReturnType<typeof extractTypeFRowFinancials>;
  }> = [];

  for (const file of typeFFiles) {
    const filePath = path.resolve(file);
    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️  ファイルが見つかりません: ${filePath}`);
      continue;
    }

    const records = loadTypeFCSVByIndex(filePath);
    
    // ヘッダー行をスキップ（最初の行）
    for (let i = 1; i < records.length; i++) {
      const row = records[i];
      if (row.length < 20) {
        // 最小限の列数がない場合はスキップ
        continue;
      }
      
      try {
        const financials = extractTypeFRowFinancials(row, filePath);
        // 会社名がない場合はスキップ
        if (!financials.companyName) {
          continue;
        }
        allRows.push({
          file: path.basename(file),
          rowIndex: i + 1,
          financials,
        });
      } catch (err: any) {
        console.warn(`  ⚠️  行 ${i + 1} の処理でエラー: ${err.message}`);
      }
    }
    
    console.log(`  ✅ ${path.basename(file)}: ${records.length - 1} 行を処理`);
  }

  console.log(`\n📊 タイプFの総行数: ${allRows.length} 行\n`);

  // 財務情報を更新
  console.log("🔄 財務情報を更新中...");

  let updatedCount = 0;
  let skippedCount = 0;
  let notFoundCount = 0;
  let batchCount = 0;
  let batch: WriteBatch = db.batch();

  for (const { file, rowIndex, financials } of allRows) {
    try {
      // 企業を特定
      const docRef = await findCompanyByTypeF(
        financials.companyName,
        financials.prefecture,
        financials.representativeName
      );

      if (!docRef) {
        notFoundCount++;
        if (notFoundCount % 100 === 0) {
          console.log(`  ⚠️  企業が見つからない: ${notFoundCount} 件`);
        }
        continue;
      }

      // 更新データを準備
      const updateData: Record<string, any> = {};
      let hasUpdate = false;

      // 資本金
      if (financials.capitalStock !== null) {
        updateData.capitalStock = financials.capitalStock;
        hasUpdate = true;
      }

      // 直近決算年月
      if (financials.latestFiscalYearMonth) {
        updateData.latestFiscalYearMonth = financials.latestFiscalYearMonth;
        // fiscalMonthも更新（互換性のため）
        updateData.fiscalMonth = financials.latestFiscalYearMonth;
        hasUpdate = true;
      }

      // 直近売上
      if (financials.latestRevenue !== null) {
        updateData.latestRevenue = financials.latestRevenue;
        // revenueも更新（互換性のため）
        updateData.revenue = financials.latestRevenue;
        hasUpdate = true;
      }

      // 直近利益
      if (financials.latestProfit !== null) {
        updateData.latestProfit = financials.latestProfit;
        hasUpdate = true;
      }

      if (hasUpdate) {
        updateData.updatedAt = admin.firestore.FieldValue.serverTimestamp();
        batch.update(docRef, updateData);
        updatedCount++;
        batchCount++;

        if (batchCount >= BATCH_LIMIT) {
          await batch.commit();
          console.log(`  💾 バッチコミット (${batchCount} 件) ...`);
          batch = db.batch();
          batchCount = 0;
        }
      } else {
        skippedCount++;
      }
    } catch (error: any) {
      console.error(`  ❌ エラー (${file} 行${rowIndex}, ${financials.companyName}): ${error.message}`);
      skippedCount++;
    }

    if ((updatedCount + skippedCount + notFoundCount) % 500 === 0) {
      console.log(`  進捗: 更新 ${updatedCount} 件、スキップ ${skippedCount} 件、見つからない ${notFoundCount} 件`);
    }
  }

  // 残りのバッチをコミット
  if (batchCount > 0) {
    await batch.commit();
    console.log(`  💾 最後のバッチコミット (${batchCount} 件) ...`);
  }

  console.log(`\n✅ 処理完了`);
  console.log(`   📊 タイプF総行数: ${allRows.length} 行`);
  console.log(`   ✅ 更新件数: ${updatedCount} 件`);
  console.log(`   ⏭️  スキップ件数: ${skippedCount} 件`);
  console.log(`   ❌ 見つからない件数: ${notFoundCount} 件`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

