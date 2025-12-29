/* 
  指定されたCSVファイル（38, 107-125）の列ズレを修正してDBを更新するスクリプト
  
  主な機能:
  - 郵便番号の位置に業種が入っている場合を修正
  - 郵便番号は3桁-4桁の数値形式を検証
  - 業種3までしかない場合は業種4・5を自動展開
  - 取引種別,SBフラグ,NDA,AD,ステータス,備考は無視
  - 法人番号が13桁でない場合は無視
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/fix_csv_columns_and_update_db.ts
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

// 無視するフィールド
const IGNORE_FIELDS = new Set([
  "ID",
  "取引種別",
  "SBフラグ",
  "NDA",
  "AD",
  "ステータス",
  "備考",
  "Unnamed: 38",
  "Unnamed: 39",
  "Unnamed: 40",
  "Unnamed: 41",
  "Unnamed: 42",
  "Unnamed: 43",
  "Unnamed: 44",
  "Unnamed: 45",
  "Unnamed: 46",
]);

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

// 文字列のトリム処理
function trim(value: string | null | undefined): string {
  if (!value) return "";
  return String(value).trim();
}

// 郵便番号の検証（3桁-4桁の数値形式）
function isValidPostalCode(value: string | null | undefined): boolean {
  if (!value) return false;
  const trimmed = trim(value);
  if (!trimmed) return false;
  
  // ハイフンを含む形式（XXX-XXXX）
  if (/^\d{3}-\d{4}$/.test(trimmed)) return true;
  
  // ハイフンなしの7桁数値
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 7) return true;
  
  return false;
}

// 郵便番号を正規化（XXX-XXXX形式）
function normalizePostalCode(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = trim(value);
  if (!trimmed) return null;
  
  // 既に正しい形式
  if (/^\d{3}-\d{4}$/.test(trimmed)) return trimmed;
  
  // 7桁の数字に変換
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 7) {
    return `${digits.slice(0, 3)}-${digits.slice(3)}`;
  }
  
  return null;
}

// 法人番号の検証（13桁の数値）
function isValidCorporateNumber(value: string | null | undefined): boolean {
  if (!value) return false;
  const trimmed = trim(value);
  if (!trimmed) return false;
  
  // 指数表記（9.18E+12など）を処理
  if (/^\d+\.\d+E\+\d+$/i.test(trimmed)) {
    try {
      const num = parseFloat(trimmed);
      const digits = Math.floor(num).toString().replace(/\D/g, "");
      return digits.length === 13;
    } catch {
      return false;
    }
  }
  
  // 13桁の数字のみ
  const digits = trimmed.replace(/\D/g, "");
  return digits.length === 13;
}

// 法人番号を正規化
function normalizeCorporateNumber(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = trim(value);
  if (!trimmed) return null;
  
  // 指数表記（9.18E+12など）を処理
  if (/^\d+\.\d+E\+\d+$/i.test(trimmed)) {
    try {
      const num = parseFloat(trimmed);
      const digits = Math.floor(num).toString().replace(/\D/g, "");
      if (digits.length === 13) {
        return digits;
      }
    } catch {
      return null;
    }
  }
  
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 13) {
    return digits;
  }
  
  return null;
}

// 数値パース（カンマや空白を除去）
function parseNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/[,\s]/g, "");
  if (!cleaned) return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

// 財務数値のパース（千円単位を実値に変換）
function parseFinancialNumeric(
  value: string | null | undefined,
  field: string
): number | null {
  if (!value) return null;
  const cleaned = String(value).trim().replace(/[,\s]/g, "");
  if (!cleaned || cleaned === "0" || cleaned === "非上場") return null;
  
  const num = Number(cleaned);
  if (!Number.isFinite(num) || num === 0) return null;
  
  // 財務情報は千円単位なので1000倍
  const financialFields = ["capitalStock", "revenue", "latestProfit", "profit"];
  if (financialFields.includes(field)) {
    return Math.round(num * 1000);
  }
  
  return num;
}

// ヘッダーマッピング定義
type HeaderMapping = {
  csvHeader: string;
  dbField: string;
  isIndustry?: boolean;
  industryIndex?: number;
};

// ファイルごとのヘッダーマッピング定義
function getHeaderMapping(fileName: string): HeaderMapping[] {
  const baseMapping: HeaderMapping[] = [
    { csvHeader: "会社名", dbField: "name" },
    { csvHeader: "都道府県", dbField: "prefecture" },
    { csvHeader: "代表者名", dbField: "representativeName" },
    { csvHeader: "法人番号", dbField: "corporateNumber" },
    { csvHeader: "URL", dbField: "companyUrl" },
    { csvHeader: "業種1", dbField: "industry", isIndustry: true, industryIndex: 0 },
    { csvHeader: "業種2", dbField: "industries", isIndustry: true, industryIndex: 0 },
    { csvHeader: "業種3", dbField: "industries", isIndustry: true, industryIndex: 1 },
    { csvHeader: "業種4", dbField: "industries", isIndustry: true, industryIndex: 2 },
    { csvHeader: "業種（細）", dbField: "industryDetail" },
    { csvHeader: "郵便番号", dbField: "postalCode" },
    { csvHeader: "住所", dbField: "address" },
    { csvHeader: "設立", dbField: "established" },
    { csvHeader: "電話番号(窓口)", dbField: "phoneNumber" },
    { csvHeader: "代表者郵便番号", dbField: "representativeRegisteredAddress" },
    { csvHeader: "代表者住所", dbField: "representativeHomeAddress" },
    { csvHeader: "代表者誕生日", dbField: "representativeBirthDate" },
    { csvHeader: "資本金", dbField: "capitalStock" },
    { csvHeader: "上場", dbField: "listing" },
    { csvHeader: "直近決算年月", dbField: "fiscalMonth" },
    { csvHeader: "直近売上", dbField: "revenue" },
    { csvHeader: "直近利益", dbField: "latestProfit" },
    { csvHeader: "説明", dbField: "companyDescription" },
    { csvHeader: "概要", dbField: "overview" },
    { csvHeader: "仕入れ先", dbField: "suppliers" },
    { csvHeader: "取引先", dbField: "clients" },
    { csvHeader: "取引先銀行", dbField: "banks" },
    { csvHeader: "取締役", dbField: "executives" },
    { csvHeader: "株主", dbField: "shareholders" },
    { csvHeader: "社員数", dbField: "employeeCount" },
    { csvHeader: "オフィス数", dbField: "officeCount" },
    { csvHeader: "工場数", dbField: "factoryCount" },
    { csvHeader: "店舗数", dbField: "storeCount" },
  ];

  return baseMapping;
}

// 行データをマッピング（列ズレ対応）
function mapRowData(
  row: string[],
  headers: string[],
  fileName: string
): Record<string, any> {
  const result: Record<string, any> = {
    industries: [],
  };

  const headerMapping = getHeaderMapping(fileName);
  const headerIndexMap = new Map<string, number>();
  
  // ヘッダーのインデックスマップを作成
  headers.forEach((header, index) => {
    if (!IGNORE_FIELDS.has(header)) {
      headerIndexMap.set(header, index);
    }
  });

  // 郵便番号の位置を特定
  let postalCodeIndex = -1;
  for (let i = 0; i < headers.length; i++) {
    if (headers[i] === "郵便番号") {
      postalCodeIndex = i;
      break;
    }
  }

  // 業種の最大インデックスを特定（ヘッダーから）
  let maxIndustryIndex = -1;
  const industryHeaderIndices: number[] = [];
  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    if (header === "業種1") {
      maxIndustryIndex = 0;
      industryHeaderIndices.push(i);
    }
    if (header === "業種2") {
      maxIndustryIndex = 1;
      industryHeaderIndices.push(i);
    }
    if (header === "業種3") {
      maxIndustryIndex = 2;
      industryHeaderIndices.push(i);
    }
    if (header === "業種4") {
      maxIndustryIndex = 3;
      industryHeaderIndices.push(i);
    }
  }

  // 業種（細）の位置を特定
  const industryDetailIndex = headerIndexMap.get("業種（細）");

  // 各マッピングを処理
  for (const mapping of headerMapping) {
    const index = headerIndexMap.get(mapping.csvHeader);
    if (index === undefined) continue;

    let value = row[index] ? trim(row[index]) : null;
    if (!value) continue;

    // 法人番号の検証
    if (mapping.csvHeader === "法人番号") {
      if (!isValidCorporateNumber(value)) {
        value = null; // 無効な法人番号は無視
      } else {
        value = normalizeCorporateNumber(value);
      }
      if (value) {
        result[mapping.dbField] = value;
      }
      continue;
    }

    // 業種の処理
    if (mapping.isIndustry && value) {
      if (mapping.industryIndex !== undefined) {
        // 配列を必要に応じて拡張
        while (result.industries.length <= mapping.industryIndex) {
          result.industries.push(null);
        }
        result.industries[mapping.industryIndex] = value;
      }
      continue;
    }

    // 業種（細）の処理
    if (mapping.csvHeader === "業種（細）" && value) {
      result[mapping.dbField] = value;
      continue;
    }

    // 郵便番号の検証と修正
    if (mapping.csvHeader === "郵便番号") {
      // 郵便番号の位置の値が郵便番号形式でない場合、業種として扱う
      if (!isValidPostalCode(value)) {
        // これは業種として扱う
        const nextIndustryIndex = maxIndustryIndex + 1;
        if (nextIndustryIndex < 5) {
          // 配列を必要に応じて拡張
          while (result.industries.length <= nextIndustryIndex) {
            result.industries.push(null);
          }
          if (!result.industries[nextIndustryIndex]) {
            result.industries[nextIndustryIndex] = value;
          }
        }
        value = null; // 郵便番号はnullにする
      } else {
        value = normalizePostalCode(value);
      }
      if (value) {
        result[mapping.dbField] = value;
      }
      continue;
    }

    // 財務数値の処理
    if (["capitalStock", "revenue", "latestProfit"].includes(mapping.dbField)) {
      const num = parseFinancialNumeric(value, mapping.dbField);
      if (num !== null) {
        result[mapping.dbField] = num;
      }
      continue;
    }

    // 数値フィールドの処理
    if (["employeeCount", "officeCount", "factoryCount", "storeCount"].includes(mapping.dbField)) {
      const num = parseNumeric(value);
      if (num !== null) {
        result[mapping.dbField] = num;
      }
      continue;
    }

    // 文字列フィールドの処理
    if (value) {
      result[mapping.dbField] = value;
    }
  }

  // 郵便番号の位置より前の列で、業種として扱うべき値を検出
  if (postalCodeIndex >= 0) {
    // 業種の開始位置を特定
    const industryStartIndex = headerIndexMap.get("業種1") ?? -1;
    const industryDetailIdx = industryDetailIndex ?? -1;
    
    if (industryStartIndex >= 0) {
      // 業種1から郵便番号の直前までをチェック
      for (let i = industryStartIndex + 1; i < postalCodeIndex; i++) {
        const header = headers[i];
        if (IGNORE_FIELDS.has(header)) continue;
        
        // 既にマッピングされている業種列はスキップ
        if (header.startsWith("業種")) continue;
        
        // 業種（細）の列はスキップ
        if (i === industryDetailIdx) continue;
        
        const value = row[i] ? trim(row[i]) : null;
        if (!value) continue;

        // 郵便番号形式でない場合、業種として扱う
        if (!isValidPostalCode(value)) {
          // 現在の業種の最大インデックスを取得
          let currentMaxIndex = -1;
          for (let j = 0; j < result.industries.length; j++) {
            if (result.industries[j]) {
              currentMaxIndex = j;
            }
          }
          
          // ヘッダーからも確認
          if (maxIndustryIndex > currentMaxIndex) {
            currentMaxIndex = maxIndustryIndex;
          }
          
          const nextIndustryIndex = currentMaxIndex + 1;
          if (nextIndustryIndex < 5) {
            // 配列を必要に応じて拡張
            while (result.industries.length <= nextIndustryIndex) {
              result.industries.push(null);
            }
            if (!result.industries[nextIndustryIndex]) {
              result.industries[nextIndustryIndex] = value;
            }
          }
        }
      }
    }
  }

  // industries配列をクリーンアップ（null/undefinedを除去、空文字列も除去）
  if (result.industries) {
    result.industries = result.industries
      .filter((v: any) => v && trim(v))
      .map((v: any) => trim(v));
  }

  return result;
}

// 企業を検索（法人番号、会社名、都道府県、代表者名で）
async function findCompany(
  data: Record<string, any>
): Promise<DocumentReference | null> {
  // 法人番号で検索
  if (data.corporateNumber) {
    try {
      const snapshot = await companiesCol
        .where("corporateNumber", "==", data.corporateNumber)
        .limit(1)
        .get();
      
      if (!snapshot.empty) {
        return snapshot.docs[0].ref;
      }
    } catch (err) {
      // エラーは無視して次の検索方法を試す
    }
  }

  // 会社名、都道府県、代表者名で検索
  if (data.name && data.prefecture && data.representativeName) {
    try {
      const snapshot = await companiesCol
        .where("name", "==", data.name)
        .where("prefecture", "==", data.prefecture)
        .where("representativeName", "==", data.representativeName)
        .limit(1)
        .get();
      
      if (!snapshot.empty) {
        return snapshot.docs[0].ref;
      }
    } catch (err) {
      // エラーは無視
    }
  }

  // 会社名のみで検索（フォールバック）
  if (data.name) {
    try {
      const snapshot = await companiesCol
        .where("name", "==", data.name)
        .limit(1)
        .get();
      
      if (!snapshot.empty) {
        return snapshot.docs[0].ref;
      }
    } catch (err) {
      // エラーは無視
    }
  }

  return null;
}

// メイン処理
async function main() {
  console.log("🚀 CSVファイルの列ズレ修正とDB更新を開始します\n");

  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalCreated = 0;
  let totalErrors = 0;

  for (const filePath of TARGET_FILES) {
    const resolvedPath = path.resolve(filePath);
    
    if (!fs.existsSync(resolvedPath)) {
      console.log(`⚠️  ファイルが見つかりません: ${filePath}`);
      continue;
    }

    console.log(`\n📄 処理中: ${path.basename(filePath)}`);

    try {
      const content = fs.readFileSync(resolvedPath, "utf8");
      const records: string[][] = parse(content, {
        columns: false,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_records_with_error: true,
      });

      if (records.length === 0) {
        console.log("  ⚠️  データがありません");
        continue;
      }

      const headers = records[0];
      console.log(`  📊 ヘッダー数: ${headers.length}, データ行数: ${records.length - 1}`);

      let batch: WriteBatch = db.batch();
      let batchCount = 0;

      for (let i = 1; i < records.length; i++) {
        const row = records[i];
        
        try {
          const mappedData = mapRowData(row, headers, path.basename(filePath));
          
          // 必須フィールドのチェック
          if (!mappedData.name) {
            console.log(`  ⚠️  行${i + 1}: 会社名がありません`);
            totalErrors++;
            continue;
          }

          totalProcessed++;

          // 既存企業を検索
          const existingRef = await findCompany(mappedData);
          
          const updateData: Record<string, any> = {
            ...mappedData,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          };

          if (existingRef) {
            // 更新
            batch.update(existingRef, updateData);
            totalUpdated++;
          } else {
            // 新規作成
            const newRef = companiesCol.doc();
            batch.set(newRef, {
              ...updateData,
              createdAt: admin.firestore.FieldValue.serverTimestamp(),
            });
            totalCreated++;
          }

          batchCount++;

          // バッチ制限に達したらコミット
          if (batchCount >= BATCH_LIMIT) {
            try {
              await batch.commit();
              console.log(`  ✅ バッチコミット: ${batchCount}件`);
            } catch (err: any) {
              console.log(`  ❌ バッチコミットエラー: ${err.message}`);
            }
            batch = db.batch();
            batchCount = 0;
          }
        } catch (err: any) {
          console.log(`  ❌ 行${i + 1}の処理エラー: ${err.message}`);
          totalErrors++;
        }
      }

      // 残りのバッチをコミット
      if (batchCount > 0) {
        try {
          await batch.commit();
          console.log(`  ✅ 最終バッチコミット: ${batchCount}件`);
        } catch (err: any) {
          console.log(`  ❌ 最終バッチコミットエラー: ${err.message}`);
        }
      }

      console.log(`  ✅ 完了: ${path.basename(filePath)}`);
    } catch (err: any) {
      console.log(`  ❌ エラー: ${err.message}`);
      totalErrors++;
    }
  }

  console.log("\n" + "=".repeat(80));
  console.log("📊 処理結果サマリー");
  console.log("=".repeat(80));
  console.log(`総処理行数: ${totalProcessed}`);
  console.log(`更新件数: ${totalUpdated}`);
  console.log(`新規作成件数: ${totalCreated}`);
  console.log(`エラー件数: ${totalErrors}`);
  console.log("\n✅ すべての処理が完了しました");
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

