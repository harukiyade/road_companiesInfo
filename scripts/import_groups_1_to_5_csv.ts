/* 
  グループ1-5のCSVファイルをインポートするスクリプト
  
  対象グループ:
  - グループ1: 法人番号付き標準フォーマット（5ファイル）
  - グループ2: 取引種別・SBフラグ付きフォーマット（4ファイル）
  - グループ3: 標準フォーマット（54ファイル）
  - グループ4: 創業・株式保有率付きフォーマット（24ファイル）
  - グループ5: 法人番号・業種3つ付きフォーマット（5ファイル）
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
    npx ts-node scripts/import_groups_1_to_5_csv.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_DIR = path.join(process.cwd(), "csv");

// グループ別ファイル定義
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

const ALL_FILES = [...GROUP1_FILES, ...GROUP2_FILES, ...GROUP3_FILES, ...GROUP4_FILES, ...GROUP5_FILES];

// 無視するヘッダー（ID・取引種別・SBフラグ・NDA・AD・ステータス・備考・株式保有率）
const IGNORED_HEADERS = new Set([
  "ID", "id", "取引種別", "SBフラグ", "NDA", "AD", "ステータス", "備考", "株式保有率",
  "取引種別", "sbフラグ", "nda", "ad", "status", "備考", ""
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
  return str.trim();
}

function isEmptyValue(v: any): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  return false;
}

function normalizeHeader(h: string): string {
  return h
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/_/g, "")
    .replace(/[（）()]/g, "");
}

// 法人番号の検証（13桁の数値）
function isValidCorporateNumber(corpNum: string | null | undefined): boolean {
  if (!corpNum) return false;
  const normalized = corpNum.trim().replace(/[^0-9]/g, "");
  return /^[0-9]{13}$/.test(normalized);
}

// 数値IDを生成
function generateNumericDocId(corporateNumber: string | null, index: number): string {
  if (corporateNumber && isValidCorporateNumber(corporateNumber)) {
    return corporateNumber.trim().replace(/[^0-9]/g, "");
  }
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 設立年月日の変換
function parseEstablishedDate(established: string | null | undefined): { established: string | null; foundingYear: number | null } {
  if (!established || isEmptyValue(established)) {
    return { established: null, foundingYear: null };
  }

  const str = String(established).trim();
  const yearMatch = str.match(/(\d{4})年?/);
  if (yearMatch) {
    const year = parseInt(yearMatch[1]);
    if (year >= 1800 && year <= 2100) {
      return { established: str, foundingYear: year };
    }
  }
  
  return { established: str, foundingYear: null };
}

// ヘッダー → フィールド名のマッピング
function buildHeaderToFieldMap(headers: string[]): Record<string, string> {
  const map: Record<string, string> = {};

  // フィールドマッピング定義
  const fieldMappings: Array<{ target: string; aliases: string[] }> = [
    { target: "name", aliases: ["会社名", "企業名", "社名"] },
    { target: "corporateNumber", aliases: ["法人番号"] },
    { target: "phoneNumber", aliases: ["電話番号", "電話番号(窓口)"] },
    { target: "postalCode", aliases: ["会社郵便番号", "郵便番号"] },
    { target: "address", aliases: ["会社住所", "住所"] },
    { target: "companyUrl", aliases: ["URL"] },
    { target: "representativeName", aliases: ["代表者名", "代表者"] },
    { target: "representativeRegisteredAddress", aliases: ["代表者郵便番号"] },
    { target: "representativeHomeAddress", aliases: ["代表者住所", "住所"] },
    { target: "representativeBirthDate", aliases: ["代表者誕生日"] },
    { target: "businessDescriptions", aliases: ["営業種目"] },
    { target: "established", aliases: ["設立"] },
    { target: "shareholders", aliases: ["株主"] },
    { target: "executives", aliases: ["取締役", "役員"] },
    { target: "overview", aliases: ["概況", "概要", "説明"] },
    { target: "industryLarge", aliases: ["業種-大", "業種（大）", "業種1"] },
    { target: "industryMiddle", aliases: ["業種-中", "業種（中）", "業種2"] },
    { target: "industrySmall", aliases: ["業種-小", "業種（小）", "業種3"] },
    { target: "industryDetail", aliases: ["業種-細", "業種（細）"] },
    { target: "prefecture", aliases: ["都道府県"] },
    { target: "capitalStock", aliases: ["資本金"] },
    { target: "listing", aliases: ["上場"] },
    { target: "fiscalMonth", aliases: ["直近決算年月", "決算月"] },
    { target: "revenue", aliases: ["直近売上", "売上"] },
    { target: "employeeCount", aliases: ["社員数"] },
    { target: "officeCount", aliases: ["オフィス数"] },
    { target: "factoryCount", aliases: ["工場数"] },
    { target: "storeCount", aliases: ["店舗数"] },
    { target: "suppliers", aliases: ["仕入れ先"] },
    { target: "clients", aliases: ["取引先"] },
    { target: "banks", aliases: ["取引先銀行"] },
    { target: "foundingYear", aliases: ["創業"] },
    { target: "companyDescription", aliases: ["説明"] },
  ];

  for (const header of headers) {
    // 無視するヘッダーはスキップ
    if (IGNORED_HEADERS.has(header) || IGNORED_HEADERS.has(header.toLowerCase())) {
      continue;
    }

    const norm = normalizeHeader(header);
    let matched = false;

    for (const mapping of fieldMappings) {
      for (const alias of mapping.aliases) {
        if (norm === normalizeHeader(alias) || norm.includes(normalizeHeader(alias))) {
          map[header] = mapping.target;
          matched = true;
          break;
        }
      }
      if (matched) break;
    }
  }

  return map;
}

// ==============================
// CSV行をcompanies_newフィールドにマッピング
// ==============================
function mapRowToCompanyFields(
  row: Record<string, string>,
  headerToField: Record<string, string>
): Record<string, any> | null {
  const result: Record<string, any> = {};

  // 法人番号（13桁の数値でない場合は無視）
  const corpNum = normalizeString(row["法人番号"]);
  if (corpNum) {
    if (isValidCorporateNumber(corpNum)) {
      result.corporateNumber = corpNum.replace(/[^0-9]/g, "");
    } else {
      // 法人番号が無効な場合はスキップ（要件: 13桁の数値でない場合は無視）
      return null;
    }
  }
  // 法人番号がない場合はそのまま続行（グループ3など）

  // 各フィールドをマッピング
  for (const [header, value] of Object.entries(row)) {
    // 無視するヘッダーはスキップ
    if (!header || IGNORED_HEADERS.has(header) || IGNORED_HEADERS.has(header.toLowerCase()) || header.trim() === "") {
      continue;
    }

    const targetField = headerToField[header];
    if (!targetField || isEmptyValue(value)) {
      continue;
    }

    const normalizedValue = normalizeString(value);

    // 特別な処理が必要なフィールド
    if (targetField === "established") {
      const establishedData = parseEstablishedDate(value);
      if (establishedData.established) {
        result.established = establishedData.established;
      }
      if (establishedData.foundingYear !== null) {
        result.foundingYear = establishedData.foundingYear;
      }
    } else if (targetField === "shareholders") {
      // 株主は配列として扱う
      const shareholders = normalizedValue
        .split(/[，,]/)
        .map(s => s.trim())
        .filter(s => s !== "");
      if (shareholders.length > 0) {
        result.shareholders = shareholders;
      }
    } else if (targetField === "suppliers" || targetField === "clients") {
      // 仕入れ先・取引先も配列として扱う
      const items = normalizedValue
        .split(/[，,]/)
        .map(s => s.trim())
        .filter(s => s !== "");
      if (items.length > 0) {
        result[targetField] = items;
      }
    } else if (targetField === "banks") {
      // 取引先銀行も配列として扱う
      const banks = normalizedValue
        .split(/[，,]/)
        .map(s => s.trim())
        .filter(s => s !== "");
      if (banks.length > 0) {
        result.banks = banks;
      }
    } else if (targetField === "capitalStock" || targetField === "revenue" || targetField === "employeeCount" ||
               targetField === "officeCount" || targetField === "factoryCount" || targetField === "storeCount") {
      // 数値フィールド
      const num = Number(normalizedValue.replace(/[,，]/g, ""));
      if (!Number.isNaN(num)) {
        result[targetField] = num;
      } else {
        result[targetField] = normalizedValue;
      }
    } else if (targetField === "foundingYear") {
      // 創業年を抽出
      const yearMatch = normalizedValue.match(/(\d{4})年?/);
      if (yearMatch) {
        const year = parseInt(yearMatch[1]);
        if (year >= 1800 && year <= 2100) {
          result.foundingYear = year;
        }
      }
    } else {
      result[targetField] = normalizedValue;
    }
  }

  // createdAtを設定
  result.createdAt = admin.firestore.FieldValue.serverTimestamp();

  return result;
}

// ==============================
// 既存ドキュメントを検索
// ==============================
async function findExistingCompanyDoc(
  db: Firestore,
  corporateNumber: string | null,
  companyName: string | null,
  address: string | null
): Promise<string | null> {
  // 法人番号で検索（最優先）
  if (corporateNumber) {
    try {
      const docRef = db.collection(COLLECTION_NAME).doc(corporateNumber);
      const doc = await docRef.get();
      if (doc.exists) {
        return corporateNumber;
      }
    } catch (err: any) {
      // エラーは無視して続行
    }
  }

  // 企業名と住所で検索
  if (companyName && address) {
    try {
      const nameQuery = db
        .collection(COLLECTION_NAME)
        .where("name", "==", companyName.trim())
        .limit(100);
      
      const snapshot = await nameQuery.get();
      for (const doc of snapshot.docs) {
        const data = doc.data();
        const docAddress = normalizeString(data.address);
        if (docAddress === address.trim()) {
          return doc.id;
        }
      }
    } catch (err: any) {
      // エラーは無視して続行
    }
  }

  return null;
}

// ==============================
// メイン処理
// ==============================
async function main() {
  console.log("🔍 グループ1-5のCSVファイルをインポートします...\n");

  const db = initFirebase();
  const companiesCol = db.collection(COLLECTION_NAME);

  let totalImported = 0;
  let totalSkipped = 0;
  let totalErrors = 0;

  for (const csvFile of ALL_FILES) {
    const csvPath = path.join(CSV_DIR, csvFile);

    if (!fs.existsSync(csvPath)) {
      console.warn(`⚠️  ファイルが見つかりません: ${csvFile}`);
      continue;
    }

    console.log(`\n📄 処理中: ${csvFile}`);

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

      if (records.length === 0) {
        console.log(`   ⚠️  データがありません`);
        continue;
      }

      // ヘッダーを取得
      const headers = Object.keys(records[0] || {});
      const headerToField = buildHeaderToFieldMap(headers);

      console.log(`   総レコード数: ${records.length}`);

      let imported = 0;
      let skipped = 0;
      let errors = 0;
      let batch: WriteBatch = db.batch();
      let batchCount = 0;
      const BATCH_LIMIT = 200;

      for (let i = 0; i < records.length; i++) {
        try {
          const row = records[i];
          const companyData = mapRowToCompanyFields(row, headerToField);

          if (!companyData) {
            skipped++;
            continue;
          }

          // ドキュメントIDを生成
          const docId = generateNumericDocId(companyData.corporateNumber || null, i);

          // 既存ドキュメントをチェック
          const existingDocId = await findExistingCompanyDoc(
            db,
            companyData.corporateNumber || null,
            companyData.name || null,
            companyData.address || null
          );
          if (existingDocId) {
            skipped++;
            continue;
          }

          // バッチに追加
          const docRef = companiesCol.doc(docId);
          batch.set(docRef, companyData, { merge: false });
          batchCount++;
          imported++;

          // 進捗表示（100件ごと）
          if ((imported + skipped) % 100 === 0) {
            process.stdout.write(`\r   進捗: ${imported + skipped}/${records.length}件 (インポート: ${imported}件, スキップ: ${skipped}件)`);
          }

          // バッチコミット
          if (batchCount >= BATCH_LIMIT) {
            await batch.commit();
            batch = db.batch();
            batchCount = 0;
          }
        } catch (err: any) {
          errors++;
          if (errors <= 5) {
            console.error(`\n   ❌ 行 ${i + 1} の処理エラー: ${err.message}`);
          }
        }
      }

      // 最後のバッチをコミット
      if (batchCount > 0) {
        await batch.commit();
      }

      if ((imported + skipped) % 100 !== 0) {
        console.log(); // 改行
      }

      console.log(`   ✅ インポート: ${imported}件, スキップ: ${skipped}件`);

      totalImported += imported;
      totalSkipped += skipped;
      totalErrors += errors;

    } catch (err: any) {
      console.error(`   ❌ エラー: ${err.message}`);
      totalErrors++;
    }
  }

  console.log("\n" + "=".repeat(60));
  console.log("📊 インポート結果サマリー");
  console.log("=".repeat(60));
  console.log(`✅ インポート成功: ${totalImported}件`);
  console.log(`⏭️  スキップ: ${totalSkipped}件`);
  console.log(`❌ エラー: ${totalErrors}件`);
  console.log("=".repeat(60));
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
