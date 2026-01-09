/* 
  グループ1のCSVファイルから各5件ずつテストインポートするスクリプト
  
  対象ファイル: 1.csv, 103.csv, 126.csv, 2.csv, 53.csv
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
    npx ts-node scripts/test_import_group1_csv.ts
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
const TEST_LIMIT = 5; // 各ファイルから5件ずつ

// グループ1のCSVファイル
const GROUP1_FILES = ["1.csv", "103.csv", "126.csv", "2.csv", "53.csv"];

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
// 文字列正規化
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

// ==============================
// 法人番号の検証（13桁の数値）
// ==============================
function isValidCorporateNumber(corpNum: string | null | undefined): boolean {
  if (!corpNum) return false;
  const normalized = corpNum.trim().replace(/[^0-9]/g, "");
  return /^[0-9]{13}$/.test(normalized);
}

// ==============================
// 数値IDを生成
// ==============================
function generateNumericDocId(corporateNumber: string | null, index: number): string {
  if (corporateNumber && isValidCorporateNumber(corporateNumber)) {
    return corporateNumber.trim().replace(/[^0-9]/g, "");
  }
  // タイムスタンプ + インデックスで数値IDを生成
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// ==============================
// 設立年月日の変換
// ==============================
function parseEstablishedDate(established: string | null | undefined): { established: string | null; foundingYear: number | null } {
  if (!established || isEmptyValue(established)) {
    return { established: null, foundingYear: null };
  }

  const str = String(established).trim();
  
  // 年のみを抽出（例: "1980年", "1980", "4月1日1980年"）
  const yearMatch = str.match(/(\d{4})年?/);
  if (yearMatch) {
    const year = parseInt(yearMatch[1]);
    if (year >= 1800 && year <= 2100) {
      return { established: str, foundingYear: year };
    }
  }
  
  return { established: str, foundingYear: null };
}

// ==============================
// CSV行をcompanies_newフィールドにマッピング
// ==============================
function mapRowToCompanyFields(row: Record<string, string>): Record<string, any> | null {
  const result: Record<string, any> = {};

  // 法人番号（13桁チェック）
  const corpNum = normalizeString(row["法人番号"]);
  if (isValidCorporateNumber(corpNum)) {
    result.corporateNumber = corpNum.replace(/[^0-9]/g, "");
  } else {
    // 法人番号が無効な場合はnullを返してスキップ
    return null;
  }

  // 会社名
  if (!isEmptyValue(row["会社名"])) {
    result.name = normalizeString(row["会社名"]);
  }

  // 電話番号
  if (!isEmptyValue(row["電話番号"])) {
    result.phoneNumber = normalizeString(row["電話番号"]);
  }

  // 会社郵便番号
  if (!isEmptyValue(row["会社郵便番号"])) {
    result.postalCode = normalizeString(row["会社郵便番号"]);
  }

  // 会社住所
  if (!isEmptyValue(row["会社住所"])) {
    result.address = normalizeString(row["会社住所"]);
  }

  // URL
  if (!isEmptyValue(row["URL"])) {
    result.companyUrl = normalizeString(row["URL"]);
  }

  // 代表者名
  if (!isEmptyValue(row["代表者名"])) {
    result.representativeName = normalizeString(row["代表者名"]);
  }

  // 代表者郵便番号
  if (!isEmptyValue(row["代表者郵便番号"])) {
    result.representativeRegisteredAddress = normalizeString(row["代表者郵便番号"]);
  }

  // 代表者住所
  if (!isEmptyValue(row["代表者住所"])) {
    result.representativeHomeAddress = normalizeString(row["代表者住所"]);
  }

  // 代表者誕生日
  if (!isEmptyValue(row["代表者誕生日"])) {
    result.representativeBirthDate = normalizeString(row["代表者誕生日"]);
  }

  // 営業種目
  if (!isEmptyValue(row["営業種目"])) {
    result.businessDescriptions = normalizeString(row["営業種目"]);
  }

  // 設立
  const establishedData = parseEstablishedDate(row["設立"]);
  if (establishedData.established) {
    result.established = establishedData.established;
  }
  if (establishedData.foundingYear !== null) {
    result.foundingYear = establishedData.foundingYear;
  }

  // 株主（配列として扱う）
  if (!isEmptyValue(row["株主"])) {
    const shareholders = normalizeString(row["株主"])
      .split(/[，,]/)
      .map(s => s.trim())
      .filter(s => s !== "");
    if (shareholders.length > 0) {
      result.shareholders = shareholders;
    }
  }

  // 取締役
  if (!isEmptyValue(row["取締役"])) {
    result.executives = normalizeString(row["取締役"]);
  }

  // 概況
  if (!isEmptyValue(row["概況"])) {
    result.overview = normalizeString(row["概況"]);
  }

  // 業種-大
  if (!isEmptyValue(row["業種-大"])) {
    result.industryLarge = normalizeString(row["業種-大"]);
  }

  // 業種-中
  if (!isEmptyValue(row["業種-中"])) {
    result.industryMiddle = normalizeString(row["業種-中"]);
  }

  // 業種-小
  if (!isEmptyValue(row["業種-小"])) {
    result.industrySmall = normalizeString(row["業種-小"]);
  }

  // 業種-細
  if (!isEmptyValue(row["業種-細"])) {
    result.industryDetail = normalizeString(row["業種-細"]);
  }

  // createdAtを設定
  result.createdAt = admin.firestore.FieldValue.serverTimestamp();

  return result;
}

// ==============================
// メイン処理
// ==============================
async function main() {
  console.log("🔍 グループ1のCSVファイルから各5件ずつテストインポートします...\n");

  const db = initFirebase();
  const companiesCol = db.collection(COLLECTION_NAME);

  let totalImported = 0;
  let totalSkipped = 0;

  for (const csvFile of GROUP1_FILES) {
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
      }) as Array<Record<string, string>>;

      console.log(`   総レコード数: ${records.length}`);

      let imported = 0;
      let skipped = 0;
      const batch = db.batch();
      let batchCount = 0;

      for (let i = 0; i < Math.min(TEST_LIMIT, records.length); i++) {
        const row = records[i];
        const companyData = mapRowToCompanyFields(row);

        if (!companyData) {
          console.log(`   [${i + 1}] スキップ: 法人番号が無効`);
          skipped++;
          continue;
        }

        // ドキュメントIDを生成
        const docId = generateNumericDocId(companyData.corporateNumber, i);
        const docRef = companiesCol.doc(docId);

        // 既存ドキュメントをチェック
        const existingDoc = await docRef.get();
        if (existingDoc.exists) {
          console.log(`   [${i + 1}] スキップ: 既に存在 (ID: ${docId})`);
          skipped++;
          continue;
        }

        // バッチに追加
        batch.set(docRef, companyData, { merge: false });
        batchCount++;
        imported++;

        console.log(`   [${i + 1}] ✅ インポート予定: ${companyData.name || "(名前なし)"} (ID: ${docId})`);
      }

      // バッチをコミット
      if (batchCount > 0) {
        await batch.commit();
        console.log(`   💾 ${imported}件をインポートしました`);
      }

      totalImported += imported;
      totalSkipped += skipped;

    } catch (err: any) {
      console.error(`   ❌ エラー: ${err.message}`);
    }
  }

  console.log("\n" + "=".repeat(60));
  console.log("📊 インポート結果サマリー");
  console.log("=".repeat(60));
  console.log(`✅ インポート成功: ${totalImported}件`);
  console.log(`⏭️  スキップ: ${totalSkipped}件`);
  console.log("=".repeat(60));
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
