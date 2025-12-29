/* 
  116.csvをcompanies_newコレクションにインポートするスクリプト
  
  ルール:
  - 空欄はスルー
  - 法人番号が崩れている場合は無視
  - 新規追加時は現状のcompanies_newコレクションのフィールドに合わせる
  - 全て埋まらなくても良いが、フィールドの数と内容は他のものと合わせる
  - ドキュメントIDは数値にする
  - それぞれフィールドに正しく入るかを判断して入れる
  - 業種{数値}の次は郵便番号が来るので、3桁-4桁の数値が来たら検知して、それ以降を住所〜となるようにする
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import { parse } from "csv-parse/sync";
import type { Firestore, CollectionReference, WriteBatch } from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_FILE = "csv/116.csv";
const BATCH_LIMIT = 400;

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
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// COMPANY_TEMPLATE（import_companies_from_csv.tsから）
const COMPANY_TEMPLATE: Record<string, any> = {
  acquisition: null,
  adExpiration: null,
  address: null,
  businessDescriptions: null,
  capitalStock: null,
  changeCount: null,
  clients: null,
  companyDescription: null,
  companyUrl: null,
  contactFormUrl: null,
  corporateNumber: null,
  corporationType: null,
  createdAt: null,
  demandProducts: null,
  email: null,
  employeeCount: null,
  established: null,
  executives: null,
  facebook: null,
  factoryCount: null,
  fax: null,
  financials: null,
  fiscalMonth: null,
  foundingYear: null,
  headquartersAddress: null,
  industries: [],
  industry: null,
  industryCategories: [],
  industryDetail: null,
  industryLarge: null,
  industryMiddle: null,
  industrySmall: null,
  linkedin: null,
  listing: null,
  latestFiscalYearMonth: null,
  latestProfit: null,
  marketSegment: null,
  metaDescription: null,
  metaKeywords: null,
  name: null,
  officeCount: null,
  overview: null,
  phoneNumber: null,
  postalCode: null,
  prefecture: null,
  representativeAlmaMater: null,
  representativeBirthDate: null,
  representativeHomeAddress: null,
  representativeKana: null,
  representativeName: null,
  representativePhone: null,
  representativePostalCode: null,
  representativeRegisteredAddress: null,
  representativeTitle: null,
  revenue: null,
  salesNotes: null,
  shareholders: null,
  storeCount: null,
  suppliers: [],
  banks: [],
  tags: [],
  updateCount: null,
  updatedAt: null,
  urls: [],
  wantedly: null,
  youtrust: null,
};

// 郵便番号パターン（3桁-4桁）
function isPostalCode(value: string): boolean {
  if (!value || typeof value !== "string") return false;
  const trimmed = value.trim();
  // 3桁-4桁のパターン（例: 452-0834）
  return /^\d{3}-\d{4}$/.test(trimmed) || /^\d{7}$/.test(trimmed.replace(/-/g, ""));
}

// 郵便番号を正規化（3桁-4桁形式）
function normalizePostalCode(value: string): string | null {
  if (!value || typeof value !== "string") return null;
  const digits = value.replace(/\D/g, "");
  if (digits.length === 7) {
    return `${digits.substring(0, 3)}-${digits.substring(3)}`;
  }
  return null;
}

// 法人番号を検証（13桁の数字のみ有効、指数表記は無視）
function validateCorporateNumber(value: string): string | null {
  if (!value || typeof value !== "string") return null;
  const trimmed = value.trim();
  
  // 指数表記は無視
  if (trimmed.includes("E") || trimmed.includes("e")) {
    return null;
  }
  
  // 13桁の数字のみ有効
  const digits = trimmed.replace(/\D/g, "");
  if (digits.length === 13) {
    return digits;
  }
  
  return null;
}

// 数値フィールドを変換
function parseNumeric(value: string): number | null {
  if (!value || typeof value !== "string") return null;
  const cleaned = value.replace(/[,，]/g, "").trim();
  if (cleaned === "") return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

// ドキュメントIDを生成（数値のみ）
function generateNumericDocId(corporateNumber: string | null, rowIndex: number): string {
  if (corporateNumber && /^[0-9]+$/.test(corporateNumber)) {
    return corporateNumber;
  }
  const timestamp = Date.now();
  const paddedIndex = String(rowIndex).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 116.csvの行をマッピング
function mapRowToCompanyFields(row: Array<string>, headers: Array<string>): Record<string, any> {
  const result: Record<string, any> = {};
  
  // 固定カラムのインデックスを取得
  const nameIndex = headers.findIndex(h => h === "会社名" || h === "企業名");
  const prefectureIndex = headers.findIndex(h => h === "都道府県");
  const representativeNameIndex = headers.findIndex(h => h === "代表者名");
  const corporateNumberIndex = headers.findIndex(h => h === "法人番号");
  const urlIndex = headers.findIndex(h => h === "URL");
  
  // 基本情報
  if (nameIndex >= 0 && row[nameIndex]?.trim()) {
    result.name = row[nameIndex].trim();
  }
  if (prefectureIndex >= 0 && row[prefectureIndex]?.trim()) {
    result.prefecture = row[prefectureIndex].trim();
  }
  if (representativeNameIndex >= 0 && row[representativeNameIndex]?.trim()) {
    result.representativeName = row[representativeNameIndex].trim();
  }
  if (corporateNumberIndex >= 0 && row[corporateNumberIndex]?.trim()) {
    const corpNum = validateCorporateNumber(row[corporateNumberIndex]);
    if (corpNum) {
      result.corporateNumber = corpNum;
    }
  }
  if (urlIndex >= 0 && row[urlIndex]?.trim()) {
    result.companyUrl = row[urlIndex].trim();
  }
  
  // 業種の処理（業種1, 業種2, 業種3, 業種4...）
  let industryIndex = headers.findIndex(h => h === "業種1");
  if (industryIndex >= 0) {
    if (row[industryIndex]?.trim()) result.industryLarge = row[industryIndex].trim();
    industryIndex++;
  }
  
  if (industryIndex >= 0 && headers[industryIndex] === "業種2") {
    if (row[industryIndex]?.trim()) result.industryMiddle = row[industryIndex].trim();
    industryIndex++;
  }
  
  if (industryIndex >= 0 && headers[industryIndex] === "業種3") {
    if (row[industryIndex]?.trim()) result.industrySmall = row[industryIndex].trim();
    industryIndex++;
  }
  
  // 業種4以降の処理（業種4, 業種5, 業種6... または郵便番号）
  const industryCategories: string[] = [];
  while (industryIndex < headers.length && industryIndex < row.length) {
    const header = headers[industryIndex]?.trim();
    const value = row[industryIndex]?.trim();
    
    if (!value || value === "") {
      industryIndex++;
      continue;
    }
    
    // 郵便番号パターンを検出
    if (isPostalCode(value)) {
      // 郵便番号が見つかったので、ここから住所カラムに移行
      const postalCode = normalizePostalCode(value);
      if (postalCode) {
        result.postalCode = postalCode;
      }
      industryIndex++;
      break; // 業種の処理を終了
    }
    
    // 業種カテゴリーとして追加
    if (header && header.startsWith("業種")) {
      industryCategories.push(value);
      industryIndex++;
    } else {
      // 業種カテゴリーではないので終了
      break;
    }
  }
  
  if (industryCategories.length > 0) {
    result.industryCategories = industryCategories;
  }
  
  // 郵便番号が見つからなかった場合、次のカラムを確認
  if (!result.postalCode && industryIndex < row.length) {
    const value = row[industryIndex]?.trim();
    if (value && isPostalCode(value)) {
      result.postalCode = normalizePostalCode(value);
      industryIndex++;
    }
  }
  
  // 住所（郵便番号の次）
  if (industryIndex < headers.length && industryIndex < row.length) {
    const header = headers[industryIndex]?.trim();
    const value = row[industryIndex]?.trim();
    
    if (header === "住所" || header === "所在地") {
      if (value) result.address = value;
      industryIndex++;
    } else if (value && !isPostalCode(value)) {
      // ヘッダーが不明でも、郵便番号でなければ住所の可能性
      result.address = value;
      industryIndex++;
    }
  }
  
  // 残りの固定カラムを処理
  const fixedMappings: Record<string, string> = {
    "設立": "established",
    "電話番号(窓口)": "phoneNumber",
    "代表者郵便番号": "representativePostalCode",
    "代表者住所": "representativeHomeAddress",
    "代表者誕生日": "representativeBirthDate",
    "資本金": "capitalStock",
    "上場": "listing",
    "直近決算年月": "latestFiscalYearMonth",
    "直近売上": "revenue",
    "直近利益": "latestProfit",
    "説明": "companyDescription",
    "概要": "overview",
    "仕入れ先": "suppliers",
    "取引先": "clients",
    "取引先銀行": "banks",
    "取締役": "executives",
    "株主": "shareholders",
    "社員数": "employeeCount",
    "オフィス数": "officeCount",
    "工場数": "factoryCount",
    "店舗数": "storeCount",
  };
  
  for (let i = 0; i < headers.length; i++) {
    const header = headers[i]?.trim();
    const value = row[i]?.trim();
    
    if (!header || !value || value === "") continue;
    
    const field = fixedMappings[header];
    if (field) {
      if (field === "suppliers" || field === "banks") {
        // 配列として保存
        result[field] = value.split(/[，,]/).map(s => s.trim()).filter(s => s);
      } else if (field === "capitalStock" || field === "revenue" || field === "latestProfit") {
        // 財務情報フィールド（千円単位なので1000倍）
        const num = parseNumeric(value);
        if (num !== null) {
          result[field] = num * 1000;
        }
      } else if (field === "employeeCount" || field === "officeCount" || field === "factoryCount" || field === "storeCount") {
        // 数値フィールド（財務情報以外）
        const num = parseNumeric(value);
        if (num !== null) {
          result[field] = num;
        }
      } else if (field === "representativePostalCode") {
        // 郵便番号を正規化
        const postalCode = normalizePostalCode(value);
        if (postalCode) {
          result[field] = postalCode;
        }
      } else {
        result[field] = value;
      }
    }
  }
  
  return result;
}

async function main() {
  console.log(`📄 CSVファイルを読み込み中: ${CSV_FILE}\n`);
  
  const filePath = path.resolve(CSV_FILE);
  if (!fs.existsSync(filePath)) {
    console.error(`❌ ファイルが見つかりません: ${CSV_FILE}`);
    process.exit(1);
  }
  
  const buf = fs.readFileSync(filePath);
  const records: Array<Array<string>> = parse(buf, {
    columns: false,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    skip_records_with_error: true,
  });
  
  if (records.length < 2) {
    console.error(`❌ エラー: CSVファイルにデータ行がありません（総行数: ${records.length}）`);
    process.exit(1);
  }
  
  const headers = records[0];
  console.log(`📊 総行数: ${records.length - 1} 行（ヘッダー除く）\n`);
  
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  let createdCount = 0;
  let skippedCount = 0;
  
  for (let i = 1; i < records.length; i++) {
    const row = records[i];
    const rowNumber = i + 1;
    
    const mapped = mapRowToCompanyFields(row, headers);
    
    // 企業名がない場合はスキップ
    if (!mapped.name || mapped.name.trim() === "") {
      skippedCount++;
      continue;
    }
    
    // ドキュメントIDを生成
    const docId = generateNumericDocId(mapped.corporateNumber || null, i);
    const docRef = companiesCol.doc(docId);
    
    // テンプレートをベースにマッピング結果をマージ
    const writeData = {
      ...COMPANY_TEMPLATE,
      ...mapped,
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    
    batch.set(docRef, writeData, { merge: false });
    batchCount++;
    createdCount++;
    
    if (batchCount >= BATCH_LIMIT) {
      console.log(`💾 バッチコミット (${batchCount} 件) ...`);
      await batch.commit();
      batch = db.batch();
      batchCount = 0;
    }
    
    if (createdCount % 100 === 0) {
      console.log(`📊 処理中: ${createdCount} 件...`);
    }
  }
  
  // 残りのバッチをコミット
  if (batchCount > 0) {
    console.log(`💾 最終バッチコミット (${batchCount} 件) ...`);
    await batch.commit();
  }
  
  console.log(`\n${"=".repeat(60)}`);
  console.log(`📊 インポート結果`);
  console.log(`${"=".repeat(60)}`);
  console.log(`✨ 新規作成: ${createdCount} 件`);
  console.log(`⏭️  スキップ: ${skippedCount} 件`);
  console.log(`\n✅ 処理完了`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
