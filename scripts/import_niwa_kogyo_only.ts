/* 
  「丹羽興業株式会社」のみを116.csvからインポートするテストスクリプト
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import { parse } from "csv-parse/sync";
import type { Firestore, CollectionReference, DocumentReference, WriteBatch } from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_FILE = "csv/116.csv";
const TARGET_ROW = 2; // CSVの2行目（ヘッダー行を除く）

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

// import_companies_from_csv.tsから必要な関数をコピー
function normalizeHeader(h: string): string {
  return h
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/_/g, "")
    .replace(/[（）()]/g, "");
}

function castValue(field: string, raw: string): any {
  const v = raw.trim();
  if (v === "") return null;

  if (field === "corporateNumber") {
    const s = v.replace(/"/g, "");
    // 例: 3.12E+12 / 3.12e+12
    if (/^\d+(\.\d+)?e\+\d+$/i.test(s)) {
      const n = Number(s);
      if (!Number.isNaN(n)) {
        return Math.round(n).toString();
      }
    }
    return s;
  }

  const NUMERIC_FIELDS = new Set<string>([
    "capitalStock", "revenue", "employeeCount", "factoryCount",
    "officeCount", "storeCount", "foundingYear", "fiscalMonth",
    "changeCount", "updateCount",
  ]);

  if (NUMERIC_FIELDS.has(field)) {
    const n = Number(v.replace(/[,，]/g, ""));
    if (!Number.isNaN(n)) return n;
    return v;
  }

  return v;
}

function generateNumericDocId(
  corporateNumber: string | null,
  rowIndex: number
): string {
  // corporateNumberが存在し、数字のみの場合 → そのまま使用
  if (corporateNumber && /^[0-9]+$/.test(corporateNumber.trim())) {
    return corporateNumber.trim();
  }
  
  // それ以外の場合 → Date.now() + 行番号から数字のみの一意IDを生成
  const timestamp = Date.now();
  const paddedIndex = String(rowIndex).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 簡易的なマッピング（116.csvの構造に基づく）
function mapRowToCompanyFields(row: Array<string>, headers: Array<string>): Record<string, any> {
  const result: Record<string, any> = {};
  
  const headerMap: Record<string, string> = {
    "会社名": "name",
    "都道府県": "prefecture",
    "代表者名": "representativeName",
    "法人番号": "corporateNumber",
    "URL": "companyUrl",
    "業種1": "industryLarge",
    "業種2": "industryMiddle",
    "業種3": "industrySmall",
    "業種4": "industryDetail",
    "郵便番号": "postalCode",
    "住所": "address",
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
  
  for (let i = 0; i < headers.length && i < row.length; i++) {
    const header = headers[i].trim();
    const value = row[i]?.trim();
    
    if (!value || value === "") continue;
    
    const field = headerMap[header];
    if (field) {
      if (field === "suppliers" || field === "banks") {
        // 配列として保存
        result[field] = value.split(/[，,]/).map(s => s.trim()).filter(s => s);
      } else {
        result[field] = castValue(field, value);
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
  
  if (records.length < TARGET_ROW) {
    console.error(`❌ エラー: CSVファイルに${TARGET_ROW}行目がありません（総行数: ${records.length}）`);
    process.exit(1);
  }
  
  const headers = records[0];
  const row = records[TARGET_ROW - 1]; // 0ベースインデックス
  
  console.log(`📋 処理対象: ${TARGET_ROW}行目`);
  console.log(`   会社名: ${row[0] || '(なし)'}\n`);
  
  const mapped = mapRowToCompanyFields(row, headers);
  
  console.log(`📊 マッピング結果:`);
  console.log(`   企業名: ${mapped.name || '(なし)'}`);
  console.log(`   法人番号: ${mapped.corporateNumber || '(なし)'}`);
  console.log(`   住所: ${mapped.address || '(なし)'}`);
  console.log(`   説明: ${mapped.companyDescription || '(なし)'}`);
  console.log(`   概要: ${mapped.overview ? (mapped.overview.substring(0, 50) + '...') : '(なし)'}`);
  console.log(`   取引先: ${mapped.clients || '(なし)'}`);
  console.log(`   仕入れ先: ${Array.isArray(mapped.suppliers) ? mapped.suppliers.join(', ') : '(なし)'}`);
  console.log(`   取引先銀行: ${Array.isArray(mapped.banks) ? mapped.banks.join(', ') : '(なし)'}`);
  console.log(`   取締役: ${mapped.executives || '(なし)'}`);
  console.log(`   株主: ${mapped.shareholders || '(なし)'}\n`);
  
  const corporateNumber = mapped.corporateNumber || null;
  const companyName = mapped.name || null;
  
  // ドキュメントIDを決定
  const docId = generateNumericDocId(corporateNumber, TARGET_ROW);
  console.log(`📝 ドキュメントID: ${docId}\n`);
  
  // 既存ドキュメントを確認
  const existingRef = companiesCol.doc(docId);
  const existingDoc = await existingRef.get();
  
  if (existingDoc.exists) {
    console.log(`⚠️  既存ドキュメントが見つかりました: ${docId}`);
    const existingData = existingDoc.data();
    console.log(`   既存の企業名: ${existingData?.name || '(なし)'}`);
    console.log(`   既存の法人番号: ${existingData?.corporateNumber || '(なし)'}`);
    console.log(`\n既存ドキュメントを削除してから再作成しますか？ (y/n): `);
    // 自動で削除して再作成
    await existingRef.delete();
    console.log(`✅ 既存ドキュメントを削除しました\n`);
  }
  
  // テンプレートを作成
  const COMPANY_TEMPLATE: Record<string, any> = {
    acquisition: null,
    address: null,
    affiliations: null,
    banks: [],
    businessDescriptions: null,
    capitalStock: null,
    clients: null,
    companyDescription: null,
    companyUrl: null,
    corporateNumber: null,
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
    employeeCount: null,
    executives: null,
    factoryCount: null,
    industryDetail: null,
    industryLarge: null,
    industryMiddle: null,
    industrySmall: null,
    latestFiscalYearMonth: null,
    latestProfit: null,
    listing: null,
    name: null,
    officeCount: null,
    overview: null,
    phoneNumber: null,
    postalCode: null,
    prefecture: null,
    representativeBirthDate: null,
    representativeHomeAddress: null,
    representativeName: null,
    representativePostalCode: null,
    revenue: null,
    shareholders: null,
    storeCount: null,
    suppliers: [],
    updatedAt: admin.firestore.FieldValue.serverTimestamp(),
  };
  
  const writeData = {
    ...COMPANY_TEMPLATE,
    ...mapped,
  };
  
  console.log(`💾 ドキュメントを作成中...`);
  await existingRef.set(writeData);
  console.log(`✅ 作成完了: ${docId}`);
  console.log(`   企業名: ${writeData.name}`);
  console.log(`   法人番号: ${writeData.corporateNumber || '(なし)'}`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
