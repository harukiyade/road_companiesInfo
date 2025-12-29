/*
  タイプE（116.csv）の特定ドキュメントを削除し、指定行を新規作成するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_and_recreate_type_e_116.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentReference } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const TYPE_E_FILE = "csv/116.csv";
// 削除対象の会社名
const COMPANY_NAMES_TO_DELETE = [
  "株式会社ワールド・アメニティー",
  "株式会社ジックス",
  "株式会社興和アークビルド"
];
const ROWS_TO_CREATE = [11, 12, 17]; // 11行目、12行目、17行目

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
const companiesCol = db.collection(COLLECTION_NAME);

function trim(v?: string | null): string | null {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function parseNumeric(v: string): number | null {
  const cleaned = v.replace(/[^\d.-]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function validateCorporateNumber(v: string): string | null {
  if (!v) return null;
  
  let value = String(v).trim();
  if (!value) return null;
  
  // 指数表記（例: 2.01E+12）の場合はnullを返す
  if (value.includes("E") || value.includes("e")) {
    return null;
  }
  
  const digits = value.replace(/\D/g, "");
  // 13桁の数値でない場合はnull
  if (digits.length === 13) {
    return digits;
  }
  return null;
}

function isNumericValue(value: string | null | undefined): boolean {
  if (!value) return false;
  const trimmed = String(value).trim();
  if (!trimmed) return false;
  const cleaned = trimmed.replace(/[,\-\s]/g, "");
  return /^\d+$/.test(cleaned) && cleaned.length > 0;
}

function validatePostalCode(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  const digits = trimmed.replace(/\D/g, "");
  
  // ユーザー指定: 13桁の数値でない場合はnull
  // ただし、実際の郵便番号は7桁なので、7桁の場合は通常の郵便番号として処理
  // 13桁の数値は法人番号の可能性があるため、郵便番号としてはnull
  if (digits.length === 13) {
    return null;
  }
  
  // 7桁の郵便番号形式をチェック（通常の郵便番号）
  if (digits.length === 7) {
    return digits.replace(/(\d{3})(\d{4})/, "$1-$2");
  }
  
  // その他の場合はnull
  return null;
}

// タイプEの行データを列インデックスに基づいてマッピング
function mapTypeERowByIndex(row: Array<string>): Record<string, any> {
  const mapped: Record<string, any> = {};
  let colIndex = 0;
  
  // 1. 会社名
  if (row[colIndex]) mapped.name = trim(row[colIndex]);
  colIndex++;
  
  // 2. 都道府県
  if (row[colIndex]) mapped.prefecture = trim(row[colIndex]);
  colIndex++;
  
  // 3. 代表者名
  if (row[colIndex]) mapped.representativeName = trim(row[colIndex]);
  colIndex++;
  
  // 4. 法人番号
  if (row[colIndex]) {
    const validated = validateCorporateNumber(row[colIndex]);
    if (validated) mapped.corporateNumber = validated;
  }
  colIndex++;
  
  // 5. URL
  if (row[colIndex]) mapped.companyUrl = trim(row[colIndex]);
  colIndex++;
  
  // 6. 業種1
  if (row[colIndex]) mapped.industryLarge = trim(row[colIndex]);
  colIndex++;
  
  // 7. 業種2
  if (row[colIndex]) mapped.industryMiddle = trim(row[colIndex]);
  colIndex++;
  
  // 8. 業種3
  if (row[colIndex]) mapped.industrySmall = trim(row[colIndex]);
  colIndex++;
  
  // 9. 業種4
  if (row[colIndex]) mapped.industryDetail = trim(row[colIndex]);
  colIndex++;
  
  // 10-11. 業種5・業種6の処理（動的判定）
  // industryCategoriesを初期化
  mapped.industryCategories = [];
  
  const industry5Value = row[colIndex] ? trim(row[colIndex]) : null;
  
  if (industry5Value && isNumericValue(industry5Value)) {
    // 業種5の位置に数値が来た = 業種5と6はない、これは郵便番号
    const postalCode = validatePostalCode(industry5Value);
    mapped.postalCode = postalCode; // 7桁でない場合はnull
    colIndex++;
  } else {
    // 業種5がある（非数値）
    if (industry5Value) {
      mapped.industryCategories.push(industry5Value);
    }
    colIndex++;
    
    // 業種6の位置をチェック
    const industry6Value = row[colIndex] ? trim(row[colIndex]) : null;
    if (industry6Value) {
      if (isNumericValue(industry6Value)) {
        // 業種6の位置に郵便番号が来た（業種6はない）
        const postalCode = validatePostalCode(industry6Value);
        mapped.postalCode = postalCode; // 7桁でない場合はnull
        colIndex++;
      } else {
        // 業種6がある（非数値）
        mapped.industryCategories.push(industry6Value);
        colIndex++;
        
        // 次の位置が郵便番号
        if (row[colIndex]) {
          const postalCode = validatePostalCode(row[colIndex]);
          mapped.postalCode = postalCode; // 7桁でない場合はnull
        }
        colIndex++;
      }
    } else {
      // 業種6がない場合、次の位置が郵便番号
      if (row[colIndex]) {
        const postalCode = validatePostalCode(row[colIndex]);
        mapped.postalCode = postalCode; // 7桁でない場合はnull
      }
      colIndex++;
    }
  }
  
  // 12. 住所（郵便番号の次）
  // 郵便番号がまだ設定されていない場合、この位置が郵便番号の可能性をチェック
  if (!mapped.postalCode && row[colIndex]) {
    const postalCode = validatePostalCode(row[colIndex]);
    if (postalCode) {
      mapped.postalCode = postalCode;
      colIndex++;
      // 次の位置が住所
      if (row[colIndex]) mapped.address = trim(row[colIndex]);
      colIndex++;
    } else {
      // 郵便番号でない場合は住所として処理
      if (row[colIndex]) mapped.address = trim(row[colIndex]);
      colIndex++;
    }
  } else {
    // 郵便番号は既に設定済み、この位置が住所
    if (row[colIndex]) mapped.address = trim(row[colIndex]);
    colIndex++;
  }
  
  // 13. 設立
  if (row[colIndex]) mapped.established = trim(row[colIndex]);
  colIndex++;
  
  // 14. 電話番号(窓口)
  if (row[colIndex]) mapped.phoneNumber = trim(row[colIndex]);
  colIndex++;
  
  // 15. 代表者郵便番号
  if (row[colIndex]) {
    const postalCode = validatePostalCode(row[colIndex]);
    if (postalCode) {
      mapped.representativePostalCode = postalCode;
    }
  }
  colIndex++;
  
  // 16. 代表者住所
  if (row[colIndex]) mapped.representativeHomeAddress = trim(row[colIndex]);
  colIndex++;
  
  // 17. 代表者誕生日
  if (row[colIndex]) mapped.representativeBirthDate = trim(row[colIndex]);
  colIndex++;
  
  // 18. 資本金
  if (row[colIndex]) {
    const capitalValue = trim(row[colIndex]);
    if (capitalValue) {
      const num = parseNumeric(capitalValue);
      if (num !== null) {
        mapped.capitalStock = num;
      }
    }
  }
  colIndex++;
  
  // 19. 上場
  if (row[colIndex]) mapped.listing = trim(row[colIndex]);
  colIndex++;
  
  // 20. 直近決算年月
  if (row[colIndex]) {
    mapped.latestFiscalYearMonth = trim(row[colIndex]);
  }
  colIndex++;
  
  // 21. 直近売上
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.revenue = num;
  }
  colIndex++;
  
  // 22. 直近利益
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.latestProfit = num;
  }
  colIndex++;
  
  // 23. 説明
  if (row[colIndex]) mapped.companyDescription = trim(row[colIndex]);
  colIndex++;
  
  // 24. 概要
  if (row[colIndex]) mapped.overview = trim(row[colIndex]);
  colIndex++;
  
  // 25. 仕入れ先
  if (row[colIndex]) {
    const suppliersValue = trim(row[colIndex]);
    if (suppliersValue) {
      // 配列として保存（カンマ区切りの場合は分割）
      mapped.suppliers = suppliersValue.split(/[，,]/).map(s => s.trim()).filter(s => s);
    }
  }
  colIndex++;
  
  // 26. 取引先
  if (row[colIndex]) mapped.clients = trim(row[colIndex]);
  colIndex++;
  
  // 27. 取引先銀行
  if (row[colIndex]) {
    const banksValue = trim(row[colIndex]);
    if (banksValue) {
      // 配列として保存（カンマ区切りの場合は分割）
      mapped.banks = banksValue.split(/[，,]/).map(s => s.trim()).filter(s => s);
    }
  }
  colIndex++;
  
  // 28. 取締役
  if (row[colIndex]) mapped.executives = trim(row[colIndex]);
  colIndex++;
  
  // 29. 株主
  if (row[colIndex]) {
    const shareholdersValue = trim(row[colIndex]);
    if (shareholdersValue) {
      mapped.shareholders = shareholdersValue;
    }
  }
  colIndex++;
  
  // 30. 社員数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.employeeCount = num;
  }
  colIndex++;
  
  // 31. オフィス数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.officeCount = num;
  }
  colIndex++;
  
  // 32. 工場数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.factoryCount = num;
  }
  colIndex++;
  
  // 33. 店舗数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.storeCount = num;
  }
  colIndex++;
  
  return mapped;
}

function generateTestDocId(corporateNumber: string | null, rowIndex: number): string {
  // テスト用とわかるように文字列IDを生成
  if (corporateNumber) {
    return `test_${corporateNumber}`;
  }
  // 法人番号がない場合は、行番号ベースのIDを生成
  return `test_row_${rowIndex}`;
}

const COMPANY_TEMPLATE: Record<string, any> = {
  acquisition: null,
  adExpiration: null,
  address: null,
  affiliations: null,
  averageAge: null,
  averageOvertimeHours: null,
  averagePaidLeave: null,
  averageYearsOfService: null,
  badges: [],
  bankCorporateNumber: null,
  banks: [],
  businessDescriptions: null,
  businessItems: [],
  businessSummary: null,
  capitalStock: null,
  changeCount: null,
  clients: null,
  companyDescription: null,
  companyUrl: null,
  contactFormUrl: null,
  contactPhoneNumber: null,
  corporateNumber: null,
  corporationType: null,
  createdAt: null,
  departmentName1: null,
  departmentAddress1: null,
  departmentPhone1: null,
  departmentName2: null,
  departmentAddress2: null,
  departmentPhone2: null,
  departmentName3: null,
  departmentAddress3: null,
  departmentPhone3: null,
  departmentName4: null,
  departmentAddress4: null,
  departmentPhone4: null,
  departmentName5: null,
  departmentAddress5: null,
  departmentPhone5: null,
  departmentName6: null,
  departmentAddress6: null,
  departmentPhone6: null,
  departmentName7: null,
  departmentAddress7: null,
  departmentPhone7: null,
  dateOfEstablishment: null,
  demandProducts: null,
  departmentLocation: null,
  email: null,
  employeeCount: null,
  employeeNumber: null,
  established: null,
  executives: null,
  executiveName1: null,
  executivePosition1: null,
  executiveName2: null,
  executivePosition2: null,
  executiveName3: null,
  executivePosition3: null,
  executiveName4: null,
  executivePosition4: null,
  executiveName5: null,
  executivePosition5: null,
  executiveName6: null,
  executivePosition6: null,
  executiveName7: null,
  executivePosition7: null,
  executiveName8: null,
  executivePosition8: null,
  executiveName9: null,
  executivePosition9: null,
  executiveName10: null,
  executivePosition10: null,
  externalDetailUrl: null,
  facebook: null,
  factoryCount: null,
  fax: null,
  femaleExecutiveRatio: null,
  financials: null,
  fiscalMonth: null,
  fiscalMonth1: null,
  fiscalMonth2: null,
  fiscalMonth3: null,
  fiscalMonth4: null,
  fiscalMonth5: null,
  founding: null,
  foundingYear: null,
  headquartersAddress: null,
  industries: [],
  industry: null,
  industryCategories: null,
  industryDetail: null,
  industryLarge: null,
  industryMiddle: null,
  industrySmall: null,
  issuedShares: null,
  kana: null,
  latestFiscalYearMonth: null,
  latestProfit: null,
  latestRevenue: null,
  profit1: null,
  profit2: null,
  profit3: null,
  profit4: null,
  profit5: null,
  linkedin: null,
  listing: null,
  location: null,
  marketSegment: null,
  netAssets: null,
  metaDescription: null,
  metaKeywords: null,
  name: null,
  nameEn: null,
  nikkeiCode: null,
  numberOfActivity: null,
  officeCount: null,
  operatingIncome: null,
  overview: null,
  phoneNumber: null,
  postalCode: null,
  prefecture: null,
  profileUrl: null,
  qualificationGrade: null,
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
  revenueFromStatements: null,
  revenue1: null,
  revenue2: null,
  revenue3: null,
  revenue4: null,
  revenue5: null,
  salesNotes: null,
  shareholders: null,
  specialNote: null,
  specialties: null,
  storeCount: null,
  subsidiaries: [],
  suppliers: [],
  tags: [],
  totalAssets: null,
  totalLiabilities: null,
  tradingStatus: null,
  transportation: null,
  updateCount: null,
  updateDate: null,
  updatedAt: null,
  urls: [],
  wantedly: null,
  youtrust: null,
};

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  
  // 1. 既存ドキュメントを削除（会社名で検索）
  console.log(`🗑️  既存ドキュメントを削除（会社名で検索）...`);
  const docIdsToDelete: string[] = [];
  
  for (const companyName of COMPANY_NAMES_TO_DELETE) {
    const snapshot = await companiesCol
      .where("name", "==", companyName)
      .limit(10)
      .get();
    
    // 116.csvのデータと一致するものを特定
    let targetDocId: string | null = null;
    if (companyName === "株式会社ワールド・アメニティー") {
      // 郵便番号171-0022、住所に「南池袋」を含む
      for (const doc of snapshot.docs) {
        const data = doc.data();
        if (data.postalCode === "171-0022" || 
            (data.address && data.address.includes("南池袋"))) {
          targetDocId = doc.id;
          break;
        }
      }
    } else if (companyName === "株式会社ジックス") {
      // 郵便番号170-0004、住所に「北大塚」を含む
      for (const doc of snapshot.docs) {
        const data = doc.data();
        if (data.postalCode === "170-0004" || 
            (data.address && data.address.includes("北大塚"))) {
          targetDocId = doc.id;
          break;
        }
      }
    } else if (companyName === "株式会社興和アークビルド") {
      // 郵便番号158-0098、住所に「上用賀」を含む
      for (const doc of snapshot.docs) {
        const data = doc.data();
        if (data.postalCode === "158-0098" || 
            (data.address && data.address.includes("上用賀"))) {
          targetDocId = doc.id;
          break;
        }
      }
    }
    
    // 見つからなかった場合は最初のドキュメントを削除
    if (!targetDocId && snapshot.docs.length > 0) {
      targetDocId = snapshot.docs[0].id;
    }
    
    if (targetDocId) {
      docIdsToDelete.push(targetDocId);
      const docRef = companiesCol.doc(targetDocId);
      if (!DRY_RUN) {
        await docRef.delete();
        console.log(`  ✅ 削除完了: ${targetDocId} (${companyName})`);
      } else {
        console.log(`  🔍 (DRY_RUN) 削除予定: ${targetDocId} (${companyName})`);
      }
    } else {
      console.log(`  ⚠️  削除対象が見つかりません: ${companyName}`);
    }
  }
  
  if (docIdsToDelete.length === 0) {
    console.log(`  ⚠️  削除対象のドキュメントが見つかりませんでした`);
  }
  
  // 2. CSVファイルを読み込み
  const filePath = path.resolve(TYPE_E_FILE);
  if (!fs.existsSync(filePath)) {
    console.error(`❌ ファイルが見つかりません: ${TYPE_E_FILE}`);
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
  
  console.log(`\n📄 ${path.basename(TYPE_E_FILE)}: ${records.length} 行`);
  
  // 3. 指定行を新規作成
  // CSVパーサーは columns: false で全行を読み込むので、records[0]がヘッダー行、records[1]が1行目
  // したがって、rowNum行目のデータは records[rowNum - 1] になる
  for (const rowNum of ROWS_TO_CREATE) {
    const rowIndex = rowNum - 1; // 0ベースインデックス（ヘッダー行を含む）
    
    if (rowIndex < 1 || rowIndex >= records.length) {
      console.warn(`⚠️  行 ${rowNum} は存在しません（総行数: ${records.length}, インデックス: ${rowIndex}）`);
      continue;
    }
    
    const row = records[rowIndex];
    console.log(`\n📋 行 ${rowNum} のデータを処理中...`);
    console.log(`  会社名（列0）: ${row[0] || '(空)'}`);
    const mapped = mapTypeERowByIndex(row);
    
    if (!mapped.name) {
      console.warn(`⚠️  行 ${rowNum}: 会社名がありません`);
      continue;
    }
    
    // 法人番号が13桁でない場合はnullにする
    if (mapped.corporateNumber && mapped.corporateNumber.length !== 13) {
      console.warn(`  ⚠️  行 ${rowNum}: 法人番号が13桁でないためnullに設定: "${mapped.corporateNumber}"`);
      mapped.corporateNumber = null;
    }
    
    const newData: Record<string, any> = {
      ...COMPANY_TEMPLATE,
      ...mapped,
      csvType: "type_e",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    
    // テスト用の文字列IDを生成
    const docId = generateTestDocId(mapped.corporateNumber, rowNum);
    const newRef = companiesCol.doc(docId);
    
    if (DRY_RUN) {
      console.log(`\n🆕 (DRY_RUN) 新規作成予定: 行 ${rowNum} (docId: ${docId})`);
      console.log(`  会社名: ${mapped.name}`);
      console.log(`  データ:`, JSON.stringify(newData, null, 2));
    } else {
      await newRef.set(newData);
      console.log(`\n✅ 新規作成完了: 行 ${rowNum} (docId: ${docId}, 会社名: ${mapped.name})`);
    }
  }
  
  console.log(`\n✅ 処理完了`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に実行するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

