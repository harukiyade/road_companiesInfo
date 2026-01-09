/*
  タイプF（124.csv）の特定ドキュメントを削除し、指定行を新規作成するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_and_recreate_type_f_124.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentReference } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const TYPE_F_FILE = "csv/124.csv";
// 削除対象のドキュメントID
const DOC_ID_TO_DELETE = "Da1bklitrNuy1PRFWaLS";
// テストとして追加する行（CSVの行番号、1ベース）
const ROWS_TO_CREATE = [4, 5, 7, 9]; // 4行目、5行目、7行目、9行目

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
  // 3桁-4桁の郵便番号形式をチェック
  const cleaned = trimmed.replace(/[,\-\s]/g, "");
  return /^\d{3,4}$/.test(cleaned) || /^\d{7}$/.test(cleaned);
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

// タイプFの行データを列インデックスに基づいてマッピング
// 列順: 会社名・都道府県・代表者名・取引種別・SBフラグ・NDA・AD・ステータス・備考・URL・業種1・業種2・業種3・業種4・業種5・業種6・業種7・郵便番号・住所・設立・電話番号(窓口)・代表者郵便番号・代表者住所・代表者誕生日・資本金・上場・直近決算年月・直近売上・直近利益・説明・概要・仕入れ先・取引先・取引先銀行・取締役・株主・社員数・オフィス数・工場数・店舗数
function mapTypeFRowByIndex(row: Array<string>): Record<string, any> {
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
  
  // 4-9. 取引種別・SBフラグ・NDA・AD・ステータス・備考（無視）
  colIndex += 6;
  
  // 10. URL
  if (row[colIndex]) mapped.companyUrl = trim(row[colIndex]);
  colIndex++;
  
  // 11. 業種1
  if (row[colIndex]) mapped.industryLarge = trim(row[colIndex]);
  colIndex++;
  
  // 12. 業種2
  if (row[colIndex]) mapped.industryMiddle = trim(row[colIndex]);
  colIndex++;
  
  // 13. 業種3
  if (row[colIndex]) mapped.industrySmall = trim(row[colIndex]);
  colIndex++;
  
  // 14-17. 業種4・業種5・業種6・業種7の処理（動的判定）
  // industryCategoriesを初期化
  mapped.industryCategories = [];
  
  // 業種4の位置をチェック
  const industry4Value = row[colIndex] ? trim(row[colIndex]) : null;
  if (industry4Value && !isNumericValue(industry4Value)) {
    // 業種4がある（非数値）
    mapped.industryDetail = industry4Value;
    colIndex++;
    
    // 業種5の位置をチェック
    const industry5Value = row[colIndex] ? trim(row[colIndex]) : null;
    if (industry5Value) {
      if (isNumericValue(industry5Value)) {
        // 業種5の位置に郵便番号が来た（業種5と6と7はない）
        const postalCode = validatePostalCode(industry5Value);
        mapped.postalCode = postalCode; // 7桁でない場合はnull
        colIndex++;
      } else {
        // 業種5がある（非数値）
        mapped.industryCategories.push(industry5Value);
        colIndex++;
        
        // 業種6の位置をチェック
        const industry6Value = row[colIndex] ? trim(row[colIndex]) : null;
        if (industry6Value) {
          if (isNumericValue(industry6Value)) {
            // 業種6の位置に郵便番号が来た（業種6と7はない）
            const postalCode = validatePostalCode(industry6Value);
            mapped.postalCode = postalCode; // 7桁でない場合はnull
            colIndex++;
          } else {
            // 業種6がある（非数値）
            mapped.industryCategories.push(industry6Value);
            colIndex++;
            
            // 業種7の位置をチェック
            const industry7Value = row[colIndex] ? trim(row[colIndex]) : null;
            if (industry7Value) {
              if (isNumericValue(industry7Value)) {
                // 業種7の位置に郵便番号が来た（業種7はない）
                const postalCode = validatePostalCode(industry7Value);
                mapped.postalCode = postalCode; // 7桁でない場合はnull
                colIndex++;
              } else {
                // 業種7がある（非数値）
                mapped.industryCategories.push(industry7Value);
                colIndex++;
                
                // 次の位置が郵便番号
                if (row[colIndex]) {
                  const postalCode = validatePostalCode(row[colIndex]);
                  mapped.postalCode = postalCode; // 7桁でない場合はnull
                }
                colIndex++;
              }
            } else {
              // 業種7がない場合、次の位置が郵便番号
              if (row[colIndex]) {
                const postalCode = validatePostalCode(row[colIndex]);
                mapped.postalCode = postalCode; // 7桁でない場合はnull
              }
              colIndex++;
            }
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
    } else {
      // 業種5がない場合、次の位置が郵便番号
      if (row[colIndex]) {
        const postalCode = validatePostalCode(row[colIndex]);
        mapped.postalCode = postalCode; // 7桁でない場合はnull
      }
      colIndex++;
    }
  } else {
    // 業種4がない、または業種4の位置に数値が来た
    if (industry4Value && isNumericValue(industry4Value)) {
      // 業種4の位置に郵便番号が来た（業種4と5と6と7はない）
      const postalCode = validatePostalCode(industry4Value);
      mapped.postalCode = postalCode; // 7桁でない場合はnull
      colIndex++;
    } else {
      // 業種4がない場合、次の位置が郵便番号
      if (row[colIndex]) {
        const postalCode = validatePostalCode(row[colIndex]);
        mapped.postalCode = postalCode; // 7桁でない場合はnull
      }
      colIndex++;
    }
  }
  
  // 18. 住所（郵便番号の次）
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
  
  // 19. 設立
  if (row[colIndex]) mapped.established = trim(row[colIndex]);
  colIndex++;
  
  // 20. 電話番号(窓口)
  if (row[colIndex]) mapped.phoneNumber = trim(row[colIndex]);
  colIndex++;
  
  // 21. 代表者郵便番号
  if (row[colIndex]) {
    const postalCode = validatePostalCode(row[colIndex]);
    if (postalCode) {
      mapped.representativePostalCode = postalCode;
    }
  }
  colIndex++;
  
  // 22. 代表者住所
  if (row[colIndex]) mapped.representativeHomeAddress = trim(row[colIndex]);
  colIndex++;
  
  // 23. 代表者誕生日
  if (row[colIndex]) mapped.representativeBirthDate = trim(row[colIndex]);
  colIndex++;
  
  // 24. 資本金
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
  
  // 25. 上場
  if (row[colIndex]) mapped.listing = trim(row[colIndex]);
  colIndex++;
  
  // 26. 直近決算年月
  if (row[colIndex]) {
    mapped.latestFiscalYearMonth = trim(row[colIndex]);
  }
  colIndex++;
  
  // 27. 直近売上
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.revenue = num;
  }
  colIndex++;
  
  // 28. 直近利益
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.latestProfit = num;
  }
  colIndex++;
  
  // 29. 説明
  if (row[colIndex]) mapped.companyDescription = trim(row[colIndex]);
  colIndex++;
  
  // 30. 概要
  if (row[colIndex]) mapped.overview = trim(row[colIndex]);
  colIndex++;
  
  // 31. 仕入れ先
  if (row[colIndex]) {
    const suppliersValue = trim(row[colIndex]);
    if (suppliersValue) {
      // 配列として保存（カンマ区切りの場合は分割）
      mapped.suppliers = suppliersValue.split(/[，,]/).map(s => s.trim()).filter(s => s);
    }
  }
  colIndex++;
  
  // 32. 取引先
  if (row[colIndex]) mapped.clients = trim(row[colIndex]);
  colIndex++;
  
  // 33. 取引先銀行
  if (row[colIndex]) {
    const banksValue = trim(row[colIndex]);
    if (banksValue) {
      // 配列として保存（カンマ区切りの場合は分割）
      mapped.banks = banksValue.split(/[，,]/).map(s => s.trim()).filter(s => s);
    }
  }
  colIndex++;
  
  // 34. 取締役
  if (row[colIndex]) mapped.executives = trim(row[colIndex]);
  colIndex++;
  
  // 35. 株主
  if (row[colIndex]) {
    const shareholdersValue = trim(row[colIndex]);
    if (shareholdersValue) {
      mapped.shareholders = shareholdersValue;
    }
  }
  colIndex++;
  
  // 36. 社員数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.employeeCount = num;
  }
  colIndex++;
  
  // 37. オフィス数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.officeCount = num;
  }
  colIndex++;
  
  // 38. 工場数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.factoryCount = num;
  }
  colIndex++;
  
  // 39. 店舗数
  if (row[colIndex]) {
    const num = parseNumeric(row[colIndex]);
    if (num !== null) mapped.storeCount = num;
  }
  colIndex++;
  
  return mapped;
}

function generateNumericDocId(corporateNumber: string | null, rowIndex: number): string {
  // corporateNumberが存在し、数字のみの場合 → そのまま使用
  if (corporateNumber && /^[0-9]+$/.test(corporateNumber.trim())) {
    return corporateNumber.trim();
  }

  // それ以外の場合 → Date.now() + 行番号から数字のみの一意IDを生成
  const timestamp = Date.now();
  const paddedIndex = String(rowIndex).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
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
  industryCategories: [],
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
  
  // 1. 既存ドキュメントを削除（ドキュメントIDで直接削除）
  console.log(`🗑️  既存ドキュメントを削除（docId: ${DOC_ID_TO_DELETE}）...`);
  const docRef = companiesCol.doc(DOC_ID_TO_DELETE);
  const docSnapshot = await docRef.get();
  
  if (docSnapshot.exists) {
    if (!DRY_RUN) {
      await docRef.delete();
      console.log(`  ✅ 削除完了: ${DOC_ID_TO_DELETE}`);
    } else {
      console.log(`  🔍 (DRY_RUN) 削除予定: ${DOC_ID_TO_DELETE}`);
    }
  } else {
    console.log(`  ⚠️  削除対象のドキュメントが見つかりません: ${DOC_ID_TO_DELETE}`);
  }
  
  // 2. CSVファイルを読み込み
  const filePath = path.resolve(TYPE_F_FILE);
  if (!fs.existsSync(filePath)) {
    console.error(`❌ ファイルが見つかりません: ${TYPE_F_FILE}`);
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
  
  console.log(`\n📄 ${path.basename(TYPE_F_FILE)}: ${records.length} 行`);
  
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
    const mapped = mapTypeFRowByIndex(row);
    
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
      csvType: "type_f",
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
      updatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };
    
    // 数値IDを生成
    const docId = generateNumericDocId(mapped.corporateNumber, rowNum);
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

