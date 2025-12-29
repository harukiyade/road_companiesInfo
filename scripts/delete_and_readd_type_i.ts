/* 
  タイプI（132.csv）を削除して、34行目の「TusHoldings Co., Ltd.,」で再作成
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const TYPE_I_DOC_ID = "rfODM79w8VPGnadmd8yy";
const CSV_PATH = "csv/132.csv";
const TARGET_ROW = 35; // 35行目（ヘッダーを除く）

// Firebase初期化
if (!admin.apps.length) {
  const serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || 
    path.join(__dirname, "../albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
  
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccountPath)
  });
}

const db: Firestore = admin.firestore();

/**
 * 既存のフィールド構造に準拠した空のテンプレート（companies_newコレクションの完全なフィールド構造）
 */
function getEmptyTemplate(): Record<string, any> {
  return {
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
    dateOfEstablishment: null,
    demandProducts: null,
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
    linkedin: null,
    listing: null,
    location: null,
    marketSegment: null,
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
    profit1: null,
    profit2: null,
    profit3: null,
    profit4: null,
    profit5: null,
    qualificationGrade: null,
    registrant: null,
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
}

/**
 * タイプI（132.csv）のデータをマッピング
 */
function mapTypeI(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.prefecture = row["都道府県"] || null;
  data.representativeName = row["代表者名"] || null;
  data.corporateNumber = row["法人番号"] || null;
  // URLが空の場合は「説明」フィールドから取得
  data.companyUrl = row["URL"] || row["説明"] || null;
  data.industry = row["業種1"] || null;
  data.postalCode = row["郵便番号"] || null;
  data.address = row["住所"] || null;
  data.headquartersAddress = row["住所"] || null;
  data.established = row["設立"] || null;
  data.phoneNumber = row["電話番号(窓口)"] || null;
  data.representativeRegisteredAddress = row["代表者郵便番号"] || null;
  data.representativeHomeAddress = row["代表者住所"] || null;
  data.representativeBirthDate = row["代表者誕生日"] || null;
  // 説明フィールドがURLでない場合は、companyDescriptionとして保存
  if (row["説明"] && !row["説明"].startsWith("http")) {
    data.companyDescription = row["説明"] || null;
  }
  // 概要フィールド
  data.overview = row["概要"] || null;
  // 取締役情報
  if (row["取締役"]) {
    data.executives = row["取締役"] || null;
  }
  // 上場情報
  data.listing = row["上場"] || null;
  // 資本金（0も有効な値として扱う）
  if (row["資本金"] !== undefined && row["資本金"] !== null && String(row["資本金"]).trim() !== "") {
    const capitalStr = String(row["資本金"]).replace(/,/g, "").trim();
    const capitalValue = parseFloat(capitalStr);
    if (!isNaN(capitalValue)) {
      data.capitalStock = capitalValue;
    }
  }
  // 従業員数（0も有効な値として扱う）
  if (row["社員数"] !== undefined && row["社員数"] !== null && String(row["社員数"]).trim() !== "") {
    const employeeStr = String(row["社員数"]).replace(/,/g, "").trim();
    const employeeValue = parseInt(employeeStr);
    if (!isNaN(employeeValue)) {
      data.employeeCount = employeeValue;
    }
  }
  // オフィス数、工場数、店舗数（0も有効な値として扱う）
  if (row["オフィス数"] !== undefined && row["オフィス数"] !== null && String(row["オフィス数"]).trim() !== "") {
    const officeStr = String(row["オフィス数"]).replace(/,/g, "").trim();
    const officeValue = parseInt(officeStr);
    if (!isNaN(officeValue)) {
      data.officeCount = officeValue;
    }
  }
  if (row["工場数"] !== undefined && row["工場数"] !== null && String(row["工場数"]).trim() !== "") {
    const factoryStr = String(row["工場数"]).replace(/,/g, "").trim();
    const factoryValue = parseInt(factoryStr);
    if (!isNaN(factoryValue)) {
      data.factoryCount = factoryValue;
    }
  }
  if (row["店舗数"] !== undefined && row["店舗数"] !== null && String(row["店舗数"]).trim() !== "") {
    const storeStr = String(row["店舗数"]).replace(/,/g, "").trim();
    const storeValue = parseInt(storeStr);
    if (!isNaN(storeValue)) {
      data.storeCount = storeValue;
    }
  }
  
  // 最新の決算情報を取得
  if (row["決算月1"]) data.fiscalMonth = row["決算月1"];
  if (row["売上1"]) data.revenue = parseFloat(String(row["売上1"]).replace(/,/g, ""));
  if (row["利益1"]) data.financials = row["利益1"];
  
  return data;
}

async function main() {
  console.log("================================================================================");
  console.log("タイプI: 削除と再追加（132.csv 35行目 - 株式会社Financial Well‐being Japan）");
  console.log("================================================================================");
  console.log();

  // 1. 既存のドキュメントを削除
  console.log("【STEP 1】既存のドキュメントを削除中...");
  try {
    const docRef = db.collection(COLLECTION_NAME).doc(TYPE_I_DOC_ID);
    const docSnap = await docRef.get();

    if (docSnap.exists) {
      const oldData = docSnap.data();
      console.log(`削除対象: ${oldData?.name || "(名前なし)"} (ID: ${TYPE_I_DOC_ID})`);
      await docRef.delete();
      console.log(`✓ 削除完了 (ID: ${TYPE_I_DOC_ID})`);
    } else {
      console.log(`⚠️  ドキュメントが見つかりません (ID: ${TYPE_I_DOC_ID})`);
    }
  } catch (error: any) {
    console.error(`❌ 削除エラー: ${error.message}`);
    throw error;
  }

  console.log();

  // 2. CSVから34行目のデータを取得
  console.log(`【STEP 2】${CSV_PATH}から${TARGET_ROW}行目のデータを読み込み中...`);
  const csvContent = fs.readFileSync(CSV_PATH, "utf-8");
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
  }) as Record<string, any>[];
  
  if (records.length < TARGET_ROW - 1) {
    throw new Error(`${CSV_PATH}: ${TARGET_ROW}行目のデータが見つかりません（総行数: ${records.length}）`);
  }
  
  // 34行目のデータを取得（ヘッダー行を除くので、34行目はインデックス33）
  const row = records[TARGET_ROW - 2]; // ヘッダー行を除くので-2（34行目 = インデックス33）
  
  console.log(`✓ データ取得完了`);
  console.log(`  【CSVの内容（35行目）】`);
  console.log(`  会社名: ${row["会社名"]}`);
  console.log(`  都道府県: ${row["都道府県"]}`);
  console.log(`  代表者名: ${row["代表者名"]}`);
  console.log(`  法人番号: ${row["法人番号"] || "(なし)"}`);
  console.log(`  住所: ${row["住所"]}`);
  console.log(`  郵便番号: ${row["郵便番号"] || "(なし)"}`);
  console.log(`  電話番号(窓口): ${row["電話番号(窓口)"] || "(なし)"}`);
  console.log(`  URL: ${row["URL"] || "(なし)"}`);
  console.log(`  業種1: ${row["業種1"] || "(なし)"}`);
  console.log(`  設立: ${row["設立"] || "(なし)"}`);
  console.log(`  決算月1: ${row["決算月1"] || "(なし)"}`);
  console.log(`  売上1: ${row["売上1"] || "(なし)"}`);
  console.log(`  利益1: ${row["利益1"] || "(なし)"}`);
  console.log(`  上場: ${row["上場"] || "(なし)"}`);
  console.log(`  資本金: ${row["資本金"] || "(なし)"}`);
  console.log(`  社員数: ${row["社員数"] || "(なし)"}`);

  console.log();

  // 3. データをマッピング
  console.log("【STEP 3】データをマッピング中...");
  const companyData = mapTypeI(row);
  
  console.log(`✓ マッピング完了`);
  console.log(`  企業名: ${companyData.name}`);
  console.log(`  都道府県: ${companyData.prefecture}`);
  console.log(`  代表者名: ${companyData.representativeName}`);
  console.log(`  法人番号: ${companyData.corporateNumber || "(なし)"}`);
  console.log(`  住所: ${companyData.address}`);
  console.log(`  郵便番号: ${companyData.postalCode || "(なし)"}`);
  console.log(`  電話番号: ${companyData.phoneNumber || "(なし)"}`);
  console.log(`  URL: ${companyData.companyUrl || "(なし)"}`);
  console.log(`  業種: ${companyData.industry || "(なし)"}`);
  console.log(`  設立: ${companyData.established || "(なし)"}`);
  console.log(`  決算月: ${companyData.fiscalMonth || "(なし)"}`);
  console.log(`  売上: ${companyData.revenue || "(なし)"}`);
  console.log(`  利益: ${companyData.financials || "(なし)"}`);
  console.log(`  上場: ${companyData.listing || "(なし)"}`);
  console.log(`  資本金: ${companyData.capitalStock !== null && companyData.capitalStock !== undefined ? companyData.capitalStock : "(なし)"}`);
  console.log(`  社員数: ${companyData.employeeCount !== null && companyData.employeeCount !== undefined ? companyData.employeeCount : "(なし)"}`);

  console.log();

  // 4. Firestoreに同じIDで新規作成
  console.log("【STEP 4】Firestoreに同じIDで新規作成中...");
  const docRef = db.collection(COLLECTION_NAME).doc(TYPE_I_DOC_ID);
  await docRef.set(companyData);
  console.log(`✓ 作成完了`);
  console.log(`  ドキュメントID: ${TYPE_I_DOC_ID}`);

  console.log();

  // 5. 確認
  console.log("【STEP 5】作成したデータを確認中...");
  const newDocSnap = await docRef.get();
  const newData = newDocSnap.data();
  
  if (newData) {
    console.log(`✓ データ確認完了`);
    console.log();
    console.log("【フィールド確認】");
    console.log(`  企業名: ${newData.name}`);
    console.log(`  都道府県: ${newData.prefecture}`);
    console.log(`  代表者名: ${newData.representativeName}`);
    console.log(`  法人番号: ${newData.corporateNumber || "(なし)"}`);
    console.log(`  住所: ${newData.address}`);
    console.log(`  郵便番号: ${newData.postalCode || "(なし)"}`);
    console.log(`  電話番号: ${newData.phoneNumber || "(なし)"}`);
    console.log(`  URL: ${newData.companyUrl || "(なし)"}`);
    console.log(`  業種: ${newData.industry || "(なし)"}`);
    console.log(`  設立: ${newData.established || "(なし)"}`);
    console.log(`  決算月: ${newData.fiscalMonth || "(なし)"}`);
    console.log(`  売上: ${newData.revenue || "(なし)"}`);
    console.log(`  利益: ${newData.financials || "(なし)"}`);
    console.log(`  上場: ${newData.listing || "(なし)"}`);
    console.log(`  資本金: ${newData.capitalStock !== null && newData.capitalStock !== undefined ? newData.capitalStock : "(なし)"}`);
    console.log(`  社員数: ${newData.employeeCount !== null && newData.employeeCount !== undefined ? newData.employeeCount : "(なし)"}`);
    console.log();
    
    console.log("🎉 タイプI（132.csv 35行目 - 株式会社Financial Well‐being Japan）の作成が完了しました！");
  } else {
    console.error("❌ データの確認に失敗しました");
  }

  console.log("\n================================================================================");
  console.log("完了");
  console.log("================================================================================");
  console.log(`\n【タイプIのドキュメントID】`);
  console.log(`タイプI: ${TYPE_I_DOC_ID}`);
  console.log(`CSVソース: ${CSV_PATH}（35行目）`);
  console.log(`企業名: ${companyData.name}`);
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

