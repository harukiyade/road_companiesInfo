/* 
  タイプG（127.csv）を削除して、27行目の「北日本水産物（株）」で再作成
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const TYPE_G_DOC_ID = "Aoh1ZtNAMbpCpV1GudQV";
const CSV_PATH = "csv/127.csv";
const TARGET_ROW = 27; // 27行目（ヘッダーを除く）

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
 * 既存のフィールド構造に準拠した空のテンプレート
 */
function getEmptyTemplate(): Record<string, any> {
  return {
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
    foundingDate: null,
    foundingYear: null,
    headquartersAddress: null,
    industries: [],
    industry: null,
    industryCategories: null,
    industryDetail: null,
    industryLarge: null,
    industryMiddle: null,
    industrySmall: null,
    linkedin: null,
    listing: null,
    marketSegment: null,
    metaDescription: null,
    metaKeywords: null,
    name: null,
    officeCount: null,
    overview: null,
    phoneNumber: null,
    postalCode: null,
    prefecture: null,
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
    salesNotes: null,
    shareholders: [],
    storeCount: null,
    suppliers: [],
    tags: [],
    updateCount: null,
    updatedAt: null,
    urls: [],
    wantedly: null,
    youtrust: null,
  };
}

/**
 * タイプG（127.csv）のデータをマッピング
 */
function mapTypeG(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  // 会社情報
  data.name = row["会社名"] || null;
  data.corporateNumber = row["法人番号"] || null;
  data.prefecture = row["都道府県"] || null;
  data.address = row["住所"] || null;
  data.headquartersAddress = row["住所"] || null;
  data.industry = row["業種"] || null;
  
  // 財務情報
  data.capitalStock = row["資本金"] ? parseFloat(String(row["資本金"]).replace(/,/g, "")) : null;
  data.revenue = row["売上"] ? parseFloat(String(row["売上"]).replace(/,/g, "")) : null;
  data.financials = row["直近利益"] || null;
  data.employeeCount = row["従業員数"] ? parseInt(String(row["従業員数"]).replace(/,/g, "")) : null;
  data.established = row["設立"] || null;
  data.fiscalMonth = row["決算月"] || null;
  data.listing = row["上場"] || null;
  
  // 代表者情報
  data.representativeName = row["代表者名"] || null;
  
  // 事業情報
  data.businessDescriptions = row["businessDescriptions"] || null;
  data.companyUrl = row["URL"] || null;
  data.overview = row["overview"] || null;
  
  // 銀行を配列に変換
  if (row["銀行"]) {
    const banksStr = String(row["銀行"]);
    const banksArr = banksStr.split(/[・、,]/).map(s => s.trim()).filter(s => s);
    (data as any).banks = banksArr;
  }
  
  // affiliations（所属団体）を配列に
  if (row["affiliations"]) {
    const affiliationsStr = String(row["affiliations"]);
    const affiliationsArr = affiliationsStr.split(/[、,]/).map(s => s.trim()).filter(s => s);
    (data as any).affiliations = affiliationsArr;
  }
  
  return data;
}

async function main() {
  console.log("================================================================================");
  console.log("タイプG: 削除と再追加（127.csv 27行目）");
  console.log("================================================================================");
  console.log();

  // 1. 既存のドキュメントを削除
  console.log("【STEP 1】既存のドキュメントを削除中...");
  try {
    const docRef = db.collection(COLLECTION_NAME).doc(TYPE_G_DOC_ID);
    const docSnap = await docRef.get();

    if (docSnap.exists) {
      const oldData = docSnap.data();
      console.log(`削除対象: ${oldData?.name}`);
      await docRef.delete();
      console.log(`✓ 削除完了 (ID: ${TYPE_G_DOC_ID})`);
    } else {
      console.log(`⚠️  ドキュメントが見つかりません (ID: ${TYPE_G_DOC_ID})`);
    }
  } catch (error: any) {
    console.error(`❌ 削除エラー: ${error.message}`);
    throw error;
  }

  console.log();

  // 2. CSVから27行目のデータを取得（手動パース）
  console.log(`【STEP 2】127.csvから${TARGET_ROW}行目のデータを読み込み中...`);
  const csvContent = fs.readFileSync(CSV_PATH, "utf-8");
  const lines = csvContent.split("\n");
  
  if (lines.length < TARGET_ROW) {
    throw new Error(`${CSV_PATH}: ${TARGET_ROW}行目のデータが見つかりません（総行数: ${lines.length}）`);
  }
  
  // ヘッダー行を取得
  const headerLine = lines[0];
  const headers = headerLine.split(",");
  
  // 27行目のデータ行を取得
  const dataLine = lines[TARGET_ROW - 1];
  
  // CSVの値を手動でパース（カンマ区切り、ただし引用符内のカンマは考慮）
  const values: string[] = [];
  let currentValue = "";
  let inQuotes = false;
  
  for (let i = 0; i < dataLine.length; i++) {
    const char = dataLine[i];
    
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      values.push(currentValue.trim());
      currentValue = "";
    } else {
      currentValue += char;
    }
  }
  values.push(currentValue.trim()); // 最後の値
  
  // ヘッダーと値をマッピング
  const row: Record<string, any> = {};
  headers.forEach((header, index) => {
    row[header.trim()] = values[index] || null;
  });
  console.log(`✓ データ取得完了`);
  console.log(`  【CSVの内容（27行目）】`);
  console.log(`  会社名: ${row["会社名"]}`);
  console.log(`  会社名（英語）: ${row["会社名（英語）"]}`);
  console.log(`  法人番号: ${row["法人番号"]}`);
  console.log(`  都道府県: ${row["都道府県"]}`);
  console.log(`  住所: ${row["住所"]}`);
  console.log(`  業種: ${row["業種"]}`);
  console.log(`  資本金: ${row["資本金"]}`);
  console.log(`  売上: ${row["売上"]}`);
  console.log(`  直近利益: ${row["直近利益"]}`);
  console.log(`  従業員数: ${row["従業員数"]}`);
  console.log(`  設立: ${row["設立"]}`);
  console.log(`  決算月: ${row["決算月"]}`);
  console.log(`  上場: ${row["上場"]}`);
  console.log(`  代表者名: ${row["代表者名"]}`);
  console.log(`  businessDescriptions: ${row["businessDescriptions"]}`);
  console.log(`  URL: ${row["URL"]}`);
  console.log(`  銀行: ${row["銀行"] || "(なし)"}`);
  console.log(`  affiliations: ${row["affiliations"] || "(なし)"}`);

  console.log();

  // 3. データをマッピング
  console.log("【STEP 3】データをマッピング中...");
  const companyData = mapTypeG(row);
  
  console.log(`✓ マッピング完了`);
  console.log(`  企業名: ${companyData.name}`);
  console.log(`  法人番号: ${companyData.corporateNumber}`);
  console.log(`  都道府県: ${companyData.prefecture}`);
  console.log(`  住所: ${companyData.address}`);
  console.log(`  業種: ${companyData.industry}`);
  console.log(`  資本金: ${companyData.capitalStock}`);
  console.log(`  売上: ${companyData.revenue}`);
  console.log(`  従業員数: ${companyData.employeeCount}`);
  console.log(`  設立: ${companyData.established}`);
  console.log(`  決算月: ${companyData.fiscalMonth}`);
  console.log(`  上場: ${companyData.listing}`);
  console.log(`  代表者名: ${companyData.representativeName}`);
  console.log(`  営業種目: ${companyData.businessDescriptions}`);
  console.log(`  URL: ${companyData.companyUrl}`);
  console.log(`  銀行: ${JSON.stringify((companyData as any).banks)}`);

  console.log();

  // 4. Firestoreに新規追加
  console.log("【STEP 4】Firestoreに追加中...");
  const docRef = await db.collection(COLLECTION_NAME).add(companyData);
  console.log(`✓ 追加完了`);
  console.log(`  新しいドキュメントID: ${docRef.id}`);

  console.log();

  // 5. 確認
  console.log("【STEP 5】追加したデータを確認中...");
  const newDocSnap = await docRef.get();
  const newData = newDocSnap.data();
  
  if (newData) {
    console.log(`✓ データ確認完了`);
    console.log();
    console.log("【フィールド確認】");
    console.log(`  企業名: ${newData.name}`);
    console.log(`  法人番号: ${newData.corporateNumber}`);
    console.log(`  都道府県: ${newData.prefecture}`);
    console.log(`  住所: ${newData.address}`);
    console.log(`  業種: ${newData.industry}`);
    console.log(`  資本金: ${newData.capitalStock}`);
    console.log(`  売上: ${newData.revenue}`);
    console.log(`  直近利益: ${newData.financials}`);
    console.log(`  従業員数: ${newData.employeeCount}`);
    console.log(`  設立: ${newData.established}`);
    console.log(`  決算月: ${newData.fiscalMonth}`);
    console.log(`  上場: ${newData.listing}`);
    console.log(`  代表者名: ${newData.representativeName}`);
    console.log(`  営業種目: ${newData.businessDescriptions}`);
    console.log(`  URL: ${newData.companyUrl}`);
    console.log(`  銀行: ${JSON.stringify((newData as any).banks)}`);
    console.log();
    
    console.log("🎉 タイプG（127.csv 27行目）の作成が完了しました！");
  }

  console.log("\n================================================================================");
  console.log("完了");
  console.log("================================================================================");
  console.log(`\n【新しいタイプGのドキュメントID】`);
  console.log(`タイプG: ${docRef.id}`);
  console.log(`CSVソース: csv/127.csv（27行目）`);
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

