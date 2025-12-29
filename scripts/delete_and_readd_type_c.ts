/* 
  タイプC（105.csv）を削除して、正しくマッピングして再作成
  
  105.csvの構造:
  会社名,電話番号,郵便番号,住所,URL,代表者,郵便番号,住所,創業,,設立,株式保有率,役員,概要,業種（大）,業種（細）,業種（中）,業種（小）,業種（細）
  
  注意: 「郵便番号」「住所」「業種（細）」が重複、「創業」の後に空列がある
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const TYPE_C_DOC_ID = "RDUjjfEINYCGkEtxdu6y";
const CSV_PATH = "csv/105.csv";

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
 * 105.csvを行単位で手動パース（重複ヘッダー対応）
 */
function parseCSV105(csvPath: string): Record<string, any> | null {
  try {
    const csvContent = fs.readFileSync(csvPath, "utf-8");
    const lines = csvContent.split("\n");
    
    if (lines.length < 2) {
      return null;
    }
    
    // 2行目のデータ（1行目はヘッダー）
    const dataLine = lines[1];
    const values = dataLine.split(",");
    
    // 手動でインデックスを指定してマッピング
    return {
      "会社名": values[0] || null,
      "電話番号": values[1] || null,
      "会社郵便番号": values[2] || null,
      "会社住所": values[3] || null,
      "URL": values[4] || null,
      "代表者": values[5] || null,
      "代表者郵便番号": values[6] || null,
      "代表者住所": values[7] || null,
      "創業": values[8] || null,
      "営業種目": values[9] || null, // 空のヘッダー列
      "設立": values[10] || null,
      "株式保有率": values[11] || null,
      "役員": values[12] || null,
      "概要": values[13] || null,
      "業種（大）": values[14] || null,
      "業種（細）1": values[15] || null,
      "業種（中）": values[16] || null,
      "業種（小）": values[17] || null,
      "業種（細）2": values[18] || null,
    };
  } catch (error: any) {
    console.error(`❌ ${csvPath}: 読み込みエラー - ${error.message}`);
    return null;
  }
}

/**
 * タイプC（105.csv）のデータをマッピング
 */
function mapTypeC(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  // 会社情報
  data.name = row["会社名"] || null;
  data.phoneNumber = row["電話番号"] || null;
  data.postalCode = row["会社郵便番号"] || null;
  data.address = row["会社住所"] || null;
  data.headquartersAddress = row["会社住所"] || null;
  data.companyUrl = row["URL"] || null;
  
  // 代表者情報
  data.representativeName = row["代表者"] || null;
  data.representativePostalCode = row["代表者郵便番号"] || null;
  data.representativeRegisteredAddress = row["代表者住所"] || null;
  data.representativeHomeAddress = row["代表者住所"] || null;
  
  // 事業情報
  data.foundingYear = row["創業"] ? parseInt(String(row["創業"]).substring(0, 4)) : null;
  data.businessDescriptions = row["営業種目"] || null;
  data.established = row["設立"] || null;
  data.executives = row["役員"] || null;
  data.overview = row["概要"] || null;
  
  // 業種情報
  data.industryLarge = row["業種（大）"] || null;
  data.industryMiddle = row["業種（中）"] || null;
  data.industrySmall = row["業種（小）"] || null;
  data.industryDetail = row["業種（細）1"] || row["業種（細）2"] || null;
  
  // 株主を配列に変換
  if (row["株式保有率"]) {
    const shareholders = String(row["株式保有率"]).split(/[，,]/).map(s => s.trim()).filter(s => s);
    data.shareholders = shareholders;
  }
  
  return data;
}

async function main() {
  console.log("================================================================================");
  console.log("タイプC: 削除と再追加（105.csv - 正しいマッピング版）");
  console.log("================================================================================");
  console.log();

  // 1. 既存のドキュメントを削除
  console.log("【STEP 1】既存のドキュメントを削除中...");
  try {
    const docRef = db.collection(COLLECTION_NAME).doc(TYPE_C_DOC_ID);
    const docSnap = await docRef.get();

    if (docSnap.exists) {
      const oldData = docSnap.data();
      console.log(`削除対象: ${oldData?.name}`);
      await docRef.delete();
      console.log(`✓ 削除完了 (ID: ${TYPE_C_DOC_ID})`);
    } else {
      console.log(`⚠️  ドキュメントが見つかりません (ID: ${TYPE_C_DOC_ID})`);
    }
  } catch (error: any) {
    console.error(`❌ 削除エラー: ${error.message}`);
    throw error;
  }

  console.log();

  // 2. CSVから最初の企業データを取得（手動パース）
  console.log("【STEP 2】105.csvからデータを読み込み中（手動パース）...");
  const row = parseCSV105(CSV_PATH);
  
  if (!row) {
    throw new Error(`${CSV_PATH}: データ取得失敗`);
  }

  console.log(`✓ データ取得完了`);
  console.log(`  【CSVの内容】`);
  console.log(`  会社名: ${row["会社名"]}`);
  console.log(`  電話番号: ${row["電話番号"]}`);
  console.log(`  会社郵便番号: ${row["会社郵便番号"]}`);
  console.log(`  会社住所: ${row["会社住所"]}`);
  console.log(`  URL: ${row["URL"]}`);
  console.log(`  代表者: ${row["代表者"]}`);
  console.log(`  代表者郵便番号: ${row["代表者郵便番号"]}`);
  console.log(`  代表者住所: ${row["代表者住所"]}`);
  console.log(`  創業: ${row["創業"]}`);
  console.log(`  営業種目: ${row["営業種目"]}`);
  console.log(`  設立: ${row["設立"]}`);
  console.log(`  株式保有率: ${row["株式保有率"]}`);
  console.log(`  役員: ${row["役員"]}`);
  console.log(`  概要: ${row["概要"]}`);
  console.log(`  業種（大）: ${row["業種（大）"]}`);
  console.log(`  業種（細）1: ${row["業種（細）1"]}`);

  console.log();

  // 3. データをマッピング
  console.log("【STEP 3】データをマッピング中...");
  const companyData = mapTypeC(row);
  
  console.log(`✓ マッピング完了`);

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
    console.log(`  電話番号: ${newData.phoneNumber}`);
    console.log(`  会社郵便番号: ${newData.postalCode}`);
    console.log(`  会社住所: ${newData.address}`);
    console.log(`  URL: ${newData.companyUrl}`);
    console.log(`  代表者名: ${newData.representativeName}`);
    console.log(`  代表者郵便番号: ${newData.representativePostalCode}`);
    console.log(`  代表者住所（登録）: ${newData.representativeRegisteredAddress}`);
    console.log(`  代表者住所（自宅）: ${newData.representativeHomeAddress}`);
    console.log(`  創業年: ${newData.foundingYear}`);
    console.log(`  営業種目: ${newData.businessDescriptions}`);
    console.log(`  設立: ${newData.established}`);
    console.log(`  株主: ${JSON.stringify(newData.shareholders)}`);
    console.log(`  役員: ${newData.executives}`);
    console.log(`  概要: ${newData.overview}`);
    console.log(`  業種（大）: ${newData.industryLarge}`);
    console.log(`  業種（中）: ${newData.industryMiddle}`);
    console.log(`  業種（小）: ${newData.industrySmall}`);
    console.log(`  業種（細）: ${newData.industryDetail}`);
    console.log();
    
    console.log("🎉 タイプC（105.csv）の正しいマッピングが完了しました！");
  }

  console.log("\n================================================================================");
  console.log("完了");
  console.log("================================================================================");
  console.log(`\n【新しいタイプCのドキュメントID】`);
  console.log(`タイプC: ${docRef.id}`);
  console.log(`CSVソース: csv/105.csv`);
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

