/* 
  タイプD（111.csv）を削除して、修正版を再作成
  
  修正内容：
  1. 法人番号が13桁でない場合はnullに
  2. 業種1〜4を正しくマッピング
  3. 直近決算年月を追加
  4. 取引先銀行をsuppliersから分離してbanksに
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const TYPE_D_DOC_ID = "J6NpQJNdKQKximg7Ddb8";
const CSV_PATH = "csv/111.csv";

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
 * 法人番号が有効な13桁かチェック
 */
function validateCorporateNumber(value: any): string | null {
  if (!value) return null;
  
  const str = String(value).replace(/\s/g, "");
  
  // 科学的表記（9.18E+12など）や13桁でない場合はnull
  if (str.includes("E") || str.includes("e")) {
    return null;
  }
  
  // 13桁の数字でない場合はnull
  if (!/^\d{13}$/.test(str)) {
    return null;
  }
  
  return str;
}

/**
 * タイプD（111.csv）のデータをマッピング
 */
function mapTypeD(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  // 会社情報
  data.name = row["会社名"] || null;
  data.prefecture = row["都道府県"] || null;
  data.representativeName = row["代表者名"] || null;
  
  // 法人番号（13桁チェック）
  data.corporateNumber = validateCorporateNumber(row["法人番号"]);
  
  data.companyUrl = row["URL"] || null;
  data.postalCode = row["郵便番号"] || null;
  data.address = row["住所"] || null;
  data.headquartersAddress = row["住所"] || null;
  data.established = row["設立"] || null;
  data.phoneNumber = row["電話番号(窓口)"] || null;
  data.representativePostalCode = row["代表者郵便番号"] || null;
  data.representativeRegisteredAddress = row["代表者住所"] || null;
  data.representativeHomeAddress = row["代表者住所"] || null;
  data.representativeBirthDate = row["代表者誕生日"] || null;
  
  // 財務情報
  data.capitalStock = row["資本金"] ? parseFloat(String(row["資本金"]).replace(/,/g, "")) : null;
  data.listing = row["上場"] || null;
  data.fiscalMonth = row["直近決算年月"] || null; // 追加
  data.revenue = row["直近売上"] ? parseFloat(String(row["直近売上"]).replace(/,/g, "")) : null;
  data.financials = row["直近利益"] || null;
  
  // 業種情報（業種1〜4を正しくマッピング）
  data.industry = row["業種1"] || null;
  
  const industriesArr: string[] = [];
  if (row["業種2"]) industriesArr.push(row["業種2"]);
  if (row["業種3"]) industriesArr.push(row["業種3"]);
  if (row["業種4"]) industriesArr.push(row["業種4"]);
  data.industries = industriesArr;
  
  // 説明・概要
  data.companyDescription = row["説明"] || null;
  data.overview = row["概要"] || null;
  
  // 取引先情報
  data.clients = row["取引先"] || null;
  data.executives = row["取締役"] || null;
  
  // 仕入れ先（取引先銀行は含めない）
  if (row["仕入れ先"]) {
    const suppliersArr = String(row["仕入れ先"]).split(/[，,]/).map(s => s.trim()).filter(s => s);
    data.suppliers = suppliersArr;
  }
  
  // 取引先銀行を別途banks配列に（修正）
  if (row["取引先銀行"]) {
    const banksStr = String(row["取引先銀行"]);
    // 銀行名を分離（「三菱ＵＦＪ（小田井）」のような形式）
    const banksArr = banksStr.split(/[，,]/).map(s => s.trim()).filter(s => s);
    // companies_newのbanks配列に保存（既存のフィールド構造にはないが追加）
    (data as any).banks = banksArr;
  }
  
  // 株主を配列に変換
  if (row["株主"]) {
    const shareholders = String(row["株主"]).split(/[，,]/).map(s => s.trim()).filter(s => s);
    data.shareholders = shareholders;
  }
  
  // その他
  data.employeeCount = row["社員数"] ? parseInt(String(row["社員数"]).replace(/,/g, "")) : null;
  data.officeCount = row["オフィス数"] ? parseInt(String(row["オフィス数"])) : null;
  data.factoryCount = row["工場数"] ? parseInt(String(row["工場数"])) : null;
  data.storeCount = row["店舗数"] ? parseInt(String(row["店舗数"])) : null;
  
  return data;
}

async function main() {
  console.log("================================================================================");
  console.log("タイプD: 削除と再追加（修正版）");
  console.log("================================================================================");
  console.log();

  // 1. 既存のドキュメントを削除
  console.log("【STEP 1】既存のドキュメントを削除中...");
  try {
    const docRef = db.collection(COLLECTION_NAME).doc(TYPE_D_DOC_ID);
    const docSnap = await docRef.get();

    if (docSnap.exists) {
      const oldData = docSnap.data();
      console.log(`削除対象: ${oldData?.name}`);
      console.log(`  [修正前] corporateNumber: ${oldData?.corporateNumber}`);
      console.log(`  [修正前] fiscalMonth: ${oldData?.fiscalMonth || "(なし)"}`);
      console.log(`  [修正前] industries: ${JSON.stringify(oldData?.industries)}`);
      await docRef.delete();
      console.log(`✓ 削除完了 (ID: ${TYPE_D_DOC_ID})`);
    } else {
      console.log(`⚠️  ドキュメントが見つかりません (ID: ${TYPE_D_DOC_ID})`);
    }
  } catch (error: any) {
    console.error(`❌ 削除エラー: ${error.message}`);
    throw error;
  }

  console.log();

  // 2. CSVから最初の企業データを取得
  console.log("【STEP 2】111.csvからデータを読み込み中...");
  const csvContent = fs.readFileSync(CSV_PATH, "utf-8");
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
  }) as Record<string, any>[];

  if (records.length === 0) {
    throw new Error(`${CSV_PATH}: データが見つかりません`);
  }

  const row = records[0];
  console.log(`✓ データ取得完了`);
  console.log(`  【CSVの内容】`);
  console.log(`  会社名: ${row["会社名"]}`);
  console.log(`  法人番号（CSV）: ${row["法人番号"]}`);
  console.log(`  業種1: ${row["業種1"]}`);
  console.log(`  業種2: ${row["業種2"]}`);
  console.log(`  業種3: ${row["業種3"]}`);
  console.log(`  業種4: ${row["業種4"]}`);
  console.log(`  直近決算年月: ${row["直近決算年月"]}`);
  console.log(`  仕入れ先: ${row["仕入れ先"]}`);
  console.log(`  取引先銀行: ${row["取引先銀行"]}`);

  console.log();

  // 3. データをマッピング
  console.log("【STEP 3】データをマッピング中...");
  const companyData = mapTypeD(row);
  
  console.log(`✓ マッピング完了`);
  console.log(`  [修正後] corporateNumber: ${companyData.corporateNumber || "(null - 13桁でないため)"}`);
  console.log(`  [修正後] industry（業種1）: ${companyData.industry}`);
  console.log(`  [修正後] industries（業種2-4）: ${JSON.stringify(companyData.industries)}`);
  console.log(`  [修正後] fiscalMonth（直近決算年月）: ${companyData.fiscalMonth}`);
  console.log(`  [修正後] suppliers（仕入れ先）: ${JSON.stringify(companyData.suppliers)}`);
  console.log(`  [修正後] banks（取引先銀行）: ${JSON.stringify((companyData as any).banks)}`);

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
    console.log(`  法人番号: ${newData.corporateNumber || "(null)"}`);
    console.log(`  業種1: ${newData.industry}`);
    console.log(`  業種2-4: ${JSON.stringify(newData.industries)}`);
    console.log(`  直近決算年月: ${newData.fiscalMonth}`);
    console.log(`  直近売上: ${newData.revenue}`);
    console.log(`  仕入れ先: ${JSON.stringify(newData.suppliers)}`);
    console.log(`  取引先銀行: ${JSON.stringify((newData as any).banks)}`);
    console.log(`  取引先: ${newData.clients}`);
    console.log();
    
    console.log("🎉 タイプD（111.csv）の修正版作成が完了しました！");
  }

  console.log("\n================================================================================");
  console.log("完了");
  console.log("================================================================================");
  console.log(`\n【新しいタイプDのドキュメントID】`);
  console.log(`タイプD: ${docRef.id}`);
  console.log(`CSVソース: csv/111.csv`);
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

