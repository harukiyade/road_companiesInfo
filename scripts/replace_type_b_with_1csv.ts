/* 
  タイプBを削除して、1.csvの1行目で再作成（法人番号あり版）
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const OLD_TYPE_B_DOC_ID = "IGkVISNmXAiyM810kXBa";
const NEW_CSV_PATH = "csv/1.csv";

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
 * タイプB（1.csv）のデータをマッピング
 * 特徴: 法人番号が先頭列にある
 */
function mapTypeB(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  // 法人番号（タイプBの特徴）
  data.corporateNumber = row["法人番号"] || null;
  
  // 会社情報
  data.name = row["会社名"] || null;
  data.phoneNumber = row["電話番号"] || null;
  data.postalCode = row["会社郵便番号"] || null;
  data.address = row["会社住所"] || null;
  data.headquartersAddress = row["会社住所"] || null;
  data.companyUrl = row["URL"] || null;
  
  // 代表者情報
  data.representativeName = row["代表者名"] || null;
  data.representativePostalCode = row["代表者郵便番号"] || null;
  data.representativeRegisteredAddress = row["代表者住所"] || null;
  data.representativeHomeAddress = row["代表者住所"] || null;
  data.representativeBirthDate = row["代表者誕生日"] || null;
  
  // 事業情報
  data.businessDescriptions = row["営業種目"] || null;
  data.established = row["設立"] || null;
  data.executives = row["取締役"] || null;
  data.overview = row["概況"] || null;
  
  // 業種情報
  data.industryLarge = row["業種-大"] || null;
  data.industryMiddle = row["業種-中"] || null;
  data.industrySmall = row["業種-小"] || null;
  data.industryDetail = row["業種-細"] || null;
  
  // 株主を配列に変換
  if (row["株主"]) {
    const shareholders = String(row["株主"]).split(/[，,]/).map(s => s.trim()).filter(s => s);
    data.shareholders = shareholders;
  }
  
  return data;
}

async function main() {
  console.log("================================================================================");
  console.log("タイプB: 1.csvに差し替え（法人番号あり版）");
  console.log("================================================================================");
  console.log();

  // 1. 既存のドキュメントを削除
  console.log("【STEP 1】既存のタイプB（12.csv版）を削除中...");
  try {
    const docRef = db.collection(COLLECTION_NAME).doc(OLD_TYPE_B_DOC_ID);
    const docSnap = await docRef.get();

    if (docSnap.exists) {
      const oldData = docSnap.data();
      console.log(`削除対象: ${oldData?.name} (12.csv版)`);
      console.log(`  法人番号: ${oldData?.corporateNumber || "(なし)"}`);
      
      await docRef.delete();
      console.log(`✓ 削除完了 (ID: ${OLD_TYPE_B_DOC_ID})`);
    } else {
      console.log(`⚠️  ドキュメントが見つかりません (ID: ${OLD_TYPE_B_DOC_ID})`);
    }
  } catch (error: any) {
    console.error(`❌ 削除エラー: ${error.message}`);
    throw error;
  }

  console.log();

  // 2. 1.csvから最初の企業データを取得
  console.log("【STEP 2】1.csvからデータを読み込み中...");
  const csvContent = fs.readFileSync(NEW_CSV_PATH, "utf-8");
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
  }) as Record<string, any>[];

  if (records.length === 0) {
    throw new Error(`${NEW_CSV_PATH}: データが見つかりません`);
  }

  const row = records[0];
  console.log(`✓ データ取得完了`);
  console.log(`  【CSVの内容】`);
  console.log(`  法人番号: ${row["法人番号"]}`);
  console.log(`  会社名: ${row["会社名"]}`);
  console.log(`  電話番号: ${row["電話番号"]}`);
  console.log(`  会社郵便番号: ${row["会社郵便番号"]}`);
  console.log(`  会社住所: ${row["会社住所"]}`);
  console.log(`  URL: ${row["URL"]}`);
  console.log(`  代表者名: ${row["代表者名"]}`);
  console.log(`  代表者郵便番号: ${row["代表者郵便番号"] || "(空)"}`);
  console.log(`  代表者住所: ${row["代表者住所"] || "(空)"}`);
  console.log(`  代表者誕生日: ${row["代表者誕生日"] || "(空)"}`);
  console.log(`  営業種目: ${row["営業種目"]}`);
  console.log(`  設立: ${row["設立"]}`);

  console.log();

  // 3. データをマッピング
  console.log("【STEP 3】データをマッピング中...");
  const companyData = mapTypeB(row);
  
  console.log(`✓ マッピング完了`);
  console.log(`  corporateNumber（法人番号）: ${companyData.corporateNumber}`);
  console.log(`  name: ${companyData.name}`);
  console.log(`  representativePostalCode: ${companyData.representativePostalCode || "(空)"}`);
  console.log(`  representativeRegisteredAddress: ${companyData.representativeRegisteredAddress || "(空)"}`);

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
    console.log(`  法人番号: ${newData.corporateNumber}`);
    console.log(`  企業名: ${newData.name}`);
    console.log(`  会社郵便番号: ${newData.postalCode}`);
    console.log(`  会社住所: ${newData.address}`);
    console.log(`  URL: ${newData.companyUrl}`);
    console.log(`  代表者名: ${newData.representativeName}`);
    console.log(`  代表者郵便番号: ${newData.representativePostalCode || "(空)"}`);
    console.log(`  代表者住所（登録）: ${newData.representativeRegisteredAddress || "(空)"}`);
    console.log(`  代表者住所（自宅）: ${newData.representativeHomeAddress || "(空)"}`);
    console.log(`  代表者誕生日: ${newData.representativeBirthDate || "(空)"}`);
    console.log(`  営業種目: ${newData.businessDescriptions}`);
    console.log(`  設立: ${newData.established}`);
    console.log(`  株主: ${JSON.stringify(newData.shareholders)}`);
    console.log(`  取締役: ${newData.executives}`);
    console.log(`  概況: ${newData.overview}`);
    console.log();
    
    // 検証
    if (newData.corporateNumber === row["法人番号"]) {
      console.log("✅ corporateNumber（法人番号）が正しく入っています！");
    } else {
      console.log("❌ corporateNumberが正しくありません");
    }
    
    console.log("\n🎉 タイプB（1.csv版）の追加が完了しました！");
  }

  console.log("\n================================================================================");
  console.log("完了");
  console.log("================================================================================");
  console.log(`\n【新しいタイプBのドキュメントID】`);
  console.log(`タイプB: ${docRef.id}`);
  console.log(`CSVソース: csv/1.csv`);
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

