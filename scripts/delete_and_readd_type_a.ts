/* 
  タイプAを削除して、修正したロジックで再追加
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const TYPE_A_DOC_ID = "YapwvpPy6P5Ag3HgQSbb";
const CSV_PATH = "csv/10.csv";

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
 * タイプAのデータをマッピング（修正版）
 */
function mapTypeA(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.phoneNumber = row["電話番号"] || null;
  data.postalCode = row["会社郵便番号"] || null;
  data.address = row["会社住所"] || null;
  data.headquartersAddress = row["会社住所"] || null;
  data.companyUrl = row["URL"] || null;
  data.representativeName = row["代表者名"] || null;
  
  // 修正: representativeRegisteredAddressには代表者住所を入れる
  data.representativeRegisteredAddress = row["代表者住所"] || null;
  
  // representativeHomeAddressも代表者住所を入れる（同じ値）
  data.representativeHomeAddress = row["代表者住所"] || null;
  
  data.representativeBirthDate = row["代表者誕生日"] || null;
  data.businessDescriptions = row["営業種目"] || null;
  data.established = row["設立"] || null;
  data.executives = row["取締役"] || null;
  data.overview = row["概況"] || null;
  data.industryLarge = row["業種-大"] || null;
  data.industryMiddle = row["業種-中"] || null;
  data.industrySmall = row["業種-小"] || null;
  data.industryDetail = row["業種-細"] || null;
  
  // 株主を配列に変換
  if (row["株主"]) {
    data.shareholders = [row["株主"]];
  }
  
  return data;
}

async function main() {
  console.log("================================================================================");
  console.log("タイプA: 削除と再追加（修正版）");
  console.log("================================================================================");
  console.log();

  // 1. 既存のドキュメントを削除
  console.log("【STEP 1】既存のドキュメントを削除中...");
  try {
    const docRef = db.collection(COLLECTION_NAME).doc(TYPE_A_DOC_ID);
    const docSnap = await docRef.get();

    if (docSnap.exists) {
      const oldData = docSnap.data();
      console.log(`現在のrepresentativeRegisteredAddress: ${oldData?.representativeRegisteredAddress}`);
      console.log(`現在のrepresentativeHomeAddress: ${oldData?.representativeHomeAddress}`);
      
      await docRef.delete();
      console.log(`✓ 削除完了 (ID: ${TYPE_A_DOC_ID})`);
    } else {
      console.log(`⚠️  ドキュメントが見つかりません (ID: ${TYPE_A_DOC_ID})`);
    }
  } catch (error: any) {
    console.error(`❌ 削除エラー: ${error.message}`);
    throw error;
  }

  console.log();

  // 2. CSVから最初の企業データを取得
  console.log("【STEP 2】CSVからデータを読み込み中...");
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
  console.log(`  会社名: ${row["会社名"]}`);
  console.log(`  代表者名: ${row["代表者名"]}`);
  console.log(`  代表者郵便番号: ${row["代表者郵便番号"]}`);
  console.log(`  代表者住所: ${row["代表者住所"]}`);

  console.log();

  // 3. データをマッピング（修正版）
  console.log("【STEP 3】データをマッピング中...");
  const companyData = mapTypeA(row);
  
  console.log(`✓ マッピング完了`);
  console.log(`  representativeRegisteredAddress: ${companyData.representativeRegisteredAddress}`);
  console.log(`  representativeHomeAddress: ${companyData.representativeHomeAddress}`);

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
    console.log(`  企業名: ${newData.name}`);
    console.log(`  代表者名: ${newData.representativeName}`);
    console.log(`  代表者住所（登録）: ${newData.representativeRegisteredAddress}`);
    console.log(`  代表者住所（自宅）: ${newData.representativeHomeAddress}`);
    console.log(`  代表者誕生日: ${newData.representativeBirthDate}`);
    console.log(`  営業種目: ${newData.businessDescriptions}`);
    
    // 検証
    if (newData.representativeRegisteredAddress === row["代表者住所"]) {
      console.log("\n✅ representativeRegisteredAddressが正しく入っています！");
    } else {
      console.log("\n❌ representativeRegisteredAddressが正しくありません");
      console.log(`  期待値: ${row["代表者住所"]}`);
      console.log(`  実際の値: ${newData.representativeRegisteredAddress}`);
    }
  }

  console.log("\n================================================================================");
  console.log("完了");
  console.log("================================================================================");
  console.log(`\n新しいドキュメントID: ${docRef.id}`);
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

