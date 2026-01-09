/* 
  各タイプ（A〜J）から1社のみをcompanies_newコレクションに追加（既存フィールド構造に準拠）
  
  既存のcompanies_newコレクションのフィールド構造（63個のフィールド）に合わせてデータを追加します。
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";

// タイプ別の代表CSVファイル
const TYPE_CSV_MAP: Record<string, string> = {
  "A": "csv/10.csv",
  "B": "csv/12.csv",
  "C": "csv/105.csv",
  "D": "csv/111.csv",
  "E": "csv/116.csv",
  "F": "csv/124.csv",
  "G": "csv/127.csv",
  "H": "csv/130.csv",
  "I": "csv/132.csv",
  "J": "csv/133.csv",
};

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
 * CSVの最初の企業データ（2行目）を取得
 */
function getFirstCompanyFromCSV(csvPath: string): Record<string, any> | null {
  try {
    const csvContent = fs.readFileSync(csvPath, "utf-8");
    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
      trim: true,
    }) as Record<string, any>[];

    if (records.length === 0) {
      console.error(`❌ ${csvPath}: データが見つかりません`);
      return null;
    }

    return records[0];
  } catch (error: any) {
    console.error(`❌ ${csvPath}: 読み込みエラー - ${error.message}`);
    return null;
  }
}

/**
 * タイプAのデータをマッピング
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
  data.representativeRegisteredAddress = row["代表者郵便番号"] || null;
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

/**
 * タイプBのデータをマッピング
 */
function mapTypeB(row: Record<string, any>): Record<string, any> {
  const data = mapTypeA(row);
  data.foundingYear = row["創業"] || null;
  return data;
}

/**
 * タイプCのデータをマッピング
 */
function mapTypeC(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.phoneNumber = row["電話番号"] || null;
  data.companyUrl = row["URL"] || null;
  data.representativeName = row["代表者"] || null;
  data.representativeRegisteredAddress = row["郵便番号"] || null;
  data.representativeHomeAddress = row["住所"] || null;
  data.foundingYear = row["創業"] ? parseInt(String(row["創業"]).substring(0, 4)) : null;
  data.established = row["設立"] || null;
  data.executives = row["役員"] || null;
  data.overview = row["概要"] || null;
  data.industryLarge = row["業種（大）"] || null;
  data.industryMiddle = row["業種（中）"] || null;
  data.industrySmall = row["業種（小）"] || null;
  data.industryDetail = row["業種（細）"] || null;
  
  if (row["株式保有率"]) {
    data.shareholders = [row["株式保有率"]];
  }
  
  return data;
}

/**
 * タイプD/Eのデータをマッピング
 */
function mapTypeDE(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.prefecture = row["都道府県"] || null;
  data.representativeName = row["代表者名"] || null;
  data.corporateNumber = row["法人番号"] || null;
  data.salesNotes = row["備考"] || null;
  data.companyUrl = row["URL"] || null;
  data.industry = row["業種1"] || null;
  data.postalCode = row["郵便番号"] || null;
  data.address = row["住所"] || null;
  data.headquartersAddress = row["住所"] || null;
  data.established = row["設立"] || null;
  data.phoneNumber = row["電話番号(窓口)"] || null;
  data.representativeRegisteredAddress = row["代表者郵便番号"] || null;
  data.representativeHomeAddress = row["代表者住所"] || null;
  data.representativeBirthDate = row["代表者誕生日"] || null;
  data.capitalStock = row["資本金"] ? parseFloat(String(row["資本金"]).replace(/,/g, "")) : null;
  data.listing = row["上場"] || null;
  data.fiscalMonth = row["直近決算年月"] || null;
  data.revenue = row["直近売上"] ? parseFloat(String(row["直近売上"]).replace(/,/g, "")) : null;
  data.financials = row["直近利益"] || null;
  data.companyDescription = row["説明"] || null;
  data.overview = row["概要"] || null;
  data.clients = row["取引先"] || null;
  data.executives = row["取締役"] || null;
  data.employeeCount = row["社員数"] ? parseInt(String(row["社員数"]).replace(/,/g, "")) : null;
  data.officeCount = row["オフィス数"] ? parseInt(String(row["オフィス数"])) : null;
  data.factoryCount = row["工場数"] ? parseInt(String(row["工場数"])) : null;
  data.storeCount = row["店舗数"] ? parseInt(String(row["店舗数"])) : null;
  
  // 業種を配列に
  const industriesArr: string[] = [];
  if (row["業種2"]) industriesArr.push(row["業種2"]);
  if (row["業種3"]) industriesArr.push(row["業種3"]);
  data.industries = industriesArr;
  
  // 仕入れ先と取引銀行を配列に
  const suppliersArr: string[] = [];
  if (row["仕入れ先"]) {
    suppliersArr.push(...String(row["仕入れ先"]).split(/[，,]/).map(s => s.trim()).filter(s => s));
  }
  if (row["取引先銀行"]) {
    suppliersArr.push(...String(row["取引先銀行"]).split(/[，,]/).map(s => s.trim()).filter(s => s));
  }
  data.suppliers = suppliersArr;
  
  // 株主を配列に
  if (row["株主"]) {
    data.shareholders = String(row["株主"]).split(/[，,]/).map(s => s.trim()).filter(s => s);
  }
  
  return data;
}

/**
 * タイプFのデータをマッピング
 */
function mapTypeF(row: Record<string, any>): Record<string, any> {
  const data = mapTypeDE(row);
  data.companyDescription = row["説明"] || null;
  data.overview = row["概要"] || null;
  return data;
}

/**
 * タイプGのデータをマッピング
 */
function mapTypeG(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.corporateNumber = row["法人番号"] || null;
  data.prefecture = row["都道府県"] || null;
  data.address = row["住所"] || null;
  data.headquartersAddress = row["住所"] || null;
  data.industry = row["業種"] || null;
  data.capitalStock = row["資本金"] ? parseFloat(String(row["資本金"]).replace(/,/g, "")) : null;
  data.revenue = row["売上"] ? parseFloat(String(row["売上"]).replace(/,/g, "")) : null;
  data.financials = row["直近利益"] || null;
  data.employeeCount = row["従業員数"] ? parseInt(String(row["従業員数"]).replace(/,/g, "")) : null;
  data.established = row["設立"] || null;
  data.fiscalMonth = row["決算月"] || null;
  data.listing = row["上場"] || null;
  data.representativeName = row["代表者名"] || null;
  data.companyUrl = row["URL"] || null;
  data.overview = row["overview"] || null;
  
  // 銀行を配列に
  if (row["銀行"]) {
    data.suppliers = String(row["銀行"]).split(/[・、,]/).map(s => s.trim()).filter(s => s);
  }
  
  return data;
}

/**
 * タイプHのデータをマッピング
 */
function mapTypeH(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["name"] || null;
  data.corporateNumber = row["corporateNumber"] || null;
  data.representativeName = row["representativeName"] || null;
  data.revenue = row["revenue"] ? parseFloat(String(row["revenue"]).replace(/,/g, "")) : null;
  data.capitalStock = row["capitalStock"] ? parseFloat(String(row["capitalStock"]).replace(/,/g, "")) : null;
  data.listing = row["listing"] || null;
  data.address = row["address"] || null;
  data.headquartersAddress = row["address"] || null;
  data.employeeCount = row["employeeCount"] ? parseInt(String(row["employeeCount"]).replace(/,/g, "")) : null;
  data.established = row["established"] || null;
  data.fiscalMonth = row["fiscalMonth"] || null;
  data.industryLarge = row["industryLarge"] || null;
  data.industryMiddle = row["industryMiddle"] || null;
  data.industrySmall = row["industrySmall"] || null;
  data.industryDetail = row["industryDetail"] || null;
  data.phoneNumber = row["phoneNumber"] || null;
  data.companyUrl = row["companyUrl"] || null;
  
  // 役員情報を配列に
  const executivesArr: string[] = [];
  for (let i = 1; i <= 10; i++) {
    const name = row[`executiveName${i}`];
    const title = row[`executiveTitle${i}`];
    if (name || title) {
      executivesArr.push(`${title || ""}${name || ""}`.trim());
    }
  }
  if (executivesArr.length > 0) {
    data.executives = executivesArr.join("，");
  }
  
  return data;
}

/**
 * タイプIのデータをマッピング
 */
function mapTypeI(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.prefecture = row["都道府県"] || null;
  data.representativeName = row["代表者名"] || null;
  data.corporateNumber = row["法人番号"] || null;
  data.companyUrl = row["URL"] || null;
  data.industry = row["業種1"] || null;
  data.postalCode = row["郵便番号"] || null;
  data.address = row["住所"] || null;
  data.headquartersAddress = row["住所"] || null;
  data.established = row["設立"] || null;
  data.phoneNumber = row["電話番号(窓口)"] || null;
  data.representativeRegisteredAddress = row["代表者郵便番号"] || null;
  data.representativeHomeAddress = row["代表者住所"] || null;
  data.representativeBirthDate = row["代表者誕生日"] || null;
  
  // 最新の決算情報を取得
  if (row["決算月1"]) data.fiscalMonth = row["決算月1"];
  if (row["売上1"]) data.revenue = parseFloat(String(row["売上1"]).replace(/,/g, ""));
  if (row["利益1"]) data.financials = row["利益1"];
  
  return data;
}

/**
 * タイプJのデータをマッピング
 */
function mapTypeJ(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.prefecture = row["都道府県"] || null;
  data.representativeName = row["代表者名"] || null;
  data.corporateNumber = row["法人番号"] || null;
  data.companyUrl = row["URL"] || null;
  data.industry = row["業種1"] || null;
  data.postalCode = row["郵便番号"] || null;
  data.address = row["住所"] || null;
  data.headquartersAddress = row["住所"] || null;
  data.established = row["設立"] || null;
  data.phoneNumber = row["電話番号(窓口)"] || null;
  
  // 部署情報は特別な構造のため、ここでは基本情報のみ
  
  return data;
}

/**
 * タイプに応じたマッピング関数を選択
 */
function mapCompanyData(row: Record<string, any>, type: string): Record<string, any> {
  switch (type) {
    case "A": return mapTypeA(row);
    case "B": return mapTypeB(row);
    case "C": return mapTypeC(row);
    case "D": return mapTypeDE(row);
    case "E": return mapTypeDE(row);
    case "F": return mapTypeF(row);
    case "G": return mapTypeG(row);
    case "H": return mapTypeH(row);
    case "I": return mapTypeI(row);
    case "J": return mapTypeJ(row);
    default:
      throw new Error(`Unknown type: ${type}`);
  }
}

/**
 * メイン処理
 */
async function main() {
  console.log("=".repeat(80));
  console.log("各タイプから1社のみをcompanies_newコレクションに追加（既存構造に準拠）");
  console.log("=".repeat(80));
  console.log();

  const results: Record<string, { success: boolean; message: string; docId?: string }> = {};

  for (const [type, csvPath] of Object.entries(TYPE_CSV_MAP)) {
    console.log(`\n--- タイプ${type}: ${csvPath} ---`);

    try {
      // CSVの最初の企業データを取得
      const row = getFirstCompanyFromCSV(csvPath);
      if (!row) {
        results[type] = { success: false, message: "データ取得失敗" };
        continue;
      }

      // データをマッピング
      const companyData = mapCompanyData(row, type);
      
      console.log(`企業名: ${companyData.name || "（名前なし）"}`);
      console.log(`法人番号: ${companyData.corporateNumber || "（なし）"}`);
      console.log(`住所: ${companyData.address || "（なし）"}`);

      // Firestoreに新規追加
      const docRef = await db.collection(COLLECTION_NAME).add(companyData);
      console.log(`✓ 追加完了: ドキュメントID = ${docRef.id}`);
      results[type] = { 
        success: true, 
        message: "追加完了", 
        docId: docRef.id 
      };
    } catch (error: any) {
      console.error(`❌ エラー: ${error.message}`);
      results[type] = { success: false, message: error.message };
    }
  }

  // 結果サマリー
  console.log("\n" + "=".repeat(80));
  console.log("処理結果サマリー");
  console.log("=".repeat(80));

  const successful = Object.entries(results).filter(([_, r]) => r.success);
  const failed = Object.entries(results).filter(([_, r]) => !r.success);

  console.log(`\n✓ 成功: ${successful.length}件\n`);
  successful.forEach(([type, result]) => {
    console.log(`タイプ${type}: ${result.docId}`);
  });

  if (failed.length > 0) {
    console.log(`\n❌ 失敗: ${failed.length}件`);
    failed.forEach(([type, result]) => {
      console.log(`  - タイプ${type}: ${result.message}`);
    });
  }

  console.log("\n処理完了！");
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

