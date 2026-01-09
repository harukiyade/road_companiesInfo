/* 
  各タイプ（A〜J）から1社のみを新規にcompanies_newコレクションに追加するスクリプト
  
  各CSVファイルの最初の企業情報（2行目）を取得し、新規ドキュメントとして追加します。

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/add_one_company_per_type.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// タイプ別の代表CSVファイル（最初の企業を取得する対象）
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
function mapTypeA(row: Record<string, any>, type: string): Record<string, any> {
  return {
    name: row["会社名"] || null,
    phoneNumber: row["電話番号"] || null,
    postalCode: row["会社郵便番号"] || null,
    address: row["会社住所"] || null,
    companyUrl: row["URL"] || null,
    representativeName: row["代表者名"] || null,
    representativeRegisteredAddress: row["代表者郵便番号"] || null,
    representativeHomeAddress: row["代表者住所"] || null,
    representativeBirthDate: row["代表者誕生日"] || null,
    businessDescriptions: row["営業種目"] || null,
    established: row["設立"] || null,
    shareholders: row["株主"] || null,
    executives: row["取締役"] || null,
    overview: row["概況"] || null,
    industryLarge: row["業種-大"] || null,
    industryMiddle: row["業種-中"] || null,
    industrySmall: row["業種-小"] || null,
    industryDetail: row["業種-細"] || row["法人番号"] || null, // 注: タイプAは法人番号がない
    csvType: type,
    csvSource: TYPE_CSV_MAP[type],
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

/**
 * タイプBのデータをマッピング（タイプA + 創業・設立）
 */
function mapTypeB(row: Record<string, any>, type: string): Record<string, any> {
  const data = mapTypeA(row, type);
  // タイプBは創業と設立の情報が充実している
  return {
    ...data,
    founding: row["創業"] || null,
    dateOfEstablishment: row["設立"] || null,
  };
}

/**
 * タイプCのデータをマッピング（詳細情報）
 * 注: 105.csvには「郵便番号」と「住所」が2回出現（会社用と代表者用）
 * csv-parseは重複ヘッダーを最後の値で上書きするため、
 * row["郵便番号"]とrow["住所"]は代表者のものになっている
 */
function mapTypeC(row: Record<string, any>, type: string): Record<string, any> {
  return {
    name: row["会社名"] || null,
    phoneNumber: row["電話番号"] || null,
    postalCode: null, // 会社の郵便番号は重複により取得不可
    address: null, // 会社の住所は重複により取得不可
    companyUrl: row["URL"] || null,
    representativeName: row["代表者"] || null,
    representativeRegisteredAddress: row["郵便番号"] || null, // これは代表者の郵便番号
    representativeHomeAddress: row["住所"] || null, // これは代表者の住所
    foundingYear: row["創業"] || null,
    established: row["設立"] || null,
    shareholders: row["株式保有率"] || null,
    executives: row["役員"] || null,
    overview: row["概要"] || null,
    industryLarge: row["業種（大）"] || null,
    industryMiddle: row["業種（中）"] || null,
    industrySmall: row["業種（小）"] || null,
    industryDetail: row["業種（細）"] || null,
    csvType: type,
    csvSource: TYPE_CSV_MAP[type],
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

/**
 * タイプDのデータをマッピング（都道府県・ID詳細形式）
 */
function mapTypeD(row: Record<string, any>, type: string): Record<string, any> {
  const industries = [
    row["業種1"] || null,
    row["業種2"] || null,
    row["業種3"] || null,
  ].filter(i => i !== null);

  return {
    name: row["会社名"] || null,
    prefecture: row["都道府県"] || null,
    representativeName: row["代表者名"] || null,
    corporateNumber: row["法人番号"] || null,
    salesNotes: row["備考"] || null,
    companyUrl: row["URL"] || null,
    industry: row["業種1"] || null,
    industries: industries.length > 1 ? industries.slice(1) : [],
    postalCode: row["郵便番号"] || null,
    address: row["住所"] || null,
    established: row["設立"] || null,
    phoneNumber: row["電話番号(窓口)"] || null,
    representativeRegisteredAddress: row["代表者郵便番号"] || null,
    representativeHomeAddress: row["代表者住所"] || null,
    representativeBirthDate: row["代表者誕生日"] || null,
    capitalStock: row["資本金"] || null,
    listing: row["上場"] || null,
    fiscalMonth: row["直近決算年月"] || null,
    revenue: row["直近売上"] || null,
    financials: row["直近利益"] || null,
    companyDescription: row["説明"] || null,
    overview: row["概要"] || null,
    suppliers: row["仕入れ先"] || null,
    clients: row["取引先"] || null,
    executives: row["取締役"] || null,
    shareholders: row["株主"] || null,
    employeeCount: row["社員数"] || null,
    officeCount: row["オフィス数"] || null,
    factoryCount: row["工場数"] || null,
    storeCount: row["店舗数"] || null,
    csvType: type,
    csvSource: TYPE_CSV_MAP[type],
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

/**
 * タイプEのデータをマッピング（メールアドレスあり）
 */
function mapTypeE(row: Record<string, any>, type: string): Record<string, any> {
  const data = mapTypeD(row, type); // タイプDがベース
  return {
    ...data,
    email: row["メールアドレス"] || null,
  };
}

/**
 * タイプFのデータをマッピング（説明・概要あり）
 */
function mapTypeF(row: Record<string, any>, type: string): Record<string, any> {
  const data = mapTypeD(row, type); // タイプDがベース
  return {
    ...data,
    companyDescription: row["説明"] || null,
    overview: row["概要"] || null,
  };
}

/**
 * タイプGのデータをマッピング（銀行・決算情報）
 */
function mapTypeG(row: Record<string, any>, type: string): Record<string, any> {
  const banksStr = row["銀行"] || "";
  const banksArray = banksStr ? banksStr.split(/[・、,]/).map((b: string) => b.trim()).filter((b: string) => b) : [];

  return {
    name: row["会社名"] || null,
    nameEn: row["会社名（英語）"] || null,
    corporateNumber: row["法人番号"] || null,
    prefecture: row["都道府県"] || null,
    address: row["住所"] || null,
    industry: row["業種"] || null,
    capitalStock: row["資本金"] || null,
    revenue: row["売上"] || null,
    latestProfit: row["直近利益"] || null,
    employeeCount: row["従業員数"] || null,
    issuedShares: row["発行株式数"] || null,
    established: row["設立"] || null,
    fiscalMonth: row["決算月"] || null,
    listing: row["上場"] || null,
    representativeName: row["代表者名"] || null,
    representativeTitle: row["businessDescriptions"] || null,
    banks: banksArray,
    phoneNumber: null, // 127.csvには電話番号フィールドなし
    companyUrl: row["URL"] || null,
    contactUrl: row["contactUrl"] || null,
    affiliations: row["affiliations"] || null,
    overview: row["overview"] || null,
    history: row["history"] || null,
    totalAssets: row["totalAssets"] || null,
    totalLiabilities: row["totalLiabilities"] || null,
    netAssets: row["netAssets"] || null,
    revenueFromStatements: row["revenueFromStatements"] || null,
    operatingIncome: row["operatingIncome"] || null,
    csvType: type,
    csvSource: TYPE_CSV_MAP[type],
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

/**
 * タイプHのデータをマッピング（業種展開・役員情報）
 */
function mapTypeH(row: Record<string, any>, type: string): Record<string, any> {
  // 役員情報を集約
  const executives: any[] = [];
  for (let i = 1; i <= 10; i++) {
    const name = row[`executiveName${i}`];
    const title = row[`executiveTitle${i}`];
    if (name || title) {
      executives.push({ name: name || "", title: title || "" });
    }
  }

  return {
    name: row["name"] || null,
    corporateNumber: row["corporateNumber"] || null,
    representativeName: row["representativeName"] || null,
    revenue: row["revenue"] || null,
    capitalStock: row["capitalStock"] || null,
    listing: row["listing"] || null,
    address: row["address"] || null,
    employeeCount: row["employeeCount"] || null,
    established: row["established"] || null,
    fiscalMonth: row["fiscalMonth"] || null,
    industryLarge: row["industryLarge"] || null,
    industryMiddle: row["industryMiddle"] || null,
    industrySmall: row["industrySmall"] || null,
    industryDetail: row["industryDetail"] || null,
    phoneNumber: row["phoneNumber"] || null,
    companyUrl: row["companyUrl"] || null,
    bankCorporateNumber: row["bankCorporateNumber"] || null,
    executivesData: executives.length > 0 ? executives : null,
    csvType: type,
    csvSource: TYPE_CSV_MAP[type],
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

/**
 * タイプIのデータをマッピング（決算月・売上・利益（複数年））
 */
function mapTypeI(row: Record<string, any>, type: string): Record<string, any> {
  // 決算情報を集約
  const financials: any[] = [];
  for (let i = 1; i <= 5; i++) {
    const fiscalMonth = row[`決算月${i}`];
    const revenue = row[`売上${i}`];
    const profit = row[`利益${i}`];
    if (fiscalMonth || revenue || profit) {
      financials.push({
        fiscalMonth: fiscalMonth || null,
        revenue: revenue || null,
        profit: profit || null,
      });
    }
  }

  return {
    name: row["会社名"] || null,
    prefecture: row["都道府県"] || null,
    representativeName: row["代表者名"] || null,
    corporateNumber: row["法人番号"] || null,
    companyUrl: row["URL"] || null,
    industry: row["業種1"] || null,
    postalCode: row["郵便番号"] || null,
    address: row["住所"] || null,
    established: row["設立"] || null,
    phoneNumber: row["電話番号(窓口)"] || null,
    representativeRegisteredAddress: row["代表者郵便番号"] || null,
    representativeHomeAddress: row["代表者住所"] || null,
    representativeBirthDate: row["代表者誕生日"] || null,
    financialsData: financials.length > 0 ? financials : null,
    csvType: type,
    csvSource: TYPE_CSV_MAP[type],
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

/**
 * タイプJのデータをマッピング（部署・拠点情報）
 */
function mapTypeJ(row: Record<string, any>, type: string): Record<string, any> {
  // 部署情報を集約
  const departments: any[] = [];
  for (let i = 1; i <= 7; i++) {
    const name = row[`部署名${i}`];
    const address = row[`部署住所${i}`];
    const phone = row[`部署電話番号${i}`];
    if (name || address || phone) {
      departments.push({
        name: name || null,
        address: address || null,
        phone: phone || null,
      });
    }
  }

  return {
    name: row["会社名"] || null,
    prefecture: row["都道府県"] || null,
    representativeName: row["代表者名"] || null,
    corporateNumber: row["法人番号"] || null,
    companyUrl: row["URL"] || null,
    industry: row["業種1"] || null,
    postalCode: row["郵便番号"] || null,
    address: row["住所"] || null,
    established: row["設立"] || null,
    phoneNumber: row["電話番号(窓口)"] || null,
    departmentsData: departments.length > 0 ? departments : null,
    csvType: type,
    csvSource: TYPE_CSV_MAP[type],
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };
}

/**
 * タイプに応じたマッピング関数を選択
 */
function mapCompanyData(row: Record<string, any>, type: string): Record<string, any> {
  switch (type) {
    case "A": return mapTypeA(row, type);
    case "B": return mapTypeB(row, type);
    case "C": return mapTypeC(row, type);
    case "D": return mapTypeD(row, type);
    case "E": return mapTypeE(row, type);
    case "F": return mapTypeF(row, type);
    case "G": return mapTypeG(row, type);
    case "H": return mapTypeH(row, type);
    case "I": return mapTypeI(row, type);
    case "J": return mapTypeJ(row, type);
    default:
      throw new Error(`Unknown type: ${type}`);
  }
}

/**
 * メイン処理
 */
async function main() {
  console.log("=".repeat(80));
  console.log("各タイプから1社のみをcompanies_newコレクションに追加");
  console.log("=".repeat(80));
  console.log(`モード: ${DRY_RUN ? "DRY RUN（書き込みなし）" : "実行"}`);
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

      if (DRY_RUN) {
        console.log("✓ DRY RUN: 書き込みをスキップ");
        console.log("データプレビュー:");
        console.log(JSON.stringify(companyData, null, 2));
        results[type] = { success: true, message: "DRY RUN完了" };
      } else {
        // Firestoreに新規追加
        const docRef = await db.collection(COLLECTION_NAME).add(companyData);
        console.log(`✓ 追加完了: ドキュメントID = ${docRef.id}`);
        results[type] = { 
          success: true, 
          message: "追加完了", 
          docId: docRef.id 
        };
      }
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

  console.log(`\n✓ 成功: ${successful.length}件`);
  successful.forEach(([type, result]) => {
    console.log(`  - タイプ${type}: ${result.message}${result.docId ? ` (ID: ${result.docId})` : ""}`);
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

