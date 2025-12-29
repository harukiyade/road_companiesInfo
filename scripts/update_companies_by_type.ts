/* 
  タイプA・B・C・D・GのCSVファイルから全データを読み込み、
  既存のcompanies_newコレクション内の企業を特定して更新
  
  企業の特定方法：
  - 企業名（必須）
  - 住所（必須）
  - 代表者名（任意）
  - 法人番号（任意）
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// タイプ別のCSVファイルリスト
const TYPE_CSV_FILES: Record<string, string[]> = {
  "A": ["7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "25", "26", "27", "28", "29", "30", "31", "32", "33", "34", "35", "39", "52", "54", "55", "56", "57", "58", "59", "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76", "77", "101", "104"],
  "B": ["1", "2", "53", "103", "106", "126"],
  "C": ["23", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "100", "102", "105"],
  "D": ["24", "36", "37", "38", "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50", "107", "108", "109", "110", "111", "112", "113", "114", "115", "116", "117", "119", "133", "134"],
  "G": ["127", "128"],
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
    operatingIncome: null,
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
    revenueFromStatements: null,
    salesNotes: null,
    shareholders: [],
    storeCount: null,
    suppliers: [],
    tags: [],
    totalAssets: null,
    totalLiabilities: null,
    netAssets: null,
    updateCount: null,
    updatedAt: null,
    urls: [],
    wantedly: null,
    youtrust: null,
  };
}

/**
 * 企業を特定するためのスコアリング
 */
function calculateMatchScore(
  csvData: Record<string, any>,
  firestoreData: Record<string, any>
): number {
  let score = 0;
  
  // 企業名が完全一致（最重要）
  if (csvData.name && firestoreData.name && csvData.name === firestoreData.name) {
    score += 100;
  } else if (csvData.name && firestoreData.name) {
    // 部分一致チェック
    const csvName = String(csvData.name).replace(/[（(）)]/g, "").trim();
    const fsName = String(firestoreData.name).replace(/[（(）)]/g, "").trim();
    if (csvName === fsName || csvName.includes(fsName) || fsName.includes(csvName)) {
      score += 50;
    }
  }
  
  // 住所が一致
  if (csvData.address && firestoreData.address) {
    const csvAddr = String(csvData.address).trim();
    const fsAddr = String(firestoreData.address).trim();
    if (csvAddr === fsAddr) {
      score += 50;
    } else if (csvAddr.includes(fsAddr) || fsAddr.includes(csvAddr)) {
      score += 25;
    }
  }
  
  // 法人番号が一致
  if (csvData.corporateNumber && firestoreData.corporateNumber) {
    if (String(csvData.corporateNumber) === String(firestoreData.corporateNumber)) {
      score += 30;
    }
  }
  
  // 代表者名が一致
  if (csvData.representativeName && firestoreData.representativeName) {
    if (String(csvData.representativeName).trim() === String(firestoreData.representativeName).trim()) {
      score += 20;
    }
  }
  
  return score;
}

/**
 * 企業を特定（企業名・住所・代表者名・法人番号で検索）
 */
async function findCompany(
  csvData: Record<string, any>
): Promise<{ docId: string; data: any; score: number } | null> {
  const name = csvData.name;
  const address = csvData.address;
  const corporateNumber = csvData.corporateNumber;
  const representativeName = csvData.representativeName;
  
  if (!name || !address) {
    return null; // 企業名と住所は必須
  }
  
  // まず法人番号で検索（最も確実）
  if (corporateNumber) {
    const corpNumQuery = await db.collection(COLLECTION_NAME)
      .where("corporateNumber", "==", corporateNumber)
      .limit(5)
      .get();
    
    if (!corpNumQuery.empty) {
      const doc = corpNumQuery.docs[0];
      return {
        docId: doc.id,
        data: doc.data(),
        score: 200, // 法人番号一致は最高スコア
      };
    }
  }
  
  // 企業名で検索
  const nameQuery = await db.collection(COLLECTION_NAME)
    .where("name", "==", name)
    .limit(50)
    .get();
  
  if (nameQuery.empty) {
    return null;
  }
  
  // 住所と代表者名で絞り込み
  let bestMatch: { docId: string; data: any; score: number } | null = null;
  let bestScore = 0;
  
  nameQuery.forEach((doc) => {
    const fsData = doc.data();
    const score = calculateMatchScore(csvData, fsData);
    
    if (score > bestScore && score >= 100) { // 最低でも企業名一致が必要
      bestScore = score;
      bestMatch = {
        docId: doc.id,
        data: fsData,
        score: score,
      };
    }
  });
  
  return bestMatch;
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
  data.representativePostalCode = row["代表者郵便番号"] || null;
  data.representativeRegisteredAddress = row["代表者住所"] || null;
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
  data.corporateNumber = row["法人番号"] || null;
  return data;
}

/**
 * タイプCのデータをマッピング（手動パース対応）
 */
function mapTypeC(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.phoneNumber = row["電話番号"] || null;
  data.postalCode = row["会社郵便番号"] || null;
  data.address = row["会社住所"] || null;
  data.headquartersAddress = row["会社住所"] || null;
  data.companyUrl = row["URL"] || null;
  data.representativeName = row["代表者"] || null;
  data.representativePostalCode = row["代表者郵便番号"] || null;
  data.representativeRegisteredAddress = row["代表者住所"] || null;
  data.representativeHomeAddress = row["代表者住所"] || null;
  
  const foundingStr = row["創業"];
  if (foundingStr) {
    data.foundingDate = foundingStr;
    data.foundingYear = parseInt(String(foundingStr).substring(0, 4));
  }
  
  data.businessDescriptions = row["営業種目"] || null;
  data.established = row["設立"] || null;
  data.executives = row["役員"] || null;
  data.overview = row["概要"] || null;
  data.industryLarge = row["業種（大）"] || null;
  data.industryMiddle = row["業種（中）"] || null;
  data.industrySmall = row["業種（小）"] || null;
  data.industryDetail = row["業種（細）1"] || row["業種（細）2"] || null;
  
  if (row["株式保有率"]) {
    const shareholders = String(row["株式保有率"]).split(/[，,]/).map(s => s.trim()).filter(s => s);
    data.shareholders = shareholders;
  }
  
  return data;
}

/**
 * 法人番号が有効な13桁かチェック
 */
function validateCorporateNumber(value: any): string | null {
  if (!value) return null;
  const str = String(value).replace(/\s/g, "");
  if (str.includes("E") || str.includes("e")) return null;
  if (!/^\d{13}$/.test(str)) return null;
  return str;
}

/**
 * タイプDのデータをマッピング
 */
function mapTypeD(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.prefecture = row["都道府県"] || null;
  data.representativeName = row["代表者名"] || null;
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
  data.capitalStock = row["資本金"] ? parseFloat(String(row["資本金"]).replace(/,/g, "")) : null;
  data.listing = row["上場"] || null;
  data.fiscalMonth = row["直近決算年月"] || null;
  data.revenue = row["直近売上"] ? parseFloat(String(row["直近売上"]).replace(/,/g, "")) : null;
  data.financials = row["直近利益"] || null;
  data.companyDescription = row["説明"] || null;
  data.overview = row["概要"] || null;
  data.clients = row["取引先"] || null;
  data.executives = row["取締役"] || null;
  
  data.industryLarge = row["業種1"] || null;
  data.industryMiddle = row["業種2"] || null;
  data.industrySmall = row["業種3"] || null;
  data.industryDetail = row["業種4"] || null;
  data.industry = row["業種1"] || null;
  
  if (row["仕入れ先"]) {
    const suppliersArr = String(row["仕入れ先"]).split(/[，,]/).map(s => s.trim()).filter(s => s);
    data.suppliers = suppliersArr;
  }
  
  if (row["取引先銀行"]) {
    const banksStr = String(row["取引先銀行"]);
    const banksArr = banksStr.split(/[，,]/).map(s => s.trim()).filter(s => s);
    (data as any).banks = banksArr;
  }
  
  if (row["株主"]) {
    const shareholders = String(row["株主"]).split(/[，,]/).map(s => s.trim()).filter(s => s);
    data.shareholders = shareholders;
  }
  
  data.employeeCount = row["社員数"] ? parseInt(String(row["社員数"]).replace(/,/g, "")) : null;
  data.officeCount = row["オフィス数"] ? parseInt(String(row["オフィス数"])) : null;
  data.factoryCount = row["工場数"] ? parseInt(String(row["工場数"])) : null;
  data.storeCount = row["店舗数"] ? parseInt(String(row["店舗数"])) : null;
  
  return data;
}

/**
 * 業種名から大分類〜細分類を自動推測
 */
function inferIndustryCategories(industryName: string | null): {
  industryLarge: string | null;
  industryMiddle: string | null;
  industrySmall: string | null;
  industryDetail: string | null;
} {
  if (!industryName) {
    return {
      industryLarge: null,
      industryMiddle: null,
      industrySmall: null,
      industryDetail: null,
    };
  }

  const industry = industryName.trim();

  if (industry.includes("水産") || industry.includes("水産加工")) {
    return {
      industryLarge: "製造業",
      industryMiddle: "食料品製造業",
      industrySmall: "水産食料品製造業",
      industryDetail: industry,
    };
  }

  if (industry.includes("調味料") || industry.includes("食品添加物")) {
    return {
      industryLarge: "製造業",
      industryMiddle: "食料品製造業",
      industrySmall: "調味料・香辛料製造業",
      industryDetail: industry,
    };
  }

  return {
    industryLarge: null,
    industryMiddle: null,
    industrySmall: null,
    industryDetail: industry,
  };
}

/**
 * タイプGのデータをマッピング（手動パース対応）
 */
function mapTypeG(row: Record<string, any>): Record<string, any> {
  const data = getEmptyTemplate();
  
  data.name = row["会社名"] || null;
  data.corporateNumber = row["法人番号"] || null;
  data.prefecture = row["都道府県"] || null;
  data.address = row["住所"] || null;
  data.headquartersAddress = row["住所"] || null;
  data.industry = row["業種"] || null;
  
  const industryCategories = inferIndustryCategories(row["業種"]);
  data.industryLarge = industryCategories.industryLarge;
  data.industryMiddle = industryCategories.industryMiddle;
  data.industrySmall = industryCategories.industrySmall;
  data.industryDetail = industryCategories.industryDetail;
  
  data.capitalStock = row["資本金"] ? parseFloat(String(row["資本金"]).replace(/,/g, "")) : null;
  data.revenue = row["売上"] ? parseFloat(String(row["売上"]).replace(/,/g, "")) : null;
  data.financials = row["直近利益"] || null;
  data.employeeCount = row["従業員数"] ? parseInt(String(row["従業員数"]).replace(/,/g, "")) : null;
  data.established = row["設立"] || null;
  data.fiscalMonth = row["決算月"] || null;
  data.listing = row["上場"] || null;
  data.representativeName = row["代表者名"] || null;
  data.businessDescriptions = row["businessDescriptions"] || null;
  data.companyUrl = row["URL"] || null;
  data.overview = row["overview"] || null;
  
  data.totalAssets = row["totalAssets"] ? parseFloat(String(row["totalAssets"]).replace(/,/g, "")) : null;
  data.totalLiabilities = row["totalLiabilities"] ? parseFloat(String(row["totalLiabilities"]).replace(/,/g, "")) : null;
  data.netAssets = row["netAssets"] ? parseFloat(String(row["netAssets"]).replace(/,/g, "")) : null;
  data.revenueFromStatements = row["revenueFromStatements"] ? parseFloat(String(row["revenueFromStatements"]).replace(/,/g, "")) : null;
  data.operatingIncome = row["operatingIncome"] ? parseFloat(String(row["operatingIncome"]).replace(/,/g, "")) : null;
  
  if (row["銀行"]) {
    const banksStr = String(row["銀行"]);
    const banksArr = banksStr.split(/[・、,]/).map(s => s.trim()).filter(s => s);
    (data as any).banks = banksArr;
  }
  
  if (row["affiliations"]) {
    const affiliationsStr = String(row["affiliations"]);
    const affiliationsArr = affiliationsStr.split(/[、,]/).map(s => s.trim()).filter(s => s);
    (data as any).affiliations = affiliationsArr;
  }
  
  return data;
}

/**
 * CSVファイルを読み込む（タイプCとGは手動パース）
 */
function loadCSV(csvPath: string, type: string): Record<string, any>[] {
  const csvContent = fs.readFileSync(csvPath, "utf-8");
  
  if (type === "C") {
    // タイプCは手動パース
    const lines = csvContent.split("\n");
    const records: Record<string, any>[] = [];
    
    if (lines.length < 2) return records;
    
    const headers = lines[0].split(",");
    
    for (let i = 1; i < lines.length; i++) {
      if (!lines[i].trim()) continue;
      
      const values: string[] = [];
      let currentValue = "";
      let inQuotes = false;
      
      for (let j = 0; j < lines[i].length; j++) {
        const char = lines[i][j];
        if (char === '"') {
          inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
          values.push(currentValue.trim());
          currentValue = "";
        } else {
          currentValue += char;
        }
      }
      values.push(currentValue.trim());
      
      const row: Record<string, any> = {};
      headers.forEach((header, index) => {
        row[header.trim()] = values[index] || null;
      });
      records.push(row);
    }
    
    return records;
  } else if (type === "G") {
    // タイプGも手動パース
    const lines = csvContent.split("\n");
    const records: Record<string, any>[] = [];
    
    if (lines.length < 2) return records;
    
    const headers = lines[0].split(",");
    
    for (let i = 1; i < lines.length; i++) {
      if (!lines[i].trim()) continue;
      
      const values: string[] = [];
      let currentValue = "";
      let inQuotes = false;
      
      for (let j = 0; j < lines[i].length; j++) {
        const char = lines[i][j];
        if (char === '"') {
          inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
          values.push(currentValue.trim());
          currentValue = "";
        } else {
          currentValue += char;
        }
      }
      values.push(currentValue.trim());
      
      const row: Record<string, any> = {};
      headers.forEach((header, index) => {
        row[header.trim()] = values[index] || null;
      });
      records.push(row);
    }
    
    return records;
  } else {
    // タイプA、B、Dは通常のパース
    return parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
      trim: true,
    }) as Record<string, any>[];
  }
}

/**
 * メイン処理
 */
async function main() {
  console.log("================================================================================");
  console.log("タイプA・B・C・D・Gの企業データ更新");
  console.log("================================================================================");
  console.log(`モード: ${DRY_RUN ? "DRY RUN（書き込みなし）" : "実行"}`);
  console.log();

  const stats = {
    total: 0,
    found: 0,
    updated: 0,
    notFound: 0,
    errors: 0,
  };

  for (const [type, fileNumbers] of Object.entries(TYPE_CSV_FILES)) {
    console.log(`\n${"=".repeat(80)}`);
    console.log(`タイプ${type}の処理開始`);
    console.log("=".repeat(80));
    
    for (const fileNum of fileNumbers) {
      const csvPath = `csv/${fileNum}.csv`;
      
      if (!fs.existsSync(csvPath)) {
        console.log(`⚠️  ${csvPath} が見つかりません。スキップします。`);
        continue;
      }
      
      console.log(`\n処理中: ${csvPath}`);
      
      try {
        const records = loadCSV(csvPath, type);
        console.log(`  読み込み完了: ${records.length}件`);
        
        let fileFound = 0;
        let fileUpdated = 0;
        let fileNotFound = 0;
        
        for (const row of records) {
          stats.total++;
          
          let companyData: Record<string, any>;
          
          switch (type) {
            case "A":
              companyData = mapTypeA(row);
              break;
            case "B":
              companyData = mapTypeB(row);
              break;
            case "C":
              companyData = mapTypeC(row);
              break;
            case "D":
              companyData = mapTypeD(row);
              break;
            case "G":
              companyData = mapTypeG(row);
              break;
            default:
              continue;
          }
          
          if (!companyData.name || !companyData.address) {
            continue; // 企業名と住所がない場合はスキップ
          }
          
          // 企業を検索
          const match = await findCompany(companyData);
          
          if (match && match.score >= 100) {
            fileFound++;
            stats.found++;
            
            if (!DRY_RUN) {
              try {
                await db.collection(COLLECTION_NAME).doc(match.docId).update(companyData);
                fileUpdated++;
                stats.updated++;
              } catch (error: any) {
                console.error(`    ❌ 更新エラー: ${error.message}`);
                stats.errors++;
              }
            } else {
              fileUpdated++;
              stats.updated++;
            }
          } else {
            fileNotFound++;
            stats.notFound++;
          }
          
          // 進捗表示（100件ごと）
          if (stats.total % 100 === 0) {
            console.log(`  進捗: ${stats.total}件処理済み (見つかった: ${stats.found}, 更新: ${stats.updated}, 見つからず: ${stats.notFound})`);
          }
        }
        
        console.log(`  ✓ ${csvPath} 完了: 見つかった ${fileFound}件, 更新 ${fileUpdated}件, 見つからず ${fileNotFound}件`);
        
      } catch (error: any) {
        console.error(`  ❌ ${csvPath} エラー: ${error.message}`);
        stats.errors++;
      }
    }
  }

  console.log("\n" + "=".repeat(80));
  console.log("処理結果サマリー");
  console.log("=".repeat(80));
  console.log(`総処理件数: ${stats.total}件`);
  console.log(`企業が見つかった: ${stats.found}件`);
  console.log(`更新${DRY_RUN ? "（DRY RUN）" : ""}: ${stats.updated}件`);
  console.log(`企業が見つからなかった: ${stats.notFound}件`);
  console.log(`エラー: ${stats.errors}件`);
  console.log("=".repeat(80));
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("予期しないエラー:", err);
  process.exit(1);
});

