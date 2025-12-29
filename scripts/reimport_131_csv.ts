/* 
  131.csvの内容を一度削除し、再度インポートするスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/reimport_131_csv.ts [--dry-run]
*/

import "dotenv/config";
import fs from "fs";
import path from "path";
import Papa from "papaparse";
import admin from "firebase-admin";

// Firebase初期化
function initAdmin() {
  if (admin.apps.length) return;
  try {
    admin.initializeApp({
      credential: admin.credential.applicationDefault(),
    });
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", (error as Error).message);
    throw error;
  }
}

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const CSV_FILE = "csv/131.csv";

// companies_newコレクションの全159フィールドテンプレート
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
  dateOfEstablishment: null,
  demandProducts: null,
  departmentLocation: null,
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
  profit1: null,
  profit2: null,
  profit3: null,
  profit4: null,
  profit5: null,
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
  netAssets: null,
  tradingStatus: null,
  transportation: null,
  updateCount: null,
  updateDate: null,
  updatedAt: null,
  urls: [],
  wantedly: null,
  youtrust: null,
};

// 文字列正規化
function norm(s: string | null | undefined): string {
  if (!s) return "";
  return s.toString().trim();
}

// 空欄チェック
function isEmpty(s: string | null | undefined): boolean {
  const v = norm(s);
  return !v || v === "-" || v === "ー" || v === "―" || v === "n/a";
}

// 郵便番号を検証・正規化
function normalizePostalCode(value: string | null | undefined): string | null {
  const v = norm(value);
  if (!v) return null;
  
  const digits = v.replace(/\D/g, "");
  if (digits.length === 7) {
    return `${digits.slice(0, 3)}-${digits.slice(3)}`;
  }
  
  const POSTAL_CODE_PATTERN = /^\d{3}-?\d{4}$/;
  if (POSTAL_CODE_PATTERN.test(v)) {
    return v.includes("-") ? v : `${v.slice(0, 3)}-${v.slice(3)}`;
  }
  
  return null;
}

// 法人番号を検証
function validateCorporateNumber(value: string | null | undefined): string | null {
  const v = norm(value);
  if (!v) return null;
  
  if (/^\d+\.\d+E\+\d+$/i.test(v) || /^\d+\.\d+E-\d+$/i.test(v) || /E/i.test(v)) {
    return null;
  }
  
  const digits = v.replace(/\D/g, "");
  if (digits.length === 13) {
    return digits;
  }
  
  return null;
}

// 数値変換（カンマ、円記号などを除去）
function parseNumber(value: string | null | undefined): number | null {
  const v = norm(value);
  if (!v) return null;
  
  const cleaned = v.replace(/[,，円¥¥人|名]/g, "");
  
  const unitMatch = cleaned.match(/^([\d.]+)\s*(億|万|千)?/);
  if (unitMatch) {
    const num = parseFloat(unitMatch[1]);
    if (isNaN(num)) return null;
    
    const unit = unitMatch[2];
    if (unit === "億") return Math.round(num * 100_000_000);
    if (unit === "万") return Math.round(num * 10_000);
    if (unit === "千") return Math.round(num * 1_000);
    return Math.round(num);
  }
  
  const num = parseFloat(cleaned.replace(/[^\d.]/g, ""));
  return isNaN(num) ? null : Math.round(num);
}

// 年を抽出（設立年など）
function extractYear(value: string | null | undefined): number | null {
  const v = norm(value);
  if (!v) return null;
  
  const match = v.match(/(\d{4})年/);
  if (match) {
    const year = parseInt(match[1]);
    if (year >= 1800 && year <= 2100) return year;
  }
  
  return null;
}

// 都道府県を抽出
const PREF_LIST = [
  "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
  "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
  "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
  "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
  "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
  "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
  "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
];

function extractPrefecture(addr: string | null | undefined): string | null {
  const v = norm(addr);
  if (!v) return null;
  
  for (const pref of PREF_LIST) {
    if (v.includes(pref)) return pref;
  }
  
  return null;
}

// CSV行をcompanies_new形式に変換
function convertRowToCompany(
  row: string[],
  headers: string[]
): Record<string, any> | null {
  // テンプレートをコピーして全フィールドを初期化
  const company: Record<string, any> = JSON.parse(JSON.stringify(COMPANY_TEMPLATE));
  
  // ヘッダーマッピング
  const headerMap: Record<string, number> = {};
  headers.forEach((h, i) => {
    if (h) {
      headerMap[h] = i;
    }
  });
  
  // 会社名
  const nameIdx = headerMap["name"];
  if (nameIdx !== undefined) {
    const name = norm(row[nameIdx]);
    if (!isEmpty(name)) company.name = name;
  }
  
  // 必須フィールドチェック
  if (!company.name) return null;
  
  // 法人番号
  const corpNumIdx = headerMap["corporateNumber"];
  if (corpNumIdx !== undefined) {
    const corpNum = validateCorporateNumber(row[corpNumIdx]);
    if (corpNum) company.corporateNumber = corpNum;
  }
  
  // 代表者名
  const repIdx = headerMap["representativeName"];
  if (repIdx !== undefined) {
    const repValue = norm(row[repIdx]);
    if (!isEmpty(repValue)) {
      company.representativeName = repValue;
    }
  }
  
  // 売上
  const revenueIdx = headerMap["revenue"];
  if (revenueIdx !== undefined) {
    const revenue = parseNumber(row[revenueIdx]);
    if (revenue !== null) company.latestRevenue = revenue;
  }
  
  // 資本金
  const capitalIdx = headerMap["capitalStock"];
  if (capitalIdx !== undefined) {
    const capital = parseNumber(row[capitalIdx]);
    if (capital !== null) company.capitalStock = capital;
  }
  
  // 上場
  const listingIdx = headerMap["listing"];
  if (listingIdx !== undefined) {
    const listing = norm(row[listingIdx]);
    if (!isEmpty(listing) && listing !== "-") company.listing = listing;
  }
  
  // 住所
  const addrIdx = headerMap["address"];
  if (addrIdx !== undefined) {
    const addrValue = norm(row[addrIdx]);
    if (!isEmpty(addrValue)) {
      company.address = addrValue;
      if (!company.prefecture) {
        const pref = extractPrefecture(addrValue);
        if (pref) company.prefecture = pref;
      }
    }
  }
  
  // 社員数
  const empIdx = headerMap["employeeCount"];
  if (empIdx !== undefined) {
    const emp = parseNumber(row[empIdx]);
    if (emp !== null) company.employeeCount = emp;
  }
  
  // 設立
  const establishedIdx = headerMap["established"];
  if (establishedIdx !== undefined) {
    const established = norm(row[establishedIdx]);
    if (!isEmpty(established)) {
      company.established = established;
      const year = extractYear(established);
      if (year) company.foundingYear = year;
    }
  }
  
  // 決算月
  const fiscalIdx = headerMap["fiscalMonth"];
  if (fiscalIdx !== undefined) {
    const fiscal = parseNumber(row[fiscalIdx]);
    if (fiscal !== null && fiscal >= 1 && fiscal <= 12) {
      company.fiscalMonth = fiscal;
    }
  }
  
  // 業種
  const industryLargeIdx = headerMap["industryLarge"];
  if (industryLargeIdx !== undefined) {
    const ind = norm(row[industryLargeIdx]);
    if (!isEmpty(ind)) company.industryLarge = ind;
  }
  
  const industryMiddleIdx = headerMap["industryMiddle"];
  if (industryMiddleIdx !== undefined) {
    const ind = norm(row[industryMiddleIdx]);
    if (!isEmpty(ind)) company.industryMiddle = ind;
  }
  
  const industrySmallIdx = headerMap["industrySmall"];
  if (industrySmallIdx !== undefined) {
    const ind = norm(row[industrySmallIdx]);
    if (!isEmpty(ind)) company.industrySmall = ind;
  }
  
  const industryDetailIdx = headerMap["industryDetail"];
  if (industryDetailIdx !== undefined) {
    const ind = norm(row[industryDetailIdx]);
    if (!isEmpty(ind)) company.industryDetail = ind;
  }
  
  // industries配列を構築
  const industries: string[] = [];
  if (company.industryLarge) industries.push(company.industryLarge);
  if (company.industryMiddle) industries.push(company.industryMiddle);
  if (company.industrySmall) industries.push(company.industrySmall);
  if (company.industryDetail) industries.push(company.industryDetail);
  if (industries.length > 0) company.industries = industries;
  
  // 電話番号
  const phoneIdx = headerMap["phoneNumber"];
  if (phoneIdx !== undefined) {
    const phone = norm(row[phoneIdx]);
    if (!isEmpty(phone)) company.phoneNumber = phone;
  }
  
  // URL
  const urlIdx = headerMap["companyUrl"];
  if (urlIdx !== undefined) {
    const url = norm(row[urlIdx]);
    if (!isEmpty(url) && (url.startsWith("http://") || url.startsWith("https://"))) {
      company.companyUrl = url;
    }
  }
  
  // 部署情報
  for (let i = 1; i <= 7; i++) {
    const deptNameIdx = headerMap[`departmentName${i}`];
    const deptAddrIdx = headerMap[`departmentAddress${i}`];
    const deptPhoneIdx = headerMap[`departmentPhone${i}`];
    
    if (deptNameIdx !== undefined) {
      const deptName = norm(row[deptNameIdx]);
      if (!isEmpty(deptName)) {
        (company as any)[`departmentName${i}`] = deptName;
      }
    }
    
    if (deptAddrIdx !== undefined) {
      const deptAddr = norm(row[deptAddrIdx]);
      if (!isEmpty(deptAddr)) {
        (company as any)[`departmentAddress${i}`] = deptAddr;
      }
    }
    
    if (deptPhoneIdx !== undefined) {
      const deptPhone = norm(row[deptPhoneIdx]);
      if (!isEmpty(deptPhone)) {
        (company as any)[`departmentPhone${i}`] = deptPhone;
      }
    }
  }
  
  // タイムスタンプ
  company.updatedAt = admin.firestore.FieldValue.serverTimestamp();
  company.createdAt = admin.firestore.FieldValue.serverTimestamp();
  
  return company;
}

// 数値IDを生成
function generateNumericId(): string {
  const timestamp = Date.now();
  const random = Math.floor(Math.random() * 10000);
  return `${timestamp}${random.toString().padStart(4, "0")}`;
}

// CSVファイルを読み込む
function readCsvFile(filePath: string): { headers: string[]; rows: string[][] } {
  const buf = fs.readFileSync(filePath);
  const text = buf.toString("utf8");
  
  const parsed = Papa.parse<string[]>(text, {
    skipEmptyLines: true,
    header: false,
  });
  
  if (parsed.errors?.length) {
    console.warn(`[CSV] パース警告: ${parsed.errors.slice(0, 3).map(e => e.message).join(", ")}`);
  }
  
  const rows = parsed.data as string[][];
  if (rows.length === 0) {
    return { headers: [], rows: [] };
  }
  
  const headers = rows[0].map(h => norm(h));
  const dataRows = rows.slice(1);
  
  return { headers, rows: dataRows };
}

// メイン処理
async function main() {
  initAdmin();
  const db = admin.firestore();
  const companiesCol = db.collection(COLLECTION_NAME);
  
  const csvPath = path.resolve(process.cwd(), CSV_FILE);
  
  if (!fs.existsSync(csvPath)) {
    console.error(`❌ CSVファイルが見つかりません: ${csvPath}`);
    process.exit(1);
  }
  
  console.log(`📁 CSVファイル読み込み中: ${csvPath}`);
  const { headers, rows } = readCsvFile(csvPath);
  
  if (headers.length === 0 || rows.length === 0) {
    console.error("❌ CSVファイルにデータがありません");
    process.exit(1);
  }
  
  console.log(`📊 ヘッダー数: ${headers.length}, データ行数: ${rows.length}`);
  
  // ステップ1: 131.csvのcorporateNumberリストを取得
  console.log("\n🔍 ステップ1: 131.csvのcorporateNumberリストを取得中...");
  const corporateNumbers = new Set<string>();
  const nameToCorporateNumber = new Map<string, string>();
  
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const headerMap: Record<string, number> = {};
    headers.forEach((h, idx) => {
      if (h) headerMap[h] = idx;
    });
    
    const corpNumIdx = headerMap["corporateNumber"];
    const nameIdx = headerMap["name"];
    
    if (corpNumIdx !== undefined) {
      const corpNum = validateCorporateNumber(row[corpNumIdx]);
      if (corpNum) {
        corporateNumbers.add(corpNum);
      }
    }
    
    if (nameIdx !== undefined && corpNumIdx !== undefined) {
      const name = norm(row[nameIdx]);
      const corpNum = validateCorporateNumber(row[corpNumIdx]);
      if (name && corpNum) {
        nameToCorporateNumber.set(name, corpNum);
      }
    }
  }
  
  console.log(`  ✅ corporateNumber数: ${corporateNumbers.size}`);
  
  // ステップ2: 該当するドキュメントを削除
  console.log("\n🗑️  ステップ2: 該当するドキュメントを削除中...");
  let deletedCount = 0;
  const BATCH_DELETE_SIZE = 400;
  
  if (!DRY_RUN) {
    // corporateNumberで検索して削除
    for (const corpNum of corporateNumbers) {
      try {
        // ドキュメントIDがcorporateNumberの場合
        const docRef = companiesCol.doc(corpNum);
        const doc = await docRef.get();
        if (doc.exists) {
          await docRef.delete();
          deletedCount++;
          if (deletedCount % 100 === 0) {
            console.log(`  💾 削除済み: ${deletedCount}件`);
          }
        }
        
        // corporateNumberフィールドで検索
        const snap = await companiesCol
          .where("corporateNumber", "==", corpNum)
          .get();
        
        if (!snap.empty) {
          let batch = db.batch();
          let batchCount = 0;
          
          for (const doc of snap.docs) {
            batch.delete(doc.ref);
            batchCount++;
            deletedCount++;
            
            if (batchCount >= BATCH_DELETE_SIZE) {
              await batch.commit();
              console.log(`  💾 削除バッチコミット: ${batchCount}件 (合計: ${deletedCount}件)`);
              batch = db.batch();
              batchCount = 0;
            }
          }
          
          if (batchCount > 0) {
            await batch.commit();
            console.log(`  💾 削除バッチコミット: ${batchCount}件 (合計: ${deletedCount}件)`);
          }
        }
      } catch (error) {
        console.error(`  ❌ 削除エラー (corporateNumber: ${corpNum}):`, (error as Error).message);
      }
    }
    
    console.log(`  ✅ 削除完了: ${deletedCount}件`);
  } else {
    console.log(`  🔍 DRY_RUN: ${corporateNumbers.size}件のcorporateNumberが見つかりました（削除は実行されません）`);
  }
  
  // ステップ3: 再インポート
  console.log("\n📥 ステップ3: 131.csvを再インポート中...");
  let created = 0;
  let skipped = 0;
  let errors = 0;
  const createdDocIds: string[] = [];
  
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];
    const actualRowNumber = i + 2; // ヘッダー行を除くため+2
    
    try {
      const company = convertRowToCompany(row, headers);
      
      if (!company) {
        console.log(`  ⏭️  行 ${actualRowNumber}: スキップ（必須フィールド不足）`);
        skipped++;
        continue;
      }
      
      if (DRY_RUN) {
        console.log(`  🔍 行 ${actualRowNumber}: ${company.name || "(名前なし)"}`);
        console.log(`     フィールド数: ${Object.keys(company).length}`);
        if (company.corporateNumber) {
          console.log(`     corporateNumber: ${company.corporateNumber}`);
        }
        created++;
      } else {
        const docId = generateNumericId();
        await companiesCol.doc(docId).set(company);
        createdDocIds.push(docId);
        console.log(`  ✅ 行 ${actualRowNumber}: ${company.name} (ID: ${docId})`);
        created++;
      }
    } catch (error) {
      console.error(`  ❌ 行 ${actualRowNumber}: エラー - ${(error as Error).message}`);
      errors++;
    }
  }
  
  // 結果サマリー
  console.log("\n" + "=".repeat(60));
  console.log("📊 処理結果サマリー");
  console.log("=".repeat(60));
  console.log(`削除: ${deletedCount}件`);
  console.log(`作成: ${created}件`);
  console.log(`スキップ: ${skipped}件`);
  console.log(`エラー: ${errors}件`);
  
  if (!DRY_RUN && createdDocIds.length > 0) {
    const outputFile = path.resolve(
      process.cwd(),
      `reimported_131_csv_${Date.now()}.txt`
    );
    fs.writeFileSync(outputFile, createdDocIds.join("\n"), "utf8");
    console.log(`\n📝 作成されたドキュメントID: ${outputFile}`);
  }
  
  console.log("\n✅ 処理完了");
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
