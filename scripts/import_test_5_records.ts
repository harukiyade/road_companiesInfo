/* 
  テスト用: CSVファイルの最初の5件をインポートするスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/import_test_5_records.ts ./csv/add_20251224/1_20251224.csv
  
  DRY_RUNモード（実際には書き込まない）:
    DRY_RUN=1 npx ts-node scripts/import_test_5_records.ts ./csv/add_20251224/1_20251224.csv
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";

// ==============================
// Firebase 初期化
// ==============================
let db: Firestore | null = null;
let companiesCol: CollectionReference | null = null;

if (!DRY_RUN) {
  if (admin.apps.length === 0) {
    let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

    if (!serviceAccountPath) {
      const projectRoot = process.cwd();
      const defaultPaths = [
        "./serviceAccountKey.json",
        "./service-account-key.json",
        "./firebase-service-account.json",
        path.join(projectRoot, "serviceAccountKey.json"),
        path.join(projectRoot, "service-account-key.json"),
        path.join(projectRoot, "firebase-service-account.json"),
      ];

      for (const defaultPath of defaultPaths) {
        const resolvedPath = path.resolve(defaultPath);
        if (fs.existsSync(resolvedPath)) {
          serviceAccountPath = resolvedPath;
          break;
        }
      }
    }

    if (!serviceAccountPath) {
      console.error("❌ エラー: サービスアカウントキーファイルのパスが指定されていません");
      process.exit(1);
    }

    if (!fs.existsSync(serviceAccountPath)) {
      console.error(`❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
      process.exit(1);
    }

    try {
      const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
      const projectId = serviceAccount.project_id || process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT;

      if (!projectId) {
        console.error("❌ エラー: Project ID を検出できませんでした");
        process.exit(1);
      }

      admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
        projectId: projectId,
      });

      console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
    } catch (err: any) {
      console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
      console.error(`   詳細: ${err.message}`);
      process.exit(1);
    }
  }

  db = admin.firestore();
  companiesCol = db.collection(COLLECTION_NAME);
} else {
  console.log("🔍 DRY_RUN モード: Firestore には書き込みません\n");
}

// ==============================
// ヘルパー関数
// ==============================

function isEmptyValue(v: any): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  return false;
}

// 数値のみの文字列かチェック
function isNumericString(value: string): boolean {
  return /^\d+$/.test(value);
}

// ドキュメントIDを数値の文字列で新規生成（法人番号は使わない）
function generateNewNumericDocId(index: number): string {
  // タイムスタンプ + インデックスから数値IDを生成
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 企業IDを数値で生成（companyIdフィールド用）
function generateNewCompanyId(index: number): number {
  // タイムスタンプ + インデックスから数値IDを生成
  const timestamp = Date.now();
  return timestamp * 1000000 + index; // タイムスタンプ（ミリ秒）+ インデックス
}

// 千円単位を円単位に変換（資本金・売上・利益）
function convertFromThousandYen(value: string | null | undefined): number | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim().replace(/[,，]/g, "");
  const num = Number(cleaned);
  if (isNaN(num)) return null;
  return Math.round(num * 1000);
}

// 数値に変換（社員数、オフィス数、工場数、店舗数など）
function toNumber(value: string | null | undefined): number | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim().replace(/[,，]/g, "");
  const num = Number(cleaned);
  if (isNaN(num)) return null;
  return Math.round(num);
}

// 法人番号を正規化（13桁の数値のみ）
function normalizeCorporateNumber(value: string | null | undefined): string | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim().replace(/\D/g, "");
  if (cleaned.length === 13 && isNumericString(cleaned)) {
    return cleaned;
  }
  return null;
}

// 配列に変換（カンマ区切りの文字列を配列に）
function toArray(value: string | null | undefined): string[] | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim();
  if (!cleaned) return null;
  return cleaned.split(/[，,]/).map(s => s.trim()).filter(s => s.length > 0);
}

// 上場区分を正規化
function normalizeListing(value: string | null | undefined): string | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim();
  if (cleaned === "非上場" || cleaned === "上場") {
    return cleaned === "上場" ? "上場" : "非上場";
  }
  return cleaned || null;
}

// 日付を正規化（YYYY-MM-DD形式またはYYYY年MM月DD日形式）
function normalizeDate(value: string | null | undefined): string | null {
  if (isEmptyValue(value)) return null;
  const cleaned = String(value).trim();
  
  // YYYY-MM-DD形式
  if (/^\d{4}-\d{2}-\d{2}$/.test(cleaned)) {
    return cleaned;
  }
  
  // YYYY年MM月DD日形式 → YYYY-MM-DD
  const match = cleaned.match(/(\d{4})年(\d{1,2})月(\d{1,2})日/);
  if (match) {
    const year = match[1];
    const month = String(parseInt(match[2])).padStart(2, "0");
    const day = String(parseInt(match[3])).padStart(2, "0");
    return `${year}-${month}-${day}`;
  }
  
  // YYYY年MM月1日形式（直近決算年月）
  const match2 = cleaned.match(/(\d{4})年(\d{1,2})月1日/);
  if (match2) {
    const year = match2[1];
    const month = String(parseInt(match2[2])).padStart(2, "0");
    return `${year}-${month}-01`;
  }
  
  return cleaned || null;
}

// ==============================
// companies_new コレクション全フィールドテンプレート
// ==============================
const COMPANY_TEMPLATE: Record<string, any> = {
  // 基本情報
  companyId: null,
  name: null,
  nameEn: null,
  kana: null,
  corporateNumber: null,
  corporationType: null,
  nikkeiCode: null,
  badges: null,
  tags: null,
  createdAt: null,
  updatedAt: null,
  updateDate: null,
  updateCount: null,
  changeCount: null,
  qualificationGrade: null,
  
  // 所在地情報
  prefecture: null,
  address: null,
  headquartersAddress: null,
  postalCode: null,
  location: null,
  departmentLocation: null,
  
  // 連絡先情報
  phoneNumber: null,
  contactPhoneNumber: null,
  fax: null,
  email: null,
  companyUrl: null,
  contactFormUrl: null,
  
  // 代表者情報
  representativeName: null,
  representativeKana: null,
  representativeTitle: null,
  representativeBirthDate: null,
  representativePhone: null,
  representativePostalCode: null,
  representativeHomeAddress: null,
  representativeRegisteredAddress: null,
  representativeAlmaMater: null,
  executives: null, // 配列フィールドだが、テンプレートではnullにして後で空配列に設定
  
  // 役員情報
  executiveName1: null,
  executiveName2: null,
  executiveName3: null,
  executiveName4: null,
  executiveName5: null,
  executiveName6: null,
  executiveName7: null,
  executiveName8: null,
  executiveName9: null,
  executiveName10: null,
  executivePosition1: null,
  executivePosition2: null,
  executivePosition3: null,
  executivePosition4: null,
  executivePosition5: null,
  executivePosition6: null,
  executivePosition7: null,
  executivePosition8: null,
  executivePosition9: null,
  executivePosition10: null,
  
  // 業種情報
  industry: null,
  industryLarge: null,
  industryMiddle: null,
  industrySmall: null,
  industryDetail: null,
  industries: [],
  industryCategories: null,
  businessDescriptions: null,
  businessItems: null,
  businessSummary: null,
  specialties: null,
  demandProducts: null,
  specialNote: null,
  
  // 財務情報
  capitalStock: null,
  revenue: null,
  latestRevenue: null,
  latestProfit: null,
  revenueFromStatements: null,
  operatingIncome: null,
  totalAssets: null,
  totalLiabilities: null,
  netAssets: null,
  issuedShares: null,
  financials: null,
  listing: null,
  marketSegment: null,
  latestFiscalYearMonth: null,
  fiscalMonth: null,
  fiscalMonth1: null,
  fiscalMonth2: null,
  fiscalMonth3: null,
  fiscalMonth4: null,
  fiscalMonth5: null,
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
  
  // 企業規模・組織
  employeeCount: null,
  employeeNumber: null,
  factoryCount: null,
  officeCount: null,
  storeCount: null,
  averageAge: null,
  averageYearsOfService: null,
  averageOvertimeHours: null,
  averagePaidLeave: null,
  femaleExecutiveRatio: null,
  
  // 設立・沿革
  established: null,
  dateOfEstablishment: null,
  founding: null,
  foundingYear: null,
  acquisition: null,
  
  // 取引先・関係会社
  clients: null, // 配列フィールドだが、テンプレートではnullにして後で空配列に設定
  suppliers: null, // 配列フィールドだが、テンプレートではnullにして後で空配列に設定
  subsidiaries: null,
  affiliations: null,
  shareholders: null, // 配列フィールドだが、テンプレートではnullにして後で空配列に設定
  banks: null, // 配列フィールドだが、テンプレートではnullにして後で空配列に設定
  bankCorporateNumber: null,
  
  // 部署・拠点情報
  departmentName1: null,
  departmentName2: null,
  departmentName3: null,
  departmentName4: null,
  departmentName5: null,
  departmentName6: null,
  departmentName7: null,
  departmentAddress1: null,
  departmentAddress2: null,
  departmentAddress3: null,
  departmentAddress4: null,
  departmentAddress5: null,
  departmentAddress6: null,
  departmentAddress7: null,
  departmentPhone1: null,
  departmentPhone2: null,
  departmentPhone3: null,
  departmentPhone4: null,
  departmentPhone5: null,
  departmentPhone6: null,
  departmentPhone7: null,
  
  // 企業説明
  overview: null,
  companyDescription: null,
  salesNotes: null,
  
  // SNS・外部リンク
  urls: [],
  profileUrl: null,
  externalDetailUrl: null,
  facebook: null,
  linkedin: null,
  wantedly: null,
  youtrust: null,
  metaKeywords: null,
};

// ==============================
// CSV マッピング関数
// ==============================

function mapCsvRowToCompanyData(row: Record<string, string>, index: number): Record<string, any> {
  // テンプレートをコピー（全フィールドをnullで初期化）
  const data: Record<string, any> = JSON.parse(JSON.stringify(COMPANY_TEMPLATE));

  // 会社名
  if (!isEmptyValue(row["会社名"])) {
    data.name = String(row["会社名"]).trim();
  }

  // 都道府県
  if (!isEmptyValue(row["都道府県"])) {
    data.prefecture = String(row["都道府県"]).trim();
  }

  // 代表者名
  if (!isEmptyValue(row["代表者名"])) {
    data.representativeName = String(row["代表者名"]).trim();
  }

  // 法人番号
  const corporateNumber = normalizeCorporateNumber(row["法人番号"]);
  if (corporateNumber) {
    data.corporateNumber = corporateNumber;
  }

  // URL
  if (!isEmptyValue(row["URL"])) {
    data.companyUrl = String(row["URL"]).trim();
  }

  // 業種1, 業種2, 業種3
  if (!isEmptyValue(row["業種1"])) {
    data.industryLarge = String(row["業種1"]).trim();
    data.industry = String(row["業種1"]).trim(); // 業種1をメインのindustryにも設定
  }
  if (!isEmptyValue(row["業種2"])) {
    data.industryMiddle = String(row["業種2"]).trim();
  }
  if (!isEmptyValue(row["業種3"])) {
    data.industrySmall = String(row["業種3"]).trim();
  }

  // 郵便番号
  if (!isEmptyValue(row["郵便番号"])) {
    const postalCode = String(row["郵便番号"]).trim().replace(/\D/g, "");
    if (postalCode.length === 7) {
      data.postalCode = postalCode.replace(/(\d{3})(\d{4})/, "$1-$2");
    }
  }

  // 住所
  if (!isEmptyValue(row["住所"])) {
    data.address = String(row["住所"]).trim();
    data.headquartersAddress = String(row["住所"]).trim(); // 住所を本社住所にも設定
  }

  // 設立
  const established = normalizeDate(row["設立"]);
  if (established) {
    data.established = established;
    data.dateOfEstablishment = established;
  }

  // 電話番号(窓口)
  if (!isEmptyValue(row["電話番号(窓口)"])) {
    data.phoneNumber = String(row["電話番号(窓口)"]).trim();
    data.contactPhoneNumber = String(row["電話番号(窓口)"]).trim();
  }

  // 代表者郵便番号
  if (!isEmptyValue(row["代表者郵便番号"])) {
    const repPostalCode = String(row["代表者郵便番号"]).trim().replace(/\D/g, "");
    if (repPostalCode.length === 7) {
      data.representativePostalCode = repPostalCode.replace(/(\d{3})(\d{4})/, "$1-$2");
    }
  }

  // 代表者住所
  if (!isEmptyValue(row["代表者住所"])) {
    data.representativeHomeAddress = String(row["代表者住所"]).trim();
  }

  // 代表者誕生日
  const repBirthDate = normalizeDate(row["代表者誕生日"]);
  if (repBirthDate) {
    data.representativeBirthDate = repBirthDate;
  }

  // 資本金（千円単位 → 円単位）
  const capitalStock = convertFromThousandYen(row["資本金"]);
  if (capitalStock !== null) {
    data.capitalStock = capitalStock;
  }

  // 上場
  const listing = normalizeListing(row["上場"]);
  if (listing) {
    data.listing = listing;
  }

  // 直近決算年月
  const latestFiscalYearMonth = normalizeDate(row["直近決算年月"]);
  if (latestFiscalYearMonth) {
    data.latestFiscalYearMonth = latestFiscalYearMonth;
  }

  // 直近売上（千円単位 → 円単位）
  const latestRevenue = convertFromThousandYen(row["直近売上"]);
  if (latestRevenue !== null) {
    data.latestRevenue = latestRevenue;
  }

  // 直近利益（千円単位 → 円単位）
  const latestProfit = convertFromThousandYen(row["直近利益"]);
  if (latestProfit !== null) {
    data.latestProfit = latestProfit;
  }

  // 説明
  if (!isEmptyValue(row["説明"])) {
    data.companyDescription = String(row["説明"]).trim();
  }

  // 概要
  if (!isEmptyValue(row["概要"])) {
    data.overview = String(row["概要"]).trim();
  }

  // 仕入れ先（配列フィールド）
  const suppliers = toArray(row["仕入れ先"]);
  if (suppliers && suppliers.length > 0) {
    data.suppliers = suppliers;
  } else {
    data.suppliers = []; // 空配列として設定
  }

  // 取引先（配列フィールド）
  const clients = toArray(row["取引先"]);
  if (clients && clients.length > 0) {
    data.clients = clients;
  } else {
    data.clients = []; // 空配列として設定
  }

  // 取引先銀行（配列フィールド）
  const banks = toArray(row["取引先銀行"]);
  if (banks && banks.length > 0) {
    data.banks = banks;
  } else {
    data.banks = []; // 空配列として設定
  }

  // 取締役（配列フィールド）
  const executives = toArray(row["取締役"]);
  if (executives && executives.length > 0) {
    data.executives = executives;
  } else {
    data.executives = []; // 空配列として設定
  }

  // 株主（配列フィールド）
  const shareholders = toArray(row["株主"]);
  if (shareholders && shareholders.length > 0) {
    data.shareholders = shareholders;
  } else {
    data.shareholders = []; // 空配列として設定
  }

  // 社員数
  const employeeCount = toNumber(row["社員数"]);
  if (employeeCount !== null) {
    data.employeeCount = employeeCount;
  }

  // オフィス数
  const officeCount = toNumber(row["オフィス数"]);
  if (officeCount !== null) {
    data.officeCount = officeCount;
  }

  // 工場数
  const factoryCount = toNumber(row["工場数"]);
  if (factoryCount !== null) {
    data.factoryCount = factoryCount;
  }

  // 店舗数
  const storeCount = toNumber(row["店舗数"]);
  if (storeCount !== null) {
    data.storeCount = storeCount;
  }

  // industries配列（業種1, 業種2, 業種3を配列に）
  const industries: string[] = [];
  if (!isEmptyValue(row["業種1"])) industries.push(String(row["業種1"]).trim());
  if (!isEmptyValue(row["業種2"])) industries.push(String(row["業種2"]).trim());
  if (!isEmptyValue(row["業種3"])) industries.push(String(row["業種3"]).trim());
  if (industries.length > 0) {
    data.industries = industries;
  } else {
    data.industries = []; // 空配列として設定（nullではなく）
  }

  // 作成日時と更新日時（テンプレートでnullにしていたので上書き）
  const now = admin.firestore.Timestamp.now();
  data.createdAt = now;
  data.updatedAt = now;
  data.updateDate = now.toDate().toISOString().split("T")[0];

  // 空配列フィールドは空配列のまま（nullではない）
  if (!data.urls) data.urls = [];
  if (!data.clients) data.clients = [];
  if (!data.banks) data.banks = [];
  if (!data.executives) data.executives = [];

  return data;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  const csvPath = process.argv[2];
  
  if (!csvPath) {
    console.error("❌ エラー: CSVファイルのパスを指定してください");
    console.error("   使い方: npx ts-node scripts/import_test_5_records.ts <csv-file>");
    process.exit(1);
  }

  if (!fs.existsSync(csvPath)) {
    console.error(`❌ エラー: CSVファイルが見つかりません: ${csvPath}`);
    process.exit(1);
  }

  console.log(`📄 CSVファイルを読み込み中: ${csvPath}`);

  // CSVファイルを読み込み
  const csvContent = fs.readFileSync(csvPath, "utf8");
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
  }) as Record<string, string>[];

  console.log(`📊 CSVファイルの総行数: ${records.length} 件`);

  // 最初の5件のみを処理
  const testRecords = records.slice(0, 5);
  console.log(`\n🧪 テスト用に最初の ${testRecords.length} 件を処理します\n`);

  const batch: WriteBatch | null = db ? db.batch() : null;
  const docIds: string[] = [];

  // 重複チェック用のヘルパー関数（既存の関数と重複しないように、ローカル関数として定義）
  function normalizeStringForDuplicate(s: string | null | undefined): string {
    if (!s) return "";
    return String(s).trim().replace(/\s+/g, "");
  }

  function normalizeCorporateNumberForDuplicate(value: string | null | undefined): string | null {
    if (!value) return null;
    const cleaned = String(value).trim().replace(/\D/g, "");
    if (cleaned.length === 13) {
      return cleaned;
    }
    return null;
  }

  async function checkDuplicate(
    name: string | null,
    address: string | null,
    corporateNumber: string | null
  ): Promise<boolean> {
    if (!companiesCol || !name || !address) return false;

    const normalizedName = normalizeStringForDuplicate(name);
    const normalizedAddress = normalizeStringForDuplicate(address);
    const normalizedCorpNum = normalizeCorporateNumberForDuplicate(corporateNumber);

    if (!normalizedName || !normalizedAddress) return false;

    // 法人番号で検索（最も確実）
    if (normalizedCorpNum) {
      const snapByCorp = await companiesCol
        .where("corporateNumber", "==", normalizedCorpNum)
        .limit(1)
        .get();
      if (!snapByCorp.empty) {
        return true;
      }
    }

    // 企業名+住所で検索
    const snapByName = await companiesCol
      .where("name", "==", name.trim())
      .limit(100)
      .get();

    for (const doc of snapByName.docs) {
      const data = doc.data();
      const docAddress = normalizeStringForDuplicate(data.address || data.headquartersAddress);
      if (docAddress === normalizedAddress) {
        return true;
      }
    }

    return false;
  }

  for (let i = 0; i < testRecords.length; i++) {
    const row = testRecords[i];
    const companyData = mapCsvRowToCompanyData(row, i + 1);

    // 重複チェック
    if (!DRY_RUN && companiesCol) {
      const isDuplicate = await checkDuplicate(
        companyData.name,
        companyData.address || companyData.headquartersAddress,
        companyData.corporateNumber
      );

      if (isDuplicate) {
        console.log(`⏭️  [${i + 1}] スキップ（重複）: ${companyData.name || "(未設定)"}`);
        console.log(`   法人番号: ${companyData.corporateNumber || "(未設定)"}`);
        console.log(`   住所: ${companyData.address || companyData.headquartersAddress || "(未設定)"}`);
        console.log("");
        continue;
      }
    }

    // ドキュメントIDを新規生成（数値の文字列、法人番号は使わない）
    const docId = generateNewNumericDocId(i + 1);
    docIds.push(docId);
    
    // 企業IDを数値で生成して設定
    const companyId = generateNewCompanyId(i + 1);
    companyData.companyId = companyId;

    if (!DRY_RUN && companiesCol) {
      // 既存ドキュメントの確認（念のため、新規IDなので通常は存在しない）
      const docRef = companiesCol.doc(docId);
      const existingDoc = await docRef.get();

      if (existingDoc.exists) {
        console.log(`⚠️  ドキュメントID ${docId} は既に存在します。上書きします。`);
      }

      // バッチに追加
      if (batch) {
        batch.set(docRef, companyData, { merge: false });
      }
    }

    console.log(`✅ [${i + 1}] ドキュメントID: ${docId}`);
    console.log(`   会社名: ${companyData.name || "(未設定)"}`);
    console.log(`   法人番号: ${companyData.corporateNumber || "(未設定)"}`);
    console.log(`   都道府県: ${companyData.prefecture || "(未設定)"}`);
    console.log(`   資本金: ${companyData.capitalStock ? companyData.capitalStock.toLocaleString() + "円" : "(未設定)"}`);
    console.log(`   直近売上: ${companyData.latestRevenue ? companyData.latestRevenue.toLocaleString() + "円" : "(未設定)"}`);
    console.log(`   直近利益: ${companyData.latestProfit ? companyData.latestProfit.toLocaleString() + "円" : "(未設定)"}`);
    console.log("");
  }

  // バッチをコミット
  if (!DRY_RUN && db && batch) {
    console.log("💾 Firestoreに書き込み中...");
    await batch.commit();
    console.log("\n✅ インポート完了！");
  } else {
    console.log("\n✅ プレビュー完了（DRY_RUN モードのため書き込みませんでした）");
  }
  console.log(`\n📋 インポートされたドキュメントID一覧:`);
  docIds.forEach((docId, index) => {
    console.log(`   ${index + 1}. ${docId}`);
  });
}

// 実行
main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
