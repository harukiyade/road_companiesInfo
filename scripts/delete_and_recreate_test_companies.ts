/* 
  テストドキュメントを削除して、全159フィールドで新規作成するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/delete_and_recreate_test_companies.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";

// 各グループの代表ファイル
const GROUP_REPRESENTATIVES = {
  group1: "csv/111.csv",
  group2: "csv/118.csv",
  group3: "csv/38.csv",
  group4: "csv/107.csv",
  group5: "csv/110.csv",
  group6: "csv/119.csv",
  group7: "csv/122.csv",
};

// 全159フィールドのテンプレート
const COMPANY_TEMPLATE: Record<string, any> = {
  // 基本情報（14フィールド）
  name: null,
  nameEn: null,
  kana: null,
  corporateNumber: null,
  corporationType: null,
  nikkeiCode: null,
  badges: [],
  tags: [],
  createdAt: null,
  updatedAt: null,
  updateDate: null,
  updateCount: null,
  changeCount: null,
  qualificationGrade: null,
  registrant: null,
  
  // 所在地情報（6フィールド）
  prefecture: null,
  address: null,
  headquartersAddress: null,
  postalCode: null,
  location: null,
  departmentLocation: null,
  
  // 連絡先情報（6フィールド）
  phoneNumber: null,
  contactPhoneNumber: null,
  fax: null,
  email: null,
  companyUrl: null,
  contactFormUrl: null,
  
  // 代表者情報（10フィールド）
  representativeName: null,
  representativeKana: null,
  representativeTitle: null,
  representativeBirthDate: null,
  representativePhone: null,
  representativePostalCode: null,
  representativeHomeAddress: null,
  representativeRegisteredAddress: null,
  representativeAlmaMater: null,
  executives: null,
  
  // 役員情報（20フィールド）
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
  
  // 業種情報（13フィールド）
  industry: null,
  industryLarge: null,
  industryMiddle: null,
  industrySmall: null,
  industryDetail: null,
  industries: [],
  industryCategories: null,
  businessDescriptions: null,
  businessItems: [],
  businessSummary: null,
  specialties: null,
  demandProducts: null,
  specialNote: null,
  
  // 財務情報（29フィールド）
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
  
  // 企業規模・組織（10フィールド）
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
  
  // 設立・沿革（5フィールド）
  established: null,
  dateOfEstablishment: null,
  founding: null,
  foundingYear: null,
  acquisition: null,
  
  // 取引先・関係会社（7フィールド）
  clients: null,
  suppliers: [],
  subsidiaries: [],
  affiliations: null,
  shareholders: null,
  banks: [],
  bankCorporateNumber: null,
  
  // 部署・拠点情報（21フィールド）
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
  
  // 企業説明（4フィールド）
  overview: null,
  companyDescription: null,
  salesNotes: null,
  
  // SNS・外部リンク（9フィールド）
  urls: [],
  profileUrl: null,
  externalDetailUrl: null,
  facebook: null,
  linkedin: null,
  wantedly: null,
  youtrust: null,
  metaKeywords: null,
  metaDescription: null,
  
  // 取引状態・内部管理（4フィールド）
  tradingStatus: null,
  adExpiration: null,
  numberOfActivity: null,
  transportation: null,
};

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
      path.join(projectRoot, "config", "serviceAccountKey.json"),
      path.join(projectRoot, ".config", "serviceAccountKey.json"),
    ];

    for (const pth of defaultPaths) {
      const resolved = path.resolve(pth);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolved}`);
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
    const projectId =
      serviceAccount.project_id ||
      process.env.GCLOUD_PROJECT ||
      process.env.GCP_PROJECT;

    if (!projectId) {
      console.error("❌ エラー: Project ID を検出できませんでした");
      process.exit(1);
    }

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId,
    });

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
  } catch (err: any) {
    console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
    console.error(`   詳細: ${err.message}`);
    process.exit(1);
  }
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// ==============================
// ヘルパー関数
// ==============================
function trim(v?: string | null): string | null {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function generateNumericDocId(
  corporateNumber: string | null | undefined,
  index: number,
  existingDocId?: string
): string {
  if (
    corporateNumber &&
    typeof corporateNumber === "string" &&
    /^[0-9]+$/.test(corporateNumber.trim())
  ) {
    return corporateNumber.trim();
  }
  if (existingDocId && /^[0-9]+$/.test(existingDocId)) {
    return existingDocId;
  }
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 代表者名から生年月日を抽出
function extractBirthDateFromRepresentativeName(representativeName: string | null | undefined): string | null {
  if (!representativeName || typeof representativeName !== "string") return null;
  
  const trimmed = representativeName.trim();
  if (!trimmed) return null;
  
  const birthdatePatterns = [
    /(19\d{2}|20\d{2})[\/年-](\d{1,2})[\/月-](\d{1,2})/g,
    /(19\d{2}|20\d{2})\/(\d{1,2})\/(\d{1,2})/g,
  ];
  
  for (const pattern of birthdatePatterns) {
    const match = trimmed.match(pattern);
    if (match) {
      const dateStr = match[0];
      const parts = dateStr.split(/[\/年-]/);
      if (parts.length >= 3) {
        const year = parseInt(parts[0]);
        const month = parseInt(parts[1]);
        const day = parseInt(parts[2]);
        
        if (year >= 1900 && year <= 2100 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
          return dateStr;
        }
      }
    }
  }
  
  return null;
}

// 代表者名から個人名（氏名）のみを抽出
function extractPersonNameFromRepresentative(representativeName: string | null | undefined): string | null {
  if (!representativeName || typeof representativeName !== "string") return null;
  
  let trimmed = representativeName.trim();
  if (!trimmed) return null;
  
  const titles = [
    "代表取締役", "代表取締役社長", "代表取締役会長", "代表取締役専務",
    "代表取締役常務", "代表取締役副社長", "取締役社長", "取締役会長",
    "社長", "会長", "専務", "常務", "副社長", "代表", "代表者", "CEO", "ceo"
  ];
  
  for (const title of titles) {
    if (trimmed.startsWith(title)) {
      trimmed = trimmed.substring(title.length).trim();
      trimmed = trimmed.replace(/^[\s・、,，]/g, "").trim();
      break;
    }
    const titlePattern = new RegExp(`^${title}[\\s・、,，]`, "i");
    if (titlePattern.test(trimmed)) {
      trimmed = trimmed.replace(titlePattern, "").trim();
      break;
    }
  }
  
  trimmed = trimmed.replace(/[（(].*?[）)]/g, "").trim();
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})[\/年-]\d{1,2}[\/月-]\d{1,2}/g, "").trim();
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})\/\d{1,2}\/\d{1,2}/g, "").trim();
  
  if (/^[\d\s\-・、,，.。]+$/.test(trimmed)) {
    return null;
  }
  
  return trimmed || null;
}

// 代表者名を処理して、個人名と生年月日を分離
function processRepresentativeName(representativeName: string | null | undefined, mapped: Record<string, any>): void {
  if (!representativeName || typeof representativeName !== "string") return;
  
  const trimmed = representativeName.trim();
  if (!trimmed) return;
  
  const birthDate = extractBirthDateFromRepresentativeName(trimmed);
  if (birthDate && !mapped.representativeBirthDate) {
    mapped.representativeBirthDate = birthDate;
  }
  
  const personName = extractPersonNameFromRepresentative(trimmed);
  if (personName) {
    mapped.representativeName = personName;
  } else {
    let cleaned = trimmed;
    if (birthDate) {
      cleaned = cleaned.replace(birthDate, "").trim();
      cleaned = cleaned.replace(/^[\s・、,，\-]/g, "").replace(/[\s・、,，\-]$/g, "").trim();
    }
    if (cleaned && cleaned.length > 0) {
      mapped.representativeName = cleaned;
    }
  }
}

// 既存ドキュメントをチェック（法人番号で検索）
async function findExistingCompany(
  corporateNumber: string | null | undefined,
  companyName: string | null | undefined
): Promise<DocumentReference | null> {
  // 1. 法人番号で検索（最優先）
  if (corporateNumber && /^\d{13}$/.test(corporateNumber.trim())) {
    const normalizedCorpNum = corporateNumber.trim();
    
    // docId=法人番号で直接参照
    const directRef = companiesCol.doc(normalizedCorpNum);
    const directSnap = await directRef.get();
    if (directSnap.exists) {
      return directRef;
    }
    
    // corporateNumberフィールドで検索
    const snapByCorp = await companiesCol
      .where("corporateNumber", "==", normalizedCorpNum)
      .limit(1)
      .get();
    if (!snapByCorp.empty) {
      return snapByCorp.docs[0].ref;
    }
  }
  
  // 2. 企業名で検索（法人番号がない場合）
  if (companyName && companyName.trim()) {
    const snapByName = await companiesCol
      .where("name", "==", companyName.trim())
      .limit(1)
      .get();
    if (!snapByName.empty) {
      return snapByName.docs[0].ref;
    }
  }
  
  return null;
}

// CSV行をマッピング（簡易版）
function mapCsvRowToCompany(row: Record<string, string>): Record<string, any> {
  const mapped: Record<string, any> = { ...COMPANY_TEMPLATE };
  
  // 基本マッピング
  if (row["会社名"]) mapped.name = trim(row["会社名"]);
  if (row["法人番号"]) mapped.corporateNumber = trim(row["法人番号"]);
  if (row["都道府県"]) mapped.prefecture = trim(row["都道府県"]);
  if (row["住所"]) mapped.address = trim(row["住所"]);
  if (row["郵便番号"]) mapped.postalCode = trim(row["郵便番号"]);
  if (row["電話番号"]) mapped.phoneNumber = trim(row["電話番号"]);
  if (row["URL"]) mapped.companyUrl = trim(row["URL"]);
  if (row["代表者名"]) {
    processRepresentativeName(row["代表者名"], mapped);
  }
  if (row["代表者誕生日"]) mapped.representativeBirthDate = trim(row["代表者誕生日"]);
  if (row["資本金"]) {
    const capital = String(row["資本金"]).replace(/[,，]/g, "");
    const num = Number(capital);
    if (!Number.isNaN(num)) mapped.capitalStock = num;
  }
  if (row["売上"]) {
    const revenue = String(row["売上"]).replace(/[,，]/g, "");
    const num = Number(revenue);
    if (!Number.isNaN(num)) mapped.revenue = num;
  }
  if (row["業種1"]) mapped.industryLarge = trim(row["業種1"]);
  if (row["業種2"]) mapped.industryMiddle = trim(row["業種2"]);
  if (row["業種3"]) mapped.industrySmall = trim(row["業種3"]);
  
  // タイムスタンプ
  mapped.createdAt = admin.firestore.FieldValue.serverTimestamp();
  mapped.updatedAt = admin.firestore.FieldValue.serverTimestamp();
  
  return mapped;
}

// ==============================
// メイン処理
// ==============================
async function main() {
  try {
    console.log("\n🗑️  既存のテストドキュメントを削除中...\n");
    
    // 最新のログファイルから削除対象のIDを取得
    const logFiles = fs.readdirSync(".")
      .filter(f => f.startsWith("created_test_companies_") && f.endsWith(".txt"))
      .sort()
      .reverse();
    
    const docIdsToDelete: string[] = [];
    
    if (logFiles.length > 0) {
      const latestLog = logFiles[0];
      console.log(`📄 ログファイルを読み込み: ${latestLog}`);
      const content = fs.readFileSync(latestLog, "utf8");
      const lines = content.split("\n").filter(l => l.trim());
      
      for (const line of lines) {
        const match = line.match(/:\s*(\d+)\s*\(/);
        if (match) {
          docIdsToDelete.push(match[1]);
        }
      }
    }
    
    console.log(`📋 削除対象: ${docIdsToDelete.length}件\n`);
    
    // バッチ削除
    const BATCH_SIZE = 500;
    let deletedCount = 0;
    
    for (let i = 0; i < docIdsToDelete.length; i += BATCH_SIZE) {
      const batch = db.batch();
      const batchIds = docIdsToDelete.slice(i, i + BATCH_SIZE);
      
      for (const docId of batchIds) {
        const ref = companiesCol.doc(docId);
        batch.delete(ref);
      }
      
      await batch.commit();
      deletedCount += batchIds.length;
      console.log(`✅ ${deletedCount}/${docIdsToDelete.length}件削除完了`);
    }
    
    console.log(`\n✨ 削除完了: ${deletedCount}件\n`);
    
    // 新規作成
    console.log("📝 全159フィールドで新規ドキュメントを作成中...\n");
    
    const createdDocIds: string[] = [];
    let globalIndex = 0;
    
    for (const [groupName, csvPath] of Object.entries(GROUP_REPRESENTATIVES)) {
      if (!fs.existsSync(csvPath)) {
        console.warn(`⚠️  ファイルが見つかりません: ${csvPath}`);
        continue;
      }
      
      console.log(`📂 ${groupName}: ${csvPath}`);
      
      const csvContent = fs.readFileSync(csvPath, "utf8");
      const records = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        encoding: "utf8",
        relax_column_count: true,
        relax_quotes: true,
      }) as Record<string, string>[];
      
      // 最初の5行のみ処理
      const rowsToProcess = records.slice(0, 5);
      
      for (let i = 0; i < rowsToProcess.length; i++) {
        const row = rowsToProcess[i] as Record<string, string>;
        const mapped = mapCsvRowToCompany(row);
        
        // 既存ドキュメントをチェック
        const existingRef = await findExistingCompany(
          mapped.corporateNumber,
          mapped.name
        );
        
        if (existingRef) {
          // 既存ドキュメントが見つかった場合はスキップ
          const companyName = mapped.name || "名前なし";
          console.log(`  ⏭️  行${i + 1}: ${companyName} (既存のためスキップ)`);
          continue;
        }
        
        const docId = generateNumericDocId(mapped.corporateNumber, globalIndex);
        globalIndex++;
        
        const ref = companiesCol.doc(docId);
        await ref.set(mapped);
        
        const companyName = mapped.name || docId;
        createdDocIds.push(`${groupName} - ${path.basename(csvPath)} - 行${i + 1}: ${docId} (${companyName})`);
        console.log(`  ✅ 行${i + 1}: ${docId} (${companyName})`);
      }
      
      console.log("");
    }
    
    // ログファイルに保存
    const timestamp = Date.now();
    const logFileName = `created_test_companies_${timestamp}.txt`;
    fs.writeFileSync(logFileName, createdDocIds.join("\n"));
    
    console.log(`\n✨ 作成完了: ${createdDocIds.length}件`);
    console.log(`📄 ログファイル: ${logFileName}\n`);
    
    // フィールド数の確認
    const sampleDoc = await companiesCol.doc(createdDocIds[0].split(": ")[1].split(" ")[0]).get();
    if (sampleDoc.exists) {
      const data = sampleDoc.data();
      const fieldCount = Object.keys(data || {}).length;
      console.log(`📊 サンプルドキュメントのフィールド数: ${fieldCount}フィールド\n`);
    }
    
  } catch (error) {
    console.error("❌ エラーが発生しました:", error);
    process.exit(1);
  }
}

main().catch(console.error);

