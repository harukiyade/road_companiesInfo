/* 
  108.csvをFirestore `companies_new`にインポートするスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/import_108_csv.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_FILE = path.join(process.cwd(), "csv", "108.csv");

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

    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolvedPath}`);
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

// companies_new の新規ドキュメント用テンプレート
const COMPANY_TEMPLATE: Record<string, any> = {
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

// 郵便番号パターン（3桁-4桁）
const POSTAL_CODE_PATTERN = /^\d{3}-\d{4}$/;

// 業種フィールドを検出して、郵便番号までの範囲を特定
function detectIndustryFields(headers: string[]): {
  industryFields: string[];
  postalCodeIndex: number;
} {
  const industryFields: string[] = [];
  let postalCodeIndex = -1;

  for (let i = 0; i < headers.length; i++) {
    const header = headers[i];
    
    // 業種フィールドを検出（業種1, 業種2, 業種3, ...）
    if (/^業種\d+$/.test(header) || /^業種（細）$/.test(header)) {
      industryFields.push(header);
    }
    
    // 郵便番号を検出（3桁-4桁のパターンを持つ列を探す）
    // 実際のデータを見て判断する必要があるが、ヘッダー名でも判断可能
    if (header === "郵便番号" || /^郵便番号/.test(header)) {
      postalCodeIndex = i;
      break;
    }
  }

  return { industryFields, postalCodeIndex };
}

// 行データから業種と郵便番号以降を分離
function parseRowData(
  row: Record<string, string>,
  headers: string[]
): {
  industries: string[];
  postalCode: string | null;
  addressIndex: number;
} {
  const industries: string[] = [];
  let postalCode: string | null = null;
  let addressIndex = -1;

  // URL列の位置を特定（URLの後から業種が始まる）
  let urlIndex = -1;
  for (let i = 0; i < headers.length; i++) {
    if (headers[i] === "URL" || headers[i].toLowerCase() === "url") {
      urlIndex = i;
      break;
    }
  }

  // URLの次の列から順番に処理
  const startIndex = urlIndex >= 0 ? urlIndex + 1 : 0;

  for (let i = startIndex; i < headers.length; i++) {
    const header = headers[i];
    const value = row[header]?.trim() || "";

    // 郵便番号パターン（3桁-4桁）を検出（ヘッダー名に関係なく）
    if (POSTAL_CODE_PATTERN.test(value)) {
      postalCode = value;
      addressIndex = i + 1; // 次の列から住所
      break;
    }

    // ヘッダー名が「郵便番号」の場合（値が郵便番号パターンでない場合でも）
    if (header === "郵便番号" || /^郵便番号/.test(header)) {
      if (POSTAL_CODE_PATTERN.test(value)) {
        postalCode = value;
        addressIndex = i + 1;
        break;
      }
    }

    // 空欄の場合はスキップ（ただし業種フィールドの場合は空でも処理）
    if (!value && !/^業種\d+$/.test(header) && header !== "業種（細）") {
      continue;
    }

    // 業種フィールドの場合（業種1, 業種2, 業種3, ...）
    if (/^業種\d+$/.test(header) || /^業種（細）$/.test(header)) {
      if (value) {
        industries.push(value);
      }
    }
    // 業種フィールドではないが、URLの後から郵便番号までを業種として扱う
    // （業種4以降がヘッダーにない場合の対応）
    else if (
      header !== "URL" &&
      header !== "備考" &&
      value &&
      value.length > 0 &&
      !POSTAL_CODE_PATTERN.test(value) && // 郵便番号パターンでない
      !/^\d+$/.test(value) && // 数字のみでない
      header !== "住所" // 住所ヘッダーでない
    ) {
      // 次の列が郵便番号かどうかを確認
      let isLastIndustry = false;
      if (i + 1 < headers.length) {
        const nextHeader = headers[i + 1];
        const nextValue = row[nextHeader]?.trim() || "";
        
        // 次の列が郵便番号パターンまたは「郵便番号」ヘッダーの場合
        if (
          POSTAL_CODE_PATTERN.test(nextValue) ||
          nextHeader === "郵便番号" ||
          /^郵便番号/.test(nextHeader)
        ) {
          isLastIndustry = true;
        }
      }

      // 業種として追加
      industries.push(value);
      
      // 次の列が郵便番号の場合はここで終了
      if (isLastIndustry) {
        break;
      }
    }
  }

  // 郵便番号が見つからなかった場合、ヘッダー名で再検索
  if (!postalCode) {
    for (let i = 0; i < headers.length; i++) {
      if (headers[i] === "郵便番号" || /^郵便番号/.test(headers[i])) {
        const pcValue = row[headers[i]]?.trim() || "";
        if (pcValue && POSTAL_CODE_PATTERN.test(pcValue)) {
          postalCode = pcValue;
          addressIndex = i + 1;
          break;
        }
      }
    }
  }

  return { industries, postalCode, addressIndex };
}

// 数値をパース（カンマ除去）
function parseNumeric(value: string | null | undefined): number | null {
  if (!value) return null;
  const cleaned = value.toString().replace(/[^\d.-]/g, "");
  if (!cleaned) return null;
  const num = Number(cleaned);
  return Number.isFinite(num) ? num : null;
}

// 日付をパース
function parseDate(value: string | null | undefined): string | null {
  if (!value) return null;
  const cleaned = value.trim();
  if (!cleaned) return null;
  return cleaned;
}

// 代表者名から生年月日を抽出
function extractBirthDate(value: string): { name: string; birthDate: string | null } {
  if (!value) return { name: "", birthDate: null };
  
  let cleaned = value.trim();
  let birthDate: string | null = null;

  // 生年月日パターン（1900-2100年の範囲）
  const patterns = [
    /(19\d{2}|20\d{2})[\/年-](\d{1,2})[\/月-](\d{1,2})/g,
    /(19\d{2}|20\d{2})\/(\d{1,2})\/(\d{1,2})/g,
  ];

  for (const pattern of patterns) {
    const match = cleaned.match(pattern);
    if (match) {
      birthDate = match[0];
      cleaned = cleaned.replace(pattern, "").trim();
      cleaned = cleaned.replace(/^[\s・、,，\-]/g, "").replace(/[\s・、,，\-]$/g, "").trim();
      break;
    }
  }

  return { name: cleaned, birthDate };
}

// 代表者名から個人名を抽出
function extractPersonName(representativeName: string | null | undefined): string | null {
  if (!representativeName || typeof representativeName !== "string") return null;
  
  let trimmed = representativeName.trim();
  if (!trimmed) return null;
  
  // 役職名を除去
  const titles = [
    "代表取締役", "代表取締役社長", "代表取締役会長", "代表取締役専務",
    "代表取締役常務", "代表取締役副社長", "取締役社長", "取締役会長",
    "社長", "会長", "専務", "常務", "副社長", "代表", "代表者", "CEO", "ceo",
    "取締役", "監査役", "（取）", "（監）", "（会）", "（常）", "（専）", "（代長）", "（代会）", "（相）", "（副長）"
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
  
  // カッコ内の情報を除去
  trimmed = trimmed.replace(/[（(].*?[）)]/g, "").trim();
  
  // 生年月日パターンを除去
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})[\/年-]\d{1,2}[\/月-]\d{1,2}/g, "").trim();
  trimmed = trimmed.replace(/(19\d{2}|20\d{2})\/\d{1,2}\/\d{1,2}/g, "").trim();
  
  // 数字や記号のみの場合はnull
  if (/^[\d\s\-・、,，.。]+$/.test(trimmed)) {
    return null;
  }
  
  return trimmed || null;
}

// 行データをcompanies_newのフィールドにマッピング
function mapRowToCompanyFields(
  row: Record<string, string>,
  headers: string[]
): Record<string, any> {
  const result: Record<string, any> = { ...COMPANY_TEMPLATE };

  // 業種と郵便番号以降を分離
  const { industries, postalCode, addressIndex } = parseRowData(row, headers);

  // 基本情報
  if (row["会社名"]?.trim()) {
    result.name = row["会社名"].trim();
  }

  if (row["都道府県"]?.trim()) {
    result.prefecture = row["都道府県"].trim();
  }

  // 代表者名の処理
  if (row["代表者名"]?.trim()) {
    const { name, birthDate } = extractBirthDate(row["代表者名"]);
    const personName = extractPersonName(name);
    if (personName) {
      result.representativeName = personName;
    } else if (name) {
      result.representativeName = name;
    }
    if (birthDate) {
      result.representativeBirthDate = birthDate;
    }
  }

  // 法人番号は無視（崩れているため）

  // URL
  if (row["URL"]?.trim()) {
    const url = row["URL"].trim();
    result.companyUrl = url;
    result.urls = [url];
  }

  // 業種（業種1→industryLarge、業種2→industryMiddle、業種3→industrySmall）
  if (industries.length > 0) {
    const filteredIndustries = industries.filter((ind) => ind && ind.trim());
    result.industries = filteredIndustries;
    
    // 業種1→industryLarge
    if (filteredIndustries.length > 0) {
      result.industryLarge = filteredIndustries[0];
      result.industry = filteredIndustries[0];
    }
    
    // 業種2→industryMiddle
    if (filteredIndustries.length > 1) {
      result.industryMiddle = filteredIndustries[1];
    }
    
    // 業種3→industrySmall
    if (filteredIndustries.length > 2) {
      result.industrySmall = filteredIndustries[2];
    }
  }

  // 郵便番号
  if (postalCode) {
    result.postalCode = postalCode;
  } else if (row["郵便番号"]?.trim()) {
    const pc = row["郵便番号"].trim();
    if (POSTAL_CODE_PATTERN.test(pc)) {
      result.postalCode = pc;
    }
  }

  // 住所（郵便番号の次の列を優先的に使用）
  // CSVの構造によっては「住所」列に郵便番号や業種が入っている場合があるため、
  // 郵便番号の次の列を優先的に住所として使用
  let addressValue: string | null = null;
  
  // まず、郵便番号の次の列を確認（addressIndexが設定されている場合）
  if (addressIndex >= 0 && addressIndex < headers.length) {
    const addressHeader = headers[addressIndex];
    const value = row[addressHeader]?.trim() || "";
    // 郵便番号パターンでなく、業種っぽくない場合は住所として使用
    if (value && !POSTAL_CODE_PATTERN.test(value) && !/^[^都道府県]*業/.test(value)) {
      addressValue = value;
    }
  }
  
  // 郵便番号の次の列が使えない場合、「住所」ヘッダーの列を確認
  // ただし、「住所」列が郵便番号パターンや業種の場合は無視
  if (!addressValue && row["住所"]?.trim()) {
    const value = row["住所"].trim();
    // 郵便番号パターンでなく、業種っぽくない場合は住所として使用
    if (!POSTAL_CODE_PATTERN.test(value) && !/^[^都道府県]*業/.test(value)) {
      addressValue = value;
    }
  }
  
  // それでも住所が見つからない場合、郵便番号の位置を再確認
  if (!addressValue) {
    // URLの後の列から順に確認し、郵便番号パターンを見つけたら次の列を住所として使用
    let urlIndex = -1;
    for (let i = 0; i < headers.length; i++) {
      if (headers[i] === "URL" || headers[i].toLowerCase() === "url") {
        urlIndex = i;
        break;
      }
    }
    
    const startIndex = urlIndex >= 0 ? urlIndex + 1 : 0;
    for (let i = startIndex; i < headers.length; i++) {
      const value = row[headers[i]]?.trim() || "";
      
      // 郵便番号パターンを検出
      if (POSTAL_CODE_PATTERN.test(value)) {
        // 次の列を住所として使用
        if (i + 1 < headers.length) {
          const nextHeader = headers[i + 1];
          const nextValue = row[nextHeader]?.trim() || "";
          if (nextValue && !POSTAL_CODE_PATTERN.test(nextValue) && !/^[^都道府県]*業/.test(nextValue)) {
            addressValue = nextValue;
            break;
          }
        }
      }
    }
  }
  
  if (addressValue) {
    result.address = addressValue;
    result.headquartersAddress = addressValue;
  }

  // 設立
  if (row["設立"]?.trim()) {
    result.established = parseDate(row["設立"]);
  }

  // 電話番号
  if (row["電話番号(窓口)"]?.trim()) {
    result.phoneNumber = row["電話番号(窓口)"].trim();
  }

  // 代表者郵便番号
  if (row["代表者郵便番号"]?.trim()) {
    const repPostalCode = row["代表者郵便番号"].trim();
    if (POSTAL_CODE_PATTERN.test(repPostalCode)) {
      // 代表者郵便番号はrepresentativeRegisteredAddressに含める
    }
  }

  // 代表者住所
  if (row["代表者住所"]?.trim()) {
    result.representativeRegisteredAddress = row["代表者住所"].trim();
  }

  // 代表者誕生日（既に代表者名から抽出済み）

  // 資本金（1000倍する）
  if (row["資本金"]?.trim()) {
    const capital = parseNumeric(row["資本金"]);
    if (capital !== null) {
      result.capitalStock = capital * 1000;
    }
  }

  // 上場
  if (row["上場"]?.trim()) {
    const listing = row["上場"].trim();
    result.listing = listing === "上場" || listing.includes("上場") ? "上場" : "非上場";
  }

  // 直近決算年月
  if (row["直近決算年月"]?.trim()) {
    result.fiscalMonth = parseDate(row["直近決算年月"]);
  }

  // 直近売上（1000倍する）
  if (row["直近売上"]?.trim()) {
    const revenue = parseNumeric(row["直近売上"]);
    if (revenue !== null) {
      result.revenue = revenue * 1000;
    }
  }

  // 直近利益（1000倍する）
  if (row["直近利益"]?.trim()) {
    const profit = parseNumeric(row["直近利益"]);
    if (profit !== null) {
      // financialsオブジェクトに格納する場合
      result.financials = {
        profit: profit * 1000,
      };
    }
  }

  // 説明
  if (row["説明"]?.trim()) {
    result.companyDescription = row["説明"].trim();
  }

  // 概要
  if (row["概要"]?.trim()) {
    result.overview = row["概要"].trim();
  }

  // 仕入れ先
  if (row["仕入れ先"]?.trim()) {
    const suppliers = row["仕入れ先"]
      .split(/[，,、]/)
      .map((s) => s.trim())
      .filter((s) => s);
    result.suppliers = suppliers;
  }

  // 取引先
  if (row["取引先"]?.trim()) {
    const clients = row["取引先"]
      .split(/[，,、]/)
      .map((c) => c.trim())
      .filter((c) => c);
    result.clients = clients;
  }

  // 取引先銀行
  if (row["取引先銀行"]?.trim()) {
    const banks = row["取引先銀行"]
      .split(/[，,、]/)
      .map((b) => b.trim())
      .filter((b) => b);
    // banksフィールドがない場合はsalesNotesに格納
    result.salesNotes = banks.join("，");
  }

  // 取締役
  if (row["取締役"]?.trim()) {
    const executives = row["取締役"]
      .split(/[，,、]/)
      .map((e) => e.trim())
      .filter((e) => e);
    result.executives = executives;
  }

  // 株主
  if (row["株主"]?.trim()) {
    const shareholders = row["株主"]
      .split(/[，,、]/)
      .map((s) => s.trim())
      .filter((s) => s);
    result.shareholders = shareholders;
  }

  // 社員数
  if (row["社員数"]?.trim()) {
    const employees = parseNumeric(row["社員数"]);
    if (employees !== null) {
      result.employeeCount = employees;
    }
  }

  // オフィス数
  if (row["オフィス数"]?.trim()) {
    const offices = parseNumeric(row["オフィス数"]);
    if (offices !== null) {
      result.officeCount = offices;
    }
  }

  // 工場数
  if (row["工場数"]?.trim()) {
    const factories = parseNumeric(row["工場数"]);
    if (factories !== null) {
      result.factoryCount = factories;
    }
  }

  // 店舗数
  if (row["店舗数"]?.trim()) {
    const stores = parseNumeric(row["店舗数"]);
    if (stores !== null) {
      result.storeCount = stores;
    }
  }

  // タイムスタンプ
  result.updatedAt = admin.firestore.FieldValue.serverTimestamp();
  result.createdAt = admin.firestore.FieldValue.serverTimestamp();

  return result;
}

// ドキュメントIDを数値で生成
function generateNumericDocId(rowIndex: number): string {
  const timestamp = Date.now();
  const paddedIndex = String(rowIndex).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 既存ドキュメントを検索（企業名 + 住所）
async function findExistingCompanyDoc(
  companyName: string | null,
  address: string | null
): Promise<DocumentReference<DocumentData> | null> {
  if (!companyName || !companyName.trim()) return null;

  // 企業名で検索
  const snapByName = await companiesCol
    .where("name", "==", companyName.trim())
    .limit(1)
    .get();

  if (snapByName.empty) return null;

  // 住所も確認
  if (address && address.trim()) {
    const doc = snapByName.docs[0];
    const data = doc.data();
    const docAddress = data.address || data.headquartersAddress || "";
    if (docAddress.trim() !== address.trim()) {
      return null; // 住所が一致しない
    }
  }

  return snapByName.docs[0].ref;
}

// メイン処理
async function main() {
  console.log("📄 108.csvをインポートします\n");

  if (!fs.existsSync(CSV_FILE)) {
    console.error(`❌ エラー: ${CSV_FILE} が見つかりません`);
    process.exit(1);
  }

  console.log(`📂 CSVファイル: ${CSV_FILE}\n`);

  const content = fs.readFileSync(CSV_FILE, "utf8");
  const records: Record<string, string>[] = parse(content, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_records_with_error: true,
  });

  if (records.length === 0) {
    console.log("❌ CSVに有効なレコードがありません");
    return;
  }

  const headers = Object.keys(records[0] || {});
  console.log(`📋 ヘッダー数: ${headers.length}`);
  console.log(`📋 レコード数: ${records.length}\n`);

  let createdCount = 0;
  let updatedCount = 0;
  let skippedCount = 0;

  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 400;

  for (let i = 0; i < records.length; i++) {
    const row = records[i];
    const rowNumber = i + 2; // ヘッダー行を考慮

    // 企業名がない場合はスキップ
    if (!row["会社名"]?.trim()) {
      skippedCount++;
      continue;
    }

    const mapped = mapRowToCompanyFields(row, headers);
    const companyName = mapped.name;
    const address = mapped.address || mapped.headquartersAddress;

    // 既存ドキュメントを検索
    const existingRef = await findExistingCompanyDoc(companyName, address);

    let targetRef: DocumentReference<DocumentData>;
    if (existingRef) {
      targetRef = existingRef;
      updatedCount++;
      if (updatedCount <= 5) {
        console.log(`🔄 [行${rowNumber}] 更新: ${companyName}`);
      }
    } else {
      const docId = generateNumericDocId(i);
      targetRef = companiesCol.doc(docId);
      createdCount++;
      if (createdCount <= 5) {
        console.log(`✨ [行${rowNumber}] 新規作成: ${companyName} (docId: ${docId})`);
      }
    }

    // 既存ドキュメントの場合は完全に置き換える（merge: false）
    // 新規作成の場合はそのまま作成
    if (existingRef) {
      batch.set(targetRef, mapped, { merge: false });
    } else {
      batch.set(targetRef, mapped, { merge: true });
    }
    batchCount++;

    if (batchCount >= BATCH_LIMIT) {
      await batch.commit();
      console.log(`  ✅ バッチコミット: ${BATCH_LIMIT}件 (累計: ${createdCount + updatedCount}件)`);
      batch = db.batch();
      batchCount = 0;
    }
  }

  // 残りのバッチをコミット
  if (batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 最終バッチコミット: ${batchCount}件`);
  }

  console.log("\n" + "=".repeat(80));
  console.log("✅ インポート完了");
  console.log(`   新規作成: ${createdCount}件`);
  console.log(`   更新: ${updatedCount}件`);
  console.log(`   スキップ: ${skippedCount}件`);
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});
