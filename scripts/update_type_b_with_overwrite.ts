/*
  タイプB専用: CSVから取り込むフィールドを全て上書きする更新スクリプト
  
  要件:
  1. CSVから取り込む情報は全て置き換える（既存データを上書き）
  2. 今回取り込まないフィールドはそのまま保持
  3. 同じ企業の情報が複数ある場合、CSV以外のフィールドを統合して一つのドキュメントに
  4. 企業の特定: 「企業名」＋「法人番号」「住所」「都道府県」のどれかがマッチしていれば統合
  5. 不要になった重複ドキュメントは削除
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/update_type_b_with_overwrite.ts [--dry-run]
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
const DRY_RUN = process.argv.includes("--dry-run");

// タイプBのCSVファイル一覧
const TYPE_B_FILES = [
  "csv/23.csv", "csv/78.csv", "csv/79.csv", "csv/80.csv", "csv/81.csv",
  "csv/82.csv", "csv/83.csv", "csv/84.csv", "csv/85.csv", "csv/86.csv",
  "csv/87.csv", "csv/88.csv", "csv/89.csv", "csv/90.csv", "csv/91.csv",
  "csv/92.csv", "csv/93.csv", "csv/94.csv", "csv/95.csv", "csv/96.csv",
  "csv/97.csv", "csv/98.csv", "csv/99.csv", "csv/100.csv", "csv/102.csv",
  "csv/105.csv"
];

// ==============================
// companies_new のフィールド一覧（テンプレート）
// ==============================
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
  dateOfEstablishment: null,
  demandProducts: null,
  departmentLocation: null,
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
  shareholders: null,
  storeCount: null,
  suppliers: [],
  tags: [],
  updateCount: null,
  updatedAt: null,
  urls: [],
  wantedly: null,
  youtrust: null,
};

// Firebase初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
      console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${defaultPath}`);
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId = serviceAccount.project_id;

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });

  console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
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

function normalizeStr(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "").replace(/株式会社|有限会社|合同会社|合名会社/g, "");
}

function normalizeAddress(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "");
}

function digitsOnly(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).replace(/\D/g, "");
}

function parseNumeric(v: string): number | null {
  const cleaned = v.replace(/[^\d.-]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

// ヘッダー行から業種の列インデックスを取得
function getIndustryColumnIndices(headerRow: Array<string>): { large?: number; middle?: number; small?: number; detail?: number } {
  const indices: { large?: number; middle?: number; small?: number; detail?: number } = {};
  
  for (let i = 0; i < headerRow.length; i++) {
    const header = String(headerRow[i] || "").trim();
    if (header === "業種（大）") {
      indices.large = i;
    } else if (header === "業種（中）") {
      indices.middle = i;
    } else if (header === "業種（小）") {
      indices.small = i;
    } else if (header === "業種（細）") {
      indices.detail = i;
    }
  }
  
  return indices;
}

// タイプBのCSV行をマッピング（列インデックスベース）
// 列順序: 0:会社名, 1:電話番号, 2:郵便番号(会社), 3:住所(会社), 4:URL, 5:代表者, 6:郵便番号(代表者), 7:住所(代表者), 8:創業, 9:(空), 10:設立, 11:株式保有率, 12:役員, 13:概要, 14-17:業種（順序が異なる可能性あり）
function mapTypeBRowByIndex(row: Array<string>, industryIndices?: { large?: number; middle?: number; small?: number; detail?: number }): Record<string, any> {
  const mapped: Record<string, any> = {};
  let colIndex = 0;

  // 0: 会社名
  if (row[colIndex]) mapped.name = trim(row[colIndex]);
  colIndex++;

  // 1: 電話番号
  if (row[colIndex]) mapped.phoneNumber = trim(row[colIndex]);
  colIndex++;

  // 2: 郵便番号（会社）
  if (row[colIndex]) {
    const postal = trim(row[colIndex]);
    if (postal) {
      const digits = digitsOnly(postal);
      if (digits.length === 7) {
        mapped.postalCode = digits.replace(/(\d{3})(\d{4})/, "$1-$2");
      }
    }
  }
  colIndex++;

  // 3: 住所（会社）
  if (row[colIndex]) mapped.address = trim(row[colIndex]);
  colIndex++;

  // 4: URL
  if (row[colIndex]) mapped.companyUrl = trim(row[colIndex]);
  colIndex++;

  // 5: 代表者
  if (row[colIndex]) mapped.representativeName = trim(row[colIndex]);
  colIndex++;

  // 6: 郵便番号（代表者）
  if (row[colIndex]) {
    const repPostal = trim(row[colIndex]);
    if (repPostal) {
      const digits = digitsOnly(repPostal);
      if (digits.length === 7) {
        mapped.representativeRegisteredAddress = digits.replace(/(\d{3})(\d{4})/, "$1-$2");
      }
    }
  }
  colIndex++;

      // 7: 住所（代表者）
      if (row[colIndex]) {
        const repAddress = trim(row[colIndex]);
        if (repAddress) {
          // 代表者の郵便番号と住所を結合して representativeRegisteredAddress に設定
          if (mapped.representativeRegisteredAddress) {
            // 郵便番号がある場合は、住所も representativeRegisteredAddress に結合
            mapped.representativeRegisteredAddress = `${mapped.representativeRegisteredAddress} ${repAddress}`;
          } else {
            // 郵便番号がない場合は representativeHomeAddress に設定
            mapped.representativeHomeAddress = repAddress;
          }
        }
      }
      colIndex++;

  // 8: 創業
  if (row[colIndex]) mapped.foundingYear = trim(row[colIndex]);
  colIndex++;

  // 9: (空列)
  colIndex++;

  // 10: 設立
  if (row[colIndex]) mapped.established = trim(row[colIndex]);
  colIndex++;

  // 11: 株式保有率
  if (row[colIndex]) mapped.shareholders = trim(row[colIndex]);
  colIndex++;

  // 12: 役員
  if (row[colIndex]) mapped.executives = trim(row[colIndex]);
  colIndex++;

  // 13: 概要
  if (row[colIndex]) mapped.overview = trim(row[colIndex]);
  colIndex++;

  // 14-17: 業種（ヘッダー行から取得した列インデックスを使用）
  const industryCategories: string[] = [];
  
  if (industryIndices) {
    if (industryIndices.large !== undefined && row[industryIndices.large]) {
      const value = trim(row[industryIndices.large]);
      if (value) {
        mapped.industryLarge = value;
        industryCategories.push(value);
      }
    }
    if (industryIndices.middle !== undefined && row[industryIndices.middle]) {
      const value = trim(row[industryIndices.middle]);
      if (value) {
        mapped.industryMiddle = value;
        industryCategories.push(value);
      }
    }
    if (industryIndices.small !== undefined && row[industryIndices.small]) {
      const value = trim(row[industryIndices.small]);
      if (value) {
        mapped.industrySmall = value;
        industryCategories.push(value);
      }
    }
    if (industryIndices.detail !== undefined && row[industryIndices.detail]) {
      const value = trim(row[industryIndices.detail]);
      if (value) {
        mapped.industryDetail = value;
        industryCategories.push(value);
      }
    }
  } else {
    // フォールバック: 列順序を仮定（14-17）
    for (let i = colIndex; i < Math.min(colIndex + 4, row.length); i++) {
      const value = trim(row[i]);
      if (value) {
        if (!mapped.industryLarge) {
          mapped.industryLarge = value;
          industryCategories.push(value);
        } else if (!mapped.industryMiddle) {
          mapped.industryMiddle = value;
          industryCategories.push(value);
        } else if (!mapped.industrySmall) {
          mapped.industrySmall = value;
          industryCategories.push(value);
        } else if (!mapped.industryDetail) {
          mapped.industryDetail = value;
          industryCategories.push(value);
        }
      }
    }
  }

  if (industryCategories.length > 0) {
    mapped.industryCategories = industryCategories;
  }

  // 都道府県を住所から抽出
  if (mapped.address) {
    const prefecture = extractPrefectureFromAddress(mapped.address);
    if (prefecture) mapped.prefecture = prefecture;
  }

  return mapped;
}

// タイプBのCSV行をマッピング（ヘッダーベース - フォールバック用）
function mapTypeBRow(row: Record<string, string>): Record<string, any> {
  const mapped: Record<string, any> = {};

  // 会社名
  const name = trim(row["会社名"]);
  if (name) mapped.name = name;

  // 電話番号
  const phone = trim(row["電話番号"]);
  if (phone) mapped.phoneNumber = phone;

  // 郵便番号（会社）- 最初のものを使用
  const postal = trim(row["郵便番号"]);
  if (postal) {
    const digits = digitsOnly(postal);
    if (digits.length === 7) {
      mapped.postalCode = digits.replace(/(\d{3})(\d{4})/, "$1-$2");
    }
  }

  // 住所（会社）- 最初のものを使用
  const address = trim(row["住所"]);
  if (address) mapped.address = address;

  // URL
  const url = trim(row["URL"]);
  if (url) mapped.companyUrl = url;

  // 代表者
  const repName = trim(row["代表者"]);
  if (repName) mapped.representativeName = repName;

  // 代表者の郵便番号と住所は列インデックスで取得する必要があるため、
  // ここでは取得できない（列名ベースでは最初の値しか取れない）

  // 創業
  const founding = trim(row["創業"]);
  if (founding) mapped.foundingYear = founding;

  // 設立
  const established = trim(row["設立"]);
  if (established) mapped.established = established;

  // 株式保有率
  const shareholders = trim(row["株式保有率"]);
  if (shareholders) mapped.shareholders = shareholders;

  // 役員
  const executives = trim(row["役員"]);
  if (executives) mapped.executives = executives;

  // 概要
  const overview = trim(row["概要"]);
  if (overview) mapped.overview = overview;

  // 業種（大）
  const industryLarge = trim(row["業種（大）"]);
  if (industryLarge) mapped.industryLarge = industryLarge;

  // 業種（中）
  const industryMiddle = trim(row["業種（中）"]);
  if (industryMiddle) mapped.industryMiddle = industryMiddle;

  // 業種（小）
  const industrySmall = trim(row["業種（小）"]);
  if (industrySmall) mapped.industrySmall = industrySmall;

  // 業種（細）
  const industryDetail = trim(row["業種（細）"]);
  if (industryDetail) mapped.industryDetail = industryDetail;

  // 業種カテゴリを配列に
  const industryCategories: string[] = [];
  if (industryLarge) industryCategories.push(industryLarge);
  if (industryMiddle) industryCategories.push(industryMiddle);
  if (industrySmall) industryCategories.push(industrySmall);
  if (industryDetail) industryCategories.push(industryDetail);
  if (industryCategories.length > 0) {
    mapped.industryCategories = industryCategories;
  }

  // 都道府県を住所から抽出
  if (mapped.address) {
    const prefecture = extractPrefectureFromAddress(mapped.address);
    if (prefecture) mapped.prefecture = prefecture;
  }

  return mapped;
}

// 住所から都道府県を抽出
function extractPrefectureFromAddress(address: string | null): string | null {
  if (!address) return null;
  
  const prefectures = [
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
  ];

  for (const pref of prefectures) {
    if (address.includes(pref)) {
      return pref;
    }
  }

  return null;
}

// 法人番号の検証
function validateCorporateNumber(value: string | null | undefined): string | null {
  if (!value) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  
  // 指数表記の場合はnull
  if (trimmed.includes("E") || trimmed.includes("e")) {
    return null;
  }
  
  const digits = digitsOnly(trimmed);
  if (digits.length !== 13) {
    return null;
  }
  
  return digits;
}

// 企業を特定: 企業名 + 法人番号/住所/都道府県のどれかがマッチ
async function findMatchingCompanies(
  name: string,
  corporateNumber: string | null,
  address: string | null,
  prefecture: string | null
): Promise<Array<{ ref: DocumentReference; data: any }>> {
  const matches: Array<{ ref: DocumentReference; data: any }> = [];
  const normName = normalizeStr(name);
  const normAddr = normalizeAddress(address);
  const normPref = normalizeStr(prefecture);
  const normCorpNum = corporateNumber ? validateCorporateNumber(corporateNumber) : null;

  // 法人番号で直接検索（あれば）
  if (normCorpNum) {
    const byId = await companiesCol.doc(normCorpNum).get();
    if (byId.exists) {
      const data = byId.data();
      if (data) {
        const docName = normalizeStr(data.name);
        if (docName === normName) {
          matches.push({ ref: byId.ref, data });
          return matches; // 法人番号で完全一致したらそれを返す
        }
      }
    }

    const corpSnap = await companiesCol
      .where("corporateNumber", "==", normCorpNum)
      .limit(100)
      .get();

    for (const doc of corpSnap.docs) {
      const data = doc.data();
      const docName = normalizeStr(data.name);
      if (docName === normName) {
        matches.push({ ref: doc.ref, data });
      }
    }

    if (matches.length > 0) {
      return matches;
    }
  }

  // 企業名で検索
  const snap = await companiesCol
    .where("name", "==", name)
    .limit(100)
    .get();

  if (snap.empty) {
    // prefix検索も試す
    const prefixSnap = await companiesCol
      .where("name", ">=", name)
      .where("name", "<=", name + "\uf8ff")
      .limit(100)
      .get();

    if (prefixSnap.empty) {
      return [];
    }

    for (const doc of prefixSnap.docs) {
      const data = doc.data();
      const docName = normalizeStr(data.name);
      
      // 企業名が一致
      if (docName === normName) {
        const docCorpNum = data.corporateNumber ? validateCorporateNumber(String(data.corporateNumber)) : null;
        const docAddr = normalizeAddress(data.address);
        const docPref = normalizeStr(data.prefecture);
        
        // 法人番号、住所、都道府県のどれかがマッチ
        let isMatch = false;
        if (normCorpNum && docCorpNum && normCorpNum === docCorpNum) {
          isMatch = true;
        } else if (normAddr && docAddr && (normAddr === docAddr || docAddr.includes(normAddr) || normAddr.includes(docAddr))) {
          isMatch = true;
        } else if (normPref && docPref && normPref === docPref) {
          isMatch = true;
        }
        
        if (isMatch) {
          matches.push({ ref: doc.ref, data });
        }
      }
    }
  } else {
    for (const doc of snap.docs) {
      const data = doc.data();
      const docCorpNum = data.corporateNumber ? validateCorporateNumber(String(data.corporateNumber)) : null;
      const docAddr = normalizeAddress(data.address);
      const docPref = normalizeStr(data.prefecture);
      
      // 法人番号、住所、都道府県のどれかがマッチ
      let isMatch = false;
      if (normCorpNum && docCorpNum && normCorpNum === docCorpNum) {
        isMatch = true;
      } else if (normAddr && docAddr && (normAddr === docAddr || docAddr.includes(normAddr) || normAddr.includes(docAddr))) {
        isMatch = true;
      } else if (normPref && docPref && normPref === docPref) {
        isMatch = true;
      }
      
      if (isMatch) {
        matches.push({ ref: doc.ref, data });
      }
    }
  }

  return matches;
}

// 複数のドキュメントを統合
function mergeDocuments(
  documents: Array<{ ref: DocumentReference; data: any }>,
  csvData: Record<string, any>
): Record<string, any> {
  const merged: Record<string, any> = { ...COMPANY_TEMPLATE };
  
  // CSVから取り込むフィールドを全て上書き
  for (const [field, value] of Object.entries(csvData)) {
    if (field in COMPANY_TEMPLATE) {
      merged[field] = value;
    }
  }
  
  // CSV以外のフィールドを統合（最初の非null値を使用）
  for (const doc of documents) {
    const data = doc.data;
    for (const [field, value] of Object.entries(data)) {
      // CSVで上書きするフィールドはスキップ
      if (field in csvData) continue;
      
      // 既に値がある場合はスキップ
      if (merged[field] !== null && merged[field] !== undefined && merged[field] !== "") {
        continue;
      }
      
      // null/空でない値があれば設定
      if (value !== null && value !== undefined && value !== "") {
        if (Array.isArray(value) && value.length > 0) {
          merged[field] = value;
        } else if (!Array.isArray(value)) {
          merged[field] = value;
        }
      }
    }
  }
  
  return merged;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード: Firestore は書き換えません\n" : "⚠️  本番モード: Firestore を書き換えます\n");

  let totalRows = 0;
  let updatedCount = 0;
  let createdCount = 0;
  let mergedCount = 0;
  let deletedCount = 0;

  // Firestoreのバッチ制限: 最大500オペレーション、リクエストサイズ10MB
  // データサイズが大きいため、バッチサイズを小さくする
  const BATCH_SIZE = 50; // オペレーション数
  const MAX_BATCH_SIZE_BYTES = 8000000; // 8MB（安全マージン）
  let batch: WriteBatch | null = null;
  let batchCount = 0;
  let batchSizeBytes = 0;

  // バッチをコミットする関数
  async function commitBatchIfNeeded(force: boolean = false) {
    if (!batch || batchCount === 0) return;
    
    const shouldCommit = force || batchCount >= BATCH_SIZE || batchSizeBytes >= MAX_BATCH_SIZE_BYTES;
    
    if (shouldCommit) {
      if (!DRY_RUN) {
        await batch.commit();
      }
      console.log(`  ✅ バッチコミット: ${batchCount} オペレーション, ${Math.round(batchSizeBytes / 1024)}KB`);
      batch = null;
      batchCount = 0;
      batchSizeBytes = 0;
    }
  }

  for (const file of TYPE_B_FILES) {
    const filePath = path.resolve(file);
    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️  ファイルが見つかりません: ${file}`);
      continue;
    }

    console.log(`\n📥 CSV 読み込み開始: ${file}`);

    const buf = fs.readFileSync(filePath);
    let records: Array<Array<string>>;
    
    try {
      // 列インデックスベースで読み込み（ヘッダー行をスキップ）
      records = parse(buf, {
        columns: false,  // ヘッダーを無視して配列として読み込む
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_records_with_error: true,
      });
    } catch (err: any) {
      console.error(`❌ CSVパースエラー: ${err.message}`);
      continue;
    }

    if (records.length <= 1) {
      console.warn(`  ⚠️  データ行がありません`);
      continue;
    }

    // ヘッダー行から業種の列インデックスを取得
    const headerRow = records[0];
    const industryIndices = getIndustryColumnIndices(headerRow);

    totalRows += records.length - 1; // ヘッダー行を除く
    console.log(`  📊 ${records.length - 1} 行を読み込み（ヘッダー除く）`);

    // ヘッダー行をスキップして処理
    for (let idx = 1; idx < records.length; idx++) {
      const row = records[idx];
      
      // 進捗表示（100行ごと）
      if ((idx) % 100 === 0) {
        console.log(`  📊 処理中: ${idx}/${records.length - 1} 行`);
      }
      
      // CSV行をマッピング（列インデックスベース）
      const csvData = mapTypeBRowByIndex(row, industryIndices);
      
      if (!csvData.name) {
        if (idx < 10) {
          console.warn(`  ⚠️  [行 ${idx + 1}] 会社名がありません`);
        }
        continue;
      }

      // 企業を特定
      const matches = await findMatchingCompanies(
        csvData.name,
        csvData.corporateNumber || null,
        csvData.address || null,
        csvData.prefecture || null
      );

      if (matches.length === 0) {
        // 新規作成
        if (!batch) {
          batch = db.batch();
        }

        const newData: Record<string, any> = {
          ...COMPANY_TEMPLATE,
          ...csvData,
          csvType: "type_b",
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        };

        // 法人番号を検証して設定
        if (csvData.corporateNumber) {
          const validCorpNum = validateCorporateNumber(String(csvData.corporateNumber));
          if (validCorpNum) {
            newData.corporateNumber = validCorpNum;
          } else {
            newData.corporateNumber = null;
          }
        }

        // ドキュメントIDを生成（法人番号があれば使用、なければタイムスタンプ）
        let docId: string;
        const validCorpNum = newData.corporateNumber;
        if (validCorpNum) {
          docId = validCorpNum;
        } else {
          docId = `${Date.now()}${idx}`;
        }

        const newRef = companiesCol.doc(docId);
        batch.set(newRef, newData);
        batchCount++;
        createdCount++;
        
        // データサイズを推定（JSON文字列化で概算）
        const estimatedSize = JSON.stringify(newData).length;
        batchSizeBytes += estimatedSize;

        await commitBatchIfNeeded();
      } else if (matches.length === 1) {
        // 単一マッチ: 更新
        if (!batch) {
          batch = db.batch();
        }

        const { ref, data: current } = matches[0];
        const updateData: Record<string, any> = {};

        // CSVから取り込むフィールドを全て上書き
        for (const [field, csvValue] of Object.entries(csvData)) {
          if (field in COMPANY_TEMPLATE) {
            // 法人番号は検証
            if (field === "corporateNumber" && csvValue) {
              const validCorpNum = validateCorporateNumber(String(csvValue));
              updateData[field] = validCorpNum || null;
            } else {
              updateData[field] = csvValue;
            }
          }
        }

        updateData.csvType = "type_b";
        updateData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

        batch.update(ref, updateData);
        batchCount++;
        updatedCount++;
        
        // データサイズを推定（JSON文字列化で概算）
        const estimatedSize = JSON.stringify(updateData).length;
        batchSizeBytes += estimatedSize;

        await commitBatchIfNeeded();
      } else {
        // 複数マッチ: 統合
        if (!batch) {
          batch = db.batch();
        }

        // 最初のドキュメントに統合
        const primaryDoc = matches[0];
        const mergedData = mergeDocuments(matches, csvData);
        
        // 法人番号を検証
        if (mergedData.corporateNumber) {
          const validCorpNum = validateCorporateNumber(String(mergedData.corporateNumber));
          mergedData.corporateNumber = validCorpNum || null;
        }
        
        mergedData.csvType = "type_b";
        mergedData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

        batch.set(primaryDoc.ref, mergedData, { merge: false });
        batchCount++;
        mergedCount++;
        
        // データサイズを推定（JSON文字列化で概算）
        const estimatedSize = JSON.stringify(mergedData).length;
        batchSizeBytes += estimatedSize;

        // 残りのドキュメントを削除
        for (let i = 1; i < matches.length; i++) {
          if (!batch) {
            batch = db.batch();
          }
          batch.delete(matches[i].ref);
          batchCount++;
          deletedCount++;
          
          // 削除操作のサイズは小さいが、カウントに含める
          batchSizeBytes += 100; // 削除操作は約100バイトと仮定

          await commitBatchIfNeeded();
        }

        await commitBatchIfNeeded();
      }
    }
  }

  // 残りのバッチをコミット
  await commitBatchIfNeeded(true);

  console.log("\n✅ 処理完了");
  console.log(`  📊 CSV 総行数: ${totalRows}`);
  console.log(`  ✨ 更新件数: ${updatedCount}`);
  console.log(`  🆕 新規作成件数: ${createdCount}`);
  console.log(`  🔗 統合件数: ${mergedCount}`);
  console.log(`  🗑️  削除件数: ${deletedCount}`);

  if (DRY_RUN) {
    console.log("\n💡 実際に Firestore を更新するには、--dry-run フラグを外して実行してください。");
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

