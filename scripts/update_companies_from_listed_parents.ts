/* 
  listed_parents コレクションの情報で companies_new コレクションを補完・更新するスクリプト

  目的:
    - listed_parents には上場会社の企業情報が入っています
    - companies_new 側で不足している上場関連情報や基本情報を、listed_parents から安全に補完します
    - 特に listing フィールドの整備を最優先で行い、listing が null の企業を非上場として一括補完できる状態にします

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/update_companies_from_listed_parents.ts [--dry-run]

  ※ サービスアカウントキーを第1引数に渡す場合:
    npx ts-node scripts/update_companies_from_listed_parents.ts serviceAccountKey.json [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";

const COMPANIES_COLLECTION = "companies_new";
const LISTED_PARENTS_COLLECTION = "listed_parents";
const DRY_RUN = process.argv.includes("--dry-run");

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
      projectId: projectId,
    });

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
  } catch (err: any) {
    console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
    console.error(`   詳細: ${err.message}`);
    process.exit(1);
  }
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COMPANIES_COLLECTION);
const listedParentsCol: CollectionReference = db.collection(LISTED_PARENTS_COLLECTION);

// ==============================
// ヘルパー関数
// ==============================

function trim(v?: string | null): string | null {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

// 企業名の正規化（空白除去、表記ゆれの統一）
function normalizeCompanyName(name: string | null | undefined): string {
  if (!name) return "";
  let normalized = String(name).trim();
  
  // 「（株）」を「株式会社」に変換
  normalized = normalized.replace(/（株）/g, "株式会社");
  normalized = normalized.replace(/\(株\)/g, "株式会社");
  normalized = normalized.replace(/カブシキガイシャ/g, "株式会社");
  
  // 空白を除去
  normalized = normalized.replace(/\s+/g, "");
  
  return normalized;
}

// 住所の正規化（全角/半角、ハイフン、丁目/番地表現の揺れを統一）
function normalizeAddress(addr: string | null | undefined): string {
  if (!addr) return "";
  let normalized = String(addr).trim();
  
  // 全角数字を半角に変換
  normalized = normalized.replace(/[０-９]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) - 0xFEE0);
  });
  
  // 全角英字を半角に変換
  normalized = normalized.replace(/[Ａ-Ｚａ-ｚ]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) - 0xFEE0);
  });
  
  // 全角スペースを半角に変換
  normalized = normalized.replace(/　/g, " ");
  
  // ハイフンの統一（全角ハイフン、長音符などを半角ハイフンに）
  normalized = normalized.replace(/[－−ー―]/g, "-");
  
  // 丁目/番地の表記ゆれを統一
  normalized = normalized.replace(/(\d+)丁目/g, "$1丁目");
  normalized = normalized.replace(/(\d+)番地/g, "$1番地");
  normalized = normalized.replace(/(\d+)番/g, "$1番");
  
  // 都道府県の表記ゆれ（末尾の都道府県を削除して比較しやすくする）
  // ただし、完全一致判定のため、ここでは都道府県は保持
  
  // 空白を除去
  normalized = normalized.replace(/\s+/g, "");
  
  return normalized;
}

// 住所の部分一致判定（都道府県を除いた部分で比較）
function isAddressPartiallyMatch(addr1: string, addr2: string): boolean {
  const norm1 = normalizeAddress(addr1);
  const norm2 = normalizeAddress(addr2);
  
  if (norm1 === norm2) return true;
  
  // 都道府県を除いた部分で比較
  const prefecturePattern = /^(北海道|青森県|岩手県|宮城県|秋田県|山形県|福島県|茨城県|栃木県|群馬県|埼玉県|千葉県|東京都|神奈川県|新潟県|富山県|石川県|福井県|山梨県|長野県|岐阜県|静岡県|愛知県|三重県|滋賀県|京都府|大阪府|兵庫県|奈良県|和歌山県|鳥取県|島根県|岡山県|広島県|山口県|徳島県|香川県|愛媛県|高知県|福岡県|佐賀県|長崎県|熊本県|大分県|宮崎県|鹿児島県|沖縄県)/;
  
  const addr1WithoutPref = norm1.replace(prefecturePattern, "");
  const addr2WithoutPref = norm2.replace(prefecturePattern, "");
  
  // どちらかがもう一方を含む場合、部分一致とみなす
  if (addr1WithoutPref && addr2WithoutPref) {
    if (addr1WithoutPref.includes(addr2WithoutPref) || addr2WithoutPref.includes(addr1WithoutPref)) {
      return true;
    }
  }
  
  return false;
}

// 企業名の高精度一致判定
function isCompanyNameHighPrecisionMatch(name1: string, name2: string): boolean {
  const norm1 = normalizeCompanyName(name1);
  const norm2 = normalizeCompanyName(name2);
  
  if (norm1 === norm2) return true;
  
  // 「株式会社」を除いた部分で比較
  const withoutKabushiki = (name: string) => name.replace(/^株式会社/, "").replace(/株式会社$/, "");
  const name1Core = withoutKabushiki(norm1);
  const name2Core = withoutKabushiki(norm2);
  
  if (name1Core === name2Core && name1Core.length > 0) return true;
  
  // 一方がもう一方を含む場合（短い方の長さが長い方の80%以上の場合）
  if (name1Core && name2Core) {
    const shorter = name1Core.length < name2Core.length ? name1Core : name2Core;
    const longer = name1Core.length >= name2Core.length ? name1Core : name2Core;
    if (longer.includes(shorter) && shorter.length >= longer.length * 0.8) {
      return true;
    }
  }
  
  return false;
}

// ドキュメントIDを数字のみの文字列に統一する
function generateNumericDocId(
  corporateNumber: string | null,
  index: number
): string {
  // corporateNumberが存在し、数字のみの場合 → そのまま使用
  if (corporateNumber && /^[0-9]+$/.test(corporateNumber.trim())) {
    return corporateNumber.trim();
  }

  // それ以外の場合 → Date.now() + インデックスから数字のみの一意IDを生成
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// companies_new のテンプレート（headquartersAddress フィールドは除外）
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
  profit1: null,
  profit2: null,
  profit3: null,
  profit4: null,
  profit5: null,
  linkedin: null,
  listing: null,
  location: null,
  marketSegment: null,
  netAssets: null,
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
  tradingStatus: null,
  transportation: null,
  updateCount: null,
  updateDate: null,
  updatedAt: null,
  urls: [],
  wantedly: null,
  youtrust: null,
};

// listed_parents のフィールドを companies_new のフィールドにマッピング
function mapListedParentToCompany(listedParent: DocumentData): Record<string, any> {
  const mapped: Record<string, any> = {};
  
  // address → address
  if (listedParent.address) {
    mapped.address = trim(listedParent.address);
  }
  
  // corporateNumber → corporateNumber
  if (listedParent.corporateNumber) {
    mapped.corporateNumber = trim(String(listedParent.corporateNumber));
  }
  
  // industry → industry
  if (listedParent.industry) {
    mapped.industry = trim(listedParent.industry);
  }
  
  // submitterName → name
  if (listedParent.submitterName) {
    mapped.name = trim(listedParent.submitterName);
  }
  
  // capital → capitalStock
  if (listedParent.capital !== undefined && listedParent.capital !== null) {
    const capital = listedParent.capital;
    if (typeof capital === "number") {
      mapped.capitalStock = capital;
    } else if (typeof capital === "string") {
      const num = parseFloat(capital.replace(/[,，]/g, ""));
      if (!isNaN(num)) {
        mapped.capitalStock = num;
      }
    }
  }
  
  // fiscalMonth → fiscalMonth
  if (listedParent.fiscalMonth) {
    mapped.fiscalMonth = trim(listedParent.fiscalMonth);
  }
  
  // listed → listing（「上場」に変換）
  if (listedParent.listed !== undefined && listedParent.listed !== null) {
    // listed が true または "上場" などの文字列の場合、「上場」をセット
    if (listedParent.listed === true || String(listedParent.listed).includes("上場")) {
      mapped.listing = "上場";
    }
  }
  
  // submitterNameEn → nameEn
  if (listedParent.submitterNameEn) {
    mapped.nameEn = trim(listedParent.submitterNameEn);
  }
  
  // submitterNameKana → kana
  if (listedParent.submitterNameKana) {
    mapped.kana = trim(listedParent.submitterNameKana);
  }
  
  return mapped;
}

// companies_new で企業を特定（優先順位: corporateNumber → 企業名+住所 → 企業名高精度一致+住所部分一致）
async function findCompanyInCompaniesNew(
  listedParent: DocumentData
): Promise<{ ref: DocumentReference; matchedBy: string } | null> {
  const corporateNumber = listedParent.corporateNumber
    ? trim(String(listedParent.corporateNumber))
    : null;
  const name = listedParent.submitterName ? trim(listedParent.submitterName) : null;
  const address = listedParent.address ? trim(listedParent.address) : null;
  
  // 1. corporateNumber の一致（最優先・完全一致）
  if (corporateNumber) {
    // docId = corporateNumber で直接参照
    const directRef = companiesCol.doc(corporateNumber);
    const directSnap = await directRef.get();
    if (directSnap.exists) {
      return { ref: directRef, matchedBy: "corporateNumber" };
    }
    
    // corporateNumber フィールドで検索
    const snapByCorp = await companiesCol
      .where("corporateNumber", "==", corporateNumber)
      .limit(1)
      .get();
    if (!snapByCorp.empty) {
      return { ref: snapByCorp.docs[0].ref, matchedBy: "corporateNumber" };
    }
  }
  
  // 2. 企業名 + 住所の一致
  if (name && address) {
    const nameNorm = normalizeCompanyName(name);
    const addrNorm = normalizeAddress(address);
    
    // name と address の完全一致で検索
    const snapByNameAndAddr = await companiesCol
      .where("name", "==", name)
      .where("address", "==", address)
      .limit(1)
      .get();
    if (!snapByNameAndAddr.empty) {
      return { ref: snapByNameAndAddr.docs[0].ref, matchedBy: "nameAndAddress" };
    }
    
    // 正規化後の値で再試行（name フィールドで完全一致、address は正規化して比較）
    const snapByName = await companiesCol
      .where("name", "==", name)
      .limit(100) // 同名企業が複数ある可能性があるため、少し多めに取得
      .get();
    
    for (const doc of snapByName.docs) {
      const docData = doc.data();
      const docAddr = docData.address ? normalizeAddress(docData.address) : "";
      if (docAddr === addrNorm) {
        return { ref: doc.ref, matchedBy: "nameAndAddress" };
      }
    }
  }
  
  // 3. 企業名の高精度一致 + 住所の部分一致
  if (name) {
    const nameNorm = normalizeCompanyName(name);
    const addrNorm = address ? normalizeAddress(address) : "";
    
    // name フィールドで検索（複数候補を取得）
    const snapByName = await companiesCol
      .where("name", "==", name)
      .limit(100)
      .get();
    
    for (const doc of snapByName.docs) {
      const docData = doc.data();
      const docName = docData.name ? normalizeCompanyName(docData.name) : "";
      const docAddr = docData.address ? normalizeAddress(docData.address) : "";
      
      // 企業名の高精度一致チェック
      if (isCompanyNameHighPrecisionMatch(nameNorm, docName)) {
        // 住所が提供されている場合、部分一致をチェック
        if (!addrNorm || !docAddr || isAddressPartiallyMatch(addrNorm, docAddr)) {
          return { ref: doc.ref, matchedBy: "nameHighPrecisionAndAddressPartial" };
        }
      }
    }
    
    // 注: 企業名の高精度一致検索は、上記の企業名完全一致検索の結果内で
    // 正規化後の高精度一致をチェックすることで対応しています。
    // 全件取得は効率が悪いため、ここでは行いません。
  }
  
  return null;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  if (DRY_RUN) {
    console.log("🔍 DRY_RUN モード: Firestore は書き換えません\n");
  } else {
    console.log("⚠️  本番モード: Firestore を書き換えます\n");
  }

  console.log("📊 listed_parents コレクションを取得中...");
  
  // listed_parents の全ドキュメントを取得
  const listedParentsSnapshot = await listedParentsCol.get();
  const listedParentsDocs = listedParentsSnapshot.docs;
  const totalListedParents = listedParentsDocs.length;
  
  console.log(`✅ listed_parents 取得完了: ${totalListedParents} 件\n`);
  
  // 統計情報
  const stats = {
    totalListedParents: totalListedParents,
    matchedByCorporateNumber: 0,
    matchedByNameAndAddress: 0,
    matchedByNameHighPrecision: 0,
    notMatched: 0,
    created: 0,
    listingSetToListed: 0,
    listingSetToUnlisted: 0,
    fieldsUpdated: {
      address: 0,
      corporateNumber: 0,
      industry: 0,
      name: 0,
      capitalStock: 0,
      fiscalMonth: 0,
      listing: 0,
      nameEn: 0,
      kana: 0,
    },
  };
  
  // バッチ処理用（非上場の一括補完は大量データのため、より小さいバッチサイズを使用）
  const BATCH_LIMIT = 500;
  const UNLISTED_BATCH_LIMIT = 100; // 非上場の一括補完用（トランザクションサイズ制限対策）
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  
  console.log("🔄 companies_new を更新中...\n");
  
  // listed_parents の各ドキュメントを処理
  for (let i = 0; i < listedParentsDocs.length; i++) {
    const listedParentDoc = listedParentsDocs[i];
    const listedParentData = listedParentDoc.data();
    
    if (i % 100 === 0) {
      console.log(`📝 処理中: ${i + 1}/${totalListedParents}`);
    }
    
    // companies_new で企業を特定
    const matchResult = await findCompanyInCompaniesNew(listedParentData);
    
    if (!matchResult) {
      // 企業が見つからない場合は新規作成
      const mappedData = mapListedParentToCompany(listedParentData);
      
      // COMPANY_TEMPLATE をベースに新規ドキュメントを作成
      const newCompanyData: Record<string, any> = { ...COMPANY_TEMPLATE };
      
      // mappedData の値をマージ
      for (const [field, value] of Object.entries(mappedData)) {
        if (value !== null && value !== undefined) {
          newCompanyData[field] = value;
          if (field in stats.fieldsUpdated) {
            stats.fieldsUpdated[field as keyof typeof stats.fieldsUpdated]++;
          }
        }
      }
      
      // listing が設定されていない場合は「上場」をセット
      if (!newCompanyData.listing) {
        newCompanyData.listing = "上場";
        stats.fieldsUpdated.listing++;
        stats.listingSetToListed++;
      }
      
      // ドキュメントIDを生成
      const corporateNumber = listedParentData.corporateNumber
        ? trim(String(listedParentData.corporateNumber))
        : null;
      const docId = generateNumericDocId(corporateNumber, i);
      const newRef = companiesCol.doc(docId);
      
      if (DRY_RUN) {
        if (stats.created < 10) {
          const name = listedParentData.submitterName || listedParentData.name || "不明";
          console.log(`  🆕 (DRY_RUN) 新規作成予定 docId="${docId}" 企業名: ${name}`);
        }
        stats.created++;
      } else {
        batch.set(newRef, newCompanyData);
        batchCount++;
        stats.created++;
        
        if (batchCount >= BATCH_LIMIT) {
          await batch.commit();
          batch = db.batch();
          batchCount = 0;
          console.log(`  ✅ バッチコミット完了 (${i + 1}/${totalListedParents})`);
        }
      }
      
      continue;
    }
    
    // マッチング方法の統計
    if (matchResult.matchedBy === "corporateNumber") {
      stats.matchedByCorporateNumber++;
    } else if (matchResult.matchedBy === "nameAndAddress") {
      stats.matchedByNameAndAddress++;
    } else {
      stats.matchedByNameHighPrecision++;
    }
    
    // 既存データを取得
    const companySnap = await matchResult.ref.get();
    if (!companySnap.exists) {
      stats.notMatched++;
      continue;
    }
    
    const currentData = companySnap.data() || {};
    const mappedData = mapListedParentToCompany(listedParentData);
    
    // 更新データを構築（既存値がある場合は上書きしない、ただし listing は特別扱い）
    const updateData: Record<string, any> = {};
    
    for (const [field, newValue] of Object.entries(mappedData)) {
      if (newValue === null || newValue === undefined) continue;
      
      const currentValue = currentData[field];
      
      // listing の特別処理
      if (field === "listing") {
        // listing が null/空 の場合のみ「上場」をセット
        if (currentValue === null || currentValue === undefined || currentValue === "") {
          updateData[field] = "上場";
          stats.fieldsUpdated.listing++;
          stats.listingSetToListed++;
        }
        // 既存値がある場合は上書きしない
      } else {
        // その他のフィールドは、既存値が null/空 の場合のみ更新
        if (currentValue === null || currentValue === undefined || currentValue === "") {
          updateData[field] = newValue;
          if (field in stats.fieldsUpdated) {
            stats.fieldsUpdated[field as keyof typeof stats.fieldsUpdated]++;
          }
        }
      }
    }
    
    // 更新データがある場合のみバッチに追加
    if (Object.keys(updateData).length > 0) {
      if (DRY_RUN) {
        if (batchCount < 10) {
          console.log(`  📝 (DRY_RUN) docId="${matchResult.ref.id}" 更新予定:`, updateData);
        }
      } else {
        batch.update(matchResult.ref, updateData);
        batchCount++;
        
        if (batchCount >= BATCH_LIMIT) {
          await batch.commit();
          batch = db.batch();
          batchCount = 0;
          console.log(`  ✅ バッチコミット完了 (${i + 1}/${totalListedParents})`);
        }
      }
    }
  }
  
  // 残りのバッチをコミット
  if (batchCount > 0 && !DRY_RUN) {
    await batch.commit();
    console.log(`  ✅ 最終バッチコミット完了`);
  }
  
  console.log("\n✅ 上場企業の反映完了");
  console.log(`\n📊 統計情報:`);
  console.log(`  📋 listed_parents 総件数: ${stats.totalListedParents}`);
  console.log(`  ✅ corporateNumber で特定: ${stats.matchedByCorporateNumber}`);
  console.log(`  ✅ 企業名+住所で特定: ${stats.matchedByNameAndAddress}`);
  console.log(`  ✅ 企業名高精度+住所部分一致で特定: ${stats.matchedByNameHighPrecision}`);
  console.log(`  🆕 新規作成: ${stats.created}`);
  console.log(`  📝 listing を「上場」に設定: ${stats.listingSetToListed}`);
  console.log(`\n📝 フィールド別更新件数:`);
  console.log(`  - address: ${stats.fieldsUpdated.address}`);
  console.log(`  - corporateNumber: ${stats.fieldsUpdated.corporateNumber}`);
  console.log(`  - industry: ${stats.fieldsUpdated.industry}`);
  console.log(`  - name: ${stats.fieldsUpdated.name}`);
  console.log(`  - capitalStock: ${stats.fieldsUpdated.capitalStock}`);
  console.log(`  - fiscalMonth: ${stats.fieldsUpdated.fiscalMonth}`);
  console.log(`  - listing: ${stats.fieldsUpdated.listing}`);
  console.log(`  - nameEn: ${stats.fieldsUpdated.nameEn}`);
  console.log(`  - kana: ${stats.fieldsUpdated.kana}`);
  
  // 非上場の一括補完
  console.log("\n🔄 listing が null の企業に「非上場」を設定中...");
  
  let unlistedBatch: WriteBatch = db.batch();
  let unlistedBatchCount = 0;
  let unlistedUpdatedCount = 0;
  
  // companies_new の全ドキュメントを取得（listing が null のもの）
  let lastDoc: any = null;
  const FETCH_BATCH_SIZE = 1000;
  
  while (true) {
    let query = companiesCol
      .where("listing", "==", null)
      .limit(FETCH_BATCH_SIZE);
    
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }
    
    const snapshot = await query.get();
    
    if (snapshot.empty) {
      break;
    }
    
    for (const doc of snapshot.docs) {
      if (DRY_RUN) {
        if (unlistedUpdatedCount < 10) {
          console.log(`  📝 (DRY_RUN) docId="${doc.id}" に「非上場」を設定予定`);
        }
        unlistedUpdatedCount++;
      } else {
        unlistedBatch.update(doc.ref, { listing: "非上場" });
        unlistedBatchCount++;
        unlistedUpdatedCount++;
        
        if (unlistedBatchCount >= UNLISTED_BATCH_LIMIT) {
          await unlistedBatch.commit();
          unlistedBatch = db.batch();
          unlistedBatchCount = 0;
          if (unlistedUpdatedCount % 10000 === 0 || unlistedUpdatedCount < 10000) {
            console.log(`  ✅ 非上場バッチコミット完了 (${unlistedUpdatedCount} 件)`);
          }
        }
      }
    }
    
    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }
  
  // 残りのバッチをコミット
  if (unlistedBatchCount > 0 && !DRY_RUN) {
    await unlistedBatch.commit();
    console.log(`  ✅ 最終非上場バッチコミット完了`);
  }
  
  stats.listingSetToUnlisted = unlistedUpdatedCount;
  
  console.log(`\n✅ 非上場の一括補完完了`);
  console.log(`  📝 listing を「非上場」に設定: ${stats.listingSetToUnlisted} 件`);
  
  console.log("\n✅ 全処理完了");
  console.log(`\n📊 最終統計:`);
  console.log(`  📋 listed_parents 総件数: ${stats.totalListedParents}`);
  console.log(`  ✅ companies_new で特定できた件数: ${stats.matchedByCorporateNumber + stats.matchedByNameAndAddress + stats.matchedByNameHighPrecision}`);
  console.log(`  🆕 新規作成: ${stats.created}`);
  console.log(`  📝 listing を「上場」に設定: ${stats.listingSetToListed}`);
  console.log(`  📝 listing を「非上場」に設定: ${stats.listingSetToUnlisted}`);
  console.log(`\n📝 フィールド別更新件数:`);
  console.log(`  - address: ${stats.fieldsUpdated.address}`);
  console.log(`  - corporateNumber: ${stats.fieldsUpdated.corporateNumber}`);
  console.log(`  - industry: ${stats.fieldsUpdated.industry}`);
  console.log(`  - name: ${stats.fieldsUpdated.name}`);
  console.log(`  - capitalStock: ${stats.fieldsUpdated.capitalStock}`);
  console.log(`  - fiscalMonth: ${stats.fieldsUpdated.fiscalMonth}`);
  console.log(`  - listing: ${stats.fieldsUpdated.listing}`);
  console.log(`  - nameEn: ${stats.fieldsUpdated.nameEn}`);
  console.log(`  - kana: ${stats.fieldsUpdated.kana}`);
  
  if (DRY_RUN) {
    console.log(
      "\n💡 実際に Firestore を更新するには、--dry-run フラグを外して実行してください。"
    );
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

