/* 
  証券コードの追加スクリプト

  対象: listing フィールドが「上場」の企業
  処理:
    1. shokenCode/shokenCode.csv から証券コードと銘柄名を読み込む
    2. companies_new コレクションから listing="上場" の企業を取得
    3. 企業名（name）と銘柄名を照合して証券コードを追加
    4. 照合できない場合は新規作成（既存のフィールド構造に合わせて）

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/add_securities_code.ts [--dry-run]
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
import type { DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const SHOKEN_CODE_CSV_PATH = path.join(__dirname, "../shokenCode/shokenCode.csv");

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
  profit1: null,
  profit2: null,
  profit3: null,
  profit4: null,
  profit5: null,
  linkedin: null,
  listing: "上場",
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
  securitiesCode: null, // 証券コードフィールドを追加
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

// 企業名を正規化（法人格除去、括弧除去、空白除去など）
function normalizeCompanyNameForMatching(name: string | null | undefined): string | null {
  if (!name) return null;
  let normalized = String(name).trim();
  if (!normalized) return null;
  
  // 括弧内の内容を除去（銘柄名の「(トヨタ)」「（トヨタ）」など）
  // 全角括弧と半角括弧の両方に対応
  normalized = normalized.replace(/[（(].*?[）)]/g, "");
  
  // 法人格を除去（前後どちらでも、長いものから順に処理）
  const corporateTypes = [
    "特定非営利活動法人",
    "一般財団法人", "一般社団法人",
    "公益財団法人", "公益社団法人",
    "株式会社", "有限会社", "合同会社", "合資会社", "合名会社",
    "協同組合", "協業組合", "社会福祉法人",
    "医療法人", "学校法人", "宗教法人", "NPO法人",
    "（株）", "(株)", "㈱",
    "（有）", "(有)", "㈲",
    "（合）", "(合)",
    "（資）", "(資)",
    "（名）", "(名)",
  ];
  
  // 長い法人格から順に除去（短いものが長いものに含まれる場合を防ぐ）
  for (const type of corporateTypes) {
    // 前株: 「株式会社○○」→「○○」
    if (normalized.startsWith(type)) {
      normalized = normalized.substring(type.length);
    }
    // 後株: 「○○株式会社」→「○○」
    if (normalized.endsWith(type)) {
      normalized = normalized.substring(0, normalized.length - type.length);
    }
  }
  
  // 全角スペースを半角スペースに変換
  normalized = normalized.replace(/　/g, " ");
  // 連続するスペースや空白を全て除去
  normalized = normalized.replace(/\s+/g, "");
  // 記号を除去
  normalized = normalized.replace(/[・、。，．]/g, "");
  // 前後のスペースを除去
  normalized = normalized.trim();
  
  return normalized || null;
}

// 企業名の照合（正規化後の一致）
function matchCompanyName(name1: string | null, name2: string | null): boolean {
  if (!name1 || !name2) return false;
  
  const normalized1 = normalizeCompanyNameForMatching(name1);
  const normalized2 = normalizeCompanyNameForMatching(name2);
  
  if (!normalized1 || !normalized2) return false;
  
  // 正規化後の完全一致
  if (normalized1 === normalized2) return true;
  
  // 大文字小文字を無視した一致
  if (normalized1.toLowerCase() === normalized2.toLowerCase()) return true;
  
  // 部分一致も試す（一方が他方を含む場合）
  // 例: 「トヨタ自動車」と「トヨタ」は一致とみなす
  if (normalized1.length >= 3 && normalized2.length >= 3) {
    if (normalized1.includes(normalized2) || normalized2.includes(normalized1)) {
      // ただし、短い方の長さが長い方の70%以上である必要がある（誤検出を防ぐ）
      const minLen = Math.min(normalized1.length, normalized2.length);
      const maxLen = Math.max(normalized1.length, normalized2.length);
      if (minLen / maxLen >= 0.7) {
        return true;
      }
    }
  }
  
  return false;
}

// 証券コードCSVを読み込む
function loadSecuritiesCodeCsv(filePath: string): Map<string, string> {
  // 銘柄名 -> 証券コード のマップ
  const map = new Map<string, string>();
  
  if (!fs.existsSync(filePath)) {
    console.error(`❌ エラー: 証券コードCSVファイルが見つかりません: ${filePath}`);
    process.exit(1);
  }
  
  const csvContent = fs.readFileSync(filePath, "utf-8");
  const records: Record<string, string>[] = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
  });
  
  for (const record of records) {
    const code = trim(record["コード"]);
    const name = trim(record["銘柄名"]);
    
    if (code && name) {
      // 同じ銘柄名が複数ある場合は最初のものを優先
      if (!map.has(name)) {
        map.set(name, code);
      }
    }
  }
  
  console.log(`📊 証券コードCSV読み込み完了: ${map.size} 件`);
  return map;
}

// 数値IDを生成（新規作成用）
function generateNumericDocId(index: number): string {
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  if (DRY_RUN) {
    console.log("🔍 ドライランモード: 実際の更新は行いません\n");
  }

  // 証券コードCSVを読み込む
  console.log("📖 証券コードCSVを読み込み中...");
  const securitiesCodeMap = loadSecuritiesCodeCsv(SHOKEN_CODE_CSV_PATH);

  // listing="上場" の企業を取得
  console.log("\n🔍 listing='上場' の企業を取得中...");
  const listedCompaniesSnapshot = await companiesCol
    .where("listing", "==", "上場")
    .get();

  console.log(`📊 上場企業数: ${listedCompaniesSnapshot.size} 件\n`);

  const stats = {
    updated: 0,
    created: 0,
    notMatched: 0,
    alreadyHasCode: 0,
  };

  const batchSize = 500;
  let batch: WriteBatch | null = null;
  let batchCount = 0;
  let globalIndex = 0;

  // 既存の企業名を収集（照合用）
  const existingCompanyNames = new Set<string>();
  const existingCompanyDocs = new Map<string, DocumentReference>();
  
  for (const doc of listedCompaniesSnapshot.docs) {
    const companyData = doc.data();
    const name = normalizeCompanyNameForMatching(companyData.name);
    if (name) {
      existingCompanyNames.add(name);
      existingCompanyDocs.set(name, doc.ref);
    }
  }

  // 証券コードマップの各銘柄名について処理
  const processedSecuritiesCodes = new Set<string>(); // 処理済みの証券コードを記録

  // ステップ1: 既存の上場企業に証券コードを追加
  console.log("\n📝 既存企業への証券コード追加中...");
  
  for (const doc of listedCompaniesSnapshot.docs) {
    const companyData = doc.data();
    const companyName = companyData.name; // 元の企業名を保持
    
    if (!companyName) {
      continue;
    }

    // 既に証券コードが設定されているかチェック
    if (companyData.securitiesCode) {
      stats.alreadyHasCode++;
      continue;
    }

    // 銘柄名と照合
    let matchedCode: string | null = null;
    let matchedName: string | null = null;

    for (const [csvName, code] of securitiesCodeMap.entries()) {
      if (matchCompanyName(companyName, csvName)) {
        matchedCode = code;
        matchedName = csvName;
        break;
      }
    }

    if (matchedCode) {
      // 既存企業に証券コードを追加
      if (!batch) {
        batch = db.batch();
      }

      batch.update(doc.ref, {
        securitiesCode: matchedCode,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      processedSecuritiesCodes.add(matchedCode);
      stats.updated++;
      batchCount++;

      // 最初の10件は詳細ログを出力
      if (stats.updated <= 10) {
        console.log(`  ✅ 照合成功: "${companyName}" ↔ "${matchedName}" (証券コード: ${matchedCode})`);
      }

      if (batchCount >= batchSize) {
        if (!DRY_RUN) {
          await batch.commit();
        }
        console.log(`  ✅ バッチコミット: ${stats.updated} 件更新`);
        batch = null;
        batchCount = 0;
      }
    } else {
      // 照合できない場合
      stats.notMatched++;
      if (stats.notMatched <= 20) {
        console.log(`  ⚠️  照合できず: "${companyName}"`);
      }
    }
  }

  // 残りのバッチをコミット
  if (batch && batchCount > 0) {
    if (!DRY_RUN) {
      await batch.commit();
    }
    console.log(`  ✅ 最終バッチコミット: ${stats.updated} 件更新`);
  }

  // ステップ2: 証券コードマップに存在するが、companies_newに存在しない企業を新規作成
  console.log("\n🆕 新規企業の作成中...");
  
  batch = null;
  batchCount = 0;

  for (const [csvName, code] of securitiesCodeMap.entries()) {
    // 既に処理済みの証券コードはスキップ
    if (processedSecuritiesCodes.has(code)) {
      continue;
    }

    const normalizedCsvName = normalizeCompanyNameForMatching(csvName);
    if (!normalizedCsvName) continue;

    // 既存企業に存在するかチェック
    let exists = false;
    for (const existingName of existingCompanyNames) {
      if (matchCompanyName(normalizedCsvName, existingName)) {
        exists = true;
        break;
      }
    }

    if (!exists) {
      // 新規作成
      if (!batch) {
        batch = db.batch();
      }

      const newCompanyData = { ...COMPANY_TEMPLATE };
      newCompanyData.name = csvName;
      newCompanyData.securitiesCode = code;
      newCompanyData.listing = "上場";
      newCompanyData.createdAt = admin.firestore.FieldValue.serverTimestamp();
      newCompanyData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

      const newDocId = generateNumericDocId(globalIndex++);
      const newDocRef = companiesCol.doc(newDocId);
      batch.set(newDocRef, newCompanyData);

      stats.created++;
      batchCount++;

      if (batchCount >= batchSize) {
        if (!DRY_RUN) {
          await batch.commit();
        }
        console.log(`  ✅ バッチコミット: ${stats.created} 件作成`);
        batch = null;
        batchCount = 0;
      }
    }
  }

  // 残りのバッチをコミット
  if (batch && batchCount > 0) {
    if (!DRY_RUN) {
      await batch.commit();
    }
    console.log(`  ✅ 最終バッチコミット: ${stats.created} 件作成`);
  }

  // 統計を表示
  console.log("\n" + "=".repeat(60));
  console.log("📊 処理結果");
  console.log("=".repeat(60));
  console.log(`  ✅ 証券コードを追加した企業: ${stats.updated} 件`);
  console.log(`  🆕 新規作成した企業: ${stats.created} 件`);
  console.log(`  ⚠️  照合できなかった企業: ${stats.notMatched} 件`);
  console.log(`  ℹ️  既に証券コードがある企業: ${stats.alreadyHasCode} 件`);
  console.log("=".repeat(60));

  if (DRY_RUN) {
    console.log("\n🔍 ドライランモードのため、実際の更新は行われませんでした");
  }
}

main()
  .then(() => {
    console.log("\n✅ 処理完了");
    process.exit(0);
  })
  .catch((err) => {
    console.error("\n❌ エラーが発生しました:");
    console.error(err);
    process.exit(1);
  });

