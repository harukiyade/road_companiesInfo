/* 
  companies_newコレクションのすべてのドキュメントに対して、
  指定された155個のフィールドが存在することを保証するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/ensure_companies_new_fields.ts [--limit=N] [--dry-run]
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference, DocumentReference } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";

const COLLECTION_NAME = "companies_new";

// ドライランモード（--dry-run フラグで有効化）
const DRY_RUN = process.argv.includes("--dry-run");
const LIMIT = process.argv.find(arg => arg.startsWith("--limit="))
  ? parseInt(process.argv.find(arg => arg.startsWith("--limit="))!.split("=")[1])
  : null;

// ==============================
// 必須フィールド定義（155フィールド）
// ==============================

const REQUIRED_FIELDS = [
  // 📊 基本情報（15フィールド）
  "companyId",
  "name",
  "nameEn",
  "kana",
  "corporateNumber",
  "corporationType",
  "nikkeiCode",
  "badges",
  "tags",
  "createdAt",
  "updatedAt",
  "updateDate",
  "updateCount",
  "changeCount",
  "qualificationGrade",
  
  // 📍 所在地情報（6フィールド）
  "prefecture",
  "address",
  "headquartersAddress",
  "postalCode",
  "location",
  "departmentLocation",
  
  // 📞 連絡先情報（6フィールド）
  "phoneNumber",
  "contactPhoneNumber",
  "fax",
  "email",
  "companyUrl",
  "contactFormUrl",
  
  // 👤 代表者情報（10フィールド）
  "representativeName",
  "representativeKana",
  "representativeTitle",
  "representativeBirthDate",
  "representativePhone",
  "representativePostalCode",
  "representativeHomeAddress",
  "representativeRegisteredAddress",
  "representativeAlmaMater",
  "executives",
  
  // 👔 役員情報（20フィールド）
  "executiveName1",
  "executiveName2",
  "executiveName3",
  "executiveName4",
  "executiveName5",
  "executiveName6",
  "executiveName7",
  "executiveName8",
  "executiveName9",
  "executiveName10",
  "executivePosition1",
  "executivePosition2",
  "executivePosition3",
  "executivePosition4",
  "executivePosition5",
  "executivePosition6",
  "executivePosition7",
  "executivePosition8",
  "executivePosition9",
  "executivePosition10",
  
  // 🏢 業種情報（13フィールド）
  "industry",
  "industryLarge",
  "industryMiddle",
  "industrySmall",
  "industryDetail",
  "industries",
  "industryCategories",
  "businessDescriptions",
  "businessItems",
  "businessSummary",
  "specialties",
  "demandProducts",
  "specialNote",
  
  // 💰 財務情報（29フィールド）
  "capitalStock",
  "revenue",
  "latestRevenue",
  "latestProfit",
  "revenueFromStatements",
  "operatingIncome",
  "totalAssets",
  "totalLiabilities",
  "netAssets",
  "issuedShares",
  "financials",
  "listing",
  "marketSegment",
  "latestFiscalYearMonth",
  "fiscalMonth",
  "fiscalMonth1",
  "fiscalMonth2",
  "fiscalMonth3",
  "fiscalMonth4",
  "fiscalMonth5",
  "revenue1",
  "revenue2",
  "revenue3",
  "revenue4",
  "revenue5",
  "profit1",
  "profit2",
  "profit3",
  "profit4",
  "profit5",
  
  // 🏭 企業規模・組織（10フィールド）
  "employeeCount",
  "employeeNumber",
  "factoryCount",
  "officeCount",
  "storeCount",
  "averageAge",
  "averageYearsOfService",
  "averageOvertimeHours",
  "averagePaidLeave",
  "femaleExecutiveRatio",
  
  // 📅 設立・沿革（5フィールド）
  "established",
  "dateOfEstablishment",
  "founding",
  "foundingYear",
  "acquisition",
  
  // 🤝 取引先・関係会社（7フィールド）
  "clients",
  "suppliers",
  "subsidiaries",
  "affiliations",
  "shareholders",
  "banks",
  "bankCorporateNumber",
  
  // 🏢 部署・拠点情報（21フィールド）
  "departmentName1",
  "departmentName2",
  "departmentName3",
  "departmentName4",
  "departmentName5",
  "departmentName6",
  "departmentName7",
  "departmentAddress1",
  "departmentAddress2",
  "departmentAddress3",
  "departmentAddress4",
  "departmentAddress5",
  "departmentAddress6",
  "departmentAddress7",
  "departmentPhone1",
  "departmentPhone2",
  "departmentPhone3",
  "departmentPhone4",
  "departmentPhone5",
  "departmentPhone6",
  "departmentPhone7",
  
  // 📝 企業説明（4フィールド）
  "overview",
  "companyDescription",
  "salesNotes",
  
  // 🌐 SNS・外部リンク（8フィールド）
  "urls",
  "profileUrl",
  "externalDetailUrl",
  "facebook",
  "linkedin",
  "wantedly",
  "youtrust",
  "metaKeywords",
];

// 配列型のフィールド（nullではなく空配列で初期化）
const ARRAY_FIELDS = [
  "badges",
  "tags",
  "industries",
  "businessItems",
  "clients",
  "suppliers",
  "subsidiaries",
  "shareholders",
  "banks",
  "executives",
  "urls",
  "financials",
  "industryCategories",
];

// ==============================
// Firebase初期化
// ==============================

let db: Firestore;

function initAdmin() {
  if (admin.apps.length > 0) {
    db = admin.firestore();
    return;
  }

  const serviceAccountPath =
    process.env.GOOGLE_APPLICATION_CREDENTIALS ||
    path.join(__dirname, "../serviceAccountKey.json");

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(`❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });

  db = admin.firestore();
}

// ==============================
// ログ関数
// ==============================

function log(message: string) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
}

// ==============================
// メイン処理
// ==============================

async function main() {
  initAdmin();
  
  const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);
  
  if (DRY_RUN) {
    log("🔍 ドライランモード: 実際の更新は行いません");
  }
  
  log(`📋 必須フィールド数: ${REQUIRED_FIELDS.length} 個`);
  log(`📋 配列型フィールド数: ${ARRAY_FIELDS.length} 個`);
  
  // すべてのドキュメントを取得
  log("🔍 ドキュメントを取得中...");
  
  const PAGE_SIZE = 1000;
  let lastDoc: any = null;
  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalFieldsAdded = 0;
  const updatedDocIds: string[] = [];
  
  while (true) {
    let batchQuery = companiesCol.orderBy("__name__").limit(PAGE_SIZE);
    if (lastDoc) {
      batchQuery = batchQuery.startAfter(lastDoc);
    }
    
    const batchSnapshot = await batchQuery.get();
    
    if (batchSnapshot.empty) break;
    
    // バッチで更新
    const batchWrite = db.batch();
    let batchUpdateCount = 0;
    
    for (const doc of batchSnapshot.docs) {
      const data = doc.data();
      const missingFields: Record<string, any> = {};
      let hasMissingFields = false;
      
      // 各必須フィールドをチェック
      for (const fieldName of REQUIRED_FIELDS) {
        // フィールドが存在しない、またはundefinedの場合
        if (!(fieldName in data) || data[fieldName] === undefined) {
          // 配列型フィールドの場合は空配列、それ以外はnull
          if (ARRAY_FIELDS.includes(fieldName)) {
            missingFields[fieldName] = [];
          } else {
            missingFields[fieldName] = null;
          }
          hasMissingFields = true;
        }
      }
      
      // 欠けているフィールドがある場合は更新
      if (hasMissingFields) {
        // companyIdはドキュメントIDを設定（存在しない場合のみ）
        if (!("companyId" in data) || data.companyId === undefined) {
          missingFields.companyId = doc.id;
        }
        
        // createdAtとupdatedAtは現在時刻を設定（存在しない場合のみ）
        const now = admin.firestore.Timestamp.now();
        if (!("createdAt" in data) || data.createdAt === undefined) {
          missingFields.createdAt = now;
        }
        if (!("updatedAt" in data) || data.updatedAt === undefined) {
          missingFields.updatedAt = now;
        }
        
        // updateDateは現在日付を設定（存在しない場合のみ）
        if (!("updateDate" in data) || data.updateDate === undefined) {
          missingFields.updateDate = now.toDate().toISOString().split("T")[0];
        }
        
        // updateCountとchangeCountは0を設定（存在しない場合のみ）
        if (!("updateCount" in data) || data.updateCount === undefined) {
          missingFields.updateCount = 0;
        }
        if (!("changeCount" in data) || data.changeCount === undefined) {
          missingFields.changeCount = 0;
        }
        
        if (!DRY_RUN) {
          batchWrite.update(doc.ref, missingFields);
        }
        
        batchUpdateCount++;
        totalFieldsAdded += Object.keys(missingFields).length;
        
        if (updatedDocIds.length < 50) {
          updatedDocIds.push(doc.id);
        }
      }
      
      totalProcessed++;
      
      if (totalProcessed % 10000 === 0) {
        log(`  📊 処理中: ${totalProcessed.toLocaleString()} 件、更新: ${totalUpdated.toLocaleString()} 件`);
      }
      
      if (LIMIT && totalProcessed >= LIMIT) {
        break;
      }
    }
    
    // バッチをコミット
    if (batchUpdateCount > 0 && !DRY_RUN) {
      await batchWrite.commit();
      totalUpdated += batchUpdateCount;
      log(`  ✅ バッチ更新完了: ${batchUpdateCount} 件のドキュメントを更新`);
    } else if (batchUpdateCount > 0 && DRY_RUN) {
      totalUpdated += batchUpdateCount;
      log(`  🔍 ドライラン: ${batchUpdateCount} 件のドキュメントを更新予定`);
    }
    
    if (LIMIT && totalProcessed >= LIMIT) break;
    
    lastDoc = batchSnapshot.docs[batchSnapshot.docs.length - 1];
    
    if (batchSnapshot.size < PAGE_SIZE) break;
  }
  
  log(`\n✅ 処理完了:`);
  log(`   - 処理対象: ${totalProcessed.toLocaleString()} 件`);
  log(`   - 更新したドキュメント: ${totalUpdated.toLocaleString()} 件`);
  log(`   - 追加したフィールド数: ${totalFieldsAdded.toLocaleString()} 個`);
  
  if (updatedDocIds.length > 0) {
    log(`\n📋 更新したドキュメントID（最初の${updatedDocIds.length}件）:`);
    updatedDocIds.forEach((docId, index) => {
      log(`   ${index + 1}. ${docId}`);
    });
  }
  
  if (DRY_RUN) {
    log(`\n⚠️  ドライランモードでした。実際の更新を行うには --dry-run フラグを外してください。`);
  }
}

// 実行
main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
