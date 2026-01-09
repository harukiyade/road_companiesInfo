/* 
  companies_newコレクション内で、corporateNumberフィールドがnullまたは存在しない企業を洗い出す
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/list_null_or_missing_corporate_numbers.ts [--limit=N] [--output=file.json]
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";
import * as csv from "csv-writer";

const COLLECTION_NAME = "companies_new";

const LIMIT = process.argv.find(arg => arg.startsWith("--limit="))
  ? parseInt(process.argv.find(arg => arg.startsWith("--limit="))!.split("=")[1])
  : null;

const OUTPUT_JSON = process.argv.find(arg => arg.startsWith("--output-json="))
  ? process.argv.find(arg => arg.startsWith("--output-json="))!.split("=")[1]
  : null;

const OUTPUT_CSV = process.argv.find(arg => arg.startsWith("--output-csv="))
  ? process.argv.find(arg => arg.startsWith("--output-csv="))!.split("=")[1]
  : null;

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

function log(message: string) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
}

// ==============================
// メイン処理
// ==============================

interface CompanyInfo {
  docId: string;
  name: string | null;
  address: string | null;
  postalCode: string | null;
  corporateNumber: string | null;
  hasCorporateNumberField: boolean;
  corporateNumberStatus: "null" | "missing" | "empty";
  createdAt: string | null;
  updatedAt: string | null;
}

async function main() {
  initAdmin();
  
  const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);
  
  log("🔍 corporateNumberフィールドがnullまたは存在しない企業を検索中...");
  
  const nullCompanies: CompanyInfo[] = [];
  const missingCompanies: CompanyInfo[] = [];
  const emptyCompanies: CompanyInfo[] = [];
  
  let totalProcessed = 0;
  let totalWithCorporateNumber = 0;
  const PAGE_SIZE = 1000;
  let lastDoc: any = null;
  
  while (true) {
    let batchQuery = companiesCol.orderBy("__name__").limit(PAGE_SIZE);
    if (lastDoc) {
      batchQuery = batchQuery.startAfter(lastDoc);
    }
    
    const batchSnapshot = await batchQuery.get();
    
    if (batchSnapshot.empty) break;
    
    for (const doc of batchSnapshot.docs) {
      const data = doc.data();
      const corporateNumber = data.corporateNumber;
      const hasCorporateNumberField = "corporateNumber" in data;
      
      // corporateNumberフィールドが存在しない場合
      if (!hasCorporateNumberField) {
        missingCompanies.push({
          docId: doc.id,
          name: data.name || null,
          address: data.address || data.headquartersAddress || null,
          postalCode: data.postalCode || null,
          corporateNumber: null,
          hasCorporateNumberField: false,
          corporateNumberStatus: "missing",
          createdAt: data.createdAt ? (data.createdAt.toDate ? data.createdAt.toDate().toISOString() : new Date(data.createdAt).toISOString()) : null,
          updatedAt: data.updatedAt ? (data.updatedAt.toDate ? data.updatedAt.toDate().toISOString() : new Date(data.updatedAt).toISOString()) : null,
        });
      }
      // corporateNumberフィールドがnullの場合
      else if (corporateNumber === null) {
        nullCompanies.push({
          docId: doc.id,
          name: data.name || null,
          address: data.address || data.headquartersAddress || null,
          postalCode: data.postalCode || null,
          corporateNumber: null,
          hasCorporateNumberField: true,
          corporateNumberStatus: "null",
          createdAt: data.createdAt ? (data.createdAt.toDate ? data.createdAt.toDate().toISOString() : new Date(data.createdAt).toISOString()) : null,
          updatedAt: data.updatedAt ? (data.updatedAt.toDate ? data.updatedAt.toDate().toISOString() : new Date(data.updatedAt).toISOString()) : null,
        });
      }
      // corporateNumberフィールドが空文字列の場合
      else if (corporateNumber === "" || corporateNumber === undefined) {
        emptyCompanies.push({
          docId: doc.id,
          name: data.name || null,
          address: data.address || data.headquartersAddress || null,
          postalCode: data.postalCode || null,
          corporateNumber: null,
          hasCorporateNumberField: true,
          corporateNumberStatus: "empty",
          createdAt: data.createdAt ? (data.createdAt.toDate ? data.createdAt.toDate().toISOString() : new Date(data.createdAt).toISOString()) : null,
          updatedAt: data.updatedAt ? (data.updatedAt.toDate ? data.updatedAt.toDate().toISOString() : new Date(data.updatedAt).toISOString()) : null,
        });
      } else {
        totalWithCorporateNumber++;
      }
      
      totalProcessed++;
      
      if (totalProcessed % 10000 === 0) {
        log(`  📊 処理中: ${totalProcessed.toLocaleString()} 件、null: ${nullCompanies.length.toLocaleString()}, フィールドなし: ${missingCompanies.length.toLocaleString()}, 空文字列: ${emptyCompanies.length.toLocaleString()}, あり: ${totalWithCorporateNumber.toLocaleString()}`);
      }
      
      if (LIMIT && (nullCompanies.length + missingCompanies.length + emptyCompanies.length) >= LIMIT) {
        log(`  ⏸️  制限に達しました: ${LIMIT} 社`);
        break;
      }
    }
    
    if (LIMIT && (nullCompanies.length + missingCompanies.length + emptyCompanies.length) >= LIMIT) break;
    
    lastDoc = batchSnapshot.docs[batchSnapshot.docs.length - 1];
    
    if (batchSnapshot.size < PAGE_SIZE) break;
  }
  
  const allCompanies = [...nullCompanies, ...missingCompanies, ...emptyCompanies];
  
  log(`\n📊 検索結果:`);
  log(`   - 総処理件数: ${totalProcessed.toLocaleString()} 社`);
  log(`   - corporateNumberフィールドがnull: ${nullCompanies.length.toLocaleString()} 社`);
  log(`   - corporateNumberフィールドが存在しない: ${missingCompanies.length.toLocaleString()} 社`);
  log(`   - corporateNumberフィールドが空文字列: ${emptyCompanies.length.toLocaleString()} 社`);
  log(`   - 合計（null/存在しない/空文字列）: ${allCompanies.length.toLocaleString()} 社`);
  log(`   - corporateNumberあり: ${totalWithCorporateNumber.toLocaleString()} 社`);
  
  if (allCompanies.length > 0) {
    log(`\n📋 サンプルデータ（最初の20社）:`);
    allCompanies.slice(0, 20).forEach((doc, index) => {
      log(`\n   ${index + 1}. docId: ${doc.docId}`);
      log(`      ステータス: ${doc.corporateNumberStatus === "null" ? "null" : doc.corporateNumberStatus === "missing" ? "フィールドなし" : "空文字列"}`);
      log(`      name: ${doc.name || "(空)"}`);
      log(`      address: ${doc.address ? doc.address.substring(0, 60) + "..." : "(空)"}`);
      log(`      postalCode: ${doc.postalCode || "(空)"}`);
    });
    
    // JSONファイルに出力
    if (OUTPUT_JSON) {
      const outputPath = path.resolve(OUTPUT_JSON);
      fs.writeFileSync(outputPath, JSON.stringify(allCompanies, null, 2), "utf8");
      log(`\n💾 JSON結果をファイルに保存しました: ${outputPath}`);
    } else {
      // デフォルトの出力ファイル
      const defaultOutputPath = path.join(__dirname, "../null_or_missing_corporate_numbers.json");
      fs.writeFileSync(defaultOutputPath, JSON.stringify(allCompanies, null, 2), "utf8");
      log(`\n💾 JSON結果をファイルに保存しました: ${defaultOutputPath}`);
    }
    
    // CSVファイルに出力
    if (OUTPUT_CSV) {
      const csvPath = path.resolve(OUTPUT_CSV);
      const writer = csv.createObjectCsvWriter({
        path: csvPath,
        header: [
          { id: "docId", title: "docId" },
          { id: "name", title: "name" },
          { id: "address", title: "address" },
          { id: "postalCode", title: "postalCode" },
          { id: "corporateNumber", title: "corporateNumber" },
          { id: "hasCorporateNumberField", title: "hasCorporateNumberField" },
          { id: "corporateNumberStatus", title: "corporateNumberStatus" },
          { id: "createdAt", title: "createdAt" },
          { id: "updatedAt", title: "updatedAt" },
        ],
        encoding: "utf8",
      });
      
      await writer.writeRecords(allCompanies.map(c => ({
        docId: c.docId,
        name: c.name || "",
        address: c.address || "",
        postalCode: c.postalCode || "",
        corporateNumber: c.corporateNumber || "",
        hasCorporateNumberField: c.hasCorporateNumberField ? "true" : "false",
        corporateNumberStatus: c.corporateNumberStatus,
        createdAt: c.createdAt || "",
        updatedAt: c.updatedAt || "",
      })));
      
      log(`💾 CSV結果をファイルに保存しました: ${csvPath}`);
    } else {
      // デフォルトの出力ファイル
      const defaultCsvPath = path.join(__dirname, "../out/null_or_missing_corporate_numbers.csv");
      const outDir = path.dirname(defaultCsvPath);
      if (!fs.existsSync(outDir)) {
        fs.mkdirSync(outDir, { recursive: true });
      }
      
      const writer = csv.createObjectCsvWriter({
        path: defaultCsvPath,
        header: [
          { id: "docId", title: "docId" },
          { id: "name", title: "name" },
          { id: "address", title: "address" },
          { id: "postalCode", title: "postalCode" },
          { id: "corporateNumber", title: "corporateNumber" },
          { id: "hasCorporateNumberField", title: "hasCorporateNumberField" },
          { id: "corporateNumberStatus", title: "corporateNumberStatus" },
          { id: "createdAt", title: "createdAt" },
          { id: "updatedAt", title: "updatedAt" },
        ],
        encoding: "utf8",
      });
      
      await writer.writeRecords(allCompanies.map(c => ({
        docId: c.docId,
        name: c.name || "",
        address: c.address || "",
        postalCode: c.postalCode || "",
        corporateNumber: c.corporateNumber || "",
        hasCorporateNumberField: c.hasCorporateNumberField ? "true" : "false",
        corporateNumberStatus: c.corporateNumberStatus,
        createdAt: c.createdAt || "",
        updatedAt: c.updatedAt || "",
      })));
      
      log(`💾 CSV結果をファイルに保存しました: ${defaultCsvPath}`);
    }
  } else {
    log(`\n✅ corporateNumberフィールドがnullまたは存在しない企業は見つかりませんでした`);
  }
  
  log("\n✅ 処理完了");
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
