/* 
  companies_newコレクション内で、nameまたはaddressがあるが法人番号がnullの企業を検索
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/check_missing_corporate_numbers_with_name.ts [--limit=N] [--output=file.json]
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";

const COLLECTION_NAME = "companies_new";

const LIMIT = process.argv.find(arg => arg.startsWith("--limit="))
  ? parseInt(process.argv.find(arg => arg.startsWith("--limit="))!.split("=")[1])
  : 1000;

const OUTPUT_FILE = process.argv.find(arg => arg.startsWith("--output="))
  ? process.argv.find(arg => arg.startsWith("--output="))!.split("=")[1]
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

async function main() {
  initAdmin();
  
  const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);
  
  log("🔍 nameまたはaddressがあるが法人番号がnullの企業を検索中...");
  
  let allDocs: any[] = [];
  let totalProcessed = 0;
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
      const name = data.name;
      const address = data.address;
      
      // 法人番号がnull/undefined/空 かつ (nameまたはaddressがある)
      const isMissingCorpNum = corporateNumber === null || 
          corporateNumber === undefined || 
          corporateNumber === "" ||
          !("corporateNumber" in data);
      
      const hasNameOrAddress = (name && name.trim() !== "") || (address && address.trim() !== "");
      
      if (isMissingCorpNum && hasNameOrAddress) {
        allDocs.push({
          docId: doc.id,
          name: name || null,
          address: address || null,
          prefecture: data.prefecture || null,
          corporateNumber: corporateNumber || null,
          createdAt: data.createdAt ? (data.createdAt.toDate ? data.createdAt.toDate().toISOString() : new Date(data.createdAt).toISOString()) : null,
          updatedAt: data.updatedAt ? (data.updatedAt.toDate ? data.updatedAt.toDate().toISOString() : new Date(data.updatedAt).toISOString()) : null,
        });
      }
      
      totalProcessed++;
      if (totalProcessed % 10000 === 0) {
        log(`  📊 処理中: ${totalProcessed.toLocaleString()} 件、見つかった: ${allDocs.length.toLocaleString()} 社`);
      }
      
      if (allDocs.length >= LIMIT) {
        log(`  ⏸️  制限に達しました: ${LIMIT} 社`);
        break;
      }
    }
    
    if (allDocs.length >= LIMIT) break;
    
    lastDoc = batchSnapshot.docs[batchSnapshot.docs.length - 1];
    
    if (batchSnapshot.size < PAGE_SIZE) break;
  }
  
  log(`\n📊 検索結果:`);
  log(`   - 総処理件数: ${totalProcessed.toLocaleString()} 社`);
  log(`   - nameまたはaddressがあるが法人番号がnull: ${allDocs.length.toLocaleString()} 社`);
  
  if (allDocs.length > 0) {
    log(`\n📋 サンプルデータ（最初の20社）:`);
    allDocs.slice(0, 20).forEach((doc, index) => {
      log(`\n   ${index + 1}. docId: ${doc.docId}`);
      log(`      name: ${doc.name || "(空)"}`);
      log(`      address: ${doc.address ? doc.address.substring(0, 60) + "..." : "(空)"}`);
      log(`      prefecture: ${doc.prefecture || "(空)"}`);
    });
    
    // ファイルに出力
    if (OUTPUT_FILE) {
      const outputPath = path.resolve(OUTPUT_FILE);
      fs.writeFileSync(outputPath, JSON.stringify(allDocs, null, 2), "utf8");
      log(`\n💾 結果をファイルに保存しました: ${outputPath}`);
    }
  } else {
    log(`\n⚠️  nameまたはaddressがあるが法人番号がnullの企業は見つかりませんでした`);
  }
  
  log("\n✅ 処理完了");
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
