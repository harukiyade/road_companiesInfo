/* 
  companies_newコレクション内で法人番号がnullまたは空の企業を洗い出すスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/check_missing_corporate_numbers.ts [--limit=N] [--output=file.json]
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";

const COLLECTION_NAME = "companies_new";

// ドライランモード（--dry-run フラグで有効化）
const LIMIT = process.argv.find(arg => arg.startsWith("--limit="))
  ? parseInt(process.argv.find(arg => arg.startsWith("--limit="))!.split("=")[1])
  : null;

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
    console.error(`環境変数 GOOGLE_APPLICATION_CREDENTIALS を設定してください`);
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
  
  log("🔍 法人番号がnullまたは空の企業を検索中...");
  
  // 全件取得してフィルタリング（null検索はタイムアウトするため）
  let missingFieldCount = 0;
  let nullCount = 0;
  let emptyCount = 0;
  let allDocs: any[] = [];
  
  // バッチで取得
  const PAGE_SIZE = 1000;
  let lastDoc: any = null;
  let totalProcessed = 0;
  
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
      
      // 法人番号がnull、undefined、空文字列、または存在しない場合
      const isMissing = corporateNumber === null || 
          corporateNumber === undefined || 
          corporateNumber === "" ||
          !("corporateNumber" in data);
      
      if (isMissing) {
        missingFieldCount++;
        
        if (corporateNumber === null) nullCount++;
        if (corporateNumber === "") emptyCount++;
        
        if (!LIMIT || allDocs.length < LIMIT) {
          allDocs.push({
            docId: doc.id,
            name: data.name || null,
            address: data.address || null,
            prefecture: data.prefecture || null,
            corporateNumber: corporateNumber || null,
            createdAt: data.createdAt ? data.createdAt.toDate().toISOString() : null,
            updatedAt: data.updatedAt ? data.updatedAt.toDate().toISOString() : null,
          });
        }
      }
      
      totalProcessed++;
      if (totalProcessed % 10000 === 0) {
        log(`  📊 処理中: ${totalProcessed.toLocaleString()} 件、見つかった: ${allDocs.length.toLocaleString()} 社`);
      }
      
      if (LIMIT && allDocs.length >= LIMIT) {
        log(`  ⏸️  制限に達しました: ${LIMIT} 社`);
        break;
      }
    }
    
    if (LIMIT && allDocs.length >= LIMIT) break;
    
    lastDoc = batchSnapshot.docs[batchSnapshot.docs.length - 1];
    
    if (batchSnapshot.size < PAGE_SIZE) break;
  }
  
  log(`\n📊 検索結果:`);
  log(`   - 総処理件数: ${totalProcessed.toLocaleString()} 社`);
  log(`   - 法人番号がnull: ${nullCount.toLocaleString()} 社`);
  log(`   - 法人番号が空文字列: ${emptyCount.toLocaleString()} 社`);
  log(`   - 法人番号フィールドが存在しない/null/空: ${missingFieldCount.toLocaleString()} 社`);
  log(`   - 取得した詳細データ: ${allDocs.length.toLocaleString()} 社`);
  
  // サンプルデータを表示
  if (allDocs.length > 0) {
    log(`\n📋 サンプルデータ（最初の10社）:`);
    allDocs.slice(0, 10).forEach((doc, index) => {
      log(`\n   ${index + 1}. docId: ${doc.docId}`);
      log(`      name: ${doc.name || "(空)"}`);
      log(`      address: ${doc.address ? doc.address.substring(0, 50) + "..." : "(空)"}`);
      log(`      prefecture: ${doc.prefecture || "(空)"}`);
      log(`      corporateNumber: ${doc.corporateNumber || "(null)"}`);
    });
  }
  
  // ファイルに出力
  if (OUTPUT_FILE && allDocs.length > 0) {
    const outputPath = path.resolve(OUTPUT_FILE);
    fs.writeFileSync(outputPath, JSON.stringify(allDocs, null, 2), "utf8");
    log(`\n💾 結果をファイルに保存しました: ${outputPath}`);
  }
  
  // 統計情報
  const withName = allDocs.filter(d => d.name).length;
  const withAddress = allDocs.filter(d => d.address).length;
  const withPrefecture = allDocs.filter(d => d.prefecture).length;
  
  log(`\n📊 統計情報:`);
  log(`   - 社名がある: ${withName.toLocaleString()} 社 (${((withName / allDocs.length) * 100).toFixed(1)}%)`);
  log(`   - 住所がある: ${withAddress.toLocaleString()} 社 (${((withAddress / allDocs.length) * 100).toFixed(1)}%)`);
  log(`   - 都道府県がある: ${withPrefecture.toLocaleString()} 社 (${((withPrefecture / allDocs.length) * 100).toFixed(1)}%)`);
  
  log("\n✅ 処理完了");
}

// 実行
main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
