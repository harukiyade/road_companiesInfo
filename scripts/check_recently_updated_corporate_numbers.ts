/* 
  最近法人番号が更新されたドキュメントを確認するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/check_recently_updated_corporate_numbers.ts [--limit=N] [--minutes=M]
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";

const COLLECTION_NAME = "companies_new";

const LIMIT = process.argv.find(arg => arg.startsWith("--limit="))
  ? parseInt(process.argv.find(arg => arg.startsWith("--limit="))!.split("=")[1])
  : 100;

const MINUTES = process.argv.find(arg => arg.startsWith("--minutes="))
  ? parseInt(process.argv.find(arg => arg.startsWith("--minutes="))!.split("=")[1])
  : 60; // デフォルトは60分以内

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
  
  log(`🔍 最近${MINUTES}分以内に更新されたドキュメントで、法人番号が設定されているものを検索中...`);
  
  const cutoffTime = admin.firestore.Timestamp.fromDate(
    new Date(Date.now() - MINUTES * 60 * 1000)
  );
  
  const results: Array<{
    docId: string;
    corporateNumber: string;
    name: string;
    address: string;
    updatedAt: string;
  }> = [];
  
  // updatedAtでソートして取得（ただし、インデックスが必要）
  // インデックスがない場合は、全件スキャンしてフィルタリング
  const PAGE_SIZE = 1000;
  let lastDoc: any = null;
  let totalProcessed = 0;
  
  while (results.length < LIMIT) {
    let batchQuery = companiesCol.orderBy("updatedAt", "desc").limit(PAGE_SIZE);
    if (lastDoc) {
      batchQuery = batchQuery.startAfter(lastDoc);
    }
    
    const batchSnapshot = await batchQuery.get();
    
    if (batchSnapshot.empty) break;
    
    for (const doc of batchSnapshot.docs) {
      const data = doc.data();
      const updatedAt = data.updatedAt;
      const corporateNumber = data.corporateNumber;
      
      // updatedAtがcutoffTimeより新しいか確認
      if (updatedAt && updatedAt.toMillis && updatedAt.toMillis() >= cutoffTime.toMillis()) {
        // 法人番号が設定されているか確認
        if (corporateNumber && corporateNumber !== "" && corporateNumber !== null) {
          // 以前は法人番号がnullだった可能性が高い（今回の更新で追加された）
          results.push({
            docId: doc.id,
            corporateNumber: corporateNumber,
            name: data.name || "(社名なし)",
            address: data.address ? data.address.substring(0, 50) : "(住所なし)",
            updatedAt: updatedAt.toDate ? updatedAt.toDate().toISOString() : new Date(updatedAt).toISOString(),
          });
        }
      } else {
        // cutoffTimeより古い場合は、これ以上新しいものはない
        break;
      }
      
      totalProcessed++;
      if (totalProcessed % 10000 === 0) {
        log(`  📊 処理中: ${totalProcessed.toLocaleString()} 件、見つかった: ${results.length.toLocaleString()} 社`);
      }
      
      if (results.length >= LIMIT) break;
    }
    
    if (results.length >= LIMIT) break;
    
    lastDoc = batchSnapshot.docs[batchSnapshot.docs.length - 1];
    
    if (batchSnapshot.size < PAGE_SIZE) break;
  }
  
  log(`\n📊 検索結果:`);
  log(`   - 処理件数: ${totalProcessed.toLocaleString()} 社`);
  log(`   - 最近${MINUTES}分以内に更新され、法人番号が設定されている: ${results.length.toLocaleString()} 社`);
  
  if (results.length > 0) {
    log(`\n📋 更新されたドキュメントID（最初の${Math.min(50, results.length)}件）:`);
    results.slice(0, 50).forEach((item, index) => {
      log(`\n   ${index + 1}. docId: ${item.docId}`);
      log(`      法人番号: ${item.corporateNumber}`);
      log(`      社名: ${item.name}`);
      log(`      住所: ${item.address}`);
      log(`      更新日時: ${item.updatedAt}`);
    });
    if (results.length > 50) {
      log(`\n   ... 他 ${results.length - 50} 件`);
    }
  } else {
    log(`\n⚠️  最近${MINUTES}分以内に更新されたドキュメントは見つかりませんでした`);
    log(`   --minutes オプションで時間範囲を調整してください（例: --minutes=120）`);
  }
  
  log("\n✅ 処理完了");
}

main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
