/* 
  companies_new コレクションの prefecture フィールドを
  address または headquartersAddress から抽出して更新するスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx tsx scripts/update_prefecture_from_address.ts [--dry-run] [--limit=N]
    
  オプション:
    --dry-run: 実際には更新せず、更新予定の内容を表示
    --limit=N: 処理するドキュメント数を制限（テスト用）
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  WriteBatch,
  QueryDocumentSnapshot,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const BATCH_SIZE = 500; // Firestoreのバッチ制限

// ドライランモード（--dry-run フラグで有効化）
const DRY_RUN = process.argv.includes("--dry-run");
const LIMIT = process.argv.find((arg: string) => arg.startsWith("--limit="))
  ? parseInt(process.argv.find((arg: string) => arg.startsWith("--limit="))!.split("=")[1])
  : null;

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  
  // プレースホルダーの場合は無視
  if (serviceAccountPath && (
    serviceAccountPath.includes("/path/to/") ||
    serviceAccountPath.includes("path/to")
  )) {
    serviceAccountPath = undefined;
  }
  
  // ファイルが存在しない場合は無視
  if (serviceAccountPath && !fs.existsSync(serviceAccountPath)) {
    serviceAccountPath = undefined;
  }
  
  // デフォルトのパスを試す
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
      "/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
    ];
    
    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        console.log(`ℹ️  サービスアカウントキーを使用: ${resolvedPath}`);
        break;
      }
    }
  }
  
  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    console.error("   環境変数 GOOGLE_APPLICATION_CREDENTIALS を設定してください");
    console.error("   例: export GOOGLE_APPLICATION_CREDENTIALS='/path/to/serviceAccountKey.json'");
    console.error(`   現在の値: ${process.env.GOOGLE_APPLICATION_CREDENTIALS || "(未設定)"}`);
    process.exit(1);
  }
  
  try {
    const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  } catch (error: any) {
    console.error(`❌ サービスアカウントキーファイルの読み込みエラー: ${error.message}`);
    process.exit(1);
  }
}

const db: Firestore = admin.firestore();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// ==============================
// 都道府県リスト
// ==============================
const PREF_NAMES = [
  "北海道",
  "青森県",
  "岩手県",
  "宮城県",
  "秋田県",
  "山形県",
  "福島県",
  "茨城県",
  "栃木県",
  "群馬県",
  "埼玉県",
  "千葉県",
  "東京都",
  "神奈川県",
  "新潟県",
  "富山県",
  "石川県",
  "福井県",
  "山梨県",
  "長野県",
  "岐阜県",
  "静岡県",
  "愛知県",
  "三重県",
  "滋賀県",
  "京都府",
  "大阪府",
  "兵庫県",
  "奈良県",
  "和歌山県",
  "鳥取県",
  "島根県",
  "岡山県",
  "広島県",
  "山口県",
  "徳島県",
  "香川県",
  "愛媛県",
  "高知県",
  "福岡県",
  "佐賀県",
  "長崎県",
  "熊本県",
  "大分県",
  "宮崎県",
  "鹿児島県",
  "沖縄県",
];

// ==============================
// ユーティリティ関数
// ==============================

function log(message: string) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
}

// 住所文字列から都道府県を抽出
function extractPrefectureFromAddress(addr: string | null | undefined): string | null {
  if (!addr) return null;
  const s = String(addr).trim();
  if (!s) return null;
  
  // 先頭から都道府県名を探す（最優先）
  for (const p of PREF_NAMES) {
    if (s.startsWith(p)) return p;
  }
  
  // 先頭にない場合は、文字列内に都道府県名が含まれているか確認
  for (const p of PREF_NAMES) {
    if (s.includes(p)) return p;
  }
  
  return null;
}

// address または headquartersAddress から都道府県を抽出
function extractPrefecture(
  address: string | null | undefined,
  headquartersAddress: string | null | undefined
): string | null {
  // まず address から抽出を試みる
  const prefFromAddress = extractPrefectureFromAddress(address);
  if (prefFromAddress) return prefFromAddress;
  
  // address から抽出できなかった場合は headquartersAddress から抽出
  const prefFromHeadquarters = extractPrefectureFromAddress(headquartersAddress);
  if (prefFromHeadquarters) return prefFromHeadquarters;
  
  return null;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  log("🚀 prefecture フィールドの更新開始");
  if (DRY_RUN) {
    log("🔍 DRY_RUN モード: 実際には更新しません");
  }
  if (LIMIT) {
    log(`⚠️  制限: ${LIMIT} 件まで処理します`);
  }
  
  const stats = {
    processedCount: 0,
    updatedCount: 0,
    skippedCount: 0,
    errorCount: 0,
  };
  
  let batch: WriteBatch | null = null;
  let batchCount = 0;
  
  // バッチコミット関数
  async function commitBatch() {
    if (!batch || batchCount === 0) return;
    
    const currentBatch = batch;
    const currentBatchCount = batchCount;
    batch = null;
    batchCount = 0;
    
    if (!DRY_RUN && currentBatch) {
      await currentBatch.commit();
    }
    
    log(`  📝 進行中: ${stats.processedCount.toLocaleString()} 社処理、${stats.updatedCount.toLocaleString()} 社更新`);
  }
  
  // バッチに更新を追加
  async function addToBatch(
    docRef: any,
    prefecture: string
  ) {
    if (!batch) {
      batch = db.batch();
    }
    
    if (!DRY_RUN) {
      batch.update(docRef, {
        prefecture: prefecture,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });
      batchCount++;
    } else {
      // ドライランモードでは更新内容を表示（最初の10件のみ）
      if (stats.updatedCount < 10) {
        const docData = await docRef.get();
        const data = docData.data();
        log(`  🔍 更新予定: ${data?.name || docRef.id}`);
        log(`    現在のprefecture: ${data?.prefecture || "(null)"}`);
        log(`    新しいprefecture: ${prefecture}`);
        log(`    address: ${data?.address || "(null)"}`);
        log(`    headquartersAddress: ${data?.headquartersAddress || "(null)"}`);
      }
      batchCount++;
    }
    
    stats.updatedCount++;
    
    // バッチコミット
    if (batchCount >= BATCH_SIZE) {
      await commitBatch();
    }
  }
  
  try {
    log("📊 ドキュメントを取得中...");
    
    let query = companiesCol.orderBy("__name__").limit(LIMIT || 1000000000);
    let lastDoc: QueryDocumentSnapshot | null = null;
    let hasMore = true;
    
    while (hasMore) {
      // ページネーション用のクエリ
      if (lastDoc) {
        query = companiesCol.orderBy("__name__").startAfter(lastDoc).limit(LIMIT ? Math.min(LIMIT - stats.processedCount, 1000) : 1000);
      } else {
        query = companiesCol.orderBy("__name__").limit(LIMIT ? Math.min(LIMIT, 1000) : 1000);
      }
      
      const snapshot = await query.get();
      
      if (snapshot.empty) {
        hasMore = false;
        break;
      }
      
      for (const doc of snapshot.docs) {
        if (LIMIT && stats.processedCount >= LIMIT) {
          hasMore = false;
          break;
        }
        
        const data = doc.data();
        const address = data.address;
        const headquartersAddress = data.headquartersAddress;
        const currentPrefecture = data.prefecture;
        
        // 都道府県を抽出
        const extractedPrefecture = extractPrefecture(address, headquartersAddress);
        
        if (extractedPrefecture) {
          // 都道府県を抽出できた場合は、既存の値に関わらず更新
          await addToBatch(doc.ref, extractedPrefecture);
        } else {
          // 都道府県を抽出できなかった場合
          stats.skippedCount++;
        }
        
        stats.processedCount++;
        
        // 進捗ログ（10000件ごと）
        if (stats.processedCount % 10000 === 0) {
          log(`  📊 処理中: ${stats.processedCount.toLocaleString()} 社`);
        }
      }
      
      // 最後のドキュメントを記録
      if (snapshot.docs.length > 0) {
        lastDoc = snapshot.docs[snapshot.docs.length - 1];
      } else {
        hasMore = false;
      }
      
      // 制限に達した場合は終了
      if (LIMIT && stats.processedCount >= LIMIT) {
        hasMore = false;
      }
    }
    
    // 残りのバッチをコミット
    await commitBatch();
    
    log("\n" + "=".repeat(60));
    log("処理完了");
    log("=".repeat(60));
    log(`📊 処理済み: ${stats.processedCount.toLocaleString()} 社`);
    log(`✅ 更新: ${stats.updatedCount.toLocaleString()} 社`);
    log(`⏭️  スキップ: ${stats.skippedCount.toLocaleString()} 社（都道府県を抽出できなかった、または既に正しい値）`);
    log(`❌ エラー: ${stats.errorCount.toLocaleString()} 社`);
    
  } catch (error: any) {
    log(`❌ エラー: ${error.message}`);
    console.error(error);
    process.exit(1);
  }
}

// 実行
main();
