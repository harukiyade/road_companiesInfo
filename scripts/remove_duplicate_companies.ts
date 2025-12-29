/* 
  重複企業を削除するスクリプト
  
  重複判定基準:
  - 企業名 + 住所が一致する場合
  - 法人番号がある場合は、法人番号を優先して残す
  - 法人番号がない場合は、最も古いもの（createdAtが最も古い）を残す
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
    npx ts-node scripts/remove_duplicate_companies.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// ==============================
// Firebase 初期化
// ==============================
function initFirebase() {
  if (admin.apps.length > 0) {
    return admin.firestore();
  }

  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS
    ? process.env.GOOGLE_APPLICATION_CREDENTIALS.trim().replace(/\n/g, "").replace(/\r/g, "")
    : null;

  if (serviceAccountPath && !fs.existsSync(serviceAccountPath)) {
    serviceAccountPath = null;
  }

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
      "/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
    ];

    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        break;
      }
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
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

  return admin.firestore();
}

// ==============================
// ヘルパー関数
// ==============================
function normalizeString(str: string | null | undefined): string {
  if (!str) return "";
  return str.trim().replace(/\s+/g, " ").replace(/[　]/g, " ");
}

function createKey(name: string, address: string): string {
  const normalizedName = normalizeString(name);
  const normalizedAddress = normalizeString(address);
  return `${normalizedName}|${normalizedAddress}`;
}

function isValidCorporateNumber(corpNum: string | null | undefined): boolean {
  if (!corpNum) return false;
  const normalized = String(corpNum).trim().replace(/[^0-9]/g, "");
  return /^[0-9]{13}$/.test(normalized);
}

// ==============================
// メイン処理
// ==============================
async function main() {
  console.log("🔍 重複企業を検索して削除します...\n");
  console.log("📋 重複判定基準:");
  console.log("   - 企業名 + 住所が一致する場合");
  console.log("   - 法人番号がある場合は、法人番号を優先して残す");
  console.log("   - 法人番号がない場合は、最も古いもの（createdAtが最も古い）を残す\n");

  const db = initFirebase();
  const companiesCol = db.collection(COLLECTION_NAME);

  console.log("📊 全企業データを取得中...");
  const allDocs = await companiesCol.get();
  console.log(`   総企業数: ${allDocs.size}件\n`);

  // 企業名+住所でグループ化
  const groups: Map<string, Array<{ docId: string; data: any }>> = new Map();

  for (const doc of allDocs.docs) {
    const data = doc.data();
    const name = normalizeString(data.name);
    const address = normalizeString(data.address || data.headquartersAddress);

    if (!name || !address) {
      continue;
    }

    const key = createKey(name, address);
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key)!.push({ docId: doc.id, data });
  }

  console.log(`📊 企業名+住所の組み合わせ数: ${groups.size}件\n`);

  // 重複グループを特定（2件以上あるグループ）
  const duplicateGroups: Array<{ key: string; docs: Array<{ docId: string; data: any }> }> = [];
  for (const [key, docs] of groups.entries()) {
    if (docs.length > 1) {
      duplicateGroups.push({ key, docs });
    }
  }

  console.log(`🔍 重複グループ数: ${duplicateGroups.length}件\n`);

  let totalDuplicates = 0;
  let totalToDelete = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 200;

  const deletionLog: Array<{
    companyName: string;
    address: string;
    keep: string;
    delete: string[];
  }> = [];

  for (let i = 0; i < duplicateGroups.length; i++) {
    const { key, docs } = duplicateGroups[i];
    const [name, address] = key.split("|");

    // 残すドキュメントを決定
    let keepDoc: { docId: string; data: any } | null = null;

    // 1. 法人番号があるものを優先
    const withCorporateNumber = docs.filter(d => {
      const corpNum = d.data.corporateNumber;
      return corpNum && isValidCorporateNumber(corpNum);
    });

    if (withCorporateNumber.length > 0) {
      // 法人番号がある場合は、最初のものを残す（通常は1つのはず）
      keepDoc = withCorporateNumber[0];
    } else {
      // 法人番号がない場合は、createdAtが最も古いものを残す
      const sorted = docs.sort((a, b) => {
        const aTime = a.data.createdAt?.toMillis?.() || 0;
        const bTime = b.data.createdAt?.toMillis?.() || 0;
        return aTime - bTime;
      });
      keepDoc = sorted[0];
    }

    if (!keepDoc) {
      continue;
    }

    // 削除するドキュメント
    const toDelete = docs.filter(d => d.docId !== keepDoc!.docId);
    totalDuplicates += docs.length;
    totalToDelete += toDelete.length;

    deletionLog.push({
      companyName: name,
      address: address,
      keep: keepDoc.docId,
      delete: toDelete.map(d => d.docId),
    });

    // バッチに追加
    for (const docToDelete of toDelete) {
      const docRef = companiesCol.doc(docToDelete.docId);
      batch.delete(docRef);
      batchCount++;

      if (batchCount >= BATCH_LIMIT) {
        await batch.commit();
        batch = db.batch();
        batchCount = 0;
      }
    }

    // 進捗表示
    if ((i + 1) % 100 === 0) {
      process.stdout.write(`\r   進捗: ${i + 1}/${duplicateGroups.length}件の重複グループを処理中...`);
    }
  }

  // 最後のバッチをコミット
  if (batchCount > 0) {
    await batch.commit();
  }

  if (duplicateGroups.length % 100 !== 0) {
    console.log();
  }

  console.log("\n" + "=".repeat(60));
  console.log("📊 重複削除結果サマリー");
  console.log("=".repeat(60));
  console.log(`🔍 重複グループ数: ${duplicateGroups.length}件`);
  console.log(`📊 重複企業総数: ${totalDuplicates}件`);
  console.log(`✅ 残す企業数: ${duplicateGroups.length}件`);
  console.log(`🗑️  削除した企業数: ${totalToDelete}件`);
  console.log("=".repeat(60));

  // 削除ログを保存
  const timestamp = Date.now();
  const logFile = path.join(process.cwd(), `duplicate_deletion_log_${timestamp}.json`);
  fs.writeFileSync(logFile, JSON.stringify({
    timestamp: new Date().toISOString(),
    summary: {
      duplicateGroups: duplicateGroups.length,
      totalDuplicates,
      kept: duplicateGroups.length,
      deleted: totalToDelete,
    },
    deletions: deletionLog,
  }, null, 2), "utf-8");

  console.log(`\n📄 削除ログを保存しました: ${logFile}`);

  // テスト用: 宇都宮塗料工業株式会社の結果を表示
  const testCompany = "宇都宮塗料工業株式会社";
  const testDocs = allDocs.docs.filter(doc => {
    const data = doc.data();
    return normalizeString(data.name) === testCompany;
  });

  if (testDocs.length > 0) {
    console.log(`\n📋 テスト: "${testCompany}" の検索結果`);
    console.log(`   現在の件数: ${testDocs.length}件`);
    for (const doc of testDocs) {
      const data = doc.data();
      console.log(`   - ID: ${doc.id}, 法人番号: ${data.corporateNumber || "なし"}, 住所: ${data.address || data.headquartersAddress || "なし"}`);
    }
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
