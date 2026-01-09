/* 
  テスト用ドキュメントを削除するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/delete_test_companies_by_group.ts [ログファイル名]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference, WriteBatch } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const BATCH_LIMIT = 500;

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

// ログファイルからドキュメントIDを抽出
function parseDocIdsFromLog(logFileName: string): string[] {
  const content = fs.readFileSync(logFileName, "utf8");
  const lines = content.split("\n").filter(l => l.trim());
  const docIds: string[] = [];

  for (const line of lines) {
    // 形式: "group1 - 111.csv - 行1: 17654755738284039 (丹羽興業株式会社)"
    const match = line.match(/行\d+:\s+([^\s]+)\s+\(/);
    if (match) {
      docIds.push(match[1]);
    }
  }

  return docIds;
}

// メイン処理
async function main() {
  const logFileName = process.argv[2] || "created_test_companies_1765475578651.txt";
  
  if (!fs.existsSync(logFileName)) {
    console.error(`❌ エラー: ログファイルが見つかりません: ${logFileName}`);
    process.exit(1);
  }

  console.log(`📄 ログファイル: ${logFileName}\n`);

  // ログファイルからドキュメントIDを抽出
  const docIds = parseDocIdsFromLog(logFileName);
  console.log(`📊 削除対象: ${docIds.length}件のドキュメント\n`);

  if (docIds.length === 0) {
    console.log("⚠️  削除対象のドキュメントが見つかりませんでした");
    return;
  }

  // 削除実行
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  let totalDeleted = 0;

  console.log("🗑️  削除を開始します...\n");

  for (const docId of docIds) {
    try {
      const docRef = companiesCol.doc(docId);
      batch.delete(docRef);
      batchCount++;
      totalDeleted++;

      if (batchCount >= BATCH_LIMIT) {
        await batch.commit();
        console.log(`  ✅ バッチ削除: ${BATCH_LIMIT}件`);
        batch = db.batch();
        batchCount = 0;
      }
    } catch (err: any) {
      console.error(`  ⚠️  削除エラー (${docId}): ${err.message}`);
    }
  }

  // 残りのバッチをコミット
  if (batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 最終バッチ削除: ${batchCount}件`);
  }

  console.log("\n" + "=".repeat(80));
  console.log(`✅ 削除完了: ${totalDeleted}件のドキュメントを削除しました`);
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

