/* 
  ドキュメントIDを数値に変換するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/convert_doc_ids_to_numeric.ts [ログファイル名]
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

// 数値IDを生成（タイムスタンプベース）
function generateNumericId(): string {
  const timestamp = Date.now();
  const random = Math.floor(Math.random() * 10000);
  return `${timestamp}${String(random).padStart(4, "0")}`;
}

// ログファイルからドキュメントIDを抽出
function parseDocIdsFromLog(logFileName: string): Array<{ csvFile: string; rowNum: number; oldDocId: string; companyName: string }> {
  const content = fs.readFileSync(logFileName, "utf8");
  const lines = content.split("\n").filter(l => l.trim());
  const docIds: Array<{ csvFile: string; rowNum: number; oldDocId: string; companyName: string }> = [];

  for (const line of lines) {
    // 形式: "38.csv - 行1: WIFKE1b3thfeOxs3oxGR (丹羽興業株式会社)"
    const match = line.match(/^([^ ]+) - 行(\d+): ([^\s]+) \((.+)\)$/);
    if (match) {
      const [, csvFile, rowNum, oldDocId, companyName] = match;
      docIds.push({
        csvFile,
        rowNum: parseInt(rowNum),
        oldDocId,
        companyName,
      });
    }
  }

  return docIds;
}

// メイン処理
async function main() {
  const logFileName = process.argv[2] || "created_doc_ids_1765467679836.txt";
  
  if (!fs.existsSync(logFileName)) {
    console.error(`❌ エラー: ログファイルが見つかりません: ${logFileName}`);
    process.exit(1);
  }

  console.log(`📄 ログファイル: ${logFileName}\n`);

  // ログファイルからドキュメントIDを抽出
  const docIds = parseDocIdsFromLog(logFileName);
  console.log(`📊 変換対象: ${docIds.length}件のドキュメント\n`);

  if (docIds.length === 0) {
    console.log("⚠️  変換対象のドキュメントが見つかりませんでした");
    return;
  }

  const convertedDocIds: Array<{ csvFile: string; rowNum: number; oldDocId: string; newDocId: string; companyName: string }> = [];
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  let totalConverted = 0;

  console.log("🔄 ドキュメントIDを数値に変換中...\n");

  for (const item of docIds) {
    try {
      // 既に数値IDの場合はスキップ
      if (/^\d+$/.test(item.oldDocId)) {
        console.log(`  ⏭️  行${item.rowNum} (${item.companyName}): 既に数値IDです (${item.oldDocId})`);
        convertedDocIds.push({
          csvFile: item.csvFile,
          rowNum: item.rowNum,
          oldDocId: item.oldDocId,
          newDocId: item.oldDocId,
          companyName: item.companyName,
        });
        continue;
      }

      // 古いドキュメントを取得
      const oldDocRef = companiesCol.doc(item.oldDocId);
      const oldDoc = await oldDocRef.get();

      if (!oldDoc.exists) {
        console.log(`  ⚠️  行${item.rowNum} (${item.companyName}): ドキュメントが見つかりません (${item.oldDocId})`);
        continue;
      }

      const data = oldDoc.data();
      if (!data) {
        console.log(`  ⚠️  行${item.rowNum} (${item.companyName}): データがありません (${item.oldDocId})`);
        continue;
      }

      // 新しい数値IDを生成
      const newDocId = generateNumericId();
      const newDocRef = companiesCol.doc(newDocId);

      // 新しいドキュメントを作成（createdAtを保持）
      const newData = {
        ...data,
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      };

      batch.set(newDocRef, newData);
      batch.delete(oldDocRef);
      batchCount += 2;
      totalConverted++;

      convertedDocIds.push({
        csvFile: item.csvFile,
        rowNum: item.rowNum,
        oldDocId: item.oldDocId,
        newDocId: newDocId,
        companyName: item.companyName,
      });

      console.log(`  ✅ 行${item.rowNum} (${item.companyName}): ${item.oldDocId} → ${newDocId}`);

      if (batchCount >= BATCH_LIMIT) {
        await batch.commit();
        console.log(`  ✅ バッチコミット: ${batchCount}件`);
        batch = db.batch();
        batchCount = 0;
      }
    } catch (err: any) {
      console.error(`  ❌ エラー (${item.companyName}): ${err.message}`);
    }
  }

  // 残りのバッチをコミット
  if (batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 最終バッチコミット: ${batchCount}件`);
  }

  // 結果を表示
  console.log("\n" + "=".repeat(80));
  console.log("📋 変換結果");
  console.log("=".repeat(80));
  console.log();

  // CSVファイルごとにグループ化
  const groupedByFile = new Map<string, typeof convertedDocIds>();
  for (const item of convertedDocIds) {
    if (!groupedByFile.has(item.csvFile)) {
      groupedByFile.set(item.csvFile, []);
    }
    groupedByFile.get(item.csvFile)!.push(item);
  }

  for (const [file, items] of Array.from(groupedByFile.entries()).sort()) {
    console.log(`📄 ${file} (${items.length}件)`);
    console.log("-".repeat(80));
    for (const item of items) {
      if (item.oldDocId === item.newDocId) {
        console.log(`  行${item.rowNum}: ${item.newDocId} (${item.companyName}) - 変更なし`);
      } else {
        console.log(`  行${item.rowNum}: ${item.oldDocId} → ${item.newDocId} (${item.companyName})`);
      }
    }
    console.log();
  }

  // 結果をファイルに保存
  const timestamp = Date.now();
  const outputFile = `converted_doc_ids_${timestamp}.txt`;
  const outputContent = convertedDocIds
    .map(item => `${item.csvFile} - 行${item.rowNum}: ${item.newDocId} (${item.companyName})`)
    .join("\n");
  fs.writeFileSync(outputFile, outputContent, "utf8");

  console.log("=".repeat(80));
  console.log(`✅ 変換完了: ${totalConverted}件のドキュメントIDを数値に変換しました`);
  console.log(`📄 結果ファイル: ${outputFile}`);
  console.log("=".repeat(80));
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

