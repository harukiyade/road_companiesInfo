/* eslint-disable no-console */
/**
 * companies_newコレクションでnameフィールドに値がないドキュメントを全て削除するスクリプト
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

// ログファイルのパス
const logDir = path.join(process.cwd(), "out");
if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}
const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
const logFile = path.join(logDir, `delete_documents_without_name_${timestamp}.log`);

// ログ出力関数
function writeLog(message: string) {
  const logMessage = `[${new Date().toISOString()}] ${message}`;
  console.log(message);
  fs.appendFileSync(logFile, logMessage + "\n");
}

// Firebase Admin SDK 初期化
const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
if (!serviceAccountPath) {
  console.error("❌ FIREBASE_SERVICE_ACCOUNT_KEY環境変数が設定されていません");
  process.exit(1);
}

try {
  const serviceAccount = require(serviceAccountPath);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
  writeLog("✅ Firebase初期化完了");
} catch (error: any) {
  writeLog(`❌ Firebase初期化エラー: ${error.message}`);
  process.exit(1);
}

const db = admin.firestore();

/**
 * nameフィールドに値がないかチェック
 */
function hasNoName(data: admin.firestore.DocumentData | undefined): boolean {
  if (!data) return true;
  const name = data.name;
  return name === null || name === undefined || name === "";
}

/**
 * メイン処理: nameフィールドに値がないドキュメントを削除
 */
async function deleteDocumentsWithoutName() {
  try {
    writeLog("companies_newコレクションでnameフィールドに値がないドキュメントを検索中...");
    writeLog(`ログファイル: ${logFile}`);

    const BATCH_SIZE = 500; // Firestoreのバッチ制限
    const DELETE_BATCH_SIZE = 500; // 削除バッチサイズ（Firestoreの制限）
    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;
    let totalDeleted = 0;
    const deletedIds: string[] = [];

    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }

      // nameフィールドに値がないドキュメントをフィルタリング
      const docsToDelete = snapshot.docs.filter((doc) => {
        const data = doc.data();
        return hasNoName(data);
      });

      if (docsToDelete.length > 0) {
        writeLog(`バッチ取得: ${snapshot.size} 件, 削除対象: ${docsToDelete.length} 件`);

        // バッチ削除
        let batch = db.batch();
        let batchCount = 0;

        for (const doc of docsToDelete) {
          batch.delete(doc.ref);
          batchCount++;
          deletedIds.push(doc.id);

          // Firestoreのバッチ制限に達したらコミット
          if (batchCount >= DELETE_BATCH_SIZE) {
            await batch.commit();
            totalDeleted += batchCount;
            batch = db.batch(); // 新しいバッチを作成
            batchCount = 0;
            // レート制限対策で少し待機
            await new Promise((resolve) => setTimeout(resolve, 100));
          }
        }

        // 残りのバッチをコミット
        if (batchCount > 0) {
          await batch.commit();
          totalDeleted += batchCount;
        }
      }

      totalProcessed += snapshot.size;
      lastDoc = snapshot.docs[snapshot.docs.length - 1];

      // 5000件ごとにログ出力
      if (totalProcessed % 5000 === 0 || docsToDelete.length > 0) {
        writeLog(`累計処理: ${totalProcessed} 件, 累計削除: ${totalDeleted} 件`);
      }
    }

    writeLog("\n" + "=".repeat(60));
    writeLog("✅ 処理完了");
    writeLog(`  総処理件数: ${totalProcessed} 件`);
    writeLog(`  削除件数: ${totalDeleted} 件`);
    writeLog("=".repeat(60));

    if (deletedIds.length > 0) {
      writeLog(`\n削除されたドキュメントID（最初の10件）:`);
      deletedIds.slice(0, 10).forEach((id) => {
        writeLog(`  - ${id}`);
      });
      if (deletedIds.length > 10) {
        writeLog(`  ... 他 ${deletedIds.length - 10} 件`);
      }
    }
    
    writeLog(`\nログファイル: ${logFile}`);

    process.exit(0);
  } catch (error: any) {
    writeLog(`❌ エラー: ${error.message}`);
    writeLog(error.stack || String(error));
    process.exit(1);
  }
}

deleteDocumentsWithoutName();
