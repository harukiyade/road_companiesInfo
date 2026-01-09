/* eslint-disable no-console */
/**
 * 法人番号がドキュメントIDになっているドキュメントを数値IDに移行するスクリプト
 * 
 * 実行方法:
 *   FIREBASE_SERVICE_ACCOUNT_KEY=/path/to/serviceAccount.json node --max-old-space-size=8192 node_modules/.bin/ts-node scripts/migrate_corporate_number_to_numeric_id.ts
 * 
 * 環境変数:
 *   - FIREBASE_SERVICE_ACCOUNT_KEY: Firebaseサービスアカウントキーのパス（必須）
 *   - DRY_RUN: trueに設定すると実際の変更を行わずに確認のみ（オプション）
 *   - PARALLEL_BATCHES: 並列実行するバッチ数（デフォルト: 5）
 *   - PARALLEL_WEBINFO: webInfo更新の並列数（デフォルト: 10）
 * 
 * メモリ最適化:
 *   - 大量のドキュメントを処理する場合、Node.jsのヒープサイズを増やすことを推奨
 *   - --max-old-space-size=8192 (8GB) または --max-old-space-size=16384 (16GB)
 * 
 * 例:
 *   # Dry-runモードで確認
 *   FIREBASE_SERVICE_ACCOUNT_KEY=./serviceAccount.json DRY_RUN=true node --max-old-space-size=8192 node_modules/.bin/ts-node scripts/migrate_corporate_number_to_numeric_id.ts
 * 
 *   # 実際に実行（並列数を増やす、メモリも増やす）
 *   FIREBASE_SERVICE_ACCOUNT_KEY=./serviceAccount.json PARALLEL_BATCHES=10 node --max-old-space-size=16384 node_modules/.bin/ts-node scripts/migrate_corporate_number_to_numeric_id.ts
 */
import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

// Firebase初期化
const serviceAccountKeyPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
if (!serviceAccountKeyPath) {
  console.error("❌ FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません");
  console.error("\n実行方法:");
  console.error("  FIREBASE_SERVICE_ACCOUNT_KEY=/path/to/serviceAccount.json npx ts-node scripts/migrate_corporate_number_to_numeric_id.ts");
  console.error("\n例:");
  console.error("  FIREBASE_SERVICE_ACCOUNT_KEY=./serviceAccount.json npx ts-node scripts/migrate_corporate_number_to_numeric_id.ts");
  process.exit(1);
}

if (!fs.existsSync(serviceAccountKeyPath)) {
  console.error(`❌ エラー: サービスアカウントキーファイルが存在しません: ${serviceAccountKeyPath}`);
  process.exit(1);
}

try {
  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountKeyPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
  console.log("✅ Firebase初期化完了");
} catch (error: any) {
  console.error("❌ Firebase初期化エラー:", error.message);
  process.exit(1);
}

const db = admin.firestore();

// 並列処理の設定
const PARALLEL_BATCHES = parseInt(process.env.PARALLEL_BATCHES || "5", 10); // 同時実行バッチ数
const PARALLEL_WEBINFO = parseInt(process.env.PARALLEL_WEBINFO || "10", 10); // webInfo更新の並列数

/**
 * ドキュメントIDが法人番号かどうかを判定
 * 法人番号は13桁の数字
 */
function isCorporateNumber(docId: string): boolean {
  // 13桁の数字かどうかをチェック
  return /^\d{13}$/.test(docId);
}

/**
 * 配列をチャンクに分割
 */
function chunkArray<T>(array: T[], chunkSize: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
}

/**
 * スリープ関数
 */
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * 既存の最大数値IDを取得
 */
async function getMaxNumericId(): Promise<number> {
  let maxId = 0;
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
  const BATCH_SIZE = 5000;
  let processedCount = 0;

  console.log("既存の最大数値IDを取得中...");
  const startTime = Date.now();

  while (true) {
    let query = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(BATCH_SIZE);
    
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();
    if (snapshot.empty) {
      break;
    }

    for (const doc of snapshot.docs) {
      const docId = doc.id;
      processedCount++;
      
      // 数値IDかどうかをチェック（法人番号でない場合）
      if (!isCorporateNumber(docId)) {
        const numId = parseInt(docId, 10);
        if (!isNaN(numId) && numId > maxId) {
          maxId = numId;
        }
      }
    }

    // 進捗表示（10,000件ごと）
    if (processedCount % 10000 === 0) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
      process.stdout.write(`\r  処理中: ${processedCount.toLocaleString()} 件、最大ID: ${maxId}、経過時間: ${elapsed}秒`);
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
  console.log(`\r  処理完了: ${processedCount.toLocaleString()} 件、最大ID: ${maxId}、経過時間: ${elapsed}秒`);
  console.log(`✅ 最大数値ID: ${maxId}`);
  return maxId;
}

/**
 * メイン処理: 法人番号がドキュメントIDになっているドキュメントを数値IDに移行
 */
async function migrateCorporateNumberToNumericId() {
  try {
    // dry-runモードの確認
    const dryRun = process.env.DRY_RUN === "true" || process.argv.includes("--dry-run");
    
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const logDir = path.join(process.cwd(), "out");
    if (!fs.existsSync(logDir)) {
      fs.mkdirSync(logDir, { recursive: true });
    }

    const logPath = path.join(logDir, `migrate_corporate_number_${timestamp}.log`);
    const logStream = fs.createWriteStream(logPath, { encoding: "utf8" });

    const writeLog = (message: string) => {
      const logMessage = `[${new Date().toISOString()}] ${message}`;
      console.log(message);
      logStream.write(logMessage + "\n");
    };

    if (dryRun) {
      writeLog("🔍 DRY-RUNモード: 実際の変更は行いません");
    }
    writeLog("🚀 法人番号→数値ID移行処理を開始します");

    // 既存の最大数値IDを取得
    let nextNumericId = await getMaxNumericId() + 1;
    writeLog(`次の数値ID: ${nextNumericId}`);

    // 法人番号がドキュメントIDになっているドキュメントを特定（メモリ効率化：IDのみ保持）
    const documentIdsToMigrate: string[] = [];

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    const BATCH_SIZE = 1000;
    let totalProcessed = 0;

    writeLog("\n法人番号がドキュメントIDになっているドキュメントを検索中...");
    writeLog("（メモリ効率化のため、IDのみを保持します）");
    const searchStartTime = Date.now();

    while (true) {
      let query = db
        .collection("companies_new")
        .orderBy(admin.firestore.FieldPath.documentId())
        .limit(BATCH_SIZE);

      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }

      for (const doc of snapshot.docs) {
        const docId = doc.id;
        totalProcessed++;

        // ドキュメントIDが法人番号かどうかを判定（13桁の数字）
        if (isCorporateNumber(docId)) {
          // ドキュメントIDが法人番号形式の場合、移行対象とする（IDのみ保持）
          documentIdsToMigrate.push(docId);
        }
      }

      // 進捗表示（5,000件ごと）
      if (totalProcessed % 5000 === 0) {
        const elapsed = ((Date.now() - searchStartTime) / 1000).toFixed(1);
        process.stdout.write(`\r  処理中: ${totalProcessed.toLocaleString()} 件、移行対象: ${documentIdsToMigrate.length.toLocaleString()} 件、経過時間: ${elapsed}秒`);
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }

    const searchElapsed = ((Date.now() - searchStartTime) / 1000).toFixed(1);
    console.log(`\r  処理完了: ${totalProcessed.toLocaleString()} 件、移行対象: ${documentIdsToMigrate.length.toLocaleString()} 件、経過時間: ${searchElapsed}秒`);

    writeLog(`\n✅ 検索完了: 総処理数 ${totalProcessed} 件、移行対象 ${documentIdsToMigrate.length} 件`);

    if (documentIdsToMigrate.length === 0) {
      writeLog("移行対象のドキュメントがありませんでした");
      logStream.end();
      return;
    }

    // 移行対象のサンプルを表示（実際のデータを取得）
    writeLog(`\n移行対象のサンプル（最初の10件）:`);
    for (let i = 0; i < Math.min(10, documentIdsToMigrate.length); i++) {
      const oldDocId = documentIdsToMigrate[i];
      try {
        const doc = await db.collection("companies_new").doc(oldDocId).get();
        const data = doc.data();
        writeLog(`  ${i + 1}. ${oldDocId} → (新ID) - ${data?.name || "名前なし"}`);
      } catch (error) {
        writeLog(`  ${i + 1}. ${oldDocId} → (取得エラー)`);
      }
    }

    if (dryRun) {
      writeLog(`\n✅ DRY-RUN完了: ${documentIdsToMigrate.length} 件の移行対象を検出しました`);
      writeLog(`実際に移行するには、DRY_RUN=false を設定するか --dry-run フラグを外して実行してください`);
      logStream.end();
      return;
    }

    // 移行処理を実行
    writeLog(`\n移行処理を開始します（${documentIdsToMigrate.length} 件）...`);
    writeLog(`並列バッチ数: ${PARALLEL_BATCHES}`);

    // 古いIDから新しいIDへのマッピングを作成
    const idMapping = new Map<string, string>();

    // 事前に全ての新しいIDを割り当て（重複チェックのため）
    let currentNumericId = nextNumericId;
    const idAssignments = new Map<string, string>();

    // 既存の数値IDをチェックして重複を避ける
    writeLog("既存の数値IDをチェック中...");
    const existingIds = new Set<string>();
    let checkLastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    const CHECK_BATCH_SIZE = 5000;
    let checkProcessed = 0;
    const checkStartTime = Date.now();

    while (true) {
      let checkQuery = db
        .collection("companies_new")
        .orderBy(admin.firestore.FieldPath.documentId())
        .limit(CHECK_BATCH_SIZE);
      
      if (checkLastDoc) {
        checkQuery = checkQuery.startAfter(checkLastDoc);
      }

      const checkSnapshot = await checkQuery.get();
      if (checkSnapshot.empty) {
        break;
      }

      for (const doc of checkSnapshot.docs) {
        const docId = doc.id;
        if (!isCorporateNumber(docId)) {
          existingIds.add(docId);
        }
        checkProcessed++;
      }

      // 進捗表示（10,000件ごと）
      if (checkProcessed % 10000 === 0) {
        const elapsed = ((Date.now() - checkStartTime) / 1000).toFixed(1);
        process.stdout.write(`\r  チェック中: ${checkProcessed.toLocaleString()} 件、既存ID: ${existingIds.size.toLocaleString()} 件、経過時間: ${elapsed}秒`);
      }

      checkLastDoc = checkSnapshot.docs[checkSnapshot.docs.length - 1];
    }

    const checkElapsed = ((Date.now() - checkStartTime) / 1000).toFixed(1);
    console.log(`\r  チェック完了: ${checkProcessed.toLocaleString()} 件、既存ID: ${existingIds.size.toLocaleString()} 件、経過時間: ${checkElapsed}秒`);
    writeLog(`✅ 既存IDチェック完了: ${checkProcessed} 件処理、${existingIds.size} 件の既存IDを検出`);

    // 新しいIDを割り当て（重複チェック付き）
    const usedIds = new Set<string>();
    for (const oldDocId of documentIdsToMigrate) {
      let newDocId: string;
      let attempts = 0;
      const maxAttempts = 1000; // 無限ループ防止
      
      do {
        newDocId = currentNumericId.toString();
        currentNumericId++;
        attempts++;
        
        if (attempts > maxAttempts) {
          throw new Error(`ID割り当てに失敗しました: ${oldDocId} (試行回数超過)`);
        }
      } while (existingIds.has(newDocId) || usedIds.has(newDocId));
      
      usedIds.add(newDocId);
      idAssignments.set(oldDocId, newDocId);
      idMapping.set(oldDocId, newDocId);
    }

    writeLog(`✅ ID割り当て完了: ${idAssignments.size} 件`);

    const BATCH_WRITE_SIZE = 50; // Firestoreのバッチ制限（500オペレーション制限を考慮）
    const batches = chunkArray(documentIdsToMigrate, BATCH_WRITE_SIZE);
    let migratedCount = 0;
    let errorCount = 0;
    const errors: Array<{ oldDocId: string; error: string }> = [];

    // 並列処理でバッチを実行
    const processBatch = async (
      batch: string[], // IDのみの配列
      batchIndex: number
    ): Promise<{ success: number; errors: number }> => {
      const writeBatch = db.batch();
      let batchSuccess = 0;
      let batchErrors = 0;
      let writeCount = 0; // バッチに追加された書き込み操作の数

      // 並列で存在チェックとデータ取得を実行（高速化）
      const checkPromises = batch.map(async (oldDocId) => {
        const newDocId = idAssignments.get(oldDocId);
        if (!newDocId) {
          return { oldDocId, error: `ID割り当てが見つかりません: ${oldDocId}`, data: null, newDocId: null };
        }

        try {
          // 並列で存在チェックとデータ取得
          const [newDocCheck, oldDocCheck] = await Promise.all([
            db.collection("companies_new").doc(newDocId).get(),
            db.collection("companies_new").doc(oldDocId).get(),
          ]);

          if (newDocCheck.exists) {
            return { oldDocId, error: `新しいIDが既に存在します: ${newDocId}`, data: null, newDocId: null };
          }

          if (!oldDocCheck.exists) {
            return { oldDocId, error: `古いドキュメントが存在しません: ${oldDocId}`, data: null, newDocId: null };
          }

          return { oldDocId, error: null, data: oldDocCheck.data(), newDocId };
        } catch (error: any) {
          return { oldDocId, error: error.message, data: null, newDocId: null };
        }
      });

      const checkResults = await Promise.all(checkPromises);

      // チェック結果に基づいてバッチを作成
      for (const result of checkResults) {
        if (result.error || !result.data || !result.newDocId) {
          batchErrors++;
          errors.push({ oldDocId: result.oldDocId, error: result.error || "不明なエラー" });
          writeLog(`  ❌ エラー [${result.oldDocId}]: ${result.error || "不明なエラー"}`);
          continue;
        }

        try {
          const newDocRef = db.collection("companies_new").doc(result.newDocId);
          const oldDocRef = db.collection("companies_new").doc(result.oldDocId);

          // 新しいドキュメントを作成
          writeBatch.set(newDocRef, {
            ...result.data,
            companyId: result.newDocId, // companyIdフィールドも更新
            updatedAt: new Date().toISOString(),
          });
          writeCount++;

          // 古いドキュメントを削除
          writeBatch.delete(oldDocRef);
          writeCount++;
        } catch (error: any) {
          batchErrors++;
          errors.push({ oldDocId: result.oldDocId, error: error.message });
          writeLog(`  ❌ エラー [${result.oldDocId}]: ${error.message}`);
        }
      }

      if (writeCount > 0) {
        try {
          await writeBatch.commit();
          batchSuccess = batch.length - batchErrors;
          const globalIndex = batchIndex * BATCH_WRITE_SIZE;
          writeLog(`  ✅ バッチ[${batchIndex + 1}/${batches.length}] 完了: ${batchSuccess} 件成功, ${batchErrors} 件エラー (累計: ${globalIndex + batch.length}/${documentIdsToMigrate.length})`);
        } catch (error: any) {
          batchErrors = batch.length;
          errors.push({ oldDocId: `batch_${batchIndex}`, error: error.message });
          writeLog(`  ❌ バッチ[${batchIndex + 1}] コミットエラー: ${error.message}`);
        }
      }

      return { success: batchSuccess, errors: batchErrors };
    };

    // 並列処理でバッチを実行
    for (let i = 0; i < batches.length; i += PARALLEL_BATCHES) {
      const parallelBatches = batches.slice(i, i + PARALLEL_BATCHES);
      
      const results = await Promise.all(
        parallelBatches.map((batch, idx) => processBatch(batch, i + idx))
      );

      for (const result of results) {
        migratedCount += result.success;
        errorCount += result.errors;
      }

      // レート制限対策（並列実行後は少し長めに待機）
      if (i + PARALLEL_BATCHES < batches.length) {
        await sleep(200);
      }
    }

    // companies_webInfoコレクションの参照も更新（並列処理）
    writeLog("\ncompanies_webInfoコレクションの参照を更新中...");
    writeLog(`並列webInfo更新数: ${PARALLEL_WEBINFO}`);
    let webInfoUpdated = 0;
    const webInfoErrors: Array<{ oldDocId: string; error: string }> = [];

    const updateWebInfo = async (oldDocId: string, newDocId: string): Promise<number> => {
      let updated = 0;
      try {
        // 古いIDでwebInfoを検索（ドキュメントIDとして）
        const webInfoDocRef = db.collection("companies_webInfo").doc(oldDocId);
        const webInfoDoc = await webInfoDocRef.get();

        if (webInfoDoc.exists) {
          // ドキュメントIDを変更するため、新しいドキュメントを作成して古いものを削除
          const webInfoData = webInfoDoc.data();
          const newWebInfoRef = db.collection("companies_webInfo").doc(newDocId);
          
          // 新しいドキュメントが既に存在しないか確認
          const newWebInfoCheck = await newWebInfoRef.get();
          if (!newWebInfoCheck.exists) {
            await newWebInfoRef.set({
              ...webInfoData,
              companyId: newDocId,
              updatedAt: new Date().toISOString(),
            });
            
            await webInfoDocRef.delete();
            updated++;
          }
        }

        // companyIdフィールドで検索
        const webInfoQuery = await db
          .collection("companies_webInfo")
          .where("companyId", "==", oldDocId)
          .limit(100) // 念のため制限
          .get();

        if (!webInfoQuery.empty) {
          const updateBatch = db.batch();
          let updateCount = 0;
          
          for (const doc of webInfoQuery.docs) {
            updateBatch.update(doc.ref, {
              companyId: newDocId,
              updatedAt: new Date().toISOString(),
            });
            updated++;
            updateCount++;
          }
          
          if (updateCount > 0) {
            await updateBatch.commit();
          }
        }
      } catch (error: any) {
        webInfoErrors.push({ oldDocId, error: error.message });
        writeLog(`  ❌ webInfo更新エラー [${oldDocId}]: ${error.message}`);
      }
      return updated;
    };

    // 並列処理でwebInfoを更新
    const idMappingEntries = Array.from(idMapping.entries());
    const webInfoChunks = chunkArray(idMappingEntries, PARALLEL_WEBINFO);

    for (const chunk of webInfoChunks) {
      const results = await Promise.all(
        chunk.map(([oldDocId, newDocId]) => updateWebInfo(oldDocId, newDocId))
      );
      
      for (const result of results) {
        webInfoUpdated += result;
      }

      // レート制限対策
      if (webInfoChunks.indexOf(chunk) < webInfoChunks.length - 1) {
        await sleep(100);
      }
    }

    writeLog(`\n✅ 移行処理完了:`);
    writeLog(`  移行成功: ${migratedCount} 件`);
    writeLog(`  エラー: ${errorCount} 件`);
    writeLog(`  webInfo更新: ${webInfoUpdated} 件`);
    
    if (errors.length > 0) {
      writeLog(`\n⚠️  エラー詳細 (最初の20件):`);
      for (let i = 0; i < Math.min(20, errors.length); i++) {
        writeLog(`  ${i + 1}. [${errors[i].oldDocId}]: ${errors[i].error}`);
      }
      if (errors.length > 20) {
        writeLog(`  ... 他 ${errors.length - 20} 件のエラー`);
      }
    }

    if (webInfoErrors.length > 0) {
      writeLog(`\n⚠️  webInfo更新エラー (最初の10件):`);
      for (let i = 0; i < Math.min(10, webInfoErrors.length); i++) {
        writeLog(`  ${i + 1}. [${webInfoErrors[i].oldDocId}]: ${webInfoErrors[i].error}`);
      }
    }

    writeLog(`\nログファイル: ${logPath}`);
    
    // エラーがある場合はCSVファイルにも出力
    if (errors.length > 0 || webInfoErrors.length > 0) {
      const errorCsvPath = path.join(logDir, `migrate_errors_${timestamp}.csv`);
      const errorCsvStream = fs.createWriteStream(errorCsvPath, { encoding: "utf8" });
      errorCsvStream.write("type,oldDocId,error\n");
      
      for (const err of errors) {
        errorCsvStream.write(`migration,"${err.oldDocId}","${err.error.replace(/"/g, '""')}"\n`);
      }
      
      for (const err of webInfoErrors) {
        errorCsvStream.write(`webinfo,"${err.oldDocId}","${err.error.replace(/"/g, '""')}"\n`);
      }
      
      errorCsvStream.end();
      writeLog(`エラー詳細CSV: ${errorCsvPath}`);
    }

    logStream.end();
  } catch (error) {
    console.error("❌ エラー:", error);
    process.exit(1);
  }
}

migrateCorporateNumberToNumericId();
