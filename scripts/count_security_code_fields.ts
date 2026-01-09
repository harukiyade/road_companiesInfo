/* eslint-disable no-console */

/**
 * scripts/count_security_code_fields.ts
 *
 * ✅ 目的
 * - companies_newコレクションのsecurityCodeフィールドに値が入っている会社数をカウント
 * - companies_newコレクションのsecuritiesCodeフィールドに値が入っている会社数をカウント
 * - それぞれのドキュメントIDを出力
 * - 関連会社フィールド（relatedCompanies）に値が入っている会社数をカウント
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

// ------------------------------
// Firebase Admin SDK 初期化
// ------------------------------
if (!admin.apps.length) {
  try {
    const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;

    if (!serviceAccountPath) {
      console.error("❌ エラー: FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません。");
      process.exit(1);
    }

    if (!fs.existsSync(serviceAccountPath)) {
      console.error(`❌ エラー: サービスアカウントキーファイルが存在しません: ${serviceAccountPath}`);
      process.exit(1);
    }

    const serviceAccount = JSON.parse(
      fs.readFileSync(serviceAccountPath, "utf8")
    );

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: "albert-ma",
    });

    console.log("[Firebase初期化] ✅ 初期化が完了しました");
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", error);
    process.exit(1);
  }
}

const db = admin.firestore();

// ------------------------------
// メイン処理
// ------------------------------

async function countSecurityCodeFields() {
  try {
    console.log("securityCode/securitiesCodeフィールドのカウント開始...");

    const securityCodeDocIds: string[] = [];
    const securitiesCodeDocIds: string[] = [];
    const relatedCompaniesDocIds: string[] = [];

    // 出力ディレクトリを作成
    const outDir = path.join(process.cwd(), "out");
    if (!fs.existsSync(outDir)) {
      fs.mkdirSync(outDir, { recursive: true });
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const logFilePath = path.join(outDir, `count_security_code_fields_${timestamp}.log`);
    const logStream = fs.createWriteStream(logFilePath, { encoding: "utf8", flags: "w" });

    logStream.write(`# securityCode/securitiesCodeフィールドカウントログ\n`);
    logStream.write(`# 開始時刻: ${new Date().toISOString()}\n`);
    logStream.write(`#\n`);

    const BATCH_SIZE = 1000;
    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;

    /**
     * リトライ付きクエリ実行
     */
    async function executeQueryWithRetry(
      query: admin.firestore.Query,
      retryCount: number = 0
    ): Promise<admin.firestore.QuerySnapshot> {
      try {
        return await query.get();
      } catch (error: any) {
        if (
          (error.code === 14 || error.code === 4 || error.code === 13) &&
          retryCount < 3
        ) {
          const delay = 5000 * (retryCount + 1);
          console.warn(
            `⚠️  クエリエラー (code: ${error.code}), ${delay}ms後にリトライします (${retryCount + 1}/3)...`
          );
          await new Promise((resolve) => setTimeout(resolve, delay));
          return executeQueryWithRetry(query, retryCount + 1);
        }
        throw error;
      }
    }

    /**
     * 値が空でないかチェック
     */
    function hasValue(value: any): boolean {
      if (value === null || value === undefined) return false;
      if (typeof value === "string" && value.trim() === "") return false;
      if (Array.isArray(value) && value.length === 0) return false;
      if (typeof value === "object" && Object.keys(value).length === 0) return false;
      return true;
    }

    console.log("データ収集を開始...");
    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      let snapshot: admin.firestore.QuerySnapshot;
      try {
        snapshot = await executeQueryWithRetry(query);
      } catch (error: any) {
        console.error(`❌ クエリエラー:`, error.message);
        logStream.write(`ERROR: ${error.message}\n`);
        break;
      }

      if (snapshot.empty) {
        break;
      }

      // 進捗表示（10000件ごと）
      if (totalProcessed % 10000 === 0 && totalProcessed > 0) {
        console.log(`処理済み: ${totalProcessed} 件`);
      }

      // 各ドキュメントをチェック
      for (const doc of snapshot.docs) {
        const data = doc.data();
        const docId = doc.id;

        // securityCodeチェック
        if (hasValue(data.securityCode)) {
          securityCodeDocIds.push(docId);
        }

        // securitiesCodeチェック
        if (hasValue(data.securitiesCode)) {
          securitiesCodeDocIds.push(docId);
        }

        // relatedCompaniesチェック
        if (hasValue(data.relatedCompanies)) {
          relatedCompaniesDocIds.push(docId);
        }

        totalProcessed++;
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
      logStream.write(`# Progress: 処理済み: ${totalProcessed} 件 at ${new Date().toISOString()}\n`);
    }

    console.log(`\nデータ収集完了: ${totalProcessed} 件`);

    // 結果をCSVに出力
    const csvPath = path.join(outDir, `security_code_fields_count_${timestamp}.csv`);
    const csvStream = fs.createWriteStream(csvPath, { encoding: "utf8", flags: "w" });

    // CSVヘッダー
    csvStream.write("フィールド名,件数,ドキュメントID一覧\n");

    // securityCode
    csvStream.write(`securityCode,${securityCodeDocIds.length},"${securityCodeDocIds.join(", ")}"\n`);

    // securitiesCode
    csvStream.write(`securitiesCode,${securitiesCodeDocIds.length},"${securitiesCodeDocIds.join(", ")}"\n`);

    // relatedCompanies
    csvStream.write(`relatedCompanies,${relatedCompaniesDocIds.length},"${relatedCompaniesDocIds.join(", ")}"\n`);

    // CSVストリームを確実に閉じる
    await new Promise<void>((resolve, reject) => {
      csvStream.on("finish", resolve);
      csvStream.on("error", reject);
      csvStream.end();
    });

    // 詳細なドキュメントIDリストを別ファイルに出力
    const securityCodeListPath = path.join(outDir, `securityCode_docIds_${timestamp}.txt`);
    fs.writeFileSync(securityCodeListPath, securityCodeDocIds.join("\n"), "utf8");

    const securitiesCodeListPath = path.join(outDir, `securitiesCode_docIds_${timestamp}.txt`);
    fs.writeFileSync(securitiesCodeListPath, securitiesCodeDocIds.join("\n"), "utf8");

    const relatedCompaniesListPath = path.join(outDir, `relatedCompanies_docIds_${timestamp}.txt`);
    fs.writeFileSync(relatedCompaniesListPath, relatedCompaniesDocIds.join("\n"), "utf8");

    logStream.write(`# 処理完了: ${new Date().toISOString()}\n`);
    logStream.end();

    // サマリー出力
    console.log(`\n✅ 処理完了`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`\n📊 集計結果:`);
    console.log(`  securityCode: ${securityCodeDocIds.length} 社`);
    console.log(`  securitiesCode: ${securitiesCodeDocIds.length} 社`);
    console.log(`  relatedCompanies: ${relatedCompaniesDocIds.length} 社`);
    console.log(`\n📁 出力ファイル:`);
    console.log(`  - ${csvPath} (サマリーCSV)`);
    console.log(`  - ${securityCodeListPath} (securityCodeドキュメントID一覧)`);
    console.log(`  - ${securitiesCodeListPath} (securitiesCodeドキュメントID一覧)`);
    console.log(`  - ${relatedCompaniesListPath} (relatedCompaniesドキュメントID一覧)`);
    console.log(`  - ${logFilePath} (処理ログ)`);

    // ドキュメントIDの最初の10件を表示
    if (securityCodeDocIds.length > 0) {
      console.log(`\n【securityCode】最初の10件のドキュメントID:`);
      securityCodeDocIds.slice(0, 10).forEach((id, index) => {
        console.log(`  ${index + 1}. ${id}`);
      });
      if (securityCodeDocIds.length > 10) {
        console.log(`  ... 他 ${securityCodeDocIds.length - 10} 件`);
      }
    }

    if (securitiesCodeDocIds.length > 0) {
      console.log(`\n【securitiesCode】最初の10件のドキュメントID:`);
      securitiesCodeDocIds.slice(0, 10).forEach((id, index) => {
        console.log(`  ${index + 1}. ${id}`);
      });
      if (securitiesCodeDocIds.length > 10) {
        console.log(`  ... 他 ${securitiesCodeDocIds.length - 10} 件`);
      }
    }

    if (relatedCompaniesDocIds.length > 0) {
      console.log(`\n【relatedCompanies】最初の10件のドキュメントID:`);
      relatedCompaniesDocIds.slice(0, 10).forEach((id, index) => {
        console.log(`  ${index + 1}. ${id}`);
      });
      if (relatedCompaniesDocIds.length > 10) {
        console.log(`  ... 他 ${relatedCompaniesDocIds.length - 10} 件`);
      }
    }

  } catch (error: any) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error("❌ 重大エラー:", errorMsg);
    console.error(error);
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
countSecurityCodeFields()
  .then(() => {
    console.log("\n処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
