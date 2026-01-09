/* eslint-disable no-console */

/**
 * scripts/list_url_fields.ts
 *
 * ✅ 目的
 * - companies_newコレクション内で、URLが入っているフィールドを全て洗い出す
 * - 想定では企業HPと問い合わせフォームのURLのはず
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 *
 * ✅ 出力
 * - out/url_fields_XXXX.csv に出力
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

/**
 * 値がURLかどうかを判定
 */
function isUrl(value: any): boolean {
  if (typeof value !== "string") {
    return false;
  }

  const trimmed = value.trim();
  
  // http:// または https:// で始まる
  if (/^https?:\/\//i.test(trimmed)) {
    return true;
  }

  // www. で始まる（企業HPの可能性が高い）
  if (/^www\./i.test(trimmed)) {
    return true;
  }

  return false;
}

/**
 * オブジェクトや配列を再帰的に探索してURLを検出
 */
function findUrlsInValue(value: any, fieldPath: string = ""): Array<{ path: string; url: string }> {
  const results: Array<{ path: string; url: string }> = [];

  if (value === null || value === undefined) {
    return results;
  }

  if (typeof value === "string") {
    if (isUrl(value)) {
      results.push({ path: fieldPath, url: value });
    }
    return results;
  }

  if (Array.isArray(value)) {
    value.forEach((item, index) => {
      const itemPath = fieldPath ? `${fieldPath}[${index}]` : `[${index}]`;
      results.push(...findUrlsInValue(item, itemPath));
    });
    return results;
  }

  if (typeof value === "object") {
    for (const [key, val] of Object.entries(value)) {
      const newPath = fieldPath ? `${fieldPath}.${key}` : key;
      results.push(...findUrlsInValue(val, newPath));
    }
    return results;
  }

  return results;
}

/**
 * メイン処理
 */
async function main() {
  console.log("================================================================================\n");
  console.log("URLフィールドの洗い出しを開始...");
  console.log("================================================================================\n");

  const outputDir = path.join(process.cwd(), "out");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  // CSVファイルの準備（タイムスタンプ付き）
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const csvFilePath = path.join(outputDir, `url_fields_${timestamp}.csv`);

  // CSVヘッダー
  const csvHeaders = ["companyId", "companyName", "fieldName", "fieldPath", "url"];
  fs.writeFileSync(csvFilePath, csvHeaders.join(",") + "\n", { encoding: "utf8" });

  let totalCompanies = 0;
  let companiesWithUrls = 0;
  let totalUrlCount = 0;
  const fieldNameStats = new Map<string, number>(); // フィールド名ごとの出現回数
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;

  console.log("Firestoreから企業データを取得中...\n");

  try {
    while (true) {
      let query: admin.firestore.Query = db
        .collection("companies_new")
        .orderBy(admin.firestore.FieldPath.documentId())
        .limit(500);

      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();

      if (snapshot.empty) {
        break;
      }

      for (const doc of snapshot.docs) {
        totalCompanies++;
        const data = doc.data();
        const companyId = doc.id;
        const companyName = data.name || "";

        let hasUrl = false;

        // 全フィールドを探索
        for (const [fieldName, fieldValue] of Object.entries(data)) {
          const urls = findUrlsInValue(fieldValue, fieldName);

          for (const { path: fieldPath, url } of urls) {
            hasUrl = true;
            totalUrlCount++;

            // フィールド名の統計を更新
            const baseFieldName = fieldPath.split(".")[0].split("[")[0];
            fieldNameStats.set(baseFieldName, (fieldNameStats.get(baseFieldName) || 0) + 1);

            // CSVに出力
            const csvRow = [
              companyId,
              `"${companyName.replace(/"/g, '""')}"`,
              baseFieldName,
              fieldPath,
              `"${url.replace(/"/g, '""')}"`,
            ];
            fs.appendFileSync(csvFilePath, csvRow.join(",") + "\n", { encoding: "utf8" });
          }
        }

        if (hasUrl) {
          companiesWithUrls++;
        }

        // 進捗表示（1000件ごと）
        if (totalCompanies % 1000 === 0) {
          console.log(`  処理中: ${totalCompanies.toLocaleString()}件... (URL発見: ${companiesWithUrls.toLocaleString()}件)`);
        }
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];

      if (snapshot.docs.length < 500) {
        break;
      }
    }

    console.log("\n================================================================================\n");
    console.log("✅ 処理完了");
    console.log(`   総企業数: ${totalCompanies.toLocaleString()}件`);
    console.log(`   URLを持つ企業数: ${companiesWithUrls.toLocaleString()}件`);
    console.log(`   URL総数: ${totalUrlCount.toLocaleString()}件`);
    console.log(`\n   出力ファイル: ${csvFilePath}`);

    // フィールド名ごとの統計を表示
    console.log("\n   フィールド名ごとのURL出現回数:");
    const sortedFields = Array.from(fieldNameStats.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 20); // 上位20件

    for (const [fieldName, count] of sortedFields) {
      console.log(`     - ${fieldName}: ${count.toLocaleString()}件`);
    }

    console.log("\n================================================================================\n");

  } catch (error) {
    console.error("❌ エラー:", error);
    process.exit(1);
  }
}

// 実行
main()
  .then(() => {
    console.log("処理が正常に完了しました");
    process.exit(0);
  })
  .catch((error) => {
    console.error("❌ エラー:", error);
    process.exit(1);
  });
