/* eslint-disable no-console */

/**
 * scripts/export_null_fields.ts
 *
 * ✅ 目的
 * - companies_newコレクションの全ドキュメントから、指定フィールドのいずれかがnullのものを全量出力
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 *
 * ✅ 出力
 * - null_fields_detailed/null_fields_detailed_XXXX.csv に出力
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

// チェック対象フィールド
const TARGET_FIELDS = [
  "corporateNumber",
  "prefecture",
  "address",
  "phoneNumber",
  "fax",
  "email",
  "companyUrl",
  "contactFormUrl",
  "representativeName",
  "representativeKana",
  "representativeTitle",
  "representativeBirthDate",
  "representativePhone",
  "representativePostalCode",
  "representativeHomeAddress",
  "representativeRegisteredAddress",
  "representativeAlmaMater",
  "executives",
  "industry",
  "industryLarge",
  "industryMiddle",
  "industrySmall",
  "industryDetail",
  "capitalStock",
  "revenue",
  "operatingIncome",
  "totalAssets",
  "totalLiabilities",
  "netAssets",
  "listing",
  "marketSegment",
  "latestFiscalYearMonth",
  "fiscalMonth",
  "employeeCount",
  "factoryCount",
  "officeCount",
  "storeCount",
  "established",
  "clients",
  "suppliers",
  "shareholders",
  "banks",
];

/**
 * フィールドがnullかどうかをチェック
 */
function isNullField(value: any): boolean {
  return (
    value === null ||
    value === undefined ||
    value === "" ||
    (Array.isArray(value) && value.length === 0)
  );
}

/**
 * メイン処理
 */
async function main() {
  console.log("================================================================================\n");
  console.log("nullフィールドチェックを開始...");
  console.log(`対象フィールド数: ${TARGET_FIELDS.length}件`);
  console.log("================================================================================\n");

  const outputDir = path.join(process.cwd(), "null_fields_detailed");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  // CSVファイルの準備（タイムスタンプ付き）
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const csvFilePath = path.join(outputDir, `null_fields_detailed_${timestamp}.csv`);

  // CSVヘッダー
  const csvHeaders = ["companyId", "companyName", "nullFieldName"];
  fs.writeFileSync(csvFilePath, csvHeaders.join(",") + "\n", { encoding: "utf8" });

  let totalCompanies = 0;
  let companiesWithNullFields = 0;
  let totalNullFieldCount = 0;
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;

  console.log("Firestoreから企業データを取得中...\n");

  try {
    while (true) {
      let query: admin.firestore.Query = db.collection("companies_new").orderBy(admin.firestore.FieldPath.documentId()).limit(500);
      
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

        // 各フィールドをチェック
        for (const fieldName of TARGET_FIELDS) {
          const fieldValue = data[fieldName];
          
          if (isNullField(fieldValue)) {
            // CSVに出力
            const csvRow = [
              companyId,
              `"${companyName.replace(/"/g, '""')}"`,
              fieldName,
            ];
            fs.appendFileSync(csvFilePath, csvRow.join(",") + "\n", { encoding: "utf8" });
            totalNullFieldCount++;
          }
        }

        // nullフィールドがあるかチェック
        const hasNullFields = TARGET_FIELDS.some((fieldName) => {
          return isNullField(data[fieldName]);
        });

        if (hasNullFields) {
          companiesWithNullFields++;
        }

        // 進捗表示（1000件ごと）
        if (totalCompanies % 1000 === 0) {
          console.log(`  処理中: ${totalCompanies.toLocaleString()}件...`);
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
    console.log(`   nullフィールドを持つ企業数: ${companiesWithNullFields.toLocaleString()}件`);
    console.log(`   nullフィールド総数: ${totalNullFieldCount.toLocaleString()}件`);
    console.log(`\n   出力ファイル: ${csvFilePath}`);
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

