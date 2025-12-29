/* eslint-disable no-console */

/**
 * scripts/export_null_fields_detailed.ts
 *
 * ✅ 目的
 * - companies_new コレクション内の全ドキュメントでnullになっているフィールドを詳細に出力
 * - 量が多いため、複数のCSVファイルに分割して出力
 * - 各ドキュメントごとに、nullフィールドを一覧化
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
// 期待されるフィールド定義
// ------------------------------

const EXPECTED_FIELDS = {
  // 📊 基本情報
  basic: [
    "name",
    "nameEn",
    "kana",
    "corporateNumber",
    "corporationType",
    "nikkeiCode",
    "badges",
    "tags",
  ],
  // 📍 所在地情報
  location: [
    "prefecture",
    "address",
    "headquartersAddress",
    "postalCode",
    "location",
    "departmentLocation",
  ],
  // 📞 連絡先情報
  contact: [
    "phoneNumber",
    "contactPhoneNumber",
    "fax",
    "email",
    "companyUrl",
    "contactFormUrl",
  ],
  // 👤 代表者情報
  representative: [
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
  ],
  // 🏢 業種情報
  industry: [
    "industry",
    "industryLarge",
    "industryMiddle",
    "industrySmall",
    "industryDetail",
    "industries",
    "industryCategories",
    "businessDescriptions",
    "businessItems",
    "businessSummary",
    "specialties",
    "demandProducts",
    "specialNote",
  ],
  // 💰 財務情報
  financial: [
    "capitalStock",
    "revenue",
    "latestRevenue",
    "latestProfit",
    "revenueFromStatements",
    "operatingIncome",
    "totalAssets",
    "totalLiabilities",
    "netAssets",
    "issuedShares",
    "financials",
    "listing",
    "marketSegment",
    "latestFiscalYearMonth",
    "fiscalMonth",
  ],
  // 🏭 企業規模・組織
  organization: [
    "employeeCount",
    "employeeNumber",
    "factoryCount",
    "officeCount",
    "storeCount",
    "averageAge",
    "averageYearsOfService",
    "averageOvertimeHours",
    "averagePaidLeave",
    "femaleExecutiveRatio",
  ],
  // 📅 設立・沿革
  establishment: [
    "established",
    "dateOfEstablishment",
    "founding",
    "foundingYear",
    "acquisition",
  ],
  // 🤝 取引先・関係会社
  relationships: [
    "clients",
    "suppliers",
    "subsidiaries",
    "affiliations",
    "shareholders",
    "banks",
    "bankCorporateNumber",
  ],
  // 📝 企業説明
  description: [
    "overview",
    "companyDescription",
    "businessDescriptions",
    "salesNotes",
  ],
  // 🌐 SNS・外部リンク
  external: [
    "urls",
    "profileUrl",
    "externalDetailUrl",
    "facebook",
    "linkedin",
    "wantedly",
    "youtrust",
    "metaKeywords",
  ],
};

/**
 * フィールドが空かどうかをチェック
 */
function isEmpty(value: any): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === "string" && value.trim() === "") return true;
  if (Array.isArray(value) && value.length === 0) return true;
  if (typeof value === "object" && Object.keys(value).length === 0) return true;
  return false;
}

/**
 * フィールドのカテゴリを取得
 */
function getFieldCategory(fieldName: string): string {
  for (const [category, fields] of Object.entries(EXPECTED_FIELDS)) {
    if (fields.includes(fieldName)) {
      return category;
    }
  }
  return "other";
}

/**
 * CSVエスケープ
 */
function escapeCsvValue(value: any): string {
  if (value === null || value === undefined || value === "") {
    return "";
  }

  if (Array.isArray(value)) {
    if (value.length === 0) {
      return "";
    }
    return `"${value.join("; ").replace(/"/g, '""')}"`;
  }

  const str = String(value);
  if (str.trim() === "") {
    return "";
  }

  if (str.includes(",") || str.includes('"') || str.includes("\n")) {
    return `"${str.replace(/"/g, '""')}"`;
  }

  return str;
}

/**
 * 各ドキュメントのnullフィールドを詳細に出力（複数ファイルに分割）
 */
async function exportNullFieldsDetailed() {
  try {
    console.log("各ドキュメントのnullフィールド詳細を出力開始...");

    const BATCH_SIZE = 5000;
    const ROWS_PER_FILE = 10000; // 1ファイルあたりの行数
    const OUTPUT_DIR = path.join(process.cwd(), "null_fields_detailed");
    
    // 出力ディレクトリを作成
    if (!fs.existsSync(OUTPUT_DIR)) {
      fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    }

    // CSVヘッダー
    const headers = [
      "companyId",
      "companyName",
      "nullFieldName",
      "fieldCategory",
      "fieldType", // string, number, array, etc.
    ];

    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;
    let totalNullFields = 0;
    let fileIndex = 1;
    let rowCount = 0;
    let writeStream: fs.WriteStream | null = null;

    // 全期待フィールドを取得
    const allExpectedFields: string[] = [];
    Object.values(EXPECTED_FIELDS).forEach((fields) => {
      allExpectedFields.push(...fields);
    });

    const openNewFile = () => {
      if (writeStream) {
        writeStream.end();
      }
      const fileName = `null_fields_detailed_${String(fileIndex).padStart(4, "0")}.csv`;
      const filePath = path.join(OUTPUT_DIR, fileName);
      writeStream = fs.createWriteStream(filePath, { encoding: "utf8" });
      writeStream.write(headers.map(escapeCsvValue).join(",") + "\n");
      console.log(`\n📄 新規ファイル作成: ${fileName}`);
      rowCount = 0;
    };

    // 最初のファイルを開く
    openNewFile();

    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }

      console.log(`バッチ取得: ${snapshot.size} 件`);

      for (const companyDoc of snapshot.docs) {
        const companyId = companyDoc.id;
        const companyData = companyDoc.data();
        const companyName = companyData.name || "";

        // 各フィールドをチェック
        for (const field of allExpectedFields) {
          const value = companyData[field];
          
          // null/undefined/空の場合は出力
          if (isEmpty(value)) {
            // ファイルが満杯の場合は新しいファイルを開く
            if (rowCount >= ROWS_PER_FILE) {
              fileIndex++;
              openNewFile();
            }

            // フィールドの型を判定
            let fieldType = "unknown";
            if (Array.isArray(value)) {
              fieldType = "array";
            } else if (typeof value === "number") {
              fieldType = "number";
            } else if (typeof value === "string") {
              fieldType = "string";
            } else if (typeof value === "boolean") {
              fieldType = "boolean";
            } else if (value === null) {
              fieldType = "null";
            } else if (value === undefined) {
              fieldType = "undefined";
            }

            const row: string[] = [
              companyId,
              companyName,
              field,
              getFieldCategory(field),
              fieldType,
            ];

            const line = row.map(escapeCsvValue).join(",");
            writeStream!.write(line + "\n");
            rowCount++;
            totalNullFields++;
          }
        }

        totalProcessed++;

        if (totalProcessed % 1000 === 0) {
          console.log(`  処理済み: ${totalProcessed} 件 / nullフィールド: ${totalNullFields} 件 / ファイル: ${fileIndex}`);
        }
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }

    // 最後のファイルを閉じる
    if (writeStream) {
      writeStream.end();
      await new Promise<void>((resolve, reject) => {
        writeStream!.on("finish", () => resolve());
        writeStream!.on("error", (err) => reject(err));
      });
    }

    console.log(`\n✅ 出力完了`);
    console.log(`出力ディレクトリ: ${OUTPUT_DIR}`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`総nullフィールド数: ${totalNullFields} 件`);
    console.log(`出力ファイル数: ${fileIndex} 件`);
    console.log(`1ファイルあたりの最大行数: ${ROWS_PER_FILE} 行`);
  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
exportNullFieldsDetailed()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });

