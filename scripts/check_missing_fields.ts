/* eslint-disable no-console */

/**
 * scripts/check_missing_fields.ts
 *
 * ✅ 目的
 * - companies_new コレクション内の全企業情報に対して、どのフィールドが足りていないかを1ドキュメントずつ整理
 * - 結果をCSVファイルに出力
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

/**
 * 期待されるフィールドのリスト（Webスクレイピングで取得可能なフィールドを含む）
 */
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
 * Webスクレイピングで取得可能なフィールドのマッピング
 * (CompanyWebInfo -> companies_new)
 */
const WEBINFO_FIELD_MAPPING: { [key: string]: string[] } = {
  // Webスクレイピングで取得可能なフィールド
  listingStatus: ["listing"],
  securitiesCode: ["nikkeiCode"],
  website: ["companyUrl"],
  contactFormUrl: ["contactFormUrl"],
  capital: ["capitalStock"],
  revenue: ["revenue", "latestRevenue"],
  profit: ["latestProfit"],
  netAssets: ["netAssets"],
  totalAssets: ["totalAssets"],
  totalLiabilities: ["totalLiabilities"],
  operatingIncome: ["operatingIncome"],
  industry: ["industry"],
  companyDescription: ["companyDescription", "overview", "businessDescriptions"],
  companyOverview: ["overview", "companyDescription"],
  employeeCount: ["employeeCount", "employeeNumber"],
  officeCount: ["officeCount"],
  factoryCount: ["factoryCount"],
  storeCount: ["storeCount"],
  contactEmail: ["email"],
  contactPhone: ["phoneNumber", "contactPhoneNumber"],
  fax: ["fax"],
  settlementMonth: ["fiscalMonth", "latestFiscalYearMonth"],
  representative: ["representativeName"],
  representativeKana: ["representativeKana"],
  representativeAddress: ["representativeHomeAddress", "representativeRegisteredAddress"],
  representativeSchool: ["representativeAlmaMater"],
  representativeBirthDate: ["representativeBirthDate"],
  officers: ["executives"],
  shareholders: ["shareholders"],
  banks: ["banks"],
  licenses: [], // 新規フィールドとして追加可能
  sns: ["facebook", "linkedin", "wantedly", "youtrust", "urls"],
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
 * ドキュメントの不足フィールドをチェック
 * 注意: フィールドが存在しない場合も、値がnull/undefined/空の場合も「不足」と判定
 */
function checkMissingFields(
  companyId: string,
  companyData: any,
  webInfoData: any | null
): {
  companyId: string;
  companyName: string;
  missingFields: string[];
  missingCategories: { [category: string]: string[] };
  canFetchFromWeb: string[];
} {
  const missingFields: string[] = [];
  const missingCategories: { [category: string]: string[] } = {};
  const canFetchFromWeb: string[] = [];

  // 全期待フィールドを取得
  const allExpectedFields: string[] = [];
  Object.values(EXPECTED_FIELDS).forEach((fields) => {
    allExpectedFields.push(...fields);
  });

  // 各フィールドをチェック
  // フィールドが存在しない場合も、undefinedとして扱い、isEmptyでtrueになる
  for (const field of allExpectedFields) {
    // フィールドが存在しない場合はundefined、存在する場合はその値
    const value = companyData[field];
    
    // 値がnull/undefined/空の場合は「不足」と判定
    if (isEmpty(value)) {
      missingFields.push(field);
      
      // カテゴリ別に分類
      for (const [category, fields] of Object.entries(EXPECTED_FIELDS)) {
        if (fields.includes(field)) {
          if (!missingCategories[category]) {
            missingCategories[category] = [];
          }
          missingCategories[category].push(field);
          break;
        }
      }

      // Webスクレイピングで取得可能かチェック
      for (const [webInfoField, mappedFields] of Object.entries(WEBINFO_FIELD_MAPPING)) {
        if (mappedFields.includes(field)) {
          // webInfoに既にデータがあるかチェック
          if (webInfoData) {
            const webInfoValue = webInfoData[webInfoField];
            if (!isEmpty(webInfoValue)) {
              // webInfoにはあるが、companies_newにない場合は取得可能
              canFetchFromWeb.push(field);
            }
          } else {
            // webInfoが存在しない場合は、Webスクレイピングで取得可能
            canFetchFromWeb.push(field);
          }
          break;
        }
      }
    }
  }

  return {
    companyId,
    companyName: companyData.name || "",
    missingFields,
    missingCategories,
    canFetchFromWeb,
  };
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
 * 不足フィールドをチェックしてCSVに出力
 */
async function checkAndExportMissingFields() {
  try {
    console.log("企業データの不足フィールドチェックを開始...");

    const BATCH_SIZE = 5000;
    const outputPath = path.join(process.cwd(), "companies_missing_fields.csv");

    // CSVヘッダー
    const headers = [
      "companyId",
      "companyName",
      "totalMissingFields",
      "missingFields",
      "missingCategories",
      "canFetchFromWeb",
      "canFetchFromWebCount",
      "missingBasic",
      "missingLocation",
      "missingContact",
      "missingRepresentative",
      "missingIndustry",
      "missingFinancial",
      "missingOrganization",
      "missingEstablishment",
      "missingRelationships",
      "missingDescription",
      "missingExternal",
    ];

    const writeStream = fs.createWriteStream(outputPath, { encoding: "utf8" });
    writeStream.write(headers.map(escapeCsvValue).join(",") + "\n");

    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;
    let totalWithMissingFields = 0;

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

        // webInfoを取得
        const webInfoRef = db.collection("companies_webInfo").doc(companyId);
        const webInfoDoc = await webInfoRef.get();
        const webInfoData = webInfoDoc.exists ? webInfoDoc.data() : null;

        // 不足フィールドをチェック（フィールドが存在しない場合も含む）
        const result = checkMissingFields(companyId, companyData, webInfoData);

        // 不足フィールドがある場合のみCSVに出力
        if (result.missingFields.length > 0) {
          totalWithMissingFields++;

          // カテゴリ別の不足数を計算
          const categoryCounts: { [key: string]: number } = {};
          for (const [category, fields] of Object.entries(result.missingCategories)) {
            categoryCounts[category] = fields.length;
          }

          const row: string[] = [
            result.companyId,
            result.companyName,
            result.missingFields.length.toString(),
            result.missingFields.join("; "),
            JSON.stringify(result.missingCategories),
            result.canFetchFromWeb.join("; "),
            result.canFetchFromWeb.length.toString(),
            (categoryCounts.basic || 0).toString(),
            (categoryCounts.location || 0).toString(),
            (categoryCounts.contact || 0).toString(),
            (categoryCounts.representative || 0).toString(),
            (categoryCounts.industry || 0).toString(),
            (categoryCounts.financial || 0).toString(),
            (categoryCounts.organization || 0).toString(),
            (categoryCounts.establishment || 0).toString(),
            (categoryCounts.relationships || 0).toString(),
            (categoryCounts.description || 0).toString(),
            (categoryCounts.external || 0).toString(),
          ];

          const line = row.map(escapeCsvValue).join(",");
          writeStream.write(line + "\n");
        }

        totalProcessed++;

        if (totalProcessed % 100 === 0) {
          console.log(`処理済み: ${totalProcessed} 件 / 不足フィールドあり: ${totalWithMissingFields} 件`);
        }
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }

    writeStream.end();
    await new Promise<void>((resolve, reject) => {
      writeStream.on("finish", () => resolve());
      writeStream.on("error", (err) => reject(err));
    });

    console.log(`\n✅ チェック完了`);
    console.log(`CSVファイル: ${outputPath}`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`不足フィールドあり: ${totalWithMissingFields} 件`);
    console.log(`不足フィールドなし: ${totalProcessed - totalWithMissingFields} 件`);
  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
checkAndExportMissingFields()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });

