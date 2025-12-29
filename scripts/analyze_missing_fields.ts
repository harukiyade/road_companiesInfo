/* eslint-disable no-console */

/**
 * scripts/analyze_missing_fields.ts
 * 
 * 目的: companies_newコレクション内の各ドキュメントの不足フィールドを洗い出し、Firestoreに保存
 * 
 * 処理内容:
 * 1. 各ドキュメントに `missingFields` 配列フィールドを追加（不足しているフィールド名のリスト）
 * 2. 各ドキュメントに `missingFieldsCount` 数値フィールドを追加（不足フィールド数）
 * 3. 各ドキュメントに `importantMissingFields` 配列フィールドを追加（重要フィールドの不足リスト）
 * 4. 各ドキュメントに `importantMissingFieldsCount` 数値フィールドを追加（重要フィールドの不足数）
 * 5. 統計情報を `field_analysis_stats` コレクションに保存
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

// Firebase初期化
const serviceAccountKeyPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
if (!serviceAccountKeyPath) {
  console.error("❌ FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません");
  process.exit(1);
}

try {
  const serviceAccount = require(serviceAccountKeyPath);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
  console.log("✅ Firebase初期化完了");
} catch (error: any) {
  console.error("❌ Firebase初期化エラー:", error.message);
  process.exit(1);
}

const db = admin.firestore();

// 分析対象の全フィールドリスト（companies_newコレクションのスキーマに基づく）
const ALL_FIELDS = [
  // 基本情報（14フィールド）
  "name", "nameEn", "kana", "corporateNumber", "corporationType", "nikkeiCode",
  "badges", "tags", "createdAt", "updatedAt", "updateDate", "updateCount", "changeCount", "qualificationGrade",
  // 所在地情報（6フィールド）
  "prefecture", "address", "headquartersAddress", "postalCode", "location", "departmentLocation",
  // 連絡先情報（6フィールド）
  "phoneNumber", "contactPhoneNumber", "fax", "email", "companyUrl", "contactFormUrl",
  // 代表者情報（10フィールド）
  "representativeName", "representativeKana", "representativeTitle", "representativeBirthDate",
  "representativePhone", "representativePostalCode", "representativeHomeAddress",
  "representativeRegisteredAddress", "representativeAlmaMater", "executives",
  // 業種情報（13フィールド）
  "industry", "industryLarge", "industryMiddle", "industrySmall", "industryDetail",
  "industries", "industryCategories", "businessDescriptions", "businessItems",
  "businessSummary", "specialties", "demandProducts", "specialNote",
  // 財務情報（29フィールド）
  "capitalStock", "revenue", "latestRevenue", "latestProfit", "revenueFromStatements",
  "operatingIncome", "totalAssets", "totalLiabilities", "netAssets", "issuedShares",
  "financials", "listing", "marketSegment", "latestFiscalYearMonth", "fiscalMonth",
  "fiscalMonth1", "fiscalMonth2", "fiscalMonth3", "fiscalMonth4", "fiscalMonth5",
  "revenue1", "revenue2", "revenue3", "revenue4", "revenue5",
  "profit1", "profit2", "profit3", "profit4", "profit5",
  // 企業規模・組織（10フィールド）
  "employeeCount", "employeeNumber", "factoryCount", "officeCount", "storeCount",
  "averageAge", "averageYearsOfService", "averageOvertimeHours", "averagePaidLeave", "femaleExecutiveRatio",
  // 設立・沿革（5フィールド）
  "established", "dateOfEstablishment", "founding", "foundingYear", "acquisition",
  // 取引先・関係会社（7フィールド）
  "clients", "suppliers", "subsidiaries", "affiliations", "shareholders", "banks", "bankCorporateNumber",
  // 企業説明（4フィールド）
  "overview", "companyDescription", "businessDescriptions", "salesNotes",
  // SNS・外部リンク（8フィールド）
  "urls", "profileUrl", "externalDetailUrl", "facebook", "linkedin", "wantedly", "youtrust", "metaKeywords",
];

// 必須フィールド（常にチェックする）
const REQUIRED_FIELDS = ["name", "corporateNumber"];

// 重要フィールド（優先的に取得すべき）
const IMPORTANT_FIELDS = [
  "phoneNumber", "email", "companyUrl", "address", "prefecture",
  "representativeName", "industry", "capitalStock", "revenue", "employeeCount",
  "established", "listing"
];

/**
 * フィールドが空かどうかを判定
 */
function isEmpty(value: any): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === "string" && value.trim() === "") return true;
  if (Array.isArray(value) && value.length === 0) return true;
  if (typeof value === "object" && Object.keys(value).length === 0) return true;
  return false;
}

/**
 * 不足フィールドを分析
 */
async function analyzeMissingFields() {
  // 統計用のデータ構造
  const fieldStats: { [key: string]: { missing: number; total: number; category: string } } = {};
  const missingCountDistribution: { [key: number]: number } = {};

  // フィールドのカテゴリ分類
  const fieldCategories: { [key: string]: string } = {};
  ALL_FIELDS.forEach(field => {
    if (field.startsWith("representative") || field === "executives") {
      fieldCategories[field] = "代表者情報";
    } else if (["prefecture", "address", "headquartersAddress", "postalCode", "location", "departmentLocation"].includes(field)) {
      fieldCategories[field] = "所在地情報";
    } else if (["phoneNumber", "contactPhoneNumber", "fax", "email", "companyUrl", "contactFormUrl"].includes(field)) {
      fieldCategories[field] = "連絡先情報";
    } else if (["industry", "industryLarge", "industryMiddle", "industrySmall", "industryDetail", "industries", "industryCategories", "businessDescriptions", "businessItems", "businessSummary", "specialties", "demandProducts", "specialNote"].includes(field)) {
      fieldCategories[field] = "業種情報";
    } else if (["capitalStock", "revenue", "latestRevenue", "latestProfit", "revenueFromStatements", "operatingIncome", "totalAssets", "totalLiabilities", "netAssets", "issuedShares", "financials", "listing", "marketSegment", "latestFiscalYearMonth", "fiscalMonth"].includes(field) || field.startsWith("fiscalMonth") || field.startsWith("revenue") || field.startsWith("profit")) {
      fieldCategories[field] = "財務情報";
    } else if (["employeeCount", "employeeNumber", "factoryCount", "officeCount", "storeCount", "averageAge", "averageYearsOfService", "averageOvertimeHours", "averagePaidLeave", "femaleExecutiveRatio"].includes(field)) {
      fieldCategories[field] = "企業規模・組織";
    } else if (["established", "dateOfEstablishment", "founding", "foundingYear", "acquisition"].includes(field)) {
      fieldCategories[field] = "設立・沿革";
    } else if (["clients", "suppliers", "subsidiaries", "affiliations", "shareholders", "banks", "bankCorporateNumber"].includes(field)) {
      fieldCategories[field] = "取引先・関係会社";
    } else if (["overview", "companyDescription", "businessDescriptions", "salesNotes"].includes(field)) {
      fieldCategories[field] = "企業説明";
    } else if (["urls", "profileUrl", "externalDetailUrl", "facebook", "linkedin", "wantedly", "youtrust", "metaKeywords"].includes(field)) {
      fieldCategories[field] = "SNS・外部リンク";
    } else {
      fieldCategories[field] = "基本情報";
    }

    // 統計初期化
    fieldStats[field] = { missing: 0, total: 0, category: fieldCategories[field] };
  });

  let totalCompanies = 0;
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
  const BATCH_SIZE = 5000;

  console.log("企業データの分析を開始...");

  while (true) {
    let query = db.collection("companies_new").limit(BATCH_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();
    if (snapshot.empty) {
      break;
    }

    console.log(`バッチ処理中: ${totalCompanies + 1} ～ ${totalCompanies + snapshot.size} 件`);

    for (const companyDoc of snapshot.docs) {
      totalCompanies++;
      const companyId = companyDoc.id;
      const companyData = companyDoc.data();
      const companyName = companyData.name || "";

      // 不足フィールドを収集
      const missingFields: string[] = [];
      const importantMissingFields: string[] = [];

      ALL_FIELDS.forEach(field => {
        fieldStats[field].total++;
        const value = companyData[field];
        
        if (isEmpty(value)) {
          fieldStats[field].missing++;
          missingFields.push(field);
          
          if (IMPORTANT_FIELDS.includes(field)) {
            importantMissingFields.push(field);
          }
        }
      });

      const missingCount = missingFields.length;
      const importantMissingCount = importantMissingFields.length;

      // 不足カウント分布を記録
      if (!missingCountDistribution[missingCount]) {
        missingCountDistribution[missingCount] = 0;
      }
      missingCountDistribution[missingCount]++;

      // CSVに書き込み
      const missingFieldsStr = missingFields.join("; ");
      const importantMissingFieldsStr = importantMissingFields.join("; ");
      missingFieldsStream.write(
        `${companyId},"${companyName.replace(/"/g, '""')}",` +
        `"${missingFieldsStr.replace(/"/g, '""')}",${missingCount},` +
        `"${importantMissingFieldsStr.replace(/"/g, '""')}",${importantMissingCount}\n`
      );
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }

  // フィールド統計を書き込み
  const sortedFields = Object.entries(fieldStats).sort((a, b) => {
    const rateA = a[1].total > 0 ? a[1].missing / a[1].total : 0;
    const rateB = b[1].total > 0 ? b[1].missing / b[1].total : 0;
    return rateB - rateA; // 不足率の高い順
  });

  for (const [fieldName, stats] of sortedFields) {
    const missingRate = stats.total > 0 ? (stats.missing / stats.total * 100).toFixed(2) : "0.00";
    fieldStatsStream.write(
      `${fieldName},${stats.missing},${stats.total},${missingRate}%,${stats.category}\n`
    );
  }

  // 不足数分布を書き込み
  const sortedDistribution = Object.entries(missingCountDistribution)
    .map(([count, companyCount]) => ({
      count: parseInt(count),
      companyCount,
      percentage: ((companyCount / totalCompanies) * 100).toFixed(2)
    }))
    .sort((a, b) => a.count - b.count);

  for (const dist of sortedDistribution) {
    distributionStream.write(`${dist.count},${dist.companyCount},${dist.percentage}%\n`);
  }

  // ストリームを閉じる
  missingFieldsStream.end();
  fieldStatsStream.end();
  distributionStream.end();

  await new Promise<void>((resolve) => {
    let closed = 0;
    const checkClose = () => {
      closed++;
      if (closed === 3) resolve();
    };
    missingFieldsStream.on("finish", checkClose);
    fieldStatsStream.on("finish", checkClose);
    distributionStream.on("finish", checkClose);
  });

  console.log(`\n✅ 分析完了`);
  console.log(`総企業数: ${totalCompanies} 件`);
  console.log(`\n出力ファイル:`);
  console.log(`  1. 各企業の不足フィールド: ${missingFieldsCsvPath}`);
  console.log(`  2. フィールド別不足率統計: ${fieldStatsCsvPath}`);
  console.log(`  3. 不足フィールド数分布: ${distributionCsvPath}`);

  // サマリー表示
  console.log(`\n📊 サマリー:`);
  const topMissingFields = sortedFields.slice(0, 10);
  console.log(`\n不足率トップ10フィールド:`);
  topMissingFields.forEach(([field, stats], index) => {
    const rate = stats.total > 0 ? (stats.missing / stats.total * 100).toFixed(2) : "0.00";
    console.log(`  ${index + 1}. ${field}: ${rate}% (${stats.missing}/${stats.total}) [${stats.category}]`);
  });

  const avgMissingCount = Object.entries(missingCountDistribution).reduce((sum, [count, companyCount]) => {
    return sum + (parseInt(count) * companyCount);
  }, 0) / totalCompanies;
  console.log(`\n平均不足フィールド数: ${avgMissingCount.toFixed(2)} フィールド/企業`);

  process.exit(0);
}

analyzeMissingFields().catch((error) => {
  console.error("エラー:", error);
  process.exit(1);
});

