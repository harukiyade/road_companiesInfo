/* eslint-disable no-console */

/**
 * scripts/list_null_fields.ts
 * 
 * 目的: companies_newコレクション内で指定フィールドがnullのドキュメントを
 *       フィールド名とセットで洗い出し、CSVファイルに出力
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

// 対象フィールドリスト
const TARGET_FIELDS = [
  "corporateNumber",      // 法人番号
  "prefecture",           // 都道府県
  "address",              // 住所
  "phoneNumber",          // 電話番号
  "fax",                  // FAX番号
  "email",                // メールアドレス
  "companyUrl",           // 企業URL
  "contactFormUrl",       // 問い合わせフォームURL
  "representativeName",   // 代表者名
  "representativeKana",   // 代表者名（カナ）
  "representativeTitle",  // 代表者役職
  "representativeBirthDate",      // 代表者生年月日
  "representativePhone",          // 代表者電話番号
  "representativePostalCode",     // 代表者郵便番号
  "representativeHomeAddress",     // 代表者自宅住所
  "representativeRegisteredAddress", // 代表者登録
  "representativeAlmaMater",       // 代表者出身校
  "executives",                    // 役員一覧
  "industry",                      // 業種
  "industryLarge",                 // 業種（大分類）
  "industryMiddle",                // 業種（中分類）
  "industrySmall",                 // 業種（小分類）
  "industryDetail",                // 業種（詳細）
  "capitalStock",                  // 資本金
  "revenue",                       // 売上高
  "operatingIncome",               // 営業利益
  "totalAssets",                   // 総資産
  "totalLiabilities",              // 総負債
  "netAssets",                     // 純資産
  "listing",                       // 上場区分
  "marketSegment",                 // 市場区分
  "latestFiscalYearMonth",         // 最新決算年月
  "fiscalMonth",                   // 決算月
  "employeeCount",                 // 従業員数
  "factoryCount",                  // 工場数
  "officeCount",                   // オフィス数
  "storeCount",                    // 店舗数
  "established",                   // 設立日
  "clients",                       // 取引先
  "suppliers",                     // 仕入先
  "shareholders",                  // 株主
  "banks",                         // 取引銀行
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
 * CSV値のエスケープ
 */
function escapeCsvValue(value: string): string {
  if (value.includes(",") || value.includes('"') || value.includes("\n")) {
    return `"${value.replace(/"/g, '""')}"`;
  }
  return value;
}

/**
 * nullフィールドを洗い出し
 */
async function listNullFields() {
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-").split("T")[0];
  const outputDir = path.join(process.cwd(), "null_fields_detailed");
  
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  // 統計用
  const fieldStats: { [key: string]: number } = {};
  TARGET_FIELDS.forEach(field => {
    fieldStats[field] = 0;
  });

  let totalCompanies = 0;
  let totalNullFields = 0;
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
  const BATCH_SIZE = 5000;
  const ROWS_PER_FILE = 10000; // 1ファイルあたりの最大行数

  let currentFileIndex = 1;
  let currentRowCount = 0;
  let currentFileStream: fs.WriteStream | null = null;

  const openNewFile = () => {
    if (currentFileStream) {
      currentFileStream.end();
    }
    const fileName = `null_fields_detailed_${String(currentFileIndex).padStart(4, "0")}.csv`;
    const filePath = path.join(outputDir, fileName);
    currentFileStream = fs.createWriteStream(filePath, { encoding: "utf8" });
    currentFileStream.write("companyId,companyName,nullFieldName,fieldCategory,fieldType\n");
    currentRowCount = 0;
    console.log(`📄 新規ファイル作成: ${fileName}`);
  };

  // フィールドのカテゴリ分類
  const getFieldCategory = (fieldName: string): string => {
    if (fieldName === "corporateNumber") return "basic";
    if (fieldName.startsWith("representative") || fieldName === "executives") return "representative";
    if (["prefecture", "address"].includes(fieldName)) return "location";
    if (["phoneNumber", "fax", "email", "companyUrl", "contactFormUrl"].includes(fieldName)) return "contact";
    if (fieldName.startsWith("industry")) return "industry";
    if (["capitalStock", "revenue", "operatingIncome", "totalAssets", "totalLiabilities", "netAssets", "listing", "marketSegment", "latestFiscalYearMonth", "fiscalMonth"].includes(fieldName)) return "financial";
    if (["employeeCount", "factoryCount", "officeCount", "storeCount"].includes(fieldName)) return "organization";
    if (fieldName === "established") return "history";
    if (["clients", "suppliers", "shareholders", "banks"].includes(fieldName)) return "relationships";
    return "other";
  };

  // フィールドの型分類
  const getFieldType = (fieldName: string): string => {
    if (["executives", "suppliers", "shareholders", "banks"].includes(fieldName)) return "array";
    if (["capitalStock", "revenue", "operatingIncome", "totalAssets", "totalLiabilities", "netAssets", "employeeCount", "factoryCount", "officeCount", "storeCount"].includes(fieldName)) return "number";
    return "null";
  };

  openNewFile();

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

      if (!companyName) {
        continue;
      }

      // 各フィールドをチェック
      for (const fieldName of TARGET_FIELDS) {
        const value = companyData[fieldName];
        
        if (isEmpty(value)) {
          // 新しいファイルが必要な場合
          if (currentRowCount >= ROWS_PER_FILE) {
            currentFileIndex++;
            openNewFile();
          }

          // CSVに書き込み
          const fieldCategory = getFieldCategory(fieldName);
          const fieldType = getFieldType(fieldName);
          const line = `${companyId},${escapeCsvValue(companyName)},${fieldName},${fieldCategory},${fieldType}\n`;
          
          if (currentFileStream) {
            currentFileStream.write(line);
            currentRowCount++;
            totalNullFields++;
            fieldStats[fieldName]++;
          }
        }
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }

  // 最後のファイルを閉じる
  if (currentFileStream) {
    currentFileStream.end();
  }

  // 統計情報を出力
  const statsPath = path.join(outputDir, `null_fields_stats_${timestamp}.csv`);
  const statsStream = fs.createWriteStream(statsPath, { encoding: "utf8" });
  statsStream.write("fieldName,fieldCategory,fieldType,nullCount\n");

  const sortedFields = Object.entries(fieldStats)
    .sort((a, b) => b[1] - a[1]); // null件数の多い順

  for (const [fieldName, count] of sortedFields) {
    const fieldCategory = getFieldCategory(fieldName);
    const fieldType = getFieldType(fieldName);
    statsStream.write(`${fieldName},${fieldCategory},${fieldType},${count}\n`);
  }

  statsStream.end();

  await new Promise<void>((resolve) => {
    if (statsStream) {
      statsStream.on("finish", () => resolve());
    } else {
      resolve();
    }
  });

  console.log(`\n✅ 分析完了`);
  console.log(`総企業数: ${totalCompanies} 件`);
  console.log(`総nullフィールド数: ${totalNullFields} 件`);
  console.log(`出力ファイル数: ${currentFileIndex} 個`);
  console.log(`\n出力ディレクトリ: ${outputDir}`);
  console.log(`統計ファイル: null_fields_stats_${timestamp}.csv`);

  // サマリー表示
  console.log(`\n📊 null件数トップ10フィールド:`);
  sortedFields.slice(0, 10).forEach(([field, count], index) => {
    const percentage = totalCompanies > 0 ? ((count / totalCompanies) * 100).toFixed(2) : "0.00";
    console.log(`  ${index + 1}. ${field}: ${count} 件 (${percentage}%)`);
  });

  process.exit(0);
}

listNullFields().catch((error) => {
  console.error("エラー:", error);
  process.exit(1);
});

