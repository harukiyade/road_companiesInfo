/* 
  グループ1-5のCSVファイルからインポートされた企業データを削除するスクリプト
  
  対象グループ:
  - グループ1: 法人番号付き標準フォーマット（5ファイル）
  - グループ2: 取引種別・SBフラグ付きフォーマット（4ファイル）
  - グループ3: 標準フォーマット（54ファイル）
  - グループ4: 創業・株式保有率付きフォーマット（24ファイル）
  - グループ5: 法人番号・業種3つ付きフォーマット（5ファイル）
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
    npx ts-node scripts/delete_groups_1_to_5_companies.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import {
  Firestore,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_DIR = path.join(process.cwd(), "csv");

// グループ別ファイル定義
const GROUP1_FILES = ["1.csv", "103.csv", "126.csv", "2.csv", "53.csv"];
const GROUP2_FILES = ["3.csv", "4.csv", "5.csv", "6.csv"];
const GROUP3_FILES = [
  "10.csv", "101.csv", "11.csv", "12.csv", "13.csv", "14.csv", "15.csv", "16.csv", "17.csv", "18.csv",
  "19.csv", "20.csv", "21.csv", "22.csv", "25.csv", "26.csv", "27.csv", "28.csv", "29.csv", "30.csv",
  "31.csv", "32.csv", "33.csv", "34.csv", "35.csv", "39.csv", "52.csv", "54.csv", "55.csv", "56.csv",
  "57.csv", "58.csv", "59.csv", "60.csv", "61.csv", "62.csv", "63.csv", "64.csv", "65.csv", "66.csv",
  "67.csv", "68.csv", "69.csv", "7.csv", "70.csv", "71.csv", "72.csv", "73.csv", "74.csv", "75.csv",
  "76.csv", "77.csv", "8.csv", "9.csv"
];
const GROUP4_FILES = [
  "102.csv", "23.csv", "78.csv", "79.csv", "80.csv", "81.csv", "82.csv", "83.csv", "84.csv", "85.csv",
  "86.csv", "87.csv", "88.csv", "89.csv", "90.csv", "91.csv", "92.csv", "93.csv", "94.csv", "95.csv",
  "96.csv", "97.csv", "98.csv", "99.csv"
];
const GROUP5_FILES = ["133.csv", "134.csv", "24.csv", "40.csv", "41.csv"];

const ALL_FILES = [...GROUP1_FILES, ...GROUP2_FILES, ...GROUP3_FILES, ...GROUP4_FILES, ...GROUP5_FILES];

// ==============================
// Firebase 初期化
// ==============================
function initFirebase() {
  if (admin.apps.length > 0) {
    return admin.firestore();
  }

  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS
    ? process.env.GOOGLE_APPLICATION_CREDENTIALS.trim().replace(/\n/g, "").replace(/\r/g, "")
    : null;

  if (serviceAccountPath && !fs.existsSync(serviceAccountPath)) {
    serviceAccountPath = null;
  }

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
      "/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
    ];

    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        break;
      }
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
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
      projectId: projectId,
    });

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
  } catch (err: any) {
    console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
    console.error(`   詳細: ${err.message}`);
    process.exit(1);
  }

  return admin.firestore();
}

// ==============================
// ヘルパー関数
// ==============================
function normalizeString(str: string | null | undefined): string {
  if (!str) return "";
  return str.trim().replace(/\s+/g, " ");
}

function isValidCorporateNumber(corpNum: string | null | undefined): boolean {
  if (!corpNum) return false;
  const normalized = corpNum.trim().replace(/[^0-9]/g, "");
  return /^[0-9]{13}$/.test(normalized);
}

// 数値IDを生成（削除対象の特定に使用）
function generateNumericDocId(corporateNumber: string | null, index: number): string {
  if (corporateNumber && isValidCorporateNumber(corporateNumber)) {
    return corporateNumber.trim().replace(/[^0-9]/g, "");
  }
  // タイムスタンプベースのIDは削除できないので、企業名+住所で検索する必要がある
  return "";
}

// ==============================
// メイン処理
// ==============================
async function main() {
  console.log("🗑️  グループ1-5のCSVファイルからインポートされた企業データを削除します...\n");
  console.log("📋 削除方法:");
  console.log("   1. 法人番号がある場合: 法人番号で直接削除（グループ1, 2, 5）");
  console.log("   2. 法人番号がない場合: 企業名+住所で検索して削除（グループ3, 4）");
  console.log("");

  const db = initFirebase();
  const companiesCol = db.collection(COLLECTION_NAME);

  let totalDeleted = 0;
  let totalNotFound = 0;
  let totalErrors = 0;

  // 法人番号リストを先に収集（グループ1, 2, 5）
  const corporateNumbersToDelete = new Set<string>();
  const nameAddressPairs: Array<{ name: string; address: string }> = [];

  console.log("📋 ステップ1: CSVから削除対象を収集中...\n");

  for (const csvFile of ALL_FILES) {
    const csvPath = path.join(CSV_DIR, csvFile);

    if (!fs.existsSync(csvPath)) {
      console.warn(`⚠️  ファイルが見つかりません: ${csvFile}`);
      continue;
    }

    try {
      const content = fs.readFileSync(csvPath, "utf-8");
      const records = parse(content, {
        columns: true,
        skip_empty_lines: true,
        bom: true,
        relax_column_count: true,
        relax_quotes: true,
        skip_records_with_error: true,
      }) as Array<Record<string, string>>;

      for (const row of records) {
        const corpNum = normalizeString(row["法人番号"]);
        const companyName = normalizeString(row["会社名"] || row["企業名"] || row["name"]);
        const address = normalizeString(row["会社住所"] || row["住所"] || row["address"]);

        if (corpNum && isValidCorporateNumber(corpNum)) {
          corporateNumbersToDelete.add(corpNum.replace(/[^0-9]/g, ""));
        } else if (companyName && address) {
          nameAddressPairs.push({ name: companyName.trim(), address: address.trim() });
        }
      }
    } catch (err: any) {
      console.error(`   ❌ ${csvFile} の読み込みエラー: ${err.message}`);
    }
  }

  console.log(`   法人番号ベースの削除対象: ${corporateNumbersToDelete.size}件`);
  console.log(`   企業名+住所ベースの削除対象: ${nameAddressPairs.length}件\n`);

  console.log("📋 ステップ2: 法人番号で削除中...\n");

  // 法人番号で削除
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 200;
  let deletedByCorpNum = 0;
  let notFoundByCorpNum = 0;

  for (const corpNum of corporateNumbersToDelete) {
    try {
      const docRef = companiesCol.doc(corpNum);
      const doc = await docRef.get();
      
      if (doc.exists) {
        batch.delete(docRef);
        batchCount++;
        deletedByCorpNum++;

        if (batchCount >= BATCH_LIMIT) {
          await batch.commit();
          batch = db.batch();
          batchCount = 0;
        }

        if (deletedByCorpNum % 100 === 0) {
          process.stdout.write(`\r   進捗: ${deletedByCorpNum}/${corporateNumbersToDelete.size}件`);
        }
      } else {
        notFoundByCorpNum++;
      }
    } catch (err: any) {
      totalErrors++;
      if (totalErrors <= 5) {
        console.error(`\n   ❌ 法人番号 ${corpNum} の削除エラー: ${err.message}`);
      }
    }
  }

  if (batchCount > 0) {
    await batch.commit();
  }

  if (deletedByCorpNum % 100 !== 0) {
    console.log();
  }

  console.log(`   ✅ 法人番号で削除: ${deletedByCorpNum}件, 見つからず: ${notFoundByCorpNum}件\n`);

  console.log("📋 ステップ3: 企業名+住所で削除中...\n");

  // 企業名+住所で削除
  batch = db.batch();
  batchCount = 0;
  let deletedByNameAddr = 0;
  let notFoundByNameAddr = 0;
  const processedDocIds = new Set<string>();

  for (let i = 0; i < nameAddressPairs.length; i++) {
    const { name, address } = nameAddressPairs[i];
    
    try {
      const nameQuery = companiesCol
        .where("name", "==", name)
        .limit(100);
      
      const snapshot = await nameQuery.get();
      let found = false;

      for (const doc of snapshot.docs) {
        // 既に削除対象に含まれている場合はスキップ
        if (processedDocIds.has(doc.id)) {
          continue;
        }

        const data = doc.data();
        const docAddress = normalizeString(data.address);
        
        if (docAddress === address) {
          batch.delete(doc.ref);
          processedDocIds.add(doc.id);
          batchCount++;
          deletedByNameAddr++;
          found = true;

          if (batchCount >= BATCH_LIMIT) {
            await batch.commit();
            batch = db.batch();
            batchCount = 0;
          }
          break;
        }
      }

      if (!found) {
        notFoundByNameAddr++;
      }

      if ((deletedByNameAddr + notFoundByNameAddr) % 100 === 0) {
        process.stdout.write(`\r   進捗: ${deletedByNameAddr + notFoundByNameAddr}/${nameAddressPairs.length}件 (削除: ${deletedByNameAddr}件, 見つからず: ${notFoundByNameAddr}件)`);
      }
    } catch (err: any) {
      totalErrors++;
      if (totalErrors <= 5) {
        console.error(`\n   ❌ 企業名+住所 (${name}, ${address}) の削除エラー: ${err.message}`);
      }
    }
  }

  if (batchCount > 0) {
    await batch.commit();
  }

  if ((deletedByNameAddr + notFoundByNameAddr) % 100 !== 0) {
    console.log();
  }

  console.log(`   ✅ 企業名+住所で削除: ${deletedByNameAddr}件, 見つからず: ${notFoundByNameAddr}件\n`);

  totalDeleted = deletedByCorpNum + deletedByNameAddr;
  totalNotFound = notFoundByCorpNum + notFoundByNameAddr;

  console.log("\n" + "=".repeat(60));
  console.log("📊 削除結果サマリー");
  console.log("=".repeat(60));
  console.log(`✅ 削除成功: ${totalDeleted}件`);
  console.log(`⏭️  見つからず: ${totalNotFound}件`);
  console.log(`❌ エラー: ${totalErrors}件`);
  console.log("=".repeat(60));
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
