/* 
  グループ1-5のCSVファイルに含まれる企業の重複を削除するスクリプト
  
  対象:
  - グループ1-5のCSVファイルに含まれる企業のみ
  
  重複判定基準:
  - 企業名 + 住所が一致する場合
  - 法人番号がある場合は、法人番号を優先して残す
  - 法人番号がない場合は、最も古いもの（createdAtが最も古い）を残す
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
    npx ts-node scripts/remove_duplicates_groups_1_to_5.ts
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
  return str.trim().replace(/\s+/g, " ").replace(/[　]/g, " ");
}

function createKey(name: string, address: string): string {
  const normalizedName = normalizeString(name);
  const normalizedAddress = normalizeString(address);
  return `${normalizedName}|${normalizedAddress}`;
}

function isValidCorporateNumber(corpNum: string | null | undefined): boolean {
  if (!corpNum) return false;
  const normalized = String(corpNum).trim().replace(/[^0-9]/g, "");
  return /^[0-9]{13}$/.test(normalized);
}

// ==============================
// CSVから対象企業を取得
// ==============================
function getTargetCompaniesFromCsv(): Map<string, { name: string; address: string; corporateNumber: string | null }> {
  const companies = new Map<string, { name: string; address: string; corporateNumber: string | null }>();

  for (const csvFile of ALL_FILES) {
    const csvPath = path.join(CSV_DIR, csvFile);

    if (!fs.existsSync(csvPath)) {
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
        const name = normalizeString(row["会社名"] || row["企業名"] || row["name"]);
        const address = normalizeString(row["会社住所"] || row["住所"] || row["address"]);
        const corporateNumber = normalizeString(row["法人番号"] || row["corporateNumber"]);

        if (!name || !address) {
          continue;
        }

        const key = createKey(name, address);
        if (!companies.has(key)) {
          companies.set(key, {
            name,
            address,
            corporateNumber: corporateNumber && isValidCorporateNumber(corporateNumber) ? corporateNumber.replace(/[^0-9]/g, "") : null,
          });
        } else {
          // 既に存在する場合、法人番号がある方を優先
          const existing = companies.get(key)!;
          if (!existing.corporateNumber && corporateNumber && isValidCorporateNumber(corporateNumber)) {
            existing.corporateNumber = corporateNumber.replace(/[^0-9]/g, "");
          }
        }
      }
    } catch (err: any) {
      console.warn(`⚠️  ${csvFile} の読み込みエラー: ${err.message}`);
    }
  }

  return companies;
}

// ==============================
// メイン処理
// ==============================
async function main() {
  console.log("🔍 グループ1-5のCSVファイルに含まれる企業の重複を削除します...\n");
  console.log("📋 重複判定基準:");
  console.log("   - 企業名 + 住所が一致する場合");
  console.log("   - 法人番号がある場合は、法人番号を優先して残す");
  console.log("   - 法人番号がない場合は、最も古いもの（createdAtが最も古い）を残す\n");

  const db = initFirebase();
  const companiesCol = db.collection(COLLECTION_NAME);

  // ステップ1: CSVから対象企業を取得
  console.log("📊 ステップ1: CSVから対象企業を取得中...");
  const targetCompanies = getTargetCompaniesFromCsv();
  console.log(`   対象企業数（CSVから）: ${targetCompanies.size}件\n`);

  // ステップ2: Firestoreから対象企業を検索
  console.log("📊 ステップ2: Firestoreから対象企業を検索中...");
  const foundCompanies: Map<string, Array<{ docId: string; data: any }>> = new Map();

  let searchCount = 0;
  for (const [key, company] of targetCompanies.entries()) {
    const { name, address, corporateNumber } = company;

    // 法人番号で検索（最優先）
    if (corporateNumber) {
      const docRef = companiesCol.doc(corporateNumber);
      const doc = await docRef.get();
      if (doc.exists) {
        const data = doc.data();
        if (data) {
          const docName = normalizeString(data.name);
          const docAddress = normalizeString(data.address || data.headquartersAddress);
          if (docName === name && docAddress === address) {
            if (!foundCompanies.has(key)) {
              foundCompanies.set(key, []);
            }
            foundCompanies.get(key)!.push({ docId: doc.id, data });
            continue;
          }
        }
      }
    }

    // 企業名+住所で検索
    const nameQuery = companiesCol
      .where("name", "==", name)
      .limit(100);
    
    const snapshot = await nameQuery.get();
    for (const doc of snapshot.docs) {
      const data = doc.data();
      if (data) {
        const docAddress = normalizeString(data.address || data.headquartersAddress);
        if (docAddress === address) {
          if (!foundCompanies.has(key)) {
            foundCompanies.set(key, []);
          }
          foundCompanies.get(key)!.push({ docId: doc.id, data });
        }
      }
    }

    searchCount++;
    if (searchCount % 100 === 0) {
      process.stdout.write(`\r   進捗: ${searchCount}/${targetCompanies.size}件を検索中...`);
    }
  }

  if (searchCount % 100 !== 0) {
    console.log();
  }

  console.log(`   見つかった企業数: ${foundCompanies.size}件\n`);

  // ステップ3: 重複を特定
  console.log("📊 ステップ3: 重複を特定中...");
  const duplicateGroups: Array<{ key: string; docs: Array<{ docId: string; data: any }> }> = [];
  for (const [key, docs] of foundCompanies.entries()) {
    if (docs.length > 1) {
      duplicateGroups.push({ key, docs });
    }
  }

  console.log(`   重複グループ数: ${duplicateGroups.length}件\n`);

  if (duplicateGroups.length === 0) {
    console.log("✅ 重複は見つかりませんでした。");
    return;
  }

  // フィールドの充実度を計算（nullでないフィールドの数）
  function calculateFieldRichness(data: any): number {
    if (!data) return 0;
    let count = 0;
    for (const [key, value] of Object.entries(data)) {
      // createdAt, updatedAtなどのシステムフィールドは除外
      if (key === "createdAt" || key === "updatedAt") continue;
      if (value !== null && value !== undefined && value !== "") {
        // 配列の場合は空でないかチェック
        if (Array.isArray(value)) {
          if (value.length > 0) count++;
        } else {
          count++;
        }
      }
    }
    return count;
  }

  // 2つのドキュメントをマージ（targetにsourceの値をマージ、targetに値がない場合のみ）
  function mergeDocuments(target: any, source: any): any {
    const merged = { ...target };
    for (const [key, value] of Object.entries(source)) {
      // システムフィールドは除外
      if (key === "createdAt" || key === "updatedAt") continue;
      
      // targetに値がない、またはnull/undefined/空文字列の場合、sourceの値を使用
      if (merged[key] === null || merged[key] === undefined || merged[key] === "") {
        if (value !== null && value !== undefined && value !== "") {
          merged[key] = value;
        }
      } else if (Array.isArray(merged[key]) && Array.isArray(value)) {
        // 配列の場合は、重複を除去してマージ
        const mergedArray = [...new Set([...merged[key], ...value])];
        if (mergedArray.length > 0) {
          merged[key] = mergedArray;
        }
      }
    }
    return merged;
  }

  // ステップ4: 重複をマージして削除
  console.log("📊 ステップ4: 重複をマージして削除中...");
  let totalDuplicates = 0;
  let totalToDelete = 0;
  let totalMerged = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 50; // バッチサイズを小さくして、ペイロードサイズエラーを回避

  const deletionLog: Array<{
    companyName: string;
    address: string;
    keep: string;
    delete: string[];
    mergedFields: string[];
  }> = [];

  for (let i = 0; i < duplicateGroups.length; i++) {
    const { key, docs } = duplicateGroups[i];
    const [name, address] = key.split("|");

    // 残すドキュメントを決定（最も充実した内容のもの）
    let keepDoc: { docId: string; data: any } | null = null;
    let maxRichness = -1;

    // 各ドキュメントの充実度を計算
    for (const doc of docs) {
      const richness = calculateFieldRichness(doc.data);
      if (richness > maxRichness) {
        maxRichness = richness;
        keepDoc = doc;
      } else if (richness === maxRichness && keepDoc) {
        // 充実度が同じ場合、法人番号がある方を優先
        const keepCorpNum = keepDoc.data.corporateNumber;
        const docCorpNum = doc.data.corporateNumber;
        if (!keepCorpNum && docCorpNum && isValidCorporateNumber(docCorpNum)) {
          keepDoc = doc;
        } else if (!keepCorpNum && !docCorpNum) {
          // 両方とも法人番号がない場合、createdAtが古い方を優先
          const keepTime = keepDoc.data.createdAt?.toMillis?.() || 0;
          const docTime = doc.data.createdAt?.toMillis?.() || 0;
          if (docTime < keepTime) {
            keepDoc = doc;
          }
        }
      }
    }

    if (!keepDoc) {
      continue;
    }

    // 削除するドキュメント
    const toDelete = docs.filter(d => d.docId !== keepDoc!.docId);
    totalDuplicates += docs.length;
    totalToDelete += toDelete.length;

    // 削除対象のドキュメントから情報をマージ
    let mergedData = { ...keepDoc.data };
    const mergedFields: string[] = [];

    for (const docToDelete of toDelete) {
      const beforeMerge = JSON.stringify(mergedData);
      mergedData = mergeDocuments(mergedData, docToDelete.data);
      const afterMerge = JSON.stringify(mergedData);
      
      if (beforeMerge !== afterMerge) {
        // マージされたフィールドを記録
        for (const key of Object.keys(docToDelete.data)) {
          if (key !== "createdAt" && key !== "updatedAt") {
            const beforeValue = JSON.parse(beforeMerge)[key];
            const afterValue = mergedData[key];
            if (beforeValue !== afterValue && afterValue !== null && afterValue !== undefined && afterValue !== "") {
              if (!mergedFields.includes(key)) {
                mergedFields.push(key);
              }
            }
          }
        }
        totalMerged++;
      }
    }

    // updatedAtを更新
    mergedData.updatedAt = admin.firestore.FieldValue.serverTimestamp();

    deletionLog.push({
      companyName: name,
      address: address,
      keep: keepDoc.docId,
      delete: toDelete.map(d => d.docId),
      mergedFields: mergedFields,
    });

    // マージしたデータで更新（merge: trueではなく、完全置換でサイズを制御）
    const keepDocRef = companiesCol.doc(keepDoc.docId);
    
    // システムフィールドを保持
    mergedData.createdAt = keepDoc.data.createdAt || admin.firestore.FieldValue.serverTimestamp();
    mergedData.updatedAt = admin.firestore.FieldValue.serverTimestamp();
    
    batch.set(keepDocRef, mergedData, { merge: false });
    batchCount++;

    // 削除対象をバッチに追加
    for (const docToDelete of toDelete) {
      const docRef = companiesCol.doc(docToDelete.docId);
      batch.delete(docRef);
      batchCount++;

      if (batchCount >= BATCH_LIMIT) {
        await batch.commit();
        batch = db.batch();
        batchCount = 0;
      }
    }

    // 進捗表示
    if ((i + 1) % 10 === 0) {
      process.stdout.write(`\r   進捗: ${i + 1}/${duplicateGroups.length}件の重複グループを処理中...`);
    }
  }

  // 最後のバッチをコミット
  if (batchCount > 0) {
    await batch.commit();
  }

  if (duplicateGroups.length % 10 !== 0) {
    console.log();
  }

  console.log("\n" + "=".repeat(60));
  console.log("📊 重複削除結果サマリー");
  console.log("=".repeat(60));
  console.log(`🔍 重複グループ数: ${duplicateGroups.length}件`);
  console.log(`📊 重複企業総数: ${totalDuplicates}件`);
  console.log(`✅ 残す企業数: ${duplicateGroups.length}件`);
  console.log(`🔄 マージした企業数: ${totalMerged}件`);
  console.log(`🗑️  削除した企業数: ${totalToDelete}件`);
  console.log("=".repeat(60));

  // 削除ログを保存
  const timestamp = Date.now();
  const logFile = path.join(process.cwd(), `duplicate_deletion_groups_1_to_5_${timestamp}.json`);
  fs.writeFileSync(logFile, JSON.stringify({
    timestamp: new Date().toISOString(),
    summary: {
      duplicateGroups: duplicateGroups.length,
      totalDuplicates,
      kept: duplicateGroups.length,
      merged: totalMerged,
      deleted: totalToDelete,
    },
    deletions: deletionLog,
  }, null, 2), "utf-8");

  console.log(`\n📄 削除ログを保存しました: ${logFile}`);

  // テスト用: 宇都宮塗料工業株式会社の結果を表示
  const testCompany = "宇都宮塗料工業株式会社";
  const testKey = Array.from(targetCompanies.keys()).find(key => key.startsWith(testCompany));
  
  if (testKey && foundCompanies.has(testKey)) {
    const testDocs = foundCompanies.get(testKey)!;
    console.log(`\n📋 テスト: "${testCompany}" の検索結果`);
    console.log(`   現在の件数: ${testDocs.length}件`);
    for (const doc of testDocs) {
      const data = doc.data();
      console.log(`   - ID: ${doc.docId}, 法人番号: ${data.corporateNumber || "なし"}, 住所: ${data.address || data.headquartersAddress || "なし"}`);
    }
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
