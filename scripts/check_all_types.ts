/*
  全タイプのデータ確認スクリプト
  
  各タイプのCSVファイルに対応するFirestoreデータを確認します。
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/check_all_types.ts [タイプ名]
  
  例:
    # 全タイプを確認
    npx ts-node scripts/check_all_types.ts
    
    # 特定タイプのみ確認
    npx ts-node scripts/check_all_types.ts A
    npx ts-node scripts/check_all_types.ts B
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";

// タイプ別CSVファイル定義（run_backfill_by_type.shから）
const TYPE_DEFINITIONS: Record<string, string[]> = {
  A: [
    "csv/7.csv", "csv/8.csv", "csv/9.csv", "csv/10.csv", "csv/11.csv",
    "csv/12.csv", "csv/13.csv", "csv/14.csv", "csv/15.csv", "csv/16.csv",
    "csv/17.csv", "csv/18.csv", "csv/19.csv", "csv/20.csv", "csv/21.csv",
    "csv/22.csv", "csv/25.csv", "csv/26.csv", "csv/27.csv", "csv/28.csv",
    "csv/29.csv", "csv/30.csv", "csv/31.csv", "csv/32.csv", "csv/33.csv",
    "csv/34.csv", "csv/35.csv", "csv/39.csv", "csv/52.csv", "csv/54.csv",
    "csv/55.csv", "csv/56.csv", "csv/57.csv", "csv/58.csv", "csv/59.csv",
    "csv/60.csv", "csv/61.csv", "csv/62.csv", "csv/63.csv", "csv/64.csv",
    "csv/65.csv", "csv/66.csv", "csv/67.csv", "csv/68.csv", "csv/69.csv",
    "csv/70.csv", "csv/71.csv", "csv/72.csv", "csv/73.csv", "csv/74.csv",
    "csv/75.csv", "csv/76.csv", "csv/77.csv", "csv/101.csv", "csv/104.csv"
  ],
  B: [
    "csv/23.csv", "csv/78.csv", "csv/79.csv", "csv/80.csv", "csv/81.csv",
    "csv/82.csv", "csv/83.csv", "csv/84.csv", "csv/85.csv", "csv/86.csv",
    "csv/87.csv", "csv/88.csv", "csv/89.csv", "csv/90.csv", "csv/91.csv",
    "csv/92.csv", "csv/93.csv", "csv/94.csv", "csv/95.csv", "csv/96.csv",
    "csv/97.csv", "csv/98.csv", "csv/99.csv", "csv/100.csv", "csv/102.csv",
    "csv/105.csv"
  ],
  C: [
    "csv/36.csv", "csv/37.csv", "csv/44.csv", "csv/49.csv", "csv/107.csv",
    "csv/109.csv"
  ],
  D: [
    "csv/1.csv", "csv/2.csv", "csv/53.csv", "csv/103.csv", "csv/106.csv",
    "csv/126.csv"
  ],
  E: [
    "csv/3.csv", "csv/4.csv", "csv/5.csv", "csv/6.csv"
  ],
  F: [
    "csv/132.csv"
  ],
  G: [
    "csv/108.csv", "csv/110.csv", "csv/111.csv", "csv/112.csv"
  ],
  H: [
    "csv/118.csv", "csv/119.csv", "csv/120.csv", "csv/122.csv"
  ],
  I: [
    "csv/130.csv", "csv/131.csv"
  ],
  Other: [
    "csv/24.csv", "csv/38.csv", "csv/40.csv", "csv/41.csv", "csv/42.csv",
    "csv/43.csv", "csv/45.csv", "csv/46.csv", "csv/47.csv", "csv/48.csv",
    "csv/50.csv", "csv/51.csv", "csv/113.csv", "csv/114.csv", "csv/115.csv",
    "csv/116.csv", "csv/117.csv", "csv/121.csv", "csv/123.csv", "csv/124.csv",
    "csv/125.csv", "csv/127.csv", "csv/128.csv", "csv/133.csv", "csv/134.csv"
  ]
};

// Firebase初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
      console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${defaultPath}`);
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId = serviceAccount.project_id;

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });

  console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})\n`);
}

const db: Firestore = admin.firestore();

function trim(v?: string | null): string | null {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function normalizeStr(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "").replace(/株式会社|有限会社|合同会社|合名会社/g, "");
}

function normalizeAddress(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "");
}

interface CSVRecord {
  source: string;
  rowIndex: number;
  name: string;
  address: string | null;
  corporateNumber: string | null;
  phoneNumber: string | null;
  postalCode: string | null;
}

interface FirestoreRecord {
  docId: string;
  name: string;
  address: string | null;
  corporateNumber: string | null;
  phoneNumber: string | null;
  postalCode: string | null;
}

async function loadCSVRecords(typeName: string, csvFiles: string[]): Promise<CSVRecord[]> {
  const records: CSVRecord[] = [];

  for (const file of csvFiles) {
    const filePath = path.resolve(file);
    if (!fs.existsSync(filePath)) {
      console.warn(`  ⚠️  ファイルが見つかりません: ${file}`);
      continue;
    }

    const buf = fs.readFileSync(filePath);
    try {
      const parsed = parse(buf, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_records_with_error: true,
      });

      parsed.forEach((row: any, idx: number) => {
        const name = trim(row["会社名"]) ?? trim(row["企業名"]) ?? trim(row["name"]);
        if (!name) return;

        const address = trim(row["会社住所"]) ?? trim(row["住所"]) ?? trim(row["address"]);
        const corporateNumber = trim(row["法人番号"]) ?? trim(row["corporateNumber"]);
        const phoneNumber = trim(row["電話番号"]) ?? trim(row["phone"]) ?? trim(row["phoneNumber"]);
        const postalCode = trim(row["会社郵便番号"]) ?? trim(row["郵便番号"]) ?? trim(row["postalCode"]);

        records.push({
          source: path.basename(file),
          rowIndex: idx + 1,
          name,
          address,
          corporateNumber,
          phoneNumber,
          postalCode,
        });
      });
    } catch (err: any) {
      console.warn(`  ⚠️ ${path.basename(file)}: CSVパースエラー - ${err.message}`);
    }
  }

  return records;
}

async function findMatchingFirestoreRecords(csvRecords: CSVRecord[]): Promise<Map<string, FirestoreRecord[]>> {
  const matches = new Map<string, FirestoreRecord[]>();

  // Firestoreから全データを取得（バッチ処理で）
  console.log("  🔎 Firestoreのデータを取得中...");
  const allDocs: FirestoreRecord[] = [];
  let lastDoc: any = null;
  const batchSize = 1000;

  while (true) {
    let query = db.collection(COLLECTION_NAME).limit(batchSize);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();
    if (snapshot.empty) break;

    snapshot.docs.forEach(doc => {
      const data = doc.data();
      allDocs.push({
        docId: doc.id,
        name: data.name || null,
        address: data.address || null,
        corporateNumber: data.corporateNumber || null,
        phoneNumber: data.phoneNumber || null,
        postalCode: data.postalCode || null,
      });
    });

    if (snapshot.docs.length < batchSize) break;
    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }

  console.log(`  ✅ Firestore: ${allDocs.length} 件のドキュメントを取得`);

  // CSVレコードごとにマッチング
  for (const csvRecord of csvRecords) {
    const matched: FirestoreRecord[] = [];

    // 法人番号で検索
    if (csvRecord.corporateNumber) {
      const found = allDocs.filter(doc => 
        doc.corporateNumber === csvRecord.corporateNumber
      );
      matched.push(...found);
    }

    // 企業名+住所で検索
    const normName = normalizeStr(csvRecord.name);
    const normAddr = normalizeAddress(csvRecord.address);

    const foundByName = allDocs.filter(doc => {
      const docName = normalizeStr(doc.name);
      const docAddr = normalizeAddress(doc.address);
      
      if (docName !== normName) return false;
      if (normAddr && docAddr && normAddr === docAddr) return true;
      if (csvRecord.postalCode && doc.postalCode && csvRecord.postalCode === doc.postalCode) return true;
      
      return false;
    });

    // 重複を除去
    const foundIds = new Set(matched.map(m => m.docId));
    foundByName.forEach(doc => {
      if (!foundIds.has(doc.docId)) {
        matched.push(doc);
      }
    });

    if (matched.length > 0) {
      matches.set(`${csvRecord.source}:${csvRecord.rowIndex}`, matched);
    }
  }

  return matches;
}

async function checkType(typeName: string, csvFiles: string[]) {
  console.log(`\n${"=".repeat(80)}`);
  console.log(`📊 タイプ${typeName} の確認`);
  console.log(`${"=".repeat(80)}\n`);

  console.log(`📁 CSVファイル数: ${csvFiles.length}`);
  
  // CSVレコードを読み込み
  console.log(`\n📄 CSVファイルを読み込み中...`);
  const csvRecords = await loadCSVRecords(typeName, csvFiles);
  console.log(`✅ CSV総レコード数: ${csvRecords.length}`);

  if (csvRecords.length === 0) {
    console.log(`⚠️  CSVレコードが見つかりませんでした\n`);
    return;
  }

  // Firestoreとマッチング
  console.log(`\n🔍 Firestoreとのマッチング中...`);
  const matches = await findMatchingFirestoreRecords(csvRecords);

  // 統計情報
  const matchedCount = matches.size;
  const unmatchedCount = csvRecords.length - matchedCount;
  const totalFirestoreMatches = Array.from(matches.values()).reduce((sum, arr) => sum + arr.length, 0);
  const duplicateCount = Array.from(matches.values()).filter(arr => arr.length > 1).length;

  console.log(`\n📊 マッチング結果:`);
  console.log(`  ✅ マッチしたCSVレコード: ${matchedCount} 件`);
  console.log(`  ❌ マッチしなかったCSVレコード: ${unmatchedCount} 件`);
  console.log(`  📦 対応するFirestoreドキュメント: ${totalFirestoreMatches} 件`);
  console.log(`  ⚠️  重複（複数のFirestoreドキュメントにマッチ）: ${duplicateCount} 件`);

  // サンプル表示（最初の5件）
  if (matches.size > 0) {
    console.log(`\n📋 マッチングサンプル（最初の5件）:`);
    let count = 0;
    for (const [csvKey, firestoreRecords] of matches.entries()) {
      if (count >= 5) break;
      const [source, rowIndex] = csvKey.split(":");
      const csvRecord = csvRecords.find(r => r.source === source && r.rowIndex === parseInt(rowIndex));
      
      if (csvRecord) {
        console.log(`\n  ${count + 1}. CSV: ${source} (行 ${rowIndex})`);
        console.log(`     企業名: ${csvRecord.name}`);
        console.log(`     住所: ${csvRecord.address || "(なし)"}`);
        console.log(`     法人番号: ${csvRecord.corporateNumber || "(なし)"}`);
        console.log(`     → Firestore: ${firestoreRecords.length} 件マッチ`);
        firestoreRecords.forEach((fs, idx) => {
          console.log(`        ${idx + 1}. docId: ${fs.docId}`);
          console.log(`           企業名: ${fs.name || "(なし)"}`);
          console.log(`           住所: ${fs.address || "(なし)"}`);
        });
      }
      count++;
    }
  }

  // 重複の詳細
  if (duplicateCount > 0) {
    console.log(`\n⚠️  重複の詳細（最初の3件）:`);
    let count = 0;
    for (const [csvKey, firestoreRecords] of matches.entries()) {
      if (firestoreRecords.length <= 1) continue;
      if (count >= 3) break;
      
      const [source, rowIndex] = csvKey.split(":");
      const csvRecord = csvRecords.find(r => r.source === source && r.rowIndex === parseInt(rowIndex));
      
      if (csvRecord) {
        console.log(`\n  ${count + 1}. CSV: ${source} (行 ${rowIndex})`);
        console.log(`     企業名: ${csvRecord.name}`);
        console.log(`     → ${firestoreRecords.length} 件の重複ドキュメント:`);
        firestoreRecords.forEach((fs, idx) => {
          console.log(`        ${idx + 1}. docId: ${fs.docId}`);
        });
      }
      count++;
    }
  }

  // ログファイルに出力
  const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, -5);
  const logPath = path.resolve(`logs/type_${typeName.toLowerCase()}_check_${timestamp}.log`);
  
  if (!fs.existsSync("logs")) {
    fs.mkdirSync("logs", { recursive: true });
  }

  let logContent = `タイプ${typeName} データ確認レポート\n`;
  logContent += `生成日時: ${new Date().toISOString()}\n`;
  logContent += `\n${"=".repeat(80)}\n\n`;
  logContent += `CSVファイル数: ${csvFiles.length}\n`;
  logContent += `CSV総レコード数: ${csvRecords.length}\n`;
  logContent += `マッチしたCSVレコード: ${matchedCount} 件\n`;
  logContent += `マッチしなかったCSVレコード: ${unmatchedCount} 件\n`;
  logContent += `対応するFirestoreドキュメント: ${totalFirestoreMatches} 件\n`;
  logContent += `重複（複数のFirestoreドキュメントにマッチ）: ${duplicateCount} 件\n`;

  fs.writeFileSync(logPath, logContent, "utf8");
  console.log(`\n📄 ログファイルを出力しました: ${logPath}`);
}

async function main() {
  const targetType = process.argv[2]?.toUpperCase();

  if (targetType && !TYPE_DEFINITIONS[targetType]) {
    console.error(`❌ エラー: タイプ "${targetType}" は存在しません`);
    console.error(`\n利用可能なタイプ: ${Object.keys(TYPE_DEFINITIONS).join(", ")}`);
    process.exit(1);
  }

  const typesToCheck = targetType ? { [targetType]: TYPE_DEFINITIONS[targetType] } : TYPE_DEFINITIONS;

  console.log("🔍 全タイプのデータ確認を開始します");
  if (targetType) {
    console.log(`📌 対象タイプ: ${targetType}\n`);
  } else {
    console.log(`📌 全タイプを確認します\n`);
  }

  for (const [typeName, csvFiles] of Object.entries(typesToCheck)) {
    await checkType(typeName, csvFiles);
  }

  console.log(`\n${"=".repeat(80)}`);
  console.log(`✅ 全タイプの確認完了`);
  console.log(`${"=".repeat(80)}\n`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

