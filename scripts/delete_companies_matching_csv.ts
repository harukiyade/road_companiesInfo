/* 
  指定したCSVファイルの企業情報と一致するドキュメントを削除するスクリプト
  
  使い方:
    DRY_RUN=1 npx tsx scripts/delete_companies_matching_csv.ts 53.csv  # 削除せず候補だけログ
    npx tsx scripts/delete_companies_matching_csv.ts 53.csv             # 実際に削除
*/

import "dotenv/config";
import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference, WriteBatch } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";
const BATCH_DELETE_SIZE = 400; // Firestoreのバッチ削除上限（500未満）

// ==============================
// Firebase 初期化
// ==============================
function initFirebaseAdmin(): Firestore {
  if (admin.apps.length) {
    return admin.firestore();
  }

  try {
    admin.initializeApp({
      credential: admin.credential.applicationDefault(),
    });
    console.log("✅ Firebase 初期化完了");
    return admin.firestore();
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", (error as Error).message);
    console.error("   環境変数 GOOGLE_APPLICATION_CREDENTIALS が正しく設定されているか確認してください");
    throw error;
  }
}

const db: Firestore = initFirebaseAdmin();
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// ==============================
// ヘルパー関数
// ==============================

function normalizeString(v: any): string {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

function normalizeCompanyName(name: string): string {
  return name
    .replace(/株式会社/g, "")
    .replace(/有限会社/g, "")
    .replace(/合資会社/g, "")
    .replace(/合名会社/g, "")
    .replace(/合同会社/g, "")
    .trim();
}

function normalizeAddress(addr: string): string {
  return addr
    .replace(/\s+/g, "")
    .replace(/[０-９]/g, (s) => String.fromCharCode(s.charCodeAt(0) - 0xFEE0))
    .replace(/[ー－]/g, "-")
    .replace(/丁目/g, "")
    .replace(/番地/g, "")
    .replace(/番/g, "")
    .trim();
}

// 企業情報を表すキーを生成
function generateCompanyKey(company: {
  name?: string;
  corporateNumber?: string;
  address?: string;
}): string {
  const name = normalizeString(company.name);
  const corpNum = normalizeString(company.corporateNumber);
  const address = normalizeString(company.address);

  // 法人番号がある場合はそれを優先
  if (corpNum) {
    return `corp:${corpNum}`;
  }

  // 企業名と住所の組み合わせ
  if (name && address) {
    const normalizedName = normalizeCompanyName(name);
    const normalizedAddr = normalizeAddress(address);
    return `name_addr:${normalizedName}:${normalizedAddr}`;
  }

  // 企業名のみ
  if (name) {
    const normalizedName = normalizeCompanyName(name);
    return `name:${normalizedName}`;
  }

  return "";
}

// ==============================
// メイン処理
// ==============================

async function main() {
  const csvFileName = process.argv[2];
  if (!csvFileName) {
    console.error("使用方法: npx tsx scripts/delete_companies_matching_csv.ts <csvFileName>");
    console.error("例: npx tsx scripts/delete_companies_matching_csv.ts 53.csv");
    process.exit(1);
  }

  if (DRY_RUN) {
    console.log(`🔍 DRY_RUN モード: Firestore は削除しません\n`);
  } else {
    console.log(`⚠️  本番モード: ${csvFileName} の企業情報と一致するドキュメントを削除します\n`);
  }

  // CSVファイルを読み込む
  const csvPath = path.join(process.cwd(), "csv", csvFileName);
  if (!fs.existsSync(csvPath)) {
    console.error(`❌ エラー: ${csvFileName} が見つかりません: ${csvPath}`);
    process.exit(1);
  }

  console.log(`📄 ${csvFileName} を読み込み中...`);
  const buf = fs.readFileSync(csvPath);
  const records: Array<Record<string, string>> = parse(buf, {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    skip_records_with_error: true,
  });

  console.log(`   読み込み完了: ${records.length} 件の企業情報\n`);

  // CSVの企業情報をキー化して保持
  const csvCompanies = new Map<string, {
    name: string;
    corporateNumber: string;
    address: string;
  }>();

  for (const record of records) {
    const name = normalizeString(record["会社名"] || record["企業名"] || record["name"] || record["companyName"] || "");
    const corporateNumber = normalizeString(record["法人番号"] || record["corporateNumber"] || "");
    const address = normalizeString(record["会社住所"] || record["住所"] || record["address"] || record["headquartersAddress"] || "");

    if (!name) continue;

    const companyKey = generateCompanyKey({ name, corporateNumber, address });
    if (companyKey) {
      csvCompanies.set(companyKey, {
        name,
        corporateNumber,
        address,
      });
    }
  }

  console.log(`📊 ${csvFileName} から ${csvCompanies.size} 件の企業キーを生成しました\n`);

  // Firestoreから一致するドキュメントを検索
  console.log("🔍 Firestore から一致するドキュメントを検索中...");

  let lastDoc: any = null;
  let totalScanned = 0;
  const deleteCandidates: Array<{ docId: string; name: string; key: string }> = [];

  while (true) {
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(1000);
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snapshot = await query.get();
    if (snapshot.empty) break;

    for (const doc of snapshot.docs) {
      totalScanned++;
      const data = doc.data();
      const name = normalizeString(data.name || data.companyName);
      const corporateNumber = normalizeString(data.corporateNumber);
      const address = normalizeString(data.address || data.headquartersAddress);

      if (!name) {
        lastDoc = doc;
        continue;
      }

      const companyKey = generateCompanyKey({ name, corporateNumber, address });
      if (!companyKey) {
        lastDoc = doc;
        continue;
      }

      // CSVの企業情報と一致するかチェック
      if (csvCompanies.has(companyKey)) {
        deleteCandidates.push({
          docId: doc.id,
          name: String(name),
          key: companyKey,
        });
      }

      lastDoc = doc;
    }

    if (totalScanned % 10000 === 0) {
      console.log(`  📊 スキャン中... (${totalScanned} 件)`);
    }
  }

  console.log(`\n📊 スキャン完了: ${totalScanned} 件のドキュメントを確認`);
  console.log(`📊 削除候補: ${deleteCandidates.length} 件のドキュメント\n`);

  if (deleteCandidates.length === 0) {
    console.log(`✅ ${csvFileName} の企業情報と一致するドキュメントは見つかりませんでした`);
    process.exit(0);
  }

  // 最初の10件を表示
  console.log("📋 削除候補の例（最初の10件）:");
  for (let i = 0; i < Math.min(10, deleteCandidates.length); i++) {
    const candidate = deleteCandidates[i];
    console.log(`   ${i + 1}. ${candidate.name} (docId: ${candidate.docId})`);
  }
  if (deleteCandidates.length > 10) {
    console.log(`   ... 他 ${deleteCandidates.length - 10} 件`);
  }
  console.log();

  // 削除を実行
  if (DRY_RUN) {
    console.log("💡 DRY_RUN モードのため、削除は実行されませんでした");
    console.log(`   削除予定: ${deleteCandidates.length} 件`);
  } else {
    console.log(`🗑️  ドキュメントを削除中...`);

    let batch: WriteBatch = db.batch();
    let batchCount = 0;
    let deletedCount = 0;

    for (const candidate of deleteCandidates) {
      batch.delete(companiesCol.doc(candidate.docId));
      batchCount++;

      if (batchCount >= BATCH_DELETE_SIZE) {
        await batch.commit();
        deletedCount += batchCount;
        console.log(`  💾 削除バッチコミット: ${batchCount} 件 (合計: ${deletedCount} 件)`);
        batch = db.batch();
        batchCount = 0;
      }
    }

    // 最後のバッチをコミット
    if (batchCount > 0) {
      await batch.commit();
      deletedCount += batchCount;
      console.log(`  💾 最後の削除バッチコミット: ${batchCount} 件 (合計: ${deletedCount} 件)`);
    }

    console.log(`\n✅ 削除完了: ${deletedCount} 件のドキュメントを削除しました`);
  }

  process.exit(0);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
