/* 
  1.csv、53.csv、126.csvの重複企業情報を整理するスクリプト
  
  53.csvを正として、1.csvと126.csvからインポートされた重複ドキュメントを削除します。
  
  使い方:
    DRY_RUN=1 npx tsx scripts/cleanup_duplicate_from_1_53_126.ts  # 削除せず候補だけログ
    npx tsx scripts/cleanup_duplicate_from_1_53_126.ts             # 実際に削除
*/

import "dotenv/config";
import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference, WriteBatch } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const PRIMARY_CSV = "53.csv"; // 正とするCSVファイル
const DUPLICATE_CSVS = ["1.csv", "126.csv"]; // 削除対象のCSVファイル
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
  if (DRY_RUN) {
    console.log("🔍 DRY_RUN モード: Firestore は削除しません\n");
  } else {
    console.log("⚠️  本番モード: Firestore から重複ドキュメントを削除します\n");
  }

  // 53.csvを読み込む
  const csvPath = path.join(process.cwd(), "csv", PRIMARY_CSV);
  if (!fs.existsSync(csvPath)) {
    console.error(`❌ エラー: ${PRIMARY_CSV} が見つかりません: ${csvPath}`);
    process.exit(1);
  }

  console.log(`📄 ${PRIMARY_CSV} を読み込み中...`);
  const buf = fs.readFileSync(csvPath);
  const records: Array<Record<string, string>> = parse(buf, {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    skip_records_with_error: true,
  });

  console.log(`   読み込み完了: ${records.length} 件の企業情報\n`);

  // 53.csvの企業情報をキー化して保持
  const primaryCompanies = new Map<string, {
    name: string;
    corporateNumber: string;
    address: string;
    row: number;
  }>();

  for (let i = 0; i < records.length; i++) {
    const record = records[i];
    const name = normalizeString(record["会社名"] || record["企業名"] || record["name"] || record["companyName"] || "");
    const corporateNumber = normalizeString(record["法人番号"] || record["corporateNumber"] || "");
    const address = normalizeString(record["会社住所"] || record["住所"] || record["address"] || record["headquartersAddress"] || "");

    if (!name) continue;

    const companyKey = generateCompanyKey({ name, corporateNumber, address });
    if (companyKey) {
      primaryCompanies.set(companyKey, {
        name,
        corporateNumber,
        address,
        row: i + 2, // ヘッダー行を考慮
      });
    }
  }

  console.log(`📊 ${PRIMARY_CSV} から ${primaryCompanies.size} 件の企業キーを生成しました\n`);

  // Firestoreから全ドキュメントを取得して、削除候補を特定
  console.log("🔍 Firestore からドキュメントを取得中...");

  let lastDoc: any = null;
  let totalScanned = 0;
  const deleteCandidates: Array<{ docId: string; key: string; source: string; data: any }> = [];
  const companyDocMap = new Map<string, Array<{ docId: string; source: string; data: any }>>();

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

      // 53.csvに含まれる企業かチェック
      if (primaryCompanies.has(companyKey)) {
        // ソース情報を取得
        const sourceFile = data.source?.file || data.lastImportSource?.file || "";
        const isFromDuplicateCsv = DUPLICATE_CSVS.some(csv => sourceFile === csv || sourceFile.includes(csv));

        // 同じ企業キーを持つドキュメントをグループ化
        if (!companyDocMap.has(companyKey)) {
          companyDocMap.set(companyKey, []);
        }
        companyDocMap.get(companyKey)!.push({ 
          docId: doc.id, 
          source: sourceFile || "(不明)",
          data 
        });
      }

      lastDoc = doc;
    }

    if (totalScanned % 10000 === 0) {
      console.log(`  📊 スキャン中... (${totalScanned} 件)`);
    }
  }

  console.log(`\n📊 スキャン完了: ${totalScanned} 件のドキュメントを確認`);
  console.log(`📊 53.csvの企業と一致: ${companyDocMap.size} 件の企業キーで一致を検出\n`);

  // 削除候補を特定
  for (const [companyKey, docs] of companyDocMap.entries()) {
    if (docs.length === 0) continue;

    const primaryInfo = primaryCompanies.get(companyKey)!;
    
    // 53.csvからインポートされたドキュメントを特定（保持するもの）
    // 優先順位: 1) source.file が "53.csv" のもの, 2) 法人番号と一致するドキュメントID, 3) source.file が "1.csv" や "126.csv" でないもの
    let keepDoc: { docId: string; source: string; data: any } | null = null;
    const deleteDocs: Array<{ docId: string; source: string; data: any }> = [];

    // 1. source.file が "53.csv" のものを探す
    const from53csv = docs.find(d => d.source === "53.csv" || d.source.includes("53.csv"));
    if (from53csv) {
      keepDoc = from53csv;
      deleteDocs.push(...docs.filter(d => d.docId !== from53csv.docId));
    } else {
      // 2. 法人番号と一致するドキュメントIDを探す
      if (primaryInfo.corporateNumber) {
        const matchingDoc = docs.find(d => d.docId === primaryInfo.corporateNumber);
        if (matchingDoc) {
          keepDoc = matchingDoc;
          deleteDocs.push(...docs.filter(d => d.docId !== matchingDoc.docId));
        }
      }

      // 3. source.file が "1.csv" や "126.csv" でないものを探す
      if (!keepDoc) {
        const notFromDuplicate = docs.find(d => {
          const source = d.source || "";
          return !DUPLICATE_CSVS.some(csv => source === csv || source.includes(csv));
        });
        if (notFromDuplicate) {
          keepDoc = notFromDuplicate;
          deleteDocs.push(...docs.filter(d => d.docId !== notFromDuplicate.docId));
        }
      }

      // 4. 見つからなかった場合、最初のドキュメントを保持
      if (!keepDoc) {
        keepDoc = docs[0];
        deleteDocs.push(...docs.slice(1));
      }
    }

    // 1.csv または 126.csv からインポートされたドキュメントを削除候補に追加
    for (const deleteDoc of deleteDocs) {
      const isFromDuplicateCsv = DUPLICATE_CSVS.some(csv => 
        deleteDoc.source === csv || 
        deleteDoc.source.includes(csv)
      );

      // 1.csv または 126.csv からインポートされたことが明確なもののみを削除
      if (isFromDuplicateCsv) {
        deleteCandidates.push({
          docId: deleteDoc.docId,
          key: companyKey,
          source: deleteDoc.source,
          data: deleteDoc.data,
        });
      }
    }

    if (deleteDocs.length > 0 && (deleteCandidates.length <= 20 || docs.length > 1)) {
      console.log(`🔍 重複検出: ${primaryInfo.name}`);
      console.log(`   保持: ${keepDoc.docId} (source: ${keepDoc.source})`);
      console.log(`   削除候補: ${deleteDocs.map(d => `${d.docId} (source: ${d.source})`).join(", ")}`);
    }
  }

  console.log(`\n📊 削除候補: ${deleteCandidates.length} 件のドキュメント\n`);

  if (deleteCandidates.length === 0) {
    console.log("✅ 削除対象の重複ドキュメントは見つかりませんでした");
    process.exit(0);
  }

  // 削除を実行
  if (DRY_RUN) {
    console.log("💡 DRY_RUN モードのため、削除は実行されませんでした");
    console.log(`   削除予定: ${deleteCandidates.length} 件`);
    
    // ソース別の集計
    const sourceCounts = new Map<string, number>();
    for (const candidate of deleteCandidates) {
      const source = candidate.source || "(不明)";
      sourceCounts.set(source, (sourceCounts.get(source) || 0) + 1);
    }
    console.log("\n📊 ソース別削除予定数:");
    for (const [source, count] of sourceCounts.entries()) {
      console.log(`   ${source}: ${count} 件`);
    }
  } else {
    console.log(`🗑️  重複ドキュメントを削除中...`);

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

    console.log(`\n✅ 削除完了: ${deletedCount} 件の重複ドキュメントを削除しました`);
  }

  process.exit(0);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
