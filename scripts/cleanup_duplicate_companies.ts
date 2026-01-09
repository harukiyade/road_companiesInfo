/* 
  101.csvに含まれる企業情報と重複するドキュメントを削除するスクリプト
  
  101.csvを正として、他のCSVからインポートされた重複ドキュメントを削除します。
  
  使い方:
    DRY_RUN=1 npx ts-node scripts/cleanup_duplicate_companies.ts  # 削除せず候補だけログ
    npx ts-node scripts/cleanup_duplicate_companies.ts             # 実際に削除
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference, WriteBatch } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const PRIMARY_CSV = "101.csv"; // 正とするCSVファイル
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";
const BATCH_DELETE_SIZE = 400; // Firestoreのバッチ削除上限（500未満）

// ==============================
// Firebase 初期化
// ==============================
let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
if (!serviceAccountPath) {
  const defaultPath = path.join(__dirname, "..", "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
  if (fs.existsSync(defaultPath)) {
    serviceAccountPath = defaultPath;
  }
}

if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
  console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
  process.exit(1);
}

try {
  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId = serviceAccount.project_id || process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT;

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

const db: Firestore = admin.firestore();
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
    .trim();
}

// 企業情報を表すキーを生成
function generateCompanyKey(company: {
  name?: string;
  corporateNumber?: string;
  address?: string;
  phoneNumber?: string;
}): string {
  const name = normalizeString(company.name);
  const corpNum = normalizeString(company.corporateNumber);
  const address = normalizeString(company.address);
  const phone = normalizeString(company.phoneNumber);

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

  // 企業名と電話番号の組み合わせ
  if (name && phone) {
    const normalizedName = normalizeCompanyName(name);
    return `name_phone:${normalizedName}:${phone}`;
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

  // 101.csvを読み込む
  const csvPath = path.join(__dirname, "..", "csv", PRIMARY_CSV);
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

  // 101.csvの企業情報をキー化して保持
  const primaryCompanies = new Map<string, {
    name: string;
    corporateNumber: string;
    address: string;
    phoneNumber: string;
    row: number;
  }>();

  for (let i = 0; i < records.length; i++) {
    const record = records[i];
    const name = normalizeString(record["会社名"] || record["企業名"] || record["name"] || record["companyName"] || "");
    const corporateNumber = normalizeString(record["法人番号"] || record["corporateNumber"] || "");
    const address = normalizeString(record["会社住所"] || record["住所"] || record["address"] || record["headquartersAddress"] || "");
    const phoneNumber = normalizeString(record["電話番号"] || record["phoneNumber"] || "");

    if (!name) continue;

    const companyKey = generateCompanyKey({ name, corporateNumber, address, phoneNumber });
    if (companyKey) {
      primaryCompanies.set(companyKey, {
        name,
        corporateNumber,
        address,
        phoneNumber,
        row: i + 2, // ヘッダー行を考慮
      });
    }
  }

  console.log(`📊 ${PRIMARY_CSV} から ${primaryCompanies.size} 件の企業キーを生成しました\n`);

  // Firestoreから全ドキュメントを取得して、重複を検出
  console.log("🔍 Firestore からドキュメントを取得中...");

  let lastDoc: any = null;
  let totalScanned = 0;
  let duplicateCandidates: Array<{ docId: string; key: string; data: any }> = [];
  const companyDocMap = new Map<string, Array<{ docId: string; data: any }>>();

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
      const phoneNumber = normalizeString(data.phoneNumber);

      if (!name) continue;

      const companyKey = generateCompanyKey({ name, corporateNumber, address, phoneNumber });
      if (!companyKey) continue;

      // 101.csvに含まれる企業かチェック
      if (primaryCompanies.has(companyKey)) {
        // 同じ企業キーを持つドキュメントをグループ化
        if (!companyDocMap.has(companyKey)) {
          companyDocMap.set(companyKey, []);
        }
        companyDocMap.get(companyKey)!.push({ docId: doc.id, data });
      }

      lastDoc = doc;
    }

    if (totalScanned % 10000 === 0) {
      console.log(`  📊 スキャン中... (${totalScanned} 件)`);
    }
  }

  console.log(`\n📊 スキャン完了: ${totalScanned} 件のドキュメントを確認`);
  console.log(`📊 重複候補: ${companyDocMap.size} 件の企業キーで重複を検出\n`);

  // 重複ドキュメントを特定（各企業キーに対して、複数のドキュメントがある場合）
  for (const [companyKey, docs] of companyDocMap.entries()) {
    if (docs.length > 1) {
      // 複数のドキュメントがある場合、最初の1つを保持し、残りを削除候補に追加
      // ドキュメントIDが数字のみで、かつ法人番号と一致するものを優先
      const primaryInfo = primaryCompanies.get(companyKey)!;
      
      // 優先順位: 1) 法人番号と一致するドキュメントID, 2) 最初に見つかったもの
      let keepDoc: { docId: string; data: any } | null = null;
      const deleteDocs: Array<{ docId: string; data: any }> = [];

      // 法人番号と一致するドキュメントIDを探す
      if (primaryInfo.corporateNumber) {
        const matchingDoc = docs.find(d => d.docId === primaryInfo.corporateNumber);
        if (matchingDoc) {
          keepDoc = matchingDoc;
          deleteDocs.push(...docs.filter(d => d.docId !== matchingDoc.docId));
        }
      }

      // 見つからなかった場合、最初のドキュメントを保持
      if (!keepDoc) {
        keepDoc = docs[0];
        deleteDocs.push(...docs.slice(1));
      }

      // 削除候補に追加
      for (const deleteDoc of deleteDocs) {
        duplicateCandidates.push({
          docId: deleteDoc.docId,
          key: companyKey,
          data: deleteDoc.data,
        });
      }

      if (duplicateCandidates.length <= 10 || docs.length > 1) {
        console.log(`🔍 重複検出: ${primaryInfo.name}`);
        console.log(`   保持: ${keepDoc.docId}`);
        console.log(`   削除候補: ${deleteDocs.map(d => d.docId).join(", ")}`);
      }
    }
  }

  console.log(`\n📊 削除候補: ${duplicateCandidates.length} 件のドキュメント\n`);

  if (duplicateCandidates.length === 0) {
    console.log("✅ 重複ドキュメントは見つかりませんでした");
    process.exit(0);
  }

  // 削除を実行
  if (DRY_RUN) {
    console.log("💡 DRY_RUN モードのため、削除は実行されませんでした");
    console.log(`   削除予定: ${duplicateCandidates.length} 件`);
  } else {
    console.log(`🗑️  重複ドキュメントを削除中...`);

    let batch: WriteBatch = db.batch();
    let batchCount = 0;
    let deletedCount = 0;

    for (const candidate of duplicateCandidates) {
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

