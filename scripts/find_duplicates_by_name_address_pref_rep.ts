/* 
  companies_newコレクションで重複企業情報を検出するスクリプト
  
  特定方法: 企業名＋住所＋都道府県＋代表者名が一致しているもの
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/find_duplicates_by_name_address_pref_rep.ts [--output report.txt]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// 出力ファイル（オプション）
const OUTPUT_FILE = process.argv.includes("--output")
  ? process.argv[process.argv.indexOf("--output") + 1]
  : null;

// 1回のクエリで読む件数
const PAGE_SIZE = 1000;

// Firebase初期化
function initFirebaseAdmin(): Firestore {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(
      projectRoot,
      "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"
    );
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
      console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${defaultPath}`);
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error(
      "❌ エラー: 環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されていないか、ファイルが見つかりません"
    );
    process.exit(1);
  }

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
    projectId,
  });

  console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
  return admin.firestore();
}

// 文字列正規化（比較用）
function normalizeString(v: string | null | undefined): string {
  if (!v) return "";
  return String(v)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/株式会社|有限会社|合同会社|合名会社|合資会社/g, "");
}

// 住所正規化（比較用）
function normalizeAddress(v: string | null | undefined): string {
  if (!v) return "";
  return String(v)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[都道府県市区町村]/g, "");
}

// 代表者名正規化（比較用）
function normalizeRepresentativeName(v: string | null | undefined): string {
  if (!v) return "";
  return String(v)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/代表取締役|代表取締役社長|代表取締役会長|代表取締役専務|代表取締役常務|代表取締役副社長|取締役社長|取締役会長|社長|会長|専務|常務|副社長|代表|代表者|CEO|ceo/g, "")
    .replace(/[（(].*?[）)]/g, ""); // カッコ内を除去
}

// 都道府県正規化
function normalizePrefecture(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().replace(/[都道府県]/g, "");
}

// 重複キーを生成（企業名＋住所＋都道府県＋代表者名）
function generateDuplicateKey(data: DocumentData): string {
  const name = normalizeString(data.name);
  const address = normalizeAddress(data.address);
  const prefecture = normalizePrefecture(data.prefecture);
  const representativeName = normalizeRepresentativeName(data.representativeName);

  return `${name}|${address}|${prefecture}|${representativeName}`;
}

// フィールドの埋まり具合を計算
function countFilledFields(data: DocumentData): number {
  let count = 0;
  for (const [key, value] of Object.entries(data)) {
    if (key === "createdAt" || key === "updatedAt") continue;

    if (value !== null && value !== undefined && value !== "") {
      if (Array.isArray(value)) {
        if (value.length > 0) count++;
      } else {
        count++;
      }
    }
  }
  return count;
}

// 軽量な情報のみを保持（メモリ効率化）
interface LightweightDuplicateInfo {
  key: string;
  docIds: string[];
  filledFieldsCounts: number[];
  // 最初のドキュメントの情報のみ保持（表示用）
  firstDoc: {
    name: string;
    address: string;
    prefecture: string;
    representativeName: string;
  };
}

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  console.log(
    `\n🔍 重複企業検出開始: collection="${COLLECTION_NAME}"\n`
  );

  // 第1パス: キーとドキュメントIDのみを収集（メモリ効率化）
  const duplicateGroups = new Map<string, LightweightDuplicateInfo>();
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
  let scanned = 0;

  console.log("📦 第1パス: ドキュメントをスキャン中（キーとIDのみ収集）...");

  while (true) {
    let query = colRef
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(PAGE_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snap = await query.get();
    if (snap.empty) {
      break;
    }

    for (const doc of snap.docs) {
      scanned++;
      const data = doc.data();

      // 必須フィールドチェック（企業名は必須）
      if (!data.name) {
        continue;
      }

      const key = generateDuplicateKey(data);
      const filledFieldsCount = countFilledFields(data);

      if (!duplicateGroups.has(key)) {
        duplicateGroups.set(key, {
          key,
          docIds: [],
          filledFieldsCounts: [],
          firstDoc: {
            name: data.name || "",
            address: data.address || "",
            prefecture: data.prefecture || "",
            representativeName: data.representativeName || "",
          },
        });
      }

      const group = duplicateGroups.get(key)!;
      group.docIds.push(doc.id);
      group.filledFieldsCounts.push(filledFieldsCount);
    }

    lastDoc = snap.docs[snap.docs.length - 1];

    if (scanned % 10000 === 0) {
      console.log(`  進行中... ${scanned}件スキャン完了`);
    }
  }

  console.log(`✅ スキャン完了: ${scanned}件\n`);

  console.log(`✅ 第1パス完了: ${scanned}件スキャン完了\n`);

  // 2件以上のグループのみを抽出（重複）
  const duplicateKeys = Array.from(duplicateGroups.entries())
    .filter(([_, group]) => group.docIds.length > 1)
    .sort((a, b) => b[1].docIds.length - a[1].docIds.length); // 重複数が多い順

  console.log(`🔍 重複検出結果:`);
  console.log(`  - 重複グループ数: ${duplicateKeys.length}`);
  console.log(
    `  - 重複ドキュメント総数: ${duplicateKeys.reduce(
      (sum, [_, group]) => sum + group.docIds.length,
      0
    )}`
  );

  if (duplicateKeys.length === 0) {
    console.log("\n✅ 重複はありません！\n");
    return;
  }

  // レポート生成（軽量データのみ使用）
  let report = `重複企業情報レポート\n`;
  report += `生成日時: ${new Date().toISOString()}\n`;
  report += `検出条件: 企業名＋住所＋都道府県＋代表者名が一致\n`;
  report += `重複グループ数: ${duplicateKeys.length}\n`;
  report += `重複ドキュメント総数: ${duplicateKeys.reduce(
    (sum, [_, group]) => sum + group.docIds.length,
    0
  )}\n`;
  report += `\n${"=".repeat(80)}\n\n`;

  // 第2パス: 重複グループごとに詳細情報を取得（必要に応じて）
  console.log("📝 第2パス: レポート生成中...\n");

  for (let i = 0; i < duplicateKeys.length; i++) {
    const [key, group] = duplicateKeys[i];

    report += `【重複グループ ${i + 1}】\n`;
    report += `  企業名: ${group.firstDoc.name || "(なし)"}\n`;
    report += `  住所: ${group.firstDoc.address || "(なし)"}\n`;
    report += `  都道府県: ${group.firstDoc.prefecture || "(なし)"}\n`;
    report += `  代表者名: ${group.firstDoc.representativeName || "(なし)"}\n`;
    report += `  重複数: ${group.docIds.length}件\n\n`;

    // 各ドキュメントの詳細
    const maxFields = Math.max(...group.filledFieldsCounts);
    for (let j = 0; j < group.docIds.length; j++) {
      const isMaster = group.filledFieldsCounts[j] === maxFields;
      const masterMark = isMaster ? " [マスター候補]" : "";

      report += `  ${j + 1}. ドキュメントID: ${group.docIds[j]}${masterMark}\n`;
      report += `     フィールド数: ${group.filledFieldsCounts[j]}\n`;
      report += `\n`;
    }

    report += `${"-".repeat(80)}\n\n`;

    if ((i + 1) % 1000 === 0) {
      console.log(`  進行中... ${i + 1}/${duplicateKeys.length}グループ処理完了`);
    }
  }

  // コンソール出力（最初の10グループのみ）
  console.log("\n📋 重複グループ詳細（最初の10グループ）:\n");
  for (let i = 0; i < Math.min(10, duplicateKeys.length); i++) {
    const [_, group] = duplicateKeys[i];
    console.log(`【重複グループ ${i + 1}】`);
    console.log(`  企業名: ${group.firstDoc.name || "(なし)"}`);
    console.log(`  住所: ${group.firstDoc.address || "(なし)"}`);
    console.log(`  都道府県: ${group.firstDoc.prefecture || "(なし)"}`);
    console.log(`  代表者名: ${group.firstDoc.representativeName || "(なし)"}`);
    console.log(`  重複数: ${group.docIds.length}件`);
    console.log(`  ドキュメントID: ${group.docIds.slice(0, 5).join(", ")}${group.docIds.length > 5 ? ` ... (他${group.docIds.length - 5}件)` : ""}`);
    console.log("");
  }

  if (duplicateKeys.length > 10) {
    console.log(`  ... 他 ${duplicateKeys.length - 10}グループ\n`);
  }

  // ファイル出力
  if (OUTPUT_FILE) {
    const outputPath = path.resolve(process.cwd(), OUTPUT_FILE);
    fs.writeFileSync(outputPath, report, "utf8");
    console.log(`\n📝 レポートを出力しました: ${outputPath}`);
  } else {
    const defaultOutputPath = path.resolve(
      process.cwd(),
      `duplicate_report_${Date.now()}.txt`
    );
    fs.writeFileSync(defaultOutputPath, report, "utf8");
    console.log(`\n📝 レポートを出力しました: ${defaultOutputPath}`);
  }

  // 統計情報
  console.log(`\n📊 統計情報:`);
  const duplicateCounts = duplicateKeys.map(([_, g]) => g.docIds.length);
  const maxDuplicates = Math.max(...duplicateCounts);
  const avgDuplicates =
    duplicateCounts.reduce((a, b) => a + b, 0) / duplicateCounts.length;

  console.log(`  - 最大重複数: ${maxDuplicates}件`);
  console.log(`  - 平均重複数: ${avgDuplicates.toFixed(2)}件`);
  console.log(`  - 2件重複: ${duplicateCounts.filter((c) => c === 2).length}グループ`);
  console.log(`  - 3件以上重複: ${duplicateCounts.filter((c) => c >= 3).length}グループ`);

  console.log(`\n✅ 処理完了\n`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

