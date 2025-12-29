// scripts/detect_deleted_from_csv_import.ts
//
// csv_import配下のファイルから削除されたドキュメントを検出するスクリプト
//
// 使い方:
//   DRY_RUN=1 npx ts-node scripts/detect_deleted_from_csv_import.ts   // 検出のみ
//   npx ts-node scripts/detect_deleted_from_csv_import.ts             // 検出して復元データファイルを生成
//
// オプション:
//   CSV_IMPORT_DIR="csv_import"  // csv_importディレクトリのパス（デフォルト: csv_import）
//   OUTPUT_FILE="deleted_docs.json"  // 出力ファイル名（デフォルト: deleted_documents_from_csv_import.json）
//
// Firestore 認証:
//   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/serviceAccountKey.json"

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// DRY_RUN=1 のときは検出のみ
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";

// csv_importディレクトリのパス
const CSV_IMPORT_DIR = process.env.CSV_IMPORT_DIR || "csv_import";

// 出力ファイル名
const OUTPUT_FILE = process.env.OUTPUT_FILE || "deleted_documents_from_csv_import.json";

function initFirebaseAdmin() {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  // デフォルトのパスを試す（相対パスと絶対パス）
  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
      path.join(projectRoot, "config", "serviceAccountKey.json"),
      path.join(projectRoot, ".config", "serviceAccountKey.json"),
    ];
    
    for (const defaultPath of defaultPaths) {
      const resolvedPath = path.resolve(defaultPath);
      if (fs.existsSync(resolvedPath)) {
        serviceAccountPath = resolvedPath;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolvedPath}`);
        break;
      }
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ エラー: サービスアカウントキーファイルのパスが指定されていません");
    console.error("");
    console.error("   以下のいずれかの方法でサービスアカウントキーファイルを指定してください:");
    console.error("");
    console.error("   方法1 - 環境変数（推奨）:");
    console.error("     export GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json");
    console.error("     npx ts-node scripts/detect_deleted_from_csv_import.ts");
    console.error("");
    console.error("   方法2 - デフォルトパス:");
    console.error("     プロジェクトルートに以下のいずれかのファイル名で配置:");
    console.error("     - serviceAccountKey.json");
    console.error("     - service-account-key.json");
    console.error("     - firebase-service-account.json");
    console.error("     - albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    console.error("");
    console.error(`   現在の作業ディレクトリ: ${process.cwd()}`);
    process.exit(1);
  }

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(
      `❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`
    );
    process.exit(1);
  }

  const serviceAccount = JSON.parse(
    fs.readFileSync(serviceAccountPath, "utf8")
  );

  const projectId =
    serviceAccount.project_id ||
    process.env.GCLOUD_PROJECT ||
    process.env.GCP_PROJECT ||
    PROJECT_ID;

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });
  console.log(`✅ Firebase Admin initialized (Project ID: ${projectId})`);

  return admin.firestore();
}

// csv_importディレクトリからドキュメントIDを読み込む
function loadDocIdsFromCsvImport(dir: string): string[] {
  const docIds: string[] = [];
  const dirPath = path.resolve(dir);

  if (!fs.existsSync(dirPath)) {
    console.error(`❌ エラー: ディレクトリが見つかりません: ${dirPath}`);
    process.exit(1);
  }

  if (!fs.statSync(dirPath).isDirectory()) {
    console.error(`❌ エラー: パスがディレクトリではありません: ${dirPath}`);
    process.exit(1);
  }

  // ディレクトリ内のすべての.txtファイルを読み込む
  const files = fs.readdirSync(dirPath).filter((f) => f.endsWith(".txt"));

  if (files.length === 0) {
    console.warn(`⚠️  警告: ${dirPath} に.txtファイルが見つかりませんでした`);
    return [];
  }

  console.log(`📂 ${dirPath} から ${files.length} 個のファイルを読み込み中...\n`);

  for (const file of files) {
    const filePath = path.join(dirPath, file);
    const content = fs.readFileSync(filePath, "utf8");
    const lines = content
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0);

    console.log(`  📄 ${file}: ${lines.length} 件のドキュメントID`);
    docIds.push(...lines);
  }

  // 重複を除去
  const uniqueDocIds = Array.from(new Set(docIds));
  console.log(`\n📊 合計: ${docIds.length} 件（重複除去後: ${uniqueDocIds.length} 件）\n`);

  return uniqueDocIds;
}

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  console.log(`\n🔍 csv_import配下のファイルから削除されたドキュメントを検出中...`);
  console.log(`   ディレクトリ: ${CSV_IMPORT_DIR}`);
  console.log(`   出力ファイル: ${OUTPUT_FILE}\n`);

  // csv_importディレクトリからドキュメントIDを読み込む
  const docIdsToCheck = loadDocIdsFromCsvImport(CSV_IMPORT_DIR);

  if (docIdsToCheck.length === 0) {
    console.log(`\n💡 チェックするドキュメントIDがありません`);
    return;
  }

  const deletedDocIds: string[] = [];
  const existingDocIds: string[] = [];
  const checkedCount = docIdsToCheck.length;
  let checked = 0;

  console.log(`📊 ${checkedCount} 件のドキュメントIDをチェック中...\n`);

  // バッチでチェック（効率化のため）
  const BATCH_CHECK_SIZE = 100;

  for (let i = 0; i < docIdsToCheck.length; i += BATCH_CHECK_SIZE) {
    const batch = docIdsToCheck.slice(i, i + BATCH_CHECK_SIZE);
    
    // 並列でチェック
    const checkPromises = batch.map(async (docId) => {
      const docRef = colRef.doc(docId);
      const doc = await docRef.get();
      return { docId, exists: doc.exists };
    });

    const results = await Promise.all(checkPromises);

    for (const { docId, exists } of results) {
      checked++;
      if (exists) {
        existingDocIds.push(docId);
      } else {
        deletedDocIds.push(docId);
        if (deletedDocIds.length <= 20) {
          console.log(`  ❌ 削除されたドキュメントID: ${docId}`);
        }
      }

      if (checked % 100 === 0) {
        console.log(`  📦 チェック中... ${checked}/${checkedCount} (削除: ${deletedDocIds.length}, 存在: ${existingDocIds.length})`);
      }
    }
  }

  console.log(`\n✅ 検出完了`);
  console.log(`  📊 チェック件数: ${checkedCount} 件`);
  console.log(`  ✅ 存在するドキュメント: ${existingDocIds.length} 件`);
  console.log(`  ❌ 削除されたドキュメント: ${deletedDocIds.length} 件`);

  if (deletedDocIds.length === 0) {
    console.log(`\n💡 削除されたドキュメントは見つかりませんでした`);
    return;
  }

  // 削除されたドキュメントIDのリストを出力
  const outputData = {
    detectedAt: new Date().toISOString(),
    sourceDirectory: CSV_IMPORT_DIR,
    totalChecked: checkedCount,
    existingCount: existingDocIds.length,
    deletedCount: deletedDocIds.length,
    deletedDocIds: deletedDocIds.sort((a, b) => {
      // 数値の場合は数値順、それ以外は文字列順
      const aNum = parseInt(a, 10);
      const bNum = parseInt(b, 10);
      if (!isNaN(aNum) && !isNaN(bNum)) {
        return aNum - bNum;
      }
      return a.localeCompare(b);
    }),
    restoreData: deletedDocIds.map((docId) => ({
      docId,
      data: {
        // 注意: 削除されたデータは復元できません
        // CSVや他のデータソースからデータを取得する必要があります
        name: null,
        corporateNumber: null,
        companyUrl: null,
        // 他のフィールドも必要に応じて追加
      },
    })),
  };

  // JSONファイルに出力
  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(outputData, null, 2), "utf8");
  console.log(`\n💾 検出結果を保存しました: ${OUTPUT_FILE}`);

  console.log(`\n📋 削除されたドキュメントID一覧（最初の50件）:`);
  const displayIds = deletedDocIds.slice(0, 50);
  console.log(`   ${displayIds.join(", ")}`);
  if (deletedDocIds.length > 50) {
    console.log(`   ... (他 ${deletedDocIds.length - 50} 件)`);
  }

  console.log(`\n💡 次のステップ:`);
  console.log(`   1. ${OUTPUT_FILE} を確認してください`);
  console.log(`   2. 削除されたドキュメントのデータをCSVや他のデータソースから取得してください`);
  console.log(`   3. restoreData セクションにデータを追加してください`);
  console.log(`   4. scripts/restore_deleted_documents.ts で復元してください`);
  console.log(`\n   例:`);
  console.log(`     RESTORE_DATA_FILE=${OUTPUT_FILE} npx ts-node scripts/restore_deleted_documents.ts`);
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
