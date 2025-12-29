// scripts/detect_deleted_documents.ts
//
// 削除されたドキュメントを自動検出するスクリプト
//
// 使い方:
//   DRY_RUN=1 npx ts-node scripts/detect_deleted_documents.ts   // 検出のみ
//   npx ts-node scripts/detect_deleted_documents.ts             // 検出して復元データファイルを生成
//
// オプション:
//   CHECK_RANGE="1-100"     // チェックするドキュメントIDの範囲（デフォルト: 1-1000）
//   OUTPUT_FILE="deleted_docs.json"  // 出力ファイル名（デフォルト: deleted_documents.json）
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

// チェックするドキュメントIDの範囲
const CHECK_RANGE = process.env.CHECK_RANGE || "1-10000";
const [minId, maxId] = CHECK_RANGE.split("-").map((s) => parseInt(s.trim(), 10));

// チェックするドキュメントIDのリスト（カンマ区切りで指定可能）
const CHECK_DOC_IDS = process.env.CHECK_DOC_IDS
  ? process.env.CHECK_DOC_IDS.split(",").map((id) => id.trim())
  : [];

// 出力ファイル名
const OUTPUT_FILE = process.env.OUTPUT_FILE || "deleted_documents.json";

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
    console.error("     npx ts-node scripts/detect_deleted_documents.ts");
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

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  console.log(`\n🔍 削除されたドキュメントを検出中...`);
  
  const deletedDocIds: string[] = [];
  const existingDocIds: string[] = [];
  
  // チェックするドキュメントIDのリストを作成
  const docIdsToCheck: string[] = [];
  
  if (CHECK_DOC_IDS.length > 0) {
    // 指定されたドキュメントIDをチェック
    docIdsToCheck.push(...CHECK_DOC_IDS);
    console.log(`   指定されたドキュメントID: ${CHECK_DOC_IDS.length} 件`);
  } else {
    // 範囲でチェック
    for (let id = minId; id <= maxId; id++) {
      docIdsToCheck.push(String(id));
    }
    console.log(`   チェック範囲: ドキュメントID ${minId} ～ ${maxId}`);
  }
  
  console.log(`   出力ファイル: ${OUTPUT_FILE}\n`);

  const checkedCount = docIdsToCheck.length;
  let checked = 0;

  // バッチでチェック（効率化のため）
  const BATCH_CHECK_SIZE = 100;

  console.log(`📊 ${docIdsToCheck.length} 件のドキュメントIDをチェック中...\n`);

  // バッチでチェック
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
    console.log(`   チェック範囲を広げる場合は、CHECK_RANGE="1-10000" などで指定してください`);
    return;
  }

  // 削除されたドキュメントIDのリストを出力
  const outputData = {
    detectedAt: new Date().toISOString(),
    checkRange: `${minId}-${maxId}`,
    totalChecked: checkedCount,
    existingCount: existingDocIds.length,
    deletedCount: deletedDocIds.length,
    deletedDocIds: deletedDocIds.sort((a, b) => parseInt(a, 10) - parseInt(b, 10)),
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

  console.log(`\n📋 削除されたドキュメントID一覧:`);
  if (deletedDocIds.length <= 50) {
    console.log(`   ${deletedDocIds.join(", ")}`);
  } else {
    console.log(`   ${deletedDocIds.slice(0, 50).join(", ")} ... (他 ${deletedDocIds.length - 50} 件)`);
  }

  console.log(`\n💡 次のステップ:`);
  console.log(`   1. ${OUTPUT_FILE} を確認してください`);
  console.log(`   2. 削除されたドキュメントのデータをCSVや他のデータソースから取得してください`);
  console.log(`   3. restoreData セクションにデータを追加してください`);
  console.log(`   4. scripts/restore_deleted_documents.ts で復元してください`);
  console.log(`\n   例:`);
  console.log(`     RESTORE_DATA_FILE=${OUTPUT_FILE} npx ts-node scripts/restore_deleted_documents.ts`);
  console.log(`\n   より広範囲をチェックする場合:`);
  console.log(`     CHECK_RANGE="1-100000" npx ts-node scripts/detect_deleted_documents.ts`);
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
