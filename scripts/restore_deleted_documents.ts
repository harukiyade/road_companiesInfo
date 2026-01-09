// scripts/restore_deleted_documents.ts
//
// 削除されたドキュメントを復元するスクリプト
//
// 使い方:
//   DRY_RUN=1 npx ts-node scripts/restore_deleted_documents.ts   // 復元せず候補だけログ
//   npx ts-node scripts/restore_deleted_documents.ts             // 実際に復元
//
// ドキュメントIDを指定:
//   DOC_IDS="1,10" npx ts-node scripts/restore_deleted_documents.ts
//
// JSONファイルから復元:
//   RESTORE_DATA_FILE=restore_data.json npx ts-node scripts/restore_deleted_documents.ts
//
// Firestore 認証:
//   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/serviceAccountKey.json"

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// DRY_RUN=1 のときは復元せずログだけ出す
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";

// 復元するドキュメントID（カンマ区切り）
const DOC_IDS = process.env.DOC_IDS
  ? process.env.DOC_IDS.split(",").map((id) => id.trim())
  : [];

// 復元データファイル（JSON形式）
const RESTORE_DATA_FILE = process.env.RESTORE_DATA_FILE;

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
    console.error("     npx ts-node scripts/restore_deleted_documents.ts");
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

// 復元データの型定義
interface RestoreData {
  docId: string;
  data: Record<string, any>;
}

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  const restoreList: RestoreData[] = [];

  // 1. JSONファイルから復元データを読み込む
  if (RESTORE_DATA_FILE) {
    if (!fs.existsSync(RESTORE_DATA_FILE)) {
      console.error(`❌ エラー: 復元データファイルが見つかりません: ${RESTORE_DATA_FILE}`);
      process.exit(1);
    }

    try {
      const fileContent = fs.readFileSync(RESTORE_DATA_FILE, "utf8");
      const data = JSON.parse(fileContent);
      
      // 形式1: 配列形式 [{ docId, data }, ...]
      if (Array.isArray(data)) {
        restoreList.push(...data);
      }
      // 形式2: 単一オブジェクト { docId, data }
      else if (data.docId && data.data) {
        restoreList.push(data);
      }
      // 形式3: 検出結果ファイル形式 { restoreData: [...], deletedDocIds: [...] }
      else if (data.restoreData && Array.isArray(data.restoreData)) {
        restoreList.push(...data.restoreData);
        console.log(`ℹ️  検出結果ファイルから restoreData を読み込みました`);
      }
      // 形式4: deletedDocIds のみがある場合、空のデータで復元リストを作成
      else if (data.deletedDocIds && Array.isArray(data.deletedDocIds)) {
        console.log(`⚠️  警告: restoreData が見つかりません。deletedDocIds から空のデータで復元リストを作成します`);
        for (const docId of data.deletedDocIds) {
          restoreList.push({
            docId,
            data: {
              // 空のデータ（後でCSVなどから補完する必要があります）
            },
          });
        }
      }
      else {
        console.error("❌ エラー: 復元データファイルの形式が正しくありません");
        console.error("   期待される形式:");
        console.error("     - [{ docId: string, data: {...} }, ...]");
        console.error("     - { docId: string, data: {...} }");
        console.error("     - { restoreData: [{ docId: string, data: {...} }, ...] }");
        console.error("     - { deletedDocIds: [string, ...], restoreData: [...] }");
        process.exit(1);
      }
      
      console.log(`📄 復元データファイルから ${restoreList.length} 件のデータを読み込みました`);
    } catch (error: any) {
      console.error(`❌ エラー: 復元データファイルの読み込みに失敗しました: ${error.message}`);
      process.exit(1);
    }
  }

  // 2. DOC_IDSが指定されている場合、既存のドキュメントを確認
  if (DOC_IDS.length > 0) {
    console.log(`\n🔍 指定されたドキュメントIDの状態を確認中...`);
    
    for (const docId of DOC_IDS) {
      const docRef = colRef.doc(docId);
      const doc = await docRef.get();
      
      if (doc.exists) {
        console.log(`⚠️  ドキュメントID "${docId}" は既に存在しています`);
        const existingData = doc.data();
        console.log(`   現在のデータ:`, JSON.stringify(existingData, null, 2));
      } else {
        console.log(`❌ ドキュメントID "${docId}" は存在しません（削除されています）`);
        console.log(`   ⚠️  このドキュメントを復元するには、データを提供する必要があります`);
        console.log(`   方法1: RESTORE_DATA_FILE でJSONファイルを指定`);
        console.log(`   方法2: スクリプト内でデータを手動で指定`);
      }
    }
  }

  // 復元リストが空の場合、警告を出して終了
  if (restoreList.length === 0) {
    console.log(`\n⚠️  復元するデータがありません`);
    console.log(`\n使用方法:`);
    console.log(`  1. DOC_IDS="1,10" でドキュメントIDを指定（既存確認のみ）`);
    console.log(`  2. RESTORE_DATA_FILE=restore_data.json で復元データを指定`);
    console.log(`\n復元データファイルの形式例:`);
    console.log(`  [`);
    console.log(`    {`);
    console.log(`      "docId": "1",`);
    console.log(`      "data": {`);
    console.log(`        "name": "会社名",`);
    console.log(`        "corporateNumber": "1234567890123",`);
    console.log(`        "companyUrl": "https://example.com"`);
    console.log(`      }`);
    console.log(`    }`);
    console.log(`  ]`);
    return;
  }

  console.log(`\n📋 復元対象: ${restoreList.length} 件`);
  
  let restored = 0;
  let skipped = 0;
  let errors = 0;

  for (const restoreItem of restoreList) {
    const { docId, data } = restoreItem;
    
    if (!docId) {
      console.error(`❌ エラー: docIdが指定されていません`);
      errors++;
      continue;
    }

    if (!data || typeof data !== "object") {
      console.error(`❌ エラー: docId="${docId}" のデータが無効です`);
      errors++;
      continue;
    }

    const docRef = colRef.doc(docId);
    const existingDoc = await docRef.get();

    if (existingDoc.exists) {
      console.log(`⏭️  ドキュメントID "${docId}" は既に存在するためスキップします`);
      skipped++;
      continue;
    }

    if (DRY_RUN) {
      console.log(`🔧 [復元候補] docId=${docId}`);
      console.log(`   データ:`, JSON.stringify(data, null, 2));
    } else {
      try {
        await docRef.set(data, { merge: false });
        restored++;
        console.log(`✅ 復元完了: docId=${docId}`);
      } catch (error: any) {
        errors++;
        console.error(`❌ 復元エラー: docId=${docId}, エラー: ${error.message}`);
      }
    }
  }

  console.log(`\n✅ 復元処理完了`);
  console.log(`  ✅ 復元: ${restored} 件`);
  console.log(`  ⏭️  スキップ: ${skipped} 件`);
  console.log(`  ❌ エラー: ${errors} 件`);
  console.log(`  📊 合計: ${restoreList.length} 件`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に復元するには、DRY_RUN=1 を外して実行してください`);
  }
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
