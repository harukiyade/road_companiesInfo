/* 
  ドキュメントIDから、どのCSVファイルからインポートされたかを特定するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/find_document_source.ts <document_id>
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";

// ==============================
// Firebase 初期化
// ==============================
let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
if (!serviceAccountPath) {
  // 第1引数がサービスアカウントキーのパスの可能性
  if (process.argv[2] && process.argv[2].endsWith(".json")) {
    serviceAccountPath = process.argv[2];
  } else {
    // デフォルトパスを試す
    const defaultPath = path.join(__dirname, "..", "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
    }
  }
}

if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
  console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
  console.error("   GOOGLE_APPLICATION_CREDENTIALS 環境変数を設定するか、");
  console.error("   第1引数にサービスアカウントキーのパスを指定してください");
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
// メイン処理
// ==============================

async function main() {
  // ドキュメントIDを取得
  const docId = process.argv[process.argv[2]?.endsWith(".json") ? 3 : 2];
  
  if (!docId) {
    console.error("❌ エラー: ドキュメントIDが指定されていません");
    console.error("");
    console.error("   使用方法:");
    console.error("     npx ts-node scripts/find_document_source.ts <document_id>");
    process.exit(1);
  }

  console.log(`\n🔍 ドキュメントID: ${docId}`);
  console.log("   検索中...\n");

  // Firestoreからドキュメントを取得
  const docRef = companiesCol.doc(docId);
  const docSnap = await docRef.get();

  if (!docSnap.exists) {
    console.error(`❌ エラー: ドキュメントID "${docId}" が見つかりません`);
    process.exit(1);
  }

  const docData = docSnap.data();
  console.log("📄 ドキュメント情報:");
  console.log(`   企業名: ${docData?.name || docData?.companyName || "(なし)"}`);
  console.log(`   法人番号: ${docData?.corporateNumber || "(なし)"}`);
  console.log(`   住所: ${docData?.address || docData?.headquartersAddress || "(なし)"}`);
  console.log(`   電話番号: ${docData?.phoneNumber || "(なし)"}`);
  console.log(`   URL: ${docData?.urls?.[0] || docData?.url || "(なし)"}`);

  // CSVファイルを検索
  const csvDir = path.join(__dirname, "..", "csv");
  const csvFiles = fs.readdirSync(csvDir)
    .filter((f) => f.toLowerCase().endsWith(".csv"))
    .map((f) => path.join(csvDir, f))
    .sort();

  console.log(`\n🔍 ${csvFiles.length}個のCSVファイルを検索中...\n`);

  let foundInFiles: string[] = [];
  let matchCount = 0;

  for (const csvFile of csvFiles) {
    const baseName = path.basename(csvFile);
    
    try {
      const buf = fs.readFileSync(csvFile);
      const records: Array<Record<string, string>> = parse(buf, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_records_with_error: true,
      });

      // 各レコードをチェック
      for (let i = 0; i < records.length; i++) {
        const record = records[i];
        
        // 企業名でマッチング
        const csvName = record["会社名"] || record["企業名"] || record["name"] || record["companyName"] || "";
        const docName = docData?.name || docData?.companyName || "";
        
        if (csvName && docName && csvName.trim() === docName.trim()) {
          // 法人番号で確認
          const csvCorpNum = record["法人番号"] || record["corporateNumber"] || "";
          const docCorpNum = docData?.corporateNumber || "";
          
          if (csvCorpNum && docCorpNum && csvCorpNum.trim() === docCorpNum.trim()) {
            foundInFiles.push(`${baseName} (行 ${i + 2})`);
            matchCount++;
            console.log(`✅ マッチ: ${baseName} - 行 ${i + 2}`);
            console.log(`   企業名: ${csvName}`);
            console.log(`   法人番号: ${csvCorpNum}`);
            break;
          } else if (!csvCorpNum && !docCorpNum) {
            // 法人番号が両方ともない場合、住所で確認
            const csvAddress = record["会社住所"] || record["住所"] || record["address"] || record["headquartersAddress"] || "";
            const docAddress = docData?.address || docData?.headquartersAddress || "";
            
            if (csvAddress && docAddress && csvAddress.trim() === docAddress.trim()) {
              foundInFiles.push(`${baseName} (行 ${i + 2})`);
              matchCount++;
              console.log(`✅ マッチ: ${baseName} - 行 ${i + 2}`);
              console.log(`   企業名: ${csvName}`);
              console.log(`   住所: ${csvAddress}`);
              break;
            }
          }
        }
      }
    } catch (err: any) {
      // エラーは無視して続行
      continue;
    }
  }

  console.log(`\n📊 検索結果:`);
  console.log(`   マッチしたCSVファイル数: ${foundInFiles.length}`);
  
  if (foundInFiles.length > 0) {
    console.log(`\n📋 マッチしたファイル:`);
    foundInFiles.forEach((f) => console.log(`   - ${f}`));
    
    // 最も可能性の高いファイルを特定（最初に見つかったもの）
    if (foundInFiles.length > 0) {
      const primaryFile = foundInFiles[0].split(" (")[0];
      console.log(`\n🎯 最も可能性の高いソースCSV: ${primaryFile}`);
    }
  } else {
    console.log(`\n⚠️  マッチするCSVファイルが見つかりませんでした`);
    console.log(`   ドキュメントID "${docId}" は、既に削除されたCSVファイルからインポートされた可能性があります`);
  }

  process.exit(0);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
