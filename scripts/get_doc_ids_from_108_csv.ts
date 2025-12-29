/* 
  108.csvからインポートされたドキュメントIDを取得するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/get_doc_ids_from_108_csv.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import admin from "firebase-admin";
import { Firestore } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const CSV_FILE = path.join(process.cwd(), "csv", "108.csv");
const OUTPUT_FILE = path.join(
  process.cwd(),
  `108_csv_doc_ids_${Date.now()}.txt`
);

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
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
    process.exit(1);
  }
  if (!fs.existsSync(serviceAccountPath)) {
    console.error(`❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
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
      projectId,
    });

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
  } catch (err: any) {
    console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
    console.error(`   詳細: ${err.message}`);
    process.exit(1);
  }
}

const db: Firestore = admin.firestore();
const companiesCol: admin.firestore.CollectionReference = db.collection(COLLECTION_NAME);

// メイン処理
async function main() {
  console.log("📄 108.csvからインポートされたドキュメントIDを取得します\n");

  if (!fs.existsSync(CSV_FILE)) {
    console.error(`❌ エラー: ${CSV_FILE} が見つかりません`);
    process.exit(1);
  }

  const content = fs.readFileSync(CSV_FILE, "utf8");
  const records: Record<string, string>[] = parse(content, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_records_with_error: true,
  });

  if (records.length === 0) {
    console.log("❌ CSVに有効なレコードがありません");
    return;
  }

  console.log(`📋 レコード数: ${records.length}\n`);

  const results: Array<{
    rowNumber: number;
    companyName: string;
    docId: string | null;
    found: boolean;
  }> = [];

  let foundCount = 0;
  let notFoundCount = 0;

  // 最初の100件と最後の100件、およびランダムに100件を取得
  const sampleIndices = new Set<number>();
  
  // 最初の100件
  for (let i = 0; i < Math.min(100, records.length); i++) {
    sampleIndices.add(i);
  }
  
  // 最後の100件
  for (let i = Math.max(0, records.length - 100); i < records.length; i++) {
    sampleIndices.add(i);
  }
  
  // 中間からランダムに100件
  const middleStart = Math.floor(records.length / 3);
  const middleEnd = Math.floor((records.length * 2) / 3);
  for (let i = 0; i < 100 && i < records.length; i++) {
    const randomIndex = Math.floor(Math.random() * (middleEnd - middleStart)) + middleStart;
    sampleIndices.add(randomIndex);
  }

  const indices = Array.from(sampleIndices).sort((a, b) => a - b);

  console.log(`🔍 ${indices.length}件のサンプルを検索します...\n`);

  for (const index of indices) {
    const row = records[index];
    const rowNumber = index + 2; // ヘッダー行を考慮
    const companyName = row["会社名"]?.trim() || "";

    if (!companyName) {
      continue;
    }

    try {
      // 企業名で検索
      const snap = await companiesCol
        .where("name", "==", companyName)
        .limit(1)
        .get();

      if (!snap.empty) {
        const docId = snap.docs[0].id;
        results.push({
          rowNumber,
          companyName,
          docId,
          found: true,
        });
        foundCount++;
        
        if (foundCount <= 20) {
          console.log(`✅ [行${rowNumber}] ${companyName} → docId: ${docId}`);
        }
      } else {
        results.push({
          rowNumber,
          companyName,
          docId: null,
          found: false,
        });
        notFoundCount++;
        
        if (notFoundCount <= 10) {
          console.log(`❌ [行${rowNumber}] ${companyName} → 見つかりませんでした`);
        }
      }
    } catch (err: any) {
      console.error(`⚠️  [行${rowNumber}] エラー: ${err.message}`);
      results.push({
        rowNumber,
        companyName,
        docId: null,
        found: false,
      });
      notFoundCount++;
    }
  }

  // 全件検索（オプション）
  const searchAll = process.env.SEARCH_ALL === "true";
  
  if (searchAll) {
    console.log(`\n🔍 全件検索を開始します...\n`);
    
    for (let i = 0; i < records.length; i++) {
      const row = records[i];
      const rowNumber = i + 2;
      const companyName = row["会社名"]?.trim() || "";

      if (!companyName) {
        continue;
      }

      // 既に検索済みの場合はスキップ
      if (sampleIndices.has(i)) {
        continue;
      }

      try {
        const snap = await companiesCol
          .where("name", "==", companyName)
          .limit(1)
          .get();

        if (!snap.empty) {
          const docId = snap.docs[0].id;
          results.push({
            rowNumber,
            companyName,
            docId,
            found: true,
          });
          foundCount++;
        } else {
          results.push({
            rowNumber,
            companyName,
            docId: null,
            found: false,
          });
          notFoundCount++;
        }

        if ((i + 1) % 1000 === 0) {
          console.log(`  進捗: ${i + 1}/${records.length}件 (見つかった: ${foundCount}件)`);
        }
      } catch (err: any) {
        console.error(`⚠️  [行${rowNumber}] エラー: ${err.message}`);
        notFoundCount++;
      }
    }
  }

  // 結果をファイルに出力
  results.sort((a, b) => a.rowNumber - b.rowNumber);

  const outputLines: string[] = [];
  outputLines.push("=".repeat(80));
  outputLines.push("108.csvからインポートされたドキュメントID一覧");
  outputLines.push("=".repeat(80));
  outputLines.push(`検索日時: ${new Date().toISOString()}`);
  outputLines.push(`検索件数: ${results.length}件`);
  outputLines.push(`見つかった: ${foundCount}件`);
  outputLines.push(`見つからなかった: ${notFoundCount}件`);
  outputLines.push("");
  outputLines.push("行番号 | 企業名 | ドキュメントID");
  outputLines.push("-".repeat(80));

  for (const result of results) {
    const status = result.found ? "✅" : "❌";
    outputLines.push(
      `${status} [行${result.rowNumber}] ${result.companyName} → ${result.docId || "見つかりませんでした"}`
    );
  }

  fs.writeFileSync(OUTPUT_FILE, outputLines.join("\n"), "utf8");

  console.log("\n" + "=".repeat(80));
  console.log("✅ 検索完了");
  console.log(`   見つかった: ${foundCount}件`);
  console.log(`   見つからなかった: ${notFoundCount}件`);
  console.log(`   結果ファイル: ${OUTPUT_FILE}`);
  console.log("=".repeat(80));
  console.log("\n📝 最初の20件のドキュメントID:");
  console.log("-".repeat(80));
  
  const foundResults = results.filter((r) => r.found).slice(0, 20);
  for (const result of foundResults) {
    console.log(`  ${result.docId} - ${result.companyName}`);
  }
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});
