/* 
  証券コード追加結果の確認スクリプト

  既に証券コードがあった企業、新規作成した企業、フィールドを追加した企業の
  ドキュメントIDを出力します。

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/check_securities_code_results.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type {
  Firestore,
  CollectionReference,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const SHOKEN_CODE_CSV_PATH = path.join(__dirname, "../shokenCode/shokenCode.csv");

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

    for (const pth of defaultPaths) {
      const resolved = path.resolve(pth);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolved}`);
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
const companiesCol: CollectionReference = db.collection(COLLECTION_NAME);

// ==============================
// ヘルパー関数
// ==============================

// 証券コードCSVから証券コード一覧を取得
function loadSecuritiesCodes(): Set<string> {
  const codes = new Set<string>();
  
  if (!fs.existsSync(SHOKEN_CODE_CSV_PATH)) {
    console.error(`❌ エラー: 証券コードCSVファイルが見つかりません: ${SHOKEN_CODE_CSV_PATH}`);
    return codes;
  }
  
  const csvContent = fs.readFileSync(SHOKEN_CODE_CSV_PATH, "utf-8");
  const { parse } = require("csv-parse/sync");
  const records: Record<string, string>[] = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
  });
  
  for (const record of records) {
    const code = record["コード"]?.trim();
    if (code) {
      codes.add(code);
    }
  }
  
  return codes;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  console.log("📖 証券コードCSVを読み込み中...");
  const securitiesCodes = loadSecuritiesCodes();
  console.log(`📊 証券コード数: ${securitiesCodes.size} 件\n`);

  // listing="上場" の企業を取得してから、securitiesCodeが設定されているものをフィルタリング
  console.log("🔍 listing='上場' の企業を取得中...");
  const listedSnapshot = await companiesCol
    .where("listing", "==", "上場")
    .get();
  
  console.log(`📊 上場企業数: ${listedSnapshot.size} 件`);
  
  // securitiesCodeが設定されている企業をフィルタリング
  const listedWithCodeSnapshot = {
    docs: listedSnapshot.docs.filter(doc => {
      const data = doc.data();
      return data.securitiesCode != null && data.securitiesCode !== "";
    }),
    size: 0,
    empty: false,
  };
  listedWithCodeSnapshot.size = listedWithCodeSnapshot.docs.length;
  listedWithCodeSnapshot.empty = listedWithCodeSnapshot.docs.length === 0;

  console.log(`📊 証券コードが設定されている上場企業数: ${listedWithCodeSnapshot.size} 件\n`);

  // 証券コードCSVに存在するコードかどうかで分類
  const categories = {
    alreadyHadCode: [] as Array<{ docId: string; name: string; code: string; createdAt: any }>,
    newlyAdded: [] as Array<{ docId: string; name: string; code: string; createdAt: any }>,
    newlyCreated: [] as Array<{ docId: string; name: string; code: string; createdAt: any }>,
  };

  // 現在時刻から1時間前を基準に、最近作成されたかどうかを判定
  const oneHourAgo = Date.now() - 60 * 60 * 1000;

  for (const doc of listedWithCodeSnapshot.docs) {
    const data = doc.data();
    const code = data.securitiesCode;
    const name = data.name || "(名前なし)";
    const createdAt = data.createdAt;
    
    // createdAtがTimestampの場合、ミリ秒に変換
    let createdAtMs: number | null = null;
    if (createdAt) {
      if (createdAt.toMillis) {
        createdAtMs = createdAt.toMillis();
      } else if (typeof createdAt === "number") {
        createdAtMs = createdAt;
      }
    }

    const entry = {
      docId: doc.id,
      name,
      code,
      createdAt: createdAtMs,
    };

    // 証券コードCSVに存在するかチェック
    if (!securitiesCodes.has(code)) {
      // CSVに存在しない = 既にあった可能性が高い（ただし、CSVにないコードもある可能性）
      categories.alreadyHadCode.push(entry);
    } else {
      // CSVに存在する = 今回追加された可能性
      // createdAtが最近（1時間以内）なら新規作成、それ以外なら追加
      if (createdAtMs && createdAtMs > oneHourAgo) {
        categories.newlyCreated.push(entry);
      } else {
        categories.newlyAdded.push(entry);
      }
    }
  }

  // より正確な判定のため、createdAtとupdatedAtを比較
  // updatedAtが最近でcreatedAtが古い = 追加された
  // updatedAtとcreatedAtが両方最近 = 新規作成
  console.log("🔍 より詳細な分類を実行中...\n");
  
  const finalCategories = {
    alreadyHadCode: [] as Array<{ docId: string; name: string; code: string }>,
    newlyAdded: [] as Array<{ docId: string; name: string; code: string }>,
    newlyCreated: [] as Array<{ docId: string; name: string; code: string }>,
  };

  const twoHoursAgo = Date.now() - 2 * 60 * 60 * 1000;

  for (const doc of listedWithCodeSnapshot.docs) {
    const data = doc.data();
    const code = data.securitiesCode;
    const name = data.name || "(名前なし)";
    const createdAt = data.createdAt;
    const updatedAt = data.updatedAt;

    let createdAtMs: number | null = null;
    let updatedAtMs: number | null = null;

    if (createdAt) {
      if (createdAt.toMillis) {
        createdAtMs = createdAt.toMillis();
      } else if (typeof createdAt === "number") {
        createdAtMs = createdAt;
      }
    }

    if (updatedAt) {
      if (updatedAt.toMillis) {
        updatedAtMs = updatedAt.toMillis();
      } else if (typeof updatedAt === "number") {
        updatedAtMs = updatedAt;
      }
    }

    // updatedAtが最近（2時間以内）で、createdAtが古い（2時間以上前） = 追加された
    if (updatedAtMs && updatedAtMs > twoHoursAgo && createdAtMs && createdAtMs < twoHoursAgo) {
      finalCategories.newlyAdded.push({ docId: doc.id, name, code });
    }
    // createdAtとupdatedAtが両方最近 = 新規作成
    else if (createdAtMs && createdAtMs > twoHoursAgo && updatedAtMs && updatedAtMs > twoHoursAgo) {
      finalCategories.newlyCreated.push({ docId: doc.id, name, code });
    }
    // それ以外 = 既にあった
    else {
      finalCategories.alreadyHadCode.push({ docId: doc.id, name, code });
    }
  }

  // 結果を出力
  console.log("=".repeat(80));
  console.log("📊 分類結果");
  console.log("=".repeat(80));
  console.log(`\n1️⃣  既に証券コードがあった企業: ${finalCategories.alreadyHadCode.length} 件`);
  console.log(`2️⃣  証券コードフィールドを追加した企業: ${finalCategories.newlyAdded.length} 件`);
  console.log(`3️⃣  新規作成した企業: ${finalCategories.newlyCreated.length} 件\n`);

  // ドキュメントIDを出力
  console.log("=".repeat(80));
  console.log("1️⃣  既に証券コードがあった企業のドキュメントID");
  console.log("=".repeat(80));
  for (const entry of finalCategories.alreadyHadCode.slice(0, 100)) {
    console.log(`${entry.docId} | ${entry.name} | ${entry.code}`);
  }
  if (finalCategories.alreadyHadCode.length > 100) {
    console.log(`... 他 ${finalCategories.alreadyHadCode.length - 100} 件`);
  }

  console.log("\n" + "=".repeat(80));
  console.log("2️⃣  証券コードフィールドを追加した企業のドキュメントID");
  console.log("=".repeat(80));
  for (const entry of finalCategories.newlyAdded) {
    console.log(`${entry.docId} | ${entry.name} | ${entry.code}`);
  }

  console.log("\n" + "=".repeat(80));
  console.log("3️⃣  新規作成した企業のドキュメントID");
  console.log("=".repeat(80));
  for (const entry of finalCategories.newlyCreated) {
    console.log(`${entry.docId} | ${entry.name} | ${entry.code}`);
  }

  // ファイルにも出力
  const outputDir = path.join(__dirname, "../logs");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  const timestamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const outputFile = path.join(outputDir, `securities_code_results_${timestamp}.txt`);

  let output = "=".repeat(80) + "\n";
  output += "証券コード追加結果\n";
  output += "=".repeat(80) + "\n\n";
  output += `1️⃣  既に証券コードがあった企業: ${finalCategories.alreadyHadCode.length} 件\n`;
  output += `2️⃣  証券コードフィールドを追加した企業: ${finalCategories.newlyAdded.length} 件\n`;
  output += `3️⃣  新規作成した企業: ${finalCategories.newlyCreated.length} 件\n\n`;

  output += "=".repeat(80) + "\n";
  output += "1️⃣  既に証券コードがあった企業のドキュメントID\n";
  output += "=".repeat(80) + "\n";
  for (const entry of finalCategories.alreadyHadCode) {
    output += `${entry.docId} | ${entry.name} | ${entry.code}\n`;
  }

  output += "\n" + "=".repeat(80) + "\n";
  output += "2️⃣  証券コードフィールドを追加した企業のドキュメントID\n";
  output += "=".repeat(80) + "\n";
  for (const entry of finalCategories.newlyAdded) {
    output += `${entry.docId} | ${entry.name} | ${entry.code}\n`;
  }

  output += "\n" + "=".repeat(80) + "\n";
  output += "3️⃣  新規作成した企業のドキュメントID\n";
  output += "=".repeat(80) + "\n";
  for (const entry of finalCategories.newlyCreated) {
    output += `${entry.docId} | ${entry.name} | ${entry.code}\n`;
  }

  fs.writeFileSync(outputFile, output, "utf-8");
  console.log(`\n📄 結果をファイルに保存しました: ${outputFile}`);
}

main()
  .then(() => {
    console.log("\n✅ 処理完了");
    process.exit(0);
  })
  .catch((err) => {
    console.error("\n❌ エラーが発生しました:");
    console.error(err);
    process.exit(1);
  });

