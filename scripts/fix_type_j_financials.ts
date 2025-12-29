/* 
  タイプJのcompanies_newコレクションの財務情報フィールドを実値に更新するスクリプト
  
  対象フィールド:
    - capitalStock
    - revenue
    - profit
    - latestRevenue
    - latestProfit
    - financials
  
  千円単位の値を1000倍して実値に変換します。
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
    npx ts-node scripts/fix_type_j_financials.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const BATCH_LIMIT = 500; // Firestoreのバッチ制限

// タイプJのCSVファイルを識別
function isTypeJCSV(filePath: string): boolean {
  const typeJFiles = ["csv/133.csv", "csv/134.csv", "csv/135.csv", "csv/136.csv"];
  return typeJFiles.some(f => filePath.endsWith(f));
}

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

// 財務数値フィールドのリスト
const FINANCIAL_FIELDS = [
  "capitalStock",
  "revenue",
  "profit",
  "latestRevenue",
  "latestProfit",
  "financials"
];

// 数値を1000倍する（千円単位から実値に変換）
function convertToActualValue(value: any): number | null {
  if (value === null || value === undefined) return null;
  
  // 数値の場合
  if (typeof value === "number") {
    // 既に大きな値（1億以上）の場合は変換済みと判断
    if (value >= 100000000) {
      return value; // 既に実値の可能性が高い
    }
    // 千円単位の値を実値に変換
    return value * 1000;
  }
  
  // 文字列の場合
  if (typeof value === "string") {
    const cleaned = value.replace(/[^\d.-]/g, "");
    if (!cleaned) return null;
    const num = Number(cleaned);
    if (!Number.isFinite(num)) return null;
    
    // 既に大きな値（1億以上）の場合は変換済みと判断
    if (num >= 100000000) {
      return num; // 既に実値の可能性が高い
    }
    // 千円単位の値を実値に変換
    return num * 1000;
  }
  
  return null;
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード: Firestore は書き換えません\n" : "⚠️  本番モード: Firestore を書き換えます\n");

  // タイプJのCSVファイルから企業を特定
  console.log("📖 タイプJのCSVファイルを読み込み中...");
  const typeJFiles = ["csv/133.csv", "csv/134.csv"];
  const typeJCorporateNumbers = new Set<string>();
  
  for (const file of typeJFiles) {
    const filePath = path.resolve(file);
    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️  ファイルが見つかりません: ${filePath}`);
      continue;
    }
    
    const csvContent = fs.readFileSync(filePath, "utf8");
    const { parse } = await import("csv-parse/sync");
    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      bom: true,
    });
    
    for (const record of records) {
      const recordData = record as Record<string, string>;
      const corporateNumber = recordData["法人番号"]?.trim();
      if (corporateNumber && /^\d{13}$/.test(corporateNumber)) {
        typeJCorporateNumbers.add(corporateNumber);
      }
    }
    
    console.log(`  ✅ ${path.basename(file)}: ${records.length} 行、法人番号 ${typeJCorporateNumbers.size} 件`);
  }
  
  console.log(`\n📊 タイプJの企業数: ${typeJCorporateNumbers.size} 件\n`);

  // タイプJの企業を取得して財務数値を更新
  console.log("🔄 財務数値を実値に変換中...");
  
  let updatedCount = 0;
  let skippedCount = 0;
  let batchCount = 0;
  let batch: WriteBatch = db.batch();
  
  for (const corporateNumber of typeJCorporateNumbers) {
    try {
      // 法人番号でドキュメントを検索
      const docRef = companiesCol.doc(corporateNumber);
      const doc = await docRef.get();
      
      if (!doc.exists) {
        // 法人番号がdocIdでない場合、corporateNumberフィールドで検索
        const snap = await companiesCol
          .where("corporateNumber", "==", corporateNumber)
          .limit(1)
          .get();
        
        if (snap.empty) {
          skippedCount++;
          continue;
        }
        
        const updateData: Record<string, any> = {};
        const data = snap.docs[0].data();
        let hasUpdate = false;
        
        // 財務数値フィールドを変換
        for (const field of FINANCIAL_FIELDS) {
          const currentValue = data[field];
          if (currentValue !== null && currentValue !== undefined) {
            const convertedValue = convertToActualValue(currentValue);
            if (convertedValue !== null && convertedValue !== currentValue) {
              updateData[field] = convertedValue;
              hasUpdate = true;
            }
          }
        }
        
        if (hasUpdate) {
          batch.update(snap.docs[0].ref, updateData);
          updatedCount++;
          batchCount++;
          
          if (batchCount >= BATCH_LIMIT) {
            if (!DRY_RUN) {
              await batch.commit();
            }
            console.log(`  💾 バッチコミット (${batchCount} 件) ...`);
            batch = db.batch();
            batchCount = 0;
          }
        } else {
          skippedCount++;
        }
      } else {
        // 法人番号がdocIdの場合
        const data = doc.data();
        const updateData: Record<string, any> = {};
        let hasUpdate = false;
        
        // 財務数値フィールドを変換
        for (const field of FINANCIAL_FIELDS) {
          const currentValue = data?.[field];
          if (currentValue !== null && currentValue !== undefined) {
            const convertedValue = convertToActualValue(currentValue);
            if (convertedValue !== null && convertedValue !== currentValue) {
              updateData[field] = convertedValue;
              hasUpdate = true;
            }
          }
        }
        
        if (hasUpdate) {
          batch.update(docRef, updateData);
          updatedCount++;
          batchCount++;
          
          if (batchCount >= BATCH_LIMIT) {
            if (!DRY_RUN) {
              await batch.commit();
            }
            console.log(`  💾 バッチコミット (${batchCount} 件) ...`);
            batch = db.batch();
            batchCount = 0;
          }
        } else {
          skippedCount++;
        }
      }
    } catch (error: any) {
      console.error(`  ❌ エラー (corporateNumber: ${corporateNumber}): ${error.message}`);
    }
    
    if ((updatedCount + skippedCount) % 500 === 0) {
      console.log(`  進捗: 更新 ${updatedCount} 件、スキップ ${skippedCount} 件`);
    }
  }
  
  // 残りのバッチをコミット
  if (batchCount > 0) {
    if (!DRY_RUN) {
      await batch.commit();
    }
    console.log(`  💾 最後のバッチコミット (${batchCount} 件) ...`);
  }
  
  console.log(`\n✅ 処理完了`);
  console.log(`   📊 タイプJ企業総数: ${typeJCorporateNumbers.size} 件`);
  console.log(`   ✅ 更新件数: ${updatedCount} 件`);
  console.log(`   ⏭️  スキップ件数: ${skippedCount} 件`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});


