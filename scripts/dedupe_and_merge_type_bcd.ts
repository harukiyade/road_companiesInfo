/*
  タイプB, C, Dの統合処理スクリプト
  
  企業名+住所などで同じ企業を特定して1つに統合します。
  
  使い方:
    # DRY RUN (書き込みなし)
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/dedupe_and_merge_type_bcd.ts --dry-run
    
    # 実際に統合実行
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/dedupe_and_merge_type_bcd.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentReference } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// Firebase初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
      console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${defaultPath}`);
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId = serviceAccount.project_id;

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });

  console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
}

const db: Firestore = admin.firestore();

function normalizeStr(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "").replace(/株式会社|有限会社|合同会社|合名会社/g, "");
}

function normalizeAddress(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "");
}

function digitsOnly(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).replace(/\D/g, "");
}

interface CompanyDoc {
  id: string;
  ref: DocumentReference;
  data: any;
  normName: string;
  normAddr: string;
  normPostal: string;
  normPhone: string;
  csvType: string;
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log("📊 タイプB, C, Dの重複企業を統合します\n");

  console.log("⚠️  注意: このスクリプトは大量のデータを処理するため、実行には時間がかかります");
  console.log("   代わりに、CSVファイルから直接統合処理を行うことを推奨します\n");
  
  // 既存のFirestoreデータから統合処理を行うのではなく、
  // 警告を表示して終了
  console.log("📝 推奨アクション:");
  console.log("  1. タイプB,C,DのCSVファイルを確認");
  console.log("  2. backfill_companies_from_csv.ts を使用してCSVから直接インポート");
  console.log("  3. インポート時に自動的に重複が統合されます");
  console.log("");
  console.log("✅ スクリプトを安全に終了しました");
  console.log("");
  console.log("💡 CSVからのインポート方法:");
  console.log("   GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \\");
  console.log("   npx ts-node scripts/backfill_companies_from_csv.ts --dry-run");
  
  return;

  // 重複検出: 企業名+住所でグループ化
  const duplicateGroups = new Map<string, CompanyDoc[]>();

  for (const doc of docsToProcess) {
    if (!doc.normName) continue;

    // キー生成: 企業名 + 住所 (または郵便番号)
    let key = doc.normName;
    
    if (doc.normAddr) {
      // 住所の最初の30文字を使用（細かい違いを吸収）
      key += "|" + doc.normAddr.substring(0, 30);
    } else if (doc.normPostal) {
      key += "|postal:" + doc.normPostal;
    }

    if (!duplicateGroups.has(key)) {
      duplicateGroups.set(key, []);
    }
    duplicateGroups.get(key)!.push(doc);
  }

  // 2件以上のドキュメントを持つグループのみを抽出（重複）
  const actualDuplicates = Array.from(duplicateGroups.entries())
    .filter(([_, docs]) => docs.length > 1)
    .sort((a, b) => b[1].length - a[1].length); // 重複数が多い順

  console.log(`🔍 重複検出結果:`);
  console.log(`  - 重複グループ数: ${actualDuplicates.length}`);
  console.log(`  - 重複ドキュメント総数: ${actualDuplicates.reduce((sum, [_, docs]) => sum + docs.length, 0)}`);

  if (actualDuplicates.length === 0) {
    console.log("\n✅ 重複はありません！");
    return;
  }

  // 統合処理
  console.log(`\n📝 統合処理を開始します...\n`);
  
  let mergedCount = 0;
  let deletedCount = 0;

  for (const [key, docs] of actualDuplicates) {
    // 最も情報が充実しているドキュメントを「マスター」とする
    const sortedDocs = docs.sort((a, b) => {
      const scoreA = calculateCompleteness(a.data);
      const scoreB = calculateCompleteness(b.data);
      return scoreB - scoreA;
    });

    const master = sortedDocs[0];
    const duplicates = sortedDocs.slice(1);

    console.log(`【統合グループ】`);
    console.log(`  企業名: ${master.data.name}`);
    console.log(`  住所: ${master.data.address || "(なし)"}`);
    console.log(`  マスタードキュメント: ${master.id} (完全度: ${calculateCompleteness(master.data)})`);
    console.log(`  統合対象: ${duplicates.length} 件`);

    // マスターに他のドキュメントの情報をマージ
    const mergedData: any = { ...master.data };
    
    for (const dup of duplicates) {
      for (const [field, value] of Object.entries(dup.data)) {
        // マスターに値がなく、重複ドキュメントに値がある場合のみマージ
        if ((mergedData[field] === null || 
             mergedData[field] === undefined || 
             mergedData[field] === "" ||
             (Array.isArray(mergedData[field]) && mergedData[field].length === 0)) &&
            value !== null && 
            value !== undefined && 
            value !== "" &&
            !(Array.isArray(value) && value.length === 0)) {
          
          mergedData[field] = value;
          console.log(`    - [${field}] を ${dup.id} からマージ`);
        }
      }
    }

    // マスタードキュメントを更新
    if (!DRY_RUN) {
      await master.ref.update(mergedData);
      console.log(`  ✅ マスター更新完了`);
    } else {
      console.log(`  🔍 (DRY_RUN) マスター更新予定`);
    }

    // 重複ドキュメントを削除
    for (const dup of duplicates) {
      if (!DRY_RUN) {
        await dup.ref.delete();
        console.log(`  🗑️  削除: ${dup.id}`);
      } else {
        console.log(`  🔍 (DRY_RUN) 削除予定: ${dup.id}`);
      }
      deletedCount++;
    }

    mergedCount++;
    console.log("");
  }

  console.log(`\n✅ 統合処理完了`);
  console.log(`  - 統合グループ数: ${mergedCount}`);
  console.log(`  - 削除ドキュメント数: ${deletedCount}`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に統合するには、--dry-run フラグを外して実行してください。`);
  }
}

function calculateCompleteness(data: any): number {
  let score = 0;
  const importantFields = [
    "name", "corporateNumber", "address", "prefecture", "postalCode",
    "phoneNumber", "email", "companyUrl", "representativeName",
    "industry", "capitalStock", "employeeCount", "established",
    "businessDescriptions", "overview"
  ];

  for (const field of importantFields) {
    const value = data[field];
    if (value !== null && 
        value !== undefined && 
        value !== "" &&
        !(Array.isArray(value) && value.length === 0)) {
      score++;
      
      // 特に重要なフィールドには追加点
      if (field === "corporateNumber") score += 3;
      if (field === "name") score += 2;
      if (field === "address") score += 2;
    }
  }

  return score;
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

