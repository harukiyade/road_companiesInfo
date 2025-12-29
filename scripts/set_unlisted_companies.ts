/* 
  上場企業以外の全てのドキュメントのlistingフィールドに「非上場」を設定するスクリプト

  処理内容:
    - listing="上場" の企業はそのまま
    - listingがnullまたは「上場」以外の値の企業に「非上場」を設定

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/set_unlisted_companies.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type {
  Firestore,
  CollectionReference,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

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
// メイン処理
// ==============================

async function main() {
  if (DRY_RUN) {
    console.log("🔍 ドライランモード: 実際の更新は行いません\n");
  }

  console.log("🔍 全ドキュメントを取得中...");
  
  const batchSize = 500;
  let batch: WriteBatch | null = null;
  let batchCount = 0;
  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalSkipped = 0;
  let lastDoc: any = null;

  const stats = {
    alreadyListed: 0,      // 既に「上場」が設定されている
    alreadyUnlisted: 0,    // 既に「非上場」が設定されている
    setToUnlisted: 0,     // 「非上場」に設定した
    nullToUnlisted: 0,     // nullから「非上場」に設定した
  };

  while (true) {
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(batchSize);
    
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();

    if (snapshot.empty) {
      break;
    }

    for (const doc of snapshot.docs) {
      const data = doc.data();
      const listing = data.listing;
      const name = data.name || doc.id;

      totalProcessed++;

      // 既に「上場」が設定されている場合はスキップ
      if (listing === "上場") {
        stats.alreadyListed++;
        totalSkipped++;
        continue;
      }

      // 既に「非上場」が設定されている場合もスキップ
      if (listing === "非上場") {
        stats.alreadyUnlisted++;
        totalSkipped++;
        continue;
      }

      // listingがnullまたは「上場」以外の値の場合、「非上場」を設定
      if (!batch) {
        batch = db.batch();
      }

      batch.update(doc.ref, {
        listing: "非上場",
        updatedAt: admin.firestore.FieldValue.serverTimestamp(),
      });

      if (listing === null || listing === undefined) {
        stats.nullToUnlisted++;
      } else {
        stats.setToUnlisted++;
      }

      totalUpdated++;
      batchCount++;

      // 最初の10件は詳細ログを出力
      if (totalUpdated <= 10) {
        console.log(`  ✅ 更新: "${name}" (${doc.id}) - listing: ${listing || "null"} → "非上場"`);
      }

      if (batchCount >= batchSize) {
        if (!DRY_RUN) {
          await batch.commit();
        }
        console.log(`  ✅ バッチコミット: ${totalUpdated} 件更新 (合計処理: ${totalProcessed} 件)`);
        batch = null;
        batchCount = 0;
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];

    // 進捗表示
    if (totalProcessed % 5000 === 0 || snapshot.size < batchSize) {
      console.log(`📊 処理中... ${totalProcessed} 件処理済み (更新: ${totalUpdated} 件, スキップ: ${totalSkipped} 件)`);
    }
  }

  // 残りのバッチをコミット
  if (batch && batchCount > 0) {
    if (!DRY_RUN) {
      await batch.commit();
    }
    console.log(`  ✅ 最終バッチコミット: ${totalUpdated} 件更新`);
  }

  // 統計を表示
  console.log("\n" + "=".repeat(80));
  console.log("📊 処理結果");
  console.log("=".repeat(80));
  console.log(`  📝 処理したドキュメント数: ${totalProcessed} 件`);
  console.log(`  ✅ 「非上場」に設定したドキュメント: ${totalUpdated} 件`);
  console.log(`    - nullから「非上場」に設定: ${stats.nullToUnlisted} 件`);
  console.log(`    - その他の値から「非上場」に設定: ${stats.setToUnlisted} 件`);
  console.log(`  ⏭️  スキップしたドキュメント: ${totalSkipped} 件`);
  console.log(`    - 既に「上場」が設定されている: ${stats.alreadyListed} 件`);
  console.log(`    - 既に「非上場」が設定されている: ${stats.alreadyUnlisted} 件`);
  console.log("=".repeat(80));

  if (DRY_RUN) {
    console.log("\n🔍 ドライランモードのため、実際の更新は行われませんでした");
  }
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
