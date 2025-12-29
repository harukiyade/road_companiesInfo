/* 
  更新されたドキュメントIDを取得するスクリプト
  
  処理したCSVファイル（38, 107-125）に関連するドキュメントIDを取得します
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, CollectionReference } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const LIMIT = 50; // 取得件数の上限

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

async function main() {
  console.log("🔍 更新されたドキュメントIDを取得中...\n");

  const documentIds: string[] = [];
  const documentDetails: Array<{
    id: string;
    name: string;
    corporateNumber: string | null;
    prefecture: string | null;
    updatedAt: any;
  }> = [];

  try {
    // 最近更新されたドキュメントを取得（updatedAtでソート）
    const snapshot = await companiesCol
      .orderBy("updatedAt", "desc")
      .limit(LIMIT)
      .get();

    console.log(`📊 取得件数: ${snapshot.size}件\n`);

    snapshot.forEach((doc) => {
      const data = doc.data();
      const id = doc.id;
      documentIds.push(id);
      
      documentDetails.push({
        id,
        name: data.name || "(名前なし)",
        corporateNumber: data.corporateNumber || null,
        prefecture: data.prefecture || null,
        updatedAt: data.updatedAt,
      });
    });

    // 結果を表示
    console.log("=".repeat(80));
    console.log("📋 確認すべきドキュメントID一覧");
    console.log("=".repeat(80));
    console.log("\n");

    documentDetails.forEach((detail, index) => {
      console.log(`${index + 1}. ${detail.id}`);
      console.log(`   会社名: ${detail.name}`);
      if (detail.corporateNumber) {
        console.log(`   法人番号: ${detail.corporateNumber}`);
      }
      if (detail.prefecture) {
        console.log(`   都道府県: ${detail.prefecture}`);
      }
      console.log(`   更新日時: ${detail.updatedAt ? detail.updatedAt.toDate().toLocaleString("ja-JP") : "不明"}`);
      console.log("");
    });

    // ファイルに保存
    const outputFile = path.resolve("updated_document_ids.txt");
    const content = documentIds.join("\n");
    fs.writeFileSync(outputFile, content, "utf8");
    console.log(`\n📝 ドキュメントID一覧を保存しました: ${outputFile}`);

    // 詳細情報も保存
    const detailFile = path.resolve("updated_document_details.txt");
    const detailContent = documentDetails
      .map((d) => `${d.id}\t${d.name}\t${d.corporateNumber || ""}\t${d.prefecture || ""}`)
      .join("\n");
    fs.writeFileSync(detailFile, `ID\t会社名\t法人番号\t都道府県\n${detailContent}`, "utf8");
    console.log(`📝 詳細情報を保存しました: ${detailFile}`);

    // サンプルとして最初の10件のIDを表示
    console.log("\n" + "=".repeat(80));
    console.log("📌 サンプルドキュメントID（最初の10件）");
    console.log("=".repeat(80));
    documentIds.slice(0, 10).forEach((id, index) => {
      console.log(`${index + 1}. ${id}`);
    });

  } catch (err: any) {
    console.error("❌ エラー:", err.message);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
});

