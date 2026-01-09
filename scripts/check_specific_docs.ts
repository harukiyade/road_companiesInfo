// scripts/check_specific_docs.ts
//
// 特定のドキュメントIDのoverview/companyDescriptionを確認するスクリプト

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

function initFirebaseAdmin() {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
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
      "/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
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

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
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

// 企業説明のパターン（「〇〇な会社」「〇〇する会社」など）
// 句点（。）で終わる場合も考慮
const COMPANY_DESCRIPTION_PATTERNS = [
  /な会社[。]?$/,
  /する会社[。]?$/,
  /する企業[。]?$/,
  /な企業[。]?$/,
  /を.*?会社[。]?$/,
  /を.*?企業[。]?$/,
  /として.*?会社[。]?$/,
  /として.*?企業[。]?$/,
  /である会社[。]?$/,
  /である企業[。]?$/,
  /手掛ける会社[。]?$/,
  /手がける会社[。]?$/,
  /手掛ける企業[。]?$/,
  /手がける企業[。]?$/,
];

// 企業説明の特徴（「〇〇な会社」などのパターン）
function isLikelyCompanyDescription(text: string): boolean {
  return COMPANY_DESCRIPTION_PATTERNS.some(pattern => pattern.test(text));
}

// 概要の特徴（端的で簡潔な説明）
function isLikelyOverview(text: string): boolean {
  // まず企業説明パターンをチェック（企業説明パターンの場合は概要ではない）
  if (isLikelyCompanyDescription(text)) {
    return false;
  }
  // 短い文章（200文字以下）は概要の可能性が高い
  if (text.length <= 200) {
    return true;
  }
  // 具体的な数値や日付が含まれる（概要の特徴）
  if (/\d{4}年|\d+年|\d+月|\d+日|\d+人|\d+社|\d+億|\d+万円/.test(text)) {
    return true;
  }
  return false;
}

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  // 確認したいドキュメントID
  const docIds = process.argv.slice(2);
  if (docIds.length === 0) {
    console.error("❌ ドキュメントIDを指定してください");
    console.error("   例: npx ts-node scripts/check_specific_docs.ts 1766735978514000004 2711182");
    process.exit(1);
  }

  for (const docId of docIds) {
    console.log(`\n📄 ドキュメントID: ${docId}`);
    console.log("=".repeat(80));
    
    const doc = await colRef.doc(docId).get();
    if (!doc.exists) {
      console.log("❌ ドキュメントが見つかりません");
      continue;
    }

    const data = doc.data();
    const overview = (data as any).overview;
    const companyDescription = (data as any).companyDescription;

    console.log(`\n📝 overview:`);
    if (overview) {
      console.log(`   長さ: ${overview.length}文字`);
      console.log(`   内容: ${overview}`);
      console.log(`   判定: ${isLikelyCompanyDescription(overview) ? "❌ 企業説明パターン（companyDescriptionに移動すべき）" : isLikelyOverview(overview) ? "✅ 概要パターン" : "⚠️  不明"}`);
    } else {
      console.log("   (空)");
    }

    console.log(`\n📝 companyDescription:`);
    if (companyDescription) {
      console.log(`   長さ: ${companyDescription.length}文字`);
      console.log(`   内容: ${companyDescription}`);
      console.log(`   判定: ${isLikelyCompanyDescription(companyDescription) ? "✅ 企業説明パターン" : isLikelyOverview(companyDescription) ? "❌ 概要パターン（overviewに移動すべき）" : "⚠️  不明"}`);
    } else {
      console.log("   (空)");
    }

    // 両方に値がある場合の推奨アクション
    if (overview && companyDescription) {
      const overviewIsDesc = isLikelyCompanyDescription(overview);
      const companyDescIsDesc = isLikelyCompanyDescription(companyDescription);
      const overviewIsOver = isLikelyOverview(overview);
      const companyDescIsOver = isLikelyOverview(companyDescription);

      console.log(`\n🔧 推奨アクション:`);
      if (overviewIsDesc && companyDescIsOver) {
        console.log("   入れ替えが必要: overviewとcompanyDescriptionを交換");
      } else if (overviewIsDesc) {
        console.log("   overviewをcompanyDescriptionに移動");
      } else if (companyDescIsOver) {
        console.log("   companyDescriptionをoverviewに移動");
      } else if (overviewIsOver && companyDescIsDesc) {
        console.log("   ✅ 正しい配置（変更不要）");
      } else {
        console.log("   ⚠️  要確認（自動判定困難）");
      }
    }
  }
}

main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
