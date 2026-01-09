// scripts/fix_specific_docs.ts
//
// 特定のドキュメントIDに対してoverview/companyDescriptionを修正するスクリプト
//
// 実行例:
//   DRY_RUN=1 npx ts-node scripts/fix_specific_docs.ts 1766735978514000004 2711182
//   npx ts-node scripts/fix_specific_docs.ts 1766735978514000004 2711182

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// DRY_RUN=1 のときは更新せずログだけ出す
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";

// 企業説明のパターン（「〇〇な会社」「〇〇する会社」など）
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

function norm(s: string | null | undefined): string | null {
  if (!s) return null;
  const trimmed = s.toString().trim();
  return trimmed === "" ? null : trimmed;
}

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  // 修正したいドキュメントID
  const docIds = process.argv.slice(2);
  if (docIds.length === 0) {
    console.error("❌ ドキュメントIDを指定してください");
    console.error("   例: npx ts-node scripts/fix_specific_docs.ts 1766735978514000004 2711182");
    process.exit(1);
  }

  console.log(`\n🔍 ${docIds.length}件のドキュメントを処理します (DRY_RUN=${DRY_RUN})\n`);

  let processed = 0;
  let updated = 0;

  for (const docId of docIds) {
    console.log(`\n📄 ドキュメントID: ${docId}`);
    console.log("=".repeat(80));
    
    const docRef = colRef.doc(docId);
    const doc = await docRef.get();
    
    if (!doc.exists) {
      console.log("❌ ドキュメントが見つかりません");
      continue;
    }

    processed += 1;
    const data = doc.data();
    const overview = norm((data as any).overview);
    const companyDescription = norm((data as any).companyDescription);

    console.log(`\n📝 現在の状態:`);
    console.log(`   overview: ${overview || "(空)"}`);
    console.log(`   companyDescription: ${companyDescription || "(空)"}`);

    // 判定
    const overviewIsDescription = overview ? isLikelyCompanyDescription(overview) : false;
    const companyDescriptionIsDescription = companyDescription ? isLikelyCompanyDescription(companyDescription) : false;
    const overviewIsOverview = overview ? isLikelyOverview(overview) : false;
    const companyDescriptionIsOverview = companyDescription ? isLikelyOverview(companyDescription) : false;

    console.log(`\n🔍 判定結果:`);
    console.log(`   overview: ${overviewIsDescription ? "企業説明パターン" : overviewIsOverview ? "概要パターン" : "不明"}`);
    console.log(`   companyDescription: ${companyDescriptionIsDescription ? "企業説明パターン" : companyDescriptionIsOverview ? "概要パターン" : "不明"}`);

    // 修正ロジック
    let updateData: any = {};
    let needsUpdate = false;
    let reason = "";

    // ケース1: overviewに企業説明パターンがあり、companyDescriptionが空または企業説明パターンではない
    if (overview && overviewIsDescription && (!companyDescription || !companyDescriptionIsDescription)) {
      updateData.overview = admin.firestore.FieldValue.delete();
      updateData.companyDescription = overview;
      needsUpdate = true;
      reason = "overviewの企業説明パターンをcompanyDescriptionに移動";
    }
    // ケース2: companyDescriptionに概要パターンがあり、overviewが空または概要パターンではない
    else if (companyDescription && companyDescriptionIsOverview && (!overview || !overviewIsOverview)) {
      updateData.overview = companyDescription;
      updateData.companyDescription = admin.firestore.FieldValue.delete();
      needsUpdate = true;
      reason = "companyDescriptionの概要パターンをoverviewに移動";
    }
    // ケース3: 両方に値があり、入れ替えが必要
    else if (overview && companyDescription && overviewIsDescription && companyDescriptionIsOverview === false && companyDescriptionIsOverview) {
      updateData.overview = companyDescription;
      updateData.companyDescription = overview;
      needsUpdate = true;
      reason = "overviewとcompanyDescriptionを入れ替え";
    }
    // ケース4: 両方に値があり、overviewが企業説明パターン、companyDescriptionが概要パターン → 入れ替え
    else if (overview && companyDescription && overviewIsDescription && companyDescriptionIsOverview) {
      updateData.overview = companyDescription;
      updateData.companyDescription = overview;
      needsUpdate = true;
      reason = "overviewが企業説明パターン、companyDescriptionが概要パターンのため入れ替え";
    }
    // ケース5: 両方に値があり、overviewが企業説明パターン、companyDescriptionが企業説明パターンではない → overviewをcompanyDescriptionに移動
    else if (overview && companyDescription && overviewIsDescription && !companyDescriptionIsDescription) {
      updateData.overview = admin.firestore.FieldValue.delete();
      updateData.companyDescription = overview; // overviewの内容をcompanyDescriptionに移動
      needsUpdate = true;
      reason = "overviewが企業説明パターンのため、companyDescriptionに移動";
    }
    // ケース6: 両方に値があり、companyDescriptionが概要パターン、overviewが概要パターンではない → companyDescriptionをoverviewに移動
    else if (overview && companyDescription && companyDescriptionIsOverview && !overviewIsOverview) {
      updateData.overview = companyDescription; // companyDescriptionの内容をoverviewに移動
      updateData.companyDescription = admin.firestore.FieldValue.delete();
      needsUpdate = true;
      reason = "companyDescriptionが概要パターンのため、overviewに移動";
    }

    if (needsUpdate) {
      console.log(`\n🔧 修正内容: ${reason}`);
      console.log(`   新しいoverview: ${updateData.overview === admin.firestore.FieldValue.delete() ? "(削除)" : (typeof updateData.overview === 'string' ? updateData.overview : "(変更なし)")}`);
      console.log(`   新しいcompanyDescription: ${updateData.companyDescription === admin.firestore.FieldValue.delete() ? "(削除)" : (typeof updateData.companyDescription === 'string' ? updateData.companyDescription : "(変更なし)")}`);

      if (!DRY_RUN) {
        try {
          await docRef.update(updateData);
          updated += 1;
          console.log(`   ✅ 更新完了`);
        } catch (error) {
          console.error(`   ❌ 更新エラー: ${error}`);
        }
      } else {
        console.log(`   [DRY_RUN] 更新は実行されませんでした`);
      }
    } else {
      console.log(`\n✅ 修正不要（既に正しい配置）`);
    }
  }

  console.log(`\n${"=".repeat(80)}`);
  console.log(`✅ 処理完了`);
  console.log(`   処理件数: ${processed}`);
  console.log(`   更新件数: ${updated} (DRY_RUN=${DRY_RUN})`);
}

main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
