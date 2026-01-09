/**
 * overview/companyDescriptionフィールドがゴールを満たしているか確認するスクリプト
 * 
 * ゴール:
 * - 企業説明フィールド（companyDescription）には「〇〇な会社」などのパターンが入っている
 * - 概要フィールド（overview）には会社の概要を端的に説明している文章が入っている
 * 
 * 使い方:
 *   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
 *   npx ts-node scripts/verify_overview_companydescription_goal.ts
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";
const PAGE_SIZE = 1000;
const SAMPLE_SIZE = 100; // サンプル数

// 企業説明のパターン（「〇〇な会社」「〇〇する会社」など）
const COMPANY_DESCRIPTION_PATTERNS = [
  /な会社$/,
  /する会社$/,
  /する企業$/,
  /な企業$/,
  /を.*?会社$/,
  /を.*?企業$/,
  /として.*?会社$/,
  /として.*?企業$/,
  /である会社$/,
  /である企業$/,
];

// 概要の特徴（端的で簡潔な説明）
function isLikelyOverview(text: string): boolean {
  // 短い文章（200文字以下）で、企業説明パターンを含まない
  if (text.length <= 200 && !COMPANY_DESCRIPTION_PATTERNS.some(pattern => pattern.test(text))) {
    return true;
  }
  // 具体的な数値や日付が含まれる（概要の特徴）
  if (/\d{4}年|\d+年|\d+月|\d+日|\d+人|\d+社|\d+億|\d+万円/.test(text)) {
    return true;
  }
  return false;
}

// 企業説明の特徴（「〇〇な会社」などのパターン）
function isLikelyCompanyDescription(text: string): boolean {
  return COMPANY_DESCRIPTION_PATTERNS.some(pattern => pattern.test(text));
}

function initFirebaseAdmin() {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      "./serviceAccountKey.json",
      "./service-account-key.json",
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
    ];
    for (const p of defaultPaths) {
      const resolved = path.resolve(p);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        break;
      }
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ サービスアカウント JSON のパスを指定してください");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId = serviceAccount.project_id || process.env.GCLOUD_PROJECT || process.env.GCP_PROJECT || PROJECT_ID;

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });

  return admin.firestore();
}

function norm(s: string | null | undefined): string | null {
  if (!s) return null;
  const trimmed = s.toString().trim();
  return trimmed === "" ? null : trimmed;
}

function preview(s: string | null, maxLength: number = 100): string {
  if (!s) return "";
  if (s.length <= maxLength) return s;
  return s.substring(0, maxLength) + "...";
}

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  console.log("🔍 overview/companyDescriptionフィールドのゴール達成状況を確認します...\n");

  // 両方のフィールドに値が入っているドキュメントをサンプリング
  let scanned = 0;
  let sampled = 0;
  let lastDoc: FirebaseFirestore.QueryDocumentSnapshot | null = null;

  const results: Array<{
    docId: string;
    overview: string;
    companyDescription: string;
    overviewIsCorrect: boolean;
    companyDescriptionIsCorrect: boolean;
    issues: string[];
  }> = [];

  while (sampled < SAMPLE_SIZE) {
    let query = colRef.orderBy(admin.firestore.FieldPath.documentId()).limit(PAGE_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snap = await query.get();
    if (snap.empty) break;

    for (const doc of snap.docs) {
      scanned += 1;

      const data = doc.data();
      const overview = norm((data as any).overview);
      const companyDescription = norm((data as any).companyDescription);

      // 両方のフィールドに値が入っている場合のみチェック
      if (overview !== null && companyDescription !== null) {
        sampled += 1;

        const overviewIsCorrect = isLikelyOverview(overview);
        const companyDescriptionIsCorrect = isLikelyCompanyDescription(companyDescription);

        const issues: string[] = [];
        if (!overviewIsCorrect) {
          issues.push("overviewが概要パターンではない");
        }
        if (!companyDescriptionIsCorrect) {
          issues.push("companyDescriptionが企業説明パターンではない");
        }

        results.push({
          docId: doc.id,
          overview: overview,
          companyDescription: companyDescription,
          overviewIsCorrect,
          companyDescriptionIsCorrect,
          issues,
        });

        if (sampled >= SAMPLE_SIZE) break;
      }
    }

    lastDoc = snap.docs[snap.docs.length - 1];
    if (sampled >= SAMPLE_SIZE) break;
  }

  // 結果を分析
  const correctCount = results.filter(r => r.overviewIsCorrect && r.companyDescriptionIsCorrect).length;
  const incorrectCount = results.length - correctCount;

  console.log("=".repeat(80));
  console.log("📊 検証結果");
  console.log("=".repeat(80));
  console.log(`スキャン数: ${scanned} 件`);
  console.log(`サンプル数: ${sampled} 件（両方のフィールドに値が入っているドキュメント）`);
  console.log(`✅ ゴール達成: ${correctCount} 件 (${((correctCount / sampled) * 100).toFixed(1)}%)`);
  console.log(`❌ ゴール未達成: ${incorrectCount} 件 (${((incorrectCount / sampled) * 100).toFixed(1)}%)`);
  console.log();

  // 問題のあるケースを表示
  const incorrectResults = results.filter(r => r.issues.length > 0);
  if (incorrectResults.length > 0) {
    console.log("=".repeat(80));
    console.log("⚠️  問題のあるケース（最大10件表示）");
    console.log("=".repeat(80));
    for (let i = 0; i < Math.min(10, incorrectResults.length); i++) {
      const r = incorrectResults[i];
      console.log(`\n[${i + 1}] Doc ID: ${r.docId}`);
      console.log(`   問題: ${r.issues.join(", ")}`);
      console.log(`   overview (${r.overview.length}文字): ${preview(r.overview, 80)}`);
      console.log(`   companyDescription (${r.companyDescription.length}文字): ${preview(r.companyDescription, 80)}`);
      console.log(`   overviewが概要パターン: ${r.overviewIsCorrect ? "✅" : "❌"}`);
      console.log(`   companyDescriptionが企業説明パターン: ${r.companyDescriptionIsCorrect ? "✅" : "❌"}`);
    }
  }

  // 正しいケースも数件表示
  const correctResults = results.filter(r => r.overviewIsCorrect && r.companyDescriptionIsCorrect);
  if (correctResults.length > 0) {
    console.log("\n" + "=".repeat(80));
    console.log("✅ ゴール達成しているケース（最大5件表示）");
    console.log("=".repeat(80));
    for (let i = 0; i < Math.min(5, correctResults.length); i++) {
      const r = correctResults[i];
      console.log(`\n[${i + 1}] Doc ID: ${r.docId}`);
      console.log(`   overview (${r.overview.length}文字): ${preview(r.overview, 80)}`);
      console.log(`   companyDescription (${r.companyDescription.length}文字): ${preview(r.companyDescription, 80)}`);
    }
  }

  // JSON形式で保存
  const outputFile = `verify_overview_companydescription_goal_${Date.now()}.json`;
  const output = {
    timestamp: new Date().toISOString(),
    scanned,
    sampled,
    correctCount,
    incorrectCount,
    correctPercentage: ((correctCount / sampled) * 100).toFixed(1),
    results,
  };

  fs.writeFileSync(outputFile, JSON.stringify(output, null, 2), "utf8");
  console.log(`\n💾 詳細な結果をJSONファイルに保存しました: ${outputFile}`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

