/**
 * companies_new コレクションのフィールド構成を調査するスクリプト
 * 
 * 複数のサンプルドキュメントを取得して、使用されているすべてのフィールド名を抽出します。
 * 
 * 使い方:
 *   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
 *   npx ts-node scripts/inspect_companies_new_fields.ts [--sample-size=100]
 */

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";

const COLLECTION_NAME = "companies_new";

// サンプルサイズ（デフォルト100件）
const SAMPLE_SIZE = parseInt(
  process.argv.find(arg => arg.startsWith("--sample-size="))?.split("=")[1] || "100",
  10
);

// Firebase 初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
    ];
    for (const p of defaultPaths) {
      const resolved = path.resolve(p);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolved}`);
        break;
      }
    }
  }

  if (!serviceAccountPath) {
    console.error("❌ サービスアカウント JSON のパスを指定してください");
    console.error("   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId =
    serviceAccount.project_id ||
    process.env.GCLOUD_PROJECT ||
    process.env.GCP_PROJECT;

  if (!projectId) {
    console.error("❌ Project ID が取得できませんでした");
    process.exit(1);
  }

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });
  console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
}

const db = admin.firestore();
const col = db.collection(COLLECTION_NAME);

interface FieldInfo {
  count: number;
  types: Set<string>;
  sampleValues: any[];
}

async function main() {
  console.log(`🔍 companies_new コレクションのフィールド構成を調査します...\n`);
  console.log(`📊 サンプルサイズ: ${SAMPLE_SIZE} 件\n`);

  // フィールド情報を収集
  const fieldMap = new Map<string, FieldInfo>();

  // ランダムにサンプルを取得（複数のクエリで分散して取得）
  const FETCH_BATCH_SIZE = 1000;
  let lastDoc: any = null;
  let totalFetched = 0;
  let sampled = 0;

  while (sampled < SAMPLE_SIZE) {
    let query = col.orderBy(admin.firestore.FieldPath.documentId()).limit(FETCH_BATCH_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();

    if (snapshot.empty) {
      console.log("⚠️  これ以上取得できるドキュメントがありません");
      break;
    }

    // サンプルを取得（必要な分だけ）
    const remaining = SAMPLE_SIZE - sampled;
    const docsToProcess = snapshot.docs.slice(0, Math.min(remaining, snapshot.docs.length));

    for (const doc of docsToProcess) {
      const data = doc.data() || {};
      
      // 各フィールドを分析
      for (const [fieldName, fieldValue] of Object.entries(data)) {
        if (!fieldMap.has(fieldName)) {
          fieldMap.set(fieldName, {
            count: 0,
            types: new Set(),
            sampleValues: [],
          });
        }

        const fieldInfo = fieldMap.get(fieldName)!;
        fieldInfo.count += 1;

        // 型を記録
        if (fieldValue === null) {
          fieldInfo.types.add("null");
        } else if (Array.isArray(fieldValue)) {
          fieldInfo.types.add("array");
          if (fieldValue.length > 0) {
            fieldInfo.types.add(`array<${typeof fieldValue[0]}>`);
          }
        } else {
          fieldInfo.types.add(typeof fieldValue);
        }

        // サンプル値を記録（最大5個まで）
        if (fieldInfo.sampleValues.length < 5 && fieldValue !== null && fieldValue !== undefined) {
          if (typeof fieldValue === "string" && fieldValue.length > 100) {
            fieldInfo.sampleValues.push(fieldValue.substring(0, 100) + "...");
          } else if (Array.isArray(fieldValue) && fieldValue.length > 3) {
            fieldInfo.sampleValues.push([...fieldValue.slice(0, 3), `... (${fieldValue.length} items)`]);
          } else {
            fieldInfo.sampleValues.push(fieldValue);
          }
        }
      }

      sampled += 1;
      if (sampled % 50 === 0) {
        console.log(`  📦 処理中... ${sampled}/${SAMPLE_SIZE} 件`);
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    totalFetched += snapshot.size;

    if (sampled >= SAMPLE_SIZE) {
      break;
    }
  }

  console.log(`\n✅ 分析完了: ${sampled} 件のドキュメントを分析しました\n`);

  // フィールドをアルファベット順にソート
  const sortedFields = Array.from(fieldMap.entries()).sort((a, b) => a[0].localeCompare(b[0]));

  // 結果を出力
  console.log("=".repeat(80));
  console.log(`📋 フィールド一覧 (全 ${sortedFields.length} フィールド)`);
  console.log("=".repeat(80));
  console.log();

  // 新しいフィールドを特に強調
  const knownNewFields = ["transactionType", "needs", "securityCode"];
  
  for (const [fieldName, fieldInfo] of sortedFields) {
    const isNewField = knownNewFields.includes(fieldName);
    const prefix = isNewField ? "✨ " : "   ";
    const percentage = ((fieldInfo.count / sampled) * 100).toFixed(1);
    
    console.log(`${prefix}${fieldName}`);
    console.log(`     出現率: ${fieldInfo.count}/${sampled} (${percentage}%)`);
    console.log(`     型: ${Array.from(fieldInfo.types).join(", ")}`);
    
    if (fieldInfo.sampleValues.length > 0) {
      console.log(`     サンプル値:`);
      for (const sample of fieldInfo.sampleValues) {
        const sampleStr = typeof sample === "object" ? JSON.stringify(sample) : String(sample);
        console.log(`       - ${sampleStr}`);
      }
    }
    console.log();
  }

  // 新規フィールドのサマリー
  const foundNewFields = sortedFields.filter(([name]) => knownNewFields.includes(name));
  if (foundNewFields.length > 0) {
    console.log("=".repeat(80));
    console.log(`✨ 新規フィールド (${foundNewFields.length} 件)`);
    console.log("=".repeat(80));
    for (const [fieldName, fieldInfo] of foundNewFields) {
      const percentage = ((fieldInfo.count / sampled) * 100).toFixed(1);
      console.log(`  ${fieldName}: ${fieldInfo.count}/${sampled} (${percentage}%) - 型: ${Array.from(fieldInfo.types).join(", ")}`);
    }
    console.log();
  }

  // JSON形式でも出力
  const outputFile = `companies_new_fields_inspection_${Date.now()}.json`;
  const outputData = {
    sampleSize: sampled,
    timestamp: new Date().toISOString(),
    fields: Object.fromEntries(
      sortedFields.map(([name, info]) => [
        name,
        {
          count: info.count,
          percentage: ((info.count / sampled) * 100).toFixed(1),
          types: Array.from(info.types),
          sampleValues: info.sampleValues,
        },
      ])
    ),
  };

  fs.writeFileSync(outputFile, JSON.stringify(outputData, null, 2), "utf8");
  console.log(`💾 詳細な結果をJSONファイルに保存しました: ${outputFile}`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

