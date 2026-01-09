// scripts/inspect_overview_companydescription.ts
//
// companies_new コレクション上で、
// overview と companyDescription の両方に値が入っているドキュメントを洗い出すスクリプトです。
//
// 実行例:
//   DRY_RUN=1 npx ts-node scripts/inspect_overview_companydescription.ts   // 詳細ログ出力
//   npx ts-node scripts/inspect_overview_companydescription.ts             // 結果をJSONファイルに出力
//
// 再開オプション:
//   START_FROM_DOC_ID="docId123" npx ts-node scripts/inspect_overview_companydescription.ts
//
// Firestore 認証:
//   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/serviceAccountKey.json"

import admin from "firebase-admin";
import * as fs from "fs";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// 1 回のクエリで読む件数
const PAGE_SIZE = 1000;

// DRY_RUN=1 のときは詳細ログを出力
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";

// 再開オプション
const START_FROM_DOC_ID = process.env.START_FROM_DOC_ID;

// 結果を保存するファイル名
const OUTPUT_FILE = `overview_companydescription_inspection_${Date.now()}.json`;

interface InspectionResult {
  docId: string;
  overview: string | null;
  companyDescription: string | null;
  overviewLength: number;
  companyDescriptionLength: number;
  overviewPreview: string;
  companyDescriptionPreview: string;
}

function initFirebaseAdmin() {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    console.error(
      "❌ エラー: 環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されていません"
    );
    process.exit(1);
  }

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(
      `❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`
    );
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

// 文字列正規化
function norm(s: string | null | undefined): string | null {
  if (!s) return null;
  const trimmed = s.toString().trim();
  return trimmed === "" ? null : trimmed;
}

// プレビュー文字列生成（最大100文字）
function preview(s: string | null, maxLength: number = 100): string {
  if (!s) return "";
  if (s.length <= maxLength) return s;
  return s.substring(0, maxLength) + "...";
}

async function main() {
  const db = initFirebaseAdmin();

  const colRef = db.collection(COLLECTION_NAME);

  let lastDoc: FirebaseFirestore.QueryDocumentSnapshot | null = null;
  
  // 再開オプション: 特定のドキュメントIDから開始
  if (START_FROM_DOC_ID) {
    try {
      const startDoc = await colRef.doc(START_FROM_DOC_ID).get();
      if (startDoc.exists) {
        lastDoc = startDoc as FirebaseFirestore.QueryDocumentSnapshot;
        console.log(`🔄 Resuming from document ID: ${START_FROM_DOC_ID}`);
      } else {
        console.warn(`⚠️  Warning: Document ID "${START_FROM_DOC_ID}" not found. Starting from beginning.`);
      }
    } catch (error) {
      console.error(`❌ Error loading start document: ${error}`);
      process.exit(1);
    }
  }

  let scanned = 0;
  let candidates: InspectionResult[] = [];

  console.log(
    `🔍 Scan start: collection="${COLLECTION_NAME}", pageSize=${PAGE_SIZE}, DRY_RUN=${DRY_RUN}`
  );

  while (true) {
    let query = colRef.orderBy(admin.firestore.FieldPath.documentId()).limit(
      PAGE_SIZE
    );
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snap = await query.get();
    if (snap.empty) {
      break;
    }

    for (const doc of snap.docs) {
      scanned += 1;

      const data = doc.data();
      const overview = norm((data as any).overview);
      const companyDescription = norm((data as any).companyDescription);

      // 両方のフィールドに値が入っている場合
      if (overview !== null && companyDescription !== null) {
        const result: InspectionResult = {
          docId: doc.id,
          overview: overview,
          companyDescription: companyDescription,
          overviewLength: overview.length,
          companyDescriptionLength: companyDescription.length,
          overviewPreview: preview(overview),
          companyDescriptionPreview: preview(companyDescription),
        };

        candidates.push(result);

        if (DRY_RUN) {
          console.log(
            `🔧 [candidate] docId=${doc.id}\n` +
            `   overview (${overview.length} chars): ${preview(overview, 80)}\n` +
            `   companyDescription (${companyDescription.length} chars): ${preview(companyDescription, 80)}`
          );
        }
      }

      if (scanned % 10000 === 0) {
        console.log(
          `📦 scanning... scanned=${scanned}, candidates=${candidates.length}`
        );
      }
    }

    lastDoc = snap.docs[snap.docs.length - 1];
  }

  // 結果をJSONファイルに保存
  const output = {
    timestamp: new Date().toISOString(),
    scanned: scanned,
    candidatesCount: candidates.length,
    candidates: candidates,
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(output, null, 2), "utf8");

  console.log("✅ Inspection finished");
  console.log(`  🔍 scanned docs : ${scanned}`);
  console.log(`  🔧 candidates   : ${candidates.length}`);
  console.log(`  📄 output file  : ${OUTPUT_FILE}`);
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});

