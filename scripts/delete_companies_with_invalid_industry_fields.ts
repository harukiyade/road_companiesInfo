// scripts/delete_companies_with_invalid_industry_fields.ts
//
// companies_new コレクションのうち、
// industryLarge, industryMiddle, industrySmall, industryDetail のいずれかに
// 業種ではなく文章が入ってしまっているドキュメントを検出し、削除するスクリプトです。
//
// 実行例:
//   DRY_RUN=1 npx tsx scripts/delete_companies_with_invalid_industry_fields.ts   // 削除せず候補だけログ
//   npx tsx scripts/delete_companies_with_invalid_industry_fields.ts             // 実際に削除
//
// Firestore 認証:
//   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/serviceAccountKey.json"

import "dotenv/config";
import admin from "firebase-admin";
import * as fs from "fs";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// 1 回のクエリで読む件数
const PAGE_SIZE = 1000;
// 1 バッチで削除する件数（Firestore の上限 500 未満にする）
const BATCH_DELETE_SIZE = 400;

// DRY_RUN=1 のときは削除せずログだけ出す
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";

// チェックポイントファイル名（再開用）
const CHECKPOINT_FILE = "delete_invalid_industry_checkpoint.json";

function initFirebaseAdmin() {
  if (admin.apps.length) {
    return admin.firestore();
  }

  try {
    // 環境変数が設定されている場合はそれを使用、なければapplicationDefault()を使用
    const serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

    if (serviceAccountPath && fs.existsSync(serviceAccountPath)) {
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
    } else {
      // applicationDefault()を使用（環境変数が設定されていない場合）
      // Project IDを環境変数から取得
      const projectId =
        process.env.GCLOUD_PROJECT ||
        process.env.GCP_PROJECT ||
        PROJECT_ID;

      admin.initializeApp({
        credential: admin.credential.applicationDefault(),
        projectId,
      });
      console.log(`✅ Firebase Admin initialized (using applicationDefault, Project ID: ${projectId})`);
    }

    return admin.firestore();
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", (error as Error).message);
    throw error;
  }
}

function normalizeString(v: unknown): string {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

// 業種フィールドに文章が入っているかどうかを判定
function isInvalidIndustryField(value: string | null | undefined): boolean {
  const normalized = normalizeString(value);
  if (!normalized) return false; // 空の場合は問題なし

  // 1. 文字数が長すぎる（50文字以上）
  if (normalized.length >= 50) {
    return true;
  }

  // 2. 句読点が含まれている
  if (normalized.includes("。") || normalized.includes("、") || normalized.includes("，")) {
    return true;
  }

  // 3. 改行が含まれている
  if (normalized.includes("\n") || normalized.includes("\r")) {
    return true;
  }

  // 4. 文章表現が含まれている
  const sentencePatterns = [
    /です/g,
    /ます/g,
    /である/g,
    /でした/g,
    /ました/g,
    /でした/g,
    /です。/g,
    /ます。/g,
    /である。/g,
    /。$/g, // 文末の句点
    /^。/g, // 文頭の句点
  ];

  for (const pattern of sentencePatterns) {
    if (pattern.test(normalized)) {
      return true;
    }
  }

  // 5. 複数の文が含まれている可能性（句点が2つ以上）
  const periodCount = (normalized.match(/。/g) || []).length;
  if (periodCount >= 2) {
    return true;
  }

  return false;
}

async function main() {
  const db = initFirebaseAdmin();

  const colRef = db.collection(COLLECTION_NAME);

  // チェックポイントから再開
  let lastDoc: FirebaseFirestore.QueryDocumentSnapshot | null = null;
  let scanned = 0;
  let candidates = 0;
  let deleted = 0;

  if (fs.existsSync(CHECKPOINT_FILE)) {
    try {
      const checkpoint = JSON.parse(fs.readFileSync(CHECKPOINT_FILE, "utf8"));
      if (checkpoint.lastDocId) {
        const lastDocRef = db.collection(COLLECTION_NAME).doc(checkpoint.lastDocId);
        const lastDocSnap = await lastDocRef.get();
        if (lastDocSnap.exists) {
          lastDoc = lastDocSnap as FirebaseFirestore.QueryDocumentSnapshot;
          scanned = checkpoint.scanned || 0;
          candidates = checkpoint.candidates || 0;
          deleted = checkpoint.deleted || 0;
          console.log(`🔄 チェックポイントから再開: lastDocId=${checkpoint.lastDocId}, scanned=${scanned}, deleted=${deleted}`);
        } else {
          console.log(`⚠️  チェックポイントのドキュメントが見つかりません。最初から開始します。`);
        }
      }
    } catch (error) {
      console.warn(`⚠️  チェックポイントファイルの読み込みに失敗しました: ${(error as Error).message}`);
    }
  }

  let batch = db.batch();
  let batchCount = 0;

  const invalidDocs: Array<{
    docId: string;
    name: string;
    invalidFields: string[];
  }> = [];

  console.log(
    `🔍 Scan start: collection="${COLLECTION_NAME}", pageSize=${PAGE_SIZE}, batchDeleteSize=${BATCH_DELETE_SIZE}, DRY_RUN=${DRY_RUN}`,
  );

  while (true) {
    let query = colRef.orderBy(admin.firestore.FieldPath.documentId()).limit(
      PAGE_SIZE,
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
      const industryLarge = normalizeString((data as any).industryLarge);
      const industryMiddle = normalizeString((data as any).industryMiddle);
      const industrySmall = normalizeString((data as any).industrySmall);
      const industryDetail = normalizeString((data as any).industryDetail);

      const invalidFields: string[] = [];

      if (isInvalidIndustryField(industryLarge)) {
        invalidFields.push(`industryLarge: "${industryLarge.substring(0, 100)}${industryLarge.length > 100 ? "..." : ""}"`);
      }
      if (isInvalidIndustryField(industryMiddle)) {
        invalidFields.push(`industryMiddle: "${industryMiddle.substring(0, 100)}${industryMiddle.length > 100 ? "..." : ""}"`);
      }
      if (isInvalidIndustryField(industrySmall)) {
        invalidFields.push(`industrySmall: "${industrySmall.substring(0, 100)}${industrySmall.length > 100 ? "..." : ""}"`);
      }
      if (isInvalidIndustryField(industryDetail)) {
        invalidFields.push(`industryDetail: "${industryDetail.substring(0, 100)}${industryDetail.length > 100 ? "..." : ""}"`);
      }

      // いずれかの業種フィールドに文章が入っている場合
      if (invalidFields.length > 0) {
        candidates += 1;
        const name = normalizeString((data as any).name) || "(名前なし)";

        invalidDocs.push({
          docId: doc.id,
          name,
          invalidFields,
        });

        if (DRY_RUN) {
          console.log(
            `🗑️ [candidate] docId=${doc.id}, name="${name}"`,
          );
          invalidFields.forEach(field => {
            console.log(`    - ${field}`);
          });
        } else {
          batch.delete(doc.ref);
          batchCount += 1;

          if (batchCount >= BATCH_DELETE_SIZE) {
            await batch.commit();
            deleted += batchCount;
            console.log(
              `💾 Committed delete batch: ${batchCount} docs (total deleted: ${deleted}, scanned: ${scanned})`,
            );
            batch = db.batch();
            batchCount = 0;
          }
        }
      }

      if (scanned % 10000 === 0) {
        console.log(
          `📦 scanning... scanned=${scanned}, candidates=${candidates}, deleted=${deleted}`,
        );
        // チェックポイントを保存
        if (!DRY_RUN && snap.docs.length > 0) {
          const checkpoint = {
            lastDocId: snap.docs[snap.docs.length - 1].id,
            scanned,
            candidates,
            deleted,
          };
          fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2), "utf8");
        }
      }
    }

    lastDoc = snap.docs[snap.docs.length - 1];
    
    // 各ページ処理後にチェックポイントを保存
    if (!DRY_RUN && snap.docs.length > 0) {
      const checkpoint = {
        lastDocId: snap.docs[snap.docs.length - 1].id,
        scanned,
        candidates,
        deleted,
      };
      fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2), "utf8");
    }
  }

  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    deleted += batchCount;
    console.log(
      `💾 Committed final delete batch: ${batchCount} docs (total deleted: ${deleted})`,
    );
  }

  // 結果をファイルに出力
  if (invalidDocs.length > 0) {
    const outputFile = `invalid_industry_fields_${Date.now()}.json`;
    fs.writeFileSync(
      outputFile,
      JSON.stringify(invalidDocs, null, 2),
      "utf8"
    );
    console.log(`\n📝 不正な業種フィールドを持つドキュメントの詳細: ${outputFile}`);
  }

  // チェックポイントファイルを削除（処理完了時）
  if (fs.existsSync(CHECKPOINT_FILE)) {
    fs.unlinkSync(CHECKPOINT_FILE);
    console.log(`🗑️  チェックポイントファイルを削除しました`);
  }

  console.log("\n✅ Cleanup finished");
  console.log(`  🔍 scanned docs : ${scanned}`);
  console.log(`  🗑️ candidates   : ${candidates}`);
  console.log(`  ❌ deleted      : ${deleted} (DRY_RUN=${DRY_RUN})`);
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});
