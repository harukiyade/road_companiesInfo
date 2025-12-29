// scripts/fix_overview_companydescription.ts
//
// companies_new コレクション上で、
// overview と companyDescription の両方に値が入っているドキュメントに対して、
// 内容を分析して適切に修正するスクリプトです。
//
// 判定ロジック:
// - 「〇〇な会社」「〇〇する会社」などのパターンが含まれている → companyDescription
// - 端的な説明（短く簡潔） → overview
//
// 実行例:
//   DRY_RUN=1 npx ts-node scripts/fix_overview_companydescription.ts   // 更新せず候補だけログ
//   npx ts-node scripts/fix_overview_companydescription.ts             // 実際に更新
//
// 再開オプション:
//   START_FROM_DOC_ID="docId123" npx ts-node scripts/fix_overview_companydescription.ts
//   SKIP_SCANNED=2110000 npx ts-node scripts/fix_overview_companydescription.ts
//
// Firestore 認証:
//   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/serviceAccountKey.json"

import admin from "firebase-admin";
import * as fs from "fs";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// 1 回のクエリで読む件数
const PAGE_SIZE = 1000;
// 1 バッチで更新する件数（Firestore の上限 500 未満にする）
const BATCH_UPDATE_SIZE = 400;
// 並列で読み取るページ数
const PARALLEL_PAGES = 5;

// DRY_RUN=1 のときは更新せずログだけ出す
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";

// 再開オプション
const START_FROM_DOC_ID = process.env.START_FROM_DOC_ID;
const SKIP_SCANNED = process.env.SKIP_SCANNED ? parseInt(process.env.SKIP_SCANNED, 10) : 0;

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
// 短い文章、箇条書き的な内容、具体的な数値や事実が含まれる
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

// 判定結果
interface FixDecision {
  docId: string;
  action: "swap" | "keep_overview" | "keep_companyDescription" | "keep_both" | "uncertain";
  reason: string;
  currentOverview: string;
  currentCompanyDescription: string;
  newOverview: string | null;
  newCompanyDescription: string | null;
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

// プレビュー文字列生成（最大80文字）
function preview(s: string | null, maxLength: number = 80): string {
  if (!s) return "";
  if (s.length <= maxLength) return s;
  return s.substring(0, maxLength) + "...";
}

// 修正判定ロジック
function decideFix(
  docId: string,
  overview: string,
  companyDescription: string
): FixDecision {
  const overviewIsDescription = isLikelyCompanyDescription(overview);
  const companyDescriptionIsDescription = isLikelyCompanyDescription(companyDescription);
  const overviewIsOverview = isLikelyOverview(overview);
  const companyDescriptionIsOverview = isLikelyOverview(companyDescription);

  // ケース1: overviewが企業説明パターン、companyDescriptionが概要パターン → 入れ替え
  if (overviewIsDescription && companyDescriptionIsOverview) {
    return {
      docId,
      action: "swap",
      reason: "overviewが企業説明パターン、companyDescriptionが概要パターンのため入れ替え",
      currentOverview: overview,
      currentCompanyDescription: companyDescription,
      newOverview: companyDescription,
      newCompanyDescription: overview,
    };
  }

  // ケース2: overviewが企業説明パターン、companyDescriptionが不明 → overviewをcompanyDescriptionに移動
  if (overviewIsDescription && !companyDescriptionIsOverview) {
    return {
      docId,
      action: "keep_companyDescription",
      reason: "overviewが企業説明パターンのため、companyDescriptionに移動",
      currentOverview: overview,
      currentCompanyDescription: companyDescription,
      newOverview: null, // overviewをクリア
      newCompanyDescription: companyDescription || overview, // 既存があれば保持、なければ移動
    };
  }

  // ケース3: companyDescriptionが概要パターン、overviewが不明 → companyDescriptionをoverviewに移動
  if (companyDescriptionIsOverview && !overviewIsOverview) {
    return {
      docId,
      action: "keep_overview",
      reason: "companyDescriptionが概要パターンのため、overviewに移動",
      currentOverview: overview,
      currentCompanyDescription: companyDescription,
      newOverview: overview || companyDescription, // 既存があれば保持、なければ移動
      newCompanyDescription: null, // companyDescriptionをクリア
    };
  }

  // ケース4: 両方とも企業説明パターン → companyDescriptionに統合、overviewをクリア
  if (overviewIsDescription && companyDescriptionIsDescription) {
    return {
      docId,
      action: "keep_companyDescription",
      reason: "両方とも企業説明パターンのため、companyDescriptionに統合",
      currentOverview: overview,
      currentCompanyDescription: companyDescription,
      newOverview: null,
      newCompanyDescription: companyDescription, // 既存を優先
    };
  }

  // ケース5: 両方とも概要パターン → overviewに統合、companyDescriptionをクリア
  if (overviewIsOverview && companyDescriptionIsOverview) {
    return {
      docId,
      action: "keep_overview",
      reason: "両方とも概要パターンのため、overviewに統合",
      currentOverview: overview,
      currentCompanyDescription: companyDescription,
      newOverview: overview, // 既存を優先
      newCompanyDescription: null,
    };
  }

  // ケース6: 既に正しい配置（overviewが概要パターン、companyDescriptionが企業説明パターン）→ そのまま保持
  if (overviewIsOverview && companyDescriptionIsDescription && !overviewIsDescription && !companyDescriptionIsOverview) {
    return {
      docId,
      action: "keep_both",
      reason: "既に正しい配置（overviewは概要パターン、companyDescriptionは企業説明パターン）のため、そのまま保持",
      currentOverview: overview,
      currentCompanyDescription: companyDescription,
      newOverview: overview, // 変更なし
      newCompanyDescription: companyDescription, // 変更なし
    };
  }

  // ケース7: 判定が難しい場合
  return {
    docId,
    action: "uncertain",
    reason: "自動判定が困難なため要確認",
    currentOverview: overview,
    currentCompanyDescription: companyDescription,
    newOverview: null,
    newCompanyDescription: null,
  };
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
  let candidates: FixDecision[] = [];
  let updated = 0;
  let uncertain: FixDecision[] = [];

  // 並列処理用の更新キュー
  const updateQueue: Array<{ docRef: FirebaseFirestore.DocumentReference; updateData: any }> = [];
  
  // チェックポイントファイル（再開用）
  const checkpointFile = "fix_overview_companydescription_checkpoint.txt";

  console.log(
    `🔍 Scan start: collection="${COLLECTION_NAME}", pageSize=${PAGE_SIZE}, batchUpdateSize=${BATCH_UPDATE_SIZE}, parallelPages=${PARALLEL_PAGES}, DRY_RUN=${DRY_RUN}`
  );
  if (SKIP_SCANNED > 0) {
    console.log(`⏭️  Will skip first ${SKIP_SCANNED} scanned documents`);
  }
  
  // チェックポイントから再開（START_FROM_DOC_IDが指定されていない場合）
  if (!START_FROM_DOC_ID && fs.existsSync(checkpointFile)) {
    try {
      const checkpointData = fs.readFileSync(checkpointFile, "utf8").trim();
      const checkpointDocId = checkpointData.split("\n")[0];
      if (checkpointDocId) {
        const checkpointDoc = await colRef.doc(checkpointDocId).get();
        if (checkpointDoc.exists) {
          lastDoc = checkpointDoc as FirebaseFirestore.QueryDocumentSnapshot;
          const checkpointScanned = checkpointData.split("\n")[1] ? parseInt(checkpointData.split("\n")[1], 10) : 0;
          scanned = checkpointScanned;
          console.log(`🔄 Resuming from checkpoint: docId=${checkpointDocId}, scanned=${scanned}`);
        }
      }
    } catch (error) {
      console.warn(`⚠️  Warning: Could not load checkpoint: ${error}`);
    }
  }

  // バッチ更新を並列で実行する関数（複数のバッチを同時にコミット）
  const batchPromises: Promise<void>[] = [];
  let batchCount = 0;
  let currentBatch = db.batch();

  async function commitBatch(batch: FirebaseFirestore.WriteBatch, count: number) {
    try {
      await batch.commit();
      updated += count;
      console.log(
        `💾 Committed update batch: ${count} docs (total updated: ${updated}, active batches: ${batchPromises.length})`
      );
    } catch (error) {
      console.error(`❌ Batch commit error: ${error}`);
    }
  }

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

    // ページ内のドキュメントを並列で処理
    const docPromises = snap.docs.map(async (doc) => {
      scanned += 1;
      
      // スキップオプション: 指定された件数までスキップ
      if (SKIP_SCANNED > 0 && scanned <= SKIP_SCANNED) {
        if (scanned % 10000 === 0) {
          console.log(`⏭️  Skipping... scanned=${scanned}/${SKIP_SCANNED}`);
        }
        lastDoc = doc as FirebaseFirestore.QueryDocumentSnapshot;
        return;
      }

      const data = doc.data();
      const overview = norm((data as any).overview);
      const companyDescription = norm((data as any).companyDescription);

      // 両方のフィールドに値が入っている場合
      if (overview !== null && companyDescription !== null) {
        const decision = decideFix(doc.id, overview, companyDescription);
        candidates.push(decision);

        if (decision.action === "uncertain") {
          uncertain.push(decision);
        }

        if (DRY_RUN) {
          console.log(
            `🔧 [${decision.action}] docId=${doc.id}\n` +
            `   理由: ${decision.reason}\n` +
            `   現在のoverview: ${preview(overview)}\n` +
            `   現在のcompanyDescription: ${preview(companyDescription)}\n` +
            `   新しいoverview: ${decision.newOverview ? preview(decision.newOverview) : "(null)"}\n` +
            `   新しいcompanyDescription: ${decision.newCompanyDescription ? preview(decision.newCompanyDescription) : "(null)"}`
          );
        } else {
          // 実際に更新（keep_bothとuncertainはスキップ）
          if (decision.action !== "uncertain" && decision.action !== "keep_both") {
            const updateData: any = {};
            if (decision.newOverview !== null) {
              updateData.overview = decision.newOverview;
            } else {
              updateData.overview = admin.firestore.FieldValue.delete();
            }
            if (decision.newCompanyDescription !== null) {
              updateData.companyDescription = decision.newCompanyDescription;
            } else {
              updateData.companyDescription = admin.firestore.FieldValue.delete();
            }

            currentBatch.update(doc.ref, updateData);
            batchCount += 1;

            // バッチが満杯になったら並列でコミット
            if (batchCount >= BATCH_UPDATE_SIZE) {
              const batchToCommit = currentBatch;
              const countToCommit = batchCount;
              currentBatch = db.batch();
              batchCount = 0;
              
              // 並列でコミット（最大5つのバッチを同時に実行）
              const commitPromise = commitBatch(batchToCommit, countToCommit).then(() => {
                const index = batchPromises.indexOf(commitPromise);
                if (index > -1) {
                  batchPromises.splice(index, 1);
                }
              });
              batchPromises.push(commitPromise);
              
              // バッチが多すぎる場合は待機
              while (batchPromises.length >= PARALLEL_PAGES) {
                await Promise.race(batchPromises);
              }
            }
          }
        }
      }
      
      lastDoc = doc as FirebaseFirestore.QueryDocumentSnapshot;
    });

    await Promise.all(docPromises);

    if (scanned % 10000 === 0) {
      console.log(
        `📦 scanning... scanned=${scanned}, candidates=${candidates.length}, updated=${updated}, uncertain=${uncertain.length}, active batches=${batchPromises.length}`
      );
      // チェックポイントを保存（10000件ごと）
      if (!DRY_RUN && lastDoc) {
        try {
          fs.writeFileSync(
            checkpointFile,
            `${lastDoc.id}\n${scanned}`,
            "utf8"
          );
        } catch (error) {
          // チェックポイント保存エラーは無視
        }
      }
    }
  }

  // 残りのバッチをコミット
  if (!DRY_RUN && batchCount > 0) {
    await commitBatch(currentBatch, batchCount);
  }

  // 全てのバッチが完了するまで待機
  await Promise.all(batchPromises);

  // 結果をJSONファイルに保存
  const outputFile = `fix_overview_companydescription_result_${Date.now()}.json`;
  const output = {
    timestamp: new Date().toISOString(),
    scanned: scanned,
    candidatesCount: candidates.length,
    updated: updated,
    uncertainCount: uncertain.length,
    candidates: candidates,
    uncertain: uncertain,
  };

  fs.writeFileSync(outputFile, JSON.stringify(output, null, 2), "utf8");

  // チェックポイントファイルを削除（処理完了時）
  if (fs.existsSync(checkpointFile)) {
    try {
      fs.unlinkSync(checkpointFile);
      console.log(`🗑️  Checkpoint file removed`);
    } catch (error) {
      // チェックポイント削除エラーは無視
    }
  }

  console.log("✅ Update finished");
  console.log(`  🔍 scanned docs : ${scanned}`);
  console.log(`  🔧 candidates   : ${candidates.length}`);
  console.log(`  ✅ updated      : ${updated} (DRY_RUN=${DRY_RUN})`);
  console.log(`  ⚠️  uncertain   : ${uncertain.length}`);
  console.log(`  📄 output file  : ${outputFile}`);
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});

