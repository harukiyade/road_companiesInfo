/* eslint-disable no-console */

/**
 * scripts/add_legal_form.ts
 *
 * ✅ 目的
 * - companies_newコレクション内の全ドキュメントに対して、legalFormフィールドを追加
 * - nameフィールドから法人格を抽出して設定
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 *
 * ✅ 処理内容
 * - 全ドキュメントをバッチ処理で取得（高速化：並列書き込み対応）
 * - nameフィールドから法人格を抽出
 * - legalFormフィールドを追加/更新
 *
 * ✅ 法人格判定ロジック
 * - 長い文字列から先にマッチング（例：「特定非営利活動法人」→「NPO法人」の順）
 * - 「医療法人社団」「医療法人財団」→「医療法人」に正規化
 * - 「NPO法人」→「特定非営利活動法人」に正規化
 * - 先頭・末尾・中間のいずれかに法人格が含まれていれば検出
 *
 * ✅ 対応法人格（26種類）
 * - 営利法人: 株式会社、有限会社、合同会社、合資会社、合名会社、相互会社
 * - 非営利・公益系: 特定非営利活動法人、NPO法人、一般社団法人、公益社団法人、一般財団法人、公益財団法人
 * - 専門機関・士業系: 医療法人、社会福祉法人、学校法人、宗教法人、監査法人、税理士法人、弁護士法人、行政書士法人、司法書士法人、社会保険労務士法人、弁理士法人
 * - 公的機関: 独立行政法人、国立大学法人、公立大学法人
 */

import admin from "firebase-admin";
import * as fs from "fs";

// ------------------------------
// Firebase Admin SDK 初期化
// ------------------------------
if (!admin.apps.length) {
  try {
    const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;

    if (!serviceAccountPath) {
      console.error("❌ エラー: FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません。");
      process.exit(1);
    }

    if (!fs.existsSync(serviceAccountPath)) {
      console.error(`❌ エラー: サービスアカウントキーファイルが存在しません: ${serviceAccountPath}`);
      process.exit(1);
    }

    const serviceAccount = JSON.parse(
      fs.readFileSync(serviceAccountPath, "utf8")
    );

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: "albert-ma",
    });

    console.log("[Firebase初期化] ✅ 初期化が完了しました");
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", error);
    process.exit(1);
  }
}

const db = admin.firestore();

// ------------------------------
// 法人格リスト（頻出順かつユニーク性の高い順）
// ------------------------------
const LEGAL_FORMS = [
  // 1. 営利法人（ビジネスの9割以上）
  "株式会社",
  "有限会社",
  "合同会社",
  "合資会社",
  "合名会社",
  "相互会社",
  
  // 2. 非営利・公益系法人
  "特定非営利活動法人", // 長い方を先に（「NPO法人」より優先）
  "NPO法人", // 表記揺れ対応（「特定非営利活動法人」の後に検索）
  "一般社団法人",
  "公益社団法人",
  "一般財団法人",
  "公益財団法人",
  
  // 3. 専門機関・士業系
  "医療法人社団", // 「医療法人社団〇〇会」などの表記に対応
  "医療法人財団", // 「医療法人財団〇〇」などの表記に対応
  "医療法人", // 上記以外の「医療法人」表記
  "社会福祉法人",
  "学校法人",
  "宗教法人",
  "監査法人",
  "税理士法人",
  "弁護士法人",
  "行政書士法人",
  "司法書士法人",
  "社会保険労務士法人",
  "弁理士法人", // 「特許業務法人」は現在の正式名称「弁理士法人」に統合
  
  // 4. 公的機関
  "独立行政法人",
  "国立大学法人",
  "公立大学法人", // 追加
];

// 長い法人格から順に検索するため、長さでソート（降順）
// これにより「特定非営利活動法人」が「NPO法人」より先に検索され、
// 「医療法人社団」が「医療法人」より先に検索される
const SORTED_LEGAL_FORMS = [...LEGAL_FORMS].sort((a, b) => b.length - a.length);

// 略記表記のマッピング（検出用）
const ABBREVIATED_FORMS: { [key: string]: string | null } = {
  "(株)": "株式会社",
  "㈱": "株式会社",
  "(有)": "有限会社",
  "㈲": "有限会社",
  "(合)": "合同会社",
  "㈾": "合同会社",
  "(資)": "合資会社",
  "㈽": "合資会社",
  "(名)": "合名会社",
  "㈺": "合名会社",
  "(相)": "相互会社",
  "㈿": "相互会社",
  "(医)": "医療法人",
  "㈻": "医療法人",
  "(学)": "学校法人",
  "㈶": "学校法人",
  "(社)": "一般社団法人", // 注意: 「社団法人」の略記も含む可能性
  "㈴": "一般社団法人",
  "(財)": "一般財団法人",
  "㈷": "一般財団法人",
  "(NPO)": "特定非営利活動法人",
  "(NPO法人)": "特定非営利活動法人",
  "Inc.": null, // 外国企業（未対応）
  "Ltd.": null, // 外国企業（未対応）
  "Corp.": null, // 外国企業（未対応）
  "LLC": null, // 外国企業（未対応）
};

/**
 * 検出した法人格を正規化（統一表記に変換）
 * @param detectedForm 検出した法人格
 * @returns 正規化された法人格
 */
function normalizeLegalForm(detectedForm: string): string {
  // 「医療法人社団」「医療法人財団」→「医療法人」に統一
  if (detectedForm.startsWith("医療法人")) {
    return "医療法人";
  }
  
  // 「NPO法人」→「特定非営利活動法人」に統一
  if (detectedForm === "NPO法人") {
    return "特定非営利活動法人";
  }
  
  return detectedForm;
}

/**
 * 会社名から法人格を抽出
 * @param companyName 会社名
 * @returns 法人格（見つからない場合はnull）
 */
function extractLegalForm(companyName: string): string | null {
  if (!companyName || typeof companyName !== "string") {
    return null;
  }

  const trimmedName = companyName.trim();

  // 空文字列の場合はnull
  if (trimmedName.length === 0) {
    return null;
  }

  // 1. 略記表記をチェック（先にチェックして、正式名称に変換）
  for (const [abbrev, fullForm] of Object.entries(ABBREVIATED_FORMS)) {
    if (fullForm === null) {
      // 外国企業の略記はスキップ（未対応）
      continue;
    }
    
    // 略記が含まれている場合
    if (trimmedName.includes(abbrev)) {
      // 正式名称で再度検索（正規化も含む）
      const normalized = normalizeLegalForm(fullForm);
      return normalized;
    }
  }

  // 2. 正式名称の法人格を検索
  // 長い法人格から順に検索（例：「特定非営利活動法人」を「NPO法人」より先に検索）
  // 「医療法人社団」を「医療法人」より先に検索
  for (const legalForm of SORTED_LEGAL_FORMS) {
    // 先頭に法人格がある場合
    if (trimmedName.startsWith(legalForm)) {
      return normalizeLegalForm(legalForm);
    }
    // 末尾に法人格がある場合
    if (trimmedName.endsWith(legalForm)) {
      return normalizeLegalForm(legalForm);
    }
    // 中間に法人格がある場合（例：「株式会社○○商事」のような形式は通常ないが、念のため）
    if (trimmedName.includes(legalForm)) {
      return normalizeLegalForm(legalForm);
    }
  }

  return null;
}

/**
 * バッチコミット（リトライ機能付き）
 */
async function commitBatchWithRetry(
  batch: admin.firestore.WriteBatch,
  retries: number = 3
): Promise<void> {
  for (let i = 0; i < retries; i++) {
    try {
      await batch.commit();
      return;
    } catch (error: any) {
      if (i === retries - 1) {
        throw error;
      }
      // レート制限エラーの場合は待機時間を増やす
      const waitTime = (i + 1) * 1000;
      console.warn(`  バッチコミットエラー（リトライ ${i + 1}/${retries}）: ${error.message} - ${waitTime}ms待機...`);
      await new Promise((resolve) => setTimeout(resolve, waitTime));
    }
  }
}

/**
 * メイン処理（高速化：並列書き込み対応）
 */
async function main() {
  console.log("================================================================================\n");
  console.log("legalFormフィールドの追加を開始...（高速化モード：並列書き込み）");
  console.log("================================================================================\n");

  const BATCH_SIZE = 1000; // 読み取りバッチサイズ（500→1000に増加）
  const WRITE_BATCH_SIZE = 100; // 書き込みバッチサイズ（50→100に増加、Firestoreの制限内）
  const CONCURRENT_BATCHES = 10; // 並列でコミットするバッチ数

  let totalProcessed = 0;
  let totalUpdated = 0;
  let totalSkipped = 0; // 既にlegalFormが設定されている場合
  let totalNotFound = 0; // 法人格が見つからなかった場合
  let totalErrors = 0; // エラー数
  const legalFormStats = new Map<string, number>(); // 法人格ごとの統計
  const notFoundSamples: Array<{ id: string; name: string; reason: string }> = []; // 未検出サンプル（最大100件）
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;

  console.log("Firestoreから企業データを取得中...\n");

  // 更新キュー（並列処理用）
  type UpdateItem = {
    ref: admin.firestore.DocumentReference;
    legalForm: string;
  };
  const updateQueue: UpdateItem[] = [];

  try {
    // フェーズ1: 全ドキュメントを読み取って更新キューに追加
    console.log("フェーズ1: ドキュメントの読み取りと法人格の抽出...\n");
    
    while (true) {
      let query: admin.firestore.Query = db
        .collection("companies_new")
        .orderBy(admin.firestore.FieldPath.documentId())
        .limit(BATCH_SIZE);

      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();

      if (snapshot.empty) {
        break;
      }

      for (const doc of snapshot.docs) {
        totalProcessed++;
        const data = doc.data();
        const companyId = doc.id;
        const companyName = data.name || "";

        // 既にlegalFormが設定されている場合はスキップ（更新しない）
        if (data.legalForm) {
          totalSkipped++;
          continue;
        }

        // 法人格を抽出
        const legalForm = extractLegalForm(companyName);

        if (legalForm) {
          // 更新キューに追加
          updateQueue.push({
            ref: doc.ref,
            legalForm,
          });
          totalUpdated++;

          // 統計を更新
          legalFormStats.set(legalForm, (legalFormStats.get(legalForm) || 0) + 1);
        } else {
          totalNotFound++;
          
          // 未検出の理由を分析
          let reason = "法人格が含まれていない";
          if (!companyName || companyName.trim().length === 0) {
            reason = "nameフィールドが空";
          } else if (/Inc\.|Ltd\.|Corp\.|LLC/i.test(companyName)) {
            reason = "外国企業（未対応）";
          } else if (/個人|事業主|屋号/i.test(companyName)) {
            reason = "個人事業主";
          } else if (/^(地方公共団体|都道府県|市|区|町|村)/.test(companyName)) {
            reason = "地方公共団体（未対応）";
          }
          
          // 未検出サンプルを記録（最大100件）
          if (notFoundSamples.length < 100) {
            notFoundSamples.push({ id: companyId, name: companyName, reason });
          }
          
          // 最初の10件のみログに出力
          if (totalNotFound <= 10) {
            console.log(`  [未検出] ${companyId}: "${companyName}" (理由: ${reason})`);
          }
        }

        // 進捗表示（1000件ごと）
        if (totalProcessed % 1000 === 0) {
          console.log(`  読み取り中: ${totalProcessed.toLocaleString()}件... (更新予定: ${totalUpdated.toLocaleString()}件, スキップ: ${totalSkipped.toLocaleString()}件, 未検出: ${totalNotFound.toLocaleString()}件)`);
        }
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];

      if (snapshot.docs.length < BATCH_SIZE) {
        break;
      }
    }

    console.log(`\n✅ 読み取り完了: ${totalProcessed.toLocaleString()}件`);
    console.log(`   更新予定: ${totalUpdated.toLocaleString()}件`);
    console.log(`   スキップ: ${totalSkipped.toLocaleString()}件`);
    console.log(`   未検出: ${totalNotFound.toLocaleString()}件\n`);

    // フェーズ2: 更新キューを並列で書き込み
    console.log("フェーズ2: Firestoreへの並列書き込み...\n");

    const updateChunks: UpdateItem[][] = [];
    for (let i = 0; i < updateQueue.length; i += WRITE_BATCH_SIZE) {
      updateChunks.push(updateQueue.slice(i, i + WRITE_BATCH_SIZE));
    }

    let committedCount = 0;
    const totalChunks = updateChunks.length;
    let processedChunks = 0;

    // 並列でバッチをコミット
    for (let i = 0; i < updateChunks.length; i += CONCURRENT_BATCHES) {
      const concurrentChunks = updateChunks.slice(i, i + CONCURRENT_BATCHES);
      
      const batchPromises = concurrentChunks.map(async (chunk, chunkIndex) => {
        const batch = db.batch();
        
        for (const item of chunk) {
          batch.update(item.ref, { legalForm: item.legalForm });
        }

        try {
          await commitBatchWithRetry(batch);
          const successCount = chunk.length;
          
          return { success: true, count: successCount };
        } catch (error: any) {
          const errorCount = chunk.length;
          console.error(`  ❌ バッチコミットエラー: ${error.message} (${errorCount}件)`);
          // エラーが発生したバッチの詳細をログに記録（最初の5件のみ）
          const logLimit = Math.min(5, chunk.length);
          for (let j = 0; j < logLimit; j++) {
            console.error(`    失敗: ${chunk[j].ref.id} - ${chunk[j].legalForm}`);
          }
          if (chunk.length > logLimit) {
            console.error(`    ... 他 ${chunk.length - logLimit}件`);
          }
          
          return { success: false, count: errorCount };
        }
      });

      const results = await Promise.allSettled(batchPromises);
      
      // 成功/失敗を集計
      for (const result of results) {
        if (result.status === "fulfilled") {
          if (result.value.success) {
            committedCount += result.value.count;
          } else {
            totalErrors += result.value.count;
          }
          processedChunks++;
        } else {
          // Promise.allSettledなので通常はここには来ないが、念のため
          totalErrors += concurrentChunks[0]?.length || 0;
          processedChunks++;
        }
      }
      
      // 進捗表示
      if (processedChunks % 10 === 0 || processedChunks === totalChunks) {
        console.log(`  書き込み進捗: ${committedCount.toLocaleString()}/${totalUpdated.toLocaleString()}件 (${Math.round((committedCount / totalUpdated) * 100)}%)`);
      }
      
      // レート制限対策（並列処理の間隔を空ける）
      if (i + CONCURRENT_BATCHES < updateChunks.length) {
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
    }

    console.log("\n================================================================================\n");
    console.log("✅ 処理完了");
    console.log(`   総処理数: ${totalProcessed.toLocaleString()}件`);
    console.log(`   更新成功: ${committedCount.toLocaleString()}件`);
    console.log(`   更新失敗: ${totalErrors.toLocaleString()}件`);
    console.log(`   スキップ数（既に設定済み）: ${totalSkipped.toLocaleString()}件`);
    console.log(`   未検出数（法人格が見つからなかった）: ${totalNotFound.toLocaleString()}件`);

    // 法人格ごとの統計を表示
    console.log("\n   法人格ごとの設定数:");
    const sortedLegalForms = Array.from(legalFormStats.entries())
      .sort((a, b) => b[1] - a[1]);

    for (const [legalForm, count] of sortedLegalForms) {
      console.log(`     - ${legalForm}: ${count.toLocaleString()}件`);
    }

    if (totalErrors > 0) {
      console.log(`\n   ⚠️  注意: ${totalErrors.toLocaleString()}件の更新に失敗しました。エラーログを確認してください。`);
    }

    // 未検出の詳細分析
    if (totalNotFound > 0) {
      console.log(`\n   未検出の詳細分析 (${totalNotFound.toLocaleString()}件):`);
      
      // 理由ごとの集計
      const reasonStats = new Map<string, number>();
      for (const sample of notFoundSamples) {
        reasonStats.set(sample.reason, (reasonStats.get(sample.reason) || 0) + 1);
      }
      
      console.log(`   理由ごとの内訳（サンプル${notFoundSamples.length}件）:`);
      for (const [reason, count] of Array.from(reasonStats.entries()).sort((a, b) => b[1] - a[1])) {
        const percentage = ((count / notFoundSamples.length) * 100).toFixed(1);
        console.log(`     - ${reason}: ${count}件 (${percentage}%)`);
      }
      
      // 未検出サンプルを表示（最初の20件）
      console.log(`\n   未検出サンプル（最初の20件）:`);
      for (let i = 0; i < Math.min(20, notFoundSamples.length); i++) {
        const sample = notFoundSamples[i];
        console.log(`     ${i + 1}. [${sample.id}] "${sample.name}" (${sample.reason})`);
      }
      
      if (notFoundSamples.length > 20) {
        console.log(`     ... 他 ${notFoundSamples.length - 20}件`);
      }
      
      console.log(`\n   💡 未検出の主な理由:`);
      console.log(`      - nameフィールドが空またはnull`);
      console.log(`      - 法人格が含まれていない（個人事業主、外国企業など）`);
      console.log(`      - 法人格リストに未登録の法人格`);
      console.log(`      - 特殊な表記（略記は対応済み: (株)、㈱など）`);
    }

    console.log("\n================================================================================\n");

  } catch (error) {
    console.error("❌ エラー:", error);
    process.exit(1);
  }
}

// 実行
main()
  .then(() => {
    console.log("処理が正常に完了しました");
    process.exit(0);
  })
  .catch((error) => {
    console.error("❌ エラー:", error);
    process.exit(1);
  });
