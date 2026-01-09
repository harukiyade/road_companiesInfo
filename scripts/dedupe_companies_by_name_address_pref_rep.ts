/* 
  companies_newコレクションの重複企業情報を削除するスクリプト
  
  特定方法: 企業名＋住所＋都道府県＋代表者名が一致しているもの
  処理内容:
    - 複数ある場合は1件に絞る
    - フィールドがより多く埋まっている方を選択
    - 埋まっていない項目があれば、2つを1つにマージしてから削除
  
  使い方:
    # DRY RUN (削除せず候補だけログ)
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/dedupe_companies_by_name_address_pref_rep.ts --dry-run
    
    # 実際に削除実行
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/dedupe_companies_by_name_address_pref_rep.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentReference, DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// 1回のクエリで読む件数
const PAGE_SIZE = 1000;
// 1バッチで削除する件数（Firestoreの上限500未満にする）
const BATCH_DELETE_SIZE = 400;

// Firebase初期化
function initFirebaseAdmin(): Firestore {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(
      projectRoot,
      "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"
    );
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
      console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${defaultPath}`);
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error(
      "❌ エラー: 環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されていないか、ファイルが見つかりません"
    );
    process.exit(1);
  }

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
  return admin.firestore();
}

// 文字列正規化（比較用）
function normalizeString(v: string | null | undefined): string {
  if (!v) return "";
  return String(v)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/株式会社|有限会社|合同会社|合名会社|合資会社/g, "");
}

// 住所正規化（比較用）
function normalizeAddress(v: string | null | undefined): string {
  if (!v) return "";
  return String(v)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[都道府県市区町村]/g, "");
}

// 代表者名正規化（比較用）
function normalizeRepresentativeName(v: string | null | undefined): string {
  if (!v) return "";
  return String(v)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/代表取締役|代表取締役社長|代表取締役会長|代表取締役専務|代表取締役常務|代表取締役副社長|取締役社長|取締役会長|社長|会長|専務|常務|副社長|代表|代表者|CEO|ceo/g, "")
    .replace(/[（(].*?[）)]/g, ""); // カッコ内を除去
}

// 都道府県正規化
function normalizePrefecture(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().replace(/[都道府県]/g, "");
}

// 重複キーを生成（企業名＋住所＋都道府県＋代表者名）
function generateDuplicateKey(data: DocumentData): string {
  const name = normalizeString(data.name);
  const address = normalizeAddress(data.address);
  const prefecture = normalizePrefecture(data.prefecture);
  const representativeName = normalizeRepresentativeName(data.representativeName);

  return `${name}|${address}|${prefecture}|${representativeName}`;
}

// フィールドの埋まり具合を計算（null/undefined/空文字/空配列でないフィールド数）
function countFilledFields(data: DocumentData): number {
  let count = 0;
  for (const [key, value] of Object.entries(data)) {
    // システムフィールドは除外
    if (key === "createdAt" || key === "updatedAt") continue;

    if (value !== null && value !== undefined && value !== "") {
      if (Array.isArray(value)) {
        if (value.length > 0) count++;
      } else {
        count++;
      }
    }
  }
  return count;
}

// 2つのドキュメントをマージ（マスターに不足しているフィールドを補完）
function mergeDocuments(
  master: DocumentData,
  source: DocumentData
): DocumentData {
  const merged = { ...master };

  for (const [key, value] of Object.entries(source)) {
    // システムフィールドは除外
    if (key === "createdAt" || key === "updatedAt") continue;

    const masterValue = merged[key];

    // マスターに値がなく、ソースに値がある場合のみマージ
    if (
      (masterValue === null ||
        masterValue === undefined ||
        masterValue === "" ||
        (Array.isArray(masterValue) && masterValue.length === 0)) &&
      value !== null &&
      value !== undefined &&
      value !== "" &&
      !(Array.isArray(value) && value.length === 0)
    ) {
      merged[key] = value;
    }
  }

  return merged;
}

// 軽量なドキュメント情報（メモリ効率化のため）
interface LightweightDocInfo {
  id: string;
  key: string;
  filledFieldsCount: number;
}

// 完全なドキュメント情報（処理時にのみ使用）
interface CompanyDoc {
  id: string;
  ref: DocumentReference;
  data: DocumentData;
  key: string;
  filledFieldsCount: number;
}

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);

  console.log(
    `\n🔍 重複企業検出開始: collection="${COLLECTION_NAME}", DRY_RUN=${DRY_RUN}\n`
  );

  // 第1パス: キーとドキュメントIDのみを収集（メモリ効率化）
  console.log("📦 第1パス: ドキュメントをスキャン中（キーとIDのみ収集）...");

  const duplicateGroups = new Map<string, LightweightDocInfo[]>();
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
  let scanned = 0;

  while (true) {
    let query = colRef
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(PAGE_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snap = await query.get();
    if (snap.empty) {
      break;
    }

    for (const doc of snap.docs) {
      scanned++;
      const data = doc.data();

      // 必須フィールドチェック（企業名は必須）
      if (!data.name) {
        continue;
      }

      const key = generateDuplicateKey(data);
      const filledFieldsCount = countFilledFields(data);

      const docInfo: LightweightDocInfo = {
        id: doc.id,
        key,
        filledFieldsCount,
      };

      if (!duplicateGroups.has(key)) {
        duplicateGroups.set(key, []);
      }
      duplicateGroups.get(key)!.push(docInfo);
    }

    lastDoc = snap.docs[snap.docs.length - 1];

    if (scanned % 10000 === 0) {
      console.log(`  進行中... ${scanned}件スキャン完了`);
    }
  }

  console.log(`✅ 第1パス完了: ${scanned}件スキャン完了\n`);

  // 2件以上のグループのみを抽出（重複）
  const duplicateKeys = Array.from(duplicateGroups.entries())
    .filter(([_, docs]) => docs.length > 1)
    .sort((a, b) => b[1].length - a[1].length); // 重複数が多い順

  console.log(`🔍 重複検出結果:`);
  console.log(`  - 重複グループ数: ${duplicateKeys.length}`);
  console.log(
    `  - 重複ドキュメント総数: ${duplicateKeys.reduce(
      (sum, [_, docs]) => sum + docs.length,
      0
    )}`
  );

  if (duplicateKeys.length === 0) {
    console.log("\n✅ 重複はありません！\n");
    return;
  }

  // 第2パス: 重複グループごとにドキュメントを取得して処理
  console.log(`\n📝 第2パス: 統合処理を開始します...\n`);

  let mergedCount = 0;
  let deletedCount = 0;
  let batch = db.batch();
  let batchCount = 0;

  for (const [key, docInfos] of duplicateKeys) {
    // 最もフィールドが埋まっているドキュメントをマスターとして選択
    const sortedInfos = [...docInfos].sort(
      (a, b) => b.filledFieldsCount - a.filledFieldsCount
    );

    // 必要なドキュメントのみを取得（db.getAllは最大10件までなので分割取得）
    const docs: CompanyDoc[] = [];
    const GET_ALL_LIMIT = 10;
    
    for (let i = 0; i < sortedInfos.length; i += GET_ALL_LIMIT) {
      const batch = sortedInfos.slice(i, i + GET_ALL_LIMIT);
      const docRefs = batch.map((info) => colRef.doc(info.id));
      
      try {
        const docSnaps = await db.getAll(...docRefs);
        
        for (const snap of docSnaps) {
          const data = snap.data();
          if (!data) {
            console.warn(`  ⚠️  ドキュメント ${snap.id} のデータが取得できませんでした（スキップ）`);
            continue;
          }
          docs.push({
            id: snap.id,
            ref: snap.ref,
            data,
            key,
            filledFieldsCount: countFilledFields(data),
          });
        }
      } catch (error) {
        // 一部のドキュメントが存在しない場合、個別に取得を試みる
        console.warn(`  ⚠️  バッチ取得でエラー: ${(error as Error).message}`);
        for (const info of batch) {
          try {
            const docSnap = await colRef.doc(info.id).get();
            if (docSnap.exists) {
              const data = docSnap.data();
              if (data) {
                docs.push({
                  id: docSnap.id,
                  ref: docSnap.ref,
                  data,
                  key,
                  filledFieldsCount: countFilledFields(data),
                });
              }
            } else {
              console.warn(`  ⚠️  ドキュメント ${info.id} は存在しません（スキップ）`);
            }
          } catch (err) {
            console.warn(`  ⚠️  ドキュメント ${info.id} の取得に失敗: ${(err as Error).message}（スキップ）`);
          }
        }
      }
    }
    
    // ドキュメントが1件も取得できなかった場合、または1件のみの場合はスキップ
    if (docs.length === 0) {
      console.warn(`  ⚠️  グループ内のドキュメントが全て取得できませんでした（スキップ）`);
      return;
    }
    
    if (docs.length === 1) {
      console.warn(`  ⚠️  グループ内のドキュメントが1件のみです（重複なし、スキップ）`);
      return;
    }
    
    // フィールド数で再ソート（取得したデータで正確な値を計算）
    docs.sort((a, b) => b.filledFieldsCount - a.filledFieldsCount);

    const master = docs[0];
    const duplicates = docs.slice(1);

    // マスターに他のドキュメントの情報をマージ
    let mergedData = { ...master.data };
    let hasMerged = false;

    for (const dup of duplicates) {
      const beforeCount = countFilledFields(mergedData);
      mergedData = mergeDocuments(mergedData, dup.data);
      const afterCount = countFilledFields(mergedData);

      if (afterCount > beforeCount) {
        hasMerged = true;
      }
    }

    // ログ出力
    console.log(`【統合グループ】`);
    console.log(`  企業名: ${master.data.name || "(なし)"}`);
    console.log(`  住所: ${master.data.address || "(なし)"}`);
    console.log(`  都道府県: ${master.data.prefecture || "(なし)"}`);
    console.log(`  代表者名: ${master.data.representativeName || "(なし)"}`);
    console.log(
      `  マスタードキュメント: ${master.id} (フィールド数: ${master.filledFieldsCount})`
    );
    if (hasMerged) {
      const mergedCount = countFilledFields(mergedData);
      console.log(
        `  マージ後フィールド数: ${mergedCount} (+${mergedCount - master.filledFieldsCount})`
      );
    }
    console.log(`  統合対象: ${duplicates.length} 件`);

    if (!DRY_RUN) {
      // マスタードキュメントを更新（マージした場合のみ）
      if (hasMerged) {
        mergedData.updatedAt = admin.firestore.FieldValue.serverTimestamp();
        await master.ref.update(mergedData);
        console.log(`  ✅ マスター更新完了`);
      }

      // 重複ドキュメントを削除（バッチ処理）
      for (const dup of duplicates) {
        batch.delete(dup.ref);
        batchCount++;
        deletedCount++;

        if (batchCount >= BATCH_DELETE_SIZE) {
          await batch.commit();
          console.log(
            `  💾 削除バッチコミット: ${batchCount}件 (累計削除: ${deletedCount})`
          );
          batch = db.batch();
          batchCount = 0;
        }
      }
    } else {
      console.log(`  🔍 (DRY_RUN) マスター更新予定`);
      for (const dup of duplicates) {
        console.log(`  🔍 (DRY_RUN) 削除予定: ${dup.id}`);
      }
      deletedCount += duplicates.length;
    }

    mergedCount++;

    if (mergedCount % 100 === 0) {
      console.log(`\n📊 進捗: ${mergedCount}/${duplicateKeys.length}グループ処理完了\n`);
    } else {
      console.log("");
    }
  }

  // 残りのバッチをコミット
  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    console.log(
      `💾 最終削除バッチコミット: ${batchCount}件 (累計削除: ${deletedCount})`
    );
  }

  console.log(`\n✅ 統合処理完了`);
  console.log(`  - 統合グループ数: ${mergedCount}`);
  console.log(`  - 削除ドキュメント数: ${deletedCount}`);
  console.log(`  - スキャン総数: ${scanned}`);

  if (DRY_RUN) {
    console.log(
      `\n💡 実際に統合するには、--dry-run フラグを外して実行してください。`
    );
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

