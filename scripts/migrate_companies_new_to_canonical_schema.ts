/* 
  companies_new コレクションを新スキーマ＆数値IDに統一するマイグレーションスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/migrate_companies_new_to_canonical_schema.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";

// ドライランモード（--dry-run フラグで有効化）
const DRY_RUN = process.argv.includes("--dry-run");

// ==============================
// 正規スキーマ定義（COMPANY_TEMPLATE）
// ==============================
const COMPANY_TEMPLATE: Record<string, any> = {
  acquisition: null,
  adExpiration: null,
  address: null,
  businessDescriptions: null,
  capitalStock: null,
  changeCount: null,
  clients: null,
  companyDescription: null,
  companyUrl: null,
  contactFormUrl: null,
  corporateNumber: null,
  corporationType: null,
  createdAt: null,
  demandProducts: null,
  email: null,
  employeeCount: null,
  established: null,
  executives: null,
  facebook: null,
  factoryCount: null,
  fax: null,
  financials: null,
  fiscalMonth: null,
  foundingYear: null,
  headquartersAddress: null,
  industries: [],
  industry: null,
  industryCategories: null,
  industryDetail: null,
  industryLarge: null,
  industryMiddle: null,
  industrySmall: null,
  linkedin: null,
  listing: null,
  marketSegment: null,
  metaDescription: null,
  metaKeywords: null,
  name: null,
  officeCount: null,
  overview: null,
  phoneNumber: null,
  postalCode: null,
  prefecture: null,
  registrant: null,
  representativeAlmaMater: null,
  representativeBirthDate: null,
  representativeHomeAddress: null,
  representativeKana: null,
  representativeName: null,
  representativePhone: null,
  representativeRegisteredAddress: null,
  representativeTitle: null,
  revenue: null,
  salesNotes: null,
  shareholders: [],
  storeCount: null,
  suppliers: [],
  tags: [],
  updateCount: null,
  updatedAt: null,
  urls: [],
  wantedly: null,
  youtrust: null,
};

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  
  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPaths = [
      "./serviceAccountKey.json",
      "./service-account-key.json",
      "./firebase-service-account.json",
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
      path.join(projectRoot, "config", "serviceAccountKey.json"),
      path.join(projectRoot, ".config", "serviceAccountKey.json"),
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
      projectId: projectId,
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

// ==============================
// ヘルパー関数
// ==============================

// ドキュメントIDを数字のみの文字列に統一する
function generateNumericDocId(
  corporateNumber: string | null | undefined,
  index: number,
  existingDocId?: string
): string {
  // corporateNumberが存在し、数字のみの場合 → そのまま使用
  if (
    corporateNumber &&
    typeof corporateNumber === "string" &&
    /^[0-9]+$/.test(corporateNumber.trim())
  ) {
    return corporateNumber.trim();
  }

  // 既存のdocIdが数字のみの場合 → そのまま使用
  if (existingDocId && /^[0-9]+$/.test(existingDocId)) {
    return existingDocId;
  }

  // それ以外の場合 → Date.now() + インデックスから数字のみの一意IDを生成
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// 既存データを新スキーマに変換
function migrateToCanonicalSchema(oldData: Record<string, any>): Record<string, any> {
  const newData: Record<string, any> = { ...COMPANY_TEMPLATE };

  // フィールド名のマッピング（旧フィールド名 → 新フィールド名）
  const fieldMapping: Record<string, string> = {
    companyName: "name",
    hpUrl: "companyUrl",
    description: "companyDescription",
    industry1: "industry",
    industry2: "industries",
    industry3: "industries",
  };

  // 配列フィールドのリスト
  const arrayFields = new Set<string>([
    "industries",
    "urls",
    "tags",
    "shareholders",
    "suppliers",
  ]);

  // 既存データから新スキーマにマッピング
  for (const [oldKey, value] of Object.entries(oldData)) {
    // フィールド名のマッピングを適用
    const newKey = fieldMapping[oldKey] || oldKey;

    // 新スキーマに含まれるフィールドのみ処理
    if (newKey in COMPANY_TEMPLATE) {
      // 値が空でない場合のみ設定
      if (value !== null && value !== undefined && value !== "") {
        // 配列フィールドの処理
        if (arrayFields.has(newKey)) {
          if (Array.isArray(value)) {
            newData[newKey] = value.filter(
              (v: any) => v !== null && v !== undefined && v !== ""
            );
          } else if (typeof value === "string" && value.trim() !== "") {
            // カンマ区切りの文字列を配列に変換
            newData[newKey] = value
              .split(",")
              .map((s: string) => s.trim())
              .filter((s: string) => s !== "");
          }
        } else if (Array.isArray(COMPANY_TEMPLATE[newKey])) {
          // テンプレートが配列の場合
          if (Array.isArray(value)) {
            newData[newKey] = value.filter(
              (v: any) => v !== null && v !== undefined && v !== ""
            );
          } else if (typeof value === "string" && value.trim() !== "") {
            newData[newKey] = value
              .split(",")
              .map((s: string) => s.trim())
              .filter((s: string) => s !== "");
          }
        } else {
          newData[newKey] = value;
        }
      }
    }
  }

  return newData;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  if (DRY_RUN) {
    console.log("🔍 DRY_RUN モード: Firestore は書き換えません\n");
  } else {
    console.log("⚠️  本番モード: Firestore を書き換えます\n");
  }

  console.log("📊 companies_new コレクションを取得中...");

  let processedCount = 0;
  let skippedCount = 0;
  let idChangedCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const BATCH_LIMIT = 200;
  const FETCH_BATCH_SIZE = 1000; // 一度に取得するドキュメント数（読み取りラウンドトリップ削減のため増加）

  const docsToDelete: DocumentReference[] = [];
  const newDocIds = new Set<string>(); // 新しいIDの重複チェック用

  // バッチ処理で取得（メモリ効率化）
  let lastDoc: any = null;
  let totalFetched = 0;
  let globalIndex = 0;

  while (true) {
    // ドキュメントID順に取得しながらページングする
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(FETCH_BATCH_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();

    if (snapshot.empty) {
      break;
    }

    totalFetched += snapshot.size;
    // ログは間引いて出す（ログ IO 負荷軽減）
    if (totalFetched <= FETCH_BATCH_SIZE || totalFetched % 5000 === 0) {
      console.log(`📦 取得中... (${totalFetched} 件)`);
    }

    for (let i = 0; i < snapshot.docs.length; i++) {
      const doc = snapshot.docs[i];
      const oldId = doc.id;
      const oldData = doc.data();

      // 新スキーマに変換
      const newData = migrateToCanonicalSchema(oldData);

      // 新しいdocIdを決定
      const corporateNumber =
        newData.corporateNumber ||
        oldData.corporateNumber ||
        (oldData as any).corporate_number;
      let newId = generateNumericDocId(corporateNumber, globalIndex, oldId);

      // oldData と newData が同一で、かつ ID も変わらない場合は書き込みをスキップして高速化
      let isSame = oldId === newId;
      if (isSame) {
        for (const key of Object.keys(COMPANY_TEMPLATE)) {
          const oldVal = (oldData as any)[key] ?? null;
          const newVal = (newData as any)[key] ?? null;
          if (JSON.stringify(oldVal) !== JSON.stringify(newVal)) {
            isSame = false;
            break;
          }
        }
      }

      if (isSame) {
        processedCount++;
        globalIndex++;
        if (globalIndex <= 10) {
          console.log(`⏭️  [${globalIndex}] 変更なしのためスキップ: "${oldId}"`);
        }
        continue;
      }

      // 新しいIDが既に使用されている場合、一意のIDを生成
      let retryCount = 0;
      while (newDocIds.has(newId) && retryCount < 10) {
        newId = generateNumericDocId(
          null,
          globalIndex + retryCount * 10000,
          undefined
        );
        retryCount++;
      }
      newDocIds.add(newId);

      // IDが変更された場合
      if (oldId !== newId) {
        idChangedCount++;

        if (!DRY_RUN) {
          // 新しいIDでドキュメントを作成
          const newRef = companiesCol.doc(newId);
          batch.set(newRef, newData, { merge: true });
          batchCount++;

          // 古いドキュメントを削除リストに追加
          docsToDelete.push(doc.ref);
        }

        if (globalIndex < 10 || idChangedCount <= 10) {
          console.log(`🔄 [${globalIndex + 1}] ID変更: "${oldId}" → "${newId}"`);
        }
      } else {
        // IDが同じ場合、既存ドキュメントを更新
        if (!DRY_RUN) {
          batch.set(doc.ref, newData, { merge: true });
          batchCount++;
        }

        if (globalIndex < 10) {
          console.log(`✅ [${globalIndex + 1}] 更新: "${oldId}"`);
        }
      }

      processedCount++;
      // newId が数字のみかどうかをチェックし、そうでなければスキップカウントに加算してログに出す
      if (!/^[0-9]+$/.test(newId)) {
        skippedCount++;
        if (skippedCount <= 10) {
          console.warn(
            `⚠️  非数値IDのまま残ったドキュメントがあります: oldId="${oldId}", newId="${newId}"`
          );
        }
      }
      globalIndex++;

      // バッチコミット
      if (batchCount >= BATCH_LIMIT) {
        if (!DRY_RUN) {
          console.log(`💾 バッチコミット (${batchCount} 件) ...`);
          await batch.commit();
          batch = db.batch();
          batchCount = 0;
        }
      }
    }

    // 次のバッチの開始位置を設定
    lastDoc = snapshot.docs[snapshot.docs.length - 1];

    // 定期的に進捗ログ
    if (totalFetched % 10000 === 0) {
      console.log(`  📊 現在までの処理済み: ${processedCount} 件`);
    }
  }

  // 最後のバッチをコミット
  if (!DRY_RUN && batchCount > 0) {
    console.log(`💾 最後のバッチコミット (${batchCount} 件) ...`);
    await batch.commit();
  }

  // 古いドキュメントを削除（新しいドキュメント作成後に実行）
  if (!DRY_RUN && docsToDelete.length > 0) {
    console.log(`\n🗑️  古いドキュメントを削除中 (${docsToDelete.length} 件)...`);
    const DELETE_BATCH_LIMIT = 200;
    for (let i = 0; i < docsToDelete.length; i += DELETE_BATCH_LIMIT) {
      const batchToDelete = docsToDelete.slice(i, i + DELETE_BATCH_LIMIT);
      const deleteBatch = db.batch();
      for (const ref of batchToDelete) {
        deleteBatch.delete(ref);
      }
      await deleteBatch.commit();
      console.log(
        `  💾 削除バッチコミット (${batchToDelete.length} 件) ...`
      );
    }
  }

  console.log("\n✅ マイグレーション完了");
  console.log(`  📊 処理件数: ${processedCount} 件`);
  console.log(`  🔄 ID変更: ${idChangedCount} 件`);
  console.log(`  ⏭️  スキップ: ${skippedCount} 件`);

  if (DRY_RUN) {
    console.log(
      "\n💡 実際にマイグレーションを実行するには、--dry-run フラグを外して実行してください"
    );
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});