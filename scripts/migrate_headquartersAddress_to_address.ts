/**
 * companies_new コレクションの headquartersAddress フィールドを address に移行するスクリプト
 *
 * 処理内容:
 * - address が null で headquartersAddress に値がある場合: headquartersAddress の値を address に移す
 * - 両方に値がある場合: より住所らしい方を address に設定
 * - 最終的に headquartersAddress フィールドを削除
 *
 * 使い方:
 *   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
 *   npx ts-node scripts/migrate_headquartersAddress_to_address.ts [--dry-run]
 */

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  WriteBatch,
  DocumentSnapshot,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// シンプルな空判定
function isEmpty(v: any): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === "string" && v.trim() === "") return true;
  return false;
}

// 文字列に変換（null/undefined の場合は空文字列）
function toString(v: any): string {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

// 住所らしさをスコアリング（高いほど住所らしい）
function scoreAddressLike(str: string): number {
  if (!str || str.length === 0) return 0;
  
  let score = 0;
  const s = str;
  
  // 都道府県名が含まれている（高いスコア）
  const prefectures = [
    "都", "道", "府", "県",
    "北海道", "青森", "岩手", "宮城", "秋田", "山形", "福島",
    "茨城", "栃木", "群馬", "埼玉", "千葉", "東京", "神奈川",
    "新潟", "富山", "石川", "福井", "山梨", "長野", "岐阜",
    "静岡", "愛知", "三重", "滋賀", "京都", "大阪", "兵庫",
    "奈良", "和歌山", "鳥取", "島根", "岡山", "広島", "山口",
    "徳島", "香川", "愛媛", "高知", "福岡", "佐賀", "長崎",
    "熊本", "大分", "宮崎", "鹿児島", "沖縄"
  ];
  for (const pref of prefectures) {
    if (s.includes(pref)) {
      score += 10;
      break;
    }
  }
  
  // 市区町村を示す文字列
  if (s.includes("市") || s.includes("区") || s.includes("町") || s.includes("村")) {
    score += 5;
  }
  
  // 番地や建物名を示す文字列
  if (s.match(/\d+[-\-]?\d+/) || s.includes("丁目") || s.includes("番地") || s.includes("号")) {
    score += 3;
  }
  
  // 建物名を示す文字列
  if (s.includes("ビル") || s.includes("マンション") || s.includes("アパート") || 
      s.includes("タワー") || s.includes("プラザ") || s.includes("センター")) {
    score += 2;
  }
  
  // 長さによる補正（住所は通常ある程度の長さがある）
  if (s.length >= 10) score += 1;
  if (s.length >= 20) score += 1;
  
  return score;
}

// どちらの値がより住所らしいかを判定
function chooseBetterAddress(address: string, headquartersAddress: string): string {
  const addressScore = scoreAddressLike(address);
  const hqScore = scoreAddressLike(headquartersAddress);
  
  // スコアが同じ場合は、より長い方を選ぶ
  if (addressScore === hqScore) {
    return address.length >= headquartersAddress.length ? address : headquartersAddress;
  }
  
  // スコアが高い方を選ぶ
  return addressScore >= hqScore ? address : headquartersAddress;
}

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
      path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
      path.join(projectRoot, "serviceAccountKey.json"),
      path.join(projectRoot, "service-account-key.json"),
      path.join(projectRoot, "firebase-service-account.json"),
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

const db: Firestore = admin.firestore();
const col: CollectionReference = db.collection(COLLECTION_NAME);

async function main() {
  if (DRY_RUN) {
    console.log("🔍 DRY RUN モード: 実際の更新は行いません");
  }
  
  console.log("🔎 companies_new 全件をスキャンします…");
  
  let lastDoc: any = null;
  let totalFetched = 0;
  const FETCH_BATCH_SIZE = 1000;
  let processedCount = 0;
  let updatedCount = 0;
  let deletedOnlyCount = 0;
  let skippedCount = 0;

  while (true) {
    // ドキュメントID順に取得しながらページングする
    let query = col
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
    if (totalFetched <= FETCH_BATCH_SIZE || totalFetched % 5000 === 0) {
      console.log(`📦 取得中... (${totalFetched} 件)`);
    }

    let batch: WriteBatch = db.batch();
    let batchCount = 0;
    const BATCH_LIMIT = 400;

    for (let i = 0; i < snapshot.docs.length; i++) {
      const doc = snapshot.docs[i];
      const data = doc.data() || {};
      lastDoc = doc;

      const address = toString(data.address);
      const headquartersAddress = toString(data.headquartersAddress);
      
      const hasAddress = !isEmpty(address);
      const hasHeadquartersAddress = !isEmpty(headquartersAddress);

      // headquartersAddress フィールドが存在しない場合はスキップ
      if (!hasHeadquartersAddress && !(data.headquartersAddress !== undefined)) {
        skippedCount++;
        processedCount++;
        continue;
      }

      let updateData: any = {};
      let needsUpdate = false;

      if (!hasAddress && hasHeadquartersAddress) {
        // address が null で headquartersAddress に値がある場合: 値を移す
        updateData.address = headquartersAddress;
        updateData.headquartersAddress = admin.firestore.FieldValue.delete();
        needsUpdate = true;
        updatedCount++;
      } else if (hasAddress && hasHeadquartersAddress) {
        // 両方に値がある場合: より住所らしい方を選ぶ
        const betterAddress = chooseBetterAddress(address, headquartersAddress);
        if (betterAddress !== address) {
          updateData.address = betterAddress;
        }
        updateData.headquartersAddress = admin.firestore.FieldValue.delete();
        needsUpdate = true;
        updatedCount++;
      } else if (hasAddress && !hasHeadquartersAddress) {
        // address に値があり、headquartersAddress が空の場合: headquartersAddress だけ削除
        updateData.headquartersAddress = admin.firestore.FieldValue.delete();
        needsUpdate = true;
        deletedOnlyCount++;
      } else {
        // 両方空の場合: headquartersAddress だけ削除
        updateData.headquartersAddress = admin.firestore.FieldValue.delete();
        needsUpdate = true;
        deletedOnlyCount++;
      }

      if (needsUpdate) {
        if (!DRY_RUN) {
          batch.update(doc.ref, updateData);
          batchCount++;
        } else {
          // DRY RUN モードではログのみ
          if (updatedCount <= 10 || updatedCount % 100 === 0) {
            console.log(`[DRY RUN] ${doc.id}: address="${updateData.address || address}", headquartersAddress削除`);
          }
          batchCount++;
        }

        if (batchCount >= BATCH_LIMIT) {
          if (!DRY_RUN) {
            console.log(`💾 バッチコミット (${batchCount} 件)…`);
            await batch.commit();
          }
          batch = db.batch();
          batchCount = 0;
        }
      }

      processedCount++;
    }

    if (batchCount > 0) {
      if (!DRY_RUN) {
        console.log(`💾 バッチコミット (${batchCount} 件)…`);
        await batch.commit();
      }
    }
  }

  console.log("✅ マイグレーション完了");
  console.log(`  処理したドキュメント: ${processedCount} 件`);
  console.log(`  address を更新したドキュメント: ${updatedCount} 件`);
  console.log(`  headquartersAddress のみ削除したドキュメント: ${deletedOnlyCount} 件`);
  console.log(`  変更不要だったドキュメント: ${skippedCount} 件`);
  
  if (DRY_RUN) {
    console.log("\n⚠️  DRY RUN モードでした。実際の更新を行うには --dry-run フラグを外してください。");
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

