/* eslint-disable no-console */

/**
 * scripts/unify_fields.ts
 *
 * ✅ 目的
 * - companies_new コレクション内の全ドキュメントでフィールドを統一
 * - 存在しないフィールドにはnullを設定（配列フィールドは空配列）
 * - 全てのドキュメントで同じフィールド構造を持つようにする
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
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

// ユーティリティ関数
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ------------------------------
// 期待されるフィールド定義（check_missing_fields.tsと同じ）
// ------------------------------

const EXPECTED_FIELDS = {
  // 📊 基本情報
  basic: [
    "name",
    "nameEn",
    "kana",
    "corporateNumber",
    "corporationType",
    "nikkeiCode",
    "badges",
    "tags",
  ],
  // 📍 所在地情報
  location: [
    "prefecture",
    "address",
    "headquartersAddress",
    "postalCode",
    "location",
    "departmentLocation",
  ],
  // 📞 連絡先情報
  contact: [
    "phoneNumber",
    "contactPhoneNumber",
    "fax",
    "email",
    "companyUrl",
    "contactFormUrl",
  ],
  // 👤 代表者情報
  representative: [
    "representativeName",
    "representativeKana",
    "representativeTitle",
    "representativeBirthDate",
    "representativePhone",
    "representativePostalCode",
    "representativeHomeAddress",
    "representativeRegisteredAddress",
    "representativeAlmaMater",
    "executives",
  ],
  // 🏢 業種情報
  industry: [
    "industry",
    "industryLarge",
    "industryMiddle",
    "industrySmall",
    "industryDetail",
    "industries",
    "industryCategories",
    "businessDescriptions",
    "businessItems",
    "businessSummary",
    "specialties",
    "demandProducts",
    "specialNote",
  ],
  // 💰 財務情報
  financial: [
    "capitalStock",
    "revenue",
    "latestRevenue",
    "latestProfit",
    "revenueFromStatements",
    "operatingIncome",
    "totalAssets",
    "totalLiabilities",
    "netAssets",
    "issuedShares",
    "financials",
    "listing",
    "marketSegment",
    "latestFiscalYearMonth",
    "fiscalMonth",
  ],
  // 🏭 企業規模・組織
  organization: [
    "employeeCount",
    "employeeNumber",
    "factoryCount",
    "officeCount",
    "storeCount",
    "averageAge",
    "averageYearsOfService",
    "averageOvertimeHours",
    "averagePaidLeave",
    "femaleExecutiveRatio",
  ],
  // 📅 設立・沿革
  establishment: [
    "established",
    "dateOfEstablishment",
    "founding",
    "foundingYear",
    "acquisition",
  ],
  // 🤝 取引先・関係会社
  relationships: [
    "clients",
    "suppliers",
    "subsidiaries",
    "affiliations",
    "shareholders",
    "banks",
    "bankCorporateNumber",
  ],
  // 📝 企業説明
  description: [
    "overview",
    "companyDescription",
    "businessDescriptions",
    "salesNotes",
  ],
  // 🌐 SNS・外部リンク
  external: [
    "urls",
    "profileUrl",
    "externalDetailUrl",
    "facebook",
    "linkedin",
    "wantedly",
    "youtrust",
    "metaKeywords",
  ],
};

/**
 * 配列フィールドのリスト
 */
const ARRAY_FIELDS = new Set([
  "badges",
  "tags",
  "executives",
  "industries",
  "industryCategories",
  "businessItems",
  "specialties",
  "demandProducts",
  "suppliers",
  "subsidiaries",
  "shareholders",
  "banks",
  "urls",
]);

/**
 * フィールドが存在しない場合のデフォルト値を取得
 */
function getDefaultValue(fieldName: string): any {
  if (ARRAY_FIELDS.has(fieldName)) {
    return [];
  }
  return null;
}

/**
 * ドキュメントに不足しているフィールドを追加（nullまたは空配列を設定）
 */
function unifyDocumentFields(companyData: any): { [key: string]: any } {
  const updates: { [key: string]: any } = {};
  
  // 全期待フィールドを取得
  const allExpectedFields: string[] = [];
  Object.values(EXPECTED_FIELDS).forEach((fields) => {
    allExpectedFields.push(...fields);
  });

  // 各フィールドをチェック
  for (const field of allExpectedFields) {
    // フィールドが存在しない場合、デフォルト値を設定
    if (!(field in companyData)) {
      updates[field] = getDefaultValue(field);
    }
  }

  return updates;
}

/**
 * メイン処理: 全ドキュメントのフィールドを統一
 */
async function unifyAllFields() {
  try {
    console.log("全ドキュメントのフィールド統一を開始...");

    const BATCH_SIZE = 100; // バッチサイズを小さく（トランザクションサイズ制限対策）
    const WRITE_BATCH_SIZE = 50; // 書き込みバッチサイズも小さく
    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;
    let totalUpdated = 0;

    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }

      console.log(`\nバッチ取得: ${snapshot.size} 件`);

      let batch = db.batch();
      let batchCount = 0;

      for (const companyDoc of snapshot.docs) {
        const companyId = companyDoc.id;
        const companyData = companyDoc.data();

        // 不足フィールドを特定
        const updates = unifyDocumentFields(companyData);

        if (Object.keys(updates).length > 0) {
          batch.update(companyDoc.ref, updates);
          batchCount++;
          totalUpdated++;

          // Firestoreのバッチ制限に達したらコミット（サイズ制限対策で50件に減らす）
          if (batchCount >= WRITE_BATCH_SIZE) {
            await batch.commit();
            console.log(`  バッチコミット完了: ${batchCount} 件`);
            batch = db.batch(); // 新しいバッチを作成
            batchCount = 0;
            // レート制限対策で少し待機
            await sleep(100);
          }
        }

        totalProcessed++;
      }

      // 残りのバッチをコミット
      if (batchCount > 0) {
        await batch.commit();
        console.log(`  バッチコミット完了: ${batchCount} 件`);
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
      
      console.log(`処理済み: ${totalProcessed} 件 / 更新: ${totalUpdated} 件`);
      
      // レート制限対策で少し待機
      await sleep(200);
    }

    console.log(`\n✅ 処理完了`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`更新数: ${totalUpdated} 件`);
  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
unifyAllFields()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });

