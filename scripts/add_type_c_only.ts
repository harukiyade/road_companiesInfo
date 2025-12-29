/* 
  タイプCのみをcompanies_newコレクションに追加する簡易スクリプト
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";

// Firebase初期化
if (!admin.apps.length) {
  const serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || 
    path.join(__dirname, "../albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
  
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccountPath)
  });
}

const db: Firestore = admin.firestore();

async function main() {
  const csvContent = fs.readFileSync("csv/105.csv", "utf-8");
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    trim: true,
  }) as Record<string, any>[];

  const row = records[0];

  const companyData = {
    name: row["会社名"] || null,
    phoneNumber: row["電話番号"] || null,
    postalCode: null,
    address: null,
    companyUrl: row["URL"] || null,
    representativeName: row["代表者"] || null,
    representativeRegisteredAddress: row["郵便番号"] || null,
    representativeHomeAddress: row["住所"] || null,
    foundingYear: row["創業"] || null,
    established: row["設立"] || null,
    shareholders: row["株式保有率"] || null,
    executives: row["役員"] || null,
    overview: row["概要"] || null,
    industryLarge: row["業種（大）"] || null,
    industryMiddle: row["業種（中）"] || null,
    industrySmall: row["業種（小）"] || null,
    industryDetail: row["業種（細）"] || null,
    csvType: "C",
    csvSource: "csv/105.csv",
    createdAt: admin.firestore.FieldValue.serverTimestamp(),
  };

  console.log("タイプC企業データ:");
  console.log(JSON.stringify(companyData, null, 2));

  const docRef = await db.collection(COLLECTION_NAME).add(companyData);
  console.log(`\n✓ 追加完了: ドキュメントID = ${docRef.id}`);
}

main().then(() => process.exit(0)).catch((err) => {
  console.error("エラー:", err);
  process.exit(1);
});

