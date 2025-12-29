/* 
  companies_new コレクションの上場企業に対して証券コードを追加するスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/add_security_code_to_listed_companies.ts
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
import { parse } from "csv-parse/sync";
import type { DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const SHOKEN_CODE_CSV_PATH = "./shokenCode/shokenCode.csv";
const BATCH_LIMIT = 500; // Firestoreのバッチ制限

// companies_new の新規ドキュメント用テンプレート
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
  securityCode: null,
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

// ドキュメントIDを数字のみの文字列に統一する
function generateNumericDocId(
  corporateNumber: string | null | undefined,
  index: number
): string {
  // corporateNumberが存在し、数字のみの場合 → そのまま使用
  if (corporateNumber && /^[0-9]+$/.test(corporateNumber.trim())) {
    return corporateNumber.trim();
  }
  
  // それ以外の場合 → Date.now() + インデックスから数字のみの一意IDを生成
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// ==============================
// Firebase 初期化
// ==============================
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
      path.join(projectRoot, "config", "serviceAccountKey.json"),
      path.join(projectRoot, ".config", "serviceAccountKey.json"),
    ];

    for (const pth of defaultPaths) {
      const resolved = path.resolve(pth);
      if (fs.existsSync(resolved)) {
        serviceAccountPath = resolved;
        console.log(`ℹ️  デフォルトのサービスアカウントキーを使用: ${resolved}`);
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

const db = admin.firestore();
const companiesCol = db.collection(COLLECTION_NAME) as CollectionReference<DocumentData>;

// ==============================
// ユーティリティ関数
// ==============================

// 企業名を正規化（空白削除、株式会社表記統一など）
function normalizeCompanyName(name: string): string {
  if (!name) return "";
  return name
    .trim()
    .replace(/\s+/g, "")
    .replace(/株式会社/g, "")
    .replace(/（株）/g, "")
    .replace(/\(株\)/g, "")
    .replace(/㈱/g, "")
    .replace(/（有）/g, "")
    .replace(/\(有\)/g, "")
    .replace(/有限会社/g, "")
    .replace(/合資会社/g, "")
    .replace(/合名会社/g, "");
}

// 銘柄名から企業名の候補を生成
function generateCompanyNameCandidates(brandName: string): string[] {
  const candidates: string[] = [];
  const normalized = brandName.trim();
  
  if (!normalized) return candidates;
  
  // 元の銘柄名
  candidates.push(normalized);
  
  // 株式会社を前につける
  candidates.push(`株式会社${normalized}`);
  
  // 株式会社を後ろにつける
  candidates.push(`${normalized}株式会社`);
  
  // 正規化版も追加
  const normalizedBrand = normalizeCompanyName(normalized);
  if (normalizedBrand && normalizedBrand !== normalized) {
    candidates.push(normalizedBrand);
    candidates.push(`株式会社${normalizedBrand}`);
    candidates.push(`${normalizedBrand}株式会社`);
  }
  
  return [...new Set(candidates)]; // 重複削除
}

// 企業名がマッチするかチェック
function isCompanyNameMatch(companyName: string | null | undefined, candidates: string[]): boolean {
  if (!companyName) return false;
  
  const companyNameTrimmed = companyName.trim();
  const normalizedCompany = normalizeCompanyName(companyNameTrimmed);
  
  // 企業名のバリエーション
  const companyNameVariants = [
    companyNameTrimmed,
    normalizedCompany,
    `株式会社${normalizedCompany}`,
    `${normalizedCompany}株式会社`,
  ];
  
  for (const candidate of candidates) {
    const candidateTrimmed = candidate.trim();
    const normalizedCandidate = normalizeCompanyName(candidateTrimmed);
    
    // 完全一致チェック
    for (const variant of companyNameVariants) {
      if (variant === candidateTrimmed || variant === normalizedCandidate) {
        return true;
      }
    }
    
    // 正規化後の完全一致チェック
    if (normalizedCompany === normalizedCandidate && normalizedCompany.length > 0) {
      return true;
    }
    
    // 部分一致チェック（より柔軟なマッチング、ただし短すぎる場合は除外）
    if (normalizedCompany.length >= 3 && normalizedCandidate.length >= 3) {
      if (normalizedCompany.includes(normalizedCandidate) || normalizedCandidate.includes(normalizedCompany)) {
        // 一方が他方の大部分を含む場合のみマッチ
        const minLen = Math.min(normalizedCompany.length, normalizedCandidate.length);
        const maxLen = Math.max(normalizedCompany.length, normalizedCandidate.length);
        if (minLen >= maxLen * 0.7) { // 70%以上一致
          return true;
        }
      }
    }
  }
  
  return false;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  console.log("🚀 証券コード追加スクリプト開始\n");

  // 1. 証券コードCSVを読み込む
  console.log("📖 証券コードCSVを読み込み中...");
  const csvPath = path.resolve(SHOKEN_CODE_CSV_PATH);
  if (!fs.existsSync(csvPath)) {
    console.error(`❌ エラー: 証券コードCSVファイルが見つかりません: ${csvPath}`);
    process.exit(1);
  }

  const csvContent = fs.readFileSync(csvPath, "utf8");
  const records = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
    bom: true,
  });

  // 証券コードマップを作成（銘柄名 -> 証券コード）
  // ETF・ETNは別途処理
  const securityCodeMap = new Map<string, string>();
  const etfEtnRecords: Array<Record<string, string>> = [];
  
  for (const record of records) {
    const recordData = record as Record<string, string>;
    const code = recordData["コード"]?.trim();
    const brandName = recordData["銘柄名"]?.trim();
    const marketType = recordData["市場・商品区分"]?.trim();
    
    if (!code || !brandName) continue;
    
    // ETF・ETNは別途保存
    if (marketType === "ETF・ETN") {
      etfEtnRecords.push(recordData);
      continue;
    }
    
    // 既に同じ銘柄名がある場合は最初のものを優先
    if (!securityCodeMap.has(brandName)) {
      securityCodeMap.set(brandName, code);
    }
  }

  console.log(`✅ 証券コードCSV読み込み完了`);
  console.log(`   - 総レコード数: ${records.length} 件`);
  console.log(`   - ETF・ETN: ${etfEtnRecords.length} 件`);
  console.log(`   - 有効な証券コード: ${securityCodeMap.size} 件\n`);

  // 2. 上場企業を取得して処理（メモリ効率を考慮してバッチ処理）
  console.log("📦 上場企業を取得・処理中...");
  
  let matchedCount = 0;
  let updatedCount = 0;
  let alreadyHasCodeCount = 0;
  let notMatchedCount = 0;
  let listedCount = 0;
  let totalFetched = 0;
  let batchCount = 0;
  let batch = db.batch();
  
  // 全件取得してからフィルタリング（listingフィールドのインデックス問題を回避）
  let lastDoc: DocumentSnapshot<DocumentData> | null = null;
  const FETCH_BATCH_SIZE = 1000;

  while (true) {
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

    // バッチごとに処理（メモリ効率化）
    for (const doc of snapshot.docs) {
      const data = doc.data();
      // listingが実際に値を持っているか確認（null/空文字列でない）
      const listing = data.listing;
      if (listing && typeof listing === "string" && listing.trim() !== "") {
        listedCount++;
        const companyName = data.name || data.companyName || "";
        
        // 既に証券コードがある場合はスキップ
        if (data.securityCode || data.securitiesCode || data.stockCode) {
          alreadyHasCodeCount++;
        } else {
          // 証券コードをマッチング
          let matchedCode: string | null = null;
          
          for (const [brandName, code] of securityCodeMap.entries()) {
            const candidates = generateCompanyNameCandidates(brandName);
            if (isCompanyNameMatch(companyName, candidates)) {
              matchedCode = code;
              matchedCount++;
              break;
            }
          }

          if (matchedCode) {
            // 証券コードを追加
            batch.update(doc.ref, {
              securityCode: matchedCode,
            });
            updatedCount++;
            batchCount++;

            if (batchCount >= BATCH_LIMIT) {
              console.log(`   💾 バッチコミット (${batchCount} 件) ...`);
              await batch.commit();
              batch = db.batch();
              batchCount = 0;
            }
          } else {
            notMatchedCount++;
          }
        }
      }
    }

    totalFetched += snapshot.size;
    if (totalFetched % 5000 === 0 || snapshot.size < FETCH_BATCH_SIZE) {
      console.log(`   進捗: ${totalFetched} 件取得 / 上場企業: ${listedCount} 件 (マッチ: ${matchedCount}, 更新: ${updatedCount}, 既存: ${alreadyHasCodeCount}, 未マッチ: ${notMatchedCount})`);
    }

    if (snapshot.size < FETCH_BATCH_SIZE) {
      break;
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }

  // 残りのバッチをコミット
  if (batchCount > 0) {
    console.log(`   💾 最後のバッチコミット (${batchCount} 件) ...`);
    await batch.commit();
  }

  console.log(`✅ 上場企業処理完了: ${listedCount} 件\n`);

  // 4. ETF・ETNを新規作成
  console.log("\n📝 ETF・ETNを新規作成中...");
  
  // まず、既存のETF・ETN（listing="ETF・ETN"）を「上場」に修正
  console.log("   🔧 既存のETF・ETNのlistingを修正中...");
  let fixBatchCount = 0;
  let fixBatch = db.batch();
  let fixedCount = 0;
  
  let fixQuery = companiesCol
    .where("listing", "==", "ETF・ETN")
    .limit(1000);
  
  let fixSnapshot = await fixQuery.get();
  while (!fixSnapshot.empty) {
    for (const doc of fixSnapshot.docs) {
      fixBatch.update(doc.ref, {
        listing: "上場",
      });
      fixedCount++;
      fixBatchCount++;
      
      if (fixBatchCount >= BATCH_LIMIT) {
        console.log(`   💾 修正バッチコミット (${fixBatchCount} 件) ...`);
        await fixBatch.commit();
        fixBatch = db.batch();
        fixBatchCount = 0;
      }
    }
    
    if (fixSnapshot.size < 1000) {
      break;
    }
    
    const lastDoc = fixSnapshot.docs[fixSnapshot.docs.length - 1];
    fixQuery = companiesCol
      .where("listing", "==", "ETF・ETN")
      .startAfter(lastDoc)
      .limit(1000);
    fixSnapshot = await fixQuery.get();
  }
  
  if (fixBatchCount > 0) {
    console.log(`   💾 最後の修正バッチコミット (${fixBatchCount} 件) ...`);
    await fixBatch.commit();
  }
  
  if (fixedCount > 0) {
    console.log(`   ✅ 既存のETF・ETNのlistingを修正: ${fixedCount} 件`);
  }
  
  // 既存のETF・ETNを一括取得（listing="上場"で検索し、securityCodeとnameの組み合わせで確認）
  console.log("   🔍 既存のETF・ETNを確認中...");
  const existingEtfEtn = new Set<string>();
  let existingQuery = companiesCol
    .where("listing", "==", "上場")
    .limit(1000);
  
  let existingSnapshot = await existingQuery.get();
  for (const doc of existingSnapshot.docs) {
    const data = doc.data();
    const name = data.name || "";
    const code = data.securityCode || "";
    if (name && code) {
      existingEtfEtn.add(`${name}::${code}`);
    }
  }
  
  // ページングで全件取得
  while (existingSnapshot.size === 1000) {
    const lastDoc = existingSnapshot.docs[existingSnapshot.docs.length - 1];
    existingQuery = companiesCol
      .where("listing", "==", "上場")
      .startAfter(lastDoc)
      .limit(1000);
    existingSnapshot = await existingQuery.get();
    for (const doc of existingSnapshot.docs) {
      const data = doc.data();
      const name = data.name || "";
      const code = data.securityCode || "";
      if (name && code) {
        existingEtfEtn.add(`${name}::${code}`);
      }
    }
  }
  
  console.log(`   ✅ 既存のETF・ETN: ${existingEtfEtn.size} 件`);

  let etfEtnCreatedCount = 0;
  let etfEtnSkippedCount = 0;
  let etfEtnBatchCount = 0;
  let etfEtnBatch = db.batch();
  let globalIndex = Date.now(); // タイムスタンプベースのインデックス

  for (const record of etfEtnRecords) {
    const code = record["コード"]?.trim();
    const brandName = record["銘柄名"]?.trim();
    const marketType = record["市場・商品区分"]?.trim();
    const industry33Code = record["33業種コード"]?.trim();
    const industry33Category = record["33業種区分"]?.trim();
    const industry17Code = record["17業種コード"]?.trim();
    const industry17Category = record["17業種区分"]?.trim();
    const scaleCode = record["規模コード"]?.trim();
    const scaleCategory = record["規模区分"]?.trim();

    if (!code || !brandName) continue;

    // 既存のETF・ETNをチェック
    const key = `${brandName}::${code}`;
    if (existingEtfEtn.has(key)) {
      etfEtnSkippedCount++;
      continue;
    }

    // 新規ドキュメントを作成
    const docId = generateNumericDocId(null, globalIndex);
    const docRef = companiesCol.doc(docId);

    // テンプレートをベースにデータを設定
    const companyData: Record<string, any> = {
      ...COMPANY_TEMPLATE,
      name: brandName,
      securityCode: code,
      listing: "上場", // ETF・ETNは上場商品なので「上場」を設定
      createdAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    // 業種情報があれば設定
    if (industry33Category) {
      companyData.industryLarge = industry33Category;
    }
    if (industry17Category) {
      companyData.industryMiddle = industry17Category;
    }
    if (scaleCategory) {
      companyData.marketSegment = scaleCategory;
    }

    etfEtnBatch.set(docRef, companyData, { merge: true });
    etfEtnCreatedCount++;
    etfEtnBatchCount++;
    globalIndex++;

    if (etfEtnBatchCount >= BATCH_LIMIT) {
      console.log(`   💾 ETF・ETNバッチコミット (${etfEtnBatchCount} 件) ...`);
      await etfEtnBatch.commit();
      etfEtnBatch = db.batch();
      etfEtnBatchCount = 0;
    }

    if (etfEtnCreatedCount % 50 === 0) {
      console.log(`   進捗: ${etfEtnCreatedCount}/${etfEtnRecords.length} 件作成済み (スキップ: ${etfEtnSkippedCount})`);
    }
  }

  // 残りのETF・ETNバッチをコミット
  if (etfEtnBatchCount > 0) {
    console.log(`   💾 最後のETF・ETNバッチコミット (${etfEtnBatchCount} 件) ...`);
    await etfEtnBatch.commit();
  }

  console.log(`✅ ETF・ETN新規作成完了: ${etfEtnCreatedCount} 件 (スキップ: ${etfEtnSkippedCount} 件)\n`);

  // 5. 結果サマリー
  console.log("\n✅ 処理完了");
  console.log(`   📊 上場企業総数: ${listedCount} 件`);
  console.log(`   ✅ 証券コード追加: ${updatedCount} 件`);
  console.log(`   ⏭️  既に証券コードあり: ${alreadyHasCodeCount} 件`);
  console.log(`   ❌ マッチしなかった: ${notMatchedCount} 件`);
  console.log(`   🔍 マッチした銘柄: ${matchedCount} 件`);
  console.log(`   📝 ETF・ETN新規作成: ${etfEtnCreatedCount} 件 (スキップ: ${etfEtnSkippedCount} 件)`);
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

