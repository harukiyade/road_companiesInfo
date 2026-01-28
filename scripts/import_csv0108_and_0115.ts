/* 
  csv0108配下とcsv0115配下の全CSVを Firestore `companies_new` に
  「本番モード（ドライランなし）」でインポート / 更新するスクリプト。

  要件:
  - 会社ID・リストID・取引種別・状態・SBフラグ・NDA・AD・ステータス・備考ヘッダーの値は無視
  - ドキュメントIDは数値で生成（既存の generateNumericDocId ロジックを踏襲）
  - 会社名・都道府県・代表者名などでドキュメントを特定
    1. corporateNumber で検索
    2. なければ name + prefecture + representativeName などの複合キー
  - 既存企業があれば null フィールドのみ CSV で埋める（上書きしない）
  - 新規企業は companies_new のスキーマに沿って作成
  - 資本金・売上・利益は「千円」の値の場合は 1000 倍して保存
    - csv0115 配下: 1000 倍
    - csv0108 配下: 1 倍（そのまま）。文字列は無視
  - csv0108/5.csv には営業所情報があるので、以下の新規フィールドを追加:
      branchOfficeName         ← 営業所名
      branchOfficePostalCode   ← 営業所郵便番号
      branchOfficePhoneNumber  ← 営業所電話番号
      branchOfficeAddress      ← 営業所所在地
  - 業種は data/industry.csv をマスタとして、「近い内容」を industry 系フィールドへ設定
    - industryLarge / industryMiddle / industrySmall / industryDetail / industries / industry

  使い方（本番モードのみ。DRY_RUNモードは用意しない）:

    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/import_csv0108_and_0115.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type {
  Firestore,
  CollectionReference,
  DocumentReference,
  WriteBatch,
} from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";
import iconv from "iconv-lite";

const COLLECTION_NAME = "companies_new";

const CSV_DIRS = ["csv0108", "csv0115"];

// Firestore バッチのサイズ
const BATCH_SIZE = 400;

// ==============================
// 型定義
// ==============================

type CompaniesNewDoc = Record<string, any>;

interface CsvContext {
  filePath: string;
  isKiloYen: boolean; // 資本金・売上・利益を1000倍するかどうか
  hasBranchOfficeFields: boolean; // 営業所系カラムを持つか（csv0108/5.csv）
}

interface IndustryMasterEntry {
  field: "industryLarge" | "industryMiddle" | "industrySmall" | "industryDetail";
  value: string;
}

interface IndustryMaster {
  all: IndustryMasterEntry[];
}

interface MatchKey {
  corporateNumber: string | null;
  name: string | null;
  prefecture: string | null;
  representativeName: string | null;
}

// ==============================
// Firebase 初期化
// ==============================

function initFirestore(): Firestore {
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

    console.log(`✅ Firebase Admin initialized (Project ID: ${projectId})`);
  }

  return admin.firestore();
}

// ==============================
// ユーティリティ
// ==============================

function isEmpty(v: any): boolean {
  if (v === null || v === undefined) return true;
  if (typeof v === "string") return v.trim() === "" || v.trim() === "-";
  return false;
}

function normalizeString(v: any): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v)
    .trim()
    .replace(/\s+/g, " ");
  return s === "" ? null : s;
}

function normalizeKeyString(v: any): string | null {
  if (v === null || v === undefined) return null;
  const s = String(v)
    .trim()
    .replace(/\s+/g, "")
    .replace(/[（）()]/g, "");
  return s === "" ? null : s;
}

function parseNumeric(value: any, multiplier: number): number | null {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  if (s === "" || s === "-" || s === "ー") return null;
  // 数値以外の文字を除去してから parse
  const cleaned = s.replace(/[,，]/g, "").replace(/[^\d.-]/g, "");
  if (cleaned === "" || cleaned === "-" || cleaned === ".") return null;
  const n = Number(cleaned);
  if (!Number.isFinite(n)) return null;
  return n * multiplier;
}

// 13桁法人番号チェック
function isValidCorporateNumber(corpNum: string | null | undefined): boolean {
  if (!corpNum) return false;
  const normalized = corpNum.trim().replace(/[^0-9]/g, "");
  return /^[0-9]{13}$/.test(normalized);
}

// 数値ドキュメントID生成（既存スクリプトと同等ロジック）
function generateNumericDocId(corporateNumber: string | null, index: number): string {
  if (corporateNumber && isValidCorporateNumber(corporateNumber)) {
    return corporateNumber.trim().replace(/[^0-9]/g, "");
  }
  const timestamp = Date.now();
  const paddedIndex = String(index).padStart(6, "0");
  return `${timestamp}${paddedIndex}`;
}

// ==============================
// industry.csv マスタ読み込み
// ==============================

function loadIndustryMaster(): IndustryMaster {
  const masterPath = path.resolve("data/industry.csv");
  if (!fs.existsSync(masterPath)) {
    console.warn("⚠️  data/industry.csv が見つかりません。業種マスタなしで続行します。");
    return { all: [] };
  }

  const raw = fs.readFileSync(masterPath, "utf8");
  const records = parse(raw, {
    columns: true,
    skip_empty_lines: true,
  }) as { フィールド: string; 値: string }[];

  const all: IndustryMasterEntry[] = [];
  for (const r of records) {
    const field = r["フィールド"];
    const value = r["値"];
    if (!field || !value) continue;
    if (
      field !== "industryLarge" &&
      field !== "industryMiddle" &&
      field !== "industrySmall" &&
      field !== "industryDetail"
    ) {
      continue;
    }
    all.push({
      field,
      value,
    } as IndustryMasterEntry);
  }

  console.log(`✅ industryマスタ読み込み: ${all.length} 件`);
  return { all };
}

// 文字列を比較用に正規化
function normalizeIndustryStr(s: string): string {
  return s
    .trim()
    .toLowerCase()
    .replace(/\s+/g, "")
    .replace(/[（）()]/g, "")
    .replace(/[・、，,]/g, "");
}

// CSVの業種文字列に最も近いマスタの1件を返す
function findClosestIndustryEntry(
  rawValue: string | null,
  master: IndustryMaster
): IndustryMasterEntry | null {
  if (!rawValue || !master.all.length) return null;
  const targetNorm = normalizeIndustryStr(rawValue);
  if (!targetNorm) return null;

  let best: IndustryMasterEntry | null = null;
  let bestScore = 0;

  for (const entry of master.all) {
    const candNorm = normalizeIndustryStr(entry.value);
    if (!candNorm) continue;

    // 完全一致
    if (candNorm === targetNorm) {
      return entry;
    }

    // 部分一致スコア
    let score = 0;
    if (candNorm.includes(targetNorm) || targetNorm.includes(candNorm)) {
      score = Math.min(targetNorm.length, candNorm.length);
    } else {
      // 先頭部分一致
      const minLen = Math.min(targetNorm.length, candNorm.length);
      let prefix = 0;
      for (let i = 0; i < minLen; i++) {
        if (targetNorm[i] === candNorm[i]) prefix++;
        else break;
      }
      score = prefix;
    }

    if (score > bestScore) {
      bestScore = score;
      best = entry;
    }
  }

  // ある程度以上（例: 3文字以上）似ている場合のみ採用
  if (best && bestScore >= 3) {
    return best;
  }
  return null;
}

// CSVの業種1〜3 から companies_new 用の業種フィールドを生成
function mapIndustriesFromCsvValues(
  industriesRaw: string[],
  master: IndustryMaster
): {
  industry: string | null;
  industryLarge: string | null;
  industryMiddle: string | null;
  industrySmall: string | null;
  industryDetail: string | null;
  industries: string[];
  industryCategories: string | null;
} {
  const results: IndustryMasterEntry[] = [];

  for (const raw of industriesRaw) {
    const norm = normalizeString(raw);
    if (!norm) continue;
    const found = findClosestIndustryEntry(norm, master);
    if (found) {
      results.push(found);
    }
  }

  const industries: string[] = [];
  let industry: string | null = null;
  let industryLarge: string | null = null;
  let industryMiddle: string | null = null;
  let industrySmall: string | null = null;
  let industryDetail: string | null = null;
  let industryCategories: string | null = null;

  for (const r of results) {
    if (!industries.includes(r.value)) {
      industries.push(r.value);
    }
    if (!industry) {
      industry = r.value;
    }
    // ラベル文字列の先頭をざっくり大分類候補にする
    const [largeCandidate] = r.value.split(/\s+/);

    if (r.field === "industryLarge") {
      if (!industryLarge) industryLarge = r.value;
      if (!industryCategories && largeCandidate) {
        industryCategories = largeCandidate;
      }
    } else if (r.field === "industryMiddle") {
      if (!industryMiddle) industryMiddle = r.value;
      if (!industryLarge && largeCandidate) industryLarge = largeCandidate;
    } else if (r.field === "industrySmall") {
      if (!industrySmall) industrySmall = r.value;
      if (!industryMiddle) industryMiddle = r.value;
      if (!industryLarge && largeCandidate) industryLarge = largeCandidate;
    } else if (r.field === "industryDetail") {
      if (!industryDetail) industryDetail = r.value;
      if (!industryMiddle) industryMiddle = r.value;
      if (!industryLarge && largeCandidate) industryLarge = largeCandidate;
    }
  }

  return {
    industry,
    industryLarge,
    industryMiddle,
    industrySmall,
    industryDetail,
    industries,
    industryCategories,
  };
}

// ==============================
// CSV 1行 → companies_new データマッピング
// ==============================

function mapCsvRowToCompany(
  row: Record<string, string>,
  ctx: CsvContext,
  industryMaster: IndustryMaster
): { data: CompaniesNewDoc; key: MatchKey } {
  const data: CompaniesNewDoc = {};

  const header = (name: string): string | null =>
    Object.prototype.hasOwnProperty.call(row, name) ? row[name] ?? null : null;

  const fileName = path.basename(ctx.filePath);
  const isCsv0108 = ctx.filePath.startsWith("csv0108/");

  // 基本情報
  const name = normalizeString(header("会社名") ?? header("商号又は名称"));
  const prefecture = normalizeString(header("都道府県"));
  const representativeName = normalizeString(header("代表者名"));
  let corporateNumberRaw =
    normalizeString(header("法人番号")) ?? normalizeString(header("法人番号(13桁)"));

  if (corporateNumberRaw && !isValidCorporateNumber(corporateNumberRaw)) {
    corporateNumberRaw = null;
  }

  if (name) data.name = name;
  if (prefecture) data.prefecture = prefecture;
  if (representativeName) data.representativeName = representativeName;
  if (corporateNumberRaw) data.corporateNumber = corporateNumberRaw;

  // 住所・連絡先
  const postalCode = normalizeString(
    header("郵便番号") ?? header("営業所郵便番号")
  );
  const address = normalizeString(
    header("住所") ?? header("営業所所在地")
  );
  const phoneNumber = normalizeString(
    header("電話番号(窓口)") ?? header("営業所電話番号")
  );
  const companyUrl = normalizeString(header("URL"));

  if (postalCode) data.postalCode = postalCode;
  if (address) data.address = address;
  if (phoneNumber) data.phoneNumber = phoneNumber;
  if (companyUrl) data.companyUrl = companyUrl;

  // 代表者住所系
  const repPostal = normalizeString(header("代表者郵便番号"));
  const repAddress = normalizeString(header("代表者住所"));
  const repBirth = normalizeString(header("代表者誕生日"));
  if (repPostal) data.representativePostalCode = repPostal;
  if (repAddress) data.representativeHomeAddress = repAddress;
  if (repBirth) data.representativeBirthDate = repBirth;

  // 財務（倍率はコンテキスト依存）
  const multiplier = ctx.isKiloYen ? 1000 : 1;
  const capitalRaw = header("資本金");
  const latestRevenueRaw = header("直近売上") ?? header("法人＿売上高");
  const latestProfitRaw = header("直近利益");

  const capital = parseNumeric(capitalRaw, multiplier);
  const latestRevenue = parseNumeric(latestRevenueRaw, multiplier);
  const latestProfit = parseNumeric(latestProfitRaw, multiplier);

  if (capital !== null) data.capitalStock = capital;
  if (latestRevenue !== null) data.latestRevenue = latestRevenue;
  if (latestProfit !== null) data.latestProfit = latestProfit;

  // csv0108/5.csv のその他財務（1倍でそのまま）
  if (isCsv0108 && fileName === "5.csv") {
    const netAssetsRaw = header("法人＿純資産合計");
    const totalAssetsRaw = header("法人＿資産合計");
    const totalLiabilitiesRaw = header("法人＿負債合計");
    const revenueFromStatementsRaw = header("法人＿売上高");
    const netAssets = parseNumeric(netAssetsRaw, 1);
    const totalAssets = parseNumeric(totalAssetsRaw, 1);
    const totalLiabilities = parseNumeric(totalLiabilitiesRaw, 1);
    const revenueFromStatements = parseNumeric(revenueFromStatementsRaw, 1);
    if (netAssets !== null) data.netAssets = netAssets;
    if (totalAssets !== null) data.totalAssets = totalAssets;
    if (totalLiabilities !== null) data.totalLiabilities = totalLiabilities;
    if (revenueFromStatements !== null) {
      data.revenueFromStatements = revenueFromStatements;
    }
  }

  // 上場・決算
  const listing = normalizeString(header("上場"));
  const latestFiscalYearMonth = normalizeString(header("直近決算年月"));
  if (listing) data.listing = listing;
  if (latestFiscalYearMonth) data.latestFiscalYearMonth = latestFiscalYearMonth;

  // 説明・概要
  const description = normalizeString(header("説明"));
  const overview = normalizeString(header("概要"));
  if (description) data.companyDescription = description;
  if (overview) data.overview = overview;

  // 取引先
  const suppliersRaw = normalizeString(header("仕入れ先"));
  const clientsRaw = normalizeString(header("取引先"));
  const banksRaw = normalizeString(header("取引先銀行"));
  if (suppliersRaw) data.suppliers = [suppliersRaw];
  if (clientsRaw) data.clients = clientsRaw;
  if (banksRaw) data.banks = [banksRaw];

  // 株主・役員・従業員数等
  const shareholders = normalizeString(header("株主"));
  const executives = normalizeString(header("取締役"));
  const employeeCount = parseNumeric(header("社員数"), 1);
  const officeCount = parseNumeric(header("オフィス数"), 1);
  const factoryCount = parseNumeric(header("工場数"), 1);
  const storeCount = parseNumeric(header("店舗数"), 1);

  if (shareholders) data.shareholders = [shareholders];
  if (executives) data.executives = executives;
  if (employeeCount !== null) data.employeeCount = employeeCount;
  if (officeCount !== null) data.officeCount = officeCount;
  if (factoryCount !== null) data.factoryCount = factoryCount;
  if (storeCount !== null) data.storeCount = storeCount;

  // 営業所情報（csv0108/5.csv 限定）
  if (ctx.hasBranchOfficeFields) {
    const branchName = normalizeString(header("営業所名"));
    const branchPostal = normalizeString(header("営業所郵便番号"));
    const branchPhone = normalizeString(header("営業所電話番号"));
    const branchAddress = normalizeString(header("営業所所在地"));
    if (branchName) data.branchOfficeName = branchName;
    if (branchPostal) data.branchOfficePostalCode = branchPostal;
    if (branchPhone) data.branchOfficePhoneNumber = branchPhone;
    if (branchAddress) data.branchOfficeAddress = branchAddress;
  }

  // 業種1〜3 を industryマスタに基づき正規化
  const rawIndustries: string[] = [];
  const ind1 = normalizeString(header("業種1"));
  const ind2 = normalizeString(header("業種2"));
  const ind3 = normalizeString(header("業種3"));
  if (ind1) rawIndustries.push(ind1);
  if (ind2) rawIndustries.push(ind2);
  if (ind3) rawIndustries.push(ind3);

  if (rawIndustries.length > 0) {
    const mapped = mapIndustriesFromCsvValues(rawIndustries, industryMaster);
    if (mapped.industry) data.industry = mapped.industry;
    if (mapped.industryLarge) data.industryLarge = mapped.industryLarge;
    if (mapped.industryMiddle) data.industryMiddle = mapped.industryMiddle;
    if (mapped.industrySmall) data.industrySmall = mapped.industrySmall;
    if (mapped.industryDetail) data.industryDetail = mapped.industryDetail;
    if (mapped.industries.length > 0) data.industries = mapped.industries;
    if (mapped.industryCategories) data.industryCategories = mapped.industryCategories;
  }

  const key: MatchKey = {
    corporateNumber: corporateNumberRaw,
    name,
    prefecture,
    representativeName,
  };

  return { data, key };
}

// ==============================
// 既存ドキュメント検索
// ==============================

async function findExistingCompanyDoc(
  colRef: CollectionReference,
  key: MatchKey
): Promise<DocumentReference | null> {
  // 1. 法人番号
  if (key.corporateNumber) {
    const snap = await colRef
      .where("corporateNumber", "==", key.corporateNumber)
      .limit(2)
      .get();
    if (!snap.empty) {
      if (snap.size > 1) {
        console.warn(
          `⚠️  corporateNumber=${key.corporateNumber} で複数ドキュメントが見つかりました。最初の1件を使用します。`
        );
      }
      return snap.docs[0].ref;
    }
  }

  // 2. 会社名 + 都道府県 + 代表者名
  if (key.name && key.prefecture && key.representativeName) {
    const snap = await colRef
      .where("name", "==", key.name)
      .where("prefecture", "==", key.prefecture)
      .where("representativeName", "==", key.representativeName)
      .limit(2)
      .get();
    if (!snap.empty) {
      if (snap.size > 1) {
        console.warn(
          `⚠️  name+prefecture+representativeName で複数ドキュメントが見つかりました。最初の1件を使用します。 name="${key.name}", pref="${key.prefecture}", rep="${key.representativeName}"`
        );
      }
      return snap.docs[0].ref;
    }
  }

  // 3. 会社名 + 都道府県
  if (key.name && key.prefecture) {
    const snap = await colRef
      .where("name", "==", key.name)
      .where("prefecture", "==", key.prefecture)
      .limit(2)
      .get();
    if (!snap.empty) {
      if (snap.size > 1) {
        console.warn(
          `⚠️  name+prefecture で複数ドキュメントが見つかりました。最初の1件を使用します。 name="${key.name}", pref="${key.prefecture}"`
        );
      }
      return snap.docs[0].ref;
    }
  }

  return null;
}

// companies_new の既存値が null/未定義/空文字のときだけ CSV 値で埋める
function mergeCsvIntoExisting(existing: CompaniesNewDoc, csvData: CompaniesNewDoc): CompaniesNewDoc {
  const merged: CompaniesNewDoc = { ...existing };
  for (const [field, value] of Object.entries(csvData)) {
    const current = (existing as any)[field];
    if (current === null || current === undefined || current === "") {
      (merged as any)[field] = value;
    }
  }
  return merged;
}

// ==============================
// CSVファイルの読み込み
// ==============================

function readCsvFile(filePath: string): Record<string, string>[] {
  const buf = fs.readFileSync(filePath);

  // csv0108 は UTF-8 前提、csv0115 は Shift_JIS の可能性が高いのでディレクトリで分岐
  const rel = path.relative(process.cwd(), filePath);
  const is0115 = rel.startsWith("csv0115" + path.sep);

  let text: string;
  if (is0115) {
    text = iconv.decode(buf, "cp932");
  } else {
    text = buf.toString("utf8");
  }

  const records = parse(text, {
    columns: true,
    skip_empty_lines: true,
  }) as Record<string, string>[];

  return records;
}

// ==============================
// メイン処理
// ==============================

async function processAll() {
  const db = initFirestore();
  const colRef = db.collection(COLLECTION_NAME);
  const industryMaster = loadIndustryMaster();

  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  let processedRows = 0;
  let updatedDocs = 0;
  let createdDocs = 0;

  for (const dir of CSV_DIRS) {
    const dirPath = path.resolve(dir);
    if (!fs.existsSync(dirPath)) {
      console.warn(`⚠️  ディレクトリが存在しません: ${dirPath}`);
      continue;
    }

    const files = fs
      .readdirSync(dirPath)
      .filter((f) => f.toLowerCase().endsWith(".csv"))
      .sort();

    console.log(`📂 処理ディレクトリ: ${dirPath} (ファイル数: ${files.length})`);

    for (const file of files) {
      const relPath = path.join(dir, file);
      const absPath = path.resolve(relPath);

      const isKiloYen = dir === "csv0115";
      const hasBranchOfficeFields = dir === "csv0108" && file === "5.csv";

      const ctx: CsvContext = {
        filePath: `${dir}/${file}`,
        isKiloYen,
        hasBranchOfficeFields,
      };

      console.log(`\n=== 📄 ファイル処理開始: ${relPath} (isKiloYen=${isKiloYen}) ===`);

      const rows = readCsvFile(absPath);
      console.log(`   行数: ${rows.length}`);

      let rowIndex = 0;
      for (const row of rows) {
        rowIndex++;
        processedRows++;

        const { data: csvData, key } = mapCsvRowToCompany(row, ctx, industryMaster);

        if (!key.name && !key.corporateNumber) {
          // 同定不能な行はスキップ
          continue;
        }

        const existingRef = await findExistingCompanyDoc(colRef, key);

        if (existingRef) {
          const snap = await existingRef.get();
          const existingData = snap.data() || {};
          const merged = mergeCsvIntoExisting(existingData, csvData);
          batch.set(existingRef, merged, { merge: true });
          updatedDocs++;
        } else {
          // 新規作成
          const docId = generateNumericDocId(
            key.corporateNumber,
            processedRows
          );
          const newRef = colRef.doc(docId);
          const newData: CompaniesNewDoc = {
            ...csvData,
            companyId: docId,
          };
          batch.set(newRef, newData, { merge: false });
          createdDocs++;
        }

        batchCount++;
        if (batchCount >= BATCH_SIZE) {
          await batch.commit();
          console.log(
            `💾 バッチコミット: ${batchCount} 件 (累計: processedRows=${processedRows}, updated=${updatedDocs}, created=${createdDocs})`
          );
          batch = db.batch();
          batchCount = 0;
        }
      }
    }
  }

  if (batchCount > 0) {
    await batch.commit();
    console.log(
      `💾 最終バッチコミット: ${batchCount} 件 (累計: processedRows=${processedRows}, updated=${updatedDocs}, created=${createdDocs})`
    );
  }

  console.log("\n✅ すべての処理が完了しました。");
  console.log(`   総処理行数 : ${processedRows}`);
  console.log(`   更新ドキュメント数 : ${updatedDocs}`);
  console.log(`   新規作成ドキュメント数 : ${createdDocs}`);
}

processAll().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

