/* 
  洗い出したcorporateNumberがnullまたは存在しないドキュメントに対して、
  企業名と住所を使って国税庁のCSVデータから法人番号を特定して更新する
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    PREF_ZIP_PATH=pref/00_zenkoku_all_20251226.zip \
    INPUT_LIST=null_or_missing_corporate_numbers.json \
    DRY_RUN=1 \
    npx tsx scripts/update_corporate_numbers_from_list.ts
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference, DocumentReference, WriteBatch } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse";
import { createReadStream } from "fs";
import * as csv from "csv-writer";

const COLLECTION_NAME = "companies_new";
const BATCH_SIZE = 400; // Firestore batch limit is 500, use 400 for safety

// ==============================
// Firebase初期化
// ==============================

let db: Firestore;
let companiesCol: CollectionReference;

function initAdmin() {
  if (admin.apps.length > 0) {
    db = admin.firestore();
    companiesCol = db.collection(COLLECTION_NAME);
    return;
  }

  const serviceAccountPath =
    process.env.GOOGLE_APPLICATION_CREDENTIALS ||
    path.join(__dirname, "../serviceAccountKey.json");

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(`❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });

  db = admin.firestore();
  companiesCol = db.collection(COLLECTION_NAME);
}

function log(message: string) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
}

// ==============================
// 正規化関数（backfill_corporate_number_from_pref.tsからコピー）
// ==============================

function normalizeCompanyName(name: string | null | undefined): string | null {
  if (!name || name.trim() === "") return null;

  let normalized = name.trim();

  const corporateTypes = [
    { pattern: /\(株\)|（株）|㈱/g, replacement: "株式会社" },
    { pattern: /\(有\)|（有）|㈲/g, replacement: "有限会社" },
    { pattern: /\(合\)|（合）|㈱合/g, replacement: "合同会社" },
    { pattern: /\(医\)|（医）/g, replacement: "医療法人" },
    { pattern: /\(学\)|（学）/g, replacement: "学校法人" },
    { pattern: /\(福\)|（福）/g, replacement: "社会福祉法人" },
    { pattern: /\(宗\)|（宗）/g, replacement: "宗教法人" },
    { pattern: /\(社\)|（社）/g, replacement: "一般社団法人" },
    { pattern: /\(財\)|（財）/g, replacement: "一般財団法人" },
    { pattern: /\(特\)|（特）/g, replacement: "特定非営利活動法人" },
  ];

  for (const { pattern, replacement } of corporateTypes) {
    normalized = normalized.replace(pattern, replacement);
  }

  normalized = normalized.replace(/[（）()【】「」『』［］]/g, "");
  normalized = normalized.replace(/[\s\u3000]+/g, "");
  normalized = normalized.replace(/[Ａ-Ｚａ-ｚ０-９]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) - 0xFEE0);
  });
  normalized = normalized.replace(/[－―ー−‐‑]/g, "-");
  normalized = normalized.replace(/[ァ-ヶ]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) + 0x60);
  });

  return normalized;
}

function normalizeAddress(address: string | null | undefined): string | null {
  if (!address || address.trim() === "") return null;

  let normalized = address.trim();

  const prefectures = [
    { pattern: /^北海道|^ほっかいどう/gi, replacement: "北海道" },
    { pattern: /^青森県|^あおもりけん/gi, replacement: "青森県" },
    { pattern: /^岩手県|^いわてけん/gi, replacement: "岩手県" },
    { pattern: /^宮城県|^みやぎけん/gi, replacement: "宮城県" },
    { pattern: /^秋田県|^あきたけん/gi, replacement: "秋田県" },
    { pattern: /^山形県|^やまがたけん/gi, replacement: "山形県" },
    { pattern: /^福島県|^ふくしまけん/gi, replacement: "福島県" },
    { pattern: /^茨城県|^いばらきけん/gi, replacement: "茨城県" },
    { pattern: /^栃木県|^とちぎけん/gi, replacement: "栃木県" },
    { pattern: /^群馬県|^ぐんまけん/gi, replacement: "群馬県" },
    { pattern: /^埼玉県|^さいたまけん/gi, replacement: "埼玉県" },
    { pattern: /^千葉県|^ちばけん/gi, replacement: "千葉県" },
    { pattern: /^東京都|^とうきょうと/gi, replacement: "東京都" },
    { pattern: /^神奈川県|^かながわけん/gi, replacement: "神奈川県" },
    { pattern: /^新潟県|^にいがたけん/gi, replacement: "新潟県" },
    { pattern: /^富山県|^とやまけん/gi, replacement: "富山県" },
    { pattern: /^石川県|^いしかわけん/gi, replacement: "石川県" },
    { pattern: /^福井県|^ふくいけん/gi, replacement: "福井県" },
    { pattern: /^山梨県|^やまなしけん/gi, replacement: "山梨県" },
    { pattern: /^長野県|^ながのけん/gi, replacement: "長野県" },
    { pattern: /^岐阜県|^ぎふけん/gi, replacement: "岐阜県" },
    { pattern: /^静岡県|^しずおかけん/gi, replacement: "静岡県" },
    { pattern: /^愛知県|^あいちけん/gi, replacement: "愛知県" },
    { pattern: /^三重県|^みえけん/gi, replacement: "三重県" },
    { pattern: /^滋賀県|^しがけん/gi, replacement: "滋賀県" },
    { pattern: /^京都府|^きょうとふ/gi, replacement: "京都府" },
    { pattern: /^大阪府|^おおさかふ/gi, replacement: "大阪府" },
    { pattern: /^兵庫県|^ひょうごけん/gi, replacement: "兵庫県" },
    { pattern: /^奈良県|^ならけん/gi, replacement: "奈良県" },
    { pattern: /^和歌山県|^わかやまけん/gi, replacement: "和歌山県" },
    { pattern: /^鳥取県|^とっとりけん/gi, replacement: "鳥取県" },
    { pattern: /^島根県|^しまねけん/gi, replacement: "島根県" },
    { pattern: /^岡山県|^おかやまけん/gi, replacement: "岡山県" },
    { pattern: /^広島県|^ひろしまけん/gi, replacement: "広島県" },
    { pattern: /^山口県|^やまぐちけん/gi, replacement: "山口県" },
    { pattern: /^徳島県|^とくしまけん/gi, replacement: "徳島県" },
    { pattern: /^香川県|^かがわけん/gi, replacement: "香川県" },
    { pattern: /^愛媛県|^えひめけん/gi, replacement: "愛媛県" },
    { pattern: /^高知県|^こうちけん/gi, replacement: "高知県" },
    { pattern: /^福岡県|^ふくおかけん/gi, replacement: "福岡県" },
    { pattern: /^佐賀県|^さがけん/gi, replacement: "佐賀県" },
    { pattern: /^長崎県|^ながさきけん/gi, replacement: "長崎県" },
    { pattern: /^熊本県|^くまもとけん/gi, replacement: "熊本県" },
    { pattern: /^大分県|^おおいたけん/gi, replacement: "大分県" },
    { pattern: /^宮崎県|^みやざきけん/gi, replacement: "宮崎県" },
    { pattern: /^鹿児島県|^かごしまけん/gi, replacement: "鹿児島県" },
    { pattern: /^沖縄県|^おきなわけん/gi, replacement: "沖縄県" },
  ];

  for (const { pattern, replacement } of prefectures) {
    if (pattern.test(normalized)) {
      normalized = normalized.replace(pattern, replacement);
      break;
    }
  }

  const cityMatch = normalized.match(/^(.+?[都道府県])(.+?[市区町村])(.+?[町丁目])/);
  if (cityMatch) {
    normalized = cityMatch[1] + cityMatch[2] + cityMatch[3];
  } else {
    const cityOnlyMatch = normalized.match(/^(.+?[都道府県])(.+?[市区町村])/);
    if (cityOnlyMatch) {
      normalized = cityOnlyMatch[1] + cityOnlyMatch[2];
    }
  }

  normalized = normalized.replace(/[\s\u3000]+/g, "");
  normalized = normalized.replace(/[－―ー−‐‑]/g, "-");

  return normalized;
}

function normalizePostalCode(postalCode: string | null | undefined): string | null {
  if (!postalCode || postalCode.trim() === "") return null;
  return postalCode.trim().replace(/[-ー−‐‑]/g, "");
}

// ==============================
// ZIP展開とCSV読み込み
// ==============================

function extractZip(zipPath: string): string {
  log(`📦 ZIPファイルを展開中: ${zipPath}`);
  
  const extractDir = path.join(path.dirname(zipPath), "extracted");
  if (!fs.existsSync(extractDir)) {
    fs.mkdirSync(extractDir, { recursive: true });
  }

  const zipFileName = path.basename(zipPath, ".zip");
  const extractedCsvPath = path.join(extractDir, `${zipFileName}.csv`);

  if (fs.existsSync(extractedCsvPath)) {
    log(`  ✅ 既に展開済み: ${extractedCsvPath}`);
    return extractedCsvPath;
  }

  try {
    const { execSync } = require("child_process");
    execSync(`unzip -o "${zipPath}" -d "${extractDir}"`, { stdio: "inherit" });
    
    const files = fs.readdirSync(extractDir);
    const csvFile = files.find(f => f.endsWith(".csv") && !f.endsWith(".asc"));
    
    if (!csvFile) {
      throw new Error("CSVファイルが見つかりません");
    }

    const finalPath = path.join(extractDir, csvFile);
    log(`  ✅ 展開完了: ${finalPath}`);
    return finalPath;
  } catch (error: any) {
    console.error(`❌ ZIP展開エラー: ${error.message}`);
    throw error;
  }
}

function detectEncoding(buffer: Buffer): "utf8" | "utf16le" | "shift_jis" {
  if (buffer.length >= 2 && buffer[0] === 0xFF && buffer[1] === 0xFE) {
    return "utf16le";
  }
  if (buffer.length >= 3 && buffer[0] === 0xEF && buffer[1] === 0xBB && buffer[2] === 0xBF) {
    return "utf8";
  }

  try {
    const utf8Text = buffer.toString("utf8");
    if (utf8Text.includes("法人番号") || utf8Text.includes("商号")) {
      return "utf8";
    }
  } catch {}

  return "utf8";
}

/**
 * CSVファイルを読み込んで索引を構築
 */
async function buildIndexFromPrefCsv(csvPath: string, limit?: number): Promise<{
  nameAddressMap: Map<string, Set<string>>;
  namePostalMap: Map<string, Set<string>>;
  nameOnlyMap: Map<string, Set<string>>;
  totalRecords: number;
}> {
  log(`📖 CSVファイルを読み込み中: ${csvPath}`);

  const nameAddressMap = new Map<string, Set<string>>();
  const namePostalMap = new Map<string, Set<string>>();
  const nameOnlyMap = new Map<string, Set<string>>();
  
  let totalRecords = 0;

  const sampleBuffer = fs.readFileSync(csvPath, null, { start: 0, end: 10000 });
  const encoding = detectEncoding(sampleBuffer);
  log(`  📝 文字コード: ${encoding}`);

  return new Promise((resolve, reject) => {
    const readStream = createReadStream(csvPath, { encoding: encoding === "utf16le" ? "utf16le" : "utf8" });
    
    const parser = parse({
      columns: false,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      bom: true,
    });

    parser.on("readable", () => {
      let record: string[];
      while ((record = parser.read()) !== null) {
        if (limit && totalRecords >= limit) {
          readStream.destroy();
          parser.destroy();
          resolve({ nameAddressMap, namePostalMap, nameOnlyMap, totalRecords });
          return;
        }

        if (record.length < 16) continue;

        try {
          const corporateNumber = record[1]?.trim().replace(/^["']|["']$/g, "");
          const name = record[6]?.trim().replace(/^["']|["']$/g, "");
          const prefecture = record[9]?.trim().replace(/^["']|["']$/g, "");
          const city = record[10]?.trim().replace(/^["']|["']$/g, "");
          const addressDetail = record[11]?.trim().replace(/^["']|["']$/g, "");
          const postalCode = record[15]?.trim().replace(/^["']|["']$/g, "");

          if (!corporateNumber || !name) continue;

          const address = [prefecture, city, addressDetail].filter(Boolean).join("");

          const normalizedName = normalizeCompanyName(name);
          const normalizedAddress = normalizeAddress(address);
          const normalizedPostalCode = normalizePostalCode(postalCode);

          if (normalizedName) {
            if (!nameOnlyMap.has(normalizedName)) {
              nameOnlyMap.set(normalizedName, new Set());
            }
            const nameSet = nameOnlyMap.get(normalizedName)!;
            if (nameSet.size < 2) {
              nameSet.add(corporateNumber);
            }

            if (normalizedAddress) {
              const key = `${normalizedName}|${normalizedAddress}`;
              if (!nameAddressMap.has(key)) {
                nameAddressMap.set(key, new Set());
              }
              const addrSet = nameAddressMap.get(key)!;
              if (addrSet.size < 2) {
                addrSet.add(corporateNumber);
              }
            }

            if (normalizedPostalCode) {
              const key = `${normalizedName}|${normalizedPostalCode}`;
              if (!namePostalMap.has(key)) {
                namePostalMap.set(key, new Set());
              }
              const postalSet = namePostalMap.get(key)!;
              if (postalSet.size < 2) {
                postalSet.add(corporateNumber);
              }
            }
          }

          totalRecords++;

          if (totalRecords % 50000 === 0) {
            log(`  📊 処理中: ${totalRecords.toLocaleString()} 行、索引サイズ: 社名のみ=${nameOnlyMap.size.toLocaleString()}, 社名+住所=${nameAddressMap.size.toLocaleString()}, 社名+郵便番号=${namePostalMap.size.toLocaleString()}`);
          }
        } catch (error: any) {
          continue;
        }
      }
    });

    parser.on("end", () => {
      log(`  ✅ 読み込み完了: ${totalRecords.toLocaleString()} 行`);
      log(`  📊 索引サイズ: 社名のみ=${nameOnlyMap.size.toLocaleString()}, 社名+住所=${nameAddressMap.size.toLocaleString()}, 社名+郵便番号=${namePostalMap.size.toLocaleString()}`);
      resolve({ nameAddressMap, namePostalMap, nameOnlyMap, totalRecords });
    });

    parser.on("error", (error) => {
      reject(error);
    });

    readStream.on("error", (error) => {
      reject(error);
    });

    readStream.pipe(parser);
  });
}

// ==============================
// ドキュメントリスト読み込みと更新
// ==============================

interface CompanyInfo {
  docId: string;
  name: string | null;
  address: string | null;
  postalCode: string | null;
  corporateNumber: string | null;
  hasCorporateNumberField: boolean;
  corporateNumberStatus: "null" | "missing" | "empty";
  createdAt: string | null;
  updatedAt: string | null;
}

interface UpdateResult {
  docId: string;
  name: string | null;
  address: string | null;
  postalCode: string | null;
  corporateNumber: string | null;
  matchType: "unique" | "multiple" | "none";
  candidates: string[];
}

/**
 * ドキュメントリストを読み込む
 */
function loadCompanyList(filePath: string): CompanyInfo[] {
  log(`📋 ドキュメントリストを読み込み中: ${filePath}`);
  
  if (!fs.existsSync(filePath)) {
    throw new Error(`ファイルが見つかりません: ${filePath}`);
  }

  const content = fs.readFileSync(filePath, "utf8");
  const data = JSON.parse(content);
  
  if (!Array.isArray(data)) {
    throw new Error("JSONファイルは配列形式である必要があります");
  }

  log(`  ✅ 読み込み完了: ${data.length.toLocaleString()} 件`);
  return data;
}

/**
 * 各ドキュメントに対して法人番号を検索
 */
function findCorporateNumber(
  company: CompanyInfo,
  nameAddressMap: Map<string, Set<string>>,
  namePostalMap: Map<string, Set<string>>,
  nameOnlyMap: Map<string, Set<string>>
): UpdateResult {
  const name = company.name || "";
  const address = company.address || "";
  const postalCode = company.postalCode || "";

  const normalizedName = normalizeCompanyName(name);
  const normalizedAddress = normalizeAddress(address);
  const normalizedPostalCode = normalizePostalCode(postalCode);

  if (!normalizedName) {
    return {
      docId: company.docId,
      name,
      address,
      postalCode,
      corporateNumber: null,
      matchType: "none",
      candidates: [],
    };
  }

  let candidates: string[] = [];
  let matchType: "unique" | "multiple" | "none" = "none";

  // 1. 社名+郵便番号で検索
  if (normalizedPostalCode) {
    const key = `${normalizedName}|${normalizedPostalCode}`;
    const postalCandidates = namePostalMap.get(key);
    if (postalCandidates && postalCandidates.size > 0) {
      candidates = Array.from(postalCandidates);
      matchType = postalCandidates.size === 1 ? "unique" : "multiple";
    }
  }

  // 2. 社名+住所で検索（郵便番号で見つからなかった場合）
  if (candidates.length === 0 && normalizedAddress) {
    const key = `${normalizedName}|${normalizedAddress}`;
    const addressCandidates = nameAddressMap.get(key);
    if (addressCandidates && addressCandidates.size > 0) {
      candidates = Array.from(addressCandidates);
      matchType = addressCandidates.size === 1 ? "unique" : "multiple";
    }
  }

  // 3. 社名のみで検索（フォールバック、ユニークな場合のみ）
  if (candidates.length === 0 && normalizedName) {
    const nameCandidates = nameOnlyMap.get(normalizedName);
    if (nameCandidates && nameCandidates.size === 1) {
      candidates = Array.from(nameCandidates);
      matchType = "unique";
    }
  }

  return {
    docId: company.docId,
    name,
    address,
    postalCode,
    corporateNumber: candidates.length === 1 ? candidates[0] : null,
    matchType,
    candidates,
  };
}

/**
 * Firestoreにバッチ更新を実行
 */
async function updateFirestore(
  uniqueMatches: UpdateResult[],
  dryRun: boolean
): Promise<number> {
  if (dryRun) {
    log(`🔍 DRY_RUN: ${uniqueMatches.length.toLocaleString()} 件の更新予定`);
    return 0;
  }

  log(`📝 Firestoreに更新中: ${uniqueMatches.length.toLocaleString()} 件`);

  let updatedCount = 0;
  let batch: WriteBatch | null = null;
  let batchCount = 0;

  for (const match of uniqueMatches) {
    if (!batch) {
      batch = db.batch();
      batchCount = 0;
    }

    const docRef = companiesCol.doc(match.docId);
    const updateData: any = {
      corporateNumber: match.corporateNumber,
      corporateNumberSource: "pref_00_zenkoku_all_20251226",
      corporateNumberUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    };

    // フィールドが存在しない場合はset、存在する場合はupdate
    if (match.matchType === "unique" && match.corporateNumber) {
      batch.update(docRef, updateData);
      batchCount++;
      updatedCount++;

      if (batchCount >= BATCH_SIZE) {
        await batch.commit();
        log(`  📊 更新中: ${updatedCount.toLocaleString()} / ${uniqueMatches.length.toLocaleString()} 件`);
        batch = null;
        batchCount = 0;
      }
    }
  }

  if (batch && batchCount > 0) {
    await batch.commit();
  }

  log(`  ✅ 更新完了: ${updatedCount.toLocaleString()} 件`);
  return updatedCount;
}

/**
 * 結果をCSV出力
 */
async function writeResultsCsv(
  uniqueMatches: UpdateResult[],
  multipleMatches: UpdateResult[],
  noMatches: UpdateResult[],
  outputDir: string
): Promise<void> {
  const outputPath = path.join(outputDir, "corporate_number_update_results.csv");
  log(`📄 結果CSVを出力中: ${outputPath}`);

  const writer = csv.createObjectCsvWriter({
    path: outputPath,
    header: [
      { id: "docId", title: "docId" },
      { id: "name", title: "name" },
      { id: "address", title: "address" },
      { id: "postalCode", title: "postalCode" },
      { id: "corporateNumber", title: "corporateNumber" },
      { id: "candidates", title: "candidates" },
      { id: "matchType", title: "matchType" },
    ],
    encoding: "utf8",
  });

  const records = [
    ...uniqueMatches.map(m => ({
      docId: m.docId,
      name: m.name || "",
      address: m.address || "",
      postalCode: m.postalCode || "",
      corporateNumber: m.corporateNumber || "",
      candidates: m.candidates.join("|"),
      matchType: "unique",
    })),
    ...multipleMatches.map(m => ({
      docId: m.docId,
      name: m.name || "",
      address: m.address || "",
      postalCode: m.postalCode || "",
      corporateNumber: "",
      candidates: m.candidates.join("|"),
      matchType: "multiple",
    })),
    ...noMatches.map(m => ({
      docId: m.docId,
      name: m.name || "",
      address: m.address || "",
      postalCode: m.postalCode || "",
      corporateNumber: "",
      candidates: "",
      matchType: "none",
    })),
  ];

  await writer.writeRecords(records);
  log(`  ✅ 出力完了: ${records.length.toLocaleString()} 件`);
}

// ==============================
// メイン処理
// ==============================

async function main() {
  initAdmin();

  const zipPath = process.env.PREF_ZIP_PATH || "pref/00_zenkoku_all_20251226.zip";
  const inputList = process.env.INPUT_LIST || "null_or_missing_corporate_numbers.json";
  const dryRun = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";
  const csvLimit = process.env.CSV_LIMIT ? parseInt(process.env.CSV_LIMIT) : undefined;

  log("============================================================");
  log("法人番号補完バッチ処理（リスト指定）");
  log("============================================================");
  log(`ZIPパス: ${zipPath}`);
  log(`入力リスト: ${inputList}`);
  log(`DRY_RUN: ${dryRun}`);
  log(`CSV_LIMIT: ${csvLimit || "なし（全件読み込み）"}`);
  log("");
  log("📌 目的: 洗い出したドキュメントに法人番号を追加");
  log("");

  // 出力ディレクトリを作成
  const outputDir = path.join(__dirname, "../out");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  // 1. ドキュメントリストを読み込む
  const companyList = loadCompanyList(inputList);

  // 2. ZIP展開
  const csvPath = extractZip(zipPath);

  // 3. CSV読み込みと索引構築
  const { nameAddressMap, namePostalMap, nameOnlyMap, totalRecords } = await buildIndexFromPrefCsv(csvPath, csvLimit);

  // 4. 各ドキュメントに対して法人番号を検索
  log("🔍 各ドキュメントに対して法人番号を検索中...");
  
  const uniqueMatches: UpdateResult[] = [];
  const multipleMatches: UpdateResult[] = [];
  const noMatches: UpdateResult[] = [];

  for (let i = 0; i < companyList.length; i++) {
    const company = companyList[i];
    const result = findCorporateNumber(company, nameAddressMap, namePostalMap, nameOnlyMap);

    if (result.matchType === "unique") {
      uniqueMatches.push(result);
    } else if (result.matchType === "multiple") {
      multipleMatches.push(result);
    } else {
      noMatches.push(result);
    }

    if ((i + 1) % 1000 === 0) {
      log(`  📊 処理中: ${(i + 1).toLocaleString()} / ${companyList.length.toLocaleString()} 件（ユニーク: ${uniqueMatches.length}, 複数: ${multipleMatches.length}, なし: ${noMatches.length}）`);
    }
  }

  // 5. 結果出力
  log("\n============================================================");
  log("📊 突合結果");
  log("============================================================");
  log(`総処理数: ${companyList.length.toLocaleString()} 件`);
  log(`ユニーク一致: ${uniqueMatches.length.toLocaleString()} 件 (${((uniqueMatches.length / companyList.length) * 100).toFixed(2)}%)`);
  log(`複数候補: ${multipleMatches.length.toLocaleString()} 件 (${((multipleMatches.length / companyList.length) * 100).toFixed(2)}%)`);
  log(`候補なし: ${noMatches.length.toLocaleString()} 件 (${((noMatches.length / companyList.length) * 100).toFixed(2)}%)`);

  // 6. CSV出力
  await writeResultsCsv(uniqueMatches, multipleMatches, noMatches, outputDir);

  // 7. Firestore更新
  if (!dryRun && uniqueMatches.length > 0) {
    const updatedCount = await updateFirestore(uniqueMatches, dryRun);
    log(`\n✅ 更新完了: ${updatedCount.toLocaleString()} 件`);
  } else if (dryRun) {
    log(`\n🔍 DRY_RUN: ${uniqueMatches.length.toLocaleString()} 件の更新予定（実際の更新は行いません）`);
  }

  log("\n✅ 処理完了");
}

// 実行
main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});
