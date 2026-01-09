/* 
  法人番号公表サイトの全件データを使って、corporateNumber == null のドキュメントに法人番号を補完するバッチ
  
  使い方:
    # DRY_RUN（試行）
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    PREF_ZIP_PATH=pref/00_zenkoku_all_20251226.zip \
    DRY_RUN=1 \
    LIMIT=1000 \
    npx tsx scripts/backfill_corporate_number_from_pref.ts
    
    # 本番実行
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    PREF_ZIP_PATH=pref/00_zenkoku_all_20251226.zip \
    DRY_RUN=0 \
    npx tsx scripts/backfill_corporate_number_from_pref.ts
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference, DocumentReference, WriteBatch, Timestamp } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse";
import { createReadStream } from "fs";
import { execSync } from "child_process";
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

// ==============================
// ログ関数
// ==============================

function log(message: string) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
}

// ==============================
// 正規化関数（既存のものを拡張）
// ==============================

/**
 * 会社名を正規化（強化版）
 * - 法人格表記の統一
 * - 空白除去（全角/半角）
 * - 全角半角統一
 * - 括弧類の除去（一部）
 * - 記号ゆれの統一
 */
function normalizeCompanyName(name: string | null | undefined): string | null {
  if (!name || name.trim() === "") return null;

  let normalized = name.trim();

  // 法人格表記の統一
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

  // 括弧類の除去（一部の括弧は残す）
  normalized = normalized.replace(/[（）()【】「」『』［］]/g, "");

  // 空白除去（全角/半角）
  normalized = normalized.replace(/[\s\u3000]+/g, "");

  // 全角英数字・記号を半角に
  normalized = normalized.replace(/[Ａ-Ｚａ-ｚ０-９]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) - 0xFEE0);
  });

  // ハイフン類の統一（全角ハイフン、長音符などを半角ハイフンに）
  normalized = normalized.replace(/[－―ー−‐‑]/g, "-");

  // 全角カナを統一（全角カナに）
  normalized = normalized.replace(/[ァ-ヶ]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) + 0x60);
  });

  return normalized;
}

/**
 * 住所を正規化（強化版）
 * - 都道府県表記の統一
 * - 市区町村まで抽出（丁目/番地/建物名を除去）
 * - 空白除去（全角/半角）
 * - 記号ゆれの統一
 */
function normalizeAddress(address: string | null | undefined): string | null {
  if (!address || address.trim() === "") return null;

  let normalized = address.trim();

  // 都道府県表記の統一
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

  // 市区町村+町域名まで抽出（丁目/番地/建物名は除去、ただし町域名は含める）
  // 都道府県 + 市区町村 + 町域名のパターンにマッチ
  // 例: "東京都千代田区岩本町１－３－９" → "東京都千代田区岩本町"
  const cityMatch = normalized.match(/^(.+?[都道府県])(.+?[市区町村])(.+?[町丁目])/);
  if (cityMatch) {
    normalized = cityMatch[1] + cityMatch[2] + cityMatch[3];
  } else {
    // 市区町村までしかない場合
    const cityOnlyMatch = normalized.match(/^(.+?[都道府県])(.+?[市区町村])/);
    if (cityOnlyMatch) {
      normalized = cityOnlyMatch[1] + cityOnlyMatch[2];
    }
  }

  // 空白除去（全角/半角）
  normalized = normalized.replace(/[\s\u3000]+/g, "");

  // ハイフン類の統一
  normalized = normalized.replace(/[－―ー−‐‑]/g, "-");

  return normalized;
}

/**
 * 郵便番号を正規化（ハイフン除去）
 */
function normalizePostalCode(postalCode: string | null | undefined): string | null {
  if (!postalCode || postalCode.trim() === "") return null;
  return postalCode.trim().replace(/[-ー−‐‑]/g, "");
}

// ==============================
// ZIP展開とCSV読み込み
// ==============================

interface PrefRecord {
  corporateNumber: string;
  name: string;
  prefecture: string;
  city: string;
  address: string;
  postalCode: string | null;
}

/**
 * ZIPファイルを展開してCSVファイルのパスを返す
 */
function extractZip(zipPath: string): string {
  log(`📦 ZIPファイルを展開中: ${zipPath}`);
  
  const extractDir = path.join(path.dirname(zipPath), "extracted");
  if (!fs.existsSync(extractDir)) {
    fs.mkdirSync(extractDir, { recursive: true });
  }

  // unzipコマンドを使用（macOS/Linux）
  const zipFileName = path.basename(zipPath, ".zip");
  const extractedCsvPath = path.join(extractDir, `${zipFileName}.csv`);

  if (fs.existsSync(extractedCsvPath)) {
    log(`  ✅ 既に展開済み: ${extractedCsvPath}`);
    return extractedCsvPath;
  }

  try {
    execSync(`unzip -o "${zipPath}" -d "${extractDir}"`, { stdio: "inherit" });
    
    // 展開されたファイルを探す
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

/**
 * CSVファイルの文字コードを判定
 */
function detectEncoding(buffer: Buffer): "utf8" | "utf16le" | "shift_jis" {
  // BOMチェック
  if (buffer.length >= 2 && buffer[0] === 0xFF && buffer[1] === 0xFE) {
    return "utf16le";
  }
  if (buffer.length >= 3 && buffer[0] === 0xEF && buffer[1] === 0xBB && buffer[2] === 0xBF) {
    return "utf8";
  }

  // 試行錯誤で判定（簡易版）
  try {
    const utf8Text = buffer.toString("utf8");
    // UTF-8として読めるかチェック
    if (utf8Text.includes("法人番号") || utf8Text.includes("商号")) {
      return "utf8";
    }
  } catch {}

  // デフォルトはUTF-8
  return "utf8";
}

/**
 * CSVファイルを読み込んで索引を構築
 * 国税庁の法人番号公表サイトのCSVフォーマットに対応
 */
async function buildIndexFromPrefCsv(csvPath: string, limit?: number): Promise<{
  nameAddressMap: Map<string, Set<string>>; // key: normalizedName + normalizedAddress, value: Set of corporateNumbers
  namePostalMap: Map<string, Set<string>>; // key: normalizedName + normalizedPostalCode, value: Set of corporateNumbers
  nameOnlyMap: Map<string, Set<string>>; // key: normalizedName only, value: Set of corporateNumbers (fallback)
  totalRecords: number;
}> {
  log(`📖 CSVファイルを読み込み中: ${csvPath}`);

  const nameAddressMap = new Map<string, Set<string>>();
  const namePostalMap = new Map<string, Set<string>>();
  const nameOnlyMap = new Map<string, Set<string>>(); // 社名のみの索引（フォールバック用）
  
  let totalRecords = 0;
  let processedRecords = 0;

  // ファイルの先頭を読んで文字コードを判定
  const sampleBuffer = fs.readFileSync(csvPath, null, { start: 0, end: 10000 });
  const encoding = detectEncoding(sampleBuffer);
  log(`  📝 文字コード: ${encoding}`);

  // ストリーミング処理でCSVを読み込む（csv-parseのストリーミング機能を使用）
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
        // CSV_LIMITはtotalRecordsでチェック（processedRecordsではない）
        if (limit && totalRecords >= limit) {
          readStream.destroy();
          parser.destroy();
          resolve({ nameAddressMap, namePostalMap, nameOnlyMap, totalRecords });
          return;
        }

        if (record.length < 16) continue;

        try {
          // 国税庁のCSVフォーマット（列の位置）
          // 0: 連番, 1: 法人番号, 2-5: その他, 6: 商号又は名称, 7: 空, 8: 都道府県コード, 9: 都道府県, 10: 市区町村, 11: 町域名, 12: 空, 15: 郵便番号
          const corporateNumber = record[1]?.trim().replace(/^["']|["']$/g, "");
          const name = record[6]?.trim().replace(/^["']|["']$/g, ""); // 商号又は名称
          const prefecture = record[9]?.trim().replace(/^["']|["']$/g, ""); // 都道府県
          const city = record[10]?.trim().replace(/^["']|["']$/g, ""); // 市区町村
          const addressDetail = record[11]?.trim().replace(/^["']|["']$/g, ""); // 町域名
          const postalCode = record[15]?.trim().replace(/^["']|["']$/g, ""); // 郵便番号

          if (!corporateNumber || !name) continue;

          // 住所を構築
          const address = [prefecture, city, addressDetail].filter(Boolean).join("");

          // 正規化
          const normalizedName = normalizeCompanyName(name);
          const normalizedAddress = normalizeAddress(address);
          const normalizedPostalCode = normalizePostalCode(postalCode);

          if (normalizedName) {
            // 社名のみの索引（フォールバック用、ユニークな場合のみ保存）
            // メモリ節約のため、複数候補がある場合は最初の1件のみ保存
            if (!nameOnlyMap.has(normalizedName)) {
              nameOnlyMap.set(normalizedName, new Set());
            }
            const nameSet = nameOnlyMap.get(normalizedName)!;
            // 既に2件以上ある場合は追加しない（メモリ節約）
            if (nameSet.size < 2) {
              nameSet.add(corporateNumber);
            }

            // 社名+住所の索引（メモリ節約のため、最大2件まで）
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

            // 社名+郵便番号の索引（メモリ節約のため、最大2件まで）
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

          processedRecords++;
          totalRecords++;

          if (totalRecords % 50000 === 0 || totalRecords <= 10) {
            // メモリ使用量を計算（概算）
            const estimatedMemoryMB = (
              (nameOnlyMap.size * 100 + 
               nameAddressMap.size * 150 + 
               namePostalMap.size * 150) / 1024 / 1024
            ).toFixed(1);
            log(`  📊 処理中: ${totalRecords.toLocaleString()} 行、索引サイズ: 社名のみ=${nameOnlyMap.size.toLocaleString()}, 社名+住所=${nameAddressMap.size.toLocaleString()}, 社名+郵便番号=${namePostalMap.size.toLocaleString()} (推定メモリ: ${estimatedMemoryMB}MB)`);
            if (totalRecords <= 10) {
              // デバッグ: 最初の数件のデータを表示
              log(`    [デバッグ] サンプル: 法人番号=${corporateNumber}, 社名=${name}, 正規化社名=${normalizedName}, 住所=${address}, 正規化住所=${normalizedAddress}, 郵便番号=${postalCode}`);
            }
          }
        } catch (error: any) {
          // パースエラーはスキップ
          continue;
        }
      }
    });

    parser.on("end", () => {
      log(`  ✅ 読み込み完了: ${totalRecords.toLocaleString()} 行`);
      log(`  📊 索引サイズ: 社名のみ=${nameOnlyMap.size.toLocaleString()}, 社名+住所=${nameAddressMap.size.toLocaleString()}, 社名+郵便番号=${namePostalMap.size.toLocaleString()}`);
      
      // デバッグ: 索引のサンプルを表示
      if (nameOnlyMap.size > 0) {
        const sampleKey = Array.from(nameOnlyMap.keys()).find(k => nameOnlyMap.get(k)!.size === 1);
        if (sampleKey) {
          const sampleValues = Array.from(nameOnlyMap.get(sampleKey)!);
          log(`  [デバッグ] 索引サンプル（社名のみ、ユニーク）: キー="${sampleKey}", 値=${sampleValues.join(", ")}`);
        }
      }
      
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
// Firestoreからnullドキュメント取得と突合
// ==============================

interface MatchResult {
  docId: string;
  name: string;
  address: string | null;
  postalCode: string | null;
  candidates: string[]; // 法人番号の候補
  matchType: "unique" | "multiple" | "none";
}

/**
 * 保存済みのドキュメントIDリストを読み込む
 */
function loadDocIdList(filePath: string | undefined): string[] | null {
  if (!filePath || !fs.existsSync(filePath)) {
    return null;
  }

  try {
    const content = fs.readFileSync(filePath, "utf8");
    const data = JSON.parse(content);
    
    // 配列の場合
    if (Array.isArray(data)) {
      return data.map((item: any) => {
        if (typeof item === "string") return item;
        if (item && typeof item === "object" && "docId" in item) {
          return item.docId;
        }
        return null;
      }).filter((id: string | null): id is string => id !== null);
    }
    
    return null;
  } catch (error: any) {
    log(`  ⚠️  ドキュメントIDリストの読み込みエラー: ${error.message}`);
    return null;
  }
}

/**
 * FirestoreからcorporateNumber==nullのドキュメントを取得して突合
 * docIdListが指定されている場合、そのリストのドキュメントのみを処理
 */
async function matchNullDocuments(
  nameAddressMap: Map<string, Set<string>>,
  namePostalMap: Map<string, Set<string>>,
  nameOnlyMap: Map<string, Set<string>>,
  options: { limit?: number; dryRun: boolean; docIdList?: string[] }
): Promise<{
  uniqueMatches: MatchResult[];
  multipleMatches: MatchResult[];
  noMatches: MatchResult[];
  totalProcessed: number;
}> {
  log("🔍 Firestoreからnullドキュメントを取得して突合中...");

  const uniqueMatches: MatchResult[] = [];
  const multipleMatches: MatchResult[] = [];
  const noMatches: MatchResult[] = [];
  let nullCount = 0;
  let totalProcessed = 0;

  // docIdListが指定されている場合、そのリストのドキュメントのみを処理
  if (options.docIdList && options.docIdList.length > 0) {
    log(`  📋 指定されたドキュメントIDリストから処理: ${options.docIdList.length.toLocaleString()} 件`);
    
    const PAGE_SIZE = 100; // リスト指定時は小さめのバッチサイズ
    
    for (let i = 0; i < options.docIdList.length; i += PAGE_SIZE) {
      if (options.limit && nullCount >= options.limit) break;
      
      const batchIds = options.docIdList.slice(i, i + PAGE_SIZE);
      const docRefs = batchIds.map(id => companiesCol.doc(id));
      const docs = await Promise.all(docRefs.map(ref => ref.get()));
      
      for (const docSnap of docs) {
        if (!docSnap.exists) {
          totalProcessed++;
          if (nullCount < 5) {
            log(`  ⚠️  ドキュメントが存在しません: ${docSnap.id}`);
          }
          continue;
        }
        
        const doc = docSnap;
        const data = doc.data();
        const corporateNumber = data.corporateNumber;

        // 法人番号がnull/undefined/空のもののみ
        const isNull = corporateNumber === null || 
            corporateNumber === undefined || 
            corporateNumber === "" ||
            !("corporateNumber" in data);

        if (!isNull) {
          totalProcessed++;
          if (nullCount < 5) {
            log(`  ⚠️  法人番号が既に設定されています: ${doc.id} = ${corporateNumber}`);
          }
          continue;
        }
        
        // 以下、通常の処理と同じ
        await processDocument(doc, data, nameAddressMap, namePostalMap, nameOnlyMap, uniqueMatches, multipleMatches, noMatches, nullCount);
        nullCount++;
        totalProcessed++;
        
        if (nullCount % 100 === 0) {
          log(`  📊 処理中: ${nullCount.toLocaleString()} / ${options.docIdList.length.toLocaleString()} 件`);
        }
        
        if (options.limit && nullCount >= options.limit) break;
      }
    }
    
    log(`  ✅ 突合完了: ${nullCount.toLocaleString()} 件`);
    log(`    - ユニーク一致: ${uniqueMatches.length.toLocaleString()} 件`);
    log(`    - 複数候補: ${multipleMatches.length.toLocaleString()} 件`);
    log(`    - 候補なし: ${noMatches.length.toLocaleString()} 件`);

    return {
      uniqueMatches,
      multipleMatches,
      noMatches,
      totalProcessed: nullCount,
    };
  }

  // 通常の処理: Firestoreから全件取得
  const PAGE_SIZE = 1000;
  let lastDoc: any = null;

  while (true) {
    if (options.limit && nullCount >= options.limit) break;

    let batchQuery = companiesCol.orderBy("__name__").limit(PAGE_SIZE);
    if (lastDoc) {
      batchQuery = batchQuery.startAfter(lastDoc);
    }

    const batchSnapshot = await batchQuery.get();
    if (batchSnapshot.empty) break;

    for (const doc of batchSnapshot.docs) {
      const data = doc.data();
      const corporateNumber = data.corporateNumber;

      // 法人番号がnull/undefined/空のもののみ
      const isNull = corporateNumber === null || 
          corporateNumber === undefined || 
          corporateNumber === "" ||
          !("corporateNumber" in data);

      if (!isNull) continue;
      
      await processDocument(doc, data, nameAddressMap, namePostalMap, nameOnlyMap, uniqueMatches, multipleMatches, noMatches, nullCount);
      nullCount++;
      totalProcessed++;

      if (nullCount % 1000 === 0) {
        log(`  📊 処理中: ${nullCount.toLocaleString()} 件（ユニーク: ${uniqueMatches.length}, 複数: ${multipleMatches.length}, なし: ${noMatches.length}）`);
      }

      if (options.limit && nullCount >= options.limit) break;
    }

    if (options.limit && nullCount >= options.limit) break;

    lastDoc = batchSnapshot.docs[batchSnapshot.docs.length - 1];
    if (batchSnapshot.size < PAGE_SIZE) break;
  }

  log(`  ✅ 突合完了: ${nullCount.toLocaleString()} 件`);
  log(`    - ユニーク一致: ${uniqueMatches.length.toLocaleString()} 件`);
  log(`    - 複数候補: ${multipleMatches.length.toLocaleString()} 件`);
  log(`    - 候補なし: ${noMatches.length.toLocaleString()} 件`);

  return {
    uniqueMatches,
    multipleMatches,
    noMatches,
    totalProcessed: nullCount,
  };
}

/**
 * ドキュメントを処理して突合する（共通処理）
 */
async function processDocument(
  doc: any,
  data: any,
  nameAddressMap: Map<string, Set<string>>,
  namePostalMap: Map<string, Set<string>>,
  nameOnlyMap: Map<string, Set<string>>,
  uniqueMatches: MatchResult[],
  multipleMatches: MatchResult[],
  noMatches: MatchResult[],
  nullCount: number
): Promise<void> {
  const name = data.name || "";
  const address = data.address || data.headquartersAddress || null;
  const postalCode = data.postalCode || null;

  // 正規化
  const normalizedName = normalizeCompanyName(name);
  const normalizedAddress = normalizeAddress(address);
  const normalizedPostalCode = normalizePostalCode(postalCode);

  if (!normalizedName) {
    noMatches.push({
      docId: doc.id,
      name,
      address,
      postalCode,
      candidates: [],
      matchType: "none",
    });
    return;
  }

  // 突合（優先順位: 社名+郵便番号 > 社名+住所 > 社名のみ）
  let candidates: string[] = [];
  let matchType: "unique" | "multiple" | "none" = "none";

  // デバッグ: 最初の数件の突合過程を表示
  const isDebug = nullCount <= 5;

      // 1. 社名+郵便番号で検索
      if (normalizedPostalCode) {
        const key = `${normalizedName}|${normalizedPostalCode}`;
        if (isDebug) {
          log(`    [デバッグ] 検索キー（社名+郵便番号）: "${key}"`);
        }
        const postalCandidates = namePostalMap.get(key);
        if (postalCandidates && postalCandidates.size > 0) {
          candidates = Array.from(postalCandidates);
          matchType = postalCandidates.size === 1 ? "unique" : "multiple";
          if (isDebug) {
            log(`    [デバッグ] マッチ（社名+郵便番号）: ${candidates.length}件`);
          }
        } else if (isDebug) {
          log(`    [デバッグ] マッチなし（社名+郵便番号）`);
        }
      } else if (isDebug) {
        log(`    [デバッグ] 郵便番号なし: "${postalCode}"`);
      }

      // 2. 社名+住所で検索（郵便番号で見つからなかった場合）
      if (candidates.length === 0 && normalizedAddress) {
        const key = `${normalizedName}|${normalizedAddress}`;
        if (isDebug) {
          log(`    [デバッグ] 検索キー（社名+住所）: "${key}"`);
        }
        const addressCandidates = nameAddressMap.get(key);
        if (addressCandidates && addressCandidates.size > 0) {
          candidates = Array.from(addressCandidates);
          matchType = addressCandidates.size === 1 ? "unique" : "multiple";
          if (isDebug) {
            log(`    [デバッグ] マッチ（社名+住所）: ${candidates.length}件`);
          }
        } else if (isDebug) {
          log(`    [デバッグ] マッチなし（社名+住所）`);
          log(`    [デバッグ] 元の住所: "${address}", 正規化住所: "${normalizedAddress}"`);
        }
      } else if (isDebug && candidates.length === 0) {
        log(`    [デバッグ] 住所なし: "${address}"`);
      }

      // 3. 社名のみで検索（フォールバック、ユニークな場合のみ）
      if (candidates.length === 0 && normalizedName) {
        const nameCandidates = nameOnlyMap.get(normalizedName);
        if (nameCandidates && nameCandidates.size === 1) {
          // ユニークな場合のみ採用（複数候補は除外）
          candidates = Array.from(nameCandidates);
          matchType = "unique";
          if (isDebug) {
            log(`    [デバッグ] マッチ（社名のみ、ユニーク）: ${candidates.length}件`);
          }
        } else if (isDebug && nameCandidates && nameCandidates.size > 1) {
          log(`    [デバッグ] 社名のみで複数候補: ${nameCandidates.size}件`);
        }
      }
      
  if (isDebug) {
    log(`    [デバッグ] 元の社名: "${name}", 正規化社名: "${normalizedName}"`);
  }

  const result: MatchResult = {
    docId: doc.id,
    name,
    address,
    postalCode,
    candidates,
    matchType,
  };

  if (matchType === "unique") {
    uniqueMatches.push(result);
  } else if (matchType === "multiple") {
    multipleMatches.push(result);
  } else {
    noMatches.push(result);
  }
}

// ==============================
// バッチ更新
// ==============================

/**
 * Firestoreにバッチ更新を実行
 */
async function updateFirestore(
  uniqueMatches: MatchResult[],
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
    const corporateNumber = match.candidates[0];

    batch.update(docRef, {
      corporateNumber,
      corporateNumberSource: "pref_00_zenkoku_all_20251226",
      corporateNumberUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
    });

    batchCount++;
    updatedCount++;

    // バッチサイズに達したらコミット
    if (batchCount >= BATCH_SIZE) {
      await batch.commit();
      log(`  📊 更新中: ${updatedCount.toLocaleString()} / ${uniqueMatches.length.toLocaleString()} 件`);
      batch = null;
      batchCount = 0;
    }
  }

  // 残りをコミット
  if (batch && batchCount > 0) {
    await batch.commit();
  }

  log(`  ✅ 更新完了: ${updatedCount.toLocaleString()} 件`);
  return updatedCount;
}

// ==============================
// CSV出力
// ==============================

/**
 * 候補複数・候補なしのCSVを出力
 */
async function writeCandidatesCsv(
  multipleMatches: MatchResult[],
  noMatches: MatchResult[],
  outputDir: string
): Promise<void> {
  const outputPath = path.join(outputDir, "corporate_number_candidates.csv");
  log(`📄 候補CSVを出力中: ${outputPath}`);

  const writer = csv.createObjectCsvWriter({
    path: outputPath,
    header: [
      { id: "docId", title: "docId" },
      { id: "name", title: "name" },
      { id: "address", title: "address" },
      { id: "postalCode", title: "postalCode" },
      { id: "candidates", title: "candidates" },
      { id: "matchType", title: "matchType" },
    ],
    encoding: "utf8",
  });

  const records = [
    ...multipleMatches.map(m => ({
      docId: m.docId,
      name: m.name,
      address: m.address || "",
      postalCode: m.postalCode || "",
      candidates: m.candidates.join("|"),
      matchType: "multiple",
    })),
    ...noMatches.map(m => ({
      docId: m.docId,
      name: m.name,
      address: m.address || "",
      postalCode: m.postalCode || "",
      candidates: "",
      matchType: "none",
    })),
  ];

  await writer.writeRecords(records);
  log(`  ✅ 出力完了: ${records.length.toLocaleString()} 件`);
}

/**
 * 更新予定のCSVを出力（DRY_RUN時）
 */
async function writeUpdatePlanCsv(
  uniqueMatches: MatchResult[],
  outputDir: string
): Promise<void> {
  const outputPath = path.join(outputDir, "corporate_number_update_plan.csv");
  log(`📄 更新予定CSVを出力中: ${outputPath}`);

  const writer = csv.createObjectCsvWriter({
    path: outputPath,
    header: [
      { id: "docId", title: "docId" },
      { id: "name", title: "name" },
      { id: "address", title: "address" },
      { id: "postalCode", title: "postalCode" },
      { id: "corporateNumber", title: "corporateNumber" },
    ],
    encoding: "utf8",
  });

  const records = uniqueMatches.map(m => ({
    docId: m.docId,
    name: m.name,
    address: m.address || "",
    postalCode: m.postalCode || "",
    corporateNumber: m.candidates[0],
  }));

  await writer.writeRecords(records);
  log(`  ✅ 出力完了: ${records.length.toLocaleString()} 件`);
}

// ==============================
// メイン処理
// ==============================

async function main() {
  initAdmin();

  const zipPath = process.env.PREF_ZIP_PATH || "pref/00_zenkoku_all_20251226.zip";
  const dryRun = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";
  const limit = process.env.LIMIT ? parseInt(process.env.LIMIT) : undefined;
  // CSV_LIMITは指定されない限り全件読み込む（デフォルト: undefined = 全件）
  const csvLimit = process.env.CSV_LIMIT ? parseInt(process.env.CSV_LIMIT) : undefined;
  // 保存済みのドキュメントIDリストを使用する場合
  const docIdListPath = process.env.DOC_ID_LIST || null;

  log("============================================================");
  log("法人番号補完バッチ処理（国税庁データ使用）");
  log("============================================================");
  log(`ZIPパス: ${zipPath}`);
  log(`DRY_RUN: ${dryRun}`);
  log(`LIMIT: ${limit || "なし（全件処理）"}`);
  log(`CSV_LIMIT: ${csvLimit || "なし（全件読み込み）"}`);
  log(`DOC_ID_LIST: ${docIdListPath || "なし（Firestoreから全件取得）"}`);
  log("");
  log("📌 目的: corporateNumber == null のドキュメントに法人番号を補完");
  log("");
  
  // ドキュメントIDリストを読み込む
  const docIdList = docIdListPath ? loadDocIdList(docIdListPath) : null;
  if (docIdList) {
    log(`📋 ドキュメントIDリストを読み込みました: ${docIdList.length.toLocaleString()} 件`);
  }

  // 出力ディレクトリを作成
  const outputDir = path.join(__dirname, "../out");
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }

  // 1. ZIP展開
  const csvPath = extractZip(zipPath);

  // 2. CSV読み込みと索引構築
  const { nameAddressMap, namePostalMap, nameOnlyMap, totalRecords } = await buildIndexFromPrefCsv(csvPath, csvLimit);

  // 3. Firestoreからnullドキュメント取得と突合
  const { uniqueMatches, multipleMatches, noMatches, totalProcessed } = await matchNullDocuments(
    nameAddressMap,
    namePostalMap,
    nameOnlyMap,
    { limit, dryRun, docIdList: docIdList || undefined }
  );

  // 4. 結果出力
  log("\n============================================================");
  log("📊 突合結果");
  log("============================================================");
  log(`総処理数: ${totalProcessed.toLocaleString()} 件`);
  log(`ユニーク一致: ${uniqueMatches.length.toLocaleString()} 件 (${((uniqueMatches.length / totalProcessed) * 100).toFixed(2)}%)`);
  log(`複数候補: ${multipleMatches.length.toLocaleString()} 件 (${((multipleMatches.length / totalProcessed) * 100).toFixed(2)}%)`);
  log(`候補なし: ${noMatches.length.toLocaleString()} 件 (${((noMatches.length / totalProcessed) * 100).toFixed(2)}%)`);

  // 5. CSV出力
  await writeCandidatesCsv(multipleMatches, noMatches, outputDir);
  if (dryRun) {
    await writeUpdatePlanCsv(uniqueMatches, outputDir);
  }

  // 6. Firestore更新
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
