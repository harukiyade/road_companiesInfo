import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse";
import { stringify } from "csv-stringify/sync";
import * as iconv from "iconv-lite";
import { createWriteStream, createReadStream } from "fs";

// ============================================================================
// 設定
// ============================================================================

const INPUT_DIR = path.join(__dirname, "../csv/gBizINFO");
const OUTPUT_DIR = path.join(__dirname, "../out/gBizINFO");
const OUTPUT_FILE = path.join(OUTPUT_DIR, "companies_export.csv");

const INPUT_FILES = {
  kihonjoho: path.join(INPUT_DIR, "Kihonjoho_UTF-8.csv"),
  zaimujoho: path.join(INPUT_DIR, "Zaimujoho_UTF-8.csv"),
  chotatsujoho: path.join(INPUT_DIR, "Chotatsujoho_UTF-8.csv"),
  shokubajoho: path.join(INPUT_DIR, "Shokubajoho_SJIS_20251227.csv"),
};

// ============================================================================
// 型定義
// ============================================================================

type CsvRecord = Record<string, string>;

interface CompanyData {
  corporateNumber: string;
  [key: string]: any;
}

interface ZaimuSummary {
  capitalStock?: string;
  revenue?: string;
  employeeCount?: string;
  fiscalMonth?: string;
  latestDate?: string;
}

interface ChotatsuSummary {
  procurementCount: number;
  procurementLatestDate?: string;
  procurementLatestAmount?: string;
}

interface ShokubaSummary {
  workplaceRowCount: number;
  workplaceLatestYear?: string;
}

// ============================================================================
// ユーティリティ関数
// ============================================================================

function log(message: string) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`);
}

function parseNumeric(value: string | undefined): number | null {
  if (!value || value.trim() === "") return null;
  const cleaned = value.replace(/[^\d.-]/g, "");
  const num = parseFloat(cleaned);
  return isNaN(num) ? null : num;
}

function parseDate(value: string | undefined): Date | null {
  if (!value || value.trim() === "") return null;
  try {
    const date = new Date(value);
    return isNaN(date.getTime()) ? null : date;
  } catch {
    return null;
  }
}

function findLatestDateColumn(record: CsvRecord): string | null {
  // 日付らしいカラム名を探す
  const dateColumnNames = [
    "事業年度",
    "決算日",
    "開示日",
    "更新日",
    "最終更新日",
    "対象年度",
    "年度",
  ];

  for (const colName of dateColumnNames) {
    if (record[colName] && parseDate(record[colName])) {
      return colName;
    }
  }

  // 日付形式の値を含むカラムを探す
  for (const [key, value] of Object.entries(record)) {
    if (value && /^\d{4}[-/]\d{2}[-/]\d{2}/.test(value)) {
      const date = parseDate(value);
      if (date) return key;
    }
  }

  return null;
}

function extractYearFromString(value: string | undefined): string | null {
  if (!value) return null;
  const match = value.match(/\d{4}/);
  return match ? match[0] : null;
}

// ============================================================================
// CSV読み込み関数
// ============================================================================

// ストリーミング処理用のコールバック型
type RecordProcessor = (record: CsvRecord) => void;

async function processCsvUtf8Stream(
  filePath: string,
  processor: RecordProcessor
): Promise<number> {
  log(`📖 UTF-8 CSV読み込み開始: ${path.basename(filePath)}`);
  let rowCount = 0;

  return new Promise((resolve, reject) => {
    const parser = parse({
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      bom: true,
    });

    createReadStream(filePath, { encoding: "utf8" })
      .pipe(parser)
      .on("data", (record: CsvRecord) => {
        processor(record);
        rowCount++;
        if (rowCount % 100000 === 0) {
          log(`  📊 処理中: ${rowCount.toLocaleString()} 行`);
        }
      })
      .on("end", () => {
        log(`  ✅ 読み込み完了: ${rowCount.toLocaleString()} 行`);
        resolve(rowCount);
      })
      .on("error", (error) => {
        reject(error);
      });
  });
}

async function processCsvSjisStream(
  filePath: string,
  processor: RecordProcessor
): Promise<number> {
  log(`📖 SJIS CSV読み込み開始: ${path.basename(filePath)}`);
  let rowCount = 0;

  return new Promise((resolve, reject) => {
    const parser = parse({
      columns: true,
      skip_empty_lines: true,
      relax_quotes: true,
      relax_column_count: true,
      bom: true,
    });

    fs.createReadStream(filePath)
      .pipe(iconv.decodeStream("Shift_JIS"))
      .pipe(parser)
      .on("data", (record: CsvRecord) => {
        processor(record);
        rowCount++;
        if (rowCount % 100000 === 0) {
          log(`  📊 処理中: ${rowCount.toLocaleString()} 行`);
        }
      })
      .on("end", () => {
        log(`  ✅ 読み込み完了: ${rowCount.toLocaleString()} 行`);
        resolve(rowCount);
      })
      .on("error", (error) => {
        reject(error);
      });
  });
}

// ============================================================================
// データ処理関数
// ============================================================================

async function processKihonjohoStream(
  filePath: string
): Promise<Map<string, CompanyData>> {
  log("🔄 Kihonjoho処理開始");
  const companies = new Map<string, CompanyData>();
  let missingCorporateNumber = 0;
  let duplicates = 0;

  await processCsvUtf8Stream(filePath, (record) => {
    const corporateNumber = record["法人番号"]?.trim();
    if (!corporateNumber) {
      missingCorporateNumber++;
      return;
    }

    if (companies.has(corporateNumber)) {
      duplicates++;
      // 既存のデータと比較して、欠損が少ない方を採用（後勝ち）
      const existing = companies.get(corporateNumber)!;
      const existingNullCount = Object.values(existing).filter(
        (v) => !v || v === ""
      ).length;
      const newNullCount = Object.values(record).filter(
        (v) => !v || v === ""
      ).length;

      if (newNullCount < existingNullCount) {
        companies.set(corporateNumber, {
          corporateNumber,
          ...record,
        });
      }
    } else {
      companies.set(corporateNumber, {
        corporateNumber,
        ...record,
      });
    }
  });

  log(`  ✅ 処理完了: ${companies.size} 社`);
  if (missingCorporateNumber > 0) {
    log(`  ⚠️  法人番号欠損: ${missingCorporateNumber} 行`);
  }
  if (duplicates > 0) {
    log(`  ⚠️  重複法人番号: ${duplicates} 件（後勝ちで集約）`);
  }

  return companies;
}

async function processZaimujohoStream(
  filePath: string,
  companies: Map<string, CompanyData>
): Promise<void> {
  log("🔄 Zaimujoho処理開始");
  // 各法人番号ごとに最新1件だけを保持（メモリ効率化）
  const zaimuByCorporateNumber = new Map<string, CsvRecord>();
  let dateColumn: string | null = null;
  let firstRecordSeen = false;

  // ストリーミングで処理（各法人番号ごとに最新1件だけを保持）
  await processCsvUtf8Stream(filePath, (record) => {
    const corporateNumber = record["法人番号"]?.trim();
    if (!corporateNumber || !companies.has(corporateNumber)) return;

    // 最初のレコードで日付カラムを判定
    if (!firstRecordSeen) {
      firstRecordSeen = true;
      dateColumn = findLatestDateColumn(record);
    }

    const existing = zaimuByCorporateNumber.get(corporateNumber);
    if (!existing) {
      zaimuByCorporateNumber.set(corporateNumber, record);
      return;
    }

    // 既存レコードと比較して、より新しい方を保持
    let shouldReplace = false;
    if (dateColumn) {
      const existingDate = parseDate(existing[dateColumn]);
      const newDate = parseDate(record[dateColumn]);
      if (newDate && (!existingDate || newDate.getTime() > existingDate.getTime())) {
        shouldReplace = true;
      }
    } else {
      // 日付カラムが見つからない場合は年度で比較
      const existingYear = extractYearFromString(
        existing["事業年度"] || existing["年度"] || ""
      );
      const newYear = extractYearFromString(
        record["事業年度"] || record["年度"] || ""
      );
      if (
        newYear &&
        (!existingYear || parseInt(newYear) > parseInt(existingYear))
      ) {
        shouldReplace = true;
      }
    }

    if (shouldReplace) {
      zaimuByCorporateNumber.set(corporateNumber, record);
    }
  });

  // 最新1件を結合
  let processed = 0;

  for (const [corporateNumber, latestRecord] of zaimuByCorporateNumber) {
    if (!companies.has(corporateNumber)) continue;

    const company = companies.get(corporateNumber)!;

    // マッピング
    if (latestRecord["資本金"]) {
      company.capitalStock = latestRecord["資本金"];
    }
    if (latestRecord["売上高"]) {
      company.revenue = latestRecord["売上高"];
    }
    if (latestRecord["従業員数"]) {
      company.employeeCount = latestRecord["従業員数"];
    }
    if (latestRecord["事業年度"]) {
      const year = extractYearFromString(latestRecord["事業年度"]);
      if (year) {
        company.fiscalMonth = year; // 簡易的に年度を設定
      }
    }
    if (dateColumn && latestRecord[dateColumn]) {
      company.zaimuLatestDate = latestRecord[dateColumn];
    }

    processed++;
  }

  log(`  ✅ 処理完了: ${processed} 社に結合`);
  if (!dateColumn) {
    log(`  ⚠️  日付カラム未検出（年度で判定）`);
  }
}

async function processChotatsujohoStream(
  filePath: string,
  companies: Map<string, CompanyData>
): Promise<void> {
  log("🔄 Chotatsujoho処理開始");
  const chotatsuByCorporateNumber = new Map<
    string,
    { count: number; latestDate?: Date; maxAmount?: number }
  >();

  // ストリーミングでサマリを計算（レコード全体は保持しない）
  await processCsvUtf8Stream(filePath, (record) => {
    const corporateNumber = record["法人番号"]?.trim();
    if (!corporateNumber || !companies.has(corporateNumber)) return;

    if (!chotatsuByCorporateNumber.has(corporateNumber)) {
      chotatsuByCorporateNumber.set(corporateNumber, { count: 0 });
    }
    const summary = chotatsuByCorporateNumber.get(corporateNumber)!;
    summary.count++;

    // 最新日を更新
    const date = parseDate(record["受注日"]);
    if (date) {
      if (!summary.latestDate || date.getTime() > summary.latestDate.getTime()) {
        summary.latestDate = date;
      }
    }

    // 最大金額を更新
    const amount = parseNumeric(record["金額"]);
    if (amount !== null) {
      if (!summary.maxAmount || amount > summary.maxAmount) {
        summary.maxAmount = amount;
      }
    }
  });

  // サマリを結合
  let processed = 0;

  for (const [corporateNumber, summary] of chotatsuByCorporateNumber) {
    if (!companies.has(corporateNumber)) continue;

    const company = companies.get(corporateNumber)!;

    company.procurementCount = summary.count;
    if (summary.latestDate) {
      company.procurementLatestDate = summary.latestDate.toISOString();
    }
    if (summary.maxAmount !== undefined) {
      company.procurementLatestAmount = summary.maxAmount.toString();
    }

    processed++;
  }

  log(`  ✅ 処理完了: ${processed} 社に結合`);
}

async function processShokubajohoStream(
  filePath: string,
  companies: Map<string, CompanyData>
): Promise<void> {
  log("🔄 Shokubajoho処理開始");
  const shokubaByCorporateNumber = new Map<
    string,
    { count: number; latestYear?: string }
  >();

  // 法人番号カラムを探す（最初の数行を読み込んで判定）
  let corporateNumberColumn: string | null = null;
  let firstRecordSeen = false;

  await processCsvSjisStream(filePath, (record) => {
    // 最初のレコードで法人番号カラムを判定
    if (!firstRecordSeen) {
      firstRecordSeen = true;
      for (const key of Object.keys(record)) {
        if (key.includes("法人番号") || key.includes("corporateNumber")) {
          corporateNumberColumn = key;
          break;
        }
      }
      // 見つからない場合は最初のカラムを試す
      if (!corporateNumberColumn) {
        corporateNumberColumn = Object.keys(record)[0];
      }
    }

    if (!corporateNumberColumn) return;

    const corporateNumber = record[corporateNumberColumn]?.trim();
    if (!corporateNumber || !companies.has(corporateNumber)) return;

    if (!shokubaByCorporateNumber.has(corporateNumber)) {
      shokubaByCorporateNumber.set(corporateNumber, { count: 0 });
    }
    const summary = shokubaByCorporateNumber.get(corporateNumber)!;
    summary.count++;

    // 最新年を更新
    for (const [key, value] of Object.entries(record)) {
      if (key.includes("更新") || key.includes("年度") || key.includes("年")) {
        const year = extractYearFromString(value);
        if (year) {
          if (
            !summary.latestYear ||
            parseInt(year) > parseInt(summary.latestYear)
          ) {
            summary.latestYear = year;
          }
        }
      }
    }
  });

  if (!corporateNumberColumn) {
    log("  ⚠️  法人番号カラムが見つかりません");
    return;
  }

  // サマリを結合
  let processed = 0;

  for (const [corporateNumber, summary] of shokubaByCorporateNumber) {
    if (!companies.has(corporateNumber)) continue;

    const company = companies.get(corporateNumber)!;

    company.workplaceRowCount = summary.count;
    if (summary.latestYear) {
      company.workplaceLatestYear = summary.latestYear;
    }

    processed++;
  }

  log(`  ✅ 処理完了: ${processed} 社に結合`);
}

// ============================================================================
// フィールドマッピング関数
// ============================================================================

function mapToCompaniesNewFields(
  company: CompanyData
): Record<string, any> {
  const output: Record<string, any> = {};

  // 基本情報
  output.name = company["法人名"] || "";
  output.kana = company["法人名ふりがな"] || "";
  output.nameEn = company["法人名英語"] || "";
  output.corporateNumber = company.corporateNumber || "";
  // corporationType は gBizINFO にないので空欄

  // 住所・連絡先
  output.address = company["本社所在地"] || "";
  output.postalCode = company["郵便番号"] || "";
  // prefecture は address から抽出可能だが、今回は空欄
  output.headquartersAddress = company["本社所在地"] || "";
  // phoneNumber, contactPhoneNumber, fax, email, companyUrl, contactFormUrl は gBizINFO にないので空欄

  // 業種・事業
  // industry 関連は gBizINFO にないので空欄
  output.businessDescriptions = company["事業概要"] || "";
  output.businessItems = company["営業品目リスト"]
    ? JSON.stringify([company["営業品目リスト"]])
    : "";
  // businessSummary は空欄

  // 財務・経営
  output.capitalStock = company.capitalStock || "";
  output.revenue = company.revenue || "";
  output.employeeCount = company.employeeCount || "";
  output.foundingYear = company["創業年"] || "";
  output.fiscalMonth = company.fiscalMonth || "";
  // revenueFromStatutes, employeeNumber, financials, factoryCount, officeCount, storeCount は空欄

  // 上場関連
  // listing, marketSegment, securityCode, securitiesCode, nikkeiCode, tradingStatus は空欄

  // 取引種別
  // transactionType, needs は空欄

  // 代表者・役員
  output.representativeName = company["法人代表者名"] || "";
  output.representativeTitle = company["法人代表者役職"] || "";
  // その他の代表者フィールドは空欄

  // 組織・関連
  // subsidiaries, shareholders, suppliers, clients, relatedCompanies, banks, bankCorporateNumber は空欄

  // その他
  output.companyUrl = company["企業ホームページ"] || "";
  // その他のフィールドは空欄

  // タイムスタンプ・メタ
  const now = new Date().toISOString();
  output.createdAt = now;
  output.updatedAt = company["最終更新日"] || now;
  // その他のメタフィールドは空欄

  // 上場企業専用
  // すべて空欄

  // 追加サマリ
  output.procurementCount = company.procurementCount?.toString() || "";
  output.procurementLatestDate = company.procurementLatestDate || "";
  output.workplaceRowCount = company.workplaceRowCount?.toString() || "";
  output.workplaceLatestYear = company.workplaceLatestYear || "";

  return output;
}

// ============================================================================
// 出力カラム定義（companies_new の既存フィールド）
// ============================================================================

const OUTPUT_COLUMNS = [
  // 基本情報
  "name",
  "kana",
  "nameEn",
  "corporateNumber",
  "corporationType",
  // 住所・連絡先
  "address",
  "postalCode",
  "prefecture",
  "headquartersAddress",
  "phoneNumber",
  "contactPhoneNumber",
  "fax",
  "email",
  "companyUrl",
  "contactFormUrl",
  // 業種・事業
  "industry",
  "industries",
  "industryLarge",
  "industryMiddle",
  "industrySmall",
  "industryDetail",
  "industryCategories",
  "businessDescriptions",
  "businessItems",
  "businessSummary",
  // 財務・経営
  "capitalStock",
  "revenue",
  "revenueFromStatements",
  "employeeCount",
  "employeeNumber",
  "foundingYear",
  "fiscalMonth",
  "financials",
  "factoryCount",
  "officeCount",
  "storeCount",
  // 上場関連
  "listing",
  "marketSegment",
  "securityCode",
  "securitiesCode",
  "nikkeiCode",
  "tradingStatus",
  // 取引種別
  "transactionType",
  "needs",
  // 代表者・役員
  "representativeName",
  "representativeKana",
  "representativeTitle",
  "representativeBirthDate",
  "representativePhone",
  "representativeHomeAddress",
  "representativeRegisteredAddress",
  "representativeAlmaMater",
  "executives",
  "executiveName1",
  "executivePosition1",
  "executiveName2",
  "executivePosition2",
  "executiveName3",
  "executivePosition3",
  "executiveName4",
  "executivePosition4",
  "executiveName5",
  "executivePosition5",
  "executiveName6",
  "executivePosition6",
  "executiveName7",
  "executivePosition7",
  "executiveName8",
  "executivePosition8",
  "executiveName9",
  "executivePosition9",
  "executiveName10",
  "executivePosition10",
  // 組織・関連
  "subsidiaries",
  "shareholders",
  "suppliers",
  "clients",
  "relatedCompanies",
  "banks",
  "bankCorporateNumber",
  // その他
  "tags",
  "urls",
  "overview",
  "companyDescription",
  "demandProducts",
  "salesNotes",
  "acquisition",
  "facebook",
  "linkedin",
  "wantedly",
  "youtrust",
  "externalDetailUrl",
  "profileUrl",
  "metaDescription",
  "metaKeywords",
  "adExpiration",
  "registrant",
  "location",
  "departmentLocation",
  // タイムスタンプ・メタ
  "createdAt",
  "updatedAt",
  "extendedFieldsScrapedAt",
  "updateCount",
  "changeCount",
  // 上場企業専用
  "listedParentName",
  "listedParentCorporateNumber",
  "listedParentEdinet",
  "listedGroupAsOf",
  "listedGroupCached",
  "listedGroupConfidence",
  "listedGroupConsolidation",
  "listedGroupOwnership",
  "listedGroupSource",
  // 追加サマリ
  "procurementCount",
  "procurementLatestDate",
  "workplaceRowCount",
  "workplaceLatestYear",
];

// ============================================================================
// メイン処理
// ============================================================================

async function main() {
  log("🚀 gBizINFO CSV統合バッチ開始");

  // 入力ファイルの存在確認
  for (const [name, filePath] of Object.entries(INPUT_FILES)) {
    if (!fs.existsSync(filePath)) {
      log(`❌ エラー: 入力ファイルが見つかりません: ${filePath}`);
      process.exit(1);
    }
  }

  // 出力ディレクトリ作成
  if (!fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
    log(`📁 出力ディレクトリ作成: ${OUTPUT_DIR}`);
  }

  // 1. Kihonjoho読み込み・処理（ストリーミング）
  log("\n" + "=".repeat(60));
  log("ステップ1: Kihonjoho処理");
  log("=".repeat(60));
  const companies = await processKihonjohoStream(INPUT_FILES.kihonjoho);

  // 2. Zaimujoho読み込み・処理（ストリーミング）
  log("\n" + "=".repeat(60));
  log("ステップ2: Zaimujoho処理");
  log("=".repeat(60));
  await processZaimujohoStream(INPUT_FILES.zaimujoho, companies);

  // 3. Chotatsujoho読み込み・処理（ストリーミング）
  log("\n" + "=".repeat(60));
  log("ステップ3: Chotatsujoho処理");
  log("=".repeat(60));
  await processChotatsujohoStream(INPUT_FILES.chotatsujoho, companies);

  // 4. Shokubajoho読み込み・処理（ストリーミング）
  log("\n" + "=".repeat(60));
  log("ステップ4: Shokubajoho処理");
  log("=".repeat(60));
  await processShokubajohoStream(INPUT_FILES.shokubajoho, companies);

  // 5. 出力CSV作成（ストリーミング）
  log("\n" + "=".repeat(60));
  log("ステップ5: 出力CSV作成");
  log("=".repeat(60));

  // 法人番号が有効な会社数をカウント
  let validCompanyCount = 0;
  for (const company of companies.values()) {
    if (company.corporateNumber && company.corporateNumber.trim() !== "") {
      validCompanyCount++;
    }
  }
  log(`📝 出力レコード数: ${validCompanyCount.toLocaleString()}`);

  // CSV出力（ストリーミング）
  const writeStream = createWriteStream(OUTPUT_FILE, { encoding: "utf8" });
  
  // ヘッダー書き込み
  writeStream.write(OUTPUT_COLUMNS.map(col => `"${col}"`).join(",") + "\n");

  // レコード書き込み（ストリーミング）
  let writtenCount = 0;
  for (const company of companies.values()) {
    if (!company.corporateNumber || company.corporateNumber.trim() === "") {
      continue;
    }

    const mapped = mapToCompaniesNewFields(company);
    const row = OUTPUT_COLUMNS.map(col => {
      const value = mapped[col];
      let strValue = "";
      if (value === null || value === undefined) {
        strValue = "";
      } else if (typeof value === "object") {
        strValue = JSON.stringify(value);
      } else {
        strValue = String(value);
      }
      // CSVエスケープ: ダブルクォートをエスケープ
      const escaped = strValue.replace(/"/g, '""');
      return `"${escaped}"`;
    }).join(",");
    
    writeStream.write(row + "\n");
    writtenCount++;
    
    if (writtenCount % 10000 === 0) {
      log(`  📝 書き込み中: ${writtenCount.toLocaleString()} 行`);
    }
  }

  writeStream.end();
  
  await new Promise((resolve, reject) => {
    writeStream.on("finish", () => {
      log(`✅ 出力完了: ${OUTPUT_FILE}`);
      resolve(undefined);
    });
    writeStream.on("error", (error) => {
      log(`⚠️  書き込みエラー: ${error.message}`);
      reject(error);
    });
  });

  // サマリ表示
  log("\n" + "=".repeat(60));
  log("処理サマリ");
  log("=".repeat(60));
  log(`📊 出力企業数: ${outputRecords.length.toLocaleString()}`);
  log(`📄 出力ファイル: ${OUTPUT_FILE}`);
  log(`📏 ファイルサイズ: ${(fs.statSync(OUTPUT_FILE).size / 1024 / 1024).toFixed(2)} MB`);

  // 先頭5行を表示
  log("\n" + "=".repeat(60));
  log("出力CSV先頭5行（サンプル）");
  log("=".repeat(60));
  const fileContent = fs.readFileSync(OUTPUT_FILE, "utf8");
  const sampleLines = fileContent.split("\n").slice(0, 6);
  for (const line of sampleLines) {
    console.log(line);
  }

  log("\n✅ 処理完了");
}

// 実行
main().catch((error) => {
  console.error("❌ エラー:", error);
  process.exit(1);
});

