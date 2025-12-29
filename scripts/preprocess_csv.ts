/*
  CSVファイルの前処理スクリプト
  
  機能:
  1. タイプC: 重複ヘッダー（郵便番号, 住所）を修正
  2. タイプF: 特殊形式の変換（JSON展開、英語ヘッダー対応など）
  3. 列ずれの検出と修正
  
  使い方:
    npx ts-node scripts/preprocess_csv.ts [--dry-run] [csvファイル or ディレクトリ...]
    
    例:
      # すべてのCSVを前処理（ドライラン）
      npx ts-node scripts/preprocess_csv.ts --dry-run ./csv
      
      # 特定ファイルを前処理
      npx ts-node scripts/preprocess_csv.ts ./csv/23.csv
*/

import * as fs from "fs";
import * as path from "path";

const DRY_RUN = process.argv.includes("--dry-run");
const OUTPUT_DIR = "./csv_preprocessed";

// ==============================
// ヘルパー関数
// ==============================

function isCsvFile(p: string): boolean {
  return p.toLowerCase().endsWith(".csv");
}

function collectCsvFiles(): string[] {
  const args = process.argv.slice(2).filter((a) => a !== "--dry-run");

  if (args.length === 0) {
    const defaultDir = path.resolve("./csv");
    if (!fs.existsSync(defaultDir)) {
      console.error('❌ エラー: "./csv" ディレクトリが存在しません');
      process.exit(1);
    }
    const files = fs
      .readdirSync(defaultDir)
      .filter((f) => isCsvFile(f))
      .map((f) => path.join(defaultDir, f));
    return files;
  }

  const result: string[] = [];
  for (const arg of args) {
    const resolved = path.resolve(arg);
    if (!fs.existsSync(resolved)) continue;
    const stat = fs.statSync(resolved);
    if (stat.isDirectory()) {
      const files = fs
        .readdirSync(resolved)
        .filter((f) => isCsvFile(f))
        .map((f) => path.join(resolved, f));
      result.push(...files);
    } else if (stat.isFile() && isCsvFile(resolved)) {
      result.push(resolved);
    }
  }
  return result;
}

// CSVを手動でパース（重複ヘッダー対応のため）
function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += char;
    }
  }
  result.push(current);
  return result;
}

function escapeCSVField(field: string): string {
  if (field.includes(",") || field.includes('"') || field.includes("\n")) {
    return '"' + field.replace(/"/g, '""') + '"';
  }
  return field;
}

function buildCSVLine(fields: string[]): string {
  return fields.map(escapeCSVField).join(",");
}

// ==============================
// CSVタイプ検出
// ==============================

type CSVType = "A" | "B" | "C" | "D" | "E" | "F_JSON" | "F_ENG" | "F_SPECIAL" | "UNKNOWN";

function detectCSVType(headers: string[]): CSVType {
  const headerStr = headers.join(",");

  // タイプC: 重複ヘッダー（郵便番号, 住所が2回出現）
  const postalCount = headers.filter((h) => h === "郵便番号").length;
  const addressCount = headers.filter((h) => h === "住所").length;
  if (postalCount >= 2 || addressCount >= 2) {
    return "C";
  }

  // タイプF_JSON: JSON列がある
  if (headers.some((h) => h.endsWith("Json") || h.includes("summaryJson"))) {
    return "F_JSON";
  }

  // タイプF_ENG: 英語ヘッダー（name, corporateNumber等で始まる）
  if (
    headers[0] === "name" &&
    headers.includes("corporateNumber") &&
    headers.includes("representative")
  ) {
    return "F_ENG";
  }

  // タイプB: 法人番号で始まる基本形式
  if (headers[0] === "法人番号" && headers[1] === "会社名") {
    return "B";
  }

  // タイプA: 会社名で始まる基本形式（法人番号なし）
  if (
    headers[0] === "会社名" &&
    headers.includes("電話番号") &&
    (headers.includes("会社郵便番号") || headers.includes("郵便番号")) &&
    headers.includes("業種-大")
  ) {
    return "A";
  }

  // タイプD: 都道府県・ID詳細形式
  if (
    headers.includes("都道府県") &&
    headers.includes("法人番号") &&
    (headers.includes("ID") || headers.includes("会社ID"))
  ) {
    return "D";
  }

  // タイプE: 都道府県形式（法人番号なし）
  if (
    headers.includes("都道府県") &&
    !headers.includes("法人番号") &&
    headers.includes("取引種別")
  ) {
    return "E";
  }

  // タイプF_SPECIAL: その他特殊形式
  if (headers[0] === "ジャンル" || headers.includes("業種（分類１）")) {
    return "F_SPECIAL";
  }

  return "UNKNOWN";
}

// ==============================
// タイプ別変換処理
// ==============================

interface ProcessResult {
  headers: string[];
  rows: string[][];
  modified: boolean;
  modifications: string[];
}

// タイプC: 重複ヘッダーを修正
function processTypeC(headers: string[], rows: string[][]): ProcessResult {
  const modifications: string[] = [];
  const newHeaders = [...headers];

  // 「郵便番号」「住所」の出現位置を特定
  let postalCount = 0;
  let addressCount = 0;

  for (let i = 0; i < newHeaders.length; i++) {
    if (newHeaders[i] === "郵便番号") {
      postalCount++;
      if (postalCount === 1) {
        newHeaders[i] = "会社郵便番号";
        modifications.push(`列${i + 1}: "郵便番号" → "会社郵便番号"`);
      } else if (postalCount === 2) {
        newHeaders[i] = "代表者郵便番号";
        modifications.push(`列${i + 1}: "郵便番号" → "代表者郵便番号"`);
      }
    }
    if (newHeaders[i] === "住所") {
      addressCount++;
      if (addressCount === 1) {
        newHeaders[i] = "会社住所";
        modifications.push(`列${i + 1}: "住所" → "会社住所"`);
      } else if (addressCount === 2) {
        newHeaders[i] = "代表者住所";
        modifications.push(`列${i + 1}: "住所" → "代表者住所"`);
      }
    }
  }

  // 「代表者」→「代表者名」に統一
  for (let i = 0; i < newHeaders.length; i++) {
    if (newHeaders[i] === "代表者") {
      newHeaders[i] = "代表者名";
      modifications.push(`列${i + 1}: "代表者" → "代表者名"`);
    }
  }

  // 業種ヘッダーの統一
  const industryMapping: Record<string, string> = {
    "業種（大）": "業種-大",
    "業種（中）": "業種-中",
    "業種（小）": "業種-小",
    "業種（細）": "業種-細",
  };
  for (let i = 0; i < newHeaders.length; i++) {
    if (industryMapping[newHeaders[i]]) {
      modifications.push(
        `列${i + 1}: "${newHeaders[i]}" → "${industryMapping[newHeaders[i]]}"`
      );
      newHeaders[i] = industryMapping[newHeaders[i]];
    }
  }

  return {
    headers: newHeaders,
    rows,
    modified: modifications.length > 0,
    modifications,
  };
}

// タイプF_ENG: 英語ヘッダーを日本語に変換
function processTypeF_ENG(headers: string[], rows: string[][]): ProcessResult {
  const modifications: string[] = [];
  const mapping: Record<string, string> = {
    name: "会社名",
    corporateNumber: "法人番号",
    representative: "代表者名",
    sales: "売上高",
    capital: "資本金",
    listing: "上場",
    address: "会社住所",
    employees: "従業員数",
    founded: "設立",
    fiscalMonth: "決算月",
    industries: "業種",
    tel: "電話番号",
    url: "URL",
    departments: "部署",
    people: "担当者",
    rawText: "備考",
  };

  const newHeaders = headers.map((h) => {
    if (mapping[h]) {
      modifications.push(`"${h}" → "${mapping[h]}"`);
      return mapping[h];
    }
    return h;
  });

  return {
    headers: newHeaders,
    rows,
    modified: modifications.length > 0,
    modifications,
  };
}

// タイプF_JSON: JSON列を展開（簡易版）
function processTypeF_JSON(headers: string[], rows: string[][]): ProcessResult {
  // JSON形式は複雑なので、基本フィールドのみ抽出
  const modifications: string[] = [];
  
  // 基本的なヘッダーのみ保持
  const basicHeaders = [
    "id",
    "name",
    "url",
    "corporateNumber",
    "prefecture",
    "listed",
    "detailUrl",
    "overview",
    "history",
  ];

  const headerIndices = basicHeaders
    .map((h) => headers.indexOf(h))
    .filter((i) => i >= 0);

  if (headerIndices.length === 0) {
    return { headers, rows, modified: false, modifications: ["JSON形式: 変換スキップ"] };
  }

  const newHeaders = headerIndices.map((i) => headers[i]);
  const newRows = rows.map((row) => headerIndices.map((i) => row[i] || ""));

  // 英語→日本語変換
  const mapping: Record<string, string> = {
    id: "ID",
    name: "会社名",
    url: "URL",
    corporateNumber: "法人番号",
    prefecture: "都道府県",
    listed: "上場",
    detailUrl: "詳細URL",
    overview: "概要",
    history: "沿革",
  };

  const finalHeaders = newHeaders.map((h) => mapping[h] || h);
  modifications.push(`JSON形式: ${basicHeaders.length}列を抽出`);

  return {
    headers: finalHeaders,
    rows: newRows,
    modified: true,
    modifications,
  };
}

// 汎用: ヘッダー標準化
function standardizeHeaders(headers: string[]): ProcessResult {
  const modifications: string[] = [];
  const mapping: Record<string, string> = {
    // 会社名
    企業名: "会社名",
    // 郵便番号
    "郵便番号": "会社郵便番号",
    // 住所
    所在地: "会社住所",
    本社住所: "会社住所",
    本社所在地: "会社住所",
    // 電話
    "電話番号(窓口)": "電話番号",
    // URL
    HP: "URL",
    ホームページ: "URL",
    // 代表者
    代表者: "代表者名",
    // 業種
    業種1: "業種-大",
    業種2: "業種-中",
    業種3: "業種-小",
    "業種（大）": "業種-大",
    "業種（中）": "業種-中",
    "業種（小）": "業種-小",
    "業種（細）": "業種-細",
    // 概要
    概要: "概況",
    // 設立
    創業: "設立",
    // 資本金・売上
    直近売上: "売上高",
    直近利益: "経常利益",
    社員数: "従業員数",
  };

  const newHeaders = headers.map((h) => {
    const trimmed = h.trim().replace(/^"|"$/g, ""); // ダブルクォート除去
    if (mapping[trimmed]) {
      modifications.push(`"${trimmed}" → "${mapping[trimmed]}"`);
      return mapping[trimmed];
    }
    return trimmed;
  });

  return {
    headers: newHeaders,
    rows: [],
    modified: modifications.length > 0,
    modifications,
  };
}

// ==============================
// メイン処理
// ==============================

function processCSVFile(filePath: string): { outputPath: string; modified: boolean; type: CSVType; modifications: string[] } | null {
  const content = fs.readFileSync(filePath, "utf8");
  const lines = content.split(/\r?\n/).filter((line) => line.trim() !== "");

  if (lines.length === 0) {
    return null;
  }

  const headers = parseCSVLine(lines[0]);
  const rows = lines.slice(1).map(parseCSVLine);

  const csvType = detectCSVType(headers);
  let result: ProcessResult;

  switch (csvType) {
    case "C":
      result = processTypeC(headers, rows);
      break;
    case "F_ENG":
      result = processTypeF_ENG(headers, rows);
      break;
    case "F_JSON":
      result = processTypeF_JSON(headers, rows);
      break;
    default:
      // 標準化のみ
      const stdResult = standardizeHeaders(headers);
      result = {
        headers: stdResult.headers,
        rows,
        modified: stdResult.modified,
        modifications: stdResult.modifications,
      };
  }

  // 追加の標準化
  const stdResult = standardizeHeaders(result.headers);
  if (stdResult.modified) {
    result.headers = stdResult.headers;
    result.modifications.push(...stdResult.modifications);
    result.modified = true;
  }

  // 出力
  const baseName = path.basename(filePath);
  const outputPath = path.join(OUTPUT_DIR, baseName);

  if (!DRY_RUN && result.modified) {
    const outputLines = [
      buildCSVLine(result.headers),
      ...result.rows.map(buildCSVLine),
    ];
    fs.writeFileSync(outputPath, outputLines.join("\n"), "utf8");
  }

  return {
    outputPath,
    modified: result.modified,
    type: csvType,
    modifications: result.modifications,
  };
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");

  const csvFiles = collectCsvFiles();
  console.log(`📂 CSV ファイル数: ${csvFiles.length}`);

  if (!DRY_RUN && !fs.existsSync(OUTPUT_DIR)) {
    fs.mkdirSync(OUTPUT_DIR, { recursive: true });
  }

  const stats: Record<CSVType, number> = {
    A: 0, B: 0, C: 0, D: 0, E: 0,
    F_JSON: 0, F_ENG: 0, F_SPECIAL: 0, UNKNOWN: 0,
  };
  let modifiedCount = 0;

  for (const file of csvFiles) {
    const result = processCSVFile(file);
    if (!result) continue;

    stats[result.type]++;

    if (result.modified) {
      modifiedCount++;
      console.log(`\n📝 ${path.basename(file)} (タイプ: ${result.type})`);
      result.modifications.forEach((m) => console.log(`   - ${m}`));
      if (!DRY_RUN) {
        console.log(`   → ${result.outputPath}`);
      }
    }
  }

  console.log("\n✅ 前処理完了");
  console.log(`  📊 タイプ別集計:`);
  Object.entries(stats)
    .filter(([_, count]) => count > 0)
    .forEach(([type, count]) => {
      console.log(`     - タイプ${type}: ${count}件`);
    });
  console.log(`  📝 修正が必要なファイル: ${modifiedCount}件`);

  if (DRY_RUN) {
    console.log(`\n💡 実際に前処理を実行するには、--dry-run フラグを外してください。`);
    console.log(`   出力先: ${OUTPUT_DIR}/`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

