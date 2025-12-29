/* 
  見つからなかったCSVファイルをヘッダーの種類別にグループ化するスクリプト
  
  使い方:
    npx ts-node scripts/group_csvs_by_header.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

const CSV_DIR = path.join(process.cwd(), "csv");

// 見つからなかったCSVファイルのリストをファイルから読み込む
function loadMissingCsvFiles(): string[] {
  const filePath = path.join(process.cwd(), "missing_csv_files.txt");
  if (fs.existsSync(filePath)) {
    const content = fs.readFileSync(filePath, "utf-8");
    return content
      .split("\n")
      .map(line => line.trim())
      .filter(line => line.length > 0 && line.endsWith(".csv"));
  }
  // フォールバック: デフォルトリスト
  return [
    "1.csv", "10.csv", "100.csv", "101.csv", "102.csv", "103.csv", "105.csv", "106.csv", "107.csv", "108.csv",
    "11.csv", "110.csv", "111.csv", "112.csv", "113.csv", "114.csv", "115.csv", "116.csv", "117.csv", "118.csv",
    "119.csv", "12.csv", "120.csv", "121.csv", "122.csv", "123.csv", "124.csv", "125.csv", "126.csv", "127.csv",
    "13.csv", "130.csv", "131.csv", "132.csv", "133.csv", "134.csv", "14.csv", "15.csv", "16.csv", "17.csv",
    "18.csv", "19.csv", "2.csv", "20.csv", "21.csv", "22.csv", "23.csv", "24.csv", "25.csv", "26.csv",
    "27.csv", "28.csv", "29.csv", "3.csv", "30.csv", "31.csv", "33.csv", "34.csv", "35.csv", "36.csv",
    "39.csv", "4.csv", "40.csv", "42.csv", "43.csv", "44.csv", "45.csv", "46.csv", "47.csv", "48.csv",
    "49.csv", "5.csv", "50.csv", "51.csv", "52.csv", "53.csv", "54.csv", "55.csv", "56.csv", "57.csv",
    "58.csv", "59.csv", "6.csv", "60.csv", "61.csv", "62.csv", "63.csv", "64.csv", "65.csv", "66.csv",
    "67.csv", "68.csv", "69.csv", "7.csv", "70.csv", "71.csv", "72.csv", "73.csv", "74.csv", "75.csv",
    "76.csv", "77.csv", "78.csv", "79.csv", "8.csv", "80.csv", "81.csv", "82.csv", "83.csv", "84.csv",
    "85.csv", "86.csv", "87.csv", "88.csv", "89.csv", "9.csv", "90.csv", "91.csv", "92.csv", "93.csv",
    "94.csv", "95.csv", "96.csv", "97.csv", "98.csv", "99.csv"
  ];
}

// CSVファイルのヘッダーを取得
function getCsvHeaders(csvPath: string): string[] | null {
  try {
    const content = fs.readFileSync(csvPath, "utf-8");
    const lines = content.split("\n");
    if (lines.length === 0) return null;
    
    const headers = parse(lines[0], {
      columns: false,
      skip_empty_lines: true,
      bom: true,
    }) as string[][];
    
    if (headers.length === 0 || headers[0].length === 0) return null;
    return headers[0];
  } catch (err: any) {
    console.error(`❌ エラー (${csvPath}): ${err.message}`);
    return null;
  }
}

// ヘッダーを正規化（比較用）
function normalizeHeaders(headers: string[]): string {
  return headers
    .map(h => h.trim().toLowerCase())
    .sort()
    .join("|");
}

// メイン処理
function main() {
  console.log("🔍 CSVファイルをヘッダーの種類別にグループ化します...\n");

  const missingCsvFiles = loadMissingCsvFiles();
  console.log(`📁 対象CSVファイル数: ${missingCsvFiles.length}\n`);

  const headerGroups = new Map<string, Array<{ file: string; headers: string[] }>>();

  for (const csvFile of missingCsvFiles) {
    const csvPath = path.join(CSV_DIR, csvFile);
    
    if (!fs.existsSync(csvPath)) {
      console.warn(`⚠️  ファイルが見つかりません: ${csvFile}`);
      continue;
    }

    const headers = getCsvHeaders(csvPath);
    if (!headers || headers.length === 0) {
      console.warn(`⚠️  ヘッダーを取得できませんでした: ${csvFile}`);
      continue;
    }

    const normalizedKey = normalizeHeaders(headers);
    
    if (!headerGroups.has(normalizedKey)) {
      headerGroups.set(normalizedKey, []);
    }
    
    headerGroups.get(normalizedKey)!.push({ file: csvFile, headers });
  }

  console.log(`📊 検出されたヘッダーパターン数: ${headerGroups.size}\n`);

  // 結果を表示
  let groupIndex = 1;
  const results: Array<{
    groupIndex: number;
    headerPattern: string[];
    files: string[];
    fileCount: number;
  }> = [];

  for (const [normalizedKey, files] of headerGroups.entries()) {
    const headerPattern = files[0].headers;
    const fileList = files.map(f => f.file).sort();
    
    results.push({
      groupIndex,
      headerPattern,
      files: fileList,
      fileCount: fileList.length,
    });

    console.log("=".repeat(80));
    console.log(`グループ ${groupIndex}: ${fileList.length}ファイル`);
    console.log("=".repeat(80));
    console.log(`ヘッダー: ${headerPattern.join(", ")}`);
    console.log(`\nファイル一覧:`);
    fileList.forEach(file => {
      console.log(`  - ${file}`);
    });
    console.log(`\nインポートコマンド（グループ全体）:`);
    console.log(`GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \\`);
    console.log(`npx ts-node scripts/import_companies_from_csv.ts ./csv`);
    console.log(`\nまたは、個別にインポートする場合:`);
    fileList.slice(0, 3).forEach(file => {
      console.log(`GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \\`);
      console.log(`npx ts-node scripts/import_companies_from_csv.ts ./csv/${file}`);
    });
    if (fileList.length > 3) {
      console.log(`  ... 他 ${fileList.length - 3}ファイル`);
    }
    console.log();
    
    groupIndex++;
  }

  // 結果をJSONファイルに保存
  const timestamp = Date.now();
  const resultFile = path.join(process.cwd(), `csv_header_groups_${timestamp}.json`);
  fs.writeFileSync(resultFile, JSON.stringify(results, null, 2), "utf-8");
  console.log(`\n📄 詳細結果をファイルに保存しました: ${resultFile}`);
}

main();
