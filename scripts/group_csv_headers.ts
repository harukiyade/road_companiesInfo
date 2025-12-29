/* 
  CSVファイルのヘッダーをグループ分けするスクリプト
  
  使い方:
    npx ts-node scripts/group_csv_headers.ts
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

const TARGET_FILES = [
  "csv/38.csv",
  "csv/107.csv",
  "csv/108.csv",
  "csv/109.csv",
  "csv/110.csv",
  "csv/111.csv",
  "csv/112.csv",
  "csv/113.csv",
  "csv/114.csv",
  "csv/115.csv",
  "csv/116.csv",
  "csv/117.csv",
  "csv/118.csv",
  "csv/119.csv",
  "csv/120.csv",
  "csv/121.csv",
  "csv/122.csv",
  "csv/123.csv",
  "csv/124.csv",
  "csv/125.csv",
];

interface HeaderInfo {
  fileName: string;
  headers: string[];
  headerKey: string; // ヘッダーの内容と順番を表すキー
}

// ヘッダーを正規化（空白除去、大文字小文字統一など）
function normalizeHeader(header: string): string {
  return header.trim();
}

// ヘッダーのキーを生成（内容と順番を表す）
function generateHeaderKey(headers: string[]): string {
  return headers.map(h => normalizeHeader(h)).join("|");
}

// メイン処理
function main() {
  console.log("📊 CSVファイルのヘッダーをグループ分けします\n");

  const headerInfos: HeaderInfo[] = [];

  // 各CSVファイルのヘッダーを取得
  for (const filePath of TARGET_FILES) {
    const fileName = path.basename(filePath);
    
    try {
      if (!fs.existsSync(filePath)) {
        console.log(`⚠️  ファイルが見つかりません: ${filePath}`);
        continue;
      }

      const csvContent = fs.readFileSync(filePath, "utf8");
      const records: Record<string, string>[] = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        relax_column_count: true,
        relax_quotes: true,
        skip_records_with_error: true,
      });

      if (records.length === 0) {
        console.log(`⚠️  ${fileName}: ヘッダーが見つかりません`);
        continue;
      }

      const headers = Object.keys(records[0]);
      const headerKey = generateHeaderKey(headers);

      headerInfos.push({
        fileName,
        headers,
        headerKey,
      });

      console.log(`✅ ${fileName}: ${headers.length}列`);
    } catch (err: any) {
      console.error(`❌ エラー (${fileName}): ${err.message}`);
    }
  }

  console.log(`\n📋 合計: ${headerInfos.length}ファイルのヘッダーを取得しました\n`);

  // ヘッダーキーでグループ分け
  const groups = new Map<string, HeaderInfo[]>();
  
  for (const info of headerInfos) {
    if (!groups.has(info.headerKey)) {
      groups.set(info.headerKey, []);
    }
    groups.get(info.headerKey)!.push(info);
  }

  // 結果を表示
  console.log("=".repeat(80));
  console.log(`📊 ヘッダーグループ分け結果: ${groups.size}グループ`);
  console.log("=".repeat(80));
  console.log();

  const sortedGroups = Array.from(groups.entries()).sort((a, b) => {
    // グループ内のファイル数でソート（多い順）
    return b[1].length - a[1].length;
  });

  for (let groupIndex = 0; groupIndex < sortedGroups.length; groupIndex++) {
    const [headerKey, files] = sortedGroups[groupIndex];
    const sampleHeaders = files[0].headers;

    console.log(`📁 グループ ${groupIndex + 1} (${files.length}ファイル)`);
    console.log("-".repeat(80));
    console.log(`ファイル: ${files.map(f => f.fileName).join(", ")}`);
    console.log(`ヘッダー数: ${sampleHeaders.length}`);
    console.log(`ヘッダー一覧:`);
    sampleHeaders.forEach((header, index) => {
      console.log(`  ${index + 1}. ${header}`);
    });
    console.log();
  }

  // 結果をファイルに保存
  const timestamp = Date.now();
  const outputFile = `csv_header_groups_${timestamp}.txt`;
  let outputContent = `CSVヘッダーグループ分け結果\n`;
  outputContent += `生成日時: ${new Date().toLocaleString("ja-JP")}\n`;
  outputContent += `合計グループ数: ${groups.size}\n`;
  outputContent += `合計ファイル数: ${headerInfos.length}\n\n`;
  outputContent += "=".repeat(80) + "\n\n";

  for (let groupIndex = 0; groupIndex < sortedGroups.length; groupIndex++) {
    const [headerKey, files] = sortedGroups[groupIndex];
    const sampleHeaders = files[0].headers;

    outputContent += `グループ ${groupIndex + 1} (${files.length}ファイル)\n`;
    outputContent += "-".repeat(80) + "\n";
    outputContent += `ファイル: ${files.map(f => f.fileName).join(", ")}\n`;
    outputContent += `ヘッダー数: ${sampleHeaders.length}\n`;
    outputContent += `ヘッダー一覧:\n`;
    sampleHeaders.forEach((header, index) => {
      outputContent += `  ${index + 1}. ${header}\n`;
    });
    outputContent += "\n";
  }

  fs.writeFileSync(outputFile, outputContent, "utf8");

  console.log("=".repeat(80));
  console.log(`✅ 結果をファイルに保存しました: ${outputFile}`);
  console.log("=".repeat(80));
}

try {
  main();
} catch (err: any) {
  console.error("❌ 致命的なエラー:", err);
  process.exit(1);
}

