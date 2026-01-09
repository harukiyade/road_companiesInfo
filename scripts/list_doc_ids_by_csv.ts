/* 
  各CSVファイルの代表的なドキュメントIDを一覧表示するスクリプト
*/

import * as fs from "fs";
import * as path from "path";

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

interface DocInfo {
  csvFile: string;
  rowNum: number;
  docId: string;
  companyName: string;
}

function parseUpdatedDocsLog(): Map<string, DocInfo[]> {
  const result = new Map<string, DocInfo[]>();
  
  // 最新のログファイルを探す
  const logFiles = fs.readdirSync(".")
    .filter(f => f.startsWith("updated_docs_first_5_rows_") && f.endsWith(".txt"))
    .sort()
    .reverse();
  
  if (logFiles.length === 0) {
    console.log("⚠️  ログファイルが見つかりません");
    return result;
  }
  
  const latestLog = logFiles[0];
  console.log(`📄 ログファイル: ${latestLog}\n`);
  
  const content = fs.readFileSync(latestLog, "utf8");
  const lines = content.split("\n").filter(l => l.trim());
  
  for (const line of lines) {
    // 形式: "38.csv - 行1: 9180000000000 (丹羽興業株式会社)"
    const match = line.match(/^([^ ]+) - 行(\d+): ([^ ]+) \((.+)\)$/);
    if (match) {
      const [, csvFile, rowNum, docId, companyName] = match;
      if (!result.has(csvFile)) {
        result.set(csvFile, []);
      }
      result.get(csvFile)!.push({
        csvFile,
        rowNum: parseInt(rowNum),
        docId,
        companyName,
      });
    }
  }
  
  return result;
}

function main() {
  const docMap = parseUpdatedDocsLog();
  
  console.log("=".repeat(80));
  console.log("📋 各CSVファイルの代表的なドキュメントID一覧");
  console.log("=".repeat(80));
  console.log();
  
  for (const filePath of TARGET_FILES) {
    const fileName = path.basename(filePath);
    const docs = docMap.get(fileName) || [];
    
    if (docs.length === 0) {
      console.log(`📄 ${fileName}: 更新されたドキュメントなし`);
      continue;
    }
    
    // 行番号でソート
    docs.sort((a, b) => a.rowNum - b.rowNum);
    
    console.log(`📄 ${fileName} (${docs.length}件)`);
    console.log("-".repeat(80));
    
    for (const doc of docs) {
      console.log(`  行${doc.rowNum}: ${doc.docId} (${doc.companyName})`);
    }
    
    console.log();
  }
  
  console.log("=".repeat(80));
  console.log(`✅ 合計: ${Array.from(docMap.values()).reduce((sum, arr) => sum + arr.length, 0)}件のドキュメントが更新されました`);
}

main();

