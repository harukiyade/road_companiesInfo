/**
 * 全CSVファイルのヘッダーを分析してタイプを分類するスクリプト
 */

import * as fs from "fs";
import * as path from "path";

interface HeaderAnalysis {
  file: string;
  headers: string[];
  headerCount: number;
  type: string;
}

const csvDir = path.join(process.cwd(), "csv");
const files = fs.readdirSync(csvDir)
  .filter(f => f.endsWith(".csv"))
  .sort((a, b) => parseInt(a.replace(".csv", "")) - parseInt(b.replace(".csv", "")));

console.log(`📊 ${files.length} ファイルのヘッダーを分析中...\n`);

const analyses: HeaderAnalysis[] = [];

for (const file of files) {
  const filePath = path.join(csvDir, file);
  const content = fs.readFileSync(filePath, "utf8");
  const lines = content.split(/\r?\n/).filter(l => l.trim() !== "");
  if (lines.length === 0) continue;
  
  // CSVヘッダーをパース（カンマ区切り、ダブルクォート対応）
  const headerLine = lines[0];
  const headers: string[] = [];
  let current = "";
  let inQuotes = false;
  
  for (let i = 0; i < headerLine.length; i++) {
    const char = headerLine[i];
    if (char === '"') {
      if (inQuotes && headerLine[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === ',' && !inQuotes) {
      headers.push(current.trim().replace(/^"|"$/g, ""));
      current = "";
    } else {
      current += char;
    }
  }
  if (current) {
    headers.push(current.trim().replace(/^"|"$/g, ""));
  }
  
  // タイプ判定
  let type = "UNKNOWN";
  const headerStr = headers.join("|");
  
  if (headers.includes("法人番号") && headers.includes("会社名") && headers[0] === "法人番号") {
    type = "B";
  } else if (headers.includes("会社名") && !headers.includes("法人番号") && headers[0] === "会社名" && !headers.includes("都道府県")) {
    type = "A";
  } else if (headers.includes("会社名") && headers.includes("郵便番号") && headers.includes("住所") && headers.filter(h => h === "郵便番号" || h === "住所").length === 2) {
    type = "C";
  } else if (headers.includes("会社名") && headers.includes("都道府県") && headers.includes("法人番号") && headers.includes("ID")) {
    type = "D";
  } else if (headers.includes("会社名") && headers.includes("都道府県") && !headers.includes("法人番号")) {
    type = "E";
  } else if (headers.includes("ジャンル") && headers.includes("企業名")) {
    type = "F51";
  } else if (headers[0] === "name" && headers.includes("corporateNumber")) {
    type = "F130";
  } else if (headers.includes("会社名") && headers.includes("都道府県") && headers.includes("法人番号") && headers.includes("種別")) {
    type = "F132";
  }
  
  analyses.push({
    file,
    headers,
    headerCount: headers.length,
    type,
  });
}

// タイプごとにグループ化
const byType: Record<string, HeaderAnalysis[]> = {};
for (const analysis of analyses) {
  if (!byType[analysis.type]) {
    byType[analysis.type] = [];
  }
  byType[analysis.type].push(analysis);
}

// 結果を表示
console.log("========================================");
console.log("📋 タイプ別分類結果");
console.log("========================================\n");

for (const [type, items] of Object.entries(byType)) {
  console.log(`\n【タイプ${type}】${items.length} ファイル`);
  console.log(`ファイル: ${items.map(i => i.file.replace(".csv", "")).join(", ")}`);
  if (items.length > 0) {
    console.log(`\nヘッダー例 (${items[0].file}):`);
    items[0].headers.forEach((h, i) => {
      console.log(`  ${i + 1}. ${h}`);
    });
  }
}

// 各タイプの詳細比較
console.log("\n\n========================================");
console.log("🔍 タイプ別ヘッダー比較");
console.log("========================================\n");

for (const [type, items] of Object.entries(byType)) {
  if (items.length === 0) continue;
  
  console.log(`\n【タイプ${type}】`);
  
  // 全ファイルのヘッダーを比較
  const allHeaders = new Set<string>();
  for (const item of items) {
    item.headers.forEach(h => allHeaders.add(h));
  }
  
  console.log(`全ヘッダー数: ${allHeaders.size}`);
  console.log(`共通ヘッダー:`);
  const commonHeaders = Array.from(allHeaders).filter(h => 
    items.every(item => item.headers.includes(h))
  );
  commonHeaders.forEach(h => console.log(`  - ${h}`));
  
  // 不一致があるファイルをチェック
  const inconsistent: string[] = [];
  for (let i = 1; i < items.length; i++) {
    if (items[i].headers.length !== items[0].headers.length) {
      inconsistent.push(items[i].file);
    }
  }
  if (inconsistent.length > 0) {
    console.log(`⚠️  ヘッダー数が異なるファイル: ${inconsistent.join(", ")}`);
  }
}

// 119.csvの詳細確認
console.log("\n\n========================================");
console.log("🔍 119.csv 詳細分析");
console.log("========================================\n");

const csv119 = analyses.find(a => a.file === "119.csv");
if (csv119) {
  console.log(`タイプ: ${csv119.type}`);
  console.log(`ヘッダー数: ${csv119.headerCount}\n`);
  console.log("ヘッダー一覧:");
  csv119.headers.forEach((h, i) => {
    const mapping = getMappingForHeader(h, csv119.type);
    console.log(`  ${i + 1}. "${h}" → ${mapping || "未マッピング"}`);
  });
  
  // データの1行目を確認
  const filePath = path.join(csvDir, "119.csv");
  const content = fs.readFileSync(filePath, "utf8");
  const lines = content.split(/\r?\n/).filter(l => l.trim() !== "");
  if (lines.length > 1) {
    console.log("\nデータ例（1行目）:");
    const dataLine = lines[1];
    const values = parseCSVLine(dataLine);
    csv119.headers.forEach((h, i) => {
      const val = values[i] || "";
      const displayVal = val.length > 50 ? val.substring(0, 50) + "..." : val;
      console.log(`  ${i + 1}. ${h}: "${displayVal}"`);
    });
  }
}

function getMappingForHeader(header: string, type: string): string | null {
  // タイプDのマッピングを確認
  const mappingD: Record<string, string> = {
    "会社名": "name",
    "都道府県": "prefecture",
    "代表者名": "representativeName",
    "法人番号": "corporateNumber",
    "ID": "metaDescription",
    "取引種別": "tags",
    "SBフラグ": "tags",
    "NDA": "tags",
    "AD": "tags",
    "ステータス": "tags",
    "備考": "salesNotes",
    "URL": "companyUrl",
    "業種1": "industryLarge",
    "業種2": "industryMiddle",
    "業種3": "industrySmall",
    "郵便番号": "postalCode",
    "住所": "address",
    "設立": "established",
    "電話番号(窓口)": "phoneNumber",
    "代表者郵便番号": "representativeRegisteredAddress",
    "代表者住所": "representativeHomeAddress",
    "代表者誕生日": "representativeBirthDate",
    "資本金": "capitalStock",
    "上場": "listing",
    "直近決算年月": "fiscalMonth",
    "直近売上": "revenue",
    "直近利益": "financials",
    "説明": "companyDescription",
    "概要": "overview",
    "仕入れ先": "suppliers",
    "取引先": "clients",
    "取引先銀行": "suppliers",
    "取締役": "executives",
    "株主": "shareholders",
    "社員数": "employeeCount",
    "オフィス数": "officeCount",
    "工場数": "factoryCount",
    "店舗数": "storeCount",
  };
  
  return mappingD[header] || null;
}

function parseCSVLine(line: string): string[] {
  const values: string[] = [];
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
    } else if (char === ',' && !inQuotes) {
      values.push(current.trim().replace(/^"|"$/g, ""));
      current = "";
    } else {
      current += char;
    }
  }
  if (current) {
    values.push(current.trim().replace(/^"|"$/g, ""));
  }
  
  return values;
}

