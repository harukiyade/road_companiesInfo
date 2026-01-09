/**
 * 119.csvのマッピングをテストするスクリプト
 */

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

const MAPPING_D: Record<string, string> = {
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

type CsvRow = Record<string, string>;

const file = "csv/119.csv";
const buf = fs.readFileSync(file);
const records: CsvRow[] = parse(buf, { 
  columns: true, 
  skip_empty_lines: true, 
  relax_quotes: true,
  relax_column_count: true,
  bom: true,
}) as CsvRow[];

console.log(`📄 ${file}: ${records.length} 行\n`);

if (records.length > 0) {
  const firstRow = records[0];
  
  console.log("========================================");
  console.log("📊 ヘッダーとマッピング");
  console.log("========================================\n");
  
  for (const [header, value] of Object.entries(firstRow)) {
    const cleanHeader = header.trim().replace(/^"|"$/g, "");
    const field = MAPPING_D[cleanHeader];
    const displayValue = typeof value === "string" && value.length > 60 
      ? value.substring(0, 60) + "..." 
      : String(value);
    
    console.log(`${cleanHeader.padEnd(20)} → ${(field || "未マッピング").padEnd(25)} : "${displayValue}"`);
  }
  
  console.log("\n========================================");
  console.log("🔍 URL列の確認");
  console.log("========================================\n");
  
  // URL列を直接確認
  if (firstRow["URL"] || firstRow['"URL"'] || firstRow["\"URL\""]) {
    const urlValue = firstRow["URL"] || firstRow['"URL"'] || firstRow["\"URL\""];
    console.log(`URL列の値: "${urlValue}"`);
    console.log(`companyUrlフィールドにマッピングされるか: ${MAPPING_D["URL"] === "companyUrl" ? "✅ YES" : "❌ NO"}`);
    
    // URL検証
    const isUrl = /^https?:\/\//i.test(urlValue) || /\.(co\.jp|com|jp|net|org|io|co|info|biz)/i.test(urlValue) || /^www\./i.test(urlValue);
    console.log(`URL形式として有効か: ${isUrl ? "✅ YES" : "❌ NO"}`);
  } else {
    console.log("❌ URL列が見つかりません");
    console.log("利用可能なヘッダー:");
    Object.keys(firstRow).forEach(h => console.log(`  - ${h}`));
  }
}

