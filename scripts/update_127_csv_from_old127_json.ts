/*
  old127.csv内のJSONフィールドから情報を抽出して、127.csvに反映するスクリプト
  
  - summaryJson、basicJson、orgJsonなどのJSONフィールドから情報を抽出
  - 法人番号でマッチングして127.csvの対応する列を更新
  
  使い方:
    npx ts-node scripts/update_127_csv_from_old127_json.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

const DRY_RUN = process.argv.includes("--dry-run");
const FILE_PAIRS = [
  { old: "csv/old127.csv", current: "csv/127.csv" },
  { old: "csv/old128.csv", current: "csv/128.csv" },
];

// CSVフィールドをエスケープ
function escapeCSVField(value: string | undefined): string {
  if (!value) return "";
  const str = String(value);
  if (str.includes(",") || str.includes('"') || str.includes("\n") || str.includes("\r")) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

// JSON文字列をパース
function parseJson(jsonStr: string | null | undefined): any {
  if (!jsonStr || typeof jsonStr !== "string") return null;
  try {
    return JSON.parse(jsonStr);
  } catch {
    return null;
  }
}

// summaryJsonから情報を抽出
function extractFromSummaryJson(summaryJson: any): Record<string, string> {
  const result: Record<string, string> = {};
  
  if (!summaryJson || typeof summaryJson !== "object") return result;
  
  const kv = summaryJson.kv || {};
  
  // 英文名
  if (kv["英文名"]) {
    result["会社名（英語）"] = String(kv["英文名"]).trim();
  }
  
  // 業種
  if (kv["業種"]) {
    result["業種"] = String(kv["業種"]).trim();
  }
  
  // 本社住所
  if (kv["本社住所"]) {
    let address = String(kv["本社住所"]).trim();
    // 郵便番号を除去（〒 041-0812 のような形式）
    address = address.replace(/^〒\s*\d+[-\s]\d+\s*/, "");
    result["住所"] = address;
  }
  
  // 資本金（数値部分のみ抽出、百万円単位を考慮）
  if (kv["資本金"]) {
    const capitalStr = String(kv["資本金"]);
    // 「50(百万円)」のような形式から数値を抽出し、百万円単位を考慮
    const match = capitalStr.match(/(\d+(?:,\d+)*)\s*\(百万円\)/);
    if (match) {
      const millions = parseInt(match[1].replace(/,/g, ""), 10);
      result["資本金"] = String(millions * 1000000); // 百万円を円に変換
    } else {
      // 通常の数値形式の場合
      const numMatch = capitalStr.match(/(\d+(?:,\d+)*)/);
      if (numMatch) {
        result["資本金"] = numMatch[1].replace(/,/g, "");
      }
    }
  }
  
  // 従業員数（数値部分のみ抽出、「人」の前の数値を取得）
  if (kv["従業員数"]) {
    const employeeStr = String(kv["従業員数"]);
    // 「55人（単独）」のような形式から数値を抽出
    const match = employeeStr.match(/(\d+)\s*人/);
    if (match) {
      result["従業員数"] = match[1];
    } else {
      // 「人」がない場合も数値を抽出（ただし、年号と混同しないように4桁以上は除外）
      const numMatch = employeeStr.match(/^(\d{1,3})/);
      if (numMatch && parseInt(numMatch[1], 10) < 10000) {
        result["従業員数"] = numMatch[1];
      }
    }
  }
  
  // 設立年月日
  if (kv["設立年月日"]) {
    result["設立"] = String(kv["設立年月日"]).trim();
  }
  
  // 決算月
  if (kv["決算月"]) {
    result["決算月"] = String(kv["決算月"]).trim();
  }
  
  // 所属団体
  if (kv["所属団体"]) {
    result["affiliations"] = String(kv["所属団体"]).trim();
  }
  
  return result;
}

// basicJsonから情報を抽出
function extractFromBasicJson(basicJson: any): Record<string, string> {
  const result: Record<string, string> = {};
  
  if (!basicJson || typeof basicJson !== "object") return result;
  
  // basicJsonの構造に応じて情報を抽出
  // 必要に応じて追加
  
  return result;
}

// orgJsonから情報を抽出（代表者名など）
function extractFromOrgJson(orgJson: any): Record<string, string> {
  const result: Record<string, string> = {};
  
  if (!orgJson || typeof orgJson !== "object") return result;
  
  // orgJsonの構造に応じて情報を抽出
  // 必要に応じて追加
  
  return result;
}

async function processFilePair(oldCsvFile: string, targetCsvFile: string): Promise<number> {
  console.log(`\n📄 処理中: ${oldCsvFile} → ${targetCsvFile}`);

  // old CSVを読み込む
  if (!fs.existsSync(oldCsvFile)) {
    console.error(`  ❌ ファイルが見つかりません: ${oldCsvFile}`);
    return 0;
  }

  const oldContent = fs.readFileSync(oldCsvFile, "utf8");
  const oldRecords: Record<string, string>[] = parse(oldContent, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_records_with_error: true,
  });

  console.log(`  📄 ${oldCsvFile}: ${oldRecords.length} 件のレコードを読み込みました`);

  // 法人番号をキーに情報マップを作成
  const infoMap = new Map<string, Record<string, string>>();
  
  for (const record of oldRecords) {
    const corporateNumber = record["corporateNumber"]?.trim();
    if (!corporateNumber) continue;

    const extracted: Record<string, string> = {};

    // summaryJsonから抽出
    const summaryJson = parseJson(record["summaryJson"]);
    const summaryInfo = extractFromSummaryJson(summaryJson);
    Object.assign(extracted, summaryInfo);

    // basicJsonから抽出
    const basicJson = parseJson(record["basicJson"]);
    const basicInfo = extractFromBasicJson(basicJson);
    Object.assign(extracted, basicInfo);

    // orgJsonから抽出
    const orgJson = parseJson(record["orgJson"]);
    const orgInfo = extractFromOrgJson(orgJson);
    Object.assign(extracted, orgInfo);

    // その他のフィールド（URL、contactUrl、overview、history、banks）
    if (record["url"]) {
      extracted["URL"] = String(record["url"]).trim();
    }
    // contactUrlは汎用的なURL（日経バリューサーチのヘルプページなど）はスキップ
    if (record["contactUrl"]) {
      const contactUrl = String(record["contactUrl"]).trim();
      // 汎用的なURLでない場合のみ追加
      if (!contactUrl.includes("valuesearch.nikkei.com/vs.assets/help")) {
        extracted["contactUrl"] = contactUrl;
      }
    }
    if (record["overview"]) {
      extracted["overview"] = String(record["overview"]).trim();
    }
    if (record["history"]) {
      extracted["history"] = String(record["history"]).trim();
    }
    if (record["banks"]) {
      extracted["銀行"] = String(record["banks"]).trim();
    }
    if (record["businessDescriptions"]) {
      extracted["businessDescriptions"] = String(record["businessDescriptions"]).trim();
    }
    // overviewがbusinessDescriptionsに反映されていない場合、overviewをbusinessDescriptionsとして使用
    if (record["overview"] && !record["businessDescriptions"]) {
      const overview = String(record["overview"]).trim();
      // 「◆」を除去してbusinessDescriptionsとして使用
      const cleanedOverview = overview.replace(/◆/g, "").trim();
      if (cleanedOverview) {
        extracted["businessDescriptions"] = cleanedOverview;
      }
    }

    if (Object.keys(extracted).length > 0) {
      infoMap.set(corporateNumber, extracted);
    }
  }

  console.log(`  📊 情報マップ: ${infoMap.size} 件`);

  // target CSVを読み込む
  if (!fs.existsSync(targetCsvFile)) {
    console.error(`  ❌ ファイルが見つかりません: ${targetCsvFile}`);
    return 0;
  }

  const targetContent = fs.readFileSync(targetCsvFile, "utf8");
  const targetRecords: Record<string, string>[] = parse(targetContent, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_records_with_error: true,
  });

  console.log(`  📄 ${targetCsvFile}: ${targetRecords.length} 件のレコードを読み込みました`);

  const corporateNumberKey = Object.keys(targetRecords[0]).find(
    key => key === "法人番号" || key.toLowerCase() === "corporatenumber" || key.toLowerCase() === "corporate_number"
  );

  if (!corporateNumberKey) {
    console.error("  ❌ 「法人番号」列が見つかりません");
    return 0;
  }

  let updatedCount = 0;
  const updateDetails: Array<{ corpNum: string; field: string; old: string; new: string }> = [];

  // 各レコードを処理
  for (let i = 0; i < targetRecords.length; i++) {
    const row = targetRecords[i];
    const corporateNumber = row[corporateNumberKey]?.trim();

    if (!corporateNumber || !infoMap.has(corporateNumber)) {
      continue;
    }

    const extractedInfo = infoMap.get(corporateNumber)!;
    let rowUpdated = false;

    // 抽出した情報でCSVの列を更新（既存の値が空の場合のみ）
    for (const [field, value] of Object.entries(extractedInfo)) {
      const currentValue = row[field]?.trim() || "";
      const newValue = value.trim();

      // 既存の値が空の場合のみ更新
      if (!currentValue && newValue) {
        if (DRY_RUN && updatedCount < 50) {
          updateDetails.push({
            corpNum: corporateNumber,
            field,
            old: "(空)",
            new: newValue,
          });
        } else {
          row[field] = newValue;
        }
        rowUpdated = true;
      }
    }

    if (rowUpdated) {
      updatedCount++;
    }
  }

  if (DRY_RUN && updateDetails.length > 0) {
    console.log("  📝 更新予定の内容（最初の50件）:");
    for (const detail of updateDetails.slice(0, 50)) {
      console.log(`    [${detail.corpNum}] ${detail.field}: "${detail.old}" → "${detail.new}"`);
    }
  }

  // CSVを保存
  if (!DRY_RUN && updatedCount > 0) {
    const headers = Object.keys(targetRecords[0]);
    const csvLines: string[] = [];
    
    // ヘッダー行
    csvLines.push(headers.map(h => escapeCSVField(h)).join(","));
    
    // データ行
    for (const record of targetRecords) {
      const row = headers.map(h => escapeCSVField(record[h] || ""));
      csvLines.push(row.join(","));
    }
    
    const output = csvLines.join("\n");
    fs.writeFileSync(targetCsvFile, output, "utf8");
    console.log(`  ✅ ファイルを更新しました: ${targetCsvFile}`);
  }

  console.log(`  📊 処理結果: 更新 ${updatedCount} 件`);

  return updatedCount;
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log("📊 old CSVのJSONフィールドから情報を抽出してCSVに反映します\n");

  let totalUpdated = 0;

  for (const pair of FILE_PAIRS) {
    const updated = await processFilePair(pair.old, pair.current);
    totalUpdated += updated;
  }

  console.log(`\n✅ 全ファイルの処理完了`);
  console.log(`  - 合計更新: ${totalUpdated} 件`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に保存するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

