/*
  old127.csvとold128.csvのname列の会社名を、法人番号でマッチングして
  127.csvと128.csvの会社名列に反映するスクリプト
  
  使い方:
    npx ts-node scripts/restore_company_names_from_old127.ts [--dry-run]
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
  // カンマ、ダブルクォート、改行が含まれる場合はダブルクォートで囲む
  if (str.includes(",") || str.includes('"') || str.includes("\n") || str.includes("\r")) {
    // ダブルクォートをエスケープ（""に変換）
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

// old CSVのname列をそのまま返す（正規化処理なし）
function getCompanyNameFromOldCsv(name: string | null | undefined): string | null {
  if (!name) return null;
  let trimmed = String(name).trim();
  
  // ダブルクォートを除去（CSVのエスケープから）
  if (trimmed.startsWith('"') && trimmed.endsWith('"')) {
    trimmed = trimmed.slice(1, -1).replace(/""/g, '"');
  }
  
  trimmed = trimmed.trim();
  if (!trimmed) return null;

  // そのまま返す（正規化処理なし）
  return trimmed;
}

async function processFilePair(oldCsvPath: string, currentCsvPath: string): Promise<void> {
  console.log(`\n📄 処理中: ${oldCsvPath} → ${currentCsvPath}`);

  // old CSVを読み込む
  if (!fs.existsSync(oldCsvPath)) {
    console.error(`  ❌ ファイルが見つかりません: ${oldCsvPath}`);
    return;
  }

  const oldContent = fs.readFileSync(oldCsvPath, "utf8");
  const oldRecords: Record<string, string>[] = parse(oldContent, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_records_with_error: true,
  });

  console.log(`  📄 ${oldCsvPath}: ${oldRecords.length} 件のレコードを読み込みました`);

  // 法人番号をキーに会社名のマップを作成
  const nameMap = new Map<string, string>();
  for (const record of oldRecords) {
    const corporateNumber = record["corporateNumber"] || record["法人番号"];
    const name = record["name"] || record["会社名"];
    
    if (corporateNumber && name && name.trim()) {
      // old CSVのname列をそのまま使用（正規化処理なし）
      const companyName = getCompanyNameFromOldCsv(name);
      if (companyName) {
        // 既に存在する場合は、最初に見つかったものを優先
        if (!nameMap.has(corporateNumber)) {
          nameMap.set(corporateNumber, companyName);
        }
      }
    }
  }

  console.log(`  📊 会社名マップ: ${nameMap.size} 件`);

  // 現在のCSVを読み込む
  if (!fs.existsSync(currentCsvPath)) {
    console.error(`  ❌ ファイルが見つかりません: ${currentCsvPath}`);
    return;
  }

  const currentContent = fs.readFileSync(currentCsvPath, "utf8");
  const currentRecords: Record<string, string>[] = parse(currentContent, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_records_with_error: true,
  });

  console.log(`  📄 ${currentCsvPath}: ${currentRecords.length} 件のレコードを読み込みました`);

  // 会社名の列名を探す
  const companyNameKey = Object.keys(currentRecords[0]).find(
    key => key === "会社名" || key.toLowerCase() === "companyname" || key.toLowerCase() === "company_name"
  );

  if (!companyNameKey) {
    console.error("  ❌ 「会社名」列が見つかりません");
    return;
  }

  console.log(`  🔍 会社名列: "${companyNameKey}"`);

  // 法人番号の列名を探す
  const corporateNumberKey = Object.keys(currentRecords[0]).find(
    key => key === "法人番号" || key.toLowerCase() === "corporatenumber" || key.toLowerCase() === "corporate_number"
  );

  if (!corporateNumberKey) {
    console.error("  ❌ 「法人番号」列が見つかりません");
    return;
  }

  console.log(`  🔍 法人番号列: "${corporateNumberKey}"`);

  let updatedCount = 0;
  let notFoundCount = 0;

  // 各レコードを処理
  for (let i = 0; i < currentRecords.length; i++) {
    const row = currentRecords[i];
    const corporateNumber = row[corporateNumberKey];
    const currentName = row[companyNameKey];

    if (!corporateNumber || !corporateNumber.trim()) {
      continue;
    }

    // old CSVから会社名を取得
    const oldName = nameMap.get(corporateNumber.trim());

    if (oldName && oldName !== currentName) {
      if (DRY_RUN) {
        if (updatedCount < 10) {
          console.log(`  📝 [行 ${i + 2}] 法人番号: ${corporateNumber}`);
          console.log(`     現在: "${currentName}"`);
          console.log(`     復元: "${oldName}"`);
        }
      } else {
        row[companyNameKey] = oldName;
      }
      updatedCount++;
    } else if (!oldName) {
      notFoundCount++;
    }
  }

  // CSVを保存
  if (!DRY_RUN && updatedCount > 0) {
    const headers = Object.keys(currentRecords[0]);
    
    // CSV形式に変換
    const csvLines: string[] = [];
    
    // ヘッダー行
    csvLines.push(headers.map(h => escapeCSVField(h)).join(","));
    
    // データ行
    for (const record of currentRecords) {
      const row = headers.map(h => escapeCSVField(record[h] || ""));
      csvLines.push(row.join(","));
    }
    
    const output = csvLines.join("\n");
    fs.writeFileSync(currentCsvPath, output, "utf8");
    console.log(`  ✅ ファイルを更新しました: ${currentCsvPath}`);
  }

  console.log(`  📊 処理結果:`);
  console.log(`    - 更新: ${updatedCount} 件`);
  console.log(`    - 見つからなかった: ${notFoundCount} 件`);
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log("📊 old CSVから会社名を復元します\n");

  let totalUpdated = 0;
  let totalNotFound = 0;

  for (const pair of FILE_PAIRS) {
    const result = await processFilePair(pair.old, pair.current);
    // processFilePairはvoidを返すので、結果を集計する必要がある
    // 集計はprocessFilePair内で行う
  }

  console.log(`\n✅ 全ファイルの処理完了`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に保存するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});


