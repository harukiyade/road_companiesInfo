/* eslint-disable no-console */

/**
 * scripts/check_updated_fields.ts
 * 
 * 目的: 更新されたドキュメントIDとフィールドを軽量に確認
 * 
 * 実行オプション:
 * - SAMPLE_SIZE=100: サンプルファイル数（デフォルト: 100）
 * - START_FILE=1: 開始ファイル番号
 * - END_FILE=100: 終了ファイル番号
 * - DETAILED=true: 詳細表示（各ドキュメントIDとフィールドの対応を表示）
 */

import * as fs from "fs";
import * as path from "path";

/**
 * CSVファイルから更新されたドキュメントIDとフィールドを抽出（軽量版）
 */
function extractUpdatedFields(csvPath: string): Array<{ companyId: string; fieldName: string; value: string }> {
  const results: Array<{ companyId: string; fieldName: string; value: string }> = [];
  
  try {
    const content = fs.readFileSync(csvPath, "utf8");
    const lines = content.split("\n");
    
    if (lines.length < 2) return results;
    
    const header = lines[0].trim();
    const headers = header.split(",");
    const foundValueIndex = headers.indexOf("foundValue");
    
    if (foundValueIndex === -1) return results;
    
    // データ行を処理（最大1000行まで）
    const maxLines = Math.min(lines.length, 1001);
    for (let i = 1; i < maxLines; i++) {
      const line = lines[i].trim();
      if (!line) continue;
      
      // CSVのパース（簡易版）
      const parts: string[] = [];
      let current = "";
      let inQuotes = false;
      
      for (let j = 0; j < line.length; j++) {
        const char = line[j];
        if (char === '"') {
          inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
          parts.push(current);
          current = "";
        } else {
          current += char;
        }
      }
      parts.push(current);
      
      if (parts.length <= foundValueIndex) continue;
      
      const companyId = parts[0]?.trim();
      const fieldName = parts[2]?.trim();
      const foundValue = parts[foundValueIndex]?.trim();
      
      if (companyId && fieldName && foundValue && foundValue !== "" && foundValue !== "null") {
        const cleanValue = foundValue.replace(/^"|"$/g, "");
        if (cleanValue && cleanValue !== "") {
          results.push({
            companyId,
            fieldName,
            value: cleanValue.substring(0, 50), // 値は最大50文字まで
          });
        }
      }
    }
  } catch (error) {
    // エラーは無視
  }
  
  return results;
}

/**
 * メイン処理
 */
async function main() {
  const csvDir = path.join(process.cwd(), "null_fields_detailed");
  
  if (!fs.existsSync(csvDir)) {
    console.error(`❌ ディレクトリが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  // 実行パラメータ
  const SAMPLE_SIZE = parseInt(process.env.SAMPLE_SIZE || "100", 10);
  const START_FILE = parseInt(process.env.START_FILE || "1", 10);
  const END_FILE = parseInt(process.env.END_FILE || "10000", 10);
  const DETAILED = process.env.DETAILED === "true";

  // CSVファイル一覧を取得
  const allFiles = fs.readdirSync(csvDir)
    .filter(file => file.endsWith(".csv") && file.startsWith("null_fields_detailed_"))
    .sort();

  if (allFiles.length === 0) {
    console.error(`❌ CSVファイルが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  // ファイル番号でフィルタリング
  let filteredFiles = allFiles.filter(file => {
    const match = file.match(/null_fields_detailed_(\d+)\.csv/);
    if (!match) return false;
    const fileNum = parseInt(match[1], 10);
    return fileNum >= START_FILE && fileNum <= END_FILE;
  });

  // サンプリング
  if (filteredFiles.length > SAMPLE_SIZE) {
    console.log(`📊 ${filteredFiles.length} 個のファイルから ${SAMPLE_SIZE} 個をサンプリングします`);
    const step = Math.floor(filteredFiles.length / SAMPLE_SIZE);
    filteredFiles = filteredFiles.filter((_, index) => index % step === 0).slice(0, SAMPLE_SIZE);
  }

  console.log(`📁 ${filteredFiles.length} 個のCSVファイルを分析中...\n`);

  const companyFieldMap: { [key: string]: { [fieldName: string]: string } } = {};
  const fieldStats: { [fieldName: string]: number } = {};
  let totalUpdated = 0;

  // 進捗表示付きで処理
  for (let i = 0; i < filteredFiles.length; i++) {
    const file = filteredFiles[i];
    if ((i + 1) % 10 === 0 || i === 0) {
      process.stdout.write(`\r  処理中: ${i + 1}/${filteredFiles.length} ファイル`);
    }

    const csvPath = path.join(csvDir, file);
    const updatedFields = extractUpdatedFields(csvPath);
    
    for (const { companyId, fieldName, value } of updatedFields) {
      if (!companyFieldMap[companyId]) {
        companyFieldMap[companyId] = {};
      }
      companyFieldMap[companyId][fieldName] = value;
      
      if (!fieldStats[fieldName]) {
        fieldStats[fieldName] = 0;
      }
      fieldStats[fieldName]++;
      totalUpdated++;
    }
  }
  
  process.stdout.write(`\r  処理完了: ${filteredFiles.length}/${filteredFiles.length} ファイル\n\n`);

  // 統計情報を表示
  console.log("📊 更新統計:");
  console.log(`  更新されたドキュメント数: ${Object.keys(companyFieldMap).length.toLocaleString()} 社`);
  console.log(`  更新されたフィールド数: ${totalUpdated.toLocaleString()} 件`);

  console.log(`\n📋 フィールド別更新件数:`);
  const sortedFields = Object.entries(fieldStats)
    .sort((a, b) => b[1] - a[1]);
  
  for (const [fieldName, count] of sortedFields) {
    console.log(`  ${fieldName}: ${count.toLocaleString()} 件`);
  }

  // 詳細表示
  if (DETAILED) {
    console.log(`\n📋 更新されたドキュメントIDとフィールド（最初の50件）:`);
    const companyIds = Object.keys(companyFieldMap).slice(0, 50);
    
    for (const companyId of companyIds) {
      const fields = companyFieldMap[companyId];
      const fieldList = Object.keys(fields).join(", ");
      console.log(`  ${companyId}: ${fieldList}`);
    }
    
    if (Object.keys(companyFieldMap).length > 50) {
      console.log(`  ... (他 ${Object.keys(companyFieldMap).length - 50} 社)`);
    }
  } else {
    console.log(`\n💡 詳細表示する場合: DETAILED=true を設定してください`);
  }

  // 更新されたドキュメントIDをファイルに出力
  const outputPath = path.join(process.cwd(), "updated_company_ids_sample.txt");
  const sortedIds = Object.keys(companyFieldMap).sort((a, b) => {
    const numA = parseInt(a, 10);
    const numB = parseInt(b, 10);
    if (!isNaN(numA) && !isNaN(numB)) {
      return numA - numB;
    }
    return a.localeCompare(b);
  });
  
  fs.writeFileSync(outputPath, sortedIds.join("\n"), "utf8");
  console.log(`\n📄 更新されたドキュメントIDを出力しました: ${outputPath}`);
  console.log(`   総数: ${sortedIds.length} 件（サンプル）`);

  // フィールド別のドキュメントIDを出力
  const fieldOutputPath = path.join(process.cwd(), "updated_fields_detail.txt");
  const fieldDetail: string[] = [];
  
  for (const [companyId, fields] of Object.entries(companyFieldMap)) {
    for (const [fieldName, value] of Object.entries(fields)) {
      fieldDetail.push(`${companyId}\t${fieldName}\t${value}`);
    }
  }
  
  fs.writeFileSync(fieldOutputPath, fieldDetail.join("\n"), "utf8");
  console.log(`📄 フィールド別詳細を出力しました: ${fieldOutputPath}`);
  console.log(`   総数: ${fieldDetail.length} 件（サンプル）`);
}

main().catch((error) => {
  console.error("エラー:", error);
  process.exit(1);
});

