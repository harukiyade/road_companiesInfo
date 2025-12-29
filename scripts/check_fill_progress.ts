/* eslint-disable no-console */

/**
 * scripts/check_fill_progress.ts
 * 
 * 目的: fill_null_fields_from_csv_enhanced.tsの実行状況を確認し、
 *       更新されたドキュメントIDを出力
 */

import * as fs from "fs";
import * as path from "path";

/**
 * CSVファイルから更新されたドキュメントIDを抽出
 */
function extractUpdatedCompanyIds(csvPath: string): string[] {
  const companyIds: string[] = [];
  
  try {
    const content = fs.readFileSync(csvPath, "utf8");
    const lines = content.split("\n");
    
    // ヘッダーを確認
    if (lines.length < 2) return companyIds;
    
    const header = lines[0].trim();
    const headers = header.split(",");
    const foundValueIndex = headers.indexOf("foundValue");
    
    if (foundValueIndex === -1) return companyIds;
    
    // データ行を処理
    for (let i = 1; i < lines.length; i++) {
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
      const foundValue = parts[foundValueIndex]?.trim();
      
      // foundValueが存在し、空でない場合
      if (companyId && foundValue && foundValue !== "" && foundValue !== "null") {
        // 値が引用符で囲まれている場合は除去
        const cleanValue = foundValue.replace(/^"|"$/g, "");
        if (cleanValue && cleanValue !== "") {
          companyIds.push(companyId);
        }
      }
    }
  } catch (error) {
    console.warn(`[${path.basename(csvPath)}] 読み込みエラー:`, (error as any)?.message);
  }
  
  return companyIds;
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

  // CSVファイル一覧を取得
  const files = fs.readdirSync(csvDir)
    .filter(file => file.endsWith(".csv") && file.startsWith("null_fields_detailed_"))
    .sort();

  if (files.length === 0) {
    console.error(`❌ CSVファイルが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  console.log(`📁 ${files.length} 個のCSVファイルを分析中...\n`);

  const allUpdatedCompanyIds = new Set<string>();
  const fileStats: { [key: string]: { total: number; updated: number; companyIds: string[] } } = {};

  for (const file of files) {
    const csvPath = path.join(csvDir, file);
    const companyIds = extractUpdatedCompanyIds(csvPath);
    
    // ファイルの総行数を取得
    const content = fs.readFileSync(csvPath, "utf8");
    const lines = content.split("\n").filter(line => line.trim());
    const totalRows = lines.length - 1; // ヘッダーを除く
    
    fileStats[file] = {
      total: totalRows,
      updated: companyIds.length,
      companyIds: companyIds,
    };
    
    companyIds.forEach(id => allUpdatedCompanyIds.add(id));
  }

  // 統計情報を表示
  console.log("📊 ファイル別更新状況:");
  let totalRows = 0;
  let totalUpdated = 0;
  
  for (const [file, stats] of Object.entries(fileStats)) {
    totalRows += stats.total;
    totalUpdated += stats.updated;
    if (stats.updated > 0) {
      const percentage = stats.total > 0 ? ((stats.updated / stats.total) * 100).toFixed(2) : "0.00";
      console.log(`  ${file}: ${stats.updated}/${stats.total} 件 (${percentage}%)`);
    }
  }

  console.log(`\n✅ 全体統計:`);
  console.log(`  総行数: ${totalRows.toLocaleString()} 件`);
  console.log(`  更新済み: ${totalUpdated.toLocaleString()} 件`);
  console.log(`  更新率: ${totalRows > 0 ? ((totalUpdated / totalRows) * 100).toFixed(2) : "0.00"}%`);
  console.log(`  更新された企業数: ${allUpdatedCompanyIds.size.toLocaleString()} 社`);

  // 更新されたドキュメントIDをファイルに出力
  const outputPath = path.join(process.cwd(), "updated_company_ids.txt");
  const sortedIds = Array.from(allUpdatedCompanyIds).sort((a, b) => {
    const numA = parseInt(a, 10);
    const numB = parseInt(b, 10);
    if (!isNaN(numA) && !isNaN(numB)) {
      return numA - numB;
    }
    return a.localeCompare(b);
  });
  
  fs.writeFileSync(outputPath, sortedIds.join("\n"), "utf8");
  console.log(`\n📄 更新されたドキュメントIDを出力しました: ${outputPath}`);
  console.log(`   総数: ${sortedIds.length} 件`);

  // 最初の10件と最後の10件を表示
  if (sortedIds.length > 0) {
    console.log(`\n📋 更新されたドキュメントID（最初の10件）:`);
    sortedIds.slice(0, 10).forEach((id, index) => {
      console.log(`  ${index + 1}. ${id}`);
    });
    
    if (sortedIds.length > 10) {
      console.log(`\n📋 更新されたドキュメントID（最後の10件）:`);
      sortedIds.slice(-10).forEach((id, index) => {
        console.log(`  ${sortedIds.length - 9 + index}. ${id}`);
      });
    }
  }
}

main().catch((error) => {
  console.error("エラー:", error);
  process.exit(1);
});

