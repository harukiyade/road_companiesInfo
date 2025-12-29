/* eslint-disable no-console */

/**
 * scripts/extract_target_company_ids.ts
 * 
 * 目的: null_fields_detailed配下のCSVファイルから、
 *       上から1000件と下から1000件のドキュメントIDを抽出してファイルに出力
 */

import * as fs from "fs";
import * as path from "path";

/**
 * CSVファイルからドキュメントIDを抽出
 */
function extractCompanyIdsFromCsv(csvPath: string): Set<string> {
  const companyIds = new Set<string>();
  
  try {
    const content = fs.readFileSync(csvPath, "utf8");
    const lines = content.split("\n");
    
    // ヘッダーをスキップ
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
      
      if (parts.length > 0) {
        const companyId = parts[0]?.trim();
        if (companyId) {
          companyIds.add(companyId);
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

  const allCompanyIds = new Set<string>();

  // 全CSVファイルからドキュメントIDを抽出
  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    if ((i + 1) % 1000 === 0 || i === 0) {
      process.stdout.write(`\r  処理中: ${i + 1}/${files.length} ファイル`);
    }

    const csvPath = path.join(csvDir, file);
    const companyIds = extractCompanyIdsFromCsv(csvPath);
    
    companyIds.forEach(id => allCompanyIds.add(id));
  }
  
  process.stdout.write(`\r  処理完了: ${files.length}/${files.length} ファイル\n\n`);

  // ドキュメントIDをソート
  const sortedIds = Array.from(allCompanyIds).sort((a, b) => {
    const numA = parseInt(a, 10);
    const numB = parseInt(b, 10);
    if (!isNaN(numA) && !isNaN(numB)) {
      return numA - numB;
    }
    return a.localeCompare(b);
  });

  console.log(`📊 統計:`);
  console.log(`  総ドキュメント数: ${sortedIds.length.toLocaleString()} 件`);
  console.log(`  総CSVファイル数: ${files.length.toLocaleString()} 個`);

  // 上から1000件を抽出
  const top1000 = sortedIds.slice(0, 1000);
  const top1000Path = path.join(process.cwd(), "target_company_ids_top1000.txt");
  fs.writeFileSync(top1000Path, top1000.join("\n"), "utf8");
  console.log(`\n✅ 上から1000件を出力しました: ${top1000Path}`);

  // 下から1000件を抽出
  const bottom1000 = sortedIds.slice(-1000);
  const bottom1000Path = path.join(process.cwd(), "target_company_ids_bottom1000.txt");
  fs.writeFileSync(bottom1000Path, bottom1000.join("\n"), "utf8");
  console.log(`✅ 下から1000件を出力しました: ${bottom1000Path}`);

  // 最初の10件と最後の10件を表示
  console.log(`\n📋 上から10件:`);
  top1000.slice(0, 10).forEach((id, index) => {
    console.log(`  ${index + 1}. ${id}`);
  });

  console.log(`\n📋 下から10件:`);
  bottom1000.slice(-10).forEach((id, index) => {
    console.log(`  ${sortedIds.length - 9 + index}. ${id}`);
  });
}

main().catch((error) => {
  console.error("エラー:", error);
  process.exit(1);
});

