/*
  タイプGのCSVファイル（127.csv、128.csv）の会社名を修正するスクリプト
  
  - 「（株）」を「株式会社」に正規化（前株・後株を正しく判定）
  - 切れている社名を修正（URLや他のフィールドから補完）
  - 「日経バリューサーチ」の場合はスキップ
  
  使い方:
    npx ts-node scripts/fix_type_g_csv_company_names.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";
import axios from "axios";
import * as cheerio from "cheerio";

const DRY_RUN = process.argv.includes("--dry-run");
const TYPE_G_FILES = ["csv/127.csv", "csv/128.csv"];

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

// 「（株）」を「株式会社」に正規化（前株・後株を判定）
function normalizeCompanyNameFormat(name: string | null | undefined): string | null {
  if (!name) return null;
  const trimmed = String(name).trim();
  if (!trimmed) return null;

  // 「日経バリューサーチ」の場合はそのまま返す（スキップ対象）
  if (trimmed === "日経バリューサーチ" || trimmed.includes("日経バリューサーチ")) {
    return trimmed;
  }

  // 「（株）」を検出
  if (trimmed.includes("（株）")) {
    // 前株: 「（株）○○」→ 「株式会社○○」
    if (trimmed.startsWith("（株）")) {
      return "株式会社" + trimmed.substring(3);
    }
    // 後株: 「○○（株）」→ 「○○株式会社」
    if (trimmed.endsWith("（株）")) {
      return trimmed.substring(0, trimmed.length - 3) + "株式会社";
    }
    // 中間にある場合も後株として処理
    const index = trimmed.indexOf("（株）");
    if (index > 0) {
      return trimmed.substring(0, index) + "株式会社" + trimmed.substring(index + 3);
    }
  }

  // 既に「株式会社」が含まれている場合はそのまま
  if (trimmed.includes("株式会社")) {
    return trimmed;
  }

  return trimmed;
}

// URLから企業名を取得（Webスクレイピング）
async function extractCompanyNameFromUrl(url: string | null | undefined): Promise<string | null> {
  if (!url) return null;
  
  try {
    // URLを正規化
    let normalizedUrl = url.trim();
    if (!normalizedUrl.startsWith("http://") && !normalizedUrl.startsWith("https://")) {
      normalizedUrl = "https://" + normalizedUrl;
    }

    const urlObj = new URL(normalizedUrl);
    
    // タイムアウトを5秒に設定
    const response = await axios.get(normalizedUrl, {
      timeout: 5000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
      },
      maxRedirects: 5,
      validateStatus: (status) => status < 500
    });

    if (response.status !== 200) {
      return null;
    }

    const $ = cheerio.load(response.data);
    
    // 1. <title>タグから企業名を抽出
    const title = $("title").text().trim();
    if (title) {
      if (title.includes("株式会社") || title.includes("（株）")) {
        const patterns = [
          /([^|｜\-–—\s]{2,30}(?:株式会社|（株）)[^|｜\-–—\s]{0,20})/,
          /([^|｜\-–—\s]+(?:株式会社|（株）)[^|｜\-–—\s]*)/
        ];
        
        for (const pattern of patterns) {
          const match = title.match(pattern);
          if (match && match[1] && match[1].length <= 50) {
            const extracted = normalizeCompanyNameFormat(match[1]);
            if (extracted && extracted.length <= 50 && !extracted.includes("日経バリューサーチ")) {
              return extracted;
            }
          }
        }
      }
    }

    // 2. <h1>タグから企業名を抽出
    const h1 = $("h1").first().text().trim();
    if (h1 && (h1.includes("株式会社") || h1.includes("（株）"))) {
      const extracted = normalizeCompanyNameFormat(h1);
      if (extracted && !extracted.includes("日経バリューサーチ")) {
        return extracted;
      }
    }

    // 3. meta property="og:site_name" から企業名を抽出
    const ogSiteName = $('meta[property="og:site_name"]').attr("content");
    if (ogSiteName && (ogSiteName.includes("株式会社") || ogSiteName.includes("（株）"))) {
      const extracted = normalizeCompanyNameFormat(ogSiteName);
      if (extracted && !extracted.includes("日経バリューサーチ")) {
        return extracted;
      }
    }

    return null;
  } catch (e) {
    return null;
  }
}

// 切れている社名を修正（URLや他のフィールドから補完）
async function fixTruncatedCompanyName(
  currentName: string,
  row: Record<string, string>
): Promise<string | null> {
  // 現在の名前が既に「株式会社」を含んでいる場合はそのまま
  if (currentName.includes("株式会社")) {
    return null;
  }

  // URLから企業名を取得
  const url = row["URL"] || row["url"] || row["companyUrl"] || row["contactUrl"];
  if (url) {
    const extractedName = await extractCompanyNameFromUrl(url);
    if (extractedName && !extractedName.includes("日経バリューサーチ")) {
      return extractedName;
    }
  }

  // overviewやbusinessDescriptionsから企業名を抽出
  const overview = row["overview"] || row["businessDescriptions"];
  if (overview) {
    const lines = overview.split(/\n|。/);
    for (const line of lines) {
      const trimmed = line.trim();
      if (trimmed.length > 2 && trimmed.length < 50) {
        if (trimmed.includes("株式会社") || trimmed.includes("（株）")) {
          const extracted = normalizeCompanyNameFormat(trimmed);
          if (extracted && !extracted.includes("日経バリューサーチ")) {
            return extracted;
          }
        }
      }
    }
  }

  return null;
}

async function processCSVFile(filePath: string): Promise<void> {
  console.log(`\n📄 処理中: ${filePath}`);
  
  if (!fs.existsSync(filePath)) {
    console.error(`❌ ファイルが見つかりません: ${filePath}`);
    return;
  }

  // CSVを読み込む
  const content = fs.readFileSync(filePath, "utf8");
  const records: Record<string, string>[] = parse(content, {
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
    relax_quotes: true,
    skip_records_with_error: true,
  });

  if (records.length === 0) {
    console.log("  ⚠️  CSVに有効なレコードがありません");
    return;
  }

  console.log(`  📊 レコード数: ${records.length} 件`);

  let normalizedCount = 0;
  let fixedCount = 0;
  let skippedCount = 0;

  // 会社名の列名を探す
  const companyNameKey = Object.keys(records[0]).find(
    key => key === "会社名" || key.toLowerCase() === "companyname" || key.toLowerCase() === "company_name"
  );

  if (!companyNameKey) {
    console.error("  ❌ 「会社名」列が見つかりません");
    return;
  }

  console.log(`  🔍 会社名列: "${companyNameKey}"`);

  // 各レコードを処理
  for (let i = 0; i < records.length; i++) {
    const row = records[i];
    const currentName = row[companyNameKey];

    if (!currentName || !currentName.trim()) {
      continue;
    }

    // 「日経バリューサーチ」の場合はスキップ
    if (currentName.trim() === "日経バリューサーチ" || currentName.trim().includes("日経バリューサーチ")) {
      skippedCount++;
      continue;
    }

    let newName: string | null = null;
    let reason = "";

    // 1. 「（株）」を「株式会社」に正規化
    if (currentName.includes("（株）")) {
      newName = normalizeCompanyNameFormat(currentName);
      if (newName && newName !== currentName) {
        reason = "「（株）」正規化";
        normalizedCount++;
      }
    }

    // 2. 切れている社名を修正
    if (!newName || !newName.includes("株式会社")) {
      const fixedName = await fixTruncatedCompanyName(currentName, row);
      if (fixedName && fixedName !== currentName) {
        newName = fixedName;
        reason = "切れている社名を修正";
        fixedCount++;
      }
    }

    // 3. 既に「株式会社」が含まれているが、前後が違う場合を修正
    if (currentName.includes("株式会社")) {
      // 前株と後株の整合性を確認
      const isKabushikiAtStart = currentName.startsWith("株式会社");
      const isKabushikiAtEnd = currentName.endsWith("株式会社");
      const hasKabushikiWithSpaceAtEnd = currentName.endsWith(" 株式会社");
      
      // 「 株式会社」形式（スペース付き）の場合は後株形式に統一
      if (hasKabushikiWithSpaceAtEnd) {
        let nameWithoutKabushiki = currentName.replace(/ 株式会社$/, "").replace(/^株式会社/, "");
        nameWithoutKabushiki = nameWithoutKabushiki.trim();
        newName = nameWithoutKabushiki + "株式会社";
        if (newName !== currentName) {
          reason = "スペース付き株式会社を修正";
          normalizedCount++;
        }
      }
      // 前株と後株が混在している場合は、後株形式に統一（一般的な形式）
      else if (isKabushikiAtStart && isKabushikiAtEnd) {
        // 両方にある場合は、後株形式に統一
        let nameWithoutKabushiki = currentName.replace(/^株式会社/, "").replace(/株式会社$/, "");
        // 先頭と末尾のスペースを削除
        nameWithoutKabushiki = nameWithoutKabushiki.trim();
        newName = nameWithoutKabushiki + "株式会社";
        if (newName !== currentName) {
          reason = "前後株の整合性を修正";
          normalizedCount++;
        }
      } else if (isKabushikiAtStart && !isKabushikiAtEnd) {
        // 前株の場合は後株に変換（一般的な形式）
        let nameWithoutKabushiki = currentName.replace(/^株式会社/, "");
        // 先頭のスペースを削除
        nameWithoutKabushiki = nameWithoutKabushiki.trim();
        newName = nameWithoutKabushiki + "株式会社";
        if (newName !== currentName) {
          reason = "前株を後株に変換";
          normalizedCount++;
        }
      }
    }

    // 更新
    if (newName && newName !== currentName) {
      if (DRY_RUN) {
        console.log(`  📝 [行 ${i + 2}] ${reason}: "${currentName}" → "${newName}"`);
      } else {
        row[companyNameKey] = newName;
      }
    }

    // 進捗表示
    if ((i + 1) % 100 === 0) {
      console.log(`  📊 処理中: ${i + 1} / ${records.length} 件`);
    }
  }

  // CSVを保存
  if (!DRY_RUN) {
    const headers = Object.keys(records[0]);
    
    // CSV形式に変換
    const csvLines: string[] = [];
    
    // ヘッダー行
    csvLines.push(headers.map(h => escapeCSVField(h)).join(","));
    
    // データ行
    for (const record of records) {
      const row = headers.map(h => escapeCSVField(record[h] || ""));
      csvLines.push(row.join(","));
    }
    
    const output = csvLines.join("\n");
    fs.writeFileSync(filePath, output, "utf8");
    console.log(`  ✅ ファイルを更新しました: ${filePath}`);
  }

  console.log(`  📊 処理結果:`);
  console.log(`    - 正規化: ${normalizedCount} 件`);
  console.log(`    - 切れている社名を修正: ${fixedCount} 件`);
  console.log(`    - スキップ（日経バリューサーチ）: ${skippedCount} 件`);
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log("📊 タイプGのCSVファイルの会社名修正処理を開始します\n");

  for (const filePath of TYPE_G_FILES) {
    await processCSVFile(filePath);
  }

  console.log(`\n✅ 処理完了`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に保存するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

