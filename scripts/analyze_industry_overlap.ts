/* eslint-disable no-console */

/**
 * scripts/analyze_industry_overlap.ts
 *
 * ✅ 目的
 * - industryLarge, industryMiddle, industrySmall, industryDetailの各フィールド間で
 *   同じ値が存在するか確認
 * - 統一すべき値を特定
 */

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

interface IndustryValue {
  field: string;
  value: string;
  normalizedValue: string;
  count: number;
  originalValues: string[];
}

function normalizeText(text: string): string {
  if (!text || typeof text !== "string") {
    return "";
  }

  return text
    .trim()
    .replace(/[（(].*?[）)]/g, "") // 括弧内を削除
    .replace(/[：:].*$/, "") // コロン以降を削除
    .replace(/\s+/g, "") // 空白を削除
    .replace(/[０-９]/g, (s) => String.fromCharCode(s.charCodeAt(0) - 0xfee0)) // 全角数字→半角
    .replace(/[Ａ-Ｚａ-ｚ]/g, (s) => String.fromCharCode(s.charCodeAt(0) - 0xfee0)) // 全角英字→半角
    .normalize("NFKC"); // NFKC正規化
}

async function analyzeIndustryOverlap() {
  try {
    const csvPath = path.join(process.cwd(), "out", "industry_values_unified_2026-01-05T10-37-37-304Z.csv");
    
    if (!fs.existsSync(csvPath)) {
      console.error(`❌ エラー: CSVファイルが見つかりません: ${csvPath}`);
      process.exit(1);
    }

    console.log("CSVファイルを読み込み中...");
    const csvContent = fs.readFileSync(csvPath, "utf-8");
    const records = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }) as Array<{
      フィールド: string;
      統一後の値: string;
      正規化値: string;
      出現回数: string;
      統一前の値一覧: string;
    }>;

    // 各フィールドの値を収集
    const largeValues = new Map<string, IndustryValue>();
    const middleValues = new Map<string, IndustryValue>();
    const smallValues = new Map<string, IndustryValue>();
    const detailValues = new Map<string, IndustryValue>();

    for (const record of records) {
      const field = record.フィールド;
      const value = record.統一後の値;
      const normalizedValue = record.正規化値;
      const count = parseInt(record.出現回数, 10);
      const originalValues = record.統一前の値一覧.split(" | ").filter((v) => v.trim());

      const industryValue: IndustryValue = {
        field,
        value,
        normalizedValue,
        count,
        originalValues,
      };

      if (field === "industryLarge") {
        largeValues.set(value, industryValue);
      } else if (field === "industryMiddle") {
        middleValues.set(value, industryValue);
      } else if (field === "industrySmall") {
        smallValues.set(value, industryValue);
      } else if (field === "industryDetail") {
        detailValues.set(value, industryValue);
      }
    }

    console.log(`\n📊 各フィールドの種類数:`);
    console.log(`  industryLarge: ${largeValues.size} 種類`);
    console.log(`  industryMiddle: ${middleValues.size} 種類`);
    console.log(`  industrySmall: ${smallValues.size} 種類`);
    console.log(`  industryDetail: ${detailValues.size} 種類`);

    // 重複を検出（正規化値で比較）
    console.log(`\n🔍 重複検出中...`);

    const overlaps: Array<{
      value: string;
      normalizedValue: string;
      fields: string[];
      counts: { [field: string]: number };
    }> = [];

    // 全ての正規化値を収集
    const normalizedToFields = new Map<string, Map<string, IndustryValue>>();

    for (const [value, iv] of largeValues.entries()) {
      if (!normalizedToFields.has(iv.normalizedValue)) {
        normalizedToFields.set(iv.normalizedValue, new Map());
      }
      normalizedToFields.get(iv.normalizedValue)!.set("industryLarge", iv);
    }

    for (const [value, iv] of middleValues.entries()) {
      if (!normalizedToFields.has(iv.normalizedValue)) {
        normalizedToFields.set(iv.normalizedValue, new Map());
      }
      normalizedToFields.get(iv.normalizedValue)!.set("industryMiddle", iv);
    }

    for (const [value, iv] of smallValues.entries()) {
      if (!normalizedToFields.has(iv.normalizedValue)) {
        normalizedToFields.set(iv.normalizedValue, new Map());
      }
      normalizedToFields.get(iv.normalizedValue)!.set("industrySmall", iv);
    }

    for (const [value, iv] of detailValues.entries()) {
      if (!normalizedToFields.has(iv.normalizedValue)) {
        normalizedToFields.set(iv.normalizedValue, new Map());
      }
      normalizedToFields.get(iv.normalizedValue)!.set("industryDetail", iv);
    }

    // 複数のフィールドに存在する正規化値を検出
    for (const [normalizedValue, fieldsMap] of normalizedToFields.entries()) {
      if (fieldsMap.size > 1) {
        const fields = Array.from(fieldsMap.keys());
        const counts: { [field: string]: number } = {};
        let representativeValue = "";
        let maxCount = 0;

        for (const [field, iv] of fieldsMap.entries()) {
          counts[field] = iv.count;
          // 出現回数の多い方を代表値に
          if (iv.count > maxCount) {
            maxCount = iv.count;
            representativeValue = iv.value;
          }
        }

        overlaps.push({
          value: representativeValue,
          normalizedValue,
          fields,
          counts,
        });
      }
    }

    // 出現回数の多い順にソート
    overlaps.sort((a, b) => {
      const totalA = Object.values(a.counts).reduce((sum, count) => sum + count, 0);
      const totalB = Object.values(b.counts).reduce((sum, count) => sum + count, 0);
      return totalB - totalA;
    });

    console.log(`\n✅ 重複検出完了: ${overlaps.length} 件`);

    // 結果をCSVに出力
    const outDir = path.join(process.cwd(), "out");
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const outputPath = path.join(outDir, `industry_overlap_analysis_${timestamp}.csv`);

    const outputStream = fs.createWriteStream(outputPath, { encoding: "utf8", flags: "w" });
    outputStream.write("統一後の値,正規化値,出現フィールド,industryLarge件数,industryMiddle件数,industrySmall件数,industryDetail件数,合計件数\n");

    for (const overlap of overlaps) {
      const fieldsStr = overlap.fields.join(" | ");
      const largeCount = overlap.counts["industryLarge"] || 0;
      const middleCount = overlap.counts["industryMiddle"] || 0;
      const smallCount = overlap.counts["industrySmall"] || 0;
      const detailCount = overlap.counts["industryDetail"] || 0;
      const totalCount = largeCount + middleCount + smallCount + detailCount;

      outputStream.write(
        `"${overlap.value.replace(/"/g, '""')}","${overlap.normalizedValue.replace(/"/g, '""')}","${fieldsStr}",${largeCount},${middleCount},${smallCount},${detailCount},${totalCount}\n`
      );
    }

    outputStream.end();

    console.log(`\n📁 出力ファイル: ${outputPath}`);

    // トップ20を表示
    console.log(`\n📈 トップ20（合計出現回数順）:`);
    overlaps.slice(0, 20).forEach((overlap, index) => {
      const totalCount = Object.values(overlap.counts).reduce((sum, count) => sum + count, 0);
      console.log(`  ${index + 1}. "${overlap.value}" (${overlap.fields.join(", ")}) - 合計: ${totalCount}件`);
      for (const [field, count] of Object.entries(overlap.counts)) {
        console.log(`      ${field}: ${count}件`);
      }
    });

    console.log(`\n✅ 分析完了`);

  } catch (error: any) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error("❌ 重大エラー:", errorMsg);
    console.error(error);
    process.exit(1);
  }
}

analyzeIndustryOverlap()
  .then(() => {
    console.log("\n処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
