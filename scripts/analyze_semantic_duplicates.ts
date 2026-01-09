/* eslint-disable no-console */

/**
 * scripts/analyze_semantic_duplicates.ts
 *
 * ✅ 目的
 * - 意味的に重複している業種を検出
 * - 統一すべき業種のペアを特定
 * - 統一ルールを提案
 */

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

interface IndustryValue {
  field: string;
  value: string;
  normalizedValue: string;
  count: number;
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

/**
 * 意味的な類似度を計算（簡易版）
 */
function calculateSemanticSimilarity(value1: string, value2: string): number {
  const norm1 = normalizeText(value1);
  const norm2 = normalizeText(value2);

  // 完全一致
  if (norm1 === norm2) {
    return 1.0;
  }

  // 一方が他方を含む（包含関係）
  if (norm1.includes(norm2) || norm2.includes(norm1)) {
    const longer = norm1.length > norm2.length ? norm1 : norm2;
    const shorter = norm1.length > norm2.length ? norm2 : norm1;
    return shorter.length / longer.length;
  }

  // 共通部分を計算
  const commonChars = new Set<string>();
  for (const char of norm1) {
    if (norm2.includes(char)) {
      commonChars.add(char);
    }
  }

  const totalChars = new Set([...norm1, ...norm2]).size;
  if (totalChars === 0) return 0;

  return commonChars.size / totalChars;
}

/**
 * 意味的に重複している可能性がある業種ペアを検出
 */
async function analyzeSemanticDuplicates() {
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
    }>;

    // 各フィールドごとに値を収集
    const valuesByField = new Map<string, IndustryValue[]>();

    for (const record of records) {
      const field = record.フィールド;
      if (!valuesByField.has(field)) {
        valuesByField.set(field, []);
      }

      valuesByField.get(field)!.push({
        field,
        value: record.統一後の値,
        normalizedValue: record.正規化値,
        count: parseInt(record.出現回数, 10),
      });
    }

    console.log(`\n📊 各フィールドの種類数:`);
    for (const [field, values] of valuesByField.entries()) {
      console.log(`  ${field}: ${values.length} 種類`);
    }

    // 意味的な重複を検出
    console.log(`\n🔍 意味的な重複を検出中...`);

    const duplicates: Array<{
      field: string;
      value1: string;
      value2: string;
      similarity: number;
      count1: number;
      count2: number;
      recommendedValue: string;
    }> = [];

    // 各フィールド内で重複を検出
    for (const [field, values] of valuesByField.entries()) {
      for (let i = 0; i < values.length; i++) {
        for (let j = i + 1; j < values.length; j++) {
          const v1 = values[i];
          const v2 = values[j];

          // 正規化値が既に同じ場合はスキップ（既に統一済み）
          if (v1.normalizedValue === v2.normalizedValue) {
            continue;
          }

          // 意味的な類似度を計算
          const similarity = calculateSemanticSimilarity(v1.value, v2.value);

          // 類似度が0.5以上の場合、重複の可能性がある
          if (similarity >= 0.5) {
            // 出現回数の多い方を推奨値に
            const recommendedValue = v1.count >= v2.count ? v1.value : v2.value;
            
            duplicates.push({
              field,
              value1: v1.value,
              value2: v2.value,
              similarity,
              count1: v1.count,
              count2: v2.count,
              recommendedValue,
            });
          }
        }
      }
    }

    // 類似度の高い順にソート
    duplicates.sort((a, b) => b.similarity - a.similarity);

    console.log(`\n✅ 意味的な重複検出完了: ${duplicates.length} 件`);

    // 結果をCSVに出力
    const outDir = path.join(process.cwd(), "out");
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const outputPath = path.join(outDir, `semantic_duplicates_${timestamp}.csv`);

    // CSVファイルに出力（非同期処理を確実に完了させる）
    await new Promise<void>((resolve, reject) => {
      const outputStream = fs.createWriteStream(outputPath, { encoding: "utf8", flags: "w" });
      outputStream.write("フィールド,値1,値2,類似度,値1の出現回数,値2の出現回数,推奨統一値\n");

      for (const dup of duplicates) {
        outputStream.write(
          `${dup.field},"${dup.value1.replace(/"/g, '""')}","${dup.value2.replace(/"/g, '""')}",${dup.similarity.toFixed(3)},${dup.count1},${dup.count2},"${dup.recommendedValue.replace(/"/g, '""')}"\n`
        );
      }

      outputStream.on("finish", resolve);
      outputStream.on("error", reject);
      outputStream.end();
    });

    console.log(`\n📁 出力ファイル: ${outputPath}`);

    // トップ30を表示
    console.log(`\n📈 トップ30（類似度順）:`);
    duplicates.slice(0, 30).forEach((dup, index) => {
      console.log(`  ${index + 1}. [${dup.field}] "${dup.value1}" ↔ "${dup.value2}" (類似度: ${dup.similarity.toFixed(3)})`);
      console.log(`     推奨統一値: "${dup.recommendedValue}" (${dup.count1 >= dup.count2 ? dup.count1 : dup.count2}件)`);
    });

    console.log(`\n✅ 分析完了`);

  } catch (error: any) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error("❌ 重大エラー:", errorMsg);
    console.error(error);
    process.exit(1);
  }
}

analyzeSemanticDuplicates()
  .then(() => {
    console.log("\n処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
