/* eslint-disable no-console */

/**
 * scripts/generate_unification_rules.ts
 *
 * ✅ 目的
 * - 意味的重複と法人種別の統一ルールを統合
 * - 統一ルールをCSVファイルとして出力
 * - ユーザーが確認・承認できる形式で提供
 */

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

// ------------------------------
// 法人種別のリスト
// ------------------------------

const CORPORATION_TYPES = [
  "NPO",
  "NGO",
  "NPO法人",
  "NGO法人",
  "NPO・NGO",
  "NPO・NGO・公益法人",
  "公益法人",
  "一般社団法人",
  "一般財団法人",
  "公益社団法人",
  "公益財団法人",
  "特定非営利活動法人",
  "株式会社",
  "有限会社",
  "合資会社",
  "合名会社",
  "合同会社",
  "医療法人",
  "学校法人",
  "宗教法人",
  "社会福祉法人",
  "協同組合",
  "農業協同組合",
  "生活協同組合",
  "信用組合",
  "信用金庫",
  "相互会社",
  "特殊会社",
  "独立行政法人",
  "地方独立行政法人",
  "認可法人",
  "財団法人",
  "社団法人",
];

/**
 * NPOが含まれるかどうかを判定
 */
function containsNPO(value: string): boolean {
  if (!value || typeof value !== "string") {
    return false;
  }

  const normalizedValue = value.trim();
  
  // NPO関連のキーワードを含むかチェック
  const npoKeywords = ["NPO", "NGO", "公益法人", "一般社団法人", "一般財団法人", "公益社団法人", "公益財団法人", "特定非営利活動法人"];
  
  for (const keyword of npoKeywords) {
    if (normalizedValue.includes(keyword)) {
      return true;
    }
  }

  return false;
}

/**
 * 住所のような文字列かどうかを判定
 */
function isAddressLike(value: string): boolean {
  if (!value || typeof value !== "string") {
    return false;
  }

  const normalizedValue = value.trim();
  
  // 都道府県名を含む
  const prefectures = [
    "東京都", "北海道", "大阪府", "京都府",
    "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "神奈川県", "新潟県",
    "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県", "静岡県", "愛知県",
    "三重県", "滋賀県", "兵庫県", "奈良県", "和歌山県",
    "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県",
    "福岡県", "佐賀県", "長崎県", "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
  ];
  
  for (const prefecture of prefectures) {
    if (normalizedValue.includes(prefecture)) {
      // 都道府県名だけでなく、市区町村や番地も含む場合は住所の可能性が高い
      if (normalizedValue.includes("区") || normalizedValue.includes("市") || 
          normalizedValue.includes("町") || normalizedValue.includes("村") ||
          normalizedValue.includes("丁目") || normalizedValue.includes("番") ||
          normalizedValue.includes("号") || normalizedValue.match(/\d{2,4}-\d{2,4}-\d{4}/) ||
          normalizedValue.match(/\d{4}-\d{2}-\d{2}/) || normalizedValue.match(/〒/) ||
          normalizedValue.match(/\d{3}-?\d{4}/)) {
        return true;
      }
    }
  }

  // 郵便番号パターン（〒、3桁-4桁）
  if (normalizedValue.match(/〒/) || normalizedValue.match(/\d{3}-?\d{4}/)) {
    return true;
  }

  // 電話番号パターン（03-xxxx-xxxx形式）
  if (normalizedValue.match(/\d{2,4}-\d{2,4}-\d{4}/)) {
    // ただし、業種として適切なもの（例：「03-1234-5678」のような単独の電話番号）は除外
    // 住所と組み合わさっている場合は住所と判定
    if (normalizedValue.length > 20) {
      return true;
    }
  }

  // 日付パターン（2006-01-01形式）を含む長い文字列
  if (normalizedValue.match(/\d{4}-\d{2}-\d{2}/) && normalizedValue.length > 30) {
    return true;
  }

  // 市区町村名を含む（区、市、町、村）
  if ((normalizedValue.includes("区") || normalizedValue.includes("市") || 
       normalizedValue.includes("町") || normalizedValue.includes("村")) &&
      (normalizedValue.includes("丁目") || normalizedValue.includes("番") || normalizedValue.includes("号"))) {
    return true;
  }

  return false;
}

function isCorporationType(value: string): boolean {
  if (!value || typeof value !== "string") {
    return false;
  }

  const normalizedValue = value.trim();
  
  // 完全一致
  if (CORPORATION_TYPES.some((type) => normalizedValue === type)) {
    return true;
  }

  // 部分一致（法人種別を含む）
  for (const type of CORPORATION_TYPES) {
    if (normalizedValue.includes(type)) {
      return true;
    }
  }

  // 「法人」で終わる場合（一部例外を除く）
  if (normalizedValue.endsWith("法人") && normalizedValue.length <= 10) {
    const validIndustryWith法人 = [
      "医療法人",
      "学校法人",
      "宗教法人",
      "社会福祉法人",
    ];
    
    if (!validIndustryWith法人.includes(normalizedValue)) {
      return true;
    }
  }

  return false;
}

// ------------------------------
// メイン処理
// ------------------------------

async function generateUnificationRules() {
  try {
    // 意味的重複CSVファイルを取得
    const outDir = path.join(process.cwd(), "out");
    const semanticFiles = fs.readdirSync(outDir)
      .filter((f) => f.startsWith("semantic_duplicates_") && f.endsWith(".csv"))
      .sort()
      .reverse();
    
    if (semanticFiles.length === 0) {
      console.error("❌ エラー: 意味的重複分析CSVファイルが見つかりません。");
      console.error("   先に scripts/analyze_semantic_duplicates.ts を実行してください。");
      process.exit(1);
    }

    const semanticCsvPath = path.join(outDir, semanticFiles[0]);
    console.log(`意味的重複分析CSVファイルを読み込み中: ${semanticFiles[0]}`);

    const semanticContent = fs.readFileSync(semanticCsvPath, "utf-8");
    // ヘッダー行を確認
    const firstLine = semanticContent.split("\n")[0];
    console.log(`CSVヘッダー: ${firstLine}`);
    
    const semanticRecords = parse(semanticContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }) as Array<{
      フィールド?: string;
      値1?: string;
      値2?: string;
      類似度?: string;
      値1の出現回数?: string;
      値2の出現回数?: string;
      推奨統一値?: string;
      [key: string]: any;
    }>;
    
    console.log(`読み込んだレコード数: ${semanticRecords.length}`);
    if (semanticRecords.length > 0) {
      console.log(`最初のレコードのキー: ${Object.keys(semanticRecords[0]).join(", ")}`);
    }

    // 統一ルールを作成
    const unificationRules = new Map<string, {
      field: string;
      oldValue: string;
      newValue: string;
      reason: string;
      similarity?: number;
      count1?: number;
      count2?: number;
    }>();

    const MIN_SIMILARITY = 0.7; // 類似度の閾値

    // 1. 意味的重複の統一ルール
    let semanticCount = 0;
    for (const record of semanticRecords) {
      const field = record.フィールド || record["フィールド"] || "";
      const value1 = record.値1 || record["値1"] || "";
      const value2 = record.値2 || record["値2"] || "";
      const similarityStr = record.類似度 || record["類似度"] || "0";
      const count1Str = record.値1の出現回数 || record["値1の出現回数"] || "0";
      const count2Str = record.値2の出現回数 || record["値2の出現回数"] || "0";
      const recommendedValue = record.推奨統一値 || record["推奨統一値"] || "";

      if (!field || !value1 || !value2 || !recommendedValue) continue;

      // NPOが含まれる場合は「その他」に統一（意味的重複として扱わない）
      if (containsNPO(value1) || containsNPO(value2) || containsNPO(recommendedValue)) {
        continue; // 後で法人種別として処理される
      }

      // 住所のような文字列の場合は「その他」に統一（意味的重複として扱わない）
      if (isAddressLike(value1) || isAddressLike(value2) || isAddressLike(recommendedValue)) {
        continue; // 後で住所として処理される
      }

      const similarity = parseFloat(similarityStr);
      const count1 = parseInt(count1Str, 10) || 0;
      const count2 = parseInt(count2Str, 10) || 0;

      if (similarity >= MIN_SIMILARITY) {
        semanticCount++;
        // 値1を統一値に変更
        if (value1 !== recommendedValue) {
          const key = `${field}|${value1}`;
          if (!unificationRules.has(key) || count1 < count2) {
            unificationRules.set(key, {
              field,
              oldValue: value1,
              newValue: recommendedValue,
              reason: "意味的重複（類似度が高い）",
              similarity,
              count1,
              count2,
            });
          }
        }

        // 値2を統一値に変更
        if (value2 !== recommendedValue) {
          const key = `${field}|${value2}`;
          if (!unificationRules.has(key) || count2 < count1) {
            unificationRules.set(key, {
              field,
              oldValue: value2,
              newValue: recommendedValue,
              reason: "意味的重複（類似度が高い）",
              similarity,
              count1,
              count2,
            });
          }
        }
      }
    }

    // 2. 法人種別の統一ルール（NPOが含まれるものは全て「その他」に）
    const industryValuesCsvPath = path.join(outDir, "industry_values_unified_2026-01-05T10-37-37-304Z.csv");
    if (fs.existsSync(industryValuesCsvPath)) {
      console.log("業種値一覧CSVファイルを読み込み中...");
      const industryContent = fs.readFileSync(industryValuesCsvPath, "utf-8");
      const industryRecords = parse(industryContent, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
      }) as Array<{
        フィールド: string;
        統一後の値: string;
        正規化値: string;
        出現回数: string;
      }>;

      for (const record of industryRecords) {
        const value = record.統一後の値;
        const field = record.フィールド;
        
        // NPOが含まれるものは全て「その他」に統一
        if (containsNPO(value)) {
          const key = `${field}|${value}`;
          if (!unificationRules.has(key)) {
            unificationRules.set(key, {
              field,
              oldValue: value,
              newValue: "その他",
              reason: "法人種別（業種として不適切）",
            });
          }
        }
        // 法人種別の統一ルール
        else if (isCorporationType(value)) {
          const key = `${field}|${value}`;
          if (!unificationRules.has(key)) {
            unificationRules.set(key, {
              field,
              oldValue: value,
              newValue: "その他",
              reason: "法人種別（業種として不適切）",
            });
          }
        }
        // 住所のような文字列は「その他」に統一
        else if (isAddressLike(value)) {
          const key = `${field}|${value}`;
          if (!unificationRules.has(key)) {
            unificationRules.set(key, {
              field,
              oldValue: value,
              newValue: "その他",
              reason: "住所・連絡先情報（業種として不適切）",
            });
          }
        }
      }
    }

    console.log(`\n📊 統一ルール数: ${unificationRules.size} 件`);
    console.log(`   意味的重複: ${semanticCount} 件`);

    // 統一ルールをCSVに出力
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const outputPath = path.join(outDir, `unification_rules_${timestamp}.csv`);

    // フィールドごとにソート
    const rulesArray = Array.from(unificationRules.values());
    rulesArray.sort((a, b) => {
      // フィールド順（Large > Middle > Small > Detail）
      const fieldOrder: { [key: string]: number } = {
        industryLarge: 1,
        industryMiddle: 2,
        industrySmall: 3,
        industryDetail: 4,
      };
      const orderA = fieldOrder[a.field] || 999;
      const orderB = fieldOrder[b.field] || 999;
      
      if (orderA !== orderB) {
        return orderA - orderB;
      }
      
      // 同じフィールド内では、出現回数の多い順
      const countA = a.count1 || 0;
      const countB = b.count1 || 0;
      return countB - countA;
    });

    // CSVファイルに出力（非同期処理を確実に完了させる）
    await new Promise<void>((resolve, reject) => {
      const outputStream = fs.createWriteStream(outputPath, { encoding: "utf8", flags: "w" });
      outputStream.write("フィールド,統一前の値,統一後の値,理由,類似度,値1の出現回数,値2の出現回数\n");

      for (const rule of rulesArray) {
        outputStream.write(
          `${rule.field},"${rule.oldValue.replace(/"/g, '""')}","${rule.newValue.replace(/"/g, '""')}","${rule.reason.replace(/"/g, '""')}",${rule.similarity?.toFixed(3) || ""},${rule.count1 || ""},${rule.count2 || ""}\n`
        );
      }

      outputStream.on("finish", resolve);
      outputStream.on("error", reject);
      outputStream.end();
    });

    console.log(`\n📁 出力ファイル: ${outputPath}`);

    // サマリーを表示
    console.log(`\n📈 統一ルールサマリー:`);
    const byField = new Map<string, number>();
    const byReason = new Map<string, number>();

    for (const rule of rulesArray) {
      byField.set(rule.field, (byField.get(rule.field) || 0) + 1);
      byReason.set(rule.reason, (byReason.get(rule.reason) || 0) + 1);
    }

    console.log(`\n【フィールド別】`);
    for (const [field, count] of Array.from(byField.entries()).sort((a, b) => {
      const fieldOrder: { [key: string]: number } = {
        industryLarge: 1,
        industryMiddle: 2,
        industrySmall: 3,
        industryDetail: 4,
      };
      return (fieldOrder[a[0]] || 999) - (fieldOrder[b[0]] || 999);
    })) {
      console.log(`  ${field}: ${count} 件`);
    }

    console.log(`\n【理由別】`);
    for (const [reason, count] of Array.from(byReason.entries()).sort((a, b) => b[1] - a[1])) {
      console.log(`  ${reason}: ${count} 件`);
    }

    // トップ20を表示
    console.log(`\n📋 統一ルール（トップ20）:`);
    rulesArray.slice(0, 20).forEach((rule, index) => {
      console.log(`  ${index + 1}. [${rule.field}] "${rule.oldValue}" → "${rule.newValue}" (${rule.reason})`);
    });

    console.log(`\n✅ 統一ルール生成完了`);
    console.log(`\n💡 次のステップ:`);
    console.log(`   1. ${outputPath} を確認してください`);
    console.log(`   2. 問題がなければ、scripts/unify_industry_all.ts を実行してDBを更新してください`);

  } catch (error: any) {
    const errorMsg = error instanceof Error ? error.message : String(error);
    console.error("❌ 重大エラー:", errorMsg);
    console.error(error);
    process.exit(1);
  }
}

generateUnificationRules()
  .then(() => {
    console.log("\n処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
