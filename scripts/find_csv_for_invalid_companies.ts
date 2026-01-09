/* 
  問題のある会社名のドキュメントがどのCSVファイルに含まれているかを特定するスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/find_csv_for_invalid_companies.ts <report.json> [--output result.json]
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

interface InvalidCompany {
  docId: string;
  name: string;
  corporateNumber: string | null;
  sourceFile: string | null;
  sourceRow: number | null;
  prefecture: string | null;
}

interface Report {
  summary: {
    total: number;
    byFile: Record<string, number>;
  };
  companies: InvalidCompany[];
}

interface CsvMatch {
  csvFile: string;
  rowNumber: number;
  companyName: string;
  corporateNumber: string;
}

function findCompanyInCsv(
  csvDir: string,
  corporateNumber: string
): CsvMatch[] {
  const matches: CsvMatch[] = [];
  const csvFiles = fs.readdirSync(csvDir).filter(f => f.endsWith('.csv'));

  for (const csvFile of csvFiles) {
    const csvPath = path.join(csvDir, csvFile);
    try {
      const content = fs.readFileSync(csvPath, 'utf8');
      const records = parse(content, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
      });

      // 法人番号の列を探す
      let corporateNumberCol: string | null = null;
      let companyNameCol: string | null = null;

      if (records.length > 0) {
        const firstRow = records[0] as Record<string, any>;
        for (const [key, value] of Object.entries(firstRow)) {
          if (key.includes('法人番号') || key.includes('corporateNumber')) {
            corporateNumberCol = key;
          }
          if (key.includes('会社名') || key.includes('社名') || key.includes('企業名') || key === 'name' || key === 'companyName') {
            companyNameCol = key;
          }
        }
      }

      if (!corporateNumberCol) {
        // 列名で見つからない場合、最初の行をヘッダーとして解析
        const lines = content.split('\n');
        if (lines.length > 0) {
          const headers = lines[0].split(',');
          for (let i = 0; i < headers.length; i++) {
            if (headers[i].includes('法人番号')) {
              corporateNumberCol = headers[i];
            }
            if (headers[i].includes('会社名') || headers[i].includes('社名')) {
              companyNameCol = headers[i];
            }
          }
        }
      }

      // レコードを検索
      for (let i = 0; i < records.length; i++) {
        const record = records[i] as Record<string, any>;
        const recordCorpNum = corporateNumberCol
          ? String(record[corporateNumberCol] || '').trim()
          : '';

        if (recordCorpNum === corporateNumber) {
          const companyName = companyNameCol
            ? String(record[companyNameCol] || '').trim()
            : '';
          
          matches.push({
            csvFile,
            rowNumber: i + 2, // ヘッダー行を考慮して+2
            companyName,
            corporateNumber,
          });
          break; // 1つのCSVファイルに1回だけマッチする想定
        }
      }
    } catch (error) {
      // CSVファイルの読み込みエラーは無視
      console.warn(`⚠️  ${csvFile} の読み込みに失敗: ${error}`);
    }
  }

  return matches;
}

async function main() {
  const reportPath = process.argv[2];
  const csvDir = process.argv[3] || './csv';

  if (!reportPath) {
    console.error('❌ エラー: レポートファイルのパスが指定されていません');
    console.error('');
    console.error('使用方法:');
    console.error('  npx ts-node scripts/find_csv_for_invalid_companies.ts <report.json> [csv-dir]');
    process.exit(1);
  }

  if (!fs.existsSync(reportPath)) {
    console.error(`❌ エラー: レポートファイルが見つかりません: ${reportPath}`);
    process.exit(1);
  }

  if (!fs.existsSync(csvDir)) {
    console.error(`❌ エラー: CSVディレクトリが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  // レポートを読み込む
  const report: Report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));

  console.log('📋 レポートを読み込みました');
  console.log(`   総問題数: ${report.summary.total}件`);

  // 法人番号が「9」で始まる13桁のドキュメントをフィルタ
  let csvLikeCompanies = report.companies.filter(
    c => c.corporateNumber && /^9\d{12}$/.test(c.corporateNumber)
  );

  // テスト用: 最初の100件のみ処理
  const LIMIT = process.argv.includes('--limit') 
    ? parseInt(process.argv[process.argv.indexOf('--limit') + 1]) 
    : csvLikeCompanies.length;
  
  if (LIMIT < csvLikeCompanies.length) {
    console.log(`⚠️  テストモード: 最初の${LIMIT}件のみ処理します`);
    csvLikeCompanies = csvLikeCompanies.slice(0, LIMIT);
  }

  console.log(`\n🔍 CSV由来の可能性があるドキュメント: ${csvLikeCompanies.length}件`);
  console.log(`   これらをCSVファイルで検索します...\n`);

  const results: Map<string, CsvMatch[]> = new Map();
  let processed = 0;

  for (const company of csvLikeCompanies) {
    if (!company.corporateNumber) continue;

    processed++;
    if (processed % 100 === 0) {
      console.log(`  処理中: ${processed}/${csvLikeCompanies.length}件`);
    }

    const matches = findCompanyInCsv(csvDir, company.corporateNumber);
    if (matches.length > 0) {
      results.set(company.docId, matches);
    }
  }

  console.log(`\n✅ 検索完了`);
  console.log(`   マッチしたドキュメント: ${results.size}件`);

  // CSVファイル別に集計
  const byCsvFile: Map<string, number> = new Map();
  for (const matches of results.values()) {
    for (const match of matches) {
      byCsvFile.set(match.csvFile, (byCsvFile.get(match.csvFile) || 0) + 1);
    }
  }

  console.log(`\n📁 CSVファイル別の内訳:`);
  const sortedFiles = Array.from(byCsvFile.entries()).sort((a, b) => b[1] - a[1]);
  for (const [file, count] of sortedFiles) {
    console.log(`  ${file}: ${count}件`);
  }

  // 結果を保存
  const outputPath = process.argv.includes('--output')
    ? process.argv[process.argv.indexOf('--output') + 1]
    : `csv_matches_${Date.now()}.json`;

  const output = {
    summary: {
      totalSearched: csvLikeCompanies.length,
      totalMatched: results.size,
      byCsvFile: Object.fromEntries(byCsvFile),
    },
    matches: Array.from(results.entries()).map(([docId, matches]) => ({
      docId,
      matches,
    })),
  };

  fs.writeFileSync(outputPath, JSON.stringify(output, null, 2), 'utf8');
  console.log(`\n💾 結果を保存しました: ${outputPath}`);
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
