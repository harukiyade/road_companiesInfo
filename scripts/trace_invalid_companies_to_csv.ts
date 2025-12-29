/* 
  問題のあるnameフィールドのドキュメントがどのCSVファイルから来たかを特定するスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/trace_invalid_companies_to_csv.ts <report.json> [csv-dir] [--output result.json]
*/

import * as fs from "fs";
import * as path from "path";
import { parse } from "csv-parse/sync";

interface InvalidCompany {
  docId: string;
  name: string;
  corporateNumber: string | null;
  issueType: string;
  [key: string]: any;
}

interface Report {
  summary: {
    total: number;
    byIssueType: Record<string, number>;
    byFile: Record<string, number>;
  };
  companies: InvalidCompany[];
}

interface CsvIndex {
  corporateNumber: string;
  csvFile: string;
  rowNumber: number;
  companyName: string;
}

interface TraceResult {
  docId: string;
  name: string;
  corporateNumber: string | null;
  issueType: string;
  csvFiles: string[];
  csvDetails: Array<{
    csvFile: string;
    rowNumber: number;
    companyName: string;
  }>;
}

function buildCsvIndex(csvDir: string): Map<string, CsvIndex[]> {
  console.log('\n📚 CSVファイルをインデックス化中...');
  const index = new Map<string, CsvIndex[]>();
  const csvFiles = fs.readdirSync(csvDir).filter(f => f.endsWith('.csv'));

  let processedFiles = 0;
  for (const csvFile of csvFiles) {
    processedFiles++;
    if (processedFiles % 10 === 0) {
      console.log(`  処理中: ${processedFiles}/${csvFiles.length}ファイル`);
    }

    const csvPath = path.join(csvDir, csvFile);
    try {
      const content = fs.readFileSync(csvPath, 'utf8');
      const records = parse(content, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
      });

      if (records.length === 0) continue;

      // 法人番号と会社名の列を探す
      const firstRow = records[0] as Record<string, any>;
      let corporateNumberCol: string | null = null;
      let companyNameCol: string | null = null;

      for (const key of Object.keys(firstRow)) {
        if (key.includes('法人番号') || key.includes('corporateNumber')) {
          corporateNumberCol = key;
        }
        if (key.includes('会社名') || key.includes('社名') || key.includes('企業名') || key === 'name' || key === 'companyName') {
          companyNameCol = key;
        }
      }

      // 列名で見つからない場合、最初の行をヘッダーとして解析
      if (!corporateNumberCol) {
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

      if (!corporateNumberCol) continue;

      // レコードをインデックスに追加
      for (let i = 0; i < records.length; i++) {
        const record = records[i] as Record<string, any>;
        const recordCorpNum = String(record[corporateNumberCol] || '').trim();

        if (recordCorpNum) {
          const companyName = companyNameCol
            ? String(record[companyNameCol] || '').trim()
            : '';

          if (!index.has(recordCorpNum)) {
            index.set(recordCorpNum, []);
          }

          index.get(recordCorpNum)!.push({
            corporateNumber: recordCorpNum,
            csvFile,
            rowNumber: i + 2, // ヘッダー行を考慮して+2
            companyName,
          });
        }
      }
    } catch (error) {
      // CSVファイルの読み込みエラーは無視
      console.warn(`  ⚠️  ${csvFile} の読み込みに失敗: ${error}`);
    }
  }

  console.log(`✅ インデックス化完了: ${index.size}件の法人番号をインデックス化`);
  return index;
}

function traceCompaniesToCsv(
  invalidCompanies: InvalidCompany[],
  csvIndex: Map<string, CsvIndex[]>
): {
  traced: TraceResult[];
  csvMapping: Map<string, number>;
} {
  console.log('\n🔍 CSVファイルとの紐付けを実行中...');

  const traced: TraceResult[] = [];
  const csvMapping = new Map<string, number>();

  let matchedCount = 0;
  let unmatchedCount = 0;

  for (let i = 0; i < invalidCompanies.length; i++) {
    const company = invalidCompanies[i];

    if ((i + 1) % 10000 === 0) {
      console.log(`  処理中: ${i + 1}/${invalidCompanies.length}件 (マッチ: ${matchedCount}件, マッチなし: ${unmatchedCount}件)`);
    }

    if (!company.corporateNumber) {
      unmatchedCount++;
      traced.push({
        docId: company.docId,
        name: company.name,
        corporateNumber: null,
        issueType: company.issueType,
        csvFiles: [],
        csvDetails: [],
      });
      continue;
    }

    // CSVインデックスから検索
    const csvMatches = csvIndex.get(company.corporateNumber) || [];

    if (csvMatches.length > 0) {
      matchedCount++;
      const csvFiles = Array.from(new Set(csvMatches.map(m => m.csvFile)));

      // CSVファイル別にカウント
      for (const csvFile of csvFiles) {
        if (!csvMapping.has(csvFile)) {
          csvMapping.set(csvFile, 0);
        }
        csvMapping.set(csvFile, csvMapping.get(csvFile)! + 1);
      }

      traced.push({
        docId: company.docId,
        name: company.name,
        corporateNumber: company.corporateNumber,
        issueType: company.issueType,
        csvFiles: csvFiles,
        csvDetails: csvMatches.map(m => ({
          csvFile: m.csvFile,
          rowNumber: m.rowNumber,
          companyName: m.companyName,
        })),
      });
    } else {
      unmatchedCount++;
      traced.push({
        docId: company.docId,
        name: company.name,
        corporateNumber: company.corporateNumber,
        issueType: company.issueType,
        csvFiles: [],
        csvDetails: [],
      });
    }
  }

  console.log(`\n✅ 紐付け完了:`);
  console.log(`   マッチしたドキュメント: ${matchedCount}件`);
  console.log(`   マッチしなかったドキュメント: ${unmatchedCount}件`);

  return { traced, csvMapping };
}

function printReport(
  traced: TraceResult[],
  csvMapping: Map<string, number>
): void {
  console.log('\n' + '='.repeat(80));
  console.log('📋 CSVファイル別の内訳');
  console.log('='.repeat(80));

  const sortedFiles = Array.from(csvMapping.entries()).sort((a, b) => b[1] - a[1]);

  console.log(`\n合計 ${csvMapping.size}個のCSVファイルに問題のあるドキュメントが含まれています\n`);

  // 上位30件を表示
  for (const [file, count] of sortedFiles.slice(0, 30)) {
    console.log(`  ${file}: ${count}件`);
  }

  if (sortedFiles.length > 30) {
    console.log(`  ... 他 ${sortedFiles.length - 30}ファイル`);
  }

  // マッチしなかった件数
  const unmatched = traced.filter(t => t.csvFiles.length === 0).length;
  if (unmatched > 0) {
    console.log(`\n⚠️  CSVで見つからなかった: ${unmatched}件`);
  }

  // 問題の種類別の内訳（CSVファイル別）
  console.log('\n\n📊 問題の種類別の内訳（CSVファイル別・上位10ファイル）:');
  console.log('-'.repeat(80));

  const issueTypes = ['no_corporate_suffix', 'person_name', 'business_description', 'empty', 'other'];
  const issueTypeLabels: Record<string, string> = {
    'no_corporate_suffix': '法人格なし',
    'person_name': '個人名・役員名',
    'business_description': '事業内容',
    'empty': '空',
    'other': 'その他',
  };

  for (const [file, count] of sortedFiles.slice(0, 10)) {
    console.log(`\n${file} (合計 ${count}件):`);
    const fileTraced = traced.filter(t => t.csvFiles.includes(file));

    for (const issueType of issueTypes) {
      const typeCount = fileTraced.filter(t => t.issueType === issueType).length;
      if (typeCount > 0) {
        const label = issueTypeLabels[issueType] || issueType;
        console.log(`  ${label}: ${typeCount}件`);
      }
    }
  }
}

async function main() {
  const reportPath = process.argv[2];
  const csvDir = process.argv[3] || './csv';
  const outputIndex = process.argv.indexOf('--output');
  const outputPath = outputIndex !== -1 && outputIndex + 1 < process.argv.length
    ? process.argv[outputIndex + 1]
    : `traced_invalid_companies_${Date.now()}.json`;

  if (!reportPath) {
    console.error('❌ エラー: レポートファイルのパスが指定されていません');
    console.error('');
    console.error('使用方法:');
    console.error('  npx ts-node scripts/trace_invalid_companies_to_csv.ts <report.json> [csv-dir] [--output result.json]');
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

  console.log(`📄 レポートファイルを読み込み中: ${reportPath}`);
  const report: Report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));
  console.log(`   問題のあるドキュメント: ${report.summary.total}件`);

  // CSVインデックスを構築
  const csvIndex = buildCsvIndex(csvDir);

  // CSVファイルとの紐付け
  const { traced, csvMapping } = traceCompaniesToCsv(report.companies, csvIndex);

  // レポートを表示
  printReport(traced, csvMapping);

  // CSVファイル別の詳細情報を集計
  const csvFileDetails = new Map<string, {
    count: number;
    byIssueType: Record<string, number>;
    sampleDocIds: string[];
  }>();

  for (const [file, count] of csvMapping.entries()) {
    const fileTraced = traced.filter(t => t.csvFiles.includes(file));
    
    const byIssueType: Record<string, number> = {};
    const sampleDocIds: string[] = [];

    for (const item of fileTraced) {
      byIssueType[item.issueType] = (byIssueType[item.issueType] || 0) + 1;
      if (sampleDocIds.length < 10) {
        sampleDocIds.push(item.docId);
      }
    }

    csvFileDetails.set(file, {
      count,
      byIssueType,
      sampleDocIds,
    });
  }

  // 結果を保存（サマリー情報のみ）
  const result = {
    summary: {
      total: traced.length,
      matched: traced.filter(t => t.csvFiles.length > 0).length,
      unmatched: traced.filter(t => t.csvFiles.length === 0).length,
      csvFileCount: csvMapping.size,
      generatedAt: new Date().toISOString(),
    },
    csvMapping: Object.fromEntries(
      Array.from(csvFileDetails.entries()).map(([file, details]) => [
        file,
        details,
      ])
    ),
    csvFileCounts: Object.fromEntries(csvMapping),
  };

  // JSONをストリームで書き込み（大きなファイルでも対応）
  fs.writeFileSync(outputPath, JSON.stringify(result, null, 2), 'utf8');
  console.log(`\n💾 結果を保存しました: ${outputPath}`);
  console.log(`   ファイルサイズ: ${(fs.statSync(outputPath).size / 1024 / 1024).toFixed(2)} MB`);
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
