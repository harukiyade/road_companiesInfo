/* 
  問題のある会社名のドキュメントを削除し、どのCSVから来たかを記録するスクリプト（最適化版）
  
  CSVファイルを事前にインデックス化して高速検索

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_invalid_and_track_csv_optimized.ts <report.json> [csv-dir]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  WriteBatch,
} from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const BATCH_SIZE = 500; // Firestoreのバッチ制限

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

interface CsvIndex {
  corporateNumber: string;
  csvFile: string;
  rowNumber: number;
  companyName: string;
}

interface DeletionRecord {
  docId: string;
  name: string;
  corporateNumber: string;
  csvFiles: string[];
  deletedAt: string;
}

function initFirebase() {
  if (admin.apps.length === 0) {
    const serviceAccountPath =
      process.env.GOOGLE_APPLICATION_CREDENTIALS ||
      './serviceAccountKey.json';

    if (!fs.existsSync(serviceAccountPath)) {
      console.error(
        '❌ エラー: サービスアカウントキーファイルが見つかりません'
      );
      console.error(`   パス: ${serviceAccountPath}`);
      process.exit(1);
    }

    const serviceAccount = JSON.parse(
      fs.readFileSync(serviceAccountPath, 'utf8')
    );
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  }
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

        if (recordCorpNum && /^9\d{12}$/.test(recordCorpNum)) {
          const companyName = companyNameCol
            ? String(record[companyNameCol] || '').trim()
            : '';

          if (!index.has(recordCorpNum)) {
            index.set(recordCorpNum, []);
          }

          index.get(recordCorpNum)!.push({
            corporateNumber: recordCorpNum,
            csvFile,
            rowNumber: i + 2,
            companyName,
          });
        }
      }
    } catch (error) {
      // CSVファイルの読み込みエラーは無視
    }
  }

  console.log(`✅ インデックス化完了: ${index.size}件の法人番号をインデックス化`);
  return index;
}

async function deleteInvalidCompanies(
  db: Firestore,
  invalidCompanies: InvalidCompany[],
  csvIndex: Map<string, CsvIndex[]>
): Promise<{
  deleted: number;
  csvMapping: Map<string, string[]>;
  deletionRecords: DeletionRecord[];
}> {
  const companiesCol = db.collection(COLLECTION_NAME);
  
  // 法人番号が「9」で始まる13桁のドキュメントのみを対象
  const targetCompanies = invalidCompanies.filter(
    c => c.corporateNumber && /^9\d{12}$/.test(c.corporateNumber)
  );

  console.log(`\n🗑️  削除を開始します...`);
  console.log(`   対象: ${targetCompanies.length}件（法人番号が9で始まる13桁）`);
  console.log(`   総問題数: ${invalidCompanies.length}件\n`);

  const csvMapping = new Map<string, string[]>(); // CSVファイル -> ドキュメントIDのリスト
  const deletionRecords: DeletionRecord[] = [];
  let deletedCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  let processedCount = 0;
  let matchedCount = 0;

  for (const company of targetCompanies) {
    if (!company.corporateNumber) continue;

    processedCount++;
    if (processedCount % 500 === 0) {
      console.log(`  処理中: ${processedCount}/${targetCompanies.length}件 (削除済み: ${deletedCount}件, マッチ: ${matchedCount}件)`);
    }

    // インデックスから検索
    const csvMatches = csvIndex.get(company.corporateNumber) || [];
    const csvFiles = csvMatches.map(m => m.csvFile);

    if (csvFiles.length > 0) {
      matchedCount++;
    }

    // CSVファイル別にマッピング
    for (const csvFile of csvFiles) {
      if (!csvMapping.has(csvFile)) {
        csvMapping.set(csvFile, []);
      }
      csvMapping.get(csvFile)!.push(company.docId);
    }

    // 削除レコードを作成
    deletionRecords.push({
      docId: company.docId,
      name: company.name,
      corporateNumber: company.corporateNumber,
      csvFiles: csvFiles,
      deletedAt: new Date().toISOString(),
    });

    // バッチに追加
    const docRef = companiesCol.doc(company.docId);
    batch.delete(docRef);
    batchCount++;
    deletedCount++;

    // バッチ制限に達したらコミット
    if (batchCount >= BATCH_SIZE) {
      await batch.commit();
      console.log(`  ✅ 削除済み: ${deletedCount}/${targetCompanies.length}件`);
      batch = db.batch();
      batchCount = 0;
    }
  }

  // 残りのバッチをコミット
  if (batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 削除済み: ${deletedCount}/${targetCompanies.length}件`);
  }

  console.log(`\n✅ 削除完了: ${deletedCount}件のドキュメントを削除しました`);
  console.log(`   CSVマッチ: ${matchedCount}件`);

  return {
    deleted: deletedCount,
    csvMapping,
    deletionRecords,
  };
}

async function main() {
  const reportPath = process.argv[2];
  const csvDir = process.argv[3] || './csv';

  if (!reportPath) {
    console.error('❌ エラー: レポートファイルのパスが指定されていません');
    console.error('');
    console.error('使用方法:');
    console.error('  npx ts-node scripts/delete_invalid_and_track_csv_optimized.ts <report.json> [csv-dir]');
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
  const targetCompanies = report.companies.filter(
    c => c.corporateNumber && /^9\d{12}$/.test(c.corporateNumber)
  );

  console.log(`   削除対象: ${targetCompanies.length}件（法人番号が9で始まる13桁）`);

  // CSVインデックスを構築
  const csvIndex = buildCsvIndex(csvDir);

  // Firebase初期化
  initFirebase();
  const db = admin.firestore();

  // 削除実行
  const result = await deleteInvalidCompanies(db, report.companies, csvIndex);

  // 結果を保存
  const timestamp = Date.now();
  const resultPath = `deletion_result_${timestamp}.json`;
  const csvMappingPath = `csv_mapping_${timestamp}.json`;

  const resultData = {
    summary: {
      totalDeleted: result.deleted,
      deletedAt: new Date().toISOString(),
      csvFilesAffected: Array.from(result.csvMapping.keys()).length,
    },
    csvMapping: Object.fromEntries(
      Array.from(result.csvMapping.entries()).map(([file, docIds]) => [
        file,
        {
          count: docIds.length,
          docIds: docIds.slice(0, 100), // 最初の100件のみ保存
        },
      ])
    ),
    deletionRecords: result.deletionRecords.slice(0, 1000), // 最初の1000件のみ保存
  };

  fs.writeFileSync(resultPath, JSON.stringify(resultData, null, 2), 'utf8');
  console.log(`\n💾 削除結果を保存しました: ${resultPath}`);

  // CSVファイル別のマッピングを保存
  const csvMappingData = Object.fromEntries(
    Array.from(result.csvMapping.entries()).map(([file, docIds]) => [
      file,
      docIds.length,
    ])
  );
  fs.writeFileSync(csvMappingPath, JSON.stringify(csvMappingData, null, 2), 'utf8');
  console.log(`💾 CSVマッピングを保存しました: ${csvMappingPath}`);

  // CSVファイルリストを表示
  const csvFiles = Array.from(result.csvMapping.keys()).sort();
  if (csvFiles.length > 0) {
    console.log(`\n📝 再インポートが必要なCSVファイル (${csvFiles.length}個):`);
    console.log('-'.repeat(80));
    for (const file of csvFiles) {
      const count = result.csvMapping.get(file)!.length;
      console.log(`  - ${file} (${count}件)`);
    }
    console.log('\n再インポートコマンド例:');
    console.log('  GOOGLE_APPLICATION_CREDENTIALS=./serviceAccountKey.json \\');
    console.log('  npx ts-node scripts/import_companies_from_csv.ts csv/<ファイル名>');
  }
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
