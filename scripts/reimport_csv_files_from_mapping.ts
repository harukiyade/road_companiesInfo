/* 
  CSVマッピングファイルに基づいてCSVファイルを再インポートするスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/reimport_csv_files_from_mapping.ts <csv_mapping.json>
*/

import * as fs from "fs";
import * as path from "path";
import { execSync } from "child_process";

interface CsvMapping {
  [csvFile: string]: number;
}

async function main() {
  const mappingPath = process.argv[2];

  if (!mappingPath) {
    console.error('❌ エラー: CSVマッピングファイルのパスが指定されていません');
    console.error('');
    console.error('使用方法:');
    console.error('  npx ts-node scripts/reimport_csv_files_from_mapping.ts <csv_mapping.json>');
    process.exit(1);
  }

  if (!fs.existsSync(mappingPath)) {
    console.error(`❌ エラー: マッピングファイルが見つかりません: ${mappingPath}`);
    process.exit(1);
  }

  // マッピングを読み込む
  const mapping: CsvMapping = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));

  // CSVファイルを削除数が多い順にソート
  const csvFiles = Object.keys(mapping).sort((a, b) => mapping[b] - mapping[a]);

  console.log('📋 再インポートが必要なCSVファイル:');
  console.log('-'.repeat(80));
  for (const file of csvFiles) {
    console.log(`  - ${file}: ${mapping[file]}件`);
  }
  console.log('');

  const serviceAccountKey = process.env.GOOGLE_APPLICATION_CREDENTIALS || './albert-ma-firebase-adminsdk-iat1k-a64039899f.json';

  // 各CSVファイルを再インポート
  for (const csvFile of csvFiles) {
    const csvPath = path.join('./csv', csvFile);
    
    if (!fs.existsSync(csvPath)) {
      console.log(`⚠️  ファイルが見つかりません: ${csvPath}`);
      continue;
    }

    console.log(`\n📄 インポート中: ${csvFile} (${mapping[csvFile]}件削除された)`);
    console.log('-'.repeat(80));

    try {
      const command = `GOOGLE_APPLICATION_CREDENTIALS=${serviceAccountKey} npx ts-node scripts/import_companies_from_csv.ts ${csvPath}`;
      execSync(command, {
        stdio: 'inherit',
        cwd: process.cwd(),
      });
      console.log(`✅ ${csvFile} のインポート完了`);
    } catch (error) {
      console.error(`❌ ${csvFile} のインポートに失敗しました:`, error);
    }
  }

  console.log('\n✅ すべての再インポートが完了しました');
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
