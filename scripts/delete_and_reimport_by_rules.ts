/* 
  再インポート可能な問題のあるnameフィールドのドキュメントを削除し、
  指定されたルールに沿ってCSVを再インポートするスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_and_reimport_by_rules.ts <trace-result.json> [csv-dir] [--test] [--dry-run]
    
  --test: テストモード（各CSVファイルから5件のみインポート）
  
  ※ 途中から再開したい場合:
    START_FROM_FILE=112.csv \
    npx ts-node scripts/delete_and_reimport_by_rules.ts <trace-result.json> [csv-dir] [--test]
    
    指定したCSVファイルから処理を再開します（削除処理はスキップされます）
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  WriteBatch,
} from "firebase-admin/firestore";
import { execSync } from "child_process";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const TEST_MODE = process.argv.includes("--test");
const BATCH_SIZE = 500;

interface TraceResult {
  summary: {
    total: number;
    canReimport: number;
    cannotReimport: number;
    csvFileCount: number;
  };
  csvFileCounts: Record<string, number>;
}

function initFirebase() {
  if (admin.apps.length === 0) {
    const serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || './serviceAccountKey.json';
    if (!fs.existsSync(serviceAccountPath)) {
      console.error('❌ エラー: サービスアカウントキーファイルが見つかりません');
      console.error(`   パス: ${serviceAccountPath}`);
      process.exit(1);
    }
    const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, 'utf8'));
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
  }
}

async function loadReimportableDocIds(traceResultPath: string, csvDir: string): Promise<Set<string>> {
  console.log(`📄 トレース結果を読み込み中: ${traceResultPath}`);
  const traceResult: TraceResult = JSON.parse(fs.readFileSync(traceResultPath, 'utf8'));
  console.log(`   再インポート可能: ${traceResult.summary.canReimport}件`);

  // 全量レポートから再インポート可能なdocIdを取得
  const fullReportPath = 'invalid_company_names_full_report_1765999808803.json';
  if (!fs.existsSync(fullReportPath)) {
    console.error(`❌ エラー: 全量レポートファイルが見つかりません: ${fullReportPath}`);
    process.exit(1);
  }

  console.log(`📄 全量レポートを読み込み中: ${fullReportPath}`);
  const fullReport = JSON.parse(fs.readFileSync(fullReportPath, 'utf8'));
  console.log(`   問題のあるドキュメント: ${fullReport.summary.total}件`);

  // CSVファイルをインデックス化して、再インポート可能なdocIdを特定
  const csvIndex = await buildCsvIndex(csvDir);

  const reimportableDocIds = new Set<string>();

  for (const company of fullReport.companies) {
    if (!company.corporateNumber) continue;

    // CSVインデックスに存在する場合は再インポート可能
    if (csvIndex.has(company.corporateNumber)) {
      reimportableDocIds.add(company.docId);
    }
  }

  console.log(`✅ 再インポート可能なdocId: ${reimportableDocIds.size}件`);
  return reimportableDocIds;
}

async function buildCsvIndex(csvDir: string): Promise<Set<string>> {
  console.log('\n📚 CSVファイルをインデックス化中...');
  const index = new Set<string>();
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
      const lines = content.split('\n');
      if (lines.length < 2) continue;

      const headers = lines[0].split(',');
      let corporateNumberColIdx = -1;

      for (let i = 0; i < headers.length; i++) {
        if (headers[i].includes('法人番号')) {
          corporateNumberColIdx = i;
          break;
        }
      }

      if (corporateNumberColIdx < 0) continue;

      for (let i = 1; i < lines.length; i++) {
        const cols = lines[i].split(',');
        if (cols.length > corporateNumberColIdx) {
          const corpNum = cols[corporateNumberColIdx].trim();
          if (corpNum && /^\d+$/.test(corpNum)) {
            index.add(corpNum);
          }
        }
      }
    } catch (error) {
      // エラーは無視
    }
  }

  console.log(`✅ インデックス化完了: ${index.size}件の法人番号をインデックス化`);
  return index;
}

async function deleteDocuments(db: Firestore, docIds: Set<string>): Promise<number> {
  const companiesCol = db.collection(COLLECTION_NAME);
  
  console.log(`\n🗑️  削除を開始します...`);
  console.log(`   対象: ${docIds.size}件`);
  console.log(`   モード: ${DRY_RUN ? 'DRY RUN (実際には削除しません)' : '実際に削除'}\n`);

  let deletedCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  let processedCount = 0;

  for (const docId of docIds) {
    processedCount++;
    if (processedCount % 100 === 0) {
      console.log(`  処理中: ${processedCount}/${docIds.size}件 (削除済み: ${deletedCount}件)`);
    }

    if (!DRY_RUN) {
      const docRef = companiesCol.doc(docId);
      batch.delete(docRef);
      batchCount++;
      deletedCount++;

      if (batchCount >= BATCH_SIZE) {
        await batch.commit();
        console.log(`  ✅ 削除済み: ${deletedCount}/${docIds.size}件`);
        batch = db.batch();
        batchCount = 0;
      }
    } else {
      deletedCount++;
      if (deletedCount <= 10) {
        console.log(`  [DRY RUN] 削除予定: ${docId}`);
      }
    }
  }

  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 削除済み: ${deletedCount}/${docIds.size}件`);
  }

  if (DRY_RUN) {
    console.log(`\n⚠️  DRY RUNモード: 実際には削除していません`);
  } else {
    console.log(`\n✅ 削除完了: ${deletedCount}件のドキュメントを削除しました`);
  }

  return deletedCount;
}

// 処理済みCSVファイルを記録するログファイルを読み込む
function loadProcessedFiles(logFile: string): Set<string> {
  const processed = new Set<string>();
  
  if (!fs.existsSync(logFile)) {
    return processed;
  }

  const content = fs.readFileSync(logFile, 'utf8');
  const lines = content.split('\n');
  
  for (const line of lines) {
    // ✅ 112.csv のインポート完了 のような行からファイル名を抽出
    const match = line.match(/✅\s+([^/\s]+\.csv)\s+のインポート完了/);
    if (match) {
      processed.add(match[1]);
    }
  }
  
  return processed;
}

async function reimportCsvFiles(
  traceResult: TraceResult,
  csvDir: string
): Promise<void> {
  if (DRY_RUN) {
    console.log(`\n⚠️  DRY RUNモード: 再インポートは実行しません`);
    return;
  }

  const csvFiles = Object.keys(traceResult.csvFileCounts)
    .sort((a, b) => traceResult.csvFileCounts[b] - traceResult.csvFileCounts[a]);

  // 🔁 再開用設定
  const startFromFile = process.env.START_FROM_FILE || "";
  let started = !startFromFile; // START_FROM_FILE 未指定なら最初から開始
  
  // 既存のログファイルを探す（最新のものを使用）
  const logFiles = fs.readdirSync(process.cwd())
    .filter(f => f.startsWith('reimport_by_rules_') && f.endsWith('.txt'))
    .sort()
    .reverse();
  
  const logFile = logFiles.length > 0 && startFromFile 
    ? logFiles[0]  // 再開時は既存のログファイルを使用
    : `reimport_by_rules_${Date.now()}.txt`;  // 新規実行時は新しいログファイル
  
  // 処理済みファイルを読み込む
  const processedFiles = loadProcessedFiles(logFile);
  
  if (startFromFile) {
    console.log(`\n🔁 再開モード: ${startFromFile} から処理を再開します`);
    console.log(`   既存のログファイルを使用: ${logFile}`);
    console.log(`   処理済みファイル: ${processedFiles.size}件\n`);
  } else {
    console.log(`\n📋 再インポートを開始します（${TEST_MODE ? 'テストモード: 各5件' : '全量'}）...`);
    console.log(`   対象CSVファイル: ${csvFiles.length}ファイル`);
    console.log(`   ログファイル: ${logFile}\n`);
  }

  const serviceAccountKey = process.env.GOOGLE_APPLICATION_CREDENTIALS || './albert-ma-firebase-adminsdk-iat1k-a64039899f.json';
  let processedCount = 0;
  let skippedCount = 0;

  for (const csvFile of csvFiles) {
    // 再開用: 指定されたファイルに到達するまでスキップ
    if (!started) {
      if (csvFile === startFromFile || csvFile.includes(startFromFile)) {
        started = true;
        console.log(`\n▶️  再開位置に到達: ${csvFile}`);
      } else {
        skippedCount++;
        continue;
      }
    }
    
    // 既に処理済みのファイルはスキップ
    if (processedFiles.has(csvFile)) {
      console.log(`\n⏭️  スキップ: ${csvFile} (既に処理済み)`);
      skippedCount++;
      continue;
    }

    const csvPath = path.join(csvDir, csvFile);

    if (!fs.existsSync(csvPath)) {
      console.log(`⚠️  ファイルが見つかりません: ${csvPath}`);
      continue;
    }

    const count = traceResult.csvFileCounts[csvFile];
    console.log(`\n📄 インポート中: ${csvFile} (${count}件削除された)`);
    console.log(`   進捗: ${processedCount + 1}/${csvFiles.length - skippedCount}ファイル`);
    console.log('-'.repeat(80));

    try {
      // import_csv_by_groups.tsを実行
      let command = `GOOGLE_APPLICATION_CREDENTIALS=${serviceAccountKey} npx ts-node scripts/import_csv_by_groups.ts ${csvPath}`;
      if (DRY_RUN) {
        command += ' --dry-run';
      }
      if (TEST_MODE) {
        command += ' --test';
      }
      
      // 環境変数でテストモードを制御
      const env: Record<string, string> = {
        ...process.env,
        GOOGLE_APPLICATION_CREDENTIALS: serviceAccountKey,
      } as Record<string, string>;
      if (TEST_MODE) {
        env.TEST_MODE = 'true';
        env.TEST_LIMIT = '5';
      }
      
      execSync(command, {
        stdio: 'inherit',
        cwd: process.cwd(),
        env,
      });
      
      console.log(`✅ ${csvFile} のインポート完了`);
      fs.appendFileSync(logFile, `✅ ${csvFile} のインポート完了\n`, 'utf8');
      processedCount++;
    } catch (error) {
      console.error(`❌ ${csvFile} のインポートに失敗しました:`, error);
      fs.appendFileSync(logFile, `❌ ${csvFile} のインポートに失敗: ${error}\n`, 'utf8');
      // エラーが発生しても続行（次のファイルを処理）
    }
  }

  console.log(`\n✅ すべての再インポートが完了しました`);
  console.log(`   処理済み: ${processedCount}ファイル`);
  console.log(`   スキップ: ${skippedCount}ファイル`);
  console.log(`📝 ログファイル: ${logFile}`);
}

async function main() {
  const traceResultPath = process.argv[2];
  const csvDir = process.argv[3] || './csv';

  if (!traceResultPath) {
    console.error('❌ エラー: トレース結果ファイルのパスが指定されていません');
    console.error('');
    console.error('使用方法:');
    console.error('  npx ts-node scripts/delete_and_reimport_by_rules.ts <trace-result.json> [csv-dir] [--test] [--dry-run]');
    process.exit(1);
  }

  if (!fs.existsSync(traceResultPath)) {
    console.error(`❌ エラー: トレース結果ファイルが見つかりません: ${traceResultPath}`);
    process.exit(1);
  }

  if (!fs.existsSync(csvDir)) {
    console.error(`❌ エラー: CSVディレクトリが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  initFirebase();
  const db = admin.firestore();

  // トレース結果を読み込み
  const traceResult: TraceResult = JSON.parse(fs.readFileSync(traceResultPath, 'utf8'));

  // 🔁 再開モードの場合は削除処理をスキップ
  const startFromFile = process.env.START_FROM_FILE || "";
  if (!startFromFile) {
    // 再インポート可能なdocIdを取得
    const docIds = await loadReimportableDocIds(traceResultPath, csvDir);

    // 削除実行
    await deleteDocuments(db, docIds);
  } else {
    console.log(`\n🔁 再開モード: 削除処理はスキップします（既に削除済みと仮定）`);
  }

  // 再インポート実行
  await reimportCsvFiles(traceResult, csvDir);
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
