/* 
  問題のあるnameフィールドのドキュメントを削除し、CSVから再インポートするスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_and_reimport_invalid_names.ts [csv-dir] [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  WriteBatch,
  QueryDocumentSnapshot,
} from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";
import { execSync } from "child_process";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const BATCH_SIZE = 500;

// 法人格のパターン
const CORPORATE_SUFFIXES = [
  '株式会社', '有限会社', '合資会社', '合名会社', '合同会社',
  '一般社団法人', '一般財団法人', '公益社団法人', '公益財団法人',
  '学校法人', '医療法人', '社会福祉法人', '宗教法人',
  '特定非営利活動法人', 'NPO法人', '協同組合', '農業協同組合',
  '生活協同組合', '信用金庫', '信用組合', '労働金庫',
  '相互会社', '特殊会社', '地方公共団体', '独立行政法人',
  '税理士法人', '司法書士法人', '弁理士法人', '行政書士法人',
  '土地家屋調査士法人', '社会保険労務士法人',
  '国立大学法人', '公立大学法人', '私立大学法人',
  '国立研究開発法人', '地方独立行政法人',
  '投資法人', '特定目的会社', '有限責任事業組合',
  '商工会議所', '商工会', '工業組合', '事業協同組合',
  '森林組合', '農業共済組合', '漁業協同組合',
  '住宅供給公社', '土地開発公社', '地方公営企業'
];

const OLD_STYLE_SUFFIXES = [
  '株式會社', '有限會社', '合資會社', '合名會社'
];

// 代表者名や役員名のパターン
const PERSON_NAME_PATTERNS = [
  /^（取）.*/,  // （取）で始まる
  /^\(取\).*/,  // (取)で始まる
  /^（専）.*/,  // （専）で始まる
  /^（常）.*/,  // （常）で始まる
  /^（代会）.*/, // （代会）で始まる
  /^\(社長\).*/, // (社長)で始まる
  /^（社長）.*/, // （社長）で始まる
  /^\d{4}年\d{1,2}月\d{1,2}日$/, // 日付パターン
  /^\d{4}\/\d{1,2}\/\d{1,2}$/, // 日付パターン
];

interface InvalidCompany {
  docId: string;
  name: string;
  corporateNumber: string | null;
  prefecture: string | null;
  address: string | null;
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

function hasCorporateSuffix(name: string | null | undefined): boolean {
  if (!name || typeof name !== 'string') {
    return false;
  }

  const trimmed = name.trim();
  if (!trimmed) {
    return false;
  }

  // 通常の法人格をチェック
  for (const suffix of CORPORATE_SUFFIXES) {
    if (trimmed.includes(suffix)) {
      return true;
    }
  }

  // 旧字体の法人格をチェック
  for (const suffix of OLD_STYLE_SUFFIXES) {
    if (trimmed.includes(suffix)) {
      return true;
    }
  }

  return false;
}

function isLikelyPersonName(name: string): boolean {
  if (!name || typeof name !== 'string') {
    return false;
  }

  const trimmed = name.trim();

  // パターンマッチング
  for (const pattern of PERSON_NAME_PATTERNS) {
    if (pattern.test(trimmed)) {
      return true;
    }
  }

  // カンマ区切りで複数の名前が列挙されている場合
  if (trimmed.includes('，') || trimmed.includes(',')) {
    const parts = trimmed.split(/[，,]/);
    if (parts.length >= 2 && parts.every(p => p.trim().length <= 10)) {
      return true;
    }
  }

  return false;
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
      if (!corporateNumberCol || !companyNameCol) {
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

async function findInvalidCompanies(
  db: Firestore
): Promise<InvalidCompany[]> {
  const companiesCol = db.collection(COLLECTION_NAME);
  const invalidCompanies: InvalidCompany[] = [];

  console.log('\n🔍 問題のあるnameフィールドを検索中...');

  let totalCount = 0;
  let lastDoc: QueryDocumentSnapshot<DocumentData> | null = null;
  const batchSize = 1000;

  while (true) {
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(batchSize);

    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();

    if (snapshot.empty) {
      break;
    }

    totalCount += snapshot.size;

    for (const doc of snapshot.docs) {
      const data = doc.data();
      const name = data.name;
      const corporateNumber = data.corporateNumber;

      // nameフィールドに法人格が含まれていない、または個人名の可能性がある場合
      if (name && typeof name === 'string') {
        if (!hasCorporateSuffix(name) || isLikelyPersonName(name)) {
          // 法人番号が「9」で始まる13桁の場合のみ対象
          if (corporateNumber && /^9\d{12}$/.test(String(corporateNumber))) {
            invalidCompanies.push({
              docId: doc.id,
              name: name.trim(),
              corporateNumber: String(corporateNumber),
              prefecture: data.prefecture || null,
              address: data.address || null,
            });
          }
        }
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];

    if (snapshot.size < batchSize) {
      break;
    }

    if (totalCount % 10000 === 0) {
      console.log(`  処理中: ${totalCount}件 (問題: ${invalidCompanies.length}件)`);
    }
  }

  console.log(`✅ 検索完了: 総数 ${totalCount}件、問題のあるドキュメント ${invalidCompanies.length}件`);
  return invalidCompanies;
}

async function deleteInvalidCompanies(
  db: Firestore,
  invalidCompanies: InvalidCompany[],
  csvIndex: Map<string, CsvIndex[]>
): Promise<{
  deleted: number;
  csvMapping: Map<string, number>;
  deletionRecords: DeletionRecord[];
}> {
  const companiesCol = db.collection(COLLECTION_NAME);

  console.log(`\n🗑️  削除を開始します...`);
  console.log(`   対象: ${invalidCompanies.length}件`);
  console.log(`   モード: ${DRY_RUN ? 'DRY RUN (実際には削除しません)' : '実際に削除'}\n`);

  const csvMapping = new Map<string, number>(); // CSVファイル -> 削除数
  const deletionRecords: DeletionRecord[] = [];
  let deletedCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  let processedCount = 0;
  let matchedCount = 0;

  for (const company of invalidCompanies) {
    if (!company.corporateNumber) continue;

    processedCount++;
    if (processedCount % 100 === 0) {
      console.log(`  処理中: ${processedCount}/${invalidCompanies.length}件 (削除済み: ${deletedCount}件, マッチ: ${matchedCount}件)`);
    }

    // インデックスから検索
    const csvMatches = csvIndex.get(company.corporateNumber) || [];
    const csvFiles = Array.from(new Set(csvMatches.map(m => m.csvFile)));

    if (csvFiles.length > 0) {
      matchedCount++;
    }

    // CSVファイル別にカウント
    for (const csvFile of csvFiles) {
      if (!csvMapping.has(csvFile)) {
        csvMapping.set(csvFile, 0);
      }
      csvMapping.set(csvFile, csvMapping.get(csvFile)! + 1);
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
    if (!DRY_RUN) {
      const docRef = companiesCol.doc(company.docId);
      batch.delete(docRef);
      batchCount++;
      deletedCount++;

      // バッチ制限に達したらコミット
      if (batchCount >= BATCH_SIZE) {
        await batch.commit();
        console.log(`  ✅ 削除済み: ${deletedCount}/${invalidCompanies.length}件`);
        batch = db.batch();
        batchCount = 0;
      }
    } else {
      deletedCount++;
      if (deletedCount <= 10) {
        console.log(`  [DRY RUN] 削除予定: ${company.docId} - "${company.name}" (${csvFiles.join(', ')})`);
      }
    }
  }

  // 残りのバッチをコミット
  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 削除済み: ${deletedCount}/${invalidCompanies.length}件`);
  }

  if (DRY_RUN) {
    console.log(`\n⚠️  DRY RUNモード: 実際には削除していません`);
    console.log(`   実際に削除するには --dry-run フラグを外してください`);
  } else {
    console.log(`\n✅ 削除完了: ${deletedCount}件のドキュメントを削除しました`);
    console.log(`   CSVマッチ: ${matchedCount}件`);
  }

  return {
    deleted: deletedCount,
    csvMapping,
    deletionRecords,
  };
}

async function reimportCsvFiles(
  csvMapping: Map<string, number>,
  csvDir: string
): Promise<void> {
  if (DRY_RUN) {
    console.log(`\n⚠️  DRY RUNモード: 再インポートは実行しません`);
    return;
  }

  // CSVファイルを削除数が多い順にソート
  const csvFiles = Array.from(csvMapping.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([file]) => file);

  if (csvFiles.length === 0) {
    console.log('\n📋 再インポートが必要なCSVファイルはありません');
    return;
  }

  console.log('\n📋 再インポートが必要なCSVファイル:');
  console.log('-'.repeat(80));
  for (const file of csvFiles) {
    const count = csvMapping.get(file)!;
    console.log(`  - ${file}: ${count}件削除された`);
  }
  console.log('');

  const serviceAccountKey = process.env.GOOGLE_APPLICATION_CREDENTIALS || './albert-ma-firebase-adminsdk-iat1k-a64039899f.json';
  const timestamp = Date.now();
  const logFile = `reimport_log_${timestamp}.txt`;

  // 各CSVファイルを再インポート
  for (const csvFile of csvFiles) {
    const csvPath = path.join(csvDir, csvFile);

    if (!fs.existsSync(csvPath)) {
      console.log(`⚠️  ファイルが見つかりません: ${csvPath}`);
      continue;
    }

    const count = csvMapping.get(csvFile)!;
    console.log(`\n📄 インポート中: ${csvFile} (${count}件削除された)`);
    console.log('-'.repeat(80));

    try {
      const command = `GOOGLE_APPLICATION_CREDENTIALS=${serviceAccountKey} npx ts-node scripts/import_companies_from_csv.ts ${csvPath}`;
      execSync(command, {
        stdio: 'inherit',
        cwd: process.cwd(),
      });
      console.log(`✅ ${csvFile} のインポート完了`);
      fs.appendFileSync(logFile, `✅ ${csvFile} のインポート完了\n`, 'utf8');
    } catch (error) {
      console.error(`❌ ${csvFile} のインポートに失敗しました:`, error);
      fs.appendFileSync(logFile, `❌ ${csvFile} のインポートに失敗: ${error}\n`, 'utf8');
    }
  }

  console.log(`\n✅ すべての再インポートが完了しました`);
  console.log(`📝 ログファイル: ${logFile}`);
}

async function main() {
  // 引数からCSVディレクトリを取得（--dry-run以外の最初の引数）
  const args = process.argv.slice(2).filter(arg => !arg.startsWith('--'));
  const csvDir = args[0] || './csv';

  if (!fs.existsSync(csvDir)) {
    console.error(`❌ エラー: CSVディレクトリが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  // Firebase初期化
  initFirebase();
  const db = admin.firestore();

  // CSVインデックスを構築
  const csvIndex = buildCsvIndex(csvDir);

  // 問題のあるドキュメントを検索
  const invalidCompanies = await findInvalidCompanies(db);

  if (invalidCompanies.length === 0) {
    console.log('\n✅ 問題のあるドキュメントは見つかりませんでした。');
    return;
  }

  // 削除実行
  const result = await deleteInvalidCompanies(db, invalidCompanies, csvIndex);

  // 結果を保存
  const timestamp = Date.now();
  const resultPath = `delete_and_reimport_result_${timestamp}.json`;

  const resultData = {
    summary: {
      totalFound: invalidCompanies.length,
      totalDeleted: result.deleted,
      deletedAt: new Date().toISOString(),
      dryRun: DRY_RUN,
    },
    csvMapping: Object.fromEntries(result.csvMapping),
    deletionRecords: result.deletionRecords,
  };

  fs.writeFileSync(resultPath, JSON.stringify(resultData, null, 2), 'utf8');
  console.log(`\n💾 削除結果を保存しました: ${resultPath}`);

  // CSVファイル別の内訳を表示
  if (result.csvMapping.size > 0) {
    console.log(`\n📁 CSVファイル別の削除数:`);
    const sortedFiles = Array.from(result.csvMapping.entries()).sort((a, b) => b[1] - a[1]);
    for (const [file, count] of sortedFiles) {
      console.log(`  ${file}: ${count}件`);
    }
  }

  // 再インポート実行
  await reimportCsvFiles(result.csvMapping, csvDir);
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
