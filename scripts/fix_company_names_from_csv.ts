/* 
  DB内のnameフィールドに代表者名や役員名が入っている問題を修正するスクリプト
  
  CSVファイルから正しい企業名を取得してnameフィールドを修正

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_company_names_from_csv.ts [--dry-run] [csv-dir]
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

function buildCsvIndex(csvDir: string): Map<string, { companyName: string; csvFile: string; rowNumber: number }> {
  console.log('\n📚 CSVファイルをインデックス化中...');
  const index = new Map<string, { companyName: string; csvFile: string; rowNumber: number }>();
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

      if (!corporateNumberCol || !companyNameCol) continue;

      // レコードをインデックスに追加
      for (let i = 0; i < records.length; i++) {
        const record = records[i] as Record<string, any>;
        const recordCorpNum = String(record[corporateNumberCol] || '').trim();
        const recordCompanyName = String(record[companyNameCol] || '').trim();

        if (recordCorpNum && /^9\d{12}$/.test(recordCorpNum) && recordCompanyName) {
          // 正しい企業名かどうかをチェック
          if (hasCorporateSuffix(recordCompanyName) && !isLikelyPersonName(recordCompanyName)) {
            // 既にインデックスにある場合は、より長い（詳細な）企業名を優先
            const existing = index.get(recordCorpNum);
            if (!existing || recordCompanyName.length > existing.companyName.length) {
              index.set(recordCorpNum, {
                companyName: recordCompanyName,
                csvFile,
                rowNumber: i + 2,
              });
            }
          }
        }
      }
    } catch (error) {
      // CSVファイルの読み込みエラーは無視
    }
  }

  console.log(`✅ インデックス化完了: ${index.size}件の法人番号をインデックス化`);
  return index;
}

async function findInvalidNames(
  db: Firestore
): Promise<Array<{ docId: string; name: string; corporateNumber: string | null }>> {
  const companiesCol = db.collection(COLLECTION_NAME);
  const invalidDocs: Array<{ docId: string; name: string; corporateNumber: string | null }> = [];

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
            invalidDocs.push({
              docId: doc.id,
              name: name.trim(),
              corporateNumber: String(corporateNumber),
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
      console.log(`  処理中: ${totalCount}件 (問題: ${invalidDocs.length}件)`);
    }
  }

  console.log(`✅ 検索完了: 総数 ${totalCount}件、問題のあるドキュメント ${invalidDocs.length}件`);
  return invalidDocs;
}

async function fixCompanyNames(
  db: Firestore,
  invalidDocs: Array<{ docId: string; name: string; corporateNumber: string | null }>,
  csvIndex: Map<string, { companyName: string; csvFile: string; rowNumber: number }>
): Promise<{ fixed: number; notFound: number; csvMapping: Map<string, number> }> {
  const companiesCol = db.collection(COLLECTION_NAME);
  
  console.log(`\n🔧 修正を開始します...`);
  console.log(`   対象: ${invalidDocs.length}件`);
  console.log(`   モード: ${DRY_RUN ? 'DRY RUN (実際には修正しません)' : '実際に修正'}\n`);

  let fixedCount = 0;
  let notFoundCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  const csvMapping = new Map<string, number>();

  for (let i = 0; i < invalidDocs.length; i++) {
    const doc = invalidDocs[i];
    if (!doc.corporateNumber) continue;

    if ((i + 1) % 100 === 0) {
      console.log(`  処理中: ${i + 1}/${invalidDocs.length}件 (修正済み: ${fixedCount}件, 見つからず: ${notFoundCount}件)`);
    }

    // CSVインデックスから正しい企業名を検索
    const csvData = csvIndex.get(doc.corporateNumber);

    if (csvData && csvData.companyName) {
      const docRef = companiesCol.doc(doc.docId);

      if (DRY_RUN) {
        console.log(`  [DRY RUN] 修正予定: ${doc.docId}`);
        console.log(`    現在: "${doc.name}"`);
        console.log(`    修正後: "${csvData.companyName}" (${csvData.csvFile} 行${csvData.rowNumber})`);
      } else {
        batch.update(docRef, {
          name: csvData.companyName,
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        });
        batchCount++;
        fixedCount++;

        // CSVファイル別にカウント
        if (!csvMapping.has(csvData.csvFile)) {
          csvMapping.set(csvData.csvFile, 0);
        }
        csvMapping.set(csvData.csvFile, csvMapping.get(csvData.csvFile)! + 1);

        // バッチ制限に達したらコミット
        if (batchCount >= BATCH_SIZE) {
          await batch.commit();
          console.log(`  ✅ 修正済み: ${fixedCount}/${invalidDocs.length}件`);
          batch = db.batch();
          batchCount = 0;
        }
      }
    } else {
      notFoundCount++;
      if (notFoundCount <= 10) {
        console.log(`  ⚠️  CSVで見つかりません: ${doc.docId} - "${doc.name}" (法人番号: ${doc.corporateNumber})`);
      }
    }
  }

  // 残りのバッチをコミット
  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 修正済み: ${fixedCount}/${invalidDocs.length}件`);
  }

  if (DRY_RUN) {
    console.log(`\n⚠️  DRY RUNモード: 実際には修正していません`);
    console.log(`   実際に修正するには --dry-run フラグを外してください`);
  } else {
    console.log(`\n✅ 修正完了: ${fixedCount}件のドキュメントを修正しました`);
    console.log(`   CSVで見つからなかった: ${notFoundCount}件`);
  }

  return { fixed: fixedCount, notFound: notFoundCount, csvMapping };
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

async function main() {
  const csvDir = process.argv[2] || './csv';

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
  const invalidDocs = await findInvalidNames(db);

  if (invalidDocs.length === 0) {
    console.log('\n✅ 問題のあるドキュメントは見つかりませんでした。');
    return;
  }

  // 修正実行
  const result = await fixCompanyNames(db, invalidDocs, csvIndex);

  // 結果を保存
  const timestamp = Date.now();
  const resultPath = `fix_company_names_result_${timestamp}.json`;

  const resultData = {
    summary: {
      totalFound: invalidDocs.length,
      totalFixed: result.fixed,
      notFound: result.notFound,
      fixedAt: new Date().toISOString(),
      dryRun: DRY_RUN,
    },
    csvMapping: Object.fromEntries(result.csvMapping),
    samples: invalidDocs.slice(0, 100).map(doc => {
      const csvData = csvIndex.get(doc.corporateNumber || '');
      return {
        docId: doc.docId,
        oldName: doc.name,
        newName: csvData?.companyName || null,
        corporateNumber: doc.corporateNumber,
        csvFile: csvData?.csvFile || null,
      };
    }),
  };

  fs.writeFileSync(resultPath, JSON.stringify(resultData, null, 2), 'utf8');
  console.log(`\n💾 結果を保存しました: ${resultPath}`);

  // CSVファイル別の内訳を表示
  if (result.csvMapping.size > 0) {
    console.log(`\n📁 CSVファイル別の修正数:`);
    const sortedFiles = Array.from(result.csvMapping.entries()).sort((a, b) => b[1] - a[1]);
    for (const [file, count] of sortedFiles) {
      console.log(`  ${file}: ${count}件`);
    }
  }
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
