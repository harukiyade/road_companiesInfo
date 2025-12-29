/* 
  companies_newコレクションのnameフィールドに正しい企業名が入っていない
  ドキュメントを全量洗い出し、どのCSVからインポート可能かを判断するスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/export_and_trace_invalid_names.ts [csv-dir] [--output result.json]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  QueryDocumentSnapshot,
} from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";

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
  '住宅供給公社', '土地開発公社', '地方公営企業',
  '公認会計士', '税理士', '司法書士', '行政書士',
  '事務所', '法律事務所', '会計事務所', '税理士事務所',
];

const OLD_STYLE_SUFFIXES = [
  '株式會社', '有限會社', '合資會社', '合名會社'
];

// 代表者名や役員名のパターン
const PERSON_NAME_PATTERNS = [
  /^（取）.*/,  /^\(取\).*/,  /^（専）.*/,  /^（常）.*/,  /^（代会）.*/,
  /^\(社長\).*/, /^（社長）.*/, /^\d{4}年\d{1,2}月\d{1,2}日$/, /^\d{4}\/\d{1,2}\/\d{1,2}$/,
];

interface InvalidCompany {
  docId: string;
  name: string;
  corporateNumber: string | null;
  issueType: 'no_corporate_suffix' | 'person_name' | 'business_description' | 'empty' | 'other';
  prefecture: string | null;
  address: string | null;
  headquartersAddress: string | null;
  representativeName: string | null;
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
  canReimport: boolean;
}

function hasCorporateSuffix(name: string | null | undefined): boolean {
  if (!name || typeof name !== 'string') return false;
  const trimmed = name.trim();
  if (!trimmed) return false;

  for (const suffix of CORPORATE_SUFFIXES) {
    if (trimmed.includes(suffix)) return true;
  }
  for (const suffix of OLD_STYLE_SUFFIXES) {
    if (trimmed.includes(suffix)) return true;
  }
  return false;
}

function isLikelyPersonName(name: string): boolean {
  if (!name || typeof name !== 'string') return false;
  const trimmed = name.trim();

  for (const pattern of PERSON_NAME_PATTERNS) {
    if (pattern.test(trimmed)) return true;
  }

  if (trimmed.includes('，') || trimmed.includes(',')) {
    const parts = trimmed.split(/[，,]/);
    if (parts.length >= 2 && parts.every(p => p.trim().length <= 10)) {
      return true;
    }
  }
  return false;
}

function isLikelyBusinessDescription(name: string): boolean {
  if (!name || typeof name !== 'string') return false;
  const trimmed = name.trim();
  if (hasCorporateSuffix(trimmed)) return false;

  const businessKeywords = [
    '業務', '代行', '製造', '販売', '卸売', '小売', '運送', '建設',
    '工事', '設計', '開発', '管理', '運営', 'サービス', '事業', '業',
    '調達', 'メンテナンス', 'の運営', 'を行う', 'を手掛ける', 'を担当', 'を提供',
  ];

  for (const keyword of businessKeywords) {
    if (trimmed.includes(keyword)) return true;
  }

  if ((trimmed.includes('，') || trimmed.includes(',')) && trimmed.length > 20) {
    return true;
  }

  if (trimmed.length >= 30) return true;
  return false;
}

function classifyIssue(name: string | null | undefined): InvalidCompany['issueType'] {
  if (!name || typeof name !== 'string' || !name.trim()) return 'empty';
  const trimmed = name.trim();

  if (isLikelyPersonName(trimmed)) return 'person_name';
  if (isLikelyBusinessDescription(trimmed)) return 'business_description';
  if (!hasCorporateSuffix(trimmed)) return 'no_corporate_suffix';
  return 'other';
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

function buildCsvIndex(csvDir: string): Map<string, CsvIndex[]> {
  console.log('\n📚 CSVファイルをインデックス化中（すべての法人番号形式に対応）...');
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

      if (!corporateNumberCol) {
        const lines = content.split('\n');
        if (lines.length > 0) {
          const headers = lines[0].split(',');
          for (let i = 0; i < headers.length; i++) {
            if (headers[i].includes('法人番号')) corporateNumberCol = headers[i];
            if (headers[i].includes('会社名') || headers[i].includes('社名')) {
              companyNameCol = headers[i];
            }
          }
        }
      }

      if (!corporateNumberCol) continue;

      for (let i = 0; i < records.length; i++) {
        const record = records[i] as Record<string, any>;
        const recordCorpNum = String(record[corporateNumberCol] || '').trim();

        // すべての法人番号形式をインデックス化（「9」で始まる13桁だけでなく）
        if (recordCorpNum && /^\d+$/.test(recordCorpNum)) {
          const companyName = companyNameCol ? String(record[companyNameCol] || '').trim() : '';

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
      // エラーは無視
    }
  }

  console.log(`✅ インデックス化完了: ${index.size}件の法人番号をインデックス化`);
  return index;
}

async function findInvalidCompanies(db: Firestore): Promise<InvalidCompany[]> {
  const companiesCol = db.collection(COLLECTION_NAME);
  const invalidCompanies: InvalidCompany[] = [];

  console.log('\n🔍 companies_newコレクションを全件スキャン中...');

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
    if (snapshot.empty) break;

    totalCount += snapshot.size;

    for (const doc of snapshot.docs) {
      const data = doc.data();
      const name = data.name;

      if (!hasCorporateSuffix(name)) {
        invalidCompanies.push({
          docId: doc.id,
          name: name || '(空)',
          corporateNumber: data.corporateNumber || null,
          issueType: classifyIssue(name),
          prefecture: data.prefecture || null,
          address: data.address || null,
          headquartersAddress: data.headquartersAddress || null,
          representativeName: data.representativeName || null,
        });
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    if (snapshot.size < batchSize) break;

    if (totalCount % 10000 === 0) {
      console.log(`  処理中: ${totalCount}件 (問題: ${invalidCompanies.length}件)`);
    }
  }

  console.log(`✅ スキャン完了: 総数 ${totalCount}件、問題のあるドキュメント ${invalidCompanies.length}件`);
  return invalidCompanies;
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
        canReimport: false,
      });
      continue;
    }

    const csvMatches = csvIndex.get(company.corporateNumber) || [];

    if (csvMatches.length > 0) {
      matchedCount++;
      const csvFiles = Array.from(new Set(csvMatches.map(m => m.csvFile)));

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
        canReimport: true,
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
        canReimport: false,
      });
    }
  }

  console.log(`\n✅ 紐付け完了:`);
  console.log(`   マッチしたドキュメント: ${matchedCount}件 (再インポート可能)`);
  console.log(`   マッチしなかったドキュメント: ${unmatchedCount}件`);

  return { traced, csvMapping };
}

function printReport(traced: TraceResult[], csvMapping: Map<string, number>): void {
  console.log('\n' + '='.repeat(80));
  console.log('📋 分析レポート');
  console.log('='.repeat(80));
  console.log(`\n総問題数: ${traced.length}件`);
  console.log(`再インポート可能: ${traced.filter(t => t.canReimport).length}件`);
  console.log(`再インポート不可: ${traced.filter(t => !t.canReimport).length}件\n`);

  const sortedFiles = Array.from(csvMapping.entries()).sort((a, b) => b[1] - a[1]);
  console.log(`📁 CSVファイル別の内訳（${sortedFiles.length}ファイル）:\n`);

  for (const [file, count] of sortedFiles.slice(0, 30)) {
    console.log(`  ${file}: ${count}件`);
  }
  if (sortedFiles.length > 30) {
    console.log(`  ... 他 ${sortedFiles.length - 30}ファイル`);
  }
}

async function main() {
  const csvDir = process.argv[2] || './csv';
  const outputIndex = process.argv.indexOf('--output');
  const outputPath = outputIndex !== -1 && outputIndex + 1 < process.argv.length
    ? process.argv[outputIndex + 1]
    : `invalid_names_traced_${Date.now()}.json`;

  if (!fs.existsSync(csvDir)) {
    console.error(`❌ エラー: CSVディレクトリが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  initFirebase();
  const db = admin.firestore();

  // 問題のあるドキュメントを検索
  const invalidCompanies = await findInvalidCompanies(db);

  if (invalidCompanies.length === 0) {
    console.log('\n✅ 問題のあるドキュメントは見つかりませんでした。');
    return;
  }

  // CSVインデックスを構築
  const csvIndex = buildCsvIndex(csvDir);

  // CSVファイルとの紐付け
  const { traced, csvMapping } = traceCompaniesToCsv(invalidCompanies, csvIndex);

  // レポートを表示
  printReport(traced, csvMapping);

  // CSVファイル別の詳細情報を集計
  const csvFileDetails = new Map<string, {
    count: number;
    byIssueType: Record<string, number>;
  }>();

  for (const [file, count] of csvMapping.entries()) {
    const fileTraced = traced.filter(t => t.csvFiles.includes(file));
    const byIssueType: Record<string, number> = {};

    for (const item of fileTraced) {
      byIssueType[item.issueType] = (byIssueType[item.issueType] || 0) + 1;
    }

    csvFileDetails.set(file, { count, byIssueType });
  }

  // 結果を保存
  const result = {
    summary: {
      total: traced.length,
      canReimport: traced.filter(t => t.canReimport).length,
      cannotReimport: traced.filter(t => !t.canReimport).length,
      csvFileCount: csvMapping.size,
      generatedAt: new Date().toISOString(),
    },
    csvMapping: Object.fromEntries(
      Array.from(csvFileDetails.entries()).map(([file, details]) => [file, details])
    ),
    csvFileCounts: Object.fromEntries(csvMapping),
  };

  fs.writeFileSync(outputPath, JSON.stringify(result, null, 2), 'utf8');
  console.log(`\n💾 結果を保存しました: ${outputPath}`);
  console.log(`   ファイルサイズ: ${(fs.statSync(outputPath).size / 1024 / 1024).toFixed(2)} MB`);

  // 再インポート可能なCSVファイルのリストを出力
  if (csvMapping.size > 0) {
    const reimportableFiles = Array.from(csvMapping.entries())
      .sort((a, b) => b[1] - a[1])
      .map(([file]) => file);

    console.log(`\n📝 再インポート可能なCSVファイル（${reimportableFiles.length}ファイル）:`);
    console.log('-'.repeat(80));
    for (const file of reimportableFiles.slice(0, 30)) {
      const count = csvMapping.get(file)!;
      console.log(`  ${file}: ${count}件`);
    }
    if (reimportableFiles.length > 30) {
      console.log(`  ... 他 ${reimportableFiles.length - 30}ファイル`);
    }
  }
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
