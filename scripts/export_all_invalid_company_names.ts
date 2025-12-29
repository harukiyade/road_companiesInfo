/* 
  companies_newコレクションのnameフィールドに「株式会社」などの法人格が含まれない
  ドキュメントを全量出力するスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/export_all_invalid_company_names.ts [--output report.json]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  QueryDocumentSnapshot,
} from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";

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
  '公認会計士', '税理士', '司法書士', '行政書士', // 専門職事務所
  '事務所', '法律事務所', '会計事務所', '税理士事務所',
];

// 旧字体の法人格もチェック
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
  sourceFile: string | null;
  sourceRow: number | null;
  prefecture: string | null;
  address: string | null;
  headquartersAddress: string | null;
  representativeName: string | null;
  createdAt: any;
  updatedAt: any;
  // 問題の種類を分類
  issueType: 'no_corporate_suffix' | 'person_name' | 'business_description' | 'empty' | 'other';
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

function isLikelyBusinessDescription(name: string): boolean {
  if (!name || typeof name !== 'string') {
    return false;
  }

  const trimmed = name.trim();

  // 法人格を含む場合は事業内容ではない
  if (hasCorporateSuffix(trimmed)) {
    return false;
  }

  // 事業内容を示すキーワード
  const businessKeywords = [
    '業務', '代行', '製造', '販売', '卸売', '小売', '運送', '建設',
    '工事', '設計', '開発', '管理', '運営', 'サービス', '事業', '業',
    '調達', 'メンテナンス', '製造・', '販売、', '運送、', '工事、',
    'の運営', 'を行う', 'を手掛ける', 'を担当', 'を提供', 'の輸入',
    'の輸出', 'の修理', 'の調査', 'の研究', 'の研修', 'の販売',
  ];

  for (const keyword of businessKeywords) {
    if (trimmed.includes(keyword)) {
      return true;
    }
  }

  // カンマ区切りで複数の事業内容が列挙されている場合
  if ((trimmed.includes('，') || trimmed.includes(',')) && trimmed.length > 20) {
    return true;
  }

  // 長い説明文（30文字以上）の場合は事業内容の可能性
  if (trimmed.length >= 30) {
    return true;
  }

  return false;
}

function classifyIssue(name: string | null | undefined): InvalidCompany['issueType'] {
  if (!name || typeof name !== 'string' || !name.trim()) {
    return 'empty';
  }

  const trimmed = name.trim();

  if (isLikelyPersonName(trimmed)) {
    return 'person_name';
  }

  if (isLikelyBusinessDescription(trimmed)) {
    return 'business_description';
  }

  if (!hasCorporateSuffix(trimmed)) {
    return 'no_corporate_suffix';
  }

  return 'other';
}

function extractSourceFile(doc: QueryDocumentSnapshot<DocumentData>): {
  file: string | null;
  row: number | null;
} {
  const data = doc.data();

  // lastImportSource フィールドを確認
  if (data?.lastImportSource) {
    return {
      file: data.lastImportSource.file || null,
      row: data.lastImportSource.row || null,
    };
  }

  // source フィールドを確認（旧形式）
  if (data?.source) {
    return {
      file: data.source.file || null,
      row: data.source.row || null,
    };
  }

  return { file: null, row: null };
}

async function findInvalidCompanies(
  db: Firestore
): Promise<InvalidCompany[]> {
  const companiesCol = db.collection(COLLECTION_NAME);
  const invalidCompanies: InvalidCompany[] = [];

  console.log("🔍 companies_newコレクションを全件スキャン中...");

  let totalCount = 0;
  let processedCount = 0;
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
      processedCount++;
      const data = doc.data();
      const name = data.name;

      // nameフィールドに法人格が含まれていない場合
      if (!hasCorporateSuffix(name)) {
        const source = extractSourceFile(doc);
        const issueType = classifyIssue(name);

        invalidCompanies.push({
          docId: doc.id,
          name: name || '(空)',
          corporateNumber: data.corporateNumber || null,
          sourceFile: source.file,
          sourceRow: source.row,
          prefecture: data.prefecture || null,
          address: data.address || null,
          headquartersAddress: data.headquartersAddress || null,
          representativeName: data.representativeName || null,
          createdAt: data.createdAt || null,
          updatedAt: data.updatedAt || null,
          issueType,
        });
      }

      if (processedCount % 10000 === 0) {
        console.log(`  処理中: ${processedCount}件 (問題: ${invalidCompanies.length}件)`);
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];

    if (snapshot.size < batchSize) {
      break;
    }
  }

  console.log(`\n✅ スキャン完了: 総数 ${totalCount}件、問題のあるドキュメント ${invalidCompanies.length}件`);

  return invalidCompanies;
}

function groupByIssueType(invalidCompanies: InvalidCompany[]): Map<string, InvalidCompany[]> {
  const grouped = new Map<string, InvalidCompany[]>();

  for (const company of invalidCompanies) {
    const type = company.issueType;
    if (!grouped.has(type)) {
      grouped.set(type, []);
    }
    grouped.get(type)!.push(company);
  }

  return grouped;
}

function groupByCsvFile(invalidCompanies: InvalidCompany[]): Map<string, InvalidCompany[]> {
  const grouped = new Map<string, InvalidCompany[]>();

  for (const company of invalidCompanies) {
    const file = company.sourceFile || '(不明)';
    if (!grouped.has(file)) {
      grouped.set(file, []);
    }
    grouped.get(file)!.push(company);
  }

  return grouped;
}

function printReport(invalidCompanies: InvalidCompany[]): void {
  console.log("\n" + "=".repeat(80));
  console.log("📋 分析レポート");
  console.log("=".repeat(80));
  console.log(`\n総問題数: ${invalidCompanies.length}件\n`);

  // 問題の種類別にグループ化
  const byIssueType = groupByIssueType(invalidCompanies);
  console.log("📊 問題の種類別内訳:");
  console.log("-".repeat(80));
  const issueTypeLabels: Record<string, string> = {
    'no_corporate_suffix': '法人格なし',
    'person_name': '個人名・役員名',
    'business_description': '事業内容',
    'empty': '空',
    'other': 'その他',
  };

  for (const [type, companies] of Array.from(byIssueType.entries()).sort((a, b) => b[1].length - a[1].length)) {
    const label = issueTypeLabels[type] || type;
    console.log(`\n  ${label}: ${companies.length}件`);
    
    // 最初の5件を表示
    for (const company of companies.slice(0, 5)) {
      console.log(`    - ${company.name.substring(0, 60)}${company.name.length > 60 ? '...' : ''} (ID: ${company.docId})`);
    }
    if (companies.length > 5) {
      console.log(`    ... 他 ${companies.length - 5}件`);
    }
  }

  // CSVファイルごとにグループ化
  const grouped = groupByCsvFile(invalidCompanies);
  console.log("\n\n📁 CSVファイル別の内訳:");
  console.log("-".repeat(80));

  const sortedFiles = Array.from(grouped.entries()).sort((a, b) => b[1].length - a[1].length);

  for (const [file, companies] of sortedFiles.slice(0, 20)) {
    console.log(`\n  ${file}: ${companies.length}件`);
  }

  if (sortedFiles.length > 20) {
    console.log(`\n  ... 他 ${sortedFiles.length - 20}ファイル`);
  }

  // 不明なソースのドキュメント
  const unknownSource = invalidCompanies.filter(c => !c.sourceFile);
  if (unknownSource.length > 0) {
    console.log(`\n⚠️  ソース不明: ${unknownSource.length}件`);
  }
}

async function saveReport(
  invalidCompanies: InvalidCompany[],
  outputPath: string
): Promise<void> {
  const byIssueType = groupByIssueType(invalidCompanies);
  const byCsvFile = groupByCsvFile(invalidCompanies);

  const report = {
    summary: {
      total: invalidCompanies.length,
      byIssueType: Object.fromEntries(
        Array.from(byIssueType.entries()).map(([type, companies]) => [
          type,
          companies.length,
        ])
      ),
      byFile: Object.fromEntries(
        Array.from(byCsvFile.entries()).map(([file, companies]) => [
          file,
          companies.length,
        ])
      ),
      generatedAt: new Date().toISOString(),
    },
    companies: invalidCompanies,
    groupedByIssueType: Object.fromEntries(
      Array.from(byIssueType.entries()).map(([type, companies]) => [
        type,
        companies.map(c => ({
          docId: c.docId,
          name: c.name,
          corporateNumber: c.corporateNumber,
          sourceFile: c.sourceFile,
          sourceRow: c.sourceRow,
          prefecture: c.prefecture,
        })),
      ])
    ),
    groupedByFile: Object.fromEntries(
      Array.from(byCsvFile.entries()).map(([file, companies]) => [
        file,
        companies.map(c => ({
          docId: c.docId,
          name: c.name,
          corporateNumber: c.corporateNumber,
          sourceRow: c.sourceRow,
          prefecture: c.prefecture,
          issueType: c.issueType,
        })),
      ])
    ),
  };

  fs.writeFileSync(outputPath, JSON.stringify(report, null, 2), 'utf8');
  console.log(`\n💾 レポートを保存しました: ${outputPath}`);
  console.log(`   ファイルサイズ: ${(fs.statSync(outputPath).size / 1024 / 1024).toFixed(2)} MB`);
}

async function main() {
  // Firebase初期化
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

  const db = admin.firestore();

  console.log('🚀 問題のある会社名を全件検索中...\n');

  // 問題のあるドキュメントを検索
  const invalidCompanies = await findInvalidCompanies(db);

  if (invalidCompanies.length === 0) {
    console.log('\n✅ 問題のあるドキュメントは見つかりませんでした。');
    return;
  }

  // レポートを表示
  printReport(invalidCompanies);

  // レポートを保存
  const outputIndex = process.argv.indexOf('--output');
  if (outputIndex !== -1 && outputIndex + 1 < process.argv.length) {
    const outputPath = process.argv[outputIndex + 1];
    await saveReport(invalidCompanies, outputPath);
  } else {
    const defaultOutputPath = `invalid_company_names_full_report_${Date.now()}.json`;
    await saveReport(invalidCompanies, defaultOutputPath);
  }

  // CSVファイルリストを出力
  const grouped = groupByCsvFile(invalidCompanies);
  const csvFiles = Array.from(grouped.keys()).filter(f => f !== '(不明)');

  if (csvFiles.length > 0) {
    console.log('\n📝 問題のあるドキュメントが含まれるCSVファイル（上位20件）:');
    console.log('-'.repeat(80));
    for (const file of csvFiles.slice(0, 20).sort()) {
      const count = grouped.get(file)!.length;
      console.log(`  - ${file} (${count}件)`);
    }
    if (csvFiles.length > 20) {
      console.log(`  ... 他 ${csvFiles.length - 20}ファイル`);
    }
  }
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
