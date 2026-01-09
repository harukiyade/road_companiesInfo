/* 
  companies_newコレクションのnameフィールドに「株式会社」などの法人格が含まれない
  ドキュメントを洗い出し、どのCSVからインポートされたかを分析するスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/find_invalid_company_names.ts [--dry-run] [--output report.json]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  CollectionReference,
  QueryDocumentSnapshot,
} from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

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
  '合名会社', '合資会社', '合同会社',
  '商工会議所', '商工会', '工業組合', '事業協同組合',
  '森林組合', '農業共済組合', '漁業協同組合',
  '住宅供給公社', '土地開発公社', '地方公営企業'
];

// 旧字体の法人格もチェック
const OLD_STYLE_SUFFIXES = [
  '株式會社', '有限會社', '合資會社', '合名會社'
];

interface InvalidCompany {
  docId: string;
  name: string;
  corporateNumber: string | null;
  sourceFile: string | null;
  sourceRow: number | null;
  prefecture: string | null;
  address: string | null;
  representativeName: string | null;
  createdAt: any;
  updatedAt: any;
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

  console.log("🔍 companies_newコレクションをスキャン中...");

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
        
        invalidCompanies.push({
          docId: doc.id,
          name: name || '(空)',
          corporateNumber: data.corporateNumber || null,
          sourceFile: source.file,
          sourceRow: source.row,
          prefecture: data.prefecture || null,
          address: data.address || null,
          representativeName: data.representativeName || null,
          createdAt: data.createdAt || null,
          updatedAt: data.updatedAt || null,
        });
      }

      if (processedCount % 1000 === 0) {
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

  // CSVファイルごとにグループ化
  const grouped = groupByCsvFile(invalidCompanies);

  console.log("📁 CSVファイル別の内訳:");
  console.log("-".repeat(80));
  
  const sortedFiles = Array.from(grouped.entries()).sort((a, b) => b[1].length - a[1].length);
  
  for (const [file, companies] of sortedFiles) {
    console.log(`\n  ${file}: ${companies.length}件`);
    
    // 最初の5件を表示
    for (const company of companies.slice(0, 5)) {
      console.log(`    - ${company.name} (ID: ${company.docId})`);
      if (company.corporateNumber) {
        console.log(`      法人番号: ${company.corporateNumber}`);
      }
      if (company.prefecture) {
        console.log(`      都道府県: ${company.prefecture}`);
      }
    }
    
    if (companies.length > 5) {
      console.log(`    ... 他 ${companies.length - 5}件`);
    }
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
  const grouped = groupByCsvFile(invalidCompanies);
  
  const report = {
    summary: {
      total: invalidCompanies.length,
      byFile: Object.fromEntries(
        Array.from(grouped.entries()).map(([file, companies]) => [
          file,
          companies.length,
        ])
      ),
    },
    companies: invalidCompanies,
    groupedByFile: Object.fromEntries(
      Array.from(grouped.entries()).map(([file, companies]) => [
        file,
        companies.map(c => ({
          docId: c.docId,
          name: c.name,
          corporateNumber: c.corporateNumber,
          sourceRow: c.sourceRow,
          prefecture: c.prefecture,
        })),
      ])
    ),
  };

  fs.writeFileSync(outputPath, JSON.stringify(report, null, 2), 'utf8');
  console.log(`\n💾 レポートを保存しました: ${outputPath}`);
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

  console.log('🚀 問題のある会社名を検索中...\n');

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
    const defaultOutputPath = `invalid_company_names_report_${Date.now()}.json`;
    await saveReport(invalidCompanies, defaultOutputPath);
  }

  // CSVファイルリストを出力
  const grouped = groupByCsvFile(invalidCompanies);
  const csvFiles = Array.from(grouped.keys()).filter(f => f !== '(不明)');
  
  if (csvFiles.length > 0) {
    console.log('\n📝 再インポートが必要なCSVファイル:');
    console.log('-'.repeat(80));
    for (const file of csvFiles.sort()) {
      const count = grouped.get(file)!.length;
      console.log(`  - ${file} (${count}件)`);
    }
  }
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
