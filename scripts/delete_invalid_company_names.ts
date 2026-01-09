/* 
  問題のある会社名（法人格が含まれない）のドキュメントを削除するスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_invalid_company_names.ts <report.json> [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import {
  Firestore,
  WriteBatch,
} from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const BATCH_SIZE = 500; // Firestoreのバッチ制限

interface InvalidCompany {
  docId: string;
  name: string;
  corporateNumber: string | null;
  sourceFile: string | null;
  sourceRow: number | null;
}

interface Report {
  summary: {
    total: number;
    byFile: Record<string, number>;
  };
  companies: InvalidCompany[];
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

async function deleteInvalidCompanies(
  db: Firestore,
  invalidCompanies: InvalidCompany[]
): Promise<void> {
  const companiesCol = db.collection(COLLECTION_NAME);
  
  console.log(`\n🗑️  削除を開始します...`);
  console.log(`   対象: ${invalidCompanies.length}件`);
  console.log(`   モード: ${DRY_RUN ? 'DRY RUN (実際には削除しません)' : '実際に削除'}\n`);

  let deletedCount = 0;
  let batch: WriteBatch = db.batch();
  let batchCount = 0;

  for (let i = 0; i < invalidCompanies.length; i++) {
    const company = invalidCompanies[i];
    const docRef = companiesCol.doc(company.docId);

    if (DRY_RUN) {
      console.log(`  [DRY RUN] 削除予定: ${company.docId} - ${company.name}`);
    } else {
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
  }
}

async function main() {
  const reportPath = process.argv[2];

  if (!reportPath) {
    console.error('❌ エラー: レポートファイルのパスが指定されていません');
    console.error('');
    console.error('使用方法:');
    console.error('  npx ts-node scripts/delete_invalid_company_names.ts <report.json> [--dry-run]');
    console.error('');
    console.error('例:');
    console.error('  npx ts-node scripts/delete_invalid_company_names.ts invalid_company_names_report_1234567890.json --dry-run');
    process.exit(1);
  }

  if (!fs.existsSync(reportPath)) {
    console.error(`❌ エラー: レポートファイルが見つかりません: ${reportPath}`);
    process.exit(1);
  }

  // レポートを読み込む
  const report: Report = JSON.parse(fs.readFileSync(reportPath, 'utf8'));

  console.log('📋 レポートを読み込みました');
  console.log(`   総問題数: ${report.summary.total}件`);
  console.log(`   CSVファイル数: ${Object.keys(report.summary.byFile).length}個`);

  // Firebase初期化
  initFirebase();
  const db = admin.firestore();

  // 削除実行
  await deleteInvalidCompanies(db, report.companies);

  // CSVファイルリストを表示
  const csvFiles = Object.keys(report.summary.byFile)
    .filter(f => f !== '(不明)')
    .sort();

  if (csvFiles.length > 0) {
    console.log('\n📝 再インポートが必要なCSVファイル:');
    console.log('-'.repeat(80));
    for (const file of csvFiles) {
      const count = report.summary.byFile[file];
      console.log(`  - csv/${file} (${count}件)`);
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
