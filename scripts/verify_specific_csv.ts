#!/usr/bin/env ts-node
/**
 * 特定のCSVファイルの全行がFirestoreに入っているか詳細確認
 * 
 * 使い方:
 *   npx ts-node scripts/verify_specific_csv.ts csv/107.csv
 *   npx ts-node scripts/verify_specific_csv.ts csv/130.csv --verbose
 */

import * as fs from 'fs';
import * as path from 'path';
import admin from 'firebase-admin';
import { parse } from 'csv-parse/sync';

admin.initializeApp();
const db = admin.firestore();
const companiesCol = db.collection('companies_new');

interface CsvRow {
  [key: string]: string;
}

const VERBOSE = process.argv.includes('--verbose');
const csvPath = process.argv[2];

if (!csvPath) {
  console.error('❌ エラー: CSVファイルパスを指定してください');
  console.error('\n使い方:');
  console.error('  npx ts-node scripts/verify_specific_csv.ts csv/107.csv');
  console.error('  npx ts-node scripts/verify_specific_csv.ts csv/130.csv --verbose');
  process.exit(1);
}

async function verifyCSV() {
  console.log(`\n📁 ${path.basename(csvPath)} の確認\n`);
  
  if (!fs.existsSync(csvPath)) {
    console.error(`❌ ファイルが見つかりません: ${csvPath}`);
    process.exit(1);
  }

  const buf = fs.readFileSync(csvPath);
  const records: CsvRow[] = parse(buf, {
    columns: true,
    skip_empty_lines: true,
    relax_quotes: true,
    relax_column_count: true,
    skip_records_with_error: true,
  });

  console.log(`📊 CSV総行数: ${records.length}行\n`);

  let found = 0;
  let notFound = 0;
  const notFoundList: string[] = [];

  for (let i = 0; i < records.length; i++) {
    const row = records[i];
    const companyName = row['企業名'] || row['会社名'] || row['name'];
    const corpNum = row['法人番号'] || row['corporateNumber'];

    if (!companyName) {
      if (VERBOSE) console.log(`⚠️  行${i + 1}: 企業名なし`);
      continue;
    }

    // Firestoreで検索
    let exists = false;

    // 1. 法人番号で検索
    if (corpNum && corpNum.trim() && corpNum.length === 13) {
      const byId = await companiesCol.doc(corpNum.trim()).get();
      if (byId.exists) {
        exists = true;
      } else {
        const snap = await companiesCol.where('corporateNumber', '==', corpNum.trim()).limit(1).get();
        if (!snap.empty) {
          exists = true;
        }
      }
    }

    // 2. 企業名で検索
    if (!exists) {
      const snap = await companiesCol.where('name', '==', companyName.trim()).limit(1).get();
      if (!snap.empty) {
        exists = true;
      }
    }

    if (exists) {
      found++;
      if (VERBOSE) {
        console.log(`✅ 行${i + 1}: ${companyName}`);
      } else if ((i + 1) % 100 === 0) {
        console.log(`進行中... ${i + 1}/${records.length} (${Math.round((i+1)/records.length*100)}%)`);
      }
    } else {
      notFound++;
      notFoundList.push(`行${i + 1}: ${companyName} (法人番号: ${corpNum || 'なし'})`);
      if (VERBOSE) {
        console.log(`❌ 行${i + 1}: ${companyName} - Firestoreに見つかりません`);
      }
    }
  }

  // 結果サマリー
  console.log(`\n${'='.repeat(60)}`);
  console.log('📊 結果サマリー');
  console.log(`${'='.repeat(60)}`);
  console.log(`CSV総行数:        ${records.length}行`);
  console.log(`Firestore存在:    ${found}行 (${Math.round(found/records.length*100)}%)`);
  console.log(`Firestore未存在:  ${notFound}行 (${Math.round(notFound/records.length*100)}%)`);
  console.log(`${'='.repeat(60)}`);

  // 未存在リスト（最初の10件）
  if (notFoundList.length > 0) {
    console.log('\n❌ Firestoreに見つからなかった企業（最初の10件）:\n');
    notFoundList.slice(0, 10).forEach(item => {
      console.log(`  ${item}`);
    });
    if (notFoundList.length > 10) {
      console.log(`  ... 他 ${notFoundList.length - 10}件`);
    }
    console.log('\n💡 --verbose オプションで全件の詳細を確認できます');
  } else {
    console.log('\n🎉 全ての企業がFirestoreに存在します！');
  }

  console.log('');
}

verifyCSV()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ エラー:', err);
    process.exit(1);
  });

