#!/usr/bin/env ts-node
/**
 * タイプ別CSV→Firestore取り込み確認スクリプト
 * 
 * 各タイプの代表的なCSVから数社をサンプリングして、
 * Firestoreに正しくデータが入っているか確認します。
 */

import * as fs from 'fs';
import * as path from 'path';
import admin from 'firebase-admin';
import { parse } from 'csv-parse/sync';

admin.initializeApp();
const db = admin.firestore();
const companiesCol = db.collection('companies_new');

// タイプ別の代表的なCSVファイルと確認すべきフィールド
const TYPE_CONFIG = {
  'タイプA': {
    files: ['10.csv', '11.csv', '100.csv'],
    checkFields: ['name', 'corporateNumber', 'address', 'phoneNumber', 'companyUrl', 'businessDescriptions', 'representativeName'],
    description: '基本情報 + 営業種目'
  },
  'タイプB': {
    files: ['12.csv', '13.csv', '14.csv'],
    checkFields: ['name', 'corporateNumber', 'address', 'founding', 'dateOfEstablishment', 'representativeName'],
    description: '創業・設立あり'
  },
  'タイプC': {
    files: ['105.csv', '106.csv', '107.csv'],
    checkFields: ['name', 'corporateNumber', 'address', 'industry', 'capitalStock', 'latestRevenue'],
    description: '詳細情報（業種・資本金・売上）'
  },
  'タイプD': {
    files: ['111.csv', '112.csv', '113.csv'],
    checkFields: ['name', 'corporateNumber', 'address', 'suppliers', 'clients'],
    description: '取引先情報'
  },
  'タイプE': {
    files: ['116.csv', '117.csv'],
    checkFields: ['name', 'corporateNumber', 'address', 'email'],
    description: 'メールアドレスあり'
  },
  'タイプF': {
    files: ['124.csv', '125.csv'],
    checkFields: ['name', 'corporateNumber', 'address', 'companyDescription', 'overview'],
    description: '説明・概要あり'
  },
  'タイプG': {
    files: ['127.csv', '128.csv'],
    checkFields: ['name', 'corporateNumber', 'address', 'banks', 'latestRevenue', 'latestProfit'],
    description: '銀行・決算情報'
  },
  'タイプH': {
    files: ['130.csv', '131.csv'],
    checkFields: ['name', 'corporateNumber', 'address', 'industry1', 'industry2', 'executiveName1', 'executiveTitle1'],
    description: '業種展開・役員情報'
  },
  'タイプI': {
    files: ['132.csv'],
    checkFields: ['name', 'corporateNumber', 'address', 'fiscalMonth1', 'revenue1', 'profit1', 'fiscalMonth2', 'revenue2'],
    description: '決算月・売上・利益（複数年）'
  },
  'タイプJ': {
    files: ['133.csv', '134.csv', '135.csv'],
    checkFields: ['name', 'corporateNumber', 'address', 'departmentName1', 'departmentAddress1', 'departmentPhone1'],
    description: '部署・拠点情報'
  }
};

interface CsvRow {
  [key: string]: string;
}

async function checkTypeImport(typeName: string, config: any) {
  console.log(`\n${'='.repeat(60)}`);
  console.log(`📁 ${typeName}: ${config.description}`);
  console.log(`${'='.repeat(60)}`);

  let totalChecked = 0;
  let totalFound = 0;
  let totalFieldsOK = 0;
  let totalFieldsMissing = 0;

  for (const filename of config.files) {
    const csvPath = path.join(process.cwd(), 'csv', filename);
    
    if (!fs.existsSync(csvPath)) {
      console.log(`⚠️  ${filename}: ファイルが見つかりません`);
      continue;
    }

    console.log(`\n📄 ${filename}`);
    
    try {
      const buf = fs.readFileSync(csvPath);
      const records: CsvRow[] = parse(buf, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_records_with_error: true,
      });

      // 最初の3社をサンプリング
      const samples = records.slice(0, 3);
      
      for (const row of samples) {
        const companyName = row['企業名'] || row['会社名'] || row['name'];
        const corpNum = row['法人番号'] || row['corporateNumber'];
        
        if (!companyName) continue;
        
        totalChecked++;
        
        // Firestoreで検索
        let docSnap = null;
        
        // 1. 法人番号で検索
        if (corpNum && corpNum.trim() && corpNum.length === 13) {
          const byId = await companiesCol.doc(corpNum.trim()).get();
          if (byId.exists) {
            docSnap = byId;
          } else {
            const snap = await companiesCol.where('corporateNumber', '==', corpNum.trim()).limit(1).get();
            if (!snap.empty) {
              docSnap = snap.docs[0];
            }
          }
        }
        
        // 2. 企業名で検索
        if (!docSnap) {
          const snap = await companiesCol.where('name', '==', companyName.trim()).limit(1).get();
          if (!snap.empty) {
            docSnap = snap.docs[0];
          }
        }
        
        if (docSnap && docSnap.exists) {
          totalFound++;
          const data = docSnap.data();
          
          console.log(`  ✅ ${companyName}`);
          console.log(`     docId: ${docSnap.id}`);
          
          // フィールド確認
          const fieldResults: string[] = [];
          let fieldsOK = 0;
          let fieldsMissing = 0;
          
          for (const field of config.checkFields) {
            const value = (data as any)[field];
            if (value !== null && value !== undefined && value !== '') {
              fieldsOK++;
              totalFieldsOK++;
              fieldResults.push(`✓ ${field}`);
            } else {
              fieldsMissing++;
              totalFieldsMissing++;
              fieldResults.push(`✗ ${field}`);
            }
          }
          
          console.log(`     フィールド: ${fieldsOK}/${config.checkFields.length} (${fieldResults.join(', ')})`);
          
        } else {
          console.log(`  ❌ ${companyName}: Firestoreに見つかりません`);
        }
      }
      
    } catch (err: any) {
      console.log(`  ⚠️ CSVパースエラー: ${err.message}`);
    }
  }

  // サマリー
  console.log(`\n📊 ${typeName} サマリー:`);
  console.log(`  確認企業数: ${totalChecked}社`);
  console.log(`  Firestore存在: ${totalFound}/${totalChecked}社 (${totalChecked > 0 ? Math.round(totalFound/totalChecked*100) : 0}%)`);
  if (totalFound > 0) {
    const totalFields = totalFieldsOK + totalFieldsMissing;
    console.log(`  フィールド充足率: ${totalFieldsOK}/${totalFields} (${Math.round(totalFieldsOK/totalFields*100)}%)`);
  }
}

async function main() {
  console.log('\n🔍 タイプ別CSV→Firestore取り込み確認');
  console.log('各タイプの代表的なCSVから3社ずつサンプリングして確認します\n');

  for (const [typeName, config] of Object.entries(TYPE_CONFIG)) {
    await checkTypeImport(typeName, config);
  }

  console.log(`\n${'='.repeat(60)}`);
  console.log('✅ 確認完了');
  console.log(`${'='.repeat(60)}\n`);
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ エラー:', err);
    process.exit(1);
  });

