#!/usr/bin/env ts-node
/**
 * 緊急クリーンアップスクリプト
 * 
 * 問題:
 * 1. データが420万件（予想32万件の13倍）
 * 2. 法人番号が数値型（9.18E+12）
 * 3. 古いデータ（フィールドが空）が大量に残存
 * 
 * 対応:
 * 1. 空データ（主要フィールドがnull）を削除
 * 2. 法人番号をstring型に修正
 * 3. 重複を統合
 */

import admin from 'firebase-admin';

admin.initializeApp();
const db = admin.firestore();
const companiesCol = db.collection('companies_new');

const DRY_RUN = process.argv.includes('--dry-run');

function countFilledFields(data: admin.firestore.DocumentData): number {
  const importantFields = [
    'name', 'address', 'phoneNumber', 'companyUrl',
    'representativeName', 'industry', 'capitalStock'
  ];
  
  let count = 0;
  for (const field of importantFields) {
    const value = (data as any)[field];
    if (value !== null && value !== undefined && value !== '') {
      count++;
    }
  }
  return count;
}

async function cleanupEmptyDocuments() {
  console.log('\n🗑️  空データの削除\n');
  console.log('削除対象: name以外のフィールドがほぼ空のドキュメント\n');
  
  let lastDoc: any = null;
  let totalScanned = 0;
  let totalDeleted = 0;
  const BATCH_SIZE = 500;
  
  while (true) {
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(BATCH_SIZE);
    
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }
    
    const snapshot = await query.get();
    
    if (snapshot.empty) {
      break;
    }
    
    const batch = db.batch();
    let batchCount = 0;
    
    for (const doc of snapshot.docs) {
      const data = doc.data();
      
      // name以外のフィールドがほとんど空の場合は削除対象
      const filledCount = countFilledFields(data);
      
      if (filledCount <= 1) {
        // nameしかないまたは完全に空
        if (DRY_RUN) {
          console.log(`🔍 DRY RUN - 削除対象: docId=${doc.id}, name=${data.name || '(なし)'}, filled=${filledCount}/7`);
        } else {
          batch.delete(doc.ref);
          batchCount++;
        }
        totalDeleted++;
      }
    }
    
    if (!DRY_RUN && batchCount > 0) {
      await batch.commit();
    }
    
    totalScanned += snapshot.size;
    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    
    if (totalScanned % 10000 === 0) {
      console.log(`  進行中... ${totalScanned}件スキャン、${totalDeleted}件削除予定`);
    }
  }
  
  console.log(`\n✅ スキャン完了`);
  console.log(`   総スキャン数: ${totalScanned}件`);
  console.log(`   削除数: ${totalDeleted}件`);
  
  return totalDeleted;
}

async function fixCorporateNumberType() {
  console.log('\n🔧 法人番号の型修正\n');
  console.log('数値型 → string型に変換\n');
  
  let lastDoc: any = null;
  let totalScanned = 0;
  let totalFixed = 0;
  const BATCH_SIZE = 500;
  
  while (true) {
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(BATCH_SIZE);
    
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }
    
    const snapshot = await query.get();
    
    if (snapshot.empty) {
      break;
    }
    
    const batch = db.batch();
    let batchCount = 0;
    
    for (const doc of snapshot.docs) {
      const data = doc.data();
      const corpNum = data.corporateNumber;
      
      // 数値型の法人番号をstring型に変換
      if (corpNum !== null && corpNum !== undefined && typeof corpNum === 'number') {
        const strValue = String(Math.round(corpNum));
        
        if (strValue.length === 13) {
          if (DRY_RUN) {
            console.log(`🔍 DRY RUN - 修正: docId=${doc.id}, ${corpNum} → "${strValue}"`);
          } else {
            batch.update(doc.ref, { corporateNumber: strValue });
            batchCount++;
          }
          totalFixed++;
        }
      }
    }
    
    if (!DRY_RUN && batchCount > 0) {
      await batch.commit();
    }
    
    totalScanned += snapshot.size;
    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    
    if (totalScanned % 10000 === 0) {
      console.log(`  進行中... ${totalScanned}件スキャン、${totalFixed}件修正予定`);
    }
  }
  
  console.log(`\n✅ スキャン完了`);
  console.log(`   総スキャン数: ${totalScanned}件`);
  console.log(`   修正数: ${totalFixed}件`);
  
  return totalFixed;
}

async function main() {
  console.log('\n🚨 緊急クリーンアップスクリプト');
  console.log('='.repeat(60));
  
  if (DRY_RUN) {
    console.log('\n🔍 DRY RUN モード（実際の削除・更新は行いません）\n');
  }
  
  console.log('問題:');
  console.log('  1. データが420万件（予想32万件の13倍）');
  console.log('  2. 法人番号が数値型（9.18E+12）');
  console.log('  3. 空データが大量に残存');
  console.log('');
  
  // フェーズ1: 空データの削除
  console.log('='.repeat(60));
  console.log('【フェーズ1】空データの削除');
  console.log('='.repeat(60));
  const deletedCount = await cleanupEmptyDocuments();
  
  // フェーズ2: 法人番号の型修正
  console.log('\n' + '='.repeat(60));
  console.log('【フェーズ2】法人番号の型修正');
  console.log('='.repeat(60));
  const fixedCount = await fixCorporateNumberType();
  
  // 完了
  console.log('\n' + '='.repeat(60));
  console.log('🎉 クリーンアップ完了！');
  console.log('='.repeat(60));
  console.log(`削除数: ${deletedCount}件`);
  console.log(`修正数: ${fixedCount}件`);
  console.log('='.repeat(60) + '\n');
  
  if (!DRY_RUN) {
    console.log('📌 次のステップ:');
    console.log('  1. 総件数を再確認:');
    console.log('     npx ts-node scripts/quick_query.ts count');
    console.log('');
    console.log('  2. 丹羽興業株式会社を再確認:');
    console.log('     npx ts-node scripts/emergency_check.ts');
    console.log('');
    console.log('  3. 重複統合を実行:');
    console.log('     bash scripts/run_step4_dedupe.sh');
    console.log('');
  }
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ エラー:', err);
    process.exit(1);
  });

