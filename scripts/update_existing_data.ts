#!/usr/bin/env ts-node
/**
 * 既存データ更新スクリプト（削除なし）
 * 
 * 方針:
 * 1. 空データを削除せず、CSVデータで更新
 * 2. 法人番号を数値型→string型に修正
 * 3. 重複は最も情報が充実しているものを残し、他を統合
 * 4. 削除は最小限（完全な重複のみ）
 */

import admin from 'firebase-admin';

admin.initializeApp();
const db = admin.firestore();
const companiesCol = db.collection('companies_new');

const DRY_RUN = process.argv.includes('--dry-run');

async function fixCorporateNumberType() {
  console.log('\n🔧 法人番号の型修正（数値→string）\n');
  
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
            if (totalFixed < 10) {
              console.log(`🔍 DRY RUN - 修正: docId=${doc.id}, ${corpNum} → "${strValue}"`);
            }
          } else {
            batch.update(doc.ref, { corporateNumber: strValue });
            batchCount++;
          }
          totalFixed++;
        } else {
          // 13桁でない場合はnullに
          if (!DRY_RUN) {
            batch.update(doc.ref, { corporateNumber: null });
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
    
    if (totalScanned % 50000 === 0) {
      console.log(`  進行中... ${totalScanned.toLocaleString()}件スキャン、${totalFixed.toLocaleString()}件修正予定`);
    }
  }
  
  console.log(`\n✅ スキャン完了`);
  console.log(`   総スキャン数: ${totalScanned.toLocaleString()}件`);
  console.log(`   修正数: ${totalFixed.toLocaleString()}件`);
  
  return totalFixed;
}

async function deduplicateByNameAndAddress() {
  /**
   * 重複統合（削除は最小限）
   * - 企業名 + 住所が完全一致 → 統合
   * - 企業名のみ一致 → 別企業として保持
   */
  console.log('\n🔄 重複統合（企業名+住所が同じもののみ）\n');
  
  let lastDoc: any = null;
  let totalScanned = 0;
  let totalMerged = 0;
  const BATCH_SIZE = 500;
  
  // 企業名+住所でグルーピング
  const groups = new Map<string, admin.firestore.DocumentSnapshot[]>();
  
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
    
    for (const doc of snapshot.docs) {
      const data = doc.data();
      const name = (data.name || '').trim();
      const address = (data.address || '').trim();
      
      // 企業名と住所の両方がある場合のみグルーピング
      if (name && address) {
        const key = `${name}|${address}`;
        
        if (!groups.has(key)) {
          groups.set(key, []);
        }
        groups.get(key)!.push(doc);
      }
    }
    
    totalScanned += snapshot.size;
    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    
    if (totalScanned % 50000 === 0) {
      console.log(`  進行中... ${totalScanned.toLocaleString()}件スキャン、${groups.size.toLocaleString()}グループ`);
    }
  }
  
  console.log(`\n✅ グルーピング完了`);
  console.log(`   総スキャン数: ${totalScanned.toLocaleString()}件`);
  console.log(`   グループ数: ${groups.size.toLocaleString()}件`);
  
  // 重複グループのみ抽出
  const duplicates = Array.from(groups.entries()).filter(([_, docs]) => docs.length >= 2);
  
  console.log(`   重複グループ: ${duplicates.length.toLocaleString()}件\n`);
  
  if (duplicates.length === 0) {
    return 0;
  }
  
  console.log('🔄 重複統合を実行中...\n');
  
  let processed = 0;
  
  for (const [key, docs] of duplicates) {
    // 情報が最も充実しているものを選択
    const sorted = [...docs].sort((a, b) => {
      const dataA = a.data();
      const dataB = b.data();
      if (!dataA || !dataB) return 0;
      const countA = Object.values(dataA).filter(v => v !== null && v !== undefined && v !== '').length;
      const countB = Object.values(dataB).filter(v => v !== null && v !== undefined && v !== '').length;
      return countB - countA;
    });
    
    const primary = sorted[0];
    const others = sorted.slice(1);
    
    if (DRY_RUN && processed < 5) {
      console.log(`📦 ${key.split('|')[0]}`);
      console.log(`   重複: ${docs.length}件 → 1件に統合`);
    }
    
    if (!DRY_RUN) {
      // 他のドキュメントのデータを正に統合
      const primaryData = primary.data();
      if (!primaryData) continue;
      
      const mergedData = { ...primaryData };
      
      for (const other of others) {
        const otherData = other.data();
        if (!otherData) continue;
        
        for (const [field, value] of Object.entries(otherData)) {
          const currentValue = (mergedData as any)[field];
          if ((currentValue === null || currentValue === undefined || currentValue === '') &&
              value !== null && value !== undefined && value !== '') {
            (mergedData as any)[field] = value;
          }
        }
      }
      
      // 正のドキュメントを更新
      await primary.ref.update(mergedData);
      
      // 他のドキュメントを削除（最小限）
      const batch = db.batch();
      for (const other of others) {
        batch.delete(other.ref);
      }
      await batch.commit();
    }
    
    processed++;
    totalMerged += others.length;
    
    if (processed % 100 === 0) {
      console.log(`  進捗: ${processed}/${duplicates.length}グループ処理完了（${totalMerged}件統合）`);
    }
  }
  
  console.log(`\n✅ 統合完了`);
  console.log(`   処理グループ数: ${processed.toLocaleString()}`);
  console.log(`   統合（削除）数: ${totalMerged.toLocaleString()}件`);
  
  return totalMerged;
}

async function main() {
  console.log('\n🔧 既存データ更新スクリプト（削除最小限）');
  console.log('='.repeat(60));
  
  if (DRY_RUN) {
    console.log('\n🔍 DRY RUN モード（実際の更新・削除は行いません）\n');
  }
  
  console.log('方針:');
  console.log('  ✅ 空データは削除せず、CSVで更新');
  console.log('  ✅ 法人番号を数値型→string型に修正');
  console.log('  ✅ 重複は企業名+住所が同じもののみ統合');
  console.log('  ✅ 削除は最小限（完全重複のみ）');
  console.log('');
  
  // フェーズ1: 法人番号の型修正
  console.log('='.repeat(60));
  console.log('【フェーズ1】法人番号の型修正');
  console.log('='.repeat(60));
  const fixedCount = await fixCorporateNumberType();
  
  // フェーズ2: 重複統合
  console.log('\n' + '='.repeat(60));
  console.log('【フェーズ2】重複統合（企業名+住所が同じもののみ）');
  console.log('='.repeat(60));
  const mergedCount = await deduplicateByNameAndAddress();
  
  // 完了
  console.log('\n' + '='.repeat(60));
  console.log('🎉 更新完了！');
  console.log('='.repeat(60));
  console.log(`法人番号修正: ${fixedCount.toLocaleString()}件`);
  console.log(`重複統合: ${mergedCount.toLocaleString()}件`);
  console.log('='.repeat(60) + '\n');
  
  if (!DRY_RUN) {
    console.log('📌 次のステップ:');
    console.log('  1. CSVで空データを更新:');
    console.log('     bash scripts/run_backfill_by_type.sh');
    console.log('');
    console.log('  2. 総件数を再確認:');
    console.log('     npx ts-node scripts/quick_query.ts count');
    console.log('');
  }
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ エラー:', err);
    process.exit(1);
  });

