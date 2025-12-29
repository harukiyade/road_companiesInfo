#!/usr/bin/env ts-node
/**
 * 要件1: タイプB（12.csvグループ）の重複統合スクリプト（バッチ処理版）
 * 
 * メモリ効率的なバッチ処理で重複を統合
 * - 法人番号ごとにバッチ処理
 * - メモリに全件ロードしない
 */

import admin from 'firebase-admin';

admin.initializeApp();
const db = admin.firestore();
const companiesCol = db.collection('companies_new');

const DRY_RUN = process.argv.includes('--dry-run');

function normalizeForDedup(value: string | null | undefined): string {
  if (!value) return '';
  return String(value).trim().toLowerCase().replace(/\s+/g, '');
}

function countNonNullFields(data: admin.firestore.DocumentData): number {
  let count = 0;
  for (const [key, value] of Object.entries(data)) {
    if (value !== null && value !== undefined && value !== '') {
      count++;
    }
  }
  return count;
}

function mergeCompanyData(
  primary: admin.firestore.DocumentData,
  ...others: admin.firestore.DocumentData[]
): admin.firestore.DocumentData {
  const merged = { ...primary };
  
  for (const other of others) {
    for (const [key, value] of Object.entries(other)) {
      const currentValue = merged[key];
      
      if (currentValue === null || currentValue === undefined || currentValue === '') {
        if (value !== null && value !== undefined && value !== '') {
          merged[key] = value;
        }
      }
    }
  }
  
  return merged;
}

async function deduplicateByCorporateNumber(corpNum: string): Promise<number> {
  /**
   * 特定の法人番号の重複を統合
   * 戻り値: 削除したドキュメント数
   */
  
  // 法人番号で検索（docIdとフィールド両方）
  const docs: { ref: admin.firestore.DocumentReference; data: admin.firestore.DocumentData }[] = [];
  
  // docIdで検索
  const byId = await companiesCol.doc(corpNum).get();
  if (byId.exists) {
    docs.push({ ref: byId.ref, data: byId.data()! });
  }
  
  // フィールドで検索
  const byField = await companiesCol.where('corporateNumber', '==', corpNum).get();
  for (const doc of byField.docs) {
    // docIdで既に追加済みの場合はスキップ
    if (doc.id === corpNum) continue;
    docs.push({ ref: doc.ref, data: doc.data() });
  }
  
  if (docs.length <= 1) {
    return 0; // 重複なし
  }
  
  // 最もフィールドが充実しているドキュメントを正とする
  const sorted = [...docs].sort((a, b) => {
    const countA = countNonNullFields(a.data);
    const countB = countNonNullFields(b.data);
    return countB - countA;
  });
  
  const primary = sorted[0];
  const others = sorted.slice(1);
  
  console.log(`\n📦 法人番号: ${corpNum}`);
  console.log(`   企業名: ${primary.data.name || '（名前なし）'}`);
  console.log(`   重複数: ${docs.length}件`);
  console.log(`   正: docId=${primary.ref.id} (${countNonNullFields(primary.data)}フィールド)`);
  
  for (const other of others) {
    console.log(`   削除: docId=${other.ref.id} (${countNonNullFields(other.data)}フィールド)`);
  }
  
  // データを統合
  const mergedData = mergeCompanyData(primary.data, ...others.map(d => d.data));
  const mergedCount = countNonNullFields(mergedData);
  const primaryCount = countNonNullFields(primary.data);
  
  if (mergedCount > primaryCount) {
    console.log(`   ✨ 統合後: ${mergedCount}フィールド (+${mergedCount - primaryCount})`);
  }
  
  if (DRY_RUN) {
    console.log(`   🔍 DRY RUN: 実際の更新・削除は行いません`);
    return others.length;
  }
  
  // 正のドキュメントを更新
  await primary.ref.update(mergedData);
  
  // 重複ドキュメントを削除
  const batch = db.batch();
  for (const other of others) {
    batch.delete(other.ref);
  }
  await batch.commit();
  
  console.log(`   ✅ 統合完了`);
  
  return others.length;
}

async function findUniqueCorporateNumbers(): Promise<string[]> {
  /**
   * 重複の可能性がある法人番号を収集
   * メモリ効率的に実装
   */
  console.log('\n🔍 重複候補の法人番号を収集中...\n');
  
  const corpNumbers = new Set<string>();
  let lastDoc: any = null;
  let totalFetched = 0;
  
  const BATCH_SIZE = 1000;
  
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
      const corpNum = data.corporateNumber;
      
      if (corpNum && typeof corpNum === 'string' && corpNum.length === 13) {
        corpNumbers.add(corpNum);
      }
    }
    
    totalFetched += snapshot.size;
    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    
    if (totalFetched % 10000 === 0) {
      console.log(`  進行中... ${totalFetched}件スキャン、${corpNumbers.size}個の法人番号を発見`);
    }
  }
  
  console.log(`\n✅ 総スキャン数: ${totalFetched}件`);
  console.log(`✅ ユニーク法人番号: ${corpNumbers.size}個\n`);
  
  return Array.from(corpNumbers);
}

async function main() {
  console.log('\n🔧 タイプB 重複統合スクリプト（バッチ処理版）');
  console.log('='.repeat(60));
  
  if (DRY_RUN) {
    console.log('\n🔍 DRY RUN モード（実際の更新・削除は行いません）\n');
  }
  
  // ステップ1: 法人番号を収集
  const corpNumbers = await findUniqueCorporateNumbers();
  
  if (corpNumbers.length === 0) {
    console.log('✅ 法人番号を持つドキュメントがありません\n');
    return;
  }
  
  // ステップ2: 法人番号ごとに重複統合
  console.log('🔄 重複統合を開始します...\n');
  
  let processedCount = 0;
  let duplicateCount = 0;
  let totalDeleted = 0;
  
  for (const corpNum of corpNumbers) {
    const deleted = await deduplicateByCorporateNumber(corpNum);
    
    if (deleted > 0) {
      duplicateCount++;
      totalDeleted += deleted;
    }
    
    processedCount++;
    
    if (processedCount % 100 === 0) {
      console.log(`\n📊 進捗: ${processedCount}/${corpNumbers.length}件処理完了（重複${duplicateCount}件、削除${totalDeleted}件）\n`);
    }
  }
  
  console.log('\n' + '='.repeat(60));
  console.log('🎉 統合完了！');
  console.log(`   処理法人番号数: ${processedCount}`);
  console.log(`   重複発見数: ${duplicateCount}`);
  console.log(`   削除ドキュメント数: ${totalDeleted}`);
  console.log('='.repeat(60) + '\n');
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ エラー:', err);
    process.exit(1);
  });

