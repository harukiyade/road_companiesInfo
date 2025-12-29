#!/usr/bin/env ts-node
/**
 * 要件1: タイプB（12.csvグループ）の重複統合スクリプト
 * 
 * - 法人番号 + 住所が同じ企業を統合
 * - 情報が最も充実しているドキュメントを正とする
 * - 不足フィールドを他のドキュメントから補完
 * - 統合後、重複ドキュメントを削除
 * 
 * 注意: 企業名が同じでも法人番号・住所が違う場合は別企業として保持
 */

import admin from 'firebase-admin';

admin.initializeApp();
const db = admin.firestore();
const companiesCol = db.collection('companies_new');

const DRY_RUN = process.argv.includes('--dry-run');

interface CompanyDoc {
  ref: admin.firestore.DocumentReference;
  data: admin.firestore.DocumentData;
}

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
  /**
   * 複数のドキュメントデータを統合
   * - primaryを基準に、nullまたは空のフィールドをothersから補完
   */
  const merged = { ...primary };
  
  for (const other of others) {
    for (const [key, value] of Object.entries(other)) {
      const currentValue = merged[key];
      
      // 現在の値がnull/undefined/空文字の場合のみ上書き
      if (currentValue === null || currentValue === undefined || currentValue === '') {
        if (value !== null && value !== undefined && value !== '') {
          merged[key] = value;
        }
      }
    }
  }
  
  return merged;
}

async function findDuplicates(): Promise<Map<string, CompanyDoc[]>> {
  console.log('\n🔍 重複企業を検索中...\n');
  
  const allDocs: CompanyDoc[] = [];
  let lastDoc: any = null;
  let totalFetched = 0;
  
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
    
    for (const doc of snapshot.docs) {
      allDocs.push({ ref: doc.ref, data: doc.data() });
    }
    
    totalFetched += snapshot.size;
    lastDoc = snapshot.docs[snapshot.docs.length - 1];
    
    if (totalFetched % 5000 === 0) {
      console.log(`  進行中... ${totalFetched}件取得`);
    }
  }
  
  console.log(`✅ 総件数: ${allDocs.length}件\n`);
  
  // 法人番号 + 住所でグルーピング
  const groups = new Map<string, CompanyDoc[]>();
  
  for (const doc of allDocs) {
    const data = doc.data;
    const corpNum = normalizeForDedup(data.corporateNumber);
    const address = normalizeForDedup(data.address);
    
    // 法人番号も住所もない場合はスキップ
    if (!corpNum && !address) {
      continue;
    }
    
    // キー: 法人番号 + 住所
    const key = `${corpNum}|${address}`;
    
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    
    groups.get(key)!.push(doc);
  }
  
  // 2件以上のグループのみを抽出（重複）
  const duplicates = new Map<string, CompanyDoc[]>();
  
  for (const [key, docs] of groups.entries()) {
    if (docs.length >= 2) {
      duplicates.set(key, docs);
    }
  }
  
  console.log(`🔍 重複グループ: ${duplicates.size}件`);
  console.log(`   （重複ドキュメント総数: ${Array.from(duplicates.values()).reduce((sum, arr) => sum + arr.length, 0)}件）\n`);
  
  return duplicates;
}

async function deduplicateGroup(key: string, docs: CompanyDoc[]): Promise<void> {
  // 最もフィールドが充実しているドキュメントを正とする
  const sorted = [...docs].sort((a, b) => {
    const countA = countNonNullFields(a.data);
    const countB = countNonNullFields(b.data);
    return countB - countA; // 降順
  });
  
  const primary = sorted[0];
  const others = sorted.slice(1);
  
  console.log(`\n📦 グループ: ${key.split('|')[0] || '（法人番号なし）'}`);
  console.log(`   企業名: ${primary.data.name || '（名前なし）'}`);
  console.log(`   住所: ${primary.data.address || '（住所なし）'}`);
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
    return;
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
}

async function main() {
  console.log('\n🔧 タイプB 重複統合スクリプト');
  console.log('='.repeat(60));
  
  if (DRY_RUN) {
    console.log('\n🔍 DRY RUN モード（実際の更新・削除は行いません）\n');
  }
  
  const duplicates = await findDuplicates();
  
  if (duplicates.size === 0) {
    console.log('✅ 重複はありませんでした\n');
    return;
  }
  
  console.log('\n🔄 統合処理を開始します...\n');
  
  let processed = 0;
  let totalDeleted = 0;
  
  for (const [key, docs] of duplicates.entries()) {
    await deduplicateGroup(key, docs);
    processed++;
    totalDeleted += docs.length - 1;
    
    if (processed % 100 === 0) {
      console.log(`\n📊 進捗: ${processed}/${duplicates.size}グループ処理完了\n`);
    }
  }
  
  console.log('\n' + '='.repeat(60));
  console.log('🎉 統合完了！');
  console.log(`   処理グループ数: ${processed}`);
  console.log(`   削除ドキュメント数: ${totalDeleted}`);
  console.log('='.repeat(60) + '\n');
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ エラー:', err);
    process.exit(1);
  });

