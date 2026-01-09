#!/usr/bin/env ts-node
/**
 * Firestore DBブラウザー（インタラクティブ）
 * 
 * 使い方:
 *   GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
 *   npx ts-node scripts/db_browser.ts
 */

import * as readline from 'readline';
import admin from 'firebase-admin';

admin.initializeApp();
const db = admin.firestore();
const companiesCol = db.collection('companies_new');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

function question(prompt: string): Promise<string> {
  return new Promise((resolve) => {
    rl.question(prompt, resolve);
  });
}

async function showMenu() {
  console.log('\n========================================');
  console.log('🔍 Firestore DBブラウザー');
  console.log('========================================');
  console.log('1. 企業名で検索');
  console.log('2. 法人番号で検索');
  console.log('3. 都道府県で検索');
  console.log('4. 最新N件を表示');
  console.log('5. ランダムN件を表示');
  console.log('6. 統計情報を表示');
  console.log('7. 特定フィールドの充足率確認');
  console.log('0. 終了');
  console.log('========================================\n');
}

async function searchByName() {
  const name = await question('企業名を入力: ');
  
  console.log(`\n🔍 "${name}" で検索中...\n`);
  
  const snapshot = await companiesCol
    .where('name', '==', name.trim())
    .limit(10)
    .get();
  
  if (snapshot.empty) {
    console.log('❌ 見つかりませんでした');
    return;
  }
  
  console.log(`✅ ${snapshot.size}件見つかりました\n`);
  
  snapshot.forEach((doc, index) => {
    const data = doc.data();
    console.log(`${index + 1}. docId: ${doc.id}`);
    console.log(`   企業名: ${data.name || '（なし）'}`);
    console.log(`   法人番号: ${data.corporateNumber || '（なし）'}`);
    console.log(`   住所: ${data.address || '（なし）'}`);
    console.log(`   電話番号: ${data.phoneNumber || '（なし）'}`);
    console.log(`   URL: ${data.companyUrl || '（なし）'}`);
    console.log(`   代表者: ${data.representativeName || '（なし）'}`);
    console.log('');
  });
}

async function searchByCorporateNumber() {
  const corpNum = await question('法人番号を入力（13桁）: ');
  
  console.log(`\n🔍 法人番号 "${corpNum}" で検索中...\n`);
  
  // まずdocIdで検索
  const byId = await companiesCol.doc(corpNum.trim()).get();
  
  if (byId.exists) {
    const data = byId.data();
    console.log('✅ 見つかりました（docId一致）\n');
    console.log(`docId: ${byId.id}`);
    console.log(`企業名: ${data?.name || '（なし）'}`);
    console.log(`法人番号: ${data?.corporateNumber || '（なし）'}`);
    console.log(`住所: ${data?.address || '（なし）'}`);
    console.log(`電話番号: ${data?.phoneNumber || '（なし）'}`);
    console.log(`代表者: ${data?.representativeName || '（なし）'}`);
    console.log('');
    return;
  }
  
  // フィールドで検索
  const snapshot = await companiesCol
    .where('corporateNumber', '==', corpNum.trim())
    .limit(10)
    .get();
  
  if (snapshot.empty) {
    console.log('❌ 見つかりませんでした');
    return;
  }
  
  console.log(`✅ ${snapshot.size}件見つかりました（フィールド一致）\n`);
  
  snapshot.forEach((doc, index) => {
    const data = doc.data();
    console.log(`${index + 1}. docId: ${doc.id}`);
    console.log(`   企業名: ${data.name || '（なし）'}`);
    console.log(`   法人番号: ${data.corporateNumber || '（なし）'}`);
    console.log(`   住所: ${data.address || '（なし）'}`);
    console.log('');
  });
}

async function searchByPrefecture() {
  const pref = await question('都道府県を入力（例: 東京都）: ');
  
  console.log(`\n🔍 "${pref}" で検索中（最初の10件）...\n`);
  
  const snapshot = await companiesCol
    .where('prefecture', '==', pref.trim())
    .limit(10)
    .get();
  
  if (snapshot.empty) {
    console.log('❌ 見つかりませんでした');
    return;
  }
  
  console.log(`✅ 最初の${snapshot.size}件を表示\n`);
  
  snapshot.forEach((doc, index) => {
    const data = doc.data();
    console.log(`${index + 1}. ${data.name || '（名前なし）'}`);
    console.log(`   住所: ${data.address || '（なし）'}`);
    console.log(`   電話: ${data.phoneNumber || '（なし）'}`);
    console.log('');
  });
}

async function showLatest() {
  const count = await question('何件表示しますか？（デフォルト: 5）: ');
  const limit = parseInt(count) || 5;
  
  console.log(`\n🔍 最新${limit}件を取得中...\n`);
  
  const snapshot = await companiesCol
    .orderBy(admin.firestore.FieldPath.documentId(), 'desc')
    .limit(limit)
    .get();
  
  console.log(`✅ ${snapshot.size}件表示\n`);
  
  snapshot.forEach((doc, index) => {
    const data = doc.data();
    console.log(`${index + 1}. docId: ${doc.id}`);
    console.log(`   企業名: ${data.name || '（なし）'}`);
    console.log(`   法人番号: ${data.corporateNumber || '（なし）'}`);
    console.log(`   住所: ${data.address || '（なし）'}`);
    console.log('');
  });
}

async function showRandom() {
  const count = await question('何件表示しますか？（デフォルト: 5）: ');
  const limit = parseInt(count) || 5;
  
  console.log(`\n🔍 ランダムに${limit}件取得中...\n`);
  
  // ランダムなdocIdを生成してstartAtで取得
  const snapshot = await companiesCol
    .limit(limit)
    .get();
  
  console.log(`✅ ${snapshot.size}件表示\n`);
  
  snapshot.forEach((doc, index) => {
    const data = doc.data();
    console.log(`${index + 1}. ${data.name || '（名前なし）'}`);
    console.log(`   法人番号: ${data.corporateNumber || '（なし）'}`);
    console.log(`   住所: ${data.address || '（なし）'}`);
    console.log(`   代表者: ${data.representativeName || '（なし）'}`);
    console.log('');
  });
}

async function showStats() {
  console.log('\n🔍 統計情報を取得中...\n');
  
  // 総件数
  const countSnap = await companiesCol.count().get();
  const totalCount = countSnap.data().count;
  
  console.log(`📊 統計情報:`);
  console.log(`  総企業数: ${totalCount.toLocaleString()}件`);
  
  // サンプルで100件取得してフィールド充足率を計算
  const sampleSnap = await companiesCol.limit(100).get();
  
  const fields = [
    'name', 'corporateNumber', 'address', 'phoneNumber', 'companyUrl',
    'representativeName', 'industry', 'capitalStock', 'employeeCount'
  ];
  
  const fieldCounts: Record<string, number> = {};
  
  sampleSnap.forEach(doc => {
    const data = doc.data();
    fields.forEach(field => {
      const value = (data as any)[field];
      if (value !== null && value !== undefined && value !== '') {
        fieldCounts[field] = (fieldCounts[field] || 0) + 1;
      }
    });
  });
  
  console.log(`\n  フィールド充足率（サンプル100件）:`);
  fields.forEach(field => {
    const count = fieldCounts[field] || 0;
    const rate = Math.round(count / sampleSnap.size * 100);
    console.log(`    ${field.padEnd(20)}: ${rate}%`);
  });
  
  console.log('');
}

async function checkFieldCoverage() {
  const field = await question('確認したいフィールド名を入力: ');
  const countStr = await question('サンプル件数（デフォルト: 100）: ');
  const sampleCount = parseInt(countStr) || 100;
  
  console.log(`\n🔍 "${field}" の充足率を確認中（サンプル${sampleCount}件）...\n`);
  
  const snapshot = await companiesCol.limit(sampleCount).get();
  
  let hasValue = 0;
  let isEmpty = 0;
  const samples: any[] = [];
  
  snapshot.forEach(doc => {
    const data = doc.data();
    const value = (data as any)[field];
    
    if (value !== null && value !== undefined && value !== '') {
      hasValue++;
      if (samples.length < 5) {
        samples.push({ name: data.name, value });
      }
    } else {
      isEmpty++;
    }
  });
  
  const rate = Math.round(hasValue / snapshot.size * 100);
  
  console.log(`📊 結果:`);
  console.log(`  充足率: ${rate}% (${hasValue}/${snapshot.size})`);
  console.log(`  空: ${isEmpty}件`);
  
  if (samples.length > 0) {
    console.log(`\n  サンプル値（最初の5件）:`);
    samples.forEach((s, i) => {
      console.log(`    ${i + 1}. ${s.name}: ${JSON.stringify(s.value).slice(0, 80)}`);
    });
  }
  
  console.log('');
}

async function main() {
  console.log('\n🚀 Firestore DBブラウザーを起動しました');
  
  while (true) {
    await showMenu();
    const choice = await question('選択してください: ');
    
    switch (choice.trim()) {
      case '1':
        await searchByName();
        break;
      case '2':
        await searchByCorporateNumber();
        break;
      case '3':
        await searchByPrefecture();
        break;
      case '4':
        await showLatest();
        break;
      case '5':
        await showRandom();
        break;
      case '6':
        await showStats();
        break;
      case '7':
        await checkFieldCoverage();
        break;
      case '0':
        console.log('\n👋 終了します\n');
        rl.close();
        process.exit(0);
      default:
        console.log('❌ 無効な選択です');
    }
  }
}

main().catch(err => {
  console.error('❌ エラー:', err);
  rl.close();
  process.exit(1);
});

