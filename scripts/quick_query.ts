#!/usr/bin/env ts-node
/**
 * クイッククエリツール
 * 
 * 使い方:
 *   # 企業名で検索
 *   npx ts-node scripts/quick_query.ts name "丹羽興業株式会社"
 * 
 *   # 法人番号で検索
 *   npx ts-node scripts/quick_query.ts corp 1234567890123
 * 
 *   # 都道府県で検索（最初の10件）
 *   npx ts-node scripts/quick_query.ts pref 東京都
 * 
 *   # docIdで取得
 *   npx ts-node scripts/quick_query.ts id 1234567890123
 * 
 *   # 総件数
 *   npx ts-node scripts/quick_query.ts count
 * 
 *   # ランダム表示
 *   npx ts-node scripts/quick_query.ts random 5
 */

import admin from 'firebase-admin';

admin.initializeApp();
const db = admin.firestore();
const companiesCol = db.collection('companies_new');

const command = process.argv[2];
const arg = process.argv[3];

function displayDoc(doc: admin.firestore.DocumentSnapshot, index?: number) {
  const data = doc.data();
  if (!data) return;
  
  const prefix = index !== undefined ? `${index + 1}. ` : '';
  
  console.log(`${prefix}docId: ${doc.id}`);
  console.log(`   企業名: ${data.name || '（なし）'}`);
  console.log(`   法人番号: ${data.corporateNumber || '（なし）'}`);
  console.log(`   住所: ${data.address || '（なし）'}`);
  console.log(`   都道府県: ${data.prefecture || '（なし）'}`);
  console.log(`   郵便番号: ${data.postalCode || '（なし）'}`);
  console.log(`   電話番号: ${data.phoneNumber || '（なし）'}`);
  console.log(`   URL: ${data.companyUrl || '（なし）'}`);
  console.log(`   代表者名: ${data.representativeName || '（なし）'}`);
  console.log(`   業種: ${data.industry || '（なし）'}`);
  console.log(`   資本金: ${data.capitalStock || '（なし）'}`);
  console.log(`   従業員数: ${data.employeeCount || '（なし）'}`);
  console.log(`   売上: ${data.latestRevenue || '（なし）'}`);
  console.log(`   利益: ${data.latestProfit || '（なし）'}`);
  console.log('');
}

async function searchByName(name: string) {
  console.log(`\n🔍 企業名で検索: "${name}"\n`);
  
  const snapshot = await companiesCol
    .where('name', '==', name)
    .limit(10)
    .get();
  
  if (snapshot.empty) {
    console.log('❌ 見つかりませんでした\n');
    return;
  }
  
  console.log(`✅ ${snapshot.size}件見つかりました\n`);
  snapshot.docs.forEach((doc, i) => displayDoc(doc, i));
}

async function searchByCorporateNumber(corpNum: string) {
  console.log(`\n🔍 法人番号で検索: "${corpNum}"\n`);
  
  // docIdで検索
  const byId = await companiesCol.doc(corpNum).get();
  if (byId.exists) {
    console.log('✅ 見つかりました（docId一致）\n');
    displayDoc(byId);
    return;
  }
  
  // フィールドで検索
  const snapshot = await companiesCol
    .where('corporateNumber', '==', corpNum)
    .limit(10)
    .get();
  
  if (snapshot.empty) {
    console.log('❌ 見つかりませんでした\n');
    return;
  }
  
  console.log(`✅ ${snapshot.size}件見つかりました（フィールド一致）\n`);
  snapshot.docs.forEach((doc, i) => displayDoc(doc, i));
}

async function searchByPrefecture(pref: string) {
  console.log(`\n🔍 都道府県で検索: "${pref}"（最初の10件）\n`);
  
  const snapshot = await companiesCol
    .where('prefecture', '==', pref)
    .limit(10)
    .get();
  
  if (snapshot.empty) {
    console.log('❌ 見つかりませんでした\n');
    return;
  }
  
  console.log(`✅ ${snapshot.size}件表示\n`);
  snapshot.docs.forEach((doc, i) => {
    const data = doc.data();
    console.log(`${i + 1}. ${data.name || '（名前なし）'}`);
    console.log(`   住所: ${data.address || '（なし）'}`);
    console.log(`   電話: ${data.phoneNumber || '（なし）'}`);
    console.log('');
  });
}

async function getById(docId: string) {
  console.log(`\n🔍 docIdで取得: "${docId}"\n`);
  
  const doc = await companiesCol.doc(docId).get();
  
  if (!doc.exists) {
    console.log('❌ 見つかりませんでした\n');
    return;
  }
  
  console.log('✅ 見つかりました\n');
  displayDoc(doc);
}

async function showCount() {
  console.log('\n🔍 総件数を取得中...\n');
  
  const countSnap = await companiesCol.count().get();
  const total = countSnap.data().count;
  
  console.log(`📊 companies_new 総件数: ${total.toLocaleString()}件\n`);
}

async function showRandom(count: string) {
  const limit = parseInt(count) || 5;
  
  console.log(`\n🔍 ランダムに${limit}件表示\n`);
  
  const snapshot = await companiesCol.limit(limit).get();
  
  console.log(`✅ ${snapshot.size}件表示\n`);
  snapshot.docs.forEach((doc, i) => displayDoc(doc, i));
}

async function main() {
  if (!command) {
    console.error('\n❌ エラー: コマンドを指定してください\n');
    console.error('使い方:');
    console.error('  npx ts-node scripts/quick_query.ts name "企業名"');
    console.error('  npx ts-node scripts/quick_query.ts corp 1234567890123');
    console.error('  npx ts-node scripts/quick_query.ts pref 東京都');
    console.error('  npx ts-node scripts/quick_query.ts id 1234567890123');
    console.error('  npx ts-node scripts/quick_query.ts count');
    console.error('  npx ts-node scripts/quick_query.ts random 5');
    console.error('');
    process.exit(1);
  }
  
  switch (command) {
    case 'name':
      if (!arg) {
        console.error('❌ 企業名を指定してください');
        process.exit(1);
      }
      await searchByName(arg);
      break;
      
    case 'corp':
      if (!arg) {
        console.error('❌ 法人番号を指定してください');
        process.exit(1);
      }
      await searchByCorporateNumber(arg);
      break;
      
    case 'pref':
      if (!arg) {
        console.error('❌ 都道府県を指定してください');
        process.exit(1);
      }
      await searchByPrefecture(arg);
      break;
      
    case 'id':
      if (!arg) {
        console.error('❌ docIdを指定してください');
        process.exit(1);
      }
      await getById(arg);
      break;
      
    case 'count':
      await showCount();
      break;
      
    case 'random':
      await showRandom(arg || '5');
      break;
      
    default:
      console.error(`❌ 不明なコマンド: ${command}`);
      process.exit(1);
  }
}

main()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ エラー:', err);
    process.exit(1);
  });

