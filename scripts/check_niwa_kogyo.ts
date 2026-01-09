#!/usr/bin/env ts-node
/**
 * 丹羽興業株式会社の統合確認スクリプト
 */

import admin from 'firebase-admin';

admin.initializeApp();
const db = admin.firestore();

async function checkNiwaKogyo() {
  console.log('\n🔍 丹羽興業株式会社の統合確認\n');
  
  const snapshot = await db.collection('companies_new')
    .where('name', '==', '丹羽興業株式会社')
    .get();
  
  console.log(`✅ 検索結果: ${snapshot.size}件`);
  console.log('   期待値: 1件 (元は11件の重複)\n');
  
  if (snapshot.size > 0) {
    console.log('📋 詳細:\n');
    snapshot.forEach((doc, index) => {
      const data = doc.data();
      console.log(`${index + 1}. docId: ${doc.id}`);
      console.log(`   法人番号: ${data.corporateNumber || '（なし）'}`);
      console.log(`   住所: ${data.address || '（なし）'}`);
      console.log(`   代表者: ${data.representativeName || '（なし）'}`);
      console.log(`   郵便番号: ${data.postalCode || '（なし）'}`);
      console.log(`   電話番号: ${data.phoneNumber || '（なし）'}`);
      console.log('');
    });
  }
  
  console.log('========================================\n');
  
  // 判定
  if (snapshot.size === 1) {
    console.log('🎉 成功！11件の重複が1件に統合されました！');
  } else if (snapshot.size > 1) {
    console.log(`⚠️ まだ ${snapshot.size} 件の重複があります`);
    console.log('   → 手動での追加統合が必要かもしれません');
  } else {
    console.log('❌ 該当企業が見つかりません');
  }
  
  console.log('\n========================================\n');
}

checkNiwaKogyo()
  .then(() => process.exit(0))
  .catch(err => {
    console.error('❌ エラー:', err);
    process.exit(1);
  });

