import admin from 'firebase-admin';
admin.initializeApp();
const db = admin.firestore();

async function check() {
  console.log('\n🚨 緊急確認: 丹羽興業株式会社\n');
  
  // 1. 名前で検索（完全一致）
  const snap1 = await db.collection('companies_new').where('name', '==', '丹羽興業株式会社').get();
  console.log(`1. name=="丹羽興業株式会社": ${snap1.size}件`);
  
  // 2. 名前で検索（前方一致）
  const snap2 = await db.collection('companies_new').where('name', '>=', '丹羽興業').where('name', '<', '丹羽興業' + '\uf8ff').get();
  console.log(`2. name starts with "丹羽興業": ${snap2.size}件`);
  
  // 3. 法人番号で検索
  const snap3 = await db.collection('companies_new').where('corporateNumber', '==', '9180000000000').get();
  console.log(`3. corporateNumber=="9180000000000": ${snap3.size}件`);
  
  // 4. 住所で検索
  const snap4 = await db.collection('companies_new').where('address', '>=', '愛知県名古屋市西区木前町').where('address', '<', '愛知県名古屋市西区木前町' + '\uf8ff').get();
  console.log(`4. address contains "愛知県名古屋市西区木前町": ${snap4.size}件`);
  
  console.log('\n結果:');
  if (snap1.size > 0) {
    snap1.forEach(doc => {
      const d = doc.data();
      console.log(`  docId: ${doc.id}`);
      console.log(`  name: ${d.name}`);
      console.log(`  corporateNumber: ${d.corporateNumber}`);
      console.log(`  address: ${d.address}`);
    });
  } else {
    console.log('  ❌ 丹羽興業株式会社が見つかりません！');
    console.log('  ⚠️  重複統合で誤って削除された可能性があります');
  }
}

check().then(() => process.exit(0)).catch(err => { console.error(err); process.exit(1); });
