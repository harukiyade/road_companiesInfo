/* 
  タイプEのCSVで処理された企業のドキュメントIDとデータを確認するスクリプト
*/

import * as admin from "firebase-admin";
import * as path from "path";

const serviceAccountPath = path.join(__dirname, "../albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
const serviceAccount = require(serviceAccountPath);
admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
const db = admin.firestore();

async function main() {
  // CSVから確認したい企業名のリスト
  const companies = [
    '株式会社やぶやグループ',
    '丹羽興業株式会社',
    '藤吉工業株式会社',
    '株式会社道路計画'
  ];
  
  console.log("🔍 タイプEのCSVで処理された企業のデータを確認します\n");
  
  for (const name of companies) {
    const snap = await db.collection('companies_new')
      .where('name', '==', name)
      .limit(1)
      .get();
    
    if (snap.empty) {
      console.log(`\n❌ ${name}: 見つかりませんでした`);
    } else {
      const doc = snap.docs[0];
      const data = doc.data();
      console.log(`\n✅ ${name}`);
      console.log(`   ドキュメントID: ${doc.id}`);
      console.log(`   代表者名: ${data.representativeName || '(空)'}`);
      console.log(`   代表者誕生日: ${data.representativeBirthDate || '(空)'}`);
      console.log(`   都道府県: ${data.prefecture || '(空)'}`);
      console.log(`   法人番号: ${data.corporateNumber || '(空)'}`);
      console.log(`   住所: ${data.address || '(空)'}`);
      console.log(`   郵便番号: ${data.postalCode || '(空)'}`);
    }
  }
  
  process.exit(0);
}

main().catch(console.error);

