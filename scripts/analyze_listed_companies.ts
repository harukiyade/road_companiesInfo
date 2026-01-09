/* eslint-disable no-console */
import admin from "firebase-admin";

// Firebase初期化
const serviceAccountKeyPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;
if (!serviceAccountKeyPath) {
  console.error("❌ FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません");
  process.exit(1);
}

try {
  const serviceAccount = require(serviceAccountKeyPath);
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
  });
  console.log("✅ Firebase初期化完了");
} catch (error: any) {
  console.error("❌ Firebase初期化エラー:", error.message);
  process.exit(1);
}

const db = admin.firestore();

async function analyzeListedCompanies() {
  try {
    console.log("listing='上場'のドキュメントを分析中...\n");
    
    // 1. listingフィールドの値の種類を確認
    console.log("【1. listingフィールドの値の種類を確認】");
    const listingValues = new Map<string, number>();
    let totalCount = 0;
    let nullCount = 0;
    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    const BATCH_SIZE = 5000;
    
    while (true) {
      let query = db.collection("companies_new").limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }
      
      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }
      
      for (const doc of snapshot.docs) {
        totalCount++;
        const data = doc.data();
        const listing = data.listing;
        
        if (!listing) {
          nullCount++;
          listingValues.set("(null/undefined)", (listingValues.get("(null/undefined)") || 0) + 1);
        } else {
          listingValues.set(listing, (listingValues.get(listing) || 0) + 1);
        }
      }
      
      if (totalCount % 10000 === 0) {
        console.log(`  処理中... ${totalCount} 件`);
      }
      
      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }
    
    console.log(`\n総ドキュメント数: ${totalCount} 件`);
    console.log("\nlistingフィールドの値の分布:");
    const sortedValues = Array.from(listingValues.entries()).sort((a, b) => b[1] - a[1]);
    for (const [value, count] of sortedValues) {
      const percentage = ((count / totalCount) * 100).toFixed(2);
      console.log(`  "${value}": ${count} 件 (${percentage}%)`);
    }
    
    // 2. 上場企業の重複チェック（企業名で）
    console.log("\n【2. 上場企業の重複チェック（企業名ベース）】");
    const listedCompaniesByName = new Map<string, string[]>(); // 企業名 -> ドキュメントIDの配列
    lastDoc = null;
    let listedCount = 0;
    
    while (true) {
      let query = db.collection("companies_new").limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }
      
      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }
      
      for (const doc of snapshot.docs) {
        const data = doc.data();
        if (data.listing === "上場") {
          listedCount++;
          const companyName = data.name || "(名前なし)";
          if (!listedCompaniesByName.has(companyName)) {
            listedCompaniesByName.set(companyName, []);
          }
          listedCompaniesByName.get(companyName)!.push(doc.id);
        }
      }
      
      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }
    
    // 重複している企業名を抽出
    const duplicates = Array.from(listedCompaniesByName.entries())
      .filter(([_, docIds]) => docIds.length > 1)
      .sort((a, b) => b[1].length - a[1].length);
    
    console.log(`上場とマークされているドキュメント数: ${listedCount} 件`);
    console.log(`ユニークな企業名数: ${listedCompaniesByName.size} 件`);
    console.log(`重複している企業名数: ${duplicates.length} 件`);
    
    if (duplicates.length > 0) {
      console.log("\n重複が多い企業名トップ20:");
      duplicates.slice(0, 20).forEach(([name, docIds]) => {
        console.log(`  "${name}": ${docIds.length} 件のドキュメント`);
        console.log(`    ID: ${docIds.slice(0, 5).join(", ")}${docIds.length > 5 ? ` ... (他${docIds.length - 5}件)` : ""}`);
      });
    }
    
    // 3. 法人番号での重複チェック
    console.log("\n【3. 上場企業の重複チェック（法人番号ベース）】");
    const listedCompaniesByCorporateNumber = new Map<string, string[]>(); // 法人番号 -> ドキュメントIDの配列
    lastDoc = null;
    
    while (true) {
      let query = db.collection("companies_new").limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }
      
      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }
      
      for (const doc of snapshot.docs) {
        const data = doc.data();
        if (data.listing === "上場" && data.corporateNumber) {
          const corporateNumber = data.corporateNumber;
          if (!listedCompaniesByCorporateNumber.has(corporateNumber)) {
            listedCompaniesByCorporateNumber.set(corporateNumber, []);
          }
          listedCompaniesByCorporateNumber.get(corporateNumber)!.push(doc.id);
        }
      }
      
      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }
    
    const duplicatesByCorporateNumber = Array.from(listedCompaniesByCorporateNumber.entries())
      .filter(([_, docIds]) => docIds.length > 1)
      .sort((a, b) => b[1].length - a[1].length);
    
    console.log(`法人番号が設定されている上場企業: ${listedCompaniesByCorporateNumber.size} 件`);
    console.log(`法人番号で重複している企業数: ${duplicatesByCorporateNumber.length} 件`);
    
    if (duplicatesByCorporateNumber.length > 0) {
      console.log("\n法人番号で重複している企業トップ10:");
      duplicatesByCorporateNumber.slice(0, 10).forEach(([corporateNumber, docIds]) => {
        console.log(`  法人番号 ${corporateNumber}: ${docIds.length} 件のドキュメント`);
      });
    }
    
    // 4. サンプルデータを確認
    console.log("\n【4. 上場企業のサンプルデータ（最初の10件）】");
    lastDoc = null;
    let sampleCount = 0;
    
    while (sampleCount < 10) {
      let query = db.collection("companies_new").limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }
      
      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }
      
      for (const doc of snapshot.docs) {
        const data = doc.data();
        if (data.listing === "上場" && sampleCount < 10) {
          sampleCount++;
          console.log(`\n${sampleCount}. ドキュメントID: ${doc.id}`);
          console.log(`   企業名: ${data.name || "(なし)"}`);
          console.log(`   法人番号: ${data.corporateNumber || "(なし)"}`);
          console.log(`   listing: ${data.listing}`);
          console.log(`   corporationType: ${data.corporationType || "(なし)"}`);
          console.log(`   nikkeiCode: ${data.nikkeiCode || "(なし)"}`);
        }
      }
      
      lastDoc = snapshot.docs[snapshot.docs.length - 1];
    }
    
    // 5. 統計サマリー
    console.log("\n【5. 統計サマリー】");
    console.log(`総ドキュメント数: ${totalCount.toLocaleString()} 件`);
    console.log(`listing='上場'のドキュメント数: ${listedCount.toLocaleString()} 件`);
    console.log(`ユニークな企業名数: ${listedCompaniesByName.size.toLocaleString()} 件`);
    console.log(`重複している企業名数: ${duplicates.length.toLocaleString()} 件`);
    console.log(`\n推定される実際の上場企業数: ${listedCompaniesByName.size.toLocaleString()} 件`);
    console.log(`重複による過剰カウント: ${(listedCount - listedCompaniesByName.size).toLocaleString()} 件`);
    
    process.exit(0);
  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  }
}

analyzeListedCompanies();
