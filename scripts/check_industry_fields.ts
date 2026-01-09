import admin from "firebase-admin";
import * as fs from "fs";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

function initFirebaseAdmin() {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS || "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json";
  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  const projectId = serviceAccount.project_id || PROJECT_ID;
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });
  return admin.firestore();
}

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);
  
  const snapshot = await colRef.limit(5000).get();
  const largeSet = new Set<string>();
  const middleSet = new Set<string>();
  const smallSet = new Set<string>();
  
  snapshot.docs.forEach(doc => {
    const data = doc.data();
    if (data.industryLarge) largeSet.add(data.industryLarge);
    if (data.industryMiddle) middleSet.add(data.industryMiddle);
    if (data.industrySmall) smallSet.add(data.industrySmall);
  });
  
  console.log("=== 大分類 ===");
  Array.from(largeSet).sort().forEach(l => console.log(l));
  console.log(`\n合計: ${largeSet.size}種類\n`);
  
  console.log("=== 中分類（サンプル50件） ===");
  Array.from(middleSet).sort().slice(0, 50).forEach(m => console.log(m));
  console.log(`\n合計: ${middleSet.size}種類\n`);
  
  console.log("=== 小分類（サンプル50件） ===");
  Array.from(smallSet).sort().slice(0, 50).forEach(s => console.log(s));
  console.log(`\n合計: ${smallSet.size}種類\n`);
}

main().catch(console.error);


