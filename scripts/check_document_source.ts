import "dotenv/config";
import admin from "firebase-admin";

function initAdmin() {
  if (admin.apps.length) return;
  try {
    admin.initializeApp({
      credential: admin.credential.applicationDefault(),
    });
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", (error as Error).message);
    console.error("   環境変数 GOOGLE_APPLICATION_CREDENTIALS が正しく設定されているか確認してください");
    throw error;
  }
}

async function checkDocumentSource(documentId: string) {
  initAdmin();
  const db = admin.firestore();
  const companiesCol = db.collection("companies_new");

  try {
    const docRef = companiesCol.doc(documentId);
    const docSnap = await docRef.get();

    if (!docSnap.exists) {
      console.log(`❌ ドキュメントID ${documentId} は存在しません`);
      return;
    }

    const data = docSnap.data();
    console.log(`\n📄 ドキュメントID: ${documentId}`);
    console.log(`📋 企業名: ${data?.name || "(未設定)"}`);
    console.log(`\n🔍 ソース情報:`);
    
    // source フィールドを確認
    if (data?.source) {
      console.log(`  - source.file: ${data.source.file || "(未設定)"}`);
      console.log(`  - source.row: ${data.source.row || "(未設定)"}`);
      if (data.source.rawHeader) {
        console.log(`  - source.rawHeader: ${JSON.stringify(data.source.rawHeader)}`);
      }
    } else {
      console.log(`  - source: (未設定)`);
    }

    // lastImportSource フィールドを確認
    if (data?.lastImportSource) {
      console.log(`  - lastImportSource.file: ${data.lastImportSource.file || "(未設定)"}`);
      console.log(`  - lastImportSource.row: ${data.lastImportSource.row || "(未設定)"}`);
    } else {
      console.log(`  - lastImportSource: (未設定)`);
    }

    // その他の関連情報
    console.log(`\n📊 その他の情報:`);
    console.log(`  - 法人番号: ${data?.corporateNumber || "(未設定)"}`);
    console.log(`  - 住所: ${data?.address || "(未設定)"}`);
    console.log(`  - 更新日時: ${data?.updatedAt ? (data.updatedAt as any).toDate?.() || data.updatedAt : "(未設定)"}`);

    // 結論
    const sourceFile = data?.source?.file || data?.lastImportSource?.file;
    if (sourceFile) {
      console.log(`\n✅ このドキュメントは "${sourceFile}" からインポートされました`);
    } else {
      console.log(`\n⚠️  ソースファイル情報が見つかりませんでした`);
    }

  } catch (error) {
    console.error("❌ エラー:", (error as Error).message);
    throw error;
  }
}

async function main() {
  const documentId = process.argv[2];
  if (!documentId) {
    console.error("使用方法: npx tsx scripts/check_document_source.ts <documentId>");
    process.exit(1);
  }

  await checkDocumentSource(documentId);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
