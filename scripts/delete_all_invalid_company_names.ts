/* 
  companies_newコレクションのnameフィールドが有効な企業名でない
  ドキュメントを全量スキャンして削除するスクリプト

  有効な企業名の定義:
    - 「株式会社〇〇」の形式（法人格で始まる）
    - 「〇〇株式会社」の形式（法人格で終わる）
    - その他の法人格でも同様（有限会社、合資会社など）

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/delete_all_invalid_company_names.ts [--dry-run]
*/

import * as fs from "fs";
import admin from "firebase-admin";
import {
  Firestore,
  QueryDocumentSnapshot,
  WriteBatch,
} from "firebase-admin/firestore";
import type { DocumentData } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const BATCH_SIZE = 500; // Firestoreのバッチ制限

// 法人格のパターン
const CORPORATE_SUFFIXES = [
  '株式会社', '有限会社', '合資会社', '合名会社', '合同会社',
  '一般社団法人', '一般財団法人', '公益社団法人', '公益財団法人',
  '学校法人', '医療法人', '社会福祉法人', '宗教法人',
  '特定非営利活動法人', 'NPO法人', '協同組合', '農業協同組合',
  '生活協同組合', '信用金庫', '信用組合', '労働金庫',
  '相互会社', '特殊会社', '地方公共団体', '独立行政法人',
  '税理士法人', '司法書士法人', '弁理士法人', '行政書士法人',
  '土地家屋調査士法人', '社会保険労務士法人',
  '国立大学法人', '公立大学法人', '私立大学法人',
  '国立研究開発法人', '地方独立行政法人',
  '投資法人', '特定目的会社', '有限責任事業組合',
  '商工会議所', '商工会', '工業組合', '事業協同組合',
  '森林組合', '農業共済組合', '漁業協同組合',
  '住宅供給公社', '土地開発公社', '地方公営企業',
  '公認会計士', '税理士', '司法書士', '行政書士', // 専門職事務所
  '事務所', '法律事務所', '会計事務所', '税理士事務所',
];

// 旧字体の法人格もチェック
const OLD_STYLE_SUFFIXES = [
  '株式會社', '有限會社', '合資會社', '合名會社'
];

// 代表者名や役員名のパターン
const PERSON_NAME_PATTERNS = [
  /^（取）.*/,  // （取）で始まる
  /^\(取\).*/,  // (取)で始まる
  /^（専）.*/,  // （専）で始まる
  /^（常）.*/,  // （常）で始まる
  /^（代会）.*/, // （代会）で始まる
  /^\(社長\).*/, // (社長)で始まる
  /^（社長）.*/, // （社長）で始まる
  /^\d{4}年\d{1,2}月\d{1,2}日$/, // 日付パターン
  /^\d{4}\/\d{1,2}\/\d{1,2}$/, // 日付パターン
];

function isLikelyPersonName(name: string): boolean {
  if (!name || typeof name !== 'string') {
    return false;
  }

  const trimmed = name.trim();

  // パターンマッチング
  for (const pattern of PERSON_NAME_PATTERNS) {
    if (pattern.test(trimmed)) {
      return true;
    }
  }

  // カンマ区切りで複数の名前が列挙されている場合
  if (trimmed.includes('，') || trimmed.includes(',')) {
    const parts = trimmed.split(/[，,]/);
    if (parts.length >= 2 && parts.every(p => p.trim().length <= 10)) {
      return true;
    }
  }

  return false;
}

function isLikelyBusinessDescription(name: string): boolean {
  if (!name || typeof name !== 'string') {
    return false;
  }

  const trimmed = name.trim();

  // 事業内容を示すキーワード
  const businessKeywords = [
    '業務', '代行', '製造', '販売', '卸売', '小売', '運送', '建設',
    '工事', '設計', '開発', '管理', '運営', 'サービス', '事業', '業',
    '調達', 'メンテナンス', '製造・', '販売、', '運送、', '工事、',
    'の運営', 'を行う', 'を手掛ける', 'を担当', 'を提供', 'の輸入',
    'の輸出', 'の修理', 'の調査', 'の研究', 'の研修', 'の販売',
  ];

  for (const keyword of businessKeywords) {
    if (trimmed.includes(keyword)) {
      return true;
    }
  }

  // カンマ区切りで複数の事業内容が列挙されている場合
  if ((trimmed.includes('，') || trimmed.includes(',')) && trimmed.length > 20) {
    return true;
  }

  // 長い説明文（30文字以上）の場合は事業内容の可能性
  if (trimmed.length >= 30) {
    return true;
  }

  return false;
}

/**
 * 有効な企業名かどうかを判定
 * 「株式会社〇〇」や「〇〇株式会社」の形式のみを企業名と判断
 * 代表者名、事業内容などが含まれている場合は無効と判断
 */
function isValidCompanyName(name: string | null | undefined): boolean {
  if (!name || typeof name !== 'string') {
    return false;
  }

  const trimmed = name.trim();
  if (!trimmed) {
    return false;
  }

  // 個人名の可能性がある場合は無効
  if (isLikelyPersonName(trimmed)) {
    return false;
  }

  // 事業内容の可能性がある場合は無効（法人格が含まれていても）
  if (isLikelyBusinessDescription(trimmed)) {
    return false;
  }

  // すべての法人格（通常+旧字体）をチェック
  const allSuffixes = [...CORPORATE_SUFFIXES, ...OLD_STYLE_SUFFIXES];

  for (const suffix of allSuffixes) {
    // 「法人格〇〇」の形式（法人格で始まる）
    if (trimmed.startsWith(suffix)) {
      const companyName = trimmed.substring(suffix.length).trim();
      // 法人格の後に企業名が続いている場合（空でない、かつ個人名パターンでない、かつ事業内容でない）
      if (companyName && !isLikelyPersonName(companyName) && !isLikelyBusinessDescription(companyName)) {
        return true;
      }
    }
    
    // 「〇〇法人格」の形式（法人格で終わる）
    if (trimmed.endsWith(suffix)) {
      const companyName = trimmed.substring(0, trimmed.length - suffix.length).trim();
      // 法人格の前に企業名がある場合（空でない、かつ個人名パターンでない、かつ事業内容でない）
      if (companyName && !isLikelyPersonName(companyName) && !isLikelyBusinessDescription(companyName)) {
        return true;
      }
    }
  }

  return false;
}

function initFirebase() {
  if (admin.apps.length === 0) {
    const serviceAccountPath =
      process.env.GOOGLE_APPLICATION_CREDENTIALS ||
      './serviceAccountKey.json';

    if (!fs.existsSync(serviceAccountPath)) {
      console.error(
        '❌ エラー: サービスアカウントキーファイルが見つかりません'
      );
      console.error(`   パス: ${serviceAccountPath}`);
      process.exit(1);
    }

    const serviceAccount = JSON.parse(
      fs.readFileSync(serviceAccountPath, 'utf8')
    );
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  }
}

interface DeletedDocumentInfo {
  docId: string;
  name: string;
  corporateNumber: string | null;
  sourceFile: string | null;
  sourceRow: number | null;
}

function extractSourceFile(data: any): {
  file: string | null;
  row: number | null;
} {
  // lastImportSource フィールドを確認
  if (data?.lastImportSource) {
    return {
      file: data.lastImportSource.file || null,
      row: data.lastImportSource.row || null,
    };
  }

  // source フィールドを確認（旧形式）
  if (data?.source) {
    return {
      file: data.source.file || null,
      row: data.source.row || null,
    };
  }

  return { file: null, row: null };
}

async function deleteInvalidCompanies(db: Firestore): Promise<void> {
  const companiesCol = db.collection(COLLECTION_NAME);
  
  console.log(`\n🔍 companies_newコレクションを全件スキャン中...`);
  console.log(`   モード: ${DRY_RUN ? 'DRY RUN (実際には削除しません)' : '実際に削除'}\n`);

  let totalScanned = 0;
  let invalidCount = 0;
  let deletedCount = 0;
  let lastDoc: QueryDocumentSnapshot<DocumentData> | null = null;
  const batchSize = 1000;
  
  let batch: WriteBatch = db.batch();
  let batchCount = 0;
  
  // 削除対象のドキュメント情報を記録
  const deletedDocs: DeletedDocumentInfo[] = [];
  const reportFile = `deleted_invalid_companies_${Date.now()}.json`;

  while (true) {
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(batchSize);

    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();

    if (snapshot.empty) {
      break;
    }

    totalScanned += snapshot.size;

    for (const doc of snapshot.docs) {
      const data = doc.data();
      const name = data.name;

      // 有効な企業名かどうかを判定
      // 「株式会社〇〇」や「〇〇株式会社」の形式のみを企業名と判断
      const isValid = isValidCompanyName(name);
      
      if (!isValid) {
        invalidCount++;
        
        // ソースファイル情報を取得
        const source = extractSourceFile(data);
        
        // 削除対象の情報を記録
        deletedDocs.push({
          docId: doc.id,
          name: name || '(空)',
          corporateNumber: data.corporateNumber || null,
          sourceFile: source.file,
          sourceRow: source.row,
        });
        
        if (DRY_RUN) {
          console.log(`  [DRY RUN] 削除予定: ${doc.id} - ${(name || '(空)').substring(0, 60)} (${source.file || '(不明)'})`);
        } else {
          batch.delete(doc.ref);
          batchCount++;
          deletedCount++;

          // バッチ制限に達したらコミット
          if (batchCount >= BATCH_SIZE) {
            await batch.commit();
            console.log(`  ✅ 削除済み: ${deletedCount}件 (スキャン済み: ${totalScanned}件, 問題: ${invalidCount}件)`);
            batch = db.batch();
            batchCount = 0;
          }
        }
      }

      if (totalScanned % 10000 === 0) {
        console.log(`  処理中: ${totalScanned}件 (問題: ${invalidCount}件, 削除済み: ${deletedCount}件)`);
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];

    if (snapshot.size < batchSize) {
      break;
    }
  }

  // 残りのバッチをコミット
  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    console.log(`  ✅ 最終バッチ削除済み: ${batchCount}件`);
  }

  // CSVファイル別に集計
  const byCsvFile: Record<string, DeletedDocumentInfo[]> = {};
  for (const doc of deletedDocs) {
    const file = doc.sourceFile || '(不明)';
    if (!byCsvFile[file]) {
      byCsvFile[file] = [];
    }
    byCsvFile[file].push(doc);
  }

  // レポートを保存
  const report = {
    summary: {
      total: deletedDocs.length,
      byFile: Object.fromEntries(
        Object.entries(byCsvFile).map(([file, docs]) => [file, docs.length])
      ),
      generatedAt: new Date().toISOString(),
    },
    deletedDocuments: deletedDocs,
    groupedByFile: byCsvFile,
  };

  fs.writeFileSync(reportFile, JSON.stringify(report, null, 2), 'utf8');

  if (DRY_RUN) {
    console.log(`\n⚠️  DRY RUNモード: 実際には削除していません`);
    console.log(`   スキャン済み: ${totalScanned}件`);
    console.log(`   削除対象: ${invalidCount}件`);
    console.log(`   実際に削除するには --dry-run フラグを外してください`);
  } else {
    console.log(`\n✅ 削除完了:`);
    console.log(`   スキャン済み: ${totalScanned}件`);
    console.log(`   削除対象: ${invalidCount}件`);
    console.log(`   削除済み: ${deletedCount}件`);
  }

  console.log(`\n📋 削除レポートを保存しました: ${reportFile}`);
  console.log(`   削除されたドキュメント: ${deletedDocs.length}件`);
  console.log(`   CSVファイル別の内訳:`);
  
  const sortedFiles = Object.entries(byCsvFile)
    .sort((a, b) => b[1].length - a[1].length);
  
  for (const [file, docs] of sortedFiles.slice(0, 20)) {
    console.log(`     - ${file}: ${docs.length}件`);
  }
  
  if (sortedFiles.length > 20) {
    console.log(`     ... 他 ${sortedFiles.length - 20}ファイル`);
  }
  
  console.log(`\n💡 再インポート方法:`);
  console.log(`   各CSVファイルを以下のコマンドで再インポートしてください:`);
  console.log(`   GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \\`);
  console.log(`   npx ts-node scripts/import_csv_by_groups.ts ./csv/<ファイル名>`);
}

async function main() {
  initFirebase();
  const db = admin.firestore();

  // 削除前に全量レポートを生成（既に削除済みの場合は0件になる）
  console.log('📋 削除前に全量レポートを生成します...\n');
  await generateFullReportBeforeDelete(db);

  // 削除実行
  await deleteInvalidCompanies(db);
}

async function generateFullReportBeforeDelete(db: Firestore): Promise<void> {
  const companiesCol = db.collection(COLLECTION_NAME);
  const invalidCompanies: DeletedDocumentInfo[] = [];

  console.log('🔍 全件スキャン中（レポート生成用）...');

  let totalScanned = 0;
  let lastDoc: QueryDocumentSnapshot<DocumentData> | null = null;
  const batchSize = 1000;

  while (true) {
    let query = companiesCol
      .orderBy(admin.firestore.FieldPath.documentId())
      .limit(batchSize);

    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();

    if (snapshot.empty) {
      break;
    }

    totalScanned += snapshot.size;

    for (const doc of snapshot.docs) {
      const data = doc.data();
      const name = data.name;

      // 有効な企業名かどうかを判定
      const isValid = isValidCompanyName(name);

      if (!isValid) {
        const source = extractSourceFile(data);
        invalidCompanies.push({
          docId: doc.id,
          name: name || '(空)',
          corporateNumber: data.corporateNumber || null,
          sourceFile: source.file,
          sourceRow: source.row,
        });
      }
    }

    if (totalScanned % 10000 === 0) {
      console.log(`  処理中: ${totalScanned}件 (問題: ${invalidCompanies.length}件)`);
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];

    if (snapshot.size < batchSize) {
      break;
    }
  }

  // CSVファイル別に集計
  const byCsvFile: Record<string, DeletedDocumentInfo[]> = {};
  for (const doc of invalidCompanies) {
    const file = doc.sourceFile || '(不明)';
    if (!byCsvFile[file]) {
      byCsvFile[file] = [];
    }
    byCsvFile[file].push(doc);
  }

  // レポートを保存
  const reportFile = `invalid_companies_report_before_delete_${Date.now()}.json`;
  const report = {
    summary: {
      total: invalidCompanies.length,
      byFile: Object.fromEntries(
        Object.entries(byCsvFile).map(([file, docs]) => [file, docs.length])
      ),
      generatedAt: new Date().toISOString(),
    },
    invalidCompanies: invalidCompanies,
    groupedByFile: byCsvFile,
  };

  fs.writeFileSync(reportFile, JSON.stringify(report, null, 2), 'utf8');

  console.log(`\n✅ レポート生成完了: ${reportFile}`);
  console.log(`   問題のあるドキュメント: ${invalidCompanies.length}件`);
  console.log(`   CSVファイル別の内訳:`);

  const sortedFiles = Object.entries(byCsvFile)
    .sort((a, b) => b[1].length - a[1].length);

  for (const [file, docs] of sortedFiles.slice(0, 20)) {
    console.log(`     - ${file}: ${docs.length}件`);
  }

  if (sortedFiles.length > 20) {
    console.log(`     ... 他 ${sortedFiles.length - 20}ファイル`);
  }

  console.log('\n');
}

main().catch((error) => {
  console.error('❌ エラー:', error);
  process.exit(1);
});
