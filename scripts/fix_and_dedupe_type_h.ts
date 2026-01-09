/*
  タイプHの修正・統合処理スクリプト（130.csv, 131.csv）
  
  - 企業名+住所などで同じ企業を特定して1つに統合
  - CSVの内容を正として既存データを上書き
  - 役員情報と部署情報の適切な処理
  
  使い方:
    # DRY RUN
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_and_dedupe_type_h.ts --dry-run
    
    # 実行
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_and_dedupe_type_h.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentReference } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// タイプHのCSVファイル一覧
const TYPE_H_FILES = ["csv/130.csv", "csv/131.csv"];

// Firebase初期化
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    const projectRoot = process.cwd();
    const defaultPath = path.join(projectRoot, "albert-ma-firebase-adminsdk-iat1k-a64039899f.json");
    if (fs.existsSync(defaultPath)) {
      serviceAccountPath = defaultPath;
    }
  }

  if (!serviceAccountPath || !fs.existsSync(serviceAccountPath)) {
    console.error("❌ エラー: サービスアカウントキーファイルが見つかりません");
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: serviceAccount.project_id,
  });

  console.log(`✅ Firebase 初期化完了`);
}

const db: Firestore = admin.firestore();

function trim(v?: string | null): string | null {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s === "" ? null : s;
}

function normalizeStr(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "").replace(/株式会社|有限会社|合同会社|合名会社/g, "");
}

function normalizeAddress(v: string | null | undefined): string {
  if (!v) return "";
  return String(v).trim().toLowerCase().replace(/\s+/g, "");
}

function parseNumeric(v: string): number | null {
  const cleaned = v.replace(/[^\d.-]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

// フィールドマッピング（タイプH用）
// executiveTitleXはexecutivePositionXにマッピング
const FIELD_MAPPING: Record<string, string> = {
  "name": "name",
  "corporateNumber": "corporateNumber",
  "representativeName": "representativeName",
  "revenue": "revenue",
  "capitalStock": "capitalStock",
  "listing": "listing",
  "address": "address",
  "employeeCount": "employeeCount",
  "established": "established",
  "fiscalMonth": "fiscalMonth",
  "industryLarge": "industryLarge",
  "industryMiddle": "industryMiddle",
  "industrySmall": "industrySmall",
  "industryDetail": "industryDetail",
  "phoneNumber": "phoneNumber",
  "companyUrl": "companyUrl",
  "bankCorporateNumber": "bankCorporateNumber",
  // 部署情報（7部署まで）
  "departmentName1": "departmentName1",
  "departmentAddress1": "departmentAddress1",
  "departmentPhone1": "departmentPhone1",
  "departmentName2": "departmentName2",
  "departmentAddress2": "departmentAddress2",
  "departmentPhone2": "departmentPhone2",
  "departmentName3": "departmentName3",
  "departmentAddress3": "departmentAddress3",
  "departmentPhone3": "departmentPhone3",
  "departmentName4": "departmentName4",
  "departmentAddress4": "departmentAddress4",
  "departmentPhone4": "departmentPhone4",
  "departmentName5": "departmentName5",
  "departmentAddress5": "departmentAddress5",
  "departmentPhone5": "departmentPhone5",
  "departmentName6": "departmentName6",
  "departmentAddress6": "departmentAddress6",
  "departmentPhone6": "departmentPhone6",
  "departmentName7": "departmentName7",
  "departmentAddress7": "departmentAddress7",
  "departmentPhone7": "departmentPhone7",
  // 役員情報（10人まで）
  "executiveName1": "executiveName1",
  "executivePosition1": "executivePosition1",
  "executiveTitle1": "executivePosition1",  // executiveTitleはexecutivePositionにマッピング
  "executiveName2": "executiveName2",
  "executivePosition2": "executivePosition2",
  "executiveTitle2": "executivePosition2",
  "executiveName3": "executiveName3",
  "executivePosition3": "executivePosition3",
  "executiveTitle3": "executivePosition3",
  "executiveName4": "executiveName4",
  "executivePosition4": "executivePosition4",
  "executiveTitle4": "executivePosition4",
  "executiveName5": "executiveName5",
  "executivePosition5": "executivePosition5",
  "executiveTitle5": "executivePosition5",
  "executiveName6": "executiveName6",
  "executivePosition6": "executivePosition6",
  "executiveTitle6": "executivePosition6",
  "executiveName7": "executiveName7",
  "executivePosition7": "executivePosition7",
  "executiveTitle7": "executivePosition7",
  "executiveName8": "executiveName8",
  "executivePosition8": "executivePosition8",
  "executiveTitle8": "executivePosition8",
  "executiveName9": "executiveName9",
  "executivePosition9": "executivePosition9",
  "executiveTitle9": "executivePosition9",
  "executiveName10": "executiveName10",
  "executivePosition10": "executivePosition10",
  "executiveTitle10": "executivePosition10",
};

interface CsvRow {
  [key: string]: string;
}

interface CompanyData {
  csvFile: string;
  rowIndex: number;
  mappedData: Record<string, any>;
  normName: string;
  normAddr: string;
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log("📊 タイプHの修正・統合処理を開始します\n");
  console.log("⚠️  注意: CSVの内容を正として既存データを上書きします\n");

  const allCompanies: CompanyData[] = [];

  // CSVファイルを読み込み
  for (const file of TYPE_H_FILES) {
    const filePath = path.resolve(file);
    if (!fs.existsSync(filePath)) {
      console.warn(`⚠️  ファイルが見つかりません: ${file}`);
      continue;
    }

    const buf = fs.readFileSync(filePath);
    try {
      const records: CsvRow[] = parse(buf, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true,
        relax_column_count: true,
        skip_records_with_error: true,
      });

      console.log(`📄 ${path.basename(file)}: ${records.length} 行`);

      records.forEach((row, idx) => {
        const mappedData: Record<string, any> = {};
        
        // CSVヘッダーを正しいフィールドにマッピング
        for (const [header, value] of Object.entries(row)) {
          const trimmedHeader = header.trim();
          const mappedField = FIELD_MAPPING[trimmedHeader];
          
          if (!mappedField) {
            continue;
          }

          const trimmedValue = trim(value);
          if (trimmedValue === null) continue;

          // 既存のフィールドに値がある場合は、新しい値とマージ（executivePositionの場合）
          if (mappedData[mappedField] && mappedField.startsWith("executivePosition")) {
            // 既に値がある場合はスキップ（最初の値を優先）
            continue;
          }

          // 数値フィールドの処理
          if (["capitalStock", "employeeCount", "revenue", "latestRevenue"].includes(mappedField)) {
            const num = parseNumeric(trimmedValue);
            if (num !== null) {
              mappedData[mappedField] = num;
            }
          } else if (mappedField === "corporateNumber") {
            // 法人番号は13桁の数値のみ有効
            const digits = trimmedValue.replace(/\D/g, "");
            if (digits.length === 13) {
              mappedData[mappedField] = digits;
            }
          } else {
            mappedData[mappedField] = trimmedValue;
          }
        }

        const name = mappedData.name;
        if (!name) return;

        allCompanies.push({
          csvFile: path.basename(file),
          rowIndex: idx + 1,
          mappedData,
          normName: normalizeStr(name),
          normAddr: normalizeAddress(mappedData.address),
        });
      });

    } catch (err: any) {
      console.warn(`  ⚠️ ${path.basename(file)}: CSVパースエラー - ${err.message}`);
    }
  }

  console.log(`\n📊 総レコード数: ${allCompanies.length}\n`);

  // 重複検出: 企業名+住所でグループ化
  const duplicateGroups = new Map<string, CompanyData[]>();

  for (const company of allCompanies) {
    // キー生成: 企業名 + 住所
    let key = company.normName;
    if (company.normAddr) {
      key += "|" + company.normAddr.substring(0, 30);
    }

    if (!duplicateGroups.has(key)) {
      duplicateGroups.set(key, []);
    }
    duplicateGroups.get(key)!.push(company);
  }

  // 2件以上のレコードを持つグループのみを抽出（重複）
  const actualDuplicates = Array.from(duplicateGroups.entries())
    .filter(([_, companies]) => companies.length > 1);

  console.log(`🔍 重複検出結果:`);
  console.log(`  - 重複グループ数: ${actualDuplicates.length}`);
  console.log(`  - 重複レコード総数: ${actualDuplicates.reduce((sum, [_, companies]) => sum + companies.length, 0)}`);

  // Firestore に統合して保存（CSV を正とする）
  console.log(`\n📝 Firestoreへの保存・統合処理を開始します（CSV優先モード）...\n`);

  let createdCount = 0;
  let updatedCount = 0;
  let mergedCount = 0;

  for (const [key, companies] of duplicateGroups.entries()) {
    // 最も情報が充実しているレコードをベースにする
    const sortedCompanies = companies.sort((a, b) => {
      return Object.keys(b.mappedData).length - Object.keys(a.mappedData).length;
    });

    const master = sortedCompanies[0];
    const others = sortedCompanies.slice(1);

    // マスターに他のレコードの情報をマージ
    const mergedData: Record<string, any> = { ...master.mappedData };
    
    for (const other of others) {
      for (const [field, value] of Object.entries(other.mappedData)) {
        // CSVの内容を正とするため、より新しい（情報が多い）値で上書き
        if ((mergedData[field] === null || 
             mergedData[field] === undefined || 
             mergedData[field] === "") &&
            value !== null && 
            value !== undefined && 
            value !== "") {
          mergedData[field] = value;
        }
      }
    }

    // Firestoreで既存ドキュメントを検索
    let existingDoc: DocumentReference | null = null;

    // 法人番号で検索
    if (mergedData.corporateNumber) {
      const snap = await db.collection(COLLECTION_NAME)
        .where("corporateNumber", "==", mergedData.corporateNumber)
        .limit(1)
        .get();
      
      if (!snap.empty) {
        existingDoc = snap.docs[0].ref;
      }
    }

    // 企業名で検索（法人番号で見つからない場合）
    if (!existingDoc && mergedData.name) {
      const snap = await db.collection(COLLECTION_NAME)
        .where("name", "==", mergedData.name)
        .limit(10)
        .get();
      
      if (!snap.empty) {
        // 住所や郵便番号でさらに絞り込み
        for (const doc of snap.docs) {
          const data = doc.data();
          const docAddr = normalizeAddress(data.address);
          const companyAddr = normalizeAddress(mergedData.address);
          
          if (docAddr && companyAddr && docAddr === companyAddr) {
            existingDoc = doc.ref;
            break;
          }
        }
      }
    }

    if (existingDoc) {
      // 既存ドキュメントを更新（CSV を正として上書き）
      const updateData: Record<string, any> = {};

      for (const [field, value] of Object.entries(mergedData)) {
        // CSVの値で常に上書き（CSV を正とする）
        if (value !== null && value !== undefined && value !== "") {
          updateData[field] = value;
        }
      }

      if (Object.keys(updateData).length > 0) {
        if (!DRY_RUN) {
          await existingDoc.update({
            ...updateData,
            updatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
        }
        updatedCount++;
        
        if (companies.length > 1) {
          mergedCount++;
          console.log(`🔄 統合＋更新（CSV優先）: ${mergedData.name} (${companies.length}件を統合)`);
        } else {
          console.log(`📝 更新（CSV優先）: ${mergedData.name}`);
        }
      }
    } else {
      // 新規作成
      const docId = mergedData.corporateNumber || 
                    `${Date.now()}${String(createdCount).padStart(6, "0")}`;
      
      if (!DRY_RUN) {
        await db.collection(COLLECTION_NAME).doc(docId).set({
          ...mergedData,
          csvType: "type_h",
          createdAt: admin.firestore.FieldValue.serverTimestamp(),
          updatedAt: admin.firestore.FieldValue.serverTimestamp(),
        });
      }
      
      createdCount++;
      
      if (companies.length > 1) {
        mergedCount++;
        console.log(`🆕 新規作成（統合）: ${mergedData.name} (${companies.length}件を統合)`);
      } else {
        console.log(`🆕 新規作成: ${mergedData.name}`);
      }
    }
  }

  console.log(`\n✅ 処理完了`);
  console.log(`  - 新規作成: ${createdCount} 件`);
  console.log(`  - 更新（CSV優先）: ${updatedCount} 件`);
  console.log(`  - 統合処理: ${mergedCount} グループ`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に保存するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

