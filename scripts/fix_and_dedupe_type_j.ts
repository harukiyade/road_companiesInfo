/*
  タイプJの修正・統合処理スクリプト（133.csv, 134.csv, 135.csv, 136.csv）
  
  - 企業名+住所などで同じ企業を特定して1つに統合
  - フィールドマッピングを修正
  - 部署・拠点情報の処理
  
  使い方:
    # DRY RUN
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_and_dedupe_type_j.ts --dry-run
    
    # 実行
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_and_dedupe_type_j.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentReference } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// タイプJのCSVファイル一覧
const TYPE_J_FILES = [
  "csv/133.csv", "csv/134.csv",
  "csv/speeda/135.csv", "csv/speeda/136.csv",
  "csv/speeda/137.csv", "csv/speeda/138.csv", "csv/speeda/139.csv"
];

// 無視するフィールド
const IGNORED_FIELDS = new Set([
  "会社ID", "ID"
]);

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

// フィールドマッピング（タイプJ用）
const FIELD_MAPPING: Record<string, string> = {
  "会社名": "name",
  "企業名": "name",
  "都道府県": "prefecture",
  "代表者名": "representativeName",
  "代表名": "representativeName",  // 134.csvの場合
  "法人番号": "corporateNumber",
  "URL": "companyUrl",
  "業種1": "industryLarge",
  "業種2": "industryMiddle",
  "業種3": "industrySmall",
  "業種4": "industryDetail",
  "郵便番号": "postalCode",
  "住所": "address",
  "設立": "established",
  "電話番号(窓口)": "contactPhoneNumber",
  "電話番号": "phoneNumber",
  "代表者郵便番号": "representativePostalCode",
  "代表者住所": "representativeHomeAddress",
  "代表者誕生日": "representativeBirthDate",
  "資本金": "capitalStock",
  "上場": "listing",
  "直近決算年月": "latestFiscalYearMonth",
  "直近売上": "latestRevenue",
  "直近利益": "latestProfit",
  "説明": "companyDescription",
  "概要": "overview",
  "仕入れ先": "suppliers",
  "取引先": "clients",
  "取引先銀行": "banks",
  "取締役": "executives",
  "株主": "shareholders",
  "社員数": "employeeCount",
  "従業員数": "employeeCount",
  "オフィス数": "officeCount",
  "工場数": "factoryCount",
  "店舗数": "storeCount",
  // 部署情報（7部署まで）
  "部署名1": "departmentName1",
  "部署住所1": "departmentAddress1",
  "部署電話番号1": "departmentPhone1",
  "部署名2": "departmentName2",
  "部署住所2": "departmentAddress2",
  "部署電話番号2": "departmentPhone2",
  "部署名3": "departmentName3",
  "部署住所3": "departmentAddress3",
  "部署電話番号3": "departmentPhone3",
  "部署名4": "departmentName4",
  "部署住所4": "departmentAddress4",
  "部署電話番号4": "departmentPhone4",
  "部署名5": "departmentName5",
  "部署住所5": "departmentAddress5",
  "部署電話番号5": "departmentPhone5",
  "部署名6": "departmentName6",
  "部署住所6": "departmentAddress6",
  "部署電話番号6": "departmentPhone6",
  "部署名7": "departmentName7",
  "部署住所7": "departmentAddress7",
  "部署電話番号7": "departmentPhone7",
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
  console.log("📊 タイプJの修正・統合処理を開始します\n");

  const allCompanies: CompanyData[] = [];

  // CSVファイルを読み込み
  for (const file of TYPE_J_FILES) {
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
          
          // 無視するフィールドはスキップ
          if (IGNORED_FIELDS.has(trimmedHeader)) {
            continue;
          }

          const mappedField = FIELD_MAPPING[trimmedHeader];
          if (!mappedField) {
            continue;
          }

          const trimmedValue = trim(value);
          if (trimmedValue === null) continue;

          // 数値フィールドの処理
          if (["capitalStock", "employeeCount", "officeCount", "factoryCount", 
               "storeCount", "latestRevenue", "latestProfit"].includes(mappedField)) {
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
          } else if (mappedField === "suppliers" || mappedField === "banks") {
            // 仕入れ先や取引銀行は配列に変換
            const items = trimmedValue.split(/[、,，]/);
            const cleanedItems = items.map(s => s.trim()).filter(s => s !== "");
            if (cleanedItems.length > 0) {
              mappedData[mappedField] = cleanedItems;
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

  // Firestore に統合して保存
  console.log(`\n📝 Firestoreへの保存・統合処理を開始します...\n`);

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
        if ((mergedData[field] === null || 
             mergedData[field] === undefined || 
             mergedData[field] === "" ||
             (Array.isArray(mergedData[field]) && mergedData[field].length === 0)) &&
            value !== null && 
            value !== undefined && 
            value !== "" &&
            !(Array.isArray(value) && value.length === 0)) {
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
          
          if (data.postalCode && mergedData.postalCode && data.postalCode === mergedData.postalCode) {
            existingDoc = doc.ref;
            break;
          }
        }
      }
    }

    if (existingDoc) {
      // 既存ドキュメントを更新
      const currentData = (await existingDoc.get()).data() || {};
      const updateData: Record<string, any> = {};

      for (const [field, value] of Object.entries(mergedData)) {
        // nameは常に上書き、その他はnullの場合のみ補完
        if (field === "name") {
          if (currentData[field] !== value) {
            updateData[field] = value;
          }
        } else if ((field === "suppliers" || field === "banks") && Array.isArray(value)) {
          // 配列フィールドは既存の値とマージ
          const existingItems = Array.isArray(currentData[field]) ? currentData[field] : [];
          const newItems = [...new Set([...existingItems, ...value])];
          if (JSON.stringify([...existingItems].sort()) !== JSON.stringify([...newItems].sort())) {
            updateData[field] = newItems;
          }
        } else {
          if ((currentData[field] === null || 
               currentData[field] === undefined || 
               currentData[field] === "") &&
              value !== null && 
              value !== undefined && 
              value !== "") {
            updateData[field] = value;
          }
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
          console.log(`🔄 統合＋更新: ${mergedData.name} (${companies.length}件を統合)`);
        } else {
          console.log(`📝 更新: ${mergedData.name}`);
        }
      }
    } else {
      // 新規作成
      const docId = mergedData.corporateNumber || 
                    `${Date.now()}${String(createdCount).padStart(6, "0")}`;
      
      if (!DRY_RUN) {
        await db.collection(COLLECTION_NAME).doc(docId).set({
          ...mergedData,
          csvType: "type_j",
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
  console.log(`  - 更新: ${updatedCount} 件`);
  console.log(`  - 統合処理: ${mergedCount} グループ`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に保存するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

