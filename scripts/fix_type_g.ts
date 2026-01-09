/*
  タイプGの修正処理スクリプト（127.csv, 128.csv）
  
  - 英語ヘッダー名を正しいフィールド名にマッピング
  - valuesearch.nikkei.comで始まるURLを削除
  - フィールド内容の修正
  
  使い方:
    # DRY RUN
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_type_g.ts --dry-run
    
    # 実行
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_type_g.ts
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentReference } from "firebase-admin/firestore";
import { parse } from "csv-parse/sync";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");

// タイプGのCSVファイル一覧
const TYPE_G_FILES = ["csv/127.csv", "csv/128.csv"];

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

function parseNumeric(v: string): number | null {
  const cleaned = v.replace(/[^\d.-]/g, "");
  if (!cleaned) return null;
  const n = Number(cleaned);
  return Number.isFinite(n) ? n : null;
}

function cleanUrl(url: string | null): string | null {
  if (!url) return null;
  
  // valuesearch.nikkei.comで始まるURLは削除
  if (url.includes("valuesearch.nikkei.com")) {
    return null;
  }
  
  return url;
}

// 銀行名のクリーニング（借入金額などの情報を削除）
function cleanBankNames(banksStr: string | null): string[] {
  if (!banksStr) return [];
  
  const banks: string[] = [];
  const parts = banksStr.split(/[・、,]/);
  
  for (const part of parts) {
    const trimmed = part.trim();
    if (!trimmed) continue;
    
    // 銀行名のみを抽出（括弧内の情報を削除）
    const bankName = trimmed.replace(/[（(].*?[）)]/g, "").trim();
    if (bankName && !banks.includes(bankName)) {
      banks.push(bankName);
    }
  }
  
  return banks;
}

// フィールドマッピング（タイプG用）
const FIELD_MAPPING: Record<string, string> = {
  "会社名": "name",
  "会社名（英語）": "nameEn",
  "法人番号": "corporateNumber",
  "都道府県": "prefecture",
  "住所": "address",
  "業種": "industry",
  "資本金": "capitalStock",
  "売上": "revenue",
  "直近売上": "latestRevenue",
  "直近利益": "latestProfit",
  "従業員数": "employeeCount",
  "発行株式数": "issuedShares",
  "設立": "established",
  "決算月": "fiscalMonth",
  "上場": "listing",
  "代表者名": "representativeName",
  "代表者役職": "representativeTitle",
  "businessDescriptions": "businessDescriptions",
  "URL": "companyUrl",
  "contactUrl": "companyUrl",  // contactUrlもcompanyUrlにマッピング
  "銀行": "banks",
  "取引銀行": "banks",
  "取引先銀行": "banks",
  "affiliations": "affiliations",
  "overview": "overview",
  "history": "specialNote",  // historyはspecialNoteにマッピング
  "totalAssets": "totalAssets",
  "totalLiabilities": "totalLiabilities",
  "netAssets": "netAssets",
  "revenueFromStatements": "revenueFromStatements",
  "operatingIncome": "operatingIncome",
};

interface CsvRow {
  [key: string]: string;
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log("📊 タイプGの修正処理を開始します\n");

  let processedCount = 0;
  let updatedCount = 0;
  let urlsRemovedCount = 0;

  // CSVファイルを処理
  for (const file of TYPE_G_FILES) {
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

      console.log(`📄 ${path.basename(file)}: ${records.length} 行\n`);

      for (const row of records) {
        const mappedData: Record<string, any> = {};
        let urlRemoved = false;
        
        // CSVヘッダーを正しいフィールドにマッピング
        for (const [header, value] of Object.entries(row)) {
          const trimmedHeader = header.trim();
          const mappedField = FIELD_MAPPING[trimmedHeader];
          
          if (!mappedField) {
            // マッピングが定義されていない場合は、そのまま使う（小文字化）
            const lowerField = trimmedHeader.toLowerCase().replace(/\s+/g, "");
            if (lowerField) {
              const trimmedValue = trim(value);
              if (trimmedValue) {
                mappedData[lowerField] = trimmedValue;
              }
            }
            continue;
          }

          const trimmedValue = trim(value);
          if (trimmedValue === null) continue;

          // URL のクリーニング
          if (mappedField === "companyUrl") {
            const cleanedUrl = cleanUrl(trimmedValue);
            if (cleanedUrl) {
              mappedData[mappedField] = cleanedUrl;
            } else if (trimmedValue) {
              urlRemoved = true;
              urlsRemovedCount++;
            }
            continue;
          }

          // 銀行名のクリーニング
          if (mappedField === "banks") {
            const cleanedBanks = cleanBankNames(trimmedValue);
            if (cleanedBanks.length > 0) {
              mappedData[mappedField] = cleanedBanks;
            }
            continue;
          }

          // 数値フィールドの処理
          if (["capitalStock", "employeeCount", "revenue", "latestRevenue", "latestProfit", 
               "issuedShares", "totalAssets", "totalLiabilities", "netAssets", 
               "revenueFromStatements", "operatingIncome"].includes(mappedField)) {
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
        if (!name) continue;

        processedCount++;

        // Firestoreで既存ドキュメントを検索
        let existingDoc: DocumentReference | null = null;

        // 法人番号で検索
        if (mappedData.corporateNumber) {
          const snap = await db.collection(COLLECTION_NAME)
            .where("corporateNumber", "==", mappedData.corporateNumber)
            .limit(1)
            .get();
          
          if (!snap.empty) {
            existingDoc = snap.docs[0].ref;
          }
        }

        // 企業名で検索（法人番号で見つからない場合）
        if (!existingDoc) {
          const snap = await db.collection(COLLECTION_NAME)
            .where("name", "==", name)
            .limit(1)
            .get();
          
          if (!snap.empty) {
            existingDoc = snap.docs[0].ref;
          }
        }

        if (existingDoc) {
          // 既存ドキュメントを更新
          const currentData = (await existingDoc.get()).data() || {};
          const updateData: Record<string, any> = {};

          for (const [field, value] of Object.entries(mappedData)) {
            // nameは常に上書き、その他はnullの場合のみ補完
            if (field === "name") {
              if (currentData[field] !== value) {
                updateData[field] = value;
              }
            } else if (field === "banks" && Array.isArray(value)) {
              // banksは配列なので、既存の値とマージ
              const existingBanks = Array.isArray(currentData[field]) ? currentData[field] : [];
              const newBanks = [...new Set([...existingBanks, ...value])];
              if (JSON.stringify([...existingBanks].sort()) !== JSON.stringify([...newBanks].sort())) {
                updateData[field] = newBanks;
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
            
            if (urlRemoved) {
              console.log(`📝 更新（URL削除）: ${name}`);
            } else {
              console.log(`📝 更新: ${name}`);
            }
          }
        } else {
          // 新規作成
          const docId = mappedData.corporateNumber || 
                        `${Date.now()}${String(processedCount).padStart(6, "0")}`;
          
          if (!DRY_RUN) {
            await db.collection(COLLECTION_NAME).doc(docId).set({
              ...mappedData,
              csvType: "type_g",
              createdAt: admin.firestore.FieldValue.serverTimestamp(),
              updatedAt: admin.firestore.FieldValue.serverTimestamp(),
            });
          }
          
          if (urlRemoved) {
            console.log(`🆕 新規作成（URL削除）: ${name}`);
          } else {
            console.log(`🆕 新規作成: ${name}`);
          }
        }
      }

    } catch (err: any) {
      console.warn(`  ⚠️ ${path.basename(file)}: CSVパースエラー - ${err.message}`);
    }
  }

  console.log(`\n✅ 処理完了`);
  console.log(`  - 処理レコード数: ${processedCount} 件`);
  console.log(`  - 更新: ${updatedCount} 件`);
  console.log(`  - valuesearch.nikkei.comのURL削除: ${urlsRemovedCount} 件`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に保存するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

