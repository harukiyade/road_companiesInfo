/*
  タイプGのドキュメントで、指定URLを含むフィールドを削除し、
  JSON形式のフィールドを解析して各フィールドに振り分けるスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/fix_type_g_url_and_json.ts [--dry-run]
*/

import * as fs from "fs";
import * as path from "path";
import admin from "firebase-admin";
import type { Firestore, DocumentReference } from "firebase-admin/firestore";

const COLLECTION_NAME = "companies_new";
const DRY_RUN = process.argv.includes("--dry-run");
const DELETE_URL = "https://valuesearch.nikkei.com/vs.assets/help/views/customer-support.html";

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

// 「（株）」を「株式会社」に変換（前株・後株を判定）
function normalizeCompanyNameFormat(name: string | null | undefined): string | null {
  if (!name) return null;
  const trimmed = String(name).trim();
  if (!trimmed) return null;

  if (trimmed.includes("（株）")) {
    if (trimmed.startsWith("（株）")) {
      return "株式会社" + trimmed.substring(3);
    }
    if (trimmed.endsWith("（株）")) {
      return trimmed.substring(0, trimmed.length - 3) + "株式会社";
    }
    const index = trimmed.indexOf("（株）");
    if (index > 0) {
      return trimmed.substring(0, index) + "株式会社" + trimmed.substring(index + 3);
    }
  }

  return trimmed;
}

// 値がJSON形式かどうかを判定
function isJsonValue(value: any): boolean {
  if (value === null || value === undefined) return false;
  
  if (typeof value === "string") {
    const trimmed = value.trim();
    if ((trimmed.startsWith("{") && trimmed.endsWith("}")) ||
        (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
      try {
        JSON.parse(trimmed);
        return true;
      } catch {
        return false;
      }
    }
    return false;
  }
  
  if (typeof value === "object") {
    return Array.isArray(value) || (value.constructor === Object);
  }
  
  return false;
}

// JSONからフィールド情報を抽出してマッピング
function extractFieldsFromJson(jsonStr: string | null | undefined): Record<string, any> {
  const result: Record<string, any> = {};
  if (!jsonStr) return result;

  try {
    let parsed: any;
    if (typeof jsonStr === "string") {
      parsed = JSON.parse(jsonStr);
    } else {
      parsed = jsonStr;
    }

    // パターン1: 企業サマリ形式
    let kv = parsed?.企業サマリ?.kv;
    
    // パターン2: addressフィールドに直接kvがある形式
    if (!kv && parsed?.kv) {
      kv = parsed.kv;
    }

    if (!kv) return result;

    // 各フィールドをマッピング
    if (kv.会社名 || kv.商号) {
      result.name = normalizeCompanyNameFormat(kv.会社名 || kv.商号);
    }
    if (kv.英文名) {
      result.nameEn = trim(kv.英文名);
    }
    if (kv.法人番号) {
      const digits = String(kv.法人番号).replace(/\D/g, "");
      if (digits.length === 13) {
        result.corporateNumber = digits;
      }
    }
    if (kv.本社住所 || kv.登記簿住所) {
      result.address = trim(kv.本社住所 || kv.登記簿住所);
    }
    if (kv.業種) {
      result.industry = trim(kv.業種);
    }
    if (kv.資本金) {
      const num = parseNumeric(kv.資本金);
      if (num !== null) result.capitalStock = num;
    }
    if (kv.売上高 || kv["売上高（単独）"]) {
      const num = parseNumeric(kv.売上高 || kv["売上高（単独）"]);
      if (num !== null) result.revenue = num;
    }
    if (kv.従業員数) {
      const num = parseNumeric(kv.従業員数);
      if (num !== null) result.employeeCount = num;
    }
    if (kv.設立年月日 || kv.設立日) {
      result.established = trim(kv.設立年月日 || kv.設立日);
    }
    if (kv.決算月) {
      result.fiscalMonth = trim(kv.決算月);
    }
    if (kv.代表者名 || kv.代表者 || kv.代表取締役) {
      result.representativeName = trim(kv.代表者名 || kv.代表者 || kv.代表取締役);
    }
    if (kv.事業内容) {
      result.businessDescriptions = trim(kv.事業内容);
    }
    if (kv.URL || kv.会社HP) {
      const url = trim(kv.URL || kv.会社HP);
      if (url && url !== "ー" && url !== "-" && !url.includes(DELETE_URL)) {
        result.companyUrl = url;
      }
    }
    if (kv.所属団体) {
      result.affiliations = trim(kv.所属団体);
    }
    if (kv.都道府県) {
      result.prefecture = trim(kv.都道府県);
    }
    if (kv.郵便番号) {
      const postal = String(kv.郵便番号).replace(/\D/g, "");
      if (postal.length === 7) {
        result.postalCode = postal.replace(/(\d{3})(\d{4})/, "$1-$2");
      }
    }
    if (kv.電話番号) {
      result.phoneNumber = trim(kv.電話番号);
    }
    if (kv.発行済株式数) {
      const num = parseNumeric(kv.発行済株式数);
      if (num !== null) result.issuedShares = num;
    }
    if (kv.上場区分 || kv.上場) {
      result.listing = trim(kv.上場区分 || kv.上場);
    }
    if (kv.日経会社コード) {
      result.nikkeiCode = trim(kv.日経会社コード);
    }

    // tablesからファイナンス情報を抽出
    if (parsed?.tables && Array.isArray(parsed.tables)) {
      for (const table of parsed.tables) {
        if (table.title === "ファイナンス情報" && table.rows && Array.isArray(table.rows)) {
          for (const row of table.rows) {
            if (Array.isArray(row) && row.length >= 2) {
              const capitalStr = row[1]; // 資本金の列
              if (capitalStr) {
                const num = parseNumeric(capitalStr);
                if (num !== null && !result.capitalStock) {
                  result.capitalStock = num;
                }
              }
            }
          }
        }
      }
    }
  } catch (e) {
    // JSONパースエラーは無視
  }

  return result;
}

async function main() {
  console.log(DRY_RUN ? "🔍 DRY_RUN モード\n" : "⚠️  本番モード\n");
  console.log("📊 タイプGのURL削除とJSON解析処理を開始します\n");

  const companiesCol = db.collection(COLLECTION_NAME);
  let processedCount = 0;
  let deletedUrlCount = 0;
  let jsonParsedCount = 0;
  let updatedCount = 0;

  // すべてのドキュメントをスキャン
  let lastDoc: FirebaseFirestore.QueryDocumentSnapshot | null = null;
  const PAGE_SIZE = 1000;

  while (true) {
    let query = companiesCol.orderBy(admin.firestore.FieldPath.documentId()).limit(PAGE_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }

    const snap = await query.get();
    if (snap.empty) {
      break;
    }

    for (const doc of snap.docs) {
      processedCount++;
      const data = doc.data();
      const updateData: Record<string, any> = {};
      let hasChanges = false;

      // ① 指定URLを含むフィールドを削除
      for (const [field, value] of Object.entries(data)) {
        if (typeof value === "string" && value.includes(DELETE_URL)) {
          updateData[field] = admin.firestore.FieldValue.delete();
          deletedUrlCount++;
          hasChanges = true;
          if (deletedUrlCount <= 20) {
            console.log(`  🗑️  docId="${doc.id}" フィールド "${field}" を削除（指定URLを含む）`);
          }
        }
      }

      // ② JSON形式のフィールドを解析して各フィールドに振り分け
      for (const [field, value] of Object.entries(data)) {
        if (isJsonValue(value)) {
          const jsonStr = typeof value === "string" ? value : JSON.stringify(value);
          const extractedFields = extractFieldsFromJson(jsonStr);
          
          // 抽出したフィールドをupdateDataにマージ（既存値が空またはJSON形式の場合のみ）
          for (const [extractedField, extractedValue] of Object.entries(extractedFields)) {
            if (extractedValue !== null && extractedValue !== undefined && extractedValue !== "") {
              const currentFieldValue = data[extractedField];
              // 既存値が空、またはJSON形式、または指定URLを含む場合は上書き
              if (!currentFieldValue || 
                  isJsonValue(currentFieldValue) || 
                  (typeof currentFieldValue === "string" && currentFieldValue.includes(DELETE_URL))) {
                updateData[extractedField] = extractedValue;
                hasChanges = true;
                if (jsonParsedCount < 20) {
                  console.log(`  📝 docId="${doc.id}" JSONから抽出: ${extractedField} = ${extractedValue}`);
                }
              }
            }
          }
          
          jsonParsedCount++;
        }
      }

      // 更新実行
      if (hasChanges && Object.keys(updateData).length > 0) {
        updateData.updatedAt = admin.firestore.FieldValue.serverTimestamp();
        
        if (!DRY_RUN) {
          await doc.ref.update(updateData);
          updatedCount++;
          if (updatedCount % 100 === 0) {
            console.log(`  ✅ 更新件数: ${updatedCount} 件`);
          }
        } else {
          if (updatedCount < 20) {
            console.log(`  📝 (DRY_RUN) docId="${doc.id}" 更新予定:`, Object.keys(updateData));
          }
          updatedCount++;
        }
      }

      lastDoc = doc;
    }

    if (processedCount % 10000 === 0) {
      console.log(`  📊 処理中: ${processedCount} 件スキャン済み`);
    }
  }

  console.log(`\n✅ 処理完了`);
  console.log(`  - スキャン件数: ${processedCount} 件`);
  console.log(`  - URL削除件数: ${deletedUrlCount} 件`);
  console.log(`  - JSON解析件数: ${jsonParsedCount} 件`);
  console.log(`  - 更新件数: ${updatedCount} 件`);
  
  if (DRY_RUN) {
    console.log(`\n💡 実際に保存するには、--dry-run フラグを外して実行してください。`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

