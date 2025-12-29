/* 
  各カテゴリーの代表5社のフィールド値サンプルを生成するスクリプト

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    npx ts-node scripts/generate_field_examples.ts
*/

import admin from "firebase-admin";
import * as fs from "fs";

const COLLECTION_NAME = "companies_new";

// ==============================
// Firebase 初期化
// ==============================
if (admin.apps.length === 0) {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;
  if (!serviceAccountPath) {
    console.error("❌ エラー: GOOGLE_APPLICATION_CREDENTIALS 環境変数が設定されていません");
    process.exit(1);
  }

  try {
    const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
    const projectId =
      serviceAccount.project_id ||
      process.env.GCLOUD_PROJECT ||
      process.env.GCP_PROJECT;

    if (!projectId) {
      console.error("❌ エラー: Project ID を検出できませんでした");
      process.exit(1);
    }

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: projectId,
    });

    console.log(`✅ Firebase 初期化完了 (Project ID: ${projectId})`);
  } catch (err: any) {
    console.error("❌ エラー: サービスアカウントキーファイルの読み込みに失敗しました");
    console.error(`   詳細: ${err.message}`);
    process.exit(1);
  }
}

const db = admin.firestore();
const companiesCol = db.collection(COLLECTION_NAME);

// フィールドカテゴリー定義
const FIELD_CATEGORIES = {
  "基本情報": [
    "name", "nameEn", "kana", "corporateNumber", "corporationType",
    "nikkeiCode", "badges", "tags", "createdAt", "updatedAt",
    "updateDate", "updateCount", "changeCount", "qualificationGrade"
  ],
  "所在地情報": [
    "prefecture", "address", "headquartersAddress", "postalCode",
    "location", "departmentLocation"
  ],
  "連絡先情報": [
    "phoneNumber", "contactPhoneNumber", "fax", "email",
    "companyUrl", "contactFormUrl"
  ],
  "代表者情報": [
    "representativeName", "representativeKana", "representativeTitle",
    "representativeBirthDate", "representativePhone", "representativePostalCode",
    "representativeHomeAddress", "representativeRegisteredAddress",
    "representativeAlmaMater", "executives"
  ],
  "役員情報": [
    "executiveName1", "executivePosition1", "executiveName2", "executivePosition2",
    "executiveName3", "executivePosition3", "executiveName4", "executivePosition4",
    "executiveName5", "executivePosition5", "executiveName6", "executivePosition6",
    "executiveName7", "executivePosition7", "executiveName8", "executivePosition8",
    "executiveName9", "executivePosition9", "executiveName10", "executivePosition10"
  ],
  "業種情報": [
    "industry", "industryLarge", "industryMiddle", "industrySmall",
    "industryDetail", "industries", "industryCategories", "businessDescriptions",
    "businessItems", "businessSummary", "specialties", "demandProducts", "specialNote"
  ],
  "財務情報": [
    "capitalStock", "revenue", "latestRevenue", "latestProfit",
    "revenueFromStatements", "operatingIncome", "totalAssets", "totalLiabilities",
    "netAssets", "issuedShares", "financials", "listing", "marketSegment",
    "latestFiscalYearMonth", "fiscalMonth", "fiscalMonth1", "fiscalMonth2",
    "fiscalMonth3", "fiscalMonth4", "fiscalMonth5", "revenue1", "revenue2",
    "revenue3", "revenue4", "revenue5", "profit1", "profit2", "profit3", "profit4", "profit5"
  ],
  "企業規模・組織": [
    "employeeCount", "employeeNumber", "factoryCount", "officeCount",
    "storeCount", "averageAge", "averageYearsOfService", "averageOvertimeHours",
    "averagePaidLeave", "femaleExecutiveRatio"
  ],
  "設立・沿革": [
    "established", "dateOfEstablishment", "founding", "foundingYear", "acquisition"
  ],
  "取引先・関係会社": [
    "clients", "suppliers", "subsidiaries", "affiliations",
    "shareholders", "banks", "bankCorporateNumber"
  ],
  "部署・拠点情報": [
    "departmentName1", "departmentAddress1", "departmentPhone1",
    "departmentName2", "departmentAddress2", "departmentPhone2",
    "departmentName3", "departmentAddress3", "departmentPhone3",
    "departmentName4", "departmentAddress4", "departmentPhone4",
    "departmentName5", "departmentAddress5", "departmentPhone5",
    "departmentName6", "departmentAddress6", "departmentPhone6",
    "departmentName7", "departmentAddress7", "departmentPhone7"
  ],
  "企業説明": [
    "overview", "companyDescription", "businessDescriptions", "salesNotes"
  ],
  "SNS・外部リンク": [
    "urls", "profileUrl", "externalDetailUrl", "facebook",
    "linkedin", "wantedly", "youtrust", "metaKeywords", "metaDescription"
  ],
  "取引状態・内部管理": [
    "tradingStatus", "adExpiration", "numberOfActivity", "transportation"
  ]
};

// 値のフォーマット関数
function formatValue(value: any, field: string): string {
  if (value === null || value === undefined) {
    return "null";
  }

  if (Array.isArray(value)) {
    if (value.length === 0) return "[]";
    return `[${value.map(v => typeof v === "string" ? `"${v}"` : v).join(", ")}]`;
  }

  if (value instanceof admin.firestore.Timestamp) {
    return `Timestamp(${value.toDate().toISOString()})`;
  }

  if (typeof value === "string") {
    // 長い文字列は切り詰め
    if (value.length > 100) {
      return `"${value.substring(0, 100)}..."`;
    }
    return `"${value}"`;
  }

  if (typeof value === "number") {
    return value.toString();
  }

  if (typeof value === "boolean") {
    return value.toString();
  }

  return String(value);
}

// ==============================
// メイン処理
// ==============================

async function main() {
  try {
    console.log(`\n📋 各カテゴリーの代表5社のフィールド値サンプルを生成中...\n`);

    // 全企業を取得（最大5000件）
    const snapshot = await companiesCol.limit(5000).get();

    if (snapshot.empty) {
      console.log("⚠️  企業が見つかりませんでした");
      return;
    }

    console.log(`📊 取得した企業数: ${snapshot.size}件\n`);

    const docs = snapshot.docs;
    const output: string[] = [];

    output.push("companies_newコレクション フィールド一覧（全159フィールド）");
    output.push("");
    output.push("=".repeat(80));

    // 各カテゴリーごとに処理
    for (const [categoryName, fields] of Object.entries(FIELD_CATEGORIES)) {
      output.push("");
      output.push(`📊 ${categoryName}（${fields.length}フィールド）`);
      output.push("");

      // このカテゴリーのフィールドが埋まっている企業を探す
      const companiesWithData: Array<{ doc: any; data: any; filledCount: number; representativeNameValid?: boolean }> = [];

      for (const doc of docs) {
        const data = doc.data();
        let filledCount = 0;
        let representativeNameValid = false;

        for (const field of fields) {
          const value = data[field];
          if (value !== null && value !== undefined) {
            if (Array.isArray(value)) {
              if (value.length > 0) filledCount++;
            } else if (typeof value === "string") {
              if (value.trim().length > 0) {
                filledCount++;
                // representativeNameの場合は個人名として適切かチェック
                if (field === "representativeName" && isPersonNameOnly(value)) {
                  representativeNameValid = true;
                }
              }
            } else {
              filledCount++;
            }
          }
        }

        if (filledCount > 0) {
          companiesWithData.push({ doc, data, filledCount, representativeNameValid });
        }
      }

      // 埋まり度でソート（多い順）
      // representativeNameが含まれるカテゴリーの場合は、個人名として適切な企業を優先
      if (fields.includes("representativeName")) {
        companiesWithData.sort((a, b) => {
          // まずrepresentativeNameValidでソート（trueを優先）
          if (a.representativeNameValid !== b.representativeNameValid) {
            return a.representativeNameValid ? -1 : 1;
          }
          // 次に埋まり度でソート
          return b.filledCount - a.filledCount;
        });
      } else {
        companiesWithData.sort((a, b) => b.filledCount - a.filledCount);
      }

      // 上位5社を選択
      const selectedCompanies = companiesWithData.slice(0, 5);

      if (selectedCompanies.length === 0) {
        output.push("（データが見つかりませんでした）");
        output.push("");
        continue;
      }

      // 各フィールドについて、5社の値を表示
      for (const field of fields) {
        output.push(`${field} (${getFieldType(field)})`);
        
        // representativeNameの場合は個人名（氏名）のみを表示するようにフィルタリング
        if (field === "representativeName") {
          selectedCompanies.forEach((company, index) => {
            const value = company.data[field];
            let formatted = formatValue(value, field);
            const companyName = company.data.name || company.doc.id;
            
            // 個人名として適切かチェック
            if (value && typeof value === "string") {
              const isPersonName = isPersonNameOnly(value);
              if (!isPersonName) {
                formatted += " ⚠️ (役職名が含まれている可能性)";
              }
            }
            
            output.push(`  社${index + 1}: ${formatted}  // ${companyName}`);
          });
        } else {
          selectedCompanies.forEach((company, index) => {
            const value = company.data[field];
            const formatted = formatValue(value, field);
            const companyName = company.data.name || company.doc.id;
            output.push(`  社${index + 1}: ${formatted}  // ${companyName}`);
          });
        }
        
        output.push("");
      }

      output.push("-".repeat(80));
    }

    // 型別集計を追加
    output.push("");
    output.push("型別集計");
    output.push("");
    output.push("文字列型（string | null）: 118フィールド");
    output.push("数値型（number | null）: 32フィールド");
    output.push("配列型（array）: 9フィールド");
    output.push("badges, tags, industries, businessItems, suppliers, subsidiaries, banks, urls");
    output.push("タイムスタンプ型（timestamp | null）: 2フィールド");
    output.push("createdAt, updatedAt");

    // ファイルに出力
    const outputPath = "COMPANIES_NEW_FIELDS_EXAMPLES.md";
    fs.writeFileSync(outputPath, output.join("\n"), "utf8");

    console.log(`✅ ドキュメントを生成しました: ${outputPath}`);
    console.log(`   各カテゴリーの代表5社のフィールド値サンプルを含みます\n`);

  } catch (err: any) {
    console.error("❌ エラー:", err);
    process.exit(1);
  }
}

// フィールドの型を判定
function getFieldType(field: string): string {
  const numericFields = [
    "capitalStock", "revenue", "latestRevenue", "latestProfit",
    "revenueFromStatements", "operatingIncome", "totalAssets", "totalLiabilities",
    "netAssets", "issuedShares", "employeeCount", "employeeNumber",
    "factoryCount", "officeCount", "storeCount", "numberOfActivity",
    "updateCount", "changeCount", "revenue1", "revenue2", "revenue3",
    "revenue4", "revenue5", "profit1", "profit2", "profit3", "profit4", "profit5"
  ];

  const arrayFields = [
    "badges", "tags", "industries", "businessItems", "suppliers",
    "subsidiaries", "banks", "urls"
  ];

  const timestampFields = ["createdAt", "updatedAt"];

  if (numericFields.includes(field)) return "number";
  if (arrayFields.includes(field)) return "array";
  if (timestampFields.includes(field)) return "timestamp";
  return "string";
}

// 個人名（氏名）のみかどうかを判定
function isPersonNameOnly(value: string): boolean {
  if (!value || typeof value !== "string") return false;
  
  const trimmed = value.trim();
  
  // 役職名が含まれているかチェック
  const titles = [
    "代表取締役", "取締役", "社長", "会長", "専務", "常務", "副社長",
    "代表", "代表者", "CEO", "ceo", "代表取締役社長", "代表取締役会長",
    "代表取締役専務", "代表取締役常務", "代表取締役副社長", "取締役社長",
    "取締役会長", "執行役員", "監査役", "理事", "理事長", "組合長",
    "会長", "副会長", "委員長", "総裁", "頭取", "支店長", "部長",
    "課長", "係長", "主任", "マネージャー", "マネージャ", "Manager"
  ];
  
  for (const title of titles) {
    if (trimmed.includes(title)) {
      return false;
    }
  }
  
  // カッコ内に役職名が含まれているかチェック
  const bracketMatch = trimmed.match(/[（(](.*?)[）)]/);
  if (bracketMatch && bracketMatch[1]) {
    const bracketContent = bracketMatch[1];
    for (const title of titles) {
      if (bracketContent.includes(title)) {
        return false;
      }
    }
  }
  
  // 個人名として適切な形式かチェック（漢字、ひらがな、カタカナ、アルファベットのみ）
  const personNamePattern = /^[一-龠々〆〤あ-んア-ヴーa-zA-Z\s・]+$/;
  if (!personNamePattern.test(trimmed)) {
    return false;
  }
  
  // 数字のみや記号のみの場合は個人名ではない
  if (/^[\d\s\-・、,，.。]+$/.test(trimmed)) {
    return false;
  }
  
  return true;
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});

