/* eslint-disable no-console */

/**
 * scripts/fill_missing_fields.ts
 * 
 * 目的: companies_newコレクション内のドキュメントの不足フィールドを、
 *       指定されたサービスから取得して補完する
 * 
 * 処理内容:
 * 1. 各ドキュメントの不足フィールドを分析
 * 2. 不足フィールドがある場合、指定サービスから情報を取得
 * 3. 取得した情報で不足フィールドを補完
 * 4. Firestoreに保存
 */

import admin from "firebase-admin";
import fetch from "node-fetch";
import * as cheerio from "cheerio";

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

// 分析対象の全フィールドリスト
const ALL_FIELDS = [
  // 基本情報
  "name", "nameEn", "kana", "corporateNumber", "corporationType", "nikkeiCode",
  "badges", "tags", "createdAt", "updatedAt", "updateDate", "updateCount", "changeCount", "qualificationGrade",
  // 所在地情報
  "prefecture", "address", "headquartersAddress", "postalCode", "location", "departmentLocation",
  // 連絡先情報
  "phoneNumber", "contactPhoneNumber", "fax", "email", "companyUrl", "contactFormUrl",
  // 代表者情報
  "representativeName", "representativeKana", "representativeTitle", "representativeBirthDate",
  "representativePhone", "representativePostalCode", "representativeHomeAddress",
  "representativeRegisteredAddress", "representativeAlmaMater", "executives",
  // 業種情報
  "industry", "industryLarge", "industryMiddle", "industrySmall", "industryDetail",
  "industries", "industryCategories", "businessDescriptions", "businessItems",
  "businessSummary", "specialties", "demandProducts", "specialNote",
  // 財務情報
  "capitalStock", "revenue", "latestRevenue", "latestProfit", "revenueFromStatements",
  "operatingIncome", "totalAssets", "totalLiabilities", "netAssets", "issuedShares",
  "financials", "listing", "marketSegment", "latestFiscalYearMonth", "fiscalMonth",
  "fiscalMonth1", "fiscalMonth2", "fiscalMonth3", "fiscalMonth4", "fiscalMonth5",
  "revenue1", "revenue2", "revenue3", "revenue4", "revenue5",
  "profit1", "profit2", "profit3", "profit4", "profit5",
  // 企業規模・組織
  "employeeCount", "employeeNumber", "factoryCount", "officeCount", "storeCount",
  "averageAge", "averageYearsOfService", "averageOvertimeHours", "averagePaidLeave", "femaleExecutiveRatio",
  // 設立・沿革
  "established", "dateOfEstablishment", "founding", "foundingYear", "acquisition",
  // 取引先・関係会社
  "clients", "suppliers", "subsidiaries", "affiliations", "shareholders", "banks", "bankCorporateNumber",
  // 企業説明
  "overview", "companyDescription", "businessDescriptions", "salesNotes",
  // SNS・外部リンク
  "urls", "profileUrl", "externalDetailUrl", "facebook", "linkedin", "wantedly", "youtrust", "metaKeywords",
];

// 重要フィールド（優先的に取得すべき）
const IMPORTANT_FIELDS = [
  "phoneNumber", "email", "companyUrl", "address", "prefecture",
  "representativeName", "industry", "capitalStock", "revenue", "employeeCount",
  "established", "listing"
];

/**
 * フィールドが空かどうかを判定
 */
function isEmpty(value: any): boolean {
  if (value === null || value === undefined) return true;
  if (typeof value === "string" && value.trim() === "") return true;
  if (Array.isArray(value) && value.length === 0) return true;
  if (typeof value === "object" && Object.keys(value).length === 0) return true;
  return false;
}

/**
 * 数値を抽出
 */
function extractNumber(text: string, pattern: RegExp): number | null {
  const match = text.match(pattern);
  if (!match) return null;
  const numStr = match[1]?.replace(/,/g, "");
  if (!numStr) return null;
  const num = parseInt(numStr, 10);
  return isNaN(num) ? null : num;
}

/**
 * 金額を千円単位に正規化
 */
function normalizeToThousandYen(value: number, context: string): number {
  if (context.includes("億")) {
    return value * 100000;
  } else if (context.includes("千万")) {
    return value * 10000;
  } else if (context.includes("百万")) {
    return value * 1000;
  } else if (context.includes("万円")) {
    return value * 10;
  } else if (context.includes("千円")) {
    return value;
  } else if (context.includes("円") && !context.includes("千") && !context.includes("万") && !context.includes("億")) {
    return Math.floor(value / 1000);
  }
  return value;
}

/**
 * 指定サービスから企業情報を取得
 */
async function fetchCompanyInfoFromServices(
  companyName: string,
  corporateNumber: string | null,
  missingFields: string[]
): Promise<Partial<any>> {
  const info: Partial<any> = {};
  const urls: string[] = [];

  // 企業INDEXナビ
  urls.push(`https://cnavi-app.g-search.or.jp/search?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    urls.push(`https://cnavi-app.g-search.or.jp/search?q=${encodeURIComponent(corporateNumber)}`);
  }

  // バフェットコード
  urls.push(`https://www.buffett-code.com/global_screening?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    urls.push(`https://www.buffett-code.com/global_screening?q=${encodeURIComponent(corporateNumber)}`);
  }

  // マイナビ転職
  urls.push(`https://tenshoku.mynavi.jp/company/search?q=${encodeURIComponent(companyName)}`);

  // マイナビ2026
  urls.push(`https://job.mynavi.jp/26/pc/search/corp.html?tab=corp&q=${encodeURIComponent(companyName)}`);

  // 全国法人リスト
  urls.push(`https://houjin.jp/search?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    urls.push(`https://houjin.jp/search?q=${encodeURIComponent(corporateNumber)}`);
  }

  // 官報決算データベース
  urls.push(`https://catr.jp/s/?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    urls.push(`https://catr.jp/s/?q=${encodeURIComponent(corporateNumber)}`);
  }

  // Alarmbox
  urls.push(`https://alarmbox.jp/companyinfo/?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    urls.push(`https://alarmbox.jp/companyinfo/?q=${encodeURIComponent(corporateNumber)}`);
  }

  // 各URLから情報を取得（最大10件まで）
  const maxUrls = Math.min(10, urls.length);
  for (let i = 0; i < maxUrls; i++) {
    try {
      const url = urls[i];
      const response = await fetch(url, {
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
        timeout: 10000,
      });

      if (!response.ok) continue;

      const html = await response.text();
      const $ = cheerio.load(html);
      const text = $.text();

      // サイト別の情報抽出
      const urlLower = url.toLowerCase();
      
      if (urlLower.includes("mynavi.jp") || urlLower.includes("job.mynavi.jp")) {
        // マイナビ転職/マイナビ2026
        $('.company-info, .company-detail, .company-profile, .company-data').each((_, el) => {
          const infoText = $(el).text();
          if (missingFields.includes("industry")) {
            const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
            if (industry && !info.industry) info.industry = industry[1].trim();
          }
          if (missingFields.includes("employeeCount")) {
            const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
            if (employees) info.employeeCount = employees;
          }
          if (missingFields.includes("capitalStock")) {
            const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
            if (capital) info.capitalStock = normalizeToThousandYen(capital, infoText);
          }
          if (missingFields.includes("revenue")) {
            const revenue = extractNumber(infoText, /売上高[：:]\s*([\d,]+)/i);
            if (revenue) info.revenue = normalizeToThousandYen(revenue, infoText);
          }
        });
      } else if (urlLower.includes("houjin.jp")) {
        // 全国法人リスト
        $('.company-info, .company-detail, .company-data, table').each((_, el) => {
          const infoText = $(el).text();
          if (missingFields.includes("address")) {
            const address = infoText.match(/所在地[：:]\s*([^\n]+)/i);
            if (address && !info.address) info.address = address[1].trim();
          }
          if (missingFields.includes("phoneNumber")) {
            const phone = infoText.match(/電話番号[：:]\s*([0-9-()]+)/i);
            if (phone && !info.phoneNumber) info.phoneNumber = phone[1].trim();
          }
          if (missingFields.includes("capitalStock")) {
            const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
            if (capital) info.capitalStock = normalizeToThousandYen(capital, infoText);
          }
          if (missingFields.includes("established") || missingFields.includes("dateOfEstablishment")) {
            const established = infoText.match(/設立[：:]\s*(\d{4})年(\d{1,2})月(\d{1,2})日?/i);
            if (established && !info.establishedDate) {
              info.establishedDate = `${established[1]}-${established[2].padStart(2, "0")}-${(established[3] || "01").padStart(2, "0")}`;
              info.established = `${established[1]}年${established[2]}月${established[3] || "1"}日`;
            }
          }
        });
      } else if (urlLower.includes("alarmbox.jp")) {
        // Alarmbox
        $('.company-info, .company-detail, .company-data, .company-profile').each((_, el) => {
          const infoText = $(el).text();
          if (missingFields.includes("industry")) {
            const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
            if (industry && !info.industry) info.industry = industry[1].trim();
          }
          if (missingFields.includes("employeeCount")) {
            const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
            if (employees) info.employeeCount = employees;
          }
          if (missingFields.includes("capitalStock")) {
            const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
            if (capital) info.capitalStock = normalizeToThousandYen(capital, infoText);
          }
          if (missingFields.includes("revenue")) {
            const revenue = extractNumber(infoText, /売上高[：:]\s*([\d,]+)/i);
            if (revenue) info.revenue = normalizeToThousandYen(revenue, infoText);
          }
        });
      } else if (urlLower.includes("g-search.or.jp") || urlLower.includes("cnavi-app")) {
        // 企業INDEXナビ
        $('.company-info, .company-detail, .company-data, table').each((_, el) => {
          const infoText = $(el).text();
          if (missingFields.includes("industry")) {
            const industry = infoText.match(/業種[：:]\s*([^\n]+)/i);
            if (industry && !info.industry) info.industry = industry[1].trim();
          }
          if (missingFields.includes("employeeCount")) {
            const employees = extractNumber(infoText, /従業員数[：:]\s*(\d+)/i);
            if (employees) info.employeeCount = employees;
          }
          if (missingFields.includes("capitalStock")) {
            const capital = extractNumber(infoText, /資本金[：:]\s*([\d,]+)/i);
            if (capital) info.capitalStock = normalizeToThousandYen(capital, infoText);
          }
          if (missingFields.includes("revenue")) {
            const revenue = extractNumber(infoText, /売上高[：:]\s*([\d,]+)/i);
            if (revenue) info.revenue = normalizeToThousandYen(revenue, infoText);
          }
          if (missingFields.includes("totalAssets")) {
            const totalAssets = extractNumber(infoText, /総資産[：:]\s*([\d,]+)/i);
            if (totalAssets) info.totalAssets = normalizeToThousandYen(totalAssets, infoText);
          }
          if (missingFields.includes("netAssets")) {
            const netAssets = extractNumber(infoText, /純資産[：:]\s*([\d,]+)/i);
            if (netAssets) info.netAssets = normalizeToThousandYen(netAssets, infoText);
          }
        });
      } else if (urlLower.includes("buffett-code.com")) {
        // バフェットコード
        $('.financial-data, .company-data, table, .data-table').each((_, el) => {
          const tableText = $(el).text();
          if (missingFields.includes("revenue")) {
            const revenue = extractNumber(tableText, /売上高[：:]\s*([\d,]+)/i);
            if (revenue) info.revenue = normalizeToThousandYen(revenue, tableText);
          }
          if (missingFields.includes("latestProfit")) {
            const profit = extractNumber(tableText, /純利益[：:]\s*([\d,]+)/i);
            if (profit) info.latestProfit = normalizeToThousandYen(profit, tableText);
          }
          if (missingFields.includes("operatingIncome")) {
            const operatingIncome = extractNumber(tableText, /営業利益[：:]\s*([\d,]+)/i);
            if (operatingIncome) info.operatingIncome = normalizeToThousandYen(operatingIncome, tableText);
          }
          if (missingFields.includes("totalAssets")) {
            const totalAssets = extractNumber(tableText, /総資産[：:]\s*([\d,]+)/i);
            if (totalAssets) info.totalAssets = normalizeToThousandYen(totalAssets, tableText);
          }
          if (missingFields.includes("netAssets")) {
            const netAssets = extractNumber(tableText, /純資産[：:]\s*([\d,]+)/i);
            if (netAssets) info.netAssets = normalizeToThousandYen(netAssets, tableText);
          }
          if (missingFields.includes("capitalStock")) {
            const capital = extractNumber(tableText, /資本金[：:]\s*([\d,]+)/i);
            if (capital) info.capitalStock = normalizeToThousandYen(capital, tableText);
          }
        });
      } else if (urlLower.includes("catr.jp")) {
        // 官報決算データベース
        $('.financial-data, .kessan-data, table').each((_, el) => {
          const tableText = $(el).text();
          if (missingFields.includes("totalAssets")) {
            const totalAssets = extractNumber(tableText, /総資産[：:]\s*([\d,]+)/i);
            if (totalAssets) info.totalAssets = normalizeToThousandYen(totalAssets, tableText);
          }
          if (missingFields.includes("totalLiabilities")) {
            const totalLiabilities = extractNumber(tableText, /総負債[：:]\s*([\d,]+)/i);
            if (totalLiabilities) info.totalLiabilities = normalizeToThousandYen(totalLiabilities, tableText);
          }
          if (missingFields.includes("netAssets")) {
            const netAssets = extractNumber(tableText, /純資産[：:]\s*([\d,]+)/i);
            if (netAssets) info.netAssets = normalizeToThousandYen(netAssets, tableText);
          }
          if (missingFields.includes("capitalStock")) {
            const capital = extractNumber(tableText, /資本金[：:]\s*([\d,]+)/i);
            if (capital) info.capitalStock = normalizeToThousandYen(capital, tableText);
          }
          if (missingFields.includes("fiscalMonth")) {
            const fiscalMonth = tableText.match(/決算期[：:]\s*(\d{1,2})月/i);
            if (fiscalMonth) {
              info.fiscalMonth = `${fiscalMonth[1]}月`;
            }
          }
        });
      }

      // レート制限
      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (error) {
      // エラーは無視して続行
      console.warn(`[fetchCompanyInfoFromServices] ${urls[i]} の取得エラー:`, (error as any)?.message);
    }
  }

  return info;
}

/**
 * 不足フィールドを補完
 */
async function fillMissingFields() {
  let totalProcessed = 0;
  let totalFilled = 0;
  let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
  const BATCH_SIZE = 100; // バッチサイズを小さくして、レート制限を考慮

  console.log("不足フィールドの補完を開始...");

  while (true) {
    let query = db.collection("companies_new").limit(BATCH_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc);
    }

    const snapshot = await query.get();
    if (snapshot.empty) {
      break;
    }

    console.log(`バッチ処理中: ${totalProcessed + 1} ～ ${totalProcessed + snapshot.size} 件`);

    for (const companyDoc of snapshot.docs) {
      totalProcessed++;
      const companyId = companyDoc.id;
      const companyData = companyDoc.data();
      const companyName = companyData.name || "";
      const corporateNumber = companyData.corporateNumber || null;

      if (!companyName) {
        continue;
      }

      // 不足フィールドを分析
      const missingFields: string[] = [];
      const importantMissingFields: string[] = [];

      ALL_FIELDS.forEach(field => {
        const value = companyData[field];
        if (isEmpty(value)) {
          missingFields.push(field);
          if (IMPORTANT_FIELDS.includes(field)) {
            importantMissingFields.push(field);
          }
        }
      });

      // 重要フィールドが不足している場合のみ、サービスから取得
      if (importantMissingFields.length === 0) {
        continue;
      }

      console.log(`[${companyId}] 不足フィールド: ${importantMissingFields.length} 件 (${importantMissingFields.slice(0, 5).join(", ")}...)`);

      try {
        // サービスから情報を取得
        const fetchedInfo = await fetchCompanyInfoFromServices(
          companyName,
          corporateNumber,
          importantMissingFields
        );

        // 取得した情報で不足フィールドを補完
        const updateData: any = {};
        let hasUpdate = false;

        importantMissingFields.forEach(field => {
          if (fetchedInfo[field] !== undefined && fetchedInfo[field] !== null && fetchedInfo[field] !== "") {
            updateData[field] = fetchedInfo[field];
            hasUpdate = true;
          }
        });

        if (hasUpdate) {
          // Firestoreに保存
          await companyDoc.ref.update(updateData);
          totalFilled++;
          console.log(`[${companyId}] ✅ ${Object.keys(updateData).length} 件のフィールドを補完しました`);
        } else {
          console.log(`[${companyId}] ⚠️  情報を取得できませんでした`);
        }

        // レート制限
        await new Promise(resolve => setTimeout(resolve, 1000));
      } catch (error) {
        console.error(`[${companyId}] ❌ エラー:`, (error as any)?.message);
      }
    }

    lastDoc = snapshot.docs[snapshot.docs.length - 1];
  }

  console.log(`\n✅ 処理完了`);
  console.log(`総処理数: ${totalProcessed} 件`);
  console.log(`補完成功: ${totalFilled} 件`);
}

fillMissingFields().catch((error) => {
  console.error("エラー:", error);
  process.exit(1);
});

