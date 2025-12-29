/* eslint-disable no-console */

/**
 * scripts/fill_null_fields_from_csv.ts
 * 
 * 目的: null_fields_detailed配下のCSVファイルを読み込み、
 *       各nullフィールドに対して指定サービスから情報を取得して、
 *       CSVファイルに直接値を書き込む
 */

import * as fs from "fs";
import * as path from "path";
import fetch from "node-fetch";
import * as cheerio from "cheerio";
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
 * 指定サービスから特定フィールドの情報を取得
 */
async function fetchFieldValueFromServices(
  companyName: string,
  corporateNumber: string | null,
  fieldName: string
): Promise<string | number | null> {
  const urls: string[] = [];

  // 企業INDEXナビ
  urls.push(`https://cnavi-app.g-search.or.jp/search?q=${encodeURIComponent(companyName)}`);
  if (corporateNumber) {
    urls.push(`https://cnavi-app.g-search.or.jp/search?q=${encodeURIComponent(corporateNumber)}`);
  }

  // バフェットコード
  urls.push(`https://www.buffett-code.com/global_screening?q=${encodeURIComponent(companyName)}`);

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

  // 最大5件まで試行
  const maxUrls = Math.min(5, urls.length);
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
      const urlLower = url.toLowerCase();

      let value: string | number | null = null;

      // フィールド別の抽出ロジック
      switch (fieldName) {
        case "phoneNumber":
        case "contactPhoneNumber": {
          const phone = text.match(/電話番号[：:]\s*([0-9-()]+)/i) || text.match(/(\d{2,4}-\d{2,4}-\d{4})/);
          if (phone) value = phone[1].trim();
          break;
        }
        case "email": {
          const email = text.match(/[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/);
          if (email) value = email[0];
          break;
        }
        case "companyUrl": {
          const urlMatch = text.match(/(https?:\/\/[^\s]+)/);
          if (urlMatch) value = urlMatch[1];
          break;
        }
        case "address":
        case "headquartersAddress": {
          const address = text.match(/所在地[：:]\s*([^\n]+)/i) || text.match(/(〒\d{3}-?\d{4}[\s　]*[^\n]{10,100})/);
          if (address) value = address[1].trim();
          break;
        }
        case "prefecture": {
          const prefecture = text.match(/(東京都|北海道|(?:大阪|京都|兵庫|奈良|和歌山|滋賀|三重)府|(?:青森|岩手|宮城|秋田|山形|福島|茨城|栃木|群馬|埼玉|千葉|神奈川|新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知|三重|滋賀|京都|大阪|兵庫|奈良|和歌山|鳥取|島根|岡山|広島|山口|徳島|香川|愛媛|高知|福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄)県)/);
          if (prefecture) value = prefecture[1];
          break;
        }
        case "postalCode": {
          const postal = text.match(/(〒|郵便番号)[：:\s]*(\d{3}-?\d{4})/i);
          if (postal) value = postal[2].replace(/-/g, "");
          break;
        }
        case "representativeName": {
          const rep = text.match(/代表者[：:]\s*([^\n]+)/i) || text.match(/代表取締役[：:]\s*([^\n]+)/i) || text.match(/社長[：:]\s*([^\n]+)/i);
          if (rep) {
            const name = rep[1].trim().replace(/^(代表取締役|取締役|社長|CEO|代表)[\s　]*/, "").trim();
            if (name && name.length > 1) value = name;
          }
          break;
        }
        case "industry": {
          const industry = text.match(/業種[：:]\s*([^\n]+)/i);
          if (industry) value = industry[1].trim();
          break;
        }
        case "capitalStock": {
          const capital = extractNumber(text, /資本金[：:]\s*([\d,]+)/i);
          if (capital) value = normalizeToThousandYen(capital, text);
          break;
        }
        case "revenue": {
          const revenue = extractNumber(text, /売上高[：:]\s*([\d,]+)/i);
          if (revenue) value = normalizeToThousandYen(revenue, text);
          break;
        }
        case "employeeCount": {
          const employees = extractNumber(text, /従業員数[：:]\s*(\d+)/i);
          if (employees) value = employees;
          break;
        }
        case "established":
        case "dateOfEstablishment": {
          const established = text.match(/設立[：:]\s*(\d{4})年(\d{1,2})月(\d{1,2})日?/i);
          if (established) {
            if (fieldName === "dateOfEstablishment") {
              value = `${established[1]}-${established[2].padStart(2, "0")}-${(established[3] || "01").padStart(2, "0")}`;
            } else {
              value = `${established[1]}年${established[2]}月${established[3] || "1"}日`;
            }
          }
          break;
        }
        case "totalAssets": {
          const totalAssets = extractNumber(text, /総資産[：:]\s*([\d,]+)/i);
          if (totalAssets) value = normalizeToThousandYen(totalAssets, text);
          break;
        }
        case "netAssets": {
          const netAssets = extractNumber(text, /純資産[：:]\s*([\d,]+)/i);
          if (netAssets) value = normalizeToThousandYen(netAssets, text);
          break;
        }
        case "totalLiabilities": {
          const totalLiabilities = extractNumber(text, /総負債[：:]\s*([\d,]+)/i);
          if (totalLiabilities) value = normalizeToThousandYen(totalLiabilities, text);
          break;
        }
        case "fiscalMonth": {
          const fiscalMonth = text.match(/決算期[：:]\s*(\d{1,2})月/i);
          if (fiscalMonth) value = `${fiscalMonth[1]}月`;
          break;
        }
      }

      if (value !== null) {
        return value;
      }

      // レート制限
      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (error) {
      // エラーは無視して続行
      console.warn(`[fetchFieldValueFromServices] ${urls[i]} の取得エラー:`, (error as any)?.message);
    }
  }

  return null;
}

/**
 * CSVファイルを処理
 */
async function processCsvFile(csvPath: string): Promise<number> {
  console.log(`\n📄 処理中: ${path.basename(csvPath)}`);

  // CSVファイルを読み込み
  const content = fs.readFileSync(csvPath, "utf8");
  const lines = content.split("\n");
  const header = lines[0].trim();
  
  // ヘッダーに foundValue 列がなければ追加
  const headers = header.split(",");
  const hasFoundValue = headers.includes("foundValue");
  const newHeader = hasFoundValue ? header : `${header},foundValue`;

  // Firestoreから企業情報を取得（キャッシュ用）
  const companyCache: { [key: string]: { corporateNumber: string | null } } = {};

  let updatedCount = 0;
  const newLines: string[] = [newHeader];

  // ヘッダーを除いて処理
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const parts = line.split(",");
    if (parts.length < 4) continue;

    const companyId = parts[0];
    const companyName = parts[1];
    const nullFieldName = parts[2];

    // 既に値が取得済みの場合はスキップ
    if (hasFoundValue && parts.length > 5 && parts[5] && parts[5] !== "null" && parts[5] !== "") {
      newLines.push(line);
      continue;
    }

    // 企業情報をキャッシュから取得、なければFirestoreから取得
    if (!companyCache[companyId]) {
      try {
        const companyDoc = await db.collection("companies_new").doc(companyId).get();
        if (companyDoc.exists) {
          const data = companyDoc.data();
          companyCache[companyId] = {
            corporateNumber: data?.corporateNumber || null,
          };
        } else {
          companyCache[companyId] = { corporateNumber: null };
        }
      } catch (error) {
        companyCache[companyId] = { corporateNumber: null };
      }
    }

    const corporateNumber = companyCache[companyId]?.corporateNumber || null;

    console.log(`  [${companyId}] ${companyName} - ${nullFieldName} を取得中...`);

    try {
      // サービスから値を取得
      const value = await fetchFieldValueFromServices(companyName, corporateNumber, nullFieldName);

      if (value !== null) {
        // 値をCSVに追加
        const valueStr = typeof value === "string" ? `"${value.replace(/"/g, '""')}"` : String(value);
        const newLine = hasFoundValue 
          ? line.replace(/,"?null"?$/, `,${valueStr}`)
          : `${line},${valueStr}`;
        newLines.push(newLine);
        updatedCount++;
        console.log(`    ✅ 取得: ${value}`);
      } else {
        // 値が見つからなかった場合
        const newLine = hasFoundValue ? line : `${line},`;
        newLines.push(newLine);
        console.log(`    ⚠️  見つかりませんでした`);
      }

      // レート制限
      await new Promise(resolve => setTimeout(resolve, 1000));
    } catch (error) {
      console.error(`    ❌ エラー:`, (error as any)?.message);
      const newLine = hasFoundValue ? line : `${line},`;
      newLines.push(newLine);
    }
  }

  // CSVファイルに書き込み
  fs.writeFileSync(csvPath, newLines.join("\n"), "utf8");
  console.log(`  ✅ 完了: ${updatedCount} 件の値を取得しました`);

  return updatedCount;
}

/**
 * メイン処理
 */
async function main() {
  const csvDir = path.join(process.cwd(), "null_fields_detailed");
  
  if (!fs.existsSync(csvDir)) {
    console.error(`❌ ディレクトリが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  // CSVファイル一覧を取得
  const files = fs.readdirSync(csvDir)
    .filter(file => file.endsWith(".csv"))
    .sort();

  if (files.length === 0) {
    console.error(`❌ CSVファイルが見つかりません: ${csvDir}`);
    process.exit(1);
  }

  console.log(`📁 ${files.length} 個のCSVファイルを処理します`);

  let totalUpdated = 0;
  for (const file of files) {
    const csvPath = path.join(csvDir, file);
    const updated = await processCsvFile(csvPath);
    totalUpdated += updated;
  }

  console.log(`\n✅ 全処理完了`);
  console.log(`総取得数: ${totalUpdated} 件`);
}

main().catch((error) => {
  console.error("エラー:", error);
  process.exit(1);
});
