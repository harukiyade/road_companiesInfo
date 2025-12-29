/* eslint-disable no-console */

/**
 * scripts/fix_invalid_industry_data.ts
 *
 * ✅ 目的
 * - companies_new コレクション内の既存データで、不正なデータを修正
 * - 不正な値を削除またはnullに設定
 * - 業種、電話番号、FAX、メール、URL、生年月日などを検証
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 */

import admin from "firebase-admin";
import * as fs from "fs";

// ------------------------------
// Firebase Admin SDK 初期化
// ------------------------------
if (!admin.apps.length) {
  try {
    const serviceAccountPath = process.env.FIREBASE_SERVICE_ACCOUNT_KEY;

    if (!serviceAccountPath) {
      console.error("❌ エラー: FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません。");
      process.exit(1);
    }

    if (!fs.existsSync(serviceAccountPath)) {
      console.error(`❌ エラー: サービスアカウントキーファイルが存在しません: ${serviceAccountPath}`);
      process.exit(1);
    }

    const serviceAccount = JSON.parse(
      fs.readFileSync(serviceAccountPath, "utf8")
    );

    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: "albert-ma",
    });

    console.log("[Firebase初期化] ✅ 初期化が完了しました");
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", error);
    process.exit(1);
  }
}

const db = admin.firestore();

// ------------------------------
// 検証関数（scrape_extended_fields.tsと同じ）
// ------------------------------

/**
 * 業種として正常な値かどうかを検証
 */
function isValidIndustry(industry: string | null | undefined): boolean {
  if (!industry || typeof industry !== "string") {
    return false;
  }

  const trimmed = industry.trim();
  
  if (trimmed.length === 0 || trimmed.length > 100) {
    return false;
  }

  const jsKeywords = [
    "function", "odtag", "window", "script", "document", "getElementById",
    "addEventListener", "querySelector", "innerHTML", "createElement",
    "setAttribute", "appendChild", "onclick", "onload", "onerror",
    "eval", "setTimeout", "setInterval", "console.log", "var ", "let ", "const ",
    "twq", "fbq", "facebook.com", "twitter.com", "ads-twitter.com",
    "fbevents.js", "uwt.js", "PageView", "noscript"
  ];
  
  const lowerIndustry = trimmed.toLowerCase();
  for (const keyword of jsKeywords) {
    if (lowerIndustry.includes(keyword.toLowerCase())) {
      return false;
    }
  }

  if (/<[^>]+>/.test(trimmed) || /https?:\/\//.test(trimmed)) {
    return false;
  }

  const specialCharCount = (trimmed.match(/[!@#$%^&*()_+\-=\[\]{};':"\\|,.<>\/?]/g) || []).length;
  if (specialCharCount > trimmed.length * 0.3) {
    return false;
  }

  if (/^\d+$/.test(trimmed) || !/[一-龠ひ-ゖァ-ヶa-zA-Z]/.test(trimmed)) {
    return false;
  }

  const invalidPatterns = [
    /で検索/i,
    /すべて選択/i,
    /選択解除/i,
    /主力事業/i,
    /メーカー/i,
    /限定/i,
    /検索/i,
    /選択/i,
    /解除/i,
  ];
  
  for (const pattern of invalidPatterns) {
    if (pattern.test(trimmed) && trimmed.length > 20) {
      return false;
    }
  }

  return true;
}

/**
 * 電話番号が正常な値かどうかを検証
 */
function isValidPhoneNumber(phone: string | null | undefined): boolean {
  if (!phone || typeof phone !== "string") {
    return false;
  }
  const trimmed = phone.trim();
  if (trimmed.length === 0 || !/^[0-9\-()]+$/.test(trimmed)) {
    return false;
  }
  const digitsOnly = trimmed.replace(/[^\d]/g, "");
  if (digitsOnly.length < 10 || digitsOnly.length > 15) {
    return false;
  }
  const invalidPatterns = ["function", "script", "eval", "window", "document"];
  const lowerPhone = trimmed.toLowerCase();
  for (const pattern of invalidPatterns) {
    if (lowerPhone.includes(pattern)) {
      return false;
    }
  }
  return true;
}

/**
 * メールアドレスが正常な値かどうかを検証
 */
function isValidEmail(email: string | null | undefined): boolean {
  if (!email || typeof email !== "string") {
    return false;
  }
  const trimmed = email.trim();
  if (trimmed.length === 0 || trimmed.length > 255) {
    return false;
  }
  const emailRegex = /^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$/;
  if (!emailRegex.test(trimmed)) {
    return false;
  }
  const invalidPatterns = ["function", "script", "eval", "window", "document", "<script"];
  const lowerEmail = trimmed.toLowerCase();
  for (const pattern of invalidPatterns) {
    if (lowerEmail.includes(pattern)) {
      return false;
    }
  }
  return true;
}

/**
 * URLが正常な値かどうかを検証
 */
function isValidUrl(url: string | null | undefined): boolean {
  if (!url || typeof url !== "string") {
    return false;
  }
  const trimmed = url.trim();
  if (trimmed.length === 0) {
    return false;
  }
  try {
    const urlObj = new URL(trimmed);
    if (urlObj.protocol !== "http:" && urlObj.protocol !== "https:") {
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

/**
 * 生年月日が正常な値かどうかを検証
 */
function isValidBirthDate(date: string | null | undefined): boolean {
  if (!date || typeof date !== "string") {
    return false;
  }
  const trimmed = date.trim();
  if (trimmed.length === 0) {
    return false;
  }
  const datePatterns = [
    /^\d{4}[-\/]\d{1,2}[-\/]\d{1,2}$/,
    /^\d{4}年\d{1,2}月\d{1,2}日?$/,
  ];
  let isValidFormat = false;
  for (const pattern of datePatterns) {
    if (pattern.test(trimmed)) {
      isValidFormat = true;
      break;
    }
  }
  if (!isValidFormat) {
    return false;
  }
  try {
    const normalized = trimmed.replace(/年/g, "-").replace(/月/g, "-").replace(/日/g, "");
    const dateObj = new Date(normalized);
    if (isNaN(dateObj.getTime())) {
      return false;
    }
    const year = dateObj.getFullYear();
    if (year < 1900 || year > 2100) {
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

/**
 * メイン処理: 不正なデータを修正
 */
async function fixInvalidIndustryData() {
  try {
    console.log("不正な業種データの修正を開始...");

    const BATCH_SIZE = 500;
    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;
    let totalFixed = 0;
    const fixedCompanies: Array<{ id: string; name: string; fixedFields: string[] }> = [];

    while (true) {
      let query = companiesCollection.limit(BATCH_SIZE);
      if (lastDoc) {
        query = query.startAfter(lastDoc);
      }

      const snapshot = await query.get();
      if (snapshot.empty) {
        break;
      }

      console.log(`\nバッチ取得: ${snapshot.size} 件`);

      let batch = db.batch();
      let batchCount = 0;

      for (const companyDoc of snapshot.docs) {
        const companyId = companyDoc.id;
        const companyData = companyDoc.data();
        const updates: { [key: string]: any } = {};
        const fixedFields: string[] = [];

        // 業種フィールドをチェック
        const industryFields = ["industryLarge", "industryMiddle", "industrySmall", "industryDetail"];
        
        for (const field of industryFields) {
          const value = companyData[field];
          if (value && !isValidIndustry(value)) {
            updates[field] = null; // 不正な値はnullに設定
            fixedFields.push(field);
            console.log(`  [${companyId}] ${companyData.name || ""} - ${field} を修正: "${String(value).substring(0, 50)}..."`);
          }
        }

        // 電話番号をチェック
        const phoneFields = ["phoneNumber", "contactPhoneNumber"];
        for (const field of phoneFields) {
          const value = companyData[field];
          if (value && !isValidPhoneNumber(value)) {
            updates[field] = null;
            fixedFields.push(field);
            console.log(`  [${companyId}] ${companyData.name || ""} - ${field} を修正: "${String(value).substring(0, 50)}..."`);
          }
        }

        // FAXをチェック
        if (companyData.fax && !isValidPhoneNumber(companyData.fax)) {
          updates.fax = null;
          fixedFields.push("fax");
          console.log(`  [${companyId}] ${companyData.name || ""} - fax を修正: "${String(companyData.fax).substring(0, 50)}..."`);
        }

        // メールアドレスをチェック
        if (companyData.email && !isValidEmail(companyData.email)) {
          updates.email = null;
          fixedFields.push("email");
          console.log(`  [${companyId}] ${companyData.name || ""} - email を修正: "${String(companyData.email).substring(0, 50)}..."`);
        }

        // 問い合わせフォームURLをチェック
        if (companyData.contactFormUrl && !isValidUrl(companyData.contactFormUrl)) {
          updates.contactFormUrl = null;
          fixedFields.push("contactFormUrl");
          console.log(`  [${companyId}] ${companyData.name || ""} - contactFormUrl を修正: "${String(companyData.contactFormUrl).substring(0, 50)}..."`);
        }

        // 代表者生年月日をチェック
        if (companyData.representativeBirthDate && !isValidBirthDate(companyData.representativeBirthDate)) {
          updates.representativeBirthDate = null;
          fixedFields.push("representativeBirthDate");
          console.log(`  [${companyId}] ${companyData.name || ""} - representativeBirthDate を修正: "${String(companyData.representativeBirthDate).substring(0, 50)}..."`);
        }

        if (Object.keys(updates).length > 0) {
          batch.update(companyDoc.ref, updates);
          batchCount++;
          totalFixed++;
          fixedCompanies.push({
            id: companyId,
            name: companyData.name || "",
            fixedFields: [...fixedFields],
          });

          if (batchCount >= 500) {
            await batch.commit();
            console.log(`  バッチコミット完了: ${batchCount} 件`);
            batch = db.batch();
            batchCount = 0;
          }
        }

        totalProcessed++;
      }

      if (batchCount > 0) {
        await batch.commit();
        console.log(`  バッチコミット完了: ${batchCount} 件`);
      }

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
      console.log(`処理済み: ${totalProcessed} 件 / 修正: ${totalFixed} 件`);
    }

    console.log(`\n✅ 処理完了`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`修正数: ${totalFixed} 件`);

    if (fixedCompanies.length > 0) {
      console.log(`\n修正された企業一覧（最初の20件）:`);
      fixedCompanies.slice(0, 20).forEach((company) => {
        console.log(`  - ${company.id}: ${company.name} (${company.fixedFields.join(", ")})`);
      });
      if (fixedCompanies.length > 20) {
        console.log(`  ... 他 ${fixedCompanies.length - 20} 件`);
      }
    }

  } catch (error) {
    console.error("エラー:", error);
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
fixInvalidIndustryData()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });

