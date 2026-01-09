/* 
  国税庁法人番号システムのWeb-APIを使用して法人番号を取得し、Firestoreに反映するスクリプト
  
  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    HOUJIN_BANGOU_APPLICATION_ID=your_application_id \
    INPUT_LIST=out/remaining_docids.json \
    DRY_RUN=1 \
    LIMIT=10 \
    npx tsx scripts/search_corporate_number_from_api.ts
  
  アプリケーションIDの取得方法:
    https://www.houjin-bangou.nta.go.jp/webapi/index.html
    1. 「Web-API機能利用規約」を確認し、同意
    2. アプリケーションID発行届出フォームに必要事項を入力し、送信
    3. 発行されたアプリケーションIDを環境変数に設定
*/

import admin from "firebase-admin";
import { Firestore, CollectionReference, DocumentReference, WriteBatch } from "firebase-admin/firestore";
import * as fs from "fs";
import * as path from "path";
import { parseString } from "xml2js";

const COLLECTION_NAME = "companies_new";

// 環境変数
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";
const INPUT_LIST = process.env.INPUT_LIST || "out/remaining_docids.json";
const LIMIT = process.env.LIMIT ? parseInt(process.env.LIMIT) : undefined;
const BATCH_SIZE = 500;
const API_DELAY_MS = 1000; // API呼び出し間隔（1秒）
const APPLICATION_ID = process.env.HOUJIN_BANGOU_APPLICATION_ID; // 国税庁APIのアプリケーションID

// ==============================
// Firebase初期化
// ==============================

let db: Firestore;
let companiesCol: CollectionReference;

function initAdmin() {
  if (admin.apps.length > 0) {
    db = admin.firestore();
    companiesCol = db.collection(COLLECTION_NAME);
    return;
  }

  const serviceAccountPath =
    process.env.GOOGLE_APPLICATION_CREDENTIALS ||
    path.join(__dirname, "../serviceAccountKey.json");

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(`❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`);
    process.exit(1);
  }

  const serviceAccount = JSON.parse(fs.readFileSync(serviceAccountPath, "utf8"));
  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId: serviceAccount.project_id,
  });

  db = admin.firestore();
  companiesCol = db.collection(COLLECTION_NAME);
}

// ==============================
// 正規化関数
// ==============================

function normalizeCompanyName(name: string | null | undefined): string | null {
  if (!name) return null;
  
  let normalized = name.trim();
  
  // 法人格の統一
  normalized = normalized.replace(/[（(]株[）)]/g, "株式会社");
  normalized = normalized.replace(/[（(]有[）)]/g, "有限会社");
  normalized = normalized.replace(/[（(]合[）)]/g, "合同会社");
  
  // 空白除去
  normalized = normalized.replace(/\s+/g, "");
  
  // 全角→半角変換（英数字・記号）
  normalized = normalized.replace(/[Ａ-Ｚａ-ｚ０-９]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) - 0xFEE0);
  });
  
  // 記号除去
  normalized = normalized.replace(/[・、。，．]/g, "");
  
  return normalized;
}

// ==============================
// 国税庁API呼び出し
// ==============================

interface HoujinBangouApiResponse {
  count: number;
  divideNumber: number;
  divideSize: number;
  corporation: Array<{
    sequenceNumber: string;
    corporateNumber: string;
    process: string;
    correct: string;
    updateDate: string;
    changeDate: string;
    name: string;
    nameImageId: string;
    kind: string;
    prefectureName: string;
    cityName: string;
    streetNumber: string;
    addressImageId: string;
    prefectureCode: string;
    cityCode: string;
    postCode: string;
    addressOutside: string;
    addressOutsideImageId: string;
    closeDate: string;
    closeCause: string;
    successorCorporateNumber: string;
    changeCause: string;
    corporateNumberAssignDate: string;
    latest: string;
    enName: string;
    enPrefectureName: string;
    enCityName: string;
    enAddressOutside: string;
    furigana: string;
  }>;
}

async function searchCorporateNumberByName(
  companyName: string,
  address?: string
): Promise<string | null> {
  try {
    // アプリケーションIDの確認
    if (!APPLICATION_ID) {
      console.error(`  ❌ エラー: アプリケーションIDが設定されていません`);
      console.error(`  HOUJIN_BANGOU_APPLICATION_ID環境変数を設定してください`);
      console.error(`  取得方法: https://www.houjin-bangou.nta.go.jp/webapi/index.html`);
      return null;
    }

    // 国税庁法人番号システムのWeb-API
    // 企業名で検索
    const normalizedName = normalizeCompanyName(companyName);
    if (!normalizedName) return null;

    // APIエンドポイント: https://www.houjin-bangou.nta.go.jp/api/1/name
    // パラメータ: id（アプリケーションID、必須）、name（企業名）、type（検索タイプ）、history、change
    const apiUrl = new URL("https://www.houjin-bangou.nta.go.jp/api/1/name");
    
    // idパラメータ: アプリケーションID（必須）
    apiUrl.searchParams.append("id", APPLICATION_ID);
    
    // nameパラメータ: 法人名
    apiUrl.searchParams.append("name", normalizedName);
    
    // typeパラメータ: 01=商号または名称（企業名検索）
    apiUrl.searchParams.append("type", "01");
    
    // history: 0=最新のみ、1=履歴含む
    apiUrl.searchParams.append("history", "0");
    
    // change: 0=変更履歴なし、1=変更履歴あり
    apiUrl.searchParams.append("change", "0");

    console.log(`  📡 API呼び出し: ${apiUrl.toString().replace(APPLICATION_ID, "***")}`);

    const response = await fetch(apiUrl.toString(), {
      method: "GET",
      headers: {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0",
      },
    });

    if (!response.ok) {
      const text = await response.text();
      console.error(`  ⚠️  API呼び出しエラー: ${response.status} ${response.statusText}`);
      console.error(`  レスポンス: ${text.substring(0, 200)}`);
      return null;
    }

    // レスポンスはXMLまたはCSV形式で返される可能性があるため、Content-Typeを確認
    const contentType = response.headers.get("content-type") || "";
    const responseText = await response.text();
    let data: HoujinBangouApiResponse;

    if (contentType.includes("application/json")) {
      data = JSON.parse(responseText);
    } else if (contentType.includes("application/xml") || contentType.includes("text/xml") || responseText.trim().startsWith("<?xml")) {
      // XML形式の場合はパース
      data = await new Promise<HoujinBangouApiResponse>((resolve, reject) => {
        parseString(responseText, { explicitArray: false, mergeAttrs: true }, (err, result) => {
          if (err) {
            reject(err);
            return;
          }
          
          // XML構造をJSON形式に変換
          const root = result["result"] || result["response"] || result;
          const corporations = root["corporation"] || [];
          const corpArray = Array.isArray(corporations) ? corporations : [corporations];
          
          resolve({
            count: parseInt(root["count"] || "0", 10),
            divideNumber: parseInt(root["divideNumber"] || "1", 10),
            divideSize: parseInt(root["divideSize"] || "1", 10),
            corporation: corpArray.map((corp: any) => ({
              sequenceNumber: corp["sequenceNumber"] || "",
              corporateNumber: corp["corporateNumber"] || "",
              process: corp["process"] || "",
              correct: corp["correct"] || "",
              updateDate: corp["updateDate"] || "",
              changeDate: corp["changeDate"] || "",
              name: corp["name"] || "",
              nameImageId: corp["nameImageId"] || "",
              kind: corp["kind"] || "",
              prefectureName: corp["prefectureName"] || "",
              cityName: corp["cityName"] || "",
              streetNumber: corp["streetNumber"] || "",
              addressImageId: corp["addressImageId"] || "",
              prefectureCode: corp["prefectureCode"] || "",
              cityCode: corp["cityCode"] || "",
              postCode: corp["postCode"] || "",
              addressOutside: corp["addressOutside"] || "",
              addressOutsideImageId: corp["addressOutsideImageId"] || "",
              closeDate: corp["closeDate"] || "",
              closeCause: corp["closeCause"] || "",
              successorCorporateNumber: corp["successorCorporateNumber"] || "",
              changeCause: corp["changeCause"] || "",
              corporateNumberAssignDate: corp["corporateNumberAssignDate"] || "",
              latest: corp["latest"] || "",
              enName: corp["enName"] || "",
              enPrefectureName: corp["enPrefectureName"] || "",
              enCityName: corp["enCityName"] || "",
              enAddressOutside: corp["enAddressOutside"] || "",
              furigana: corp["furigana"] || "",
            })),
          });
        });
      });
    } else {
      // その他の形式（CSVなど）
      console.warn(`  ⚠️  予期しないContent-Type: ${contentType}`);
      console.warn(`  レスポンスの最初の200文字: ${responseText.substring(0, 200)}`);
      return null;
    }

    if (data.count === 0 || !data.corporation || data.corporation.length === 0) {
      return null;
    }

    // 複数候補がある場合は、住所で絞り込み
    if (data.corporation.length === 1) {
      return data.corporation[0].corporateNumber;
    }

    // 複数候補がある場合、住所でマッチングを試みる
    if (address) {
      const normalizedAddress = normalizeAddress(address);
      for (const corp of data.corporation) {
        const corpAddress = `${corp.prefectureName}${corp.cityName}${corp.streetNumber}`;
        const normalizedCorpAddress = normalizeAddress(corpAddress);
        if (normalizedCorpAddress && normalizedAddress) {
          // 簡易的なマッチング（都道府県+市区町村まで）
          const prefectureMatch = corp.prefectureName && address.includes(corp.prefectureName);
          const cityMatch = corp.cityName && address.includes(corp.cityName);
          if (prefectureMatch && cityMatch) {
            return corp.corporateNumber;
          }
        }
      }
    }

    // マッチしない場合は最初の候補を返す（要確認）
    console.warn(`  ⚠️  複数候補あり（${data.corporation.length}件）、最初の候補を使用`);
    return data.corporation[0].corporateNumber;
  } catch (error) {
    console.error(`  ❌ API呼び出しエラー:`, error);
    return null;
  }
}

function normalizeAddress(address: string | null | undefined): string | null {
  if (!address) return null;
  
  let normalized = address.trim();
  
  // 都道府県の統一
  normalized = normalized.replace(/^(.+?[都道府県])/, "$1");
  
  // 空白除去
  normalized = normalized.replace(/\s+/g, "");
  
  // 全角→半角変換
  normalized = normalized.replace(/[０-９]/g, (s) => {
    return String.fromCharCode(s.charCodeAt(0) - 0xFEE0);
  });
  
  return normalized;
}

// ==============================
// メイン処理
// ==============================

async function main() {
  console.log("============================================================");
  console.log("法人番号API検索バッチ処理");
  console.log("============================================================");
  console.log(`入力リスト: ${INPUT_LIST}`);
  console.log(`DRY_RUN: ${DRY_RUN}`);
  console.log(`LIMIT: ${LIMIT || "なし"}`);
  console.log(`アプリケーションID: ${APPLICATION_ID ? "設定済み" : "❌ 未設定"}`);
  if (!APPLICATION_ID) {
    console.error(`\n❌ エラー: アプリケーションIDが設定されていません`);
    console.error(`以下の環境変数を設定してください:`);
    console.error(`  export HOUJIN_BANGOU_APPLICATION_ID=your_application_id`);
    console.error(`\nアプリケーションIDの取得方法:`);
    console.error(`  https://www.houjin-bangou.nta.go.jp/webapi/index.html`);
    process.exit(1);
  }
  console.log();

  initAdmin();

  // 入力リストを読み込み
  if (!fs.existsSync(INPUT_LIST)) {
    console.error(`❌ エラー: 入力リストファイルが見つかりません: ${INPUT_LIST}`);
    process.exit(1);
  }

  const inputData = JSON.parse(fs.readFileSync(INPUT_LIST, "utf8"));
  const allDocIds = [
    ...(inputData.multiple_candidates?.docIds || []),
    ...(inputData.no_candidates?.docIds || []),
  ];

  const targetDocIds = LIMIT ? allDocIds.slice(0, LIMIT) : allDocIds;
  console.log(`📋 処理対象: ${targetDocIds.length} 件\n`);

  const results: Array<{
    docId: string;
    name: string;
    address: string;
    corporateNumber: string | null;
    status: "found" | "not_found" | "error";
  }> = [];

  let foundCount = 0;
  let notFoundCount = 0;
  let errorCount = 0;

  // 各ドキュメントを処理
  for (let i = 0; i < targetDocIds.length; i++) {
    const docId = targetDocIds[i];
    
    try {
      const docRef = companiesCol.doc(docId);
      const docSnap = await docRef.get();

      if (!docSnap.exists) {
        console.log(`[${i + 1}/${targetDocIds.length}] ⚠️  ドキュメントが存在しません: ${docId}`);
        errorCount++;
        continue;
      }

      const data = docSnap.data();
      const name = data?.name || "";
      const address = data?.address || "";

      // 既に法人番号がある場合はスキップ
      if (data?.corporateNumber) {
        console.log(`[${i + 1}/${targetDocIds.length}] ⏭️  既に法人番号あり: ${name}`);
        continue;
      }

      console.log(`[${i + 1}/${targetDocIds.length}] 🔍 検索中: ${name}`);

      // API呼び出し
      const corporateNumber = await searchCorporateNumberByName(name, address);

      if (corporateNumber) {
        console.log(`  ✅ 法人番号を取得: ${corporateNumber}`);
        foundCount++;

        results.push({
          docId,
          name,
          address,
          corporateNumber,
          status: "found",
        });

        // Firestoreに更新
        if (!DRY_RUN) {
          await docRef.update({
            corporateNumber,
            corporateNumberSource: "houjin_bangou_api",
            corporateNumberUpdatedAt: admin.firestore.FieldValue.serverTimestamp(),
          });
        }
      } else {
        console.log(`  ❌ 法人番号が見つかりませんでした`);
        notFoundCount++;

        results.push({
          docId,
          name,
          address,
          corporateNumber: null,
          status: "not_found",
        });
      }

      // API呼び出し間隔を空ける
      if (i < targetDocIds.length - 1) {
        await new Promise((resolve) => setTimeout(resolve, API_DELAY_MS));
      }
    } catch (error) {
      console.error(`[${i + 1}/${targetDocIds.length}] ❌ エラー:`, error);
      errorCount++;
    }
  }

  // 結果を出力
  console.log("\n============================================================");
  console.log("📊 処理結果");
  console.log("============================================================");
  console.log(`総処理数: ${targetDocIds.length} 件`);
  console.log(`法人番号取得: ${foundCount} 件`);
  console.log(`見つからず: ${notFoundCount} 件`);
  console.log(`エラー: ${errorCount} 件`);

  // CSV出力
  const csvPath = "out/api_search_results.csv";
  const csvLines = [
    "docId,name,address,corporateNumber,status",
    ...results.map((r) =>
      [
        r.docId,
        `"${r.name.replace(/"/g, '""')}"`,
        `"${r.address.replace(/"/g, '""')}"`,
        r.corporateNumber || "",
        r.status,
      ].join(",")
    ),
  ];
  fs.writeFileSync(csvPath, csvLines.join("\n"), "utf8");
  console.log(`\n📄 結果CSVを出力: ${csvPath}`);

  if (DRY_RUN) {
    console.log("\n⚠️  DRY_RUNモードのため、Firestoreは更新されていません");
  } else {
    console.log(`\n✅ 更新完了: ${foundCount} 件`);
  }
}

main().catch((err) => {
  console.error("❌ エラー:", err);
  process.exit(1);
});
