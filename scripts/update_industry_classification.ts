/* eslint-disable no-console */

/**
 * scripts/update_industry_classification.ts
 *
 * ✅ 目的
 * - companies_new コレクションの既存フィールド（industry、industries、industryCategoriesなど）を分析
 * - 大分類、中分類、小分類、細分類に適切に割り振る
 * - 各ドキュメントを更新
 *
 * ✅ 必要ENV
 * - FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json (必須)
 */

import admin from "firebase-admin";
import * as fs from "fs";
import * as path from "path";

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
// 業種分類マッピング（日本標準産業分類を参考）
// ------------------------------

interface IndustryClassification {
  large: string;    // 大分類
  middle: string;   // 中分類
  small: string;    // 小分類
  detail: string;   // 細分類
}

/**
 * 業種名から分類を推定するマッピング
 */
const industryMapping: { [key: string]: IndustryClassification } = {
  // 建設業
  // 建設業
  "建設": { large: "建設業", middle: "建設業", small: "建設業", detail: "建設業" },
  "建設業": { large: "建設業", middle: "建設業", small: "建設業", detail: "建設業" },
  "建設工事": { large: "建設業", middle: "建設業", small: "建設業", detail: "建設業" },
  "土木": { large: "建設業", middle: "土木工事業", small: "土木工事業", detail: "土木工事業" },
  "土木工事業": { large: "建設業", middle: "土木工事業", small: "土木工事業", detail: "土木工事業" },
  "土木工事": { large: "建設業", middle: "土木工事業", small: "土木工事業", detail: "土木工事業" },
  "建築": { large: "建設業", middle: "建築工事業", small: "建築工事業", detail: "建築工事業" },
  "建築工事業": { large: "建設業", middle: "建築工事業", small: "建築工事業", detail: "建築工事業" },
  "建築工事": { large: "建設業", middle: "建築工事業", small: "建築工事業", detail: "建築工事業" },
  "大工": { large: "建設業", middle: "建築工事業", small: "大工工事業", detail: "大工工事業" },
  "大工工事": { large: "建設業", middle: "建築工事業", small: "大工工事業", detail: "大工工事業" },
  "左官": { large: "建設業", middle: "建築工事業", small: "左官工事業", detail: "左官工事業" },
  "左官工事": { large: "建設業", middle: "建築工事業", small: "左官工事業", detail: "左官工事業" },
  "とび": { large: "建設業", middle: "建築工事業", small: "とび・土工工事業", detail: "とび・土工工事業" },
  "とび工事": { large: "建設業", middle: "建築工事業", small: "とび・土工工事業", detail: "とび・土工工事業" },
  "土工": { large: "建設業", middle: "建築工事業", small: "とび・土工工事業", detail: "とび・土工工事業" },
  "電気工事": { large: "建設業", middle: "設備工事業", small: "電気工事業", detail: "電気工事業" },
  "電気工事業": { large: "建設業", middle: "設備工事業", small: "電気工事業", detail: "電気工事業" },
  "管工事": { large: "建設業", middle: "設備工事業", small: "管工事業", detail: "管工事業" },
  "管工事業": { large: "建設業", middle: "設備工事業", small: "管工事業", detail: "管工事業" },
  "内装": { large: "建設業", middle: "その他の建設業", small: "内装仕上工事業", detail: "内装仕上工事業" },
  "内装工事": { large: "建設業", middle: "その他の建設業", small: "内装仕上工事業", detail: "内装仕上工事業" },
  "造園": { large: "建設業", middle: "その他の建設業", small: "造園工事業", detail: "造園工事業" },
  "造園工事": { large: "建設業", middle: "その他の建設業", small: "造園工事業", detail: "造園工事業" },
  "塗装": { large: "建設業", middle: "その他の建設業", small: "塗装工事業", detail: "塗装工事業" },
  "塗装工事": { large: "建設業", middle: "その他の建設業", small: "塗装工事業", detail: "塗装工事業" },
  "防水": { large: "建設業", middle: "その他の建設業", small: "防水工事業", detail: "防水工事業" },
  "防水工事": { large: "建設業", middle: "その他の建設業", small: "防水工事業", detail: "防水工事業" },
  "屋根": { large: "建設業", middle: "その他の建設業", small: "屋根工事業", detail: "屋根工事業" },
  "屋根工事": { large: "建設業", middle: "その他の建設業", small: "屋根工事業", detail: "屋根工事業" },

  // 製造業
  "製造": { large: "製造業", middle: "製造業", small: "製造業", detail: "製造業" },
  "製造業": { large: "製造業", middle: "製造業", small: "製造業", detail: "製造業" },
  "食品": { large: "製造業", middle: "食料品製造業", small: "食料品製造業", detail: "食料品製造業" },
  "食料品": { large: "製造業", middle: "食料品製造業", small: "食料品製造業", detail: "食料品製造業" },
  "飲料": { large: "製造業", middle: "飲料・たばこ・飼料製造業", small: "飲料製造業", detail: "飲料製造業" },
  "繊維": { large: "製造業", middle: "繊維工業", small: "繊維工業", detail: "繊維工業" },
  "繊維工業": { large: "製造業", middle: "繊維工業", small: "繊維工業", detail: "繊維工業" },
  "木材": { large: "製造業", middle: "木材・木製品製造業", small: "木材・木製品製造業", detail: "木材・木製品製造業" },
  "家具": { large: "製造業", middle: "家具・装備品製造業", small: "家具・装備品製造業", detail: "家具・装備品製造業" },
  "紙": { large: "製造業", middle: "パルプ・紙・紙加工品製造業", small: "パルプ・紙・紙加工品製造業", detail: "パルプ・紙・紙加工品製造業" },
  "印刷": { large: "製造業", middle: "印刷・同関連業", small: "印刷・同関連業", detail: "印刷・同関連業" },
  "化学": { large: "製造業", middle: "化学工業", small: "化学工業", detail: "化学工業" },
  "化学工業": { large: "製造業", middle: "化学工業", small: "化学工業", detail: "化学工業" },
  "医薬品": { large: "製造業", middle: "化学工業", small: "医薬品製造業", detail: "医薬品製造業" },
  "プラスチック": { large: "製造業", middle: "化学工業", small: "プラスチック製品製造業", detail: "プラスチック製品製造業" },
  "ゴム": { large: "製造業", middle: "ゴム製品製造業", small: "ゴム製品製造業", detail: "ゴム製品製造業" },
  "窯業": { large: "製造業", middle: "窯業・土石製品製造業", small: "窯業・土石製品製造業", detail: "窯業・土石製品製造業" },
  "鉄鋼": { large: "製造業", middle: "鉄鋼業", small: "鉄鋼業", detail: "鉄鋼業" },
  "非鉄金属": { large: "製造業", middle: "非鉄金属製造業", small: "非鉄金属製造業", detail: "非鉄金属製造業" },
  "金属製品": { large: "製造業", middle: "金属製品製造業", small: "金属製品製造業", detail: "金属製品製造業" },
  "機械": { large: "製造業", middle: "機械器具製造業", small: "機械器具製造業", detail: "機械器具製造業" },
  "機械器具": { large: "製造業", middle: "機械器具製造業", small: "機械器具製造業", detail: "機械器具製造業" },
  "電気機械": { large: "製造業", middle: "電気機械器具製造業", small: "電気機械器具製造業", detail: "電気機械器具製造業" },
  "情報通信機械": { large: "製造業", middle: "情報通信機械器具製造業", small: "情報通信機械器具製造業", detail: "情報通信機械器具製造業" },
  "輸送用機械": { large: "製造業", middle: "輸送用機械器具製造業", small: "輸送用機械器具製造業", detail: "輸送用機械器具製造業" },
  "自動車": { large: "製造業", middle: "輸送用機械器具製造業", small: "自動車製造業", detail: "自動車製造業" },
  "精密機械": { large: "製造業", middle: "その他の製造業", small: "精密機械器具製造業", detail: "精密機械器具製造業" },

  // 情報通信業
  "情報通信": { large: "情報通信業", middle: "情報通信業", small: "情報通信業", detail: "情報通信業" },
  "情報通信業": { large: "情報通信業", middle: "情報通信業", small: "情報通信業", detail: "情報通信業" },
  "ソフトウェア": { large: "情報通信業", middle: "情報サービス業", small: "ソフトウェア業", detail: "ソフトウェア業" },
  "ソフトウェア業": { large: "情報通信業", middle: "情報サービス業", small: "ソフトウェア業", detail: "ソフトウェア業" },
  "IT": { large: "情報通信業", middle: "情報サービス業", small: "情報サービス業", detail: "情報サービス業" },
  "ITサービス": { large: "情報通信業", middle: "情報サービス業", small: "情報サービス業", detail: "情報サービス業" },
  "システム": { large: "情報通信業", middle: "情報サービス業", small: "情報サービス業", detail: "情報サービス業" },
  "システム開発": { large: "情報通信業", middle: "情報サービス業", small: "ソフトウェア業", detail: "ソフトウェア業" },
  "システムインテグレーション": { large: "情報通信業", middle: "情報サービス業", small: "情報サービス業", detail: "情報サービス業" },
  "SI": { large: "情報通信業", middle: "情報サービス業", small: "情報サービス業", detail: "情報サービス業" },
  "インターネット": { large: "情報通信業", middle: "インターネット附随サービス業", small: "インターネット附随サービス業", detail: "インターネット附随サービス業" },
  "Web": { large: "情報通信業", middle: "インターネット附随サービス業", small: "インターネット附随サービス業", detail: "インターネット附随サービス業" },
  "通信": { large: "情報通信業", middle: "通信業", small: "通信業", detail: "通信業" },
  "通信業": { large: "情報通信業", middle: "通信業", small: "通信業", detail: "通信業" },
  "放送": { large: "情報通信業", middle: "放送業", small: "放送業", detail: "放送業" },
  "放送業": { large: "情報通信業", middle: "放送業", small: "放送業", detail: "放送業" },

  // 卸売業・小売業
  "卸売": { large: "卸売業、小売業", middle: "卸売業", small: "卸売業", detail: "卸売業" },
  "卸売業": { large: "卸売業、小売業", middle: "卸売業", small: "卸売業", detail: "卸売業" },
  "小売": { large: "卸売業、小売業", middle: "小売業", small: "小売業", detail: "小売業" },
  "小売業": { large: "卸売業、小売業", middle: "小売業", small: "小売業", detail: "小売業" },
  "百貨店": { large: "卸売業、小売業", middle: "小売業", small: "各種商品小売業", detail: "百貨店・総合スーパー" },
  "スーパー": { large: "卸売業、小売業", middle: "小売業", small: "各種商品小売業", detail: "百貨店・総合スーパー" },
  "コンビニ": { large: "卸売業、小売業", middle: "小売業", small: "各種商品小売業", detail: "コンビニエンスストア" },

  // 運輸業
  "運輸": { large: "運輸業、郵便業", middle: "運輸業", small: "運輸業", detail: "運輸業" },
  "運輸業": { large: "運輸業、郵便業", middle: "運輸業", small: "運輸業", detail: "運輸業" },
  "運送": { large: "運輸業、郵便業", middle: "運輸業", small: "道路貨物運送業", detail: "道路貨物運送業" },
  "物流": { large: "運輸業、郵便業", middle: "運輸業", small: "運輸に附帯するサービス業", detail: "物流サービス業" },
  "倉庫": { large: "運輸業、郵便業", middle: "運輸業", small: "倉庫業", detail: "倉庫業" },
  "郵便": { large: "運輸業、郵便業", middle: "郵便業", small: "郵便業", detail: "郵便業" },

  // 不動産業
  "不動産": { large: "不動産業、物品賃貸業", middle: "不動産業", small: "不動産業", detail: "不動産業" },
  "不動産業": { large: "不動産業、物品賃貸業", middle: "不動産業", small: "不動産業", detail: "不動産業" },
  "賃貸": { large: "不動産業、物品賃貸業", middle: "物品賃貸業", small: "物品賃貸業", detail: "物品賃貸業" },

  // 金融業
  "金融": { large: "金融業、保険業", middle: "金融業", small: "金融業", detail: "金融業" },
  "金融業": { large: "金融業、保険業", middle: "金融業", small: "金融業", detail: "金融業" },
  "銀行": { large: "金融業、保険業", middle: "金融業", small: "銀行業", detail: "銀行業" },
  "証券": { large: "金融業、保険業", middle: "金融業", small: "証券業、商品先物取引業", detail: "証券業" },
  "保険": { large: "金融業、保険業", middle: "保険業", small: "保険業", detail: "保険業" },

  // サービス業
  "サービス": { large: "サービス業", middle: "サービス業", small: "サービス業", detail: "サービス業" },
  "サービス業": { large: "サービス業", middle: "サービス業", small: "サービス業", detail: "サービス業" },
  "宿泊": { large: "宿泊業、飲食サービス業", middle: "宿泊業", small: "宿泊業", detail: "宿泊業" },
  "宿泊業": { large: "宿泊業、飲食サービス業", middle: "宿泊業", small: "宿泊業", detail: "宿泊業" },
  "ホテル": { large: "宿泊業、飲食サービス業", middle: "宿泊業", small: "宿泊業", detail: "宿泊業" },
  "飲食": { large: "宿泊業、飲食サービス業", middle: "飲食サービス業", small: "飲食サービス業", detail: "飲食サービス業" },
  "飲食サービス": { large: "宿泊業、飲食サービス業", middle: "飲食サービス業", small: "飲食サービス業", detail: "飲食サービス業" },
  "外食": { large: "宿泊業、飲食サービス業", middle: "飲食サービス業", small: "飲食サービス業", detail: "外食業" },
  "外食業": { large: "宿泊業、飲食サービス業", middle: "飲食サービス業", small: "飲食サービス業", detail: "外食業" },
  "レストラン": { large: "宿泊業、飲食サービス業", middle: "飲食サービス業", small: "飲食サービス業", detail: "外食業" },
  "医療": { large: "医療、福祉", middle: "医療業", small: "医療業", detail: "医療業" },
  "医療業": { large: "医療、福祉", middle: "医療業", small: "医療業", detail: "医療業" },
  "病院": { large: "医療、福祉", middle: "医療業", small: "医療業", detail: "医療業" },
  "福祉": { large: "医療、福祉", middle: "社会保険・社会福祉・介護事業", small: "社会保険・社会福祉・介護事業", detail: "社会保険・社会福祉・介護事業" },
  "介護": { large: "医療、福祉", middle: "社会保険・社会福祉・介護事業", small: "社会保険・社会福祉・介護事業", detail: "社会保険・社会福祉・介護事業" },
  "教育": { large: "教育、学習支援業", middle: "教育、学習支援業", small: "教育、学習支援業", detail: "教育、学習支援業" },
  "教育業": { large: "教育、学習支援業", middle: "教育、学習支援業", small: "教育、学習支援業", detail: "教育、学習支援業" },
  "学習支援": { large: "教育、学習支援業", middle: "教育、学習支援業", small: "教育、学習支援業", detail: "教育、学習支援業" },
  "学習支援業": { large: "教育、学習支援業", middle: "教育、学習支援業", small: "教育、学習支援業", detail: "教育、学習支援業" },
  "学術研究": { large: "学術研究、専門・技術サービス業", middle: "学術研究、専門・技術サービス業", small: "学術研究、専門・技術サービス業", detail: "学術研究、専門・技術サービス業" },
  "専門サービス": { large: "学術研究、専門・技術サービス業", middle: "専門サービス業", small: "専門サービス業", detail: "専門サービス業" },
  "専門サービス業": { large: "学術研究、専門・技術サービス業", middle: "専門サービス業", small: "専門サービス業", detail: "専門サービス業" },
  "技術サービス": { large: "学術研究、専門・技術サービス業", middle: "技術サービス業", small: "技術サービス業", detail: "技術サービス業" },
  "技術サービス業": { large: "学術研究、専門・技術サービス業", middle: "技術サービス業", small: "技術サービス業", detail: "技術サービス業" },
  "広告": { large: "学術研究、専門・技術サービス業", middle: "広告業", small: "広告業", detail: "広告業" },
  "広告業": { large: "学術研究、専門・技術サービス業", middle: "広告業", small: "広告業", detail: "広告業" },
  "人材": { large: "サービス業", middle: "職業紹介・人材派遣業", small: "職業紹介・人材派遣業", detail: "職業紹介・人材派遣業" },
  "人材派遣": { large: "サービス業", middle: "職業紹介・人材派遣業", small: "職業紹介・人材派遣業", detail: "人材派遣業" },
  "人材派遣業": { large: "サービス業", middle: "職業紹介・人材派遣業", small: "職業紹介・人材派遣業", detail: "人材派遣業" },
  "職業紹介": { large: "サービス業", middle: "職業紹介・人材派遣業", small: "職業紹介・人材派遣業", detail: "職業紹介業" },
  "職業紹介業": { large: "サービス業", middle: "職業紹介・人材派遣業", small: "職業紹介・人材派遣業", detail: "職業紹介業" },
  "清掃": { large: "サービス業", middle: "その他の事業サービス業", small: "その他の事業サービス業", detail: "清掃業" },
  "清掃業": { large: "サービス業", middle: "その他の事業サービス業", small: "その他の事業サービス業", detail: "清掃業" },
  "警備": { large: "サービス業", middle: "その他の事業サービス業", small: "その他の事業サービス業", detail: "警備業" },
  "警備業": { large: "サービス業", middle: "その他の事業サービス業", small: "その他の事業サービス業", detail: "警備業" },
  "リース": { large: "サービス業", middle: "その他の事業サービス業", small: "その他の事業サービス業", detail: "リース業" },
  "リース業": { large: "サービス業", middle: "その他の事業サービス業", small: "その他の事業サービス業", detail: "リース業" },

  // その他
  "農業": { large: "農業、林業", middle: "農業", small: "農業", detail: "農業" },
  "林業": { large: "農業、林業", middle: "林業", small: "林業", detail: "林業" },
  "漁業": { large: "漁業", middle: "漁業", small: "漁業", detail: "漁業" },
  "鉱業": { large: "鉱業、採石業、砂利採取業", middle: "鉱業、採石業、砂利採取業", small: "鉱業、採石業、砂利採取業", detail: "鉱業、採石業、砂利採取業" },
  "電気": { large: "電気・ガス・熱供給・水道業", middle: "電気業", small: "電気業", detail: "電気業" },
  "ガス": { large: "電気・ガス・熱供給・水道業", middle: "ガス・熱供給・水道業", small: "ガス・熱供給・水道業", detail: "ガス業" },
  "水道": { large: "電気・ガス・熱供給・水道業", middle: "ガス・熱供給・水道業", small: "ガス・熱供給・水道業", detail: "水道業" },
  "公務": { large: "公務", middle: "公務", small: "公務", detail: "公務" },
};

/**
 * 業種名を正規化（不要な文字を削除）
 */
function normalizeIndustryText(text: string): string {
  return text
    .trim()
    .replace(/[（(].*?[）)]/g, "") // 括弧内を削除
    .replace(/[：:].*$/, "") // コロン以降を削除
    .replace(/\s+/g, "") // 空白を削除
    .replace(/業$/, ""); // 末尾の「業」を削除（柔軟なマッチングのため）
}

/**
 * 業種名から分類を推定
 */
function classifyIndustry(industryText: string | null | undefined): IndustryClassification | null {
  if (!industryText || typeof industryText !== "string") {
    return null;
  }

  const normalized = industryText.trim();
  if (normalized.length === 0) {
    return null;
  }

  // 完全一致を優先
  if (industryMapping[normalized]) {
    return industryMapping[normalized];
  }

  // 正規化したテキストで検索
  const normalizedText = normalizeIndustryText(normalized);
  if (normalizedText !== normalized && industryMapping[normalizedText]) {
    return industryMapping[normalizedText];
  }

  // 部分一致で検索（より柔軟なマッチング）
  for (const [key, classification] of Object.entries(industryMapping)) {
    const normalizedKey = normalizeIndustryText(key);
    
    // 正規化したテキスト同士で比較
    if (normalizedText.includes(normalizedKey) || normalizedKey.includes(normalizedText)) {
      return classification;
    }
    
    // 元のテキストでも比較
    if (normalized.includes(key) || key.includes(normalized)) {
      return classification;
    }
  }

  // デフォルト: 業種名をそのまま使用
  return {
    large: normalized,
    middle: normalized,
    small: normalized,
    detail: normalized,
  };
}

/**
 * 業種キーワードのパターンマッチング（より包括的）
 */
interface KeywordPattern {
  keywords: string[];
  classification: IndustryClassification;
  weight: number; // 重み（高いほど優先）
}

const keywordPatterns: KeywordPattern[] = [
  // 建設業（高優先度）
  { keywords: ["建設業", "建設工事", "建設"], classification: { large: "建設業", middle: "建設業", small: "建設業", detail: "建設業" }, weight: 10 },
  { keywords: ["土木工事業", "土木工事", "土木"], classification: { large: "建設業", middle: "土木工事業", small: "土木工事業", detail: "土木工事業" }, weight: 10 },
  { keywords: ["建築工事業", "建築工事", "建築"], classification: { large: "建設業", middle: "建築工事業", small: "建築工事業", detail: "建築工事業" }, weight: 10 },
  { keywords: ["大工工事業", "大工工事", "大工"], classification: { large: "建設業", middle: "建築工事業", small: "大工工事業", detail: "大工工事業" }, weight: 9 },
  { keywords: ["電気工事業", "電気工事"], classification: { large: "建設業", middle: "設備工事業", small: "電気工事業", detail: "電気工事業" }, weight: 10 },
  { keywords: ["管工事業", "管工事"], classification: { large: "建設業", middle: "設備工事業", small: "管工事業", detail: "管工事業" }, weight: 9 },
  { keywords: ["内装工事業", "内装工事", "内装"], classification: { large: "建設業", middle: "その他の建設業", small: "内装仕上工事業", detail: "内装仕上工事業" }, weight: 9 },
  { keywords: ["造園工事業", "造園工事", "造園"], classification: { large: "建設業", middle: "その他の建設業", small: "造園工事業", detail: "造園工事業" }, weight: 9 },
  { keywords: ["塗装工事業", "塗装工事", "塗装"], classification: { large: "建設業", middle: "その他の建設業", small: "塗装工事業", detail: "塗装工事業" }, weight: 9 },
  { keywords: ["防水工事業", "防水工事", "防水"], classification: { large: "建設業", middle: "その他の建設業", small: "防水工事業", detail: "防水工事業" }, weight: 9 },
  { keywords: ["屋根工事業", "屋根工事", "屋根"], classification: { large: "建設業", middle: "その他の建設業", small: "屋根工事業", detail: "屋根工事業" }, weight: 9 },
  
  // 製造業
  { keywords: ["製造業", "製造"], classification: { large: "製造業", middle: "製造業", small: "製造業", detail: "製造業" }, weight: 10 },
  { keywords: ["食料品製造業", "食品製造", "食品"], classification: { large: "製造業", middle: "食料品製造業", small: "食料品製造業", detail: "食料品製造業" }, weight: 10 },
  { keywords: ["飲料製造業", "飲料"], classification: { large: "製造業", middle: "飲料・たばこ・飼料製造業", small: "飲料製造業", detail: "飲料製造業" }, weight: 9 },
  { keywords: ["繊維工業", "繊維"], classification: { large: "製造業", middle: "繊維工業", small: "繊維工業", detail: "繊維工業" }, weight: 9 },
  { keywords: ["印刷業", "印刷"], classification: { large: "製造業", middle: "印刷・同関連業", small: "印刷・同関連業", detail: "印刷・同関連業" }, weight: 9 },
  { keywords: ["化学工業", "化学"], classification: { large: "製造業", middle: "化学工業", small: "化学工業", detail: "化学工業" }, weight: 9 },
  { keywords: ["医薬品製造業", "医薬品"], classification: { large: "製造業", middle: "化学工業", small: "医薬品製造業", detail: "医薬品製造業" }, weight: 10 },
  { keywords: ["自動車製造業", "自動車"], classification: { large: "製造業", middle: "輸送用機械器具製造業", small: "自動車製造業", detail: "自動車製造業" }, weight: 10 },
  
  // 情報通信業
  { keywords: ["情報通信業", "情報通信"], classification: { large: "情報通信業", middle: "情報通信業", small: "情報通信業", detail: "情報通信業" }, weight: 10 },
  { keywords: ["ソフトウェア業", "ソフトウェア"], classification: { large: "情報通信業", middle: "情報サービス業", small: "ソフトウェア業", detail: "ソフトウェア業" }, weight: 10 },
  { keywords: ["ITサービス", "IT", "システム開発", "システム"], classification: { large: "情報通信業", middle: "情報サービス業", small: "情報サービス業", detail: "情報サービス業" }, weight: 9 },
  { keywords: ["インターネット", "Web"], classification: { large: "情報通信業", middle: "インターネット附随サービス業", small: "インターネット附随サービス業", detail: "インターネット附随サービス業" }, weight: 9 },
  { keywords: ["通信業", "通信"], classification: { large: "情報通信業", middle: "通信業", small: "通信業", detail: "通信業" }, weight: 9 },
  { keywords: ["放送業", "放送"], classification: { large: "情報通信業", middle: "放送業", small: "放送業", detail: "放送業" }, weight: 9 },
  
  // 卸売業・小売業
  { keywords: ["卸売業", "卸売"], classification: { large: "卸売業、小売業", middle: "卸売業", small: "卸売業", detail: "卸売業" }, weight: 10 },
  { keywords: ["小売業", "小売"], classification: { large: "卸売業、小売業", middle: "小売業", small: "小売業", detail: "小売業" }, weight: 10 },
  { keywords: ["百貨店", "スーパー", "コンビニ"], classification: { large: "卸売業、小売業", middle: "小売業", small: "各種商品小売業", detail: "百貨店・総合スーパー" }, weight: 9 },
  
  // 運輸業
  { keywords: ["運輸業", "運輸"], classification: { large: "運輸業、郵便業", middle: "運輸業", small: "運輸業", detail: "運輸業" }, weight: 10 },
  { keywords: ["運送業", "運送", "物流"], classification: { large: "運輸業、郵便業", middle: "運輸業", small: "道路貨物運送業", detail: "道路貨物運送業" }, weight: 9 },
  { keywords: ["倉庫業", "倉庫"], classification: { large: "運輸業、郵便業", middle: "運輸業", small: "倉庫業", detail: "倉庫業" }, weight: 9 },
  
  // 不動産業
  { keywords: ["不動産業", "不動産"], classification: { large: "不動産業、物品賃貸業", middle: "不動産業", small: "不動産業", detail: "不動産業" }, weight: 10 },
  { keywords: ["物品賃貸業", "賃貸"], classification: { large: "不動産業、物品賃貸業", middle: "物品賃貸業", small: "物品賃貸業", detail: "物品賃貸業" }, weight: 9 },
  
  // 金融業
  { keywords: ["金融業", "金融"], classification: { large: "金融業、保険業", middle: "金融業", small: "金融業", detail: "金融業" }, weight: 10 },
  { keywords: ["銀行業", "銀行"], classification: { large: "金融業、保険業", middle: "金融業", small: "銀行業", detail: "銀行業" }, weight: 10 },
  { keywords: ["証券業", "証券"], classification: { large: "金融業、保険業", middle: "金融業", small: "証券業、商品先物取引業", detail: "証券業" }, weight: 10 },
  { keywords: ["保険業", "保険"], classification: { large: "金融業、保険業", middle: "保険業", small: "保険業", detail: "保険業" }, weight: 10 },
  
  // サービス業
  { keywords: ["サービス業", "サービス"], classification: { large: "サービス業", middle: "サービス業", small: "サービス業", detail: "サービス業" }, weight: 8 },
  { keywords: ["宿泊業", "宿泊", "ホテル"], classification: { large: "宿泊業、飲食サービス業", middle: "宿泊業", small: "宿泊業", detail: "宿泊業" }, weight: 10 },
  { keywords: ["飲食サービス業", "飲食サービス", "飲食", "外食業", "外食", "レストラン"], classification: { large: "宿泊業、飲食サービス業", middle: "飲食サービス業", small: "飲食サービス業", detail: "外食業" }, weight: 10 },
  { keywords: ["医療業", "医療", "病院"], classification: { large: "医療、福祉", middle: "医療業", small: "医療業", detail: "医療業" }, weight: 10 },
  { keywords: ["福祉", "介護"], classification: { large: "医療、福祉", middle: "社会保険・社会福祉・介護事業", small: "社会保険・社会福祉・介護事業", detail: "社会保険・社会福祉・介護事業" }, weight: 9 },
  { keywords: ["教育", "学習支援"], classification: { large: "教育、学習支援業", middle: "教育、学習支援業", small: "教育、学習支援業", detail: "教育、学習支援業" }, weight: 9 },
  { keywords: ["人材派遣業", "人材派遣", "人材", "職業紹介"], classification: { large: "サービス業", middle: "職業紹介・人材派遣業", small: "職業紹介・人材派遣業", detail: "人材派遣業" }, weight: 9 },
  { keywords: ["清掃業", "清掃"], classification: { large: "サービス業", middle: "その他の事業サービス業", small: "その他の事業サービス業", detail: "清掃業" }, weight: 9 },
  { keywords: ["警備業", "警備"], classification: { large: "サービス業", middle: "その他の事業サービス業", small: "その他の事業サービス業", detail: "警備業" }, weight: 9 },
  { keywords: ["広告業", "広告"], classification: { large: "学術研究、専門・技術サービス業", middle: "広告業", small: "広告業", detail: "広告業" }, weight: 9 },
];

/**
 * テキストから業種キーワードを抽出してスコアリング
 */
function scoreIndustryKeywords(text: string): Map<string, { score: number; classification: IndustryClassification }> {
  const results = new Map<string, { score: number; classification: IndustryClassification }>();
  const normalizedText = text.toLowerCase();
  
  // パターンマッチングでスコアリング
  for (const pattern of keywordPatterns) {
    let matched = false;
    let matchScore = 0;
    
    for (const keyword of pattern.keywords) {
      const normalizedKeyword = keyword.toLowerCase();
      
      // 完全一致または単語境界での一致
      const wordBoundaryRegex = new RegExp(`\\b${normalizedKeyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
      if (wordBoundaryRegex.test(normalizedText)) {
        matched = true;
        matchScore += pattern.weight * 2; // 単語境界での一致は高スコア
      } else if (normalizedText.includes(normalizedKeyword)) {
        matched = true;
        matchScore += pattern.weight; // 部分一致は中スコア
      }
    }
    
    if (matched) {
      const key = pattern.classification.large;
      const existing = results.get(key);
      if (!existing || matchScore > existing.score) {
        results.set(key, { score: matchScore, classification: pattern.classification });
      }
    }
  }
  
  // 既存のマッピングテーブルでも検索
  for (const [key, classification] of Object.entries(industryMapping)) {
    const normalizedKey = key.toLowerCase();
    let score = 0;
    
    const wordBoundaryRegex = new RegExp(`\\b${normalizedKey.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
    if (wordBoundaryRegex.test(normalizedText)) {
      score += 8;
    } else if (normalizedText.includes(normalizedKey)) {
      score += 4;
    }
    
    if (score > 0) {
      const key = classification.large;
      const existing = results.get(key);
      if (!existing || score > existing.score) {
        results.set(key, { score, classification });
      }
    }
  }
  
  return results;
}

/**
 * 複数の業種情報から最適な分類を決定（包括的な分析）
 */
function determineClassification(
  companyData: any
): IndustryClassification | null {
  // 分析対象のフィールドを収集
  const textSources: string[] = [];
  
  // 優先度の高いフィールド
  if (companyData.industry && typeof companyData.industry === "string") {
    textSources.push(companyData.industry);
  }
  
  if (companyData.industries && Array.isArray(companyData.industries)) {
    companyData.industries.forEach((ind: any) => {
      if (typeof ind === "string" && ind.trim().length > 0) {
        textSources.push(ind);
      }
    });
  }
  
  if (companyData.industryCategories && typeof companyData.industryCategories === "string") {
    textSources.push(companyData.industryCategories);
  }
  
  // 補助的なフィールド（事業内容など）
  if (companyData.businessDescriptions && typeof companyData.businessDescriptions === "string") {
    textSources.push(companyData.businessDescriptions);
  }
  
  if (companyData.businessItems && Array.isArray(companyData.businessItems)) {
    companyData.businessItems.forEach((item: any) => {
      if (typeof item === "string" && item.trim().length > 0) {
        textSources.push(item);
      }
    });
  }
  
  if (companyData.businessSummary && typeof companyData.businessSummary === "string") {
    textSources.push(companyData.businessSummary);
  }
  
  if (companyData.specialties && typeof companyData.specialties === "string") {
    textSources.push(companyData.specialties);
  }
  
  if (companyData.companyDescription && typeof companyData.companyDescription === "string") {
    textSources.push(companyData.companyDescription);
  }
  
  if (companyData.overview && typeof companyData.overview === "string") {
    textSources.push(companyData.overview);
  }
  
  // テキストを統合
  const combinedText = textSources.join(" ");
  
  if (combinedText.trim().length === 0) {
    return null;
  }
  
  // まず、直接的な業種フィールドから判定を試みる
  if (companyData.industry) {
    const directResult = classifyIndustry(companyData.industry);
    if (directResult && directResult.large !== companyData.industry) {
      // マッピングに一致した場合
      return directResult;
    }
  }
  
  // キーワードスコアリングで判定
  const scores = scoreIndustryKeywords(combinedText);
  
  if (scores.size > 0) {
    // 最もスコアの高い業種を選択
    let maxScore = 0;
    let bestClassification: IndustryClassification | null = null;
    
    for (const [largeCategory, result] of scores.entries()) {
      if (result.score > maxScore) {
        maxScore = result.score;
        bestClassification = result.classification;
      }
    }
    
    // スコアが一定以上の場合のみ採用
    if (bestClassification && maxScore >= 5) {
      return bestClassification;
    }
  }
  
  // 各テキストソースを個別に分析
  for (const text of textSources) {
    const result = classifyIndustry(text);
    if (result && result.large !== text.trim()) {
      // マッピングに一致した場合
      return result;
    }
  }
  
  // 最後の手段: 最初の業種フィールドをそのまま使用
  if (companyData.industry && typeof companyData.industry === "string") {
    const industryText = companyData.industry.trim();
    if (industryText.length > 0) {
      return {
        large: industryText,
        middle: industryText,
        small: industryText,
        detail: industryText,
      };
    }
  }
  
  return null;
}

/**
 * メイン処理: 業種分類を更新
 */
async function updateIndustryClassification() {
  try {
    console.log("業種分類の更新を開始...");

    const BATCH_SIZE = 500; // 取得バッチサイズ
    const MAX_BATCH_COMMIT_SIZE = 50; // コミットバッチサイズ（Firestoreの制限を考慮して小さく設定）
    
    // 処理済みIDを記録するログファイル
    const progressLogPath = path.join(process.cwd(), "industry_classification_progress.log");
    const processedIds = new Set<string>();
    let isResume = false;
    
    // 既存のログファイルから処理済みIDを読み込む
    if (fs.existsSync(progressLogPath)) {
      try {
        const content = fs.readFileSync(progressLogPath, "utf8");
        const lines = content.split("\n").filter(line => line.trim().length > 0);
        for (const line of lines) {
          const match = line.match(/^PROCESSED:\s*(.+)$/);
          if (match) {
            processedIds.add(match[1].trim());
          }
        }
        if (processedIds.size > 0) {
          isResume = true;
          console.log(`[再開] 既存ログから ${processedIds.size} 件の処理済み企業を検出しました。続きから処理を再開します。`);
        }
      } catch (error) {
        console.warn(`[再開] 既存ログの読み込みエラー: ${error}`);
      }
    }
    
    // ログファイルのストリーム（追記モード）
    const logStream = fs.createWriteStream(progressLogPath, { encoding: "utf8", flags: "a" });
    
    const companiesCollection = db
      .collection("companies_new")
      .orderBy(admin.firestore.FieldPath.documentId());

    let lastDoc: admin.firestore.QueryDocumentSnapshot | null = null;
    let totalProcessed = 0;
    let totalUpdated = 0;
    const updatedCompanies: Array<{ id: string; name: string; classification: IndustryClassification }> = [];

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
      const processedInThisBatch: string[] = []; // このバッチで処理したIDを記録

      for (const companyDoc of snapshot.docs) {
        const companyId = companyDoc.id;
        
        // 既に処理済みの場合はスキップ
        if (processedIds.has(companyId)) {
          totalProcessed++;
          continue;
        }
        
        const companyData = companyDoc.data();
        const updates: { [key: string]: any } = {};

        // 包括的な分析で分類を決定
        const classification = determineClassification(companyData);

        if (classification) {
          // 既存の値と比較して、更新が必要かチェック
          let needsUpdate = false;
          const changes: string[] = [];

          if (companyData.industryLarge !== classification.large) {
            updates.industryLarge = classification.large;
            needsUpdate = true;
            changes.push(`大分類: ${companyData.industryLarge || "(空)"} → ${classification.large}`);
          }
          if (companyData.industryMiddle !== classification.middle) {
            updates.industryMiddle = classification.middle;
            needsUpdate = true;
            changes.push(`中分類: ${companyData.industryMiddle || "(空)"} → ${classification.middle}`);
          }
          if (companyData.industrySmall !== classification.small) {
            updates.industrySmall = classification.small;
            needsUpdate = true;
            changes.push(`小分類: ${companyData.industrySmall || "(空)"} → ${classification.small}`);
          }
          if (companyData.industryDetail !== classification.detail) {
            updates.industryDetail = classification.detail;
            needsUpdate = true;
            changes.push(`細分類: ${companyData.industryDetail || "(空)"} → ${classification.detail}`);
          }

          if (needsUpdate) {
            batch.update(companyDoc.ref, updates);
            batchCount++;
            totalUpdated++;
            updatedCompanies.push({
              id: companyId,
              name: companyData.name || "",
              classification,
            });

            // 詳細ログ（最初の10件のみ）
            if (totalUpdated <= 10) {
              console.log(`  [更新] ${companyId}: ${companyData.name || ""}`);
              console.log(`    元の業種情報: industry=${companyData.industry || "(空)"}, industries=${Array.isArray(companyData.industries) ? companyData.industries.join(", ") : "(空)"}`);
              changes.forEach(change => console.log(`    ${change}`));
            }

            if (batchCount >= MAX_BATCH_COMMIT_SIZE) {
              try {
                await batch.commit();
                console.log(`  バッチコミット完了: ${batchCount} 件`);
                // コミット成功後、処理済みIDをログに記録
                for (const id of processedInThisBatch) {
                  logStream.write(`PROCESSED: ${id}\n`);
                  processedIds.add(id);
                }
                processedInThisBatch.length = 0; // クリア
              } catch (error: any) {
                console.error(`  バッチコミットエラー: ${error.message}`);
                if (error.code === 3 && error.details?.includes("Transaction too big")) {
                  console.error(`  エラー: バッチサイズが大きすぎます。MAX_BATCH_COMMIT_SIZEをさらに減らしてください。`);
                }
                throw error;
              }
              batch = db.batch();
              batchCount = 0;
            }
          }
        }
        
        // 処理済みとして記録（更新が不要だった場合も含む）
        processedInThisBatch.push(companyId);
        totalProcessed++;
      }

      if (batchCount > 0) {
        try {
          await batch.commit();
          console.log(`  バッチコミット完了: ${batchCount} 件`);
          // コミット成功後、処理済みIDをログに記録
          for (const id of processedInThisBatch) {
            logStream.write(`PROCESSED: ${id}\n`);
            processedIds.add(id);
          }
          processedInThisBatch.length = 0; // クリア
        } catch (error: any) {
          console.error(`  バッチコミットエラー: ${error.message}`);
          if (error.code === 3 && error.details?.includes("Transaction too big")) {
            console.log(`  バッチサイズが大きすぎます。バッチサイズを減らして再実行してください。`);
          }
          throw error;
        }
      }
      
      // バッチ処理完了後、残りの処理済みIDも記録（コミットされなかったもの）
      for (const id of processedInThisBatch) {
        logStream.write(`PROCESSED: ${id}\n`);
        processedIds.add(id);
      }
      processedInThisBatch.length = 0; // クリア

      lastDoc = snapshot.docs[snapshot.docs.length - 1];
      console.log(`処理済み: ${totalProcessed} 件 / 更新: ${totalUpdated} 件`);
      
      // 定期的にログをフラッシュ
      logStream.write(`# Progress: ${totalProcessed} processed, ${totalUpdated} updated at ${new Date().toISOString()}\n`);
    }
    
    // ログストリームを閉じる
    logStream.end();

    console.log(`\n✅ 処理完了`);
    console.log(`総処理数: ${totalProcessed} 件`);
    console.log(`更新数: ${totalUpdated} 件`);

    if (updatedCompanies.length > 0) {
      console.log(`\n更新された企業一覧（最初の20件）:`);
      updatedCompanies.slice(0, 20).forEach((company) => {
        console.log(`  - ${company.id}: ${company.name}`);
        console.log(`    大分類: ${company.classification.large}`);
        console.log(`    中分類: ${company.classification.middle}`);
        console.log(`    小分類: ${company.classification.small}`);
        console.log(`    細分類: ${company.classification.detail}`);
      });
      if (updatedCompanies.length > 20) {
        console.log(`  ... 他 ${updatedCompanies.length - 20} 件`);
      }
    }

  } catch (error) {
    console.error("エラー:", error);
    console.error("\n⚠️  エラーが発生しましたが、処理済みの進捗はログファイルに記録されています。");
    console.error("   再実行すると、処理済みの企業はスキップされ、続きから処理が再開されます。");
    process.exit(1);
  }
}

// ------------------------------
// 実行
// ------------------------------
updateIndustryClassification()
  .then(() => {
    console.log("処理完了");
    process.exit(0);
  })
  .catch((error) => {
    console.error("エラー:", error);
    process.exit(1);
  });
