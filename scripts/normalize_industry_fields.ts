/* 
  companies_new コレクションの業種フィールドを正規化するスクリプト

  目的：
  - 業種一覧.pdfをマスタとして、industryLarge/Middle/Small/Detailを正規化
  - 表記ゆれを吸収し、親子関係を補完
  - 既存値は退避（industryRaw）し、矛盾がある場合はレビュー用フラグを設定

  使い方:
    GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
    DRY_RUN=1 npx ts-node scripts/normalize_industry_fields.ts   // 更新せず候補だけログ
    npx ts-node scripts/normalize_industry_fields.ts             // 実際に更新

  再開オプション:
    START_FROM_DOC_ID="docId123" npx ts-node scripts/normalize_industry_fields.ts
*/

import admin from "firebase-admin";
import * as fs from "fs";

const PROJECT_ID = "albert-ma";
const COLLECTION_NAME = "companies_new";

// 1回のクエリで読む件数
const PAGE_SIZE = 1000;
// 1バッチで更新する件数（Firestoreの上限500未満にする）
const BATCH_UPDATE_SIZE = 400;

// DRY_RUN=1のときは更新せずログだけ出す
const DRY_RUN = process.env.DRY_RUN === "1" || process.env.DRY_RUN === "true";

// 再開オプション
const START_FROM_DOC_ID = process.env.START_FROM_DOC_ID;
const SKIP_SCANNED = process.env.SKIP_SCANNED ? parseInt(process.env.SKIP_SCANNED, 10) : 0;

// ==============================
// 業種Taxonomy定義（PDFから抽出）
// ==============================

interface IndustryTaxonomy {
  large: string;
  middle: string;
  small: string;
}

interface TaxonomyMaps {
  // 正規化キー -> 正表記のマッピング
  smallMap: Map<string, IndustryTaxonomy>; // 正規化キー -> (large, middle, small)
  middleMap: Map<string, { large: string; middle: string }>; // 正規化キー -> (large, middle)
  largeSet: Set<string>; // 大分類の集合（正表記）
  // 正表記 -> 正規化キーの逆引き（検索用）
  smallToNormalized: Map<string, string>; // 正表記 -> 正規化キー
  middleToNormalized: Map<string, string>;
  largeToNormalized: Map<string, string>;
}

// 文字列を正規化（表記ゆれ吸収用）
function normalizeString(s: string | null | undefined): string {
  if (!s || typeof s !== "string") return "";
  return s
    .trim()
    .replace(/\s+/g, "") // 空白除去
    .replace(/[（）()]/g, "") // 括弧除去
    .replace(/[・、，,]/g, "") // 区切り文字除去
    .toLowerCase();
}

// PDFから業種taxonomyを構築
function buildIndustryTaxonomy(): TaxonomyMaps {
  const taxonomy: IndustryTaxonomy[] = [];
  
  // PDFの内容から業種階層を抽出
  // 大分類、中分類、小分類の階層構造を定義
  
  // 1. 農林・水産
  taxonomy.push({ large: "農林・水産", middle: "農業", small: "動作農業" });
  taxonomy.push({ large: "農林・水産", middle: "農業", small: "ほ場作物農業" });
  taxonomy.push({ large: "農林・水産", middle: "農業", small: "畜糧畑農業" });
  taxonomy.push({ large: "農林・水産", middle: "農業", small: "食品畑苗" });
  taxonomy.push({ large: "農林・水産", middle: "農業", small: "畜産農業" });
  taxonomy.push({ large: "農林・水産", middle: "農業", small: "蔬油農業" });
  taxonomy.push({ large: "農林・水産", middle: "農業", small: "酢種農業" });
  taxonomy.push({ large: "農林・水産", middle: "農業", small: "農業サービス業" });
  taxonomy.push({ large: "農林・水産", middle: "農業", small: "園芸サービス業" });
  taxonomy.push({ large: "農林・水産", middle: "林業", small: "栃林業" });
  taxonomy.push({ large: "農林・水産", middle: "林業", small: "製菓・木炭業" });
  taxonomy.push({ large: "農林・水産", middle: "林業", small: "素材生産業" });
  taxonomy.push({ large: "農林・水産", middle: "林業", small: "林業サービス業" });
  taxonomy.push({ large: "農林・水産", middle: "漁業", small: "細穀業" });
  taxonomy.push({ large: "農林・水産", middle: "漁業", small: "一般海深漁業" });
  taxonomy.push({ large: "農林・水産", middle: "漁業", small: "内水深漁業" });
  taxonomy.push({ large: "農林・水産", middle: "漁業", small: "水産面積業" });
  
  // 2. 鉱業
  taxonomy.push({ large: "鉱業", middle: "金属鯖業", small: "非鯖業" });
  taxonomy.push({ large: "鉱業", middle: "金属鯖業", small: "非鉄金属鯖業" });
  taxonomy.push({ large: "鉱業", middle: "金属鯖業", small: "鉄鉱鯖業" });
  taxonomy.push({ large: "鉱業", middle: "石炭・麦炭鯖業", small: "石炭鯖業" });
  taxonomy.push({ large: "鉱業", middle: "石炭・麦炭鯖業", small: "麥炭鯖業" });
  taxonomy.push({ large: "鉱業", middle: "石炭・麦炭鯖業", small: "石炭淵別業" });
  taxonomy.push({ large: "鉱業", middle: "射油・天然ガス鯖業", small: "琅琊鯖業" });
  taxonomy.push({ large: "鉱業", middle: "射油・天然ガス鯖業", small: "天然ガス鯖業" });
  taxonomy.push({ large: "鉱業", middle: "非金属鯖業", small: "採石業" });
  taxonomy.push({ large: "鉱業", middle: "非金属鯖業", small: "喫業原料用鯖業" });
  taxonomy.push({ large: "鉱業", middle: "非金属鯖業", small: "化学・肥料鯖業" });
  taxonomy.push({ large: "鉱業", middle: "非金属鯖業", small: "粘土鯖業" });
  taxonomy.push({ large: "鉱業", middle: "非金属鯖業", small: "他非金属鯖業" });
  
  // 5. 建設業
  taxonomy.push({ large: "建設業", middle: "総合工事業", small: "一般土木建築業" });
  taxonomy.push({ large: "建設業", middle: "総合工事業", small: "土木工事業" });
  taxonomy.push({ large: "建設業", middle: "総合工事業", small: "園装工事業" });
  taxonomy.push({ large: "建設業", middle: "総合工事業", small: "しゅんぢつ工事" });
  taxonomy.push({ large: "建設業", middle: "総合工事業", small: "建築工事業" });
  taxonomy.push({ large: "建設業", middle: "総合工事業", small: "木造建築工事業" });
  taxonomy.push({ large: "建設業", middle: "職然工事業", small: "大工工事業" });
  taxonomy.push({ large: "建設業", middle: "職然工事業", small: "工び大工工事業" });
  taxonomy.push({ large: "建設業", middle: "職然工事業", small: "鉄骨鉄筋工事業" });
  taxonomy.push({ large: "建設業", middle: "職然工事業", small: "石工・タイル工事" });
  taxonomy.push({ large: "建設業", middle: "職然工事業", small: "工作工事業" });
  taxonomy.push({ large: "建設業", middle: "職然工事業", small: "屋根工事業" });
  taxonomy.push({ large: "建設業", middle: "職然工事業", small: "板金・金物工事" });
  taxonomy.push({ large: "建設業", middle: "職然工事業", small: "球装工事業" });
  taxonomy.push({ large: "建設業", middle: "職然工事業", small: "その他職別工事" });
  taxonomy.push({ large: "建設業", middle: "設備工事業", small: "電気工事業" });
  taxonomy.push({ large: "建設業", middle: "設備工事業", small: "電気通信工事業" });
  taxonomy.push({ large: "建設業", middle: "設備工事業", small: "管工事業" });
  taxonomy.push({ large: "建設業", middle: "設備工事業", small: "さく井工事業" });
  taxonomy.push({ large: "建設業", middle: "設備工事業", small: "他設備工事業" });
  
  // 6. 製造業
  taxonomy.push({ large: "製造業", middle: "食料品製造業", small: "畜産食料品製造" });
  taxonomy.push({ large: "製造業", middle: "食料品製造業", small: "水産食料品製造" });
  taxonomy.push({ large: "製造業", middle: "食料品製造業", small: "保存食料製造業" });
  taxonomy.push({ large: "製造業", middle: "食料品製造業", small: "調味料製造業" });
  taxonomy.push({ large: "製造業", middle: "食料品製造業", small: "糖類製造業" });
  taxonomy.push({ large: "製造業", middle: "食料品製造業", small: "精鋭製粉業" });
  taxonomy.push({ large: "製造業", middle: "食料品製造業", small: "バン菓子製造業" });
  taxonomy.push({ large: "製造業", middle: "食料品製造業", small: "酢植物油脂製造" });
  taxonomy.push({ large: "製造業", middle: "食料品製造業", small: "栽食料品製造業" });
  taxonomy.push({ large: "製造業", middle: "飲料・飼料製造業", small: "固定飲料製造業" });
  taxonomy.push({ large: "製造業", middle: "飲料・飼料製造業", small: "酒類製造業" });
  taxonomy.push({ large: "製造業", middle: "飲料・飼料製造業", small: "茶製造業" });
  taxonomy.push({ large: "製造業", middle: "飲料・飼料製造業", small: "製米業" });
  taxonomy.push({ large: "製造業", middle: "飲料・飼料製造業", small: "鮮料・肥料製造" });
  taxonomy.push({ large: "製造業", middle: "飲料・飼料製造業", small: "たばこ製造業" });
  taxonomy.push({ large: "製造業", middle: "繊維工業", small: "製布業" });
  taxonomy.push({ large: "製造業", middle: "繊維工業", small: "紡織業" });
  taxonomy.push({ large: "製造業", middle: "繊維工業", small: "ねん布製造業" });
  taxonomy.push({ large: "製造業", middle: "繊維工業", small: "織物業" });
  taxonomy.push({ large: "製造業", middle: "繊維工業", small: "ニット製造業" });
  taxonomy.push({ large: "製造業", middle: "繊維工業", small: "薬色管理業" });
  taxonomy.push({ large: "製造業", middle: "繊維工業", small: "麺・鍋製造業" });
  taxonomy.push({ large: "製造業", middle: "繊維工業", small: "繊維雑品製造業" });
  taxonomy.push({ large: "製造業", middle: "繊維工業", small: "その他繊維工業" });
  taxonomy.push({ large: "製造業", middle: "焦繊維製品製造", small: "外衣製造業" });
  taxonomy.push({ large: "製造業", middle: "焦繊維製品製造", small: "下着製造業" });
  taxonomy.push({ large: "製造業", middle: "焦繊維製品製造", small: "帽子製造業" });
  taxonomy.push({ large: "製造業", middle: "焦繊維製品製造", small: "毛皮製衣服製造" });
  taxonomy.push({ large: "製造業", middle: "焦繊維製品製造", small: "その他衣服製造業" });
  taxonomy.push({ large: "製造業", middle: "焦繊維製品製造", small: "粗繊維製品製造" });
  taxonomy.push({ large: "製造業", middle: "木材木製品製造", small: "製材・木製品製造" });
  taxonomy.push({ large: "製造業", middle: "木材木製品製造", small: "造作・合板製造" });
  taxonomy.push({ large: "製造業", middle: "木材木製品製造", small: "木製容器製造業" });
  taxonomy.push({ large: "製造業", middle: "木材木製品製造", small: "木製植物製造業" });
  taxonomy.push({ large: "製造業", middle: "木材木製品製造", small: "その他木製品" });
  taxonomy.push({ large: "製造業", middle: "家具・装備品", small: "家具製造業" });
  taxonomy.push({ large: "製造業", middle: "家具・装備品", small: "宗教用具製造業" });
  taxonomy.push({ large: "製造業", middle: "家具・装備品", small: "建具製造業" });
  taxonomy.push({ large: "製造業", middle: "家具・装備品", small: "その他家具製造" });
  taxonomy.push({ large: "製造業", middle: "バルブ・紙製造", small: "バルブ製造業" });
  taxonomy.push({ large: "製造業", middle: "バルブ・紙製造", small: "紙製造業" });
  taxonomy.push({ large: "製造業", middle: "バルブ・紙製造", small: "加工紙製造業" });
  taxonomy.push({ large: "製造業", middle: "バルブ・紙製造", small: "紙製品製造業" });
  
  // 7. 出版・印刷関連（PDFに記載されている内容を追加）
  taxonomy.push({ large: "製造業", middle: "出版・印刷", small: "印刷業" });
  
  // 8. 卸売業（PDFの内容から追加が必要な場合はここに追加）
  
  // 9. 小売業（PDFの内容から追加が必要な場合はここに追加）
  
  // 10. 金融・保険業
  taxonomy.push({ large: "金融・保険業", middle: "銀行・信託業", small: "中央銀行" });
  taxonomy.push({ large: "金融・保険業", middle: "銀行・信託業", small: "銀行" });
  taxonomy.push({ large: "金融・保険業", middle: "銀行・信託業", small: "在日外国銀行" });
  taxonomy.push({ large: "金融・保険業", middle: "銀行・信託業", small: "政府金融機関" });
  taxonomy.push({ large: "金融・保険業", middle: "銀行・信託業", small: "信用金庫" });
  taxonomy.push({ large: "金融・保険業", middle: "農水産金融業", small: "農林水産系統組合中央機関" });
  taxonomy.push({ large: "金融・保険業", middle: "農水産金融業", small: "農林水産系統地域金融機関" });
  taxonomy.push({ large: "金融・保険業", middle: "農水産金融業", small: "農林水産業向け地域金融機関" });
  taxonomy.push({ large: "金融・保険業", middle: "農水産金融業", small: "農林水産業向け政府金融機関" });
  taxonomy.push({ large: "金融・保険業", middle: "中小企業金融業", small: "中小企業・信託金融機関" });
  taxonomy.push({ large: "金融・保険業", middle: "中小企業金融業", small: "信託金融機関" });
  taxonomy.push({ large: "金融・保険業", middle: "中小企業金融業", small: "住宅専門金融機関" });
  taxonomy.push({ large: "金融・保険業", middle: "中小企業金融業", small: "その他特定目的金融機関" });
  taxonomy.push({ large: "金融・保険業", middle: "金融修理業", small: "補助的金融業" });
  taxonomy.push({ large: "金融・保険業", middle: "投資業", small: "投資業" });
  taxonomy.push({ large: "金融・保険業", middle: "証券・商品取引", small: "証券業" });
  taxonomy.push({ large: "金融・保険業", middle: "証券・商品取引", small: "商品取引業" });
  taxonomy.push({ large: "金融・保険業", middle: "証券・商品取引", small: "取引業" });
  taxonomy.push({ large: "金融・保険業", middle: "保険業", small: "生命保険業" });
  taxonomy.push({ large: "金融・保険業", middle: "保険業", small: "期刊保険業" });
  taxonomy.push({ large: "金融・保険業", middle: "保険業", small: "共済事業" });
  taxonomy.push({ large: "金融・保険業", middle: "保険代理業", small: "保険媒介代理業" });
  taxonomy.push({ large: "金融・保険業", middle: "保険代理業", small: "保険サービス業" });
  
  // 11. 不動産業
  taxonomy.push({ large: "不動産業", middle: "不動産取引業", small: "建売業・土地売買業" });
  taxonomy.push({ large: "不動産業", middle: "不動産取引業", small: "不動産代理業・仲介業" });
  taxonomy.push({ large: "不動産業", middle: "不動産賃貸業", small: "不動産賃貸業" });
  taxonomy.push({ large: "不動産業", middle: "不動産賃貸業", small: "貸家業・貸間業" });
  taxonomy.push({ large: "不動産業", middle: "不動産賃貸業", small: "不動産管理業" });
  
  // 12. サービス業
  taxonomy.push({ large: "サービス業", middle: "物品賃貸業", small: "各種物品賃貸業" });
  taxonomy.push({ large: "サービス業", middle: "物品賃貸業", small: "産業機械器具賃貸業" });
  taxonomy.push({ large: "サービス業", middle: "物品賃貸業", small: "事務用機械器具賃貸業" });
  taxonomy.push({ large: "サービス業", middle: "物品賃貸業", small: "自動車賃貸業" });
  taxonomy.push({ large: "サービス業", middle: "物品賃貸業", small: "スポーツ・娯楽用品賃貸業" });
  taxonomy.push({ large: "サービス業", middle: "物品賃貸業", small: "その他物品賃貸業" });
  taxonomy.push({ large: "サービス業", middle: "ホテル・旅館", small: "旅館" });
  taxonomy.push({ large: "サービス業", middle: "ホテル・旅館", small: "簡易宿泊所" });
  taxonomy.push({ large: "サービス業", middle: "ホテル・旅館", small: "下宿業" });
  taxonomy.push({ large: "サービス業", middle: "ホテル・旅館", small: "その他宿泊所" });
  taxonomy.push({ large: "サービス業", middle: "家事サービス業", small: "家事サービス業" });
  taxonomy.push({ large: "サービス業", middle: "洗濯・理容業", small: "清濯業" });
  taxonomy.push({ large: "サービス業", middle: "洗濯・理容業", small: "洗瓶・染物業" });
  taxonomy.push({ large: "サービス業", middle: "洗濯・理容業", small: "理容業" });
  taxonomy.push({ large: "サービス業", middle: "洗濯・理容業", small: "美容業" });
  taxonomy.push({ large: "サービス業", middle: "洗濯・理容業", small: "公衆浴場業" });
  taxonomy.push({ large: "サービス業", middle: "洗濯・理容業", small: "特殊浴場業" });
  taxonomy.push({ large: "サービス業", middle: "他個人サービス", small: "写真業" });
  taxonomy.push({ large: "サービス業", middle: "他個人サービス", small: "衣服風縫修理業" });
  taxonomy.push({ large: "サービス業", middle: "他個人サービス", small: "物品預り業" });
  taxonomy.push({ large: "サービス業", middle: "他個人サービス", small: "葬儀・火葬業" });
  taxonomy.push({ large: "サービス業", middle: "他個人サービス", small: "その他個人サービス業" });
  taxonomy.push({ large: "サービス業", middle: "映画業", small: "映画製作・配給業" });
  taxonomy.push({ large: "サービス業", middle: "映画業", small: "映画館" });
  taxonomy.push({ large: "サービス業", middle: "映画業", small: "映画サービス業" });
  taxonomy.push({ large: "サービス業", middle: "娯楽業", small: "劇場・興行場" });
  taxonomy.push({ large: "サービス業", middle: "娯楽業", small: "興行団" });
  taxonomy.push({ large: "サービス業", middle: "娯楽業", small: "競輪・競馬等の競走場" });
  taxonomy.push({ large: "サービス業", middle: "娯楽業", small: "競輪・競馬等の競技団" });
  taxonomy.push({ large: "サービス業", middle: "娯楽業", small: "運動競技場" });
  taxonomy.push({ large: "サービス業", middle: "娯楽業", small: "公園・遊園地" });
  taxonomy.push({ large: "サービス業", middle: "娯楽業", small: "競技場" });
  taxonomy.push({ large: "サービス業", middle: "娯楽業", small: "その他娯楽業" });
  taxonomy.push({ large: "サービス業", middle: "放送業", small: "公共放送業" });
  taxonomy.push({ large: "サービス業", middle: "放送業", small: "民間放送業" });
  taxonomy.push({ large: "サービス業", middle: "放送業", small: "有線放送業" });
  taxonomy.push({ large: "サービス業", middle: "駐車場業", small: "駐車場業" });
  taxonomy.push({ large: "サービス業", middle: "自動車整備業", small: "機械修理業" });
  taxonomy.push({ large: "サービス業", middle: "その他修理業", small: "家具修理業" });
  taxonomy.push({ large: "サービス業", middle: "その他修理業", small: "かじ業" });
  taxonomy.push({ large: "サービス業", middle: "その他修理業", small: "表具業" });
  taxonomy.push({ large: "サービス業", middle: "その他修理業", small: "その他修理業" });
  taxonomy.push({ large: "サービス業", middle: "他行業", small: "情報サービス業" });
  taxonomy.push({ large: "サービス業", middle: "他行業", small: "ニュース供給業" });
  taxonomy.push({ large: "サービス業", middle: "他事業サービス", small: "適記・筆刷・報写業" });
  taxonomy.push({ large: "サービス業", middle: "他事業サービス", small: "商品検査業" });
  taxonomy.push({ large: "サービス業", middle: "他事業サービス", small: "計量証明業" });
  taxonomy.push({ large: "サービス業", middle: "他事業サービス", small: "建物サービス業" });
  taxonomy.push({ large: "サービス業", middle: "他事業サービス", small: "民営職業紹介業" });
  taxonomy.push({ large: "サービス業", middle: "他事業サービス", small: "整備業" });
  taxonomy.push({ large: "サービス業", middle: "他事業サービス", small: "人材派遣業" });
  taxonomy.push({ large: "サービス業", middle: "他事業サービス", small: "その他事業サービス業" });
  taxonomy.push({ large: "サービス業", middle: "専門サービス業", small: "解決業" });
  taxonomy.push({ large: "サービス業", middle: "専門サービス業", small: "土木建築サービス業" });
  taxonomy.push({ large: "サービス業", middle: "専門サービス業", small: "デザイン業" });
  taxonomy.push({ large: "サービス業", middle: "専門サービス業", small: "その他専門サービス業" });
  taxonomy.push({ large: "サービス業", middle: "医療業", small: "その他医療関連サービス業" });
  taxonomy.push({ large: "サービス業", middle: "廃棄物処理業", small: "一般廃棄物処理業" });
  taxonomy.push({ large: "サービス業", middle: "廃棄物処理業", small: "産業廃棄物処理業" });
  taxonomy.push({ large: "サービス業", middle: "廃棄物処理業", small: "その他廃棄物処理業" });
  taxonomy.push({ large: "サービス業", middle: "他サービス業", small: "集会場" });
  taxonomy.push({ large: "サービス業", middle: "他サービス業", small: "土壺場" });
  taxonomy.push({ large: "サービス業", middle: "他サービス業", small: "その他サービス業" });
  
  // 分類不能の産業
  taxonomy.push({ large: "分類不能の産業", middle: "分類不能の産業", small: "分類不能の産業" });
  
  // 注意: PDFの内容には一部文字化けや誤字がある可能性があります。
  // 実際のデータと照合して、必要に応じて修正してください。
  
  // マップを構築
  const smallMap = new Map<string, IndustryTaxonomy>();
  const middleMap = new Map<string, { large: string; middle: string }>();
  const largeSet = new Set<string>();
  const smallToNormalized = new Map<string, string>();
  const middleToNormalized = new Map<string, string>();
  const largeToNormalized = new Map<string, string>();
  
  for (const item of taxonomy) {
    const smallNorm = normalizeString(item.small);
    const middleNorm = normalizeString(item.middle);
    const largeNorm = normalizeString(item.large);
    
    // 小分類マップ
    if (!smallMap.has(smallNorm)) {
      smallMap.set(smallNorm, item);
      smallToNormalized.set(item.small, smallNorm);
    }
    
    // 中分類マップ
    if (!middleMap.has(middleNorm)) {
      middleMap.set(middleNorm, { large: item.large, middle: item.middle });
      middleToNormalized.set(item.middle, middleNorm);
    }
    
    // 大分類セット
    largeSet.add(item.large);
    largeToNormalized.set(item.large, largeNorm);
  }
  
  return {
    smallMap,
    middleMap,
    largeSet,
    smallToNormalized,
    middleToNormalized,
    largeToNormalized,
  };
}

// ==============================
// 正規化ロジック
// ==============================

interface IndustryFields {
  industryLarge: string | null;
  industryMiddle: string | null;
  industrySmall: string | null;
  industryDetail: string | null;
}

interface NormalizedResult {
  industryLarge: string | null;
  industryMiddle: string | null;
  industrySmall: string | null;
  industryDetail: string | null;
  industryRaw: {
    industryLarge: string | null;
    industryMiddle: string | null;
    industrySmall: string | null;
    industryDetail: string | null;
  };
  industryNeedsReview: boolean;
  industryReviewReason: string | null;
  updateReason: string; // どのルールで確定したか
}

function normalizeIndustryFields(
  fields: IndustryFields,
  taxonomy: TaxonomyMaps
): NormalizedResult {
  const result: NormalizedResult = {
    industryLarge: fields.industryLarge,
    industryMiddle: fields.industryMiddle,
    industrySmall: fields.industrySmall,
    industryDetail: fields.industryDetail,
    industryRaw: {
      industryLarge: fields.industryLarge,
      industryMiddle: fields.industryMiddle,
      industrySmall: fields.industrySmall,
      industryDetail: fields.industryDetail,
    },
    industryNeedsReview: false,
    industryReviewReason: null,
    updateReason: "no_match",
  };
  
  // 1. industrySmallがtaxonomyの小分類に一致する場合
  if (fields.industrySmall) {
    const smallNorm = normalizeString(fields.industrySmall);
    const matched = taxonomy.smallMap.get(smallNorm);
    if (matched) {
      result.industrySmall = matched.small; // PDF正表記で確定
      result.industryMiddle = matched.middle;
      result.industryLarge = matched.large;
      result.updateReason = "small_match";
      
      // 矛盾チェック
      if (fields.industryMiddle && normalizeString(fields.industryMiddle) !== normalizeString(matched.middle)) {
        result.industryNeedsReview = true;
        result.industryReviewReason = `小分類一致だが中分類が矛盾: 既存="${fields.industryMiddle}", 補完="${matched.middle}"`;
      }
      if (fields.industryLarge && normalizeString(fields.industryLarge) !== normalizeString(matched.large)) {
        result.industryNeedsReview = true;
        result.industryReviewReason = `小分類一致だが大分類が矛盾: 既存="${fields.industryLarge}", 補完="${matched.large}"`;
      }
      
      return result;
    }
  }
  
  // 2. industryMiddleがtaxonomyの中分類に一致する場合
  if (fields.industryMiddle) {
    const middleNorm = normalizeString(fields.industryMiddle);
    const matched = taxonomy.middleMap.get(middleNorm);
    if (matched) {
      result.industryMiddle = matched.middle; // PDF正表記で確定
      result.industryLarge = matched.large;
      result.updateReason = "middle_match";
      
      // 矛盾チェック
      if (fields.industryLarge && normalizeString(fields.industryLarge) !== normalizeString(matched.large)) {
        result.industryNeedsReview = true;
        result.industryReviewReason = `中分類一致だが大分類が矛盾: 既存="${fields.industryLarge}", 補完="${matched.large}"`;
      }
      
      return result;
    }
  }
  
  // 3. industryLargeがtaxonomyの大分類に一致する場合
  if (fields.industryLarge) {
    const largeNorm = normalizeString(fields.industryLarge);
    // 大分類の一致チェック
    for (const large of taxonomy.largeSet) {
      if (normalizeString(large) === largeNorm) {
        result.industryLarge = large; // PDF正表記で確定
        result.updateReason = "large_match";
        return result;
      }
    }
  }
  
  // 4. industryDetailが誤配置の可能性がある場合
  if (fields.industryDetail) {
    const detailNorm = normalizeString(fields.industryDetail);
    
    // 小分類と一致するか
    const matchedSmall = taxonomy.smallMap.get(detailNorm);
    if (matchedSmall) {
      result.industryDetail = null; // 移動するのでクリア
      result.industrySmall = matchedSmall.small;
      result.industryMiddle = matchedSmall.middle;
      result.industryLarge = matchedSmall.large;
      result.updateReason = "detail_to_small";
      return result;
    }
    
    // 中分類と一致するか
    const matchedMiddle = taxonomy.middleMap.get(detailNorm);
    if (matchedMiddle) {
      result.industryDetail = null; // 移動するのでクリア
      result.industryMiddle = matchedMiddle.middle;
      result.industryLarge = matchedMiddle.large;
      result.updateReason = "detail_to_middle";
      return result;
    }
  }
  
  // 一致しない場合はそのまま（変更なし）
  return result;
}

// ==============================
// Firebase初期化
// ==============================

function initFirebaseAdmin() {
  let serviceAccountPath = process.env.GOOGLE_APPLICATION_CREDENTIALS;

  if (!serviceAccountPath) {
    console.error(
      "❌ エラー: 環境変数 GOOGLE_APPLICATION_CREDENTIALS が設定されていません"
    );
    process.exit(1);
  }

  if (!fs.existsSync(serviceAccountPath)) {
    console.error(
      `❌ エラー: サービスアカウントキーファイルが見つかりません: ${serviceAccountPath}`
    );
    process.exit(1);
  }

  const serviceAccount = JSON.parse(
    fs.readFileSync(serviceAccountPath, "utf8")
  );

  const projectId =
    serviceAccount.project_id ||
    process.env.GCLOUD_PROJECT ||
    process.env.GCP_PROJECT ||
    PROJECT_ID;

  admin.initializeApp({
    credential: admin.credential.cert(serviceAccount),
    projectId,
  });
  console.log(`✅ Firebase Admin initialized (Project ID: ${projectId})`);

  return admin.firestore();
}

// ==============================
// メイン処理
// ==============================

async function main() {
  const db = initFirebaseAdmin();
  const colRef = db.collection(COLLECTION_NAME);
  
  // Taxonomyを構築
  console.log("📚 業種taxonomyを構築中...");
  const taxonomy = buildIndustryTaxonomy();
  console.log(`✅ Taxonomy構築完了: 小分類=${taxonomy.smallMap.size}, 中分類=${taxonomy.middleMap.size}, 大分類=${taxonomy.largeSet.size}`);
  
  let lastDoc: FirebaseFirestore.QueryDocumentSnapshot | null = null;
  
  // 再開オプション
  if (START_FROM_DOC_ID) {
    try {
      const startDoc = await colRef.doc(START_FROM_DOC_ID).get();
      if (startDoc.exists) {
        lastDoc = startDoc as FirebaseFirestore.QueryDocumentSnapshot;
        console.log(`🔄 Resuming from document ID: ${START_FROM_DOC_ID}`);
      } else {
        console.warn(`⚠️  Warning: Document ID "${START_FROM_DOC_ID}" not found. Starting from beginning.`);
      }
    } catch (error) {
      console.error(`❌ Error loading start document: ${error}`);
      process.exit(1);
    }
  }
  
  let scanned = 0;
  let updated = 0;
  let needsReview = 0;
  const updateReasons: Record<string, number> = {};
  
  let batch = db.batch();
  let batchCount = 0;
  
  const checkpointFile = "normalize_industry_checkpoint.txt";
  
  console.log(
    `🔍 Scan start: collection="${COLLECTION_NAME}", pageSize=${PAGE_SIZE}, batchUpdateSize=${BATCH_UPDATE_SIZE}, DRY_RUN=${DRY_RUN}`
  );
  if (SKIP_SCANNED > 0) {
    console.log(`⏭️  Will skip first ${SKIP_SCANNED} scanned documents`);
  }
  
  // チェックポイントから再開
  if (!START_FROM_DOC_ID && fs.existsSync(checkpointFile)) {
    try {
      const checkpointData = fs.readFileSync(checkpointFile, "utf8").trim();
      const checkpointDocId = checkpointData.split("\n")[0];
      if (checkpointDocId) {
        const checkpointDoc = await colRef.doc(checkpointDocId).get();
        if (checkpointDoc.exists) {
          lastDoc = checkpointDoc as FirebaseFirestore.QueryDocumentSnapshot;
          const checkpointScanned = checkpointData.split("\n")[1] ? parseInt(checkpointData.split("\n")[1], 10) : 0;
          scanned = checkpointScanned;
          console.log(`🔄 Resuming from checkpoint: docId=${checkpointDocId}, scanned=${scanned}`);
        }
      }
    } catch (error) {
      console.warn(`⚠️  Warning: Could not load checkpoint: ${error}`);
    }
  }
  
  while (true) {
    let query = colRef.orderBy(admin.firestore.FieldPath.documentId()).limit(PAGE_SIZE);
    if (lastDoc) {
      query = query.startAfter(lastDoc.id);
    }
    
    const snap = await query.get();
    if (snap.empty) {
      break;
    }
    
    for (const doc of snap.docs) {
      scanned += 1;
      
      if (SKIP_SCANNED > 0 && scanned <= SKIP_SCANNED) {
        if (scanned % 10000 === 0) {
          console.log(`⏭️  Skipping... scanned=${scanned}/${SKIP_SCANNED}`);
        }
        lastDoc = doc as FirebaseFirestore.QueryDocumentSnapshot;
        continue;
      }
      
      const data = doc.data();
      // 文字列型に変換（数値やその他の型が入っている可能性がある）
      const toStr = (v: any): string | null => {
        if (v === null || v === undefined) return null;
        if (typeof v === "string") return v.trim() || null;
        return String(v).trim() || null;
      };
      
      const fields: IndustryFields = {
        industryLarge: toStr((data as any).industryLarge),
        industryMiddle: toStr((data as any).industryMiddle),
        industrySmall: toStr((data as any).industrySmall),
        industryDetail: toStr((data as any).industryDetail),
      };
      
      // 正規化
      const normalized = normalizeIndustryFields(fields, taxonomy);
      
      // 変更があるかチェック
      const hasChanges =
        normalized.industryLarge !== fields.industryLarge ||
        normalized.industryMiddle !== fields.industryMiddle ||
        normalized.industrySmall !== fields.industrySmall ||
        normalized.industryDetail !== fields.industryDetail ||
        normalized.industryNeedsReview !== ((data as any).industryNeedsReview || false);
      
      if (hasChanges) {
        updateReasons[normalized.updateReason] = (updateReasons[normalized.updateReason] || 0) + 1;
        
        if (normalized.industryNeedsReview) {
          needsReview += 1;
        }
        
        if (DRY_RUN) {
          console.log(
            `🔧 [candidate] docId=${doc.id}, reason=${normalized.updateReason}, needsReview=${normalized.industryNeedsReview}`
          );
          if (normalized.industryReviewReason) {
            console.log(`   ⚠️  ${normalized.industryReviewReason}`);
          }
        } else {
          const updateData: any = {
            industryLarge: normalized.industryLarge,
            industryMiddle: normalized.industryMiddle,
            industrySmall: normalized.industrySmall,
            industryDetail: normalized.industryDetail,
            industryRaw: normalized.industryRaw,
            industryNeedsReview: normalized.industryNeedsReview,
          };
          
          if (normalized.industryReviewReason) {
            updateData.industryReviewReason = normalized.industryReviewReason;
          }
          
          batch.update(doc.ref, updateData);
          batchCount += 1;
          
          if (batchCount >= BATCH_UPDATE_SIZE) {
            await batch.commit();
            updated += batchCount;
            console.log(
              `💾 Committed update batch: ${batchCount} docs (total updated: ${updated}, scanned: ${scanned})`
            );
            batch = db.batch();
            batchCount = 0;
          }
        }
      }
      
      if (scanned % 10000 === 0) {
        console.log(
          `📦 scanning... scanned=${scanned}, updated=${updated}, needsReview=${needsReview}`
        );
        // チェックポイントを保存
        if (!DRY_RUN && lastDoc) {
          try {
            fs.writeFileSync(
              checkpointFile,
              `${lastDoc.id}\n${scanned}`,
              "utf8"
            );
          } catch (error) {
            // チェックポイント保存エラーは無視
          }
        }
      }
    }
    
    lastDoc = snap.docs[snap.docs.length - 1];
  }
  
  if (!DRY_RUN && batchCount > 0) {
    await batch.commit();
    updated += batchCount;
    console.log(
      `💾 Committed final update batch: ${batchCount} docs (total updated: ${updated})`
    );
  }
  
  // チェックポイントファイルを削除
  if (fs.existsSync(checkpointFile)) {
    try {
      fs.unlinkSync(checkpointFile);
      console.log(`🗑️  Checkpoint file removed`);
    } catch (error) {
      // チェックポイント削除エラーは無視
    }
  }
  
  console.log("✅ Update finished");
  console.log(`  🔍 scanned docs : ${scanned}`);
  console.log(`  ✅ updated      : ${updated} (DRY_RUN=${DRY_RUN})`);
  console.log(`  ⚠️  needsReview  : ${needsReview}`);
  console.log(`  📊 update reasons:`);
  for (const [reason, count] of Object.entries(updateReasons)) {
    console.log(`     ${reason}: ${count}`);
  }
}

// 実行
main().catch((err) => {
  console.error("❌ Error:", err);
  process.exit(1);
});

