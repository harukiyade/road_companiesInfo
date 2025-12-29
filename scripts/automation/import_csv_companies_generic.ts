/* scripts/import_csv_companies_generic.ts
 *
 * 変更点（要件対応）:
 *  - ヘッダー通りに値が入っているかの判断
 *  - companies_new のどのフィールドに対応するかの判断
 *    → Gemini API を併用できるように拡張
 *
 * 併用方針:
 *  - 既存の推測(ルール/パターン/学習ストア)をベースに
 *  - Gemini の推定結果を「加点/補正」する形で統合
 *  - Gemini が失敗/未設定なら従来ロジックのみで動く
 *
 * 依存:
 *   npm i firebase-admin papaparse dotenv
 *   npm i @google/generative-ai
 *
 * 実行例:
 *   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
 *   export GEMINI_API_KEY="xxxx"
 *   export USE_GEMINI=1
 *   export GEMINI_MODEL="gemini-1.5-flash"
 *   npx tsx scripts/import_csv_companies_generic.ts ./csv/135.csv
 *
 * オプション:
 *   ENCODING=utf8|sjis (default utf8)
 *   DRY_RUN=1
 *   LIMIT=1000
 *   SAVE_MAPPING=1
 *   USE_GEMINI=1
 *   GEMINI_MODEL=gemini-1.5-flash
 *   GEMINI_MAX_SAMPLE_ROWS=5
 *   GEMINI_TIMEOUT_MS=12000
 *
 * 注意:
 *  - Gemini 呼び出しを行うため、CSVが巨大でも「ファイル単位の解析」で最小限に抑えています。
 *  - 行ごとの AI 判定はコスト/速度面で重くなるので現時点では未採用。
 *    ただし“ヘッダー信頼度”と“列→フィールド対応”を AI が返すため、
 *    行解析の fallback と組み合わせて列ズレ耐性を高めます。
 */

import "dotenv/config";
import fs from "fs";
import path from "path";
import Papa from "papaparse";
import admin from "firebase-admin";
import { GoogleGenerativeAI } from "@google/generative-ai";

// optional: Shift_JIS対応したい場合だけ有効化
// import iconv from "iconv-lite";

////////////////////////////////////////////////////////////////////////////////
// 0) Firebase init
////////////////////////////////////////////////////////////////////////////////

function initAdmin() {
  if (admin.apps.length) return;
  try {
    admin.initializeApp({
      credential: admin.credential.applicationDefault(),
    });
  } catch (error) {
    console.error("❌ Firebase初期化エラー:", (error as Error).message);
    console.error("   環境変数 GOOGLE_APPLICATION_CREDENTIALS が正しく設定されているか確認してください");
    throw error;
  }
}

////////////////////////////////////////////////////////////////////////////////
// 1) Canonical schema (companies_new 側の“想定”フィールド名)
////////////////////////////////////////////////////////////////////////////////

type CanonicalCompany = {
  name?: string;
  corporateNumber?: string;
  postalCode?: string;
  prefecture?: string;
  address?: string;
  representativeName?: string;
  tel?: string;
  homepageUrl?: string;
  contactFormUrl?: string;

  industryLarge?: string;
  industryMiddle?: string;
  industrySmall?: string;
  industryDetail?: string[];

  capital?: number;
  employees?: number;
  revenue?: number;
  profit?: number;
  totalAssets?: number;
  foundedYear?: number;
  fiscalMonth?: number;

  source?: {
    file?: string;
    row?: number;
    rawHeader?: string[];
  };

  updatedAt?: FirebaseFirestore.FieldValue;
};

type CanonicalField =
  | "name"
  | "corporateNumber"
  | "postalCode"
  | "prefecture"
  | "address"
  | "representativeName"
  | "tel"
  | "homepageUrl"
  | "contactFormUrl"
  | "industryLarge"
  | "industryMiddle"
  | "industrySmall"
  | "industryDetail"
  | "capital"
  | "employees"
  | "revenue"
  | "profit"
  | "totalAssets"
  | "foundedYear"
  | "fiscalMonth";

const CANONICAL_FIELDS: CanonicalField[] = [
  "name",
  "corporateNumber",
  "postalCode",
  "prefecture",
  "address",
  "representativeName",
  "tel",
  "homepageUrl",
  "contactFormUrl",
  "industryLarge",
  "industryMiddle",
  "industrySmall",
  "industryDetail",
  "capital",
  "employees",
  "revenue",
  "profit",
  "totalAssets",
  "foundedYear",
  "fiscalMonth",
];

////////////////////////////////////////////////////////////////////////////////
// 2) ヘッダー辞書 + 学習ストア
////////////////////////////////////////////////////////////////////////////////

const HEADER_SYNONYMS: Record<CanonicalField, string[]> = {
  name: ["会社名", "企業名", "法人名", "商号", "名称", "社名"],
  corporateNumber: ["法人番号", "法人ナンバー", "corporateNumber"],
  postalCode: ["郵便番号", "郵便", "〒", "zip", "postal"],
  prefecture: ["都道府県", "県", "prefecture"],
  address: ["住所", "所在地", "本社住所", "本店所在地", "headquartersAddress", "所在地住所"],
  representativeName: ["代表者", "代表者名", "代表", "社長", "CEO", "代表取締役"],
  tel: ["電話", "電話番号", "TEL", "tel", "代表電話"],
  homepageUrl: ["HP", "ホームページ", "URL", "Web", "サイト", "homepage", "企業URL"],
  contactFormUrl: ["問い合わせURL", "問い合わせフォーム", "contact", "フォームURL"],
  industryLarge: ["業種大", "大分類", "業種(大)"],
  industryMiddle: ["業種中", "中分類", "業種(中)"],
  industrySmall: ["業種小", "小分類", "業種(小)"],
  industryDetail: ["業種", "業種詳細", "事業内容", "業態", "service"],
  capital: ["資本金", "capital"],
  employees: ["従業員", "従業員数", "社員数", "employees"],
  revenue: ["売上", "売上高", "revenue"],
  profit: ["利益", "営業利益", "経常利益", "profit"],
  totalAssets: ["総資産", "資産", "assets"],
  foundedYear: ["設立", "設立年", "創業", "創業年", "founded"],
  fiscalMonth: ["決算月", "事業年度", "fiscal"],
};

const MAPPING_STORE_PATH = path.resolve(process.cwd(), "out/header-mapping-store.json");
const KNOWLEDGE_STORE_PATH = path.resolve(process.cwd(), "out/field-knowledge-store.json");

type MappingStore = {
  headerToField: Record<
    string,
    {
      field: CanonicalField;
      count: number;
    }
  >;
};

type FieldKnowledge = {
  field: CanonicalField;
  valuePatterns: Record<string, { count: number; examples: string[] }>; // 値のパターンと出現回数
  typePatterns: Record<string, number>; // 型パターン（string, number, array等）の出現回数
  lastUpdated: string;
};

type KnowledgeStore = {
  fields: Record<CanonicalField, FieldKnowledge>;
};

function loadMappingStore(): MappingStore {
  try {
    const raw = fs.readFileSync(MAPPING_STORE_PATH, "utf8");
    return JSON.parse(raw);
  } catch {
    return { headerToField: {} };
  }
}

function saveMappingStore(store: MappingStore) {
  fs.mkdirSync(path.dirname(MAPPING_STORE_PATH), { recursive: true });
  fs.writeFileSync(MAPPING_STORE_PATH, JSON.stringify(store, null, 2), "utf8");
}

function loadKnowledgeStore(): KnowledgeStore {
  try {
    const raw = fs.readFileSync(KNOWLEDGE_STORE_PATH, "utf8");
    return JSON.parse(raw);
  } catch {
    return { fields: {} as Record<CanonicalField, FieldKnowledge> };
  }
}

function saveKnowledgeStore(store: KnowledgeStore) {
  fs.mkdirSync(path.dirname(KNOWLEDGE_STORE_PATH), { recursive: true });
  fs.writeFileSync(KNOWLEDGE_STORE_PATH, JSON.stringify(store, null, 2), "utf8");
}

function updateKnowledgeStore(
  store: KnowledgeStore,
  field: CanonicalField,
  value: string,
  detectedType: string
) {
  if (!store.fields[field]) {
    store.fields[field] = {
      field,
      valuePatterns: {},
      typePatterns: {},
      lastUpdated: new Date().toISOString(),
    };
  }

  const knowledge = store.fields[field];
  
  // 型パターンを記録
  knowledge.typePatterns[detectedType] = (knowledge.typePatterns[detectedType] || 0) + 1;
  
  // 値のパターンを記録（正規化した値）
  const normalizedValue = norm(value).slice(0, 50); // 長すぎる値は切り詰め
  if (!knowledge.valuePatterns[normalizedValue]) {
    knowledge.valuePatterns[normalizedValue] = { count: 0, examples: [] };
  }
  knowledge.valuePatterns[normalizedValue].count += 1;
  if (knowledge.valuePatterns[normalizedValue].examples.length < 3) {
    knowledge.valuePatterns[normalizedValue].examples.push(value.slice(0, 100));
  }
  
  knowledge.lastUpdated = new Date().toISOString();
}

////////////////////////////////////////////////////////////////////////////////
// 3) 文字/セル正規化
////////////////////////////////////////////////////////////////////////////////

function norm(s: string) {
  return (s ?? "")
    .toString()
    .replace(/\u00A0/g, " ")
    .replace(/[ \t\r\n]+/g, " ")
    .trim();
}

function normKey(s: string) {
  return norm(s).toLowerCase().replace(/[Ａ-Ｚａ-ｚ０-９]/g, (ch) => {
    const code = ch.charCodeAt(0);
    if (code >= 0xff10 && code <= 0xff19) return String.fromCharCode(code - 0xfee0);
    if (code >= 0xff21 && code <= 0xff3a) return String.fromCharCode(code - 0xfee0);
    if (code >= 0xff41 && code <= 0xff5a) return String.fromCharCode(code - 0xfee0);
    return ch;
  });
}

function isEmptyCell(s: string) {
  const v = norm(s);
  if (!v) return true;
  if (v === "-" || v === "ー" || v === "―" || v === "n/a") return true;
  return false;
}

////////////////////////////////////////////////////////////////////////////////
// 4) 値パターン(アンカー)判定
////////////////////////////////////////////////////////////////////////////////

const RE = {
  corporateNumber: /^\d{13}$/,
  postal: /^\d{3}-?\d{4}$/,
  tel: /^(0\d{1,4}-?\d{1,4}-?\d{3,4})$/,
  url: /^https?:\/\//i,
  yenLike: /円|¥/,
  numberLike: /^[\d,._]+$/,
  foundedYear: /^(18|19|20)\d{2}$/,
};

const PREF_LIST = [
  "北海道",
  "青森県",
  "岩手県",
  "宮城県",
  "秋田県",
  "山形県",
  "福島県",
  "茨城県",
  "栃木県",
  "群馬県",
  "埼玉県",
  "千葉県",
  "東京都",
  "神奈川県",
  "新潟県",
  "富山県",
  "石川県",
  "福井県",
  "山梨県",
  "長野県",
  "岐阜県",
  "静岡県",
  "愛知県",
  "三重県",
  "滋賀県",
  "京都府",
  "大阪府",
  "兵庫県",
  "奈良県",
  "和歌山県",
  "鳥取県",
  "島根県",
  "岡山県",
  "広島県",
  "山口県",
  "徳島県",
  "香川県",
  "愛媛県",
  "高知県",
  "福岡県",
  "佐賀県",
  "長崎県",
  "熊本県",
  "大分県",
  "宮崎県",
  "鹿児島県",
  "沖縄県",
];

function extractPrefecture(addr: string): string | undefined {
  const v = norm(addr);
  if (!v) return;
  for (const p of PREF_LIST) {
    if (v.startsWith(p)) return p;
    if (v.includes(p)) return p;
  }
  return;
}

function looksLikeCompanyName(v: string) {
  const s = norm(v);
  if (!s) return false;
  return /株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|一般財団法人|公益社団法人|公益財団法人|学校法人|医療法人|社会福祉法人|特定非営利活動法人|NPO/.test(
    s
  );
}

function looksLikeIndustryToken(v: string) {
  const s = norm(v);
  if (!s) return false;
  if (RE.postal.test(s) || RE.corporateNumber.test(s) || RE.tel.test(s) || RE.url.test(s)) return false;
  return /製造|卸|小売|建設|不動産|運輸|物流|IT|情報|ソフト|システム|サービス|医療|福祉|教育|金融|保険|広告|人材|コンサル|飲食|宿泊|農業|漁業|鉱業|電気|ガス|水道|通信|メディア|エネルギー/.test(
    s
  );
}

function parseNumberLoose(v: string): number | undefined {
  const s = norm(v);
  if (!s) return;

  const unitMatch = s.match(/^([\d.]+)\s*(億|万|千)?/);
  if (unitMatch) {
    const n = Number(unitMatch[1]);
    if (Number.isFinite(n)) {
      const unit = unitMatch[2];
      if (unit === "億") return Math.round(n * 100_000_000);
      if (unit === "万") return Math.round(n * 10_000);
      if (unit === "千") return Math.round(n * 1_000);
      return Math.round(n);
    }
  }

  const cleaned = s.replace(/[,，]/g, "").replace(/円|¥|人|名/g, "");
  const num = Number(cleaned.replace(/[^\d.]/g, ""));
  if (Number.isFinite(num) && num !== 0) return Math.round(num);
  return;
}

////////////////////////////////////////////////////////////////////////////////
// 5) ヘッダー行の推定とマッピング（ルール/学習）
////////////////////////////////////////////////////////////////////////////////

function headerLikelihoodScore(row: string[]): number {
  let score = 0;
  const cells = row.map((c) => normKey(c));
  for (const c of cells) {
    if (!c) continue;
    for (const field of Object.keys(HEADER_SYNONYMS) as CanonicalField[]) {
      const syns = HEADER_SYNONYMS[field].map(normKey);
      if (syns.some((s) => c.includes(s) || s.includes(c))) {
        score += 1;
        break;
      }
    }
  }
  return score;
}

function findHeaderRowIndex(previewRows: string[][]): number | null {
  let bestIdx = -1;
  let bestScore = 0;
  previewRows.forEach((r, i) => {
    const sc = headerLikelihoodScore(r);
    if (sc > bestScore) {
      bestScore = sc;
      bestIdx = i;
    }
  });
  if (bestScore >= 2) return bestIdx;
  return null;
}

function mapHeaderToFields(header: string[], store: MappingStore) {
  const map = new Map<number, CanonicalField>();
  header.forEach((h, idx) => {
    const key = normKey(h);
    if (!key) return;

    // 1) 学習ストア優先
    const learned = store.headerToField[key];
    if (learned && learned.count >= 2) {
      map.set(idx, learned.field);
      return;
    }

    // 2) シノニム一致
    let best: { field: CanonicalField; score: number } | null = null;
    for (const field of Object.keys(HEADER_SYNONYMS) as CanonicalField[]) {
      const syns = HEADER_SYNONYMS[field].map(normKey);
      let s = 0;
      for (const syn of syns) {
        if (!syn) continue;
        if (key === syn) s += 3;
        else if (key.includes(syn) || syn.includes(key)) s += 1;
      }
      if (best === null || s > best.score) {
        best = { field, score: s };
      }
    }

    if (best !== null && best.score > 0) {
      map.set(idx, best.field);
    }
  });

  return map;
}

function updateMappingStoreFromHeader(header: string[], headerMap: Map<number, CanonicalField>, store: MappingStore) {
  header.forEach((h, idx) => {
    const field = headerMap.get(idx);
    const key = normKey(h);
    if (!field || !key) return;
    const cur = store.headerToField[key];
    if (!cur) store.headerToField[key] = { field, count: 1 };
    else if (cur.field === field) cur.count += 1;
    else store.headerToField[key] = { field, count: 1 };
  });
}

////////////////////////////////////////////////////////////////////////////////
// 6) Gemini 連携（ヘッダー信頼度 & 列→フィールド推定）
////////////////////////////////////////////////////////////////////////////////

type GeminiHeaderAnalysis = {
  headerRowConfidence?: number; // 0-1
  trustHeader?: boolean; // ヘッダー通りに値が入っている可能性が高いか
  columnToField?: Record<string, { field: CanonicalField; confidence: number }>; // key: column index as string
  notes?: string;
  fallbackReason?: string; // フォールバック理由
};

type GeminiFieldTypeAnalysis = {
  field: CanonicalField;
  value: string;
  detectedType: "string" | "number" | "array" | "date" | "url" | "phone" | "postal" | "unknown";
  convertedValue: any;
  confidence: number;
};

/**
 * APIキーをマスクして表示（末尾4文字のみ表示）
 */
function maskApiKey(key: string | undefined): string {
  if (!key) return "(未設定)";
  if (key.length <= 4) return "****";
  return `****${key.slice(-4)}`;
}

function canUseGemini(): boolean {
  // Gemini APIは無効化されています
  return false;
  
  // 以下はコメントアウト（将来の使用に備えて保持）
  /*
  const useGemini = process.env.USE_GEMINI === "1";
  const apiKey = process.env.GEMINI_API_KEY;

  // 空文字列も未設定として扱う
  const hasValidKey = Boolean(apiKey && apiKey.trim().length > 0);

  if (useGemini && !hasValidKey) {
    console.warn("⚠️  [Gemini] GEMINI_API_KEY is missing. Fallback to heuristics only.");
    return false;
  }

  return useGemini && hasValidKey;
  */
}

function getGeminiModelName() {
  return process.env.GEMINI_MODEL || "gemini-1.5-flash";
}

/**
 * JSON文字列を抽出（fence除去、部分抽出対応）
 */
function extractJsonFromText(text: string): string | null {
  if (!text || typeof text !== "string") return null;

  const trimmed = text.trim();
  if (!trimmed) return null;

  // 1. ```json ... ``` または ``` ... ``` の除去
  let cleaned = trimmed;
  if (cleaned.startsWith("```")) {
    cleaned = cleaned.replace(/^```[a-zA-Z]*\n?/, "").replace(/```$/, "").trim();
  }

  // 2. { ... } の部分を抽出（前後に説明文があっても抽出）
  const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
  if (jsonMatch) {
    return jsonMatch[0];
  }

  // 3. そのまま返す（既にJSON形式の可能性）
  return cleaned;
}

async function callGeminiHeaderAnalysis(args: {
  header: string[];
  sampleRows: string[][];
}): Promise<GeminiHeaderAnalysis | null> {
  if (!canUseGemini()) {
    return { fallbackReason: "api_key_missing" };
  }

  // 念のため再チェック（空文字列も検出）
  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey || apiKey.trim().length === 0) {
    console.warn("⚠️  [Gemini] GEMINI_API_KEY is empty. Fallback to heuristics only.");
    return { fallbackReason: "api_key_empty" };
  }

  const genAI = new GoogleGenerativeAI(apiKey);
  const model = genAI.getGenerativeModel({ model: getGeminiModelName() });

  // sample は小さく（コスト回避）
  const maxSampleRows = process.env.GEMINI_MAX_SAMPLE_ROWS
    ? Math.min(Number(process.env.GEMINI_MAX_SAMPLE_ROWS), 10)
    : 5;
  const safeHeader = args.header.map((h) => norm(h)).slice(0, 200);
  const safeRows = args.sampleRows
    .map((r) => r.map((c) => norm(c)).slice(0, 200))
    .slice(0, maxSampleRows);

  const prompt = `
You are helping map messy Japanese company CSV columns into Firestore collection "companies_new".

We already have heuristic rules. Your job:
1) Decide whether the header and data appear aligned (no major column shift) for this file.
2) Suggest column index -> canonical field mapping with confidence.

Canonical fields (use ONLY these strings):
${CANONICAL_FIELDS.join(", ")}

Header synonyms reference (informal):
${JSON.stringify(HEADER_SYNONYMS, null, 2)}

Header row:
${JSON.stringify(safeHeader)}

Sample data rows (same column order as header):
${JSON.stringify(safeRows)}

Important:
- CSV may have column shifts where industry values spill into next columns.
- Postal code should be 3-4 digits pattern "NNN-NNNN".
- Corporate number should be 13 digits.
- URLs start with http/https.
- Addresses often contain Japanese prefecture names.

Return STRICT JSON format only:
{
  "trustHeader": boolean,
  "headerRowConfidence": number, // 0..1
  "columnToField": {
     "0": {"field":"name","confidence":0.0},
     "1": {"field":"address","confidence":0.0}
  },
  "notes": "short"
}
`;

  const timeoutMs = process.env.GEMINI_TIMEOUT_MS
    ? Number(process.env.GEMINI_TIMEOUT_MS)
    : 12000;

  try {
    console.log(`[Gemini] ヘッダー解析を開始 (APIキー: ${maskApiKey(apiKey)})`);

    // @google/generative-ai の generateContent は文字列を直接受け取る
    const res = await Promise.race([
      model.generateContent(prompt),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("Gemini API timeout")), timeoutMs)
      ),
    ]);

    const text = res.response.text();
    if (!text || typeof text !== "string") {
      console.warn("[Gemini] 空の応答を受信。フォールバックします。");
      return { fallbackReason: "empty_response" };
    }

    // JSON抽出
    const jsonStr = extractJsonFromText(text);
    if (!jsonStr) {
      console.warn("[Gemini] JSON形式の応答が見つかりません。フォールバックします。");
      console.warn(`[Gemini] 受信テキスト（先頭200文字）: ${text.slice(0, 200)}`);
      return { fallbackReason: "no_json_found" };
    }

    // JSONパース
    let parsed: any;
    try {
      parsed = JSON.parse(jsonStr);
    } catch (parseError) {
      console.warn("[Gemini] JSONパースに失敗しました。フォールバックします。");
      console.warn(`[Gemini] パース対象文字列（先頭200文字）: ${jsonStr.slice(0, 200)}`);
      return { fallbackReason: "json_parse_error" };
    }

    // 最低限バリデーション
    const out: GeminiHeaderAnalysis = {};
    if (typeof parsed.trustHeader === "boolean") {
      out.trustHeader = parsed.trustHeader;
    }
    if (typeof parsed.headerRowConfidence === "number") {
      out.headerRowConfidence = Math.max(0, Math.min(1, parsed.headerRowConfidence));
    }
    if (parsed.columnToField && typeof parsed.columnToField === "object") {
      const ctf: any = {};
      for (const [k, v] of Object.entries(parsed.columnToField)) {
        const vv: any = v;
        if (!vv?.field || !CANONICAL_FIELDS.includes(vv.field)) continue;
        const conf = typeof vv.confidence === "number" ? vv.confidence : 0;
        ctf[String(k)] = {
          field: vv.field,
          confidence: Math.max(0, Math.min(1, conf)),
        };
      }
      if (Object.keys(ctf).length > 0) {
        out.columnToField = ctf;
      }
    }
    if (typeof parsed.notes === "string") {
      out.notes = parsed.notes.slice(0, 300);
    }

    console.log(
      `[Gemini] 解析成功: trustHeader=${out.trustHeader ?? "N/A"}, confidence=${out.headerRowConfidence ?? "N/A"}`
    );
    if (out.notes) {
      console.log(`[Gemini] 備考: ${out.notes}`);
    }

    return out;
  } catch (e: any) {
    const errorMsg = e?.message ?? String(e);
    const errorType =
      errorMsg.includes("timeout") || errorMsg.includes("Timeout")
        ? "timeout"
        : errorMsg.includes("network") || errorMsg.includes("Network")
        ? "network_error"
        : errorMsg.includes("API key") || errorMsg.includes("API_KEY")
        ? "api_key_error"
        : errorMsg.includes("400") || errorMsg.includes("Bad Request")
        ? "bad_request"
        : "unknown_error";

    console.warn(`[Gemini] ヘッダー解析に失敗 (${errorType})。フォールバックします。`);
    console.warn(`[Gemini] エラー詳細: ${errorMsg.slice(0, 200)}`);

    return {
      fallbackReason: errorType,
    };
  }
}

/**
 * AIを使ったフィールドの型判定と変換（簡易版：コスト削減のためサンプリング）
 */
async function analyzeFieldTypeWithAI(
  field: CanonicalField,
  value: string,
  knowledgeStore: KnowledgeStore
): Promise<GeminiFieldTypeAnalysis | null> {
  if (!canUseGemini() || !value || value.trim().length === 0) {
    return null;
  }

  // コスト削減のため、重要なフィールドのみAI判定
  const importantFields: CanonicalField[] = ["capital", "revenue", "profit", "totalAssets", "foundedYear", "fiscalMonth"];
  if (!importantFields.includes(field)) {
    return null;
  }

  // ナレッジストアから過去のパターンを参照
  const knowledge = knowledgeStore.fields[field];
  const commonType = knowledge
    ? Object.entries(knowledge.typePatterns)
        .sort(([, a], [, b]) => b - a)[0]?.[0]
    : null;

  const apiKey = process.env.GEMINI_API_KEY;
  if (!apiKey || apiKey.trim().length === 0) {
    return null;
  }

  const genAI = new GoogleGenerativeAI(apiKey);
  const model = genAI.getGenerativeModel({ model: getGeminiModelName() });

  const prompt = `
Analyze the value for field "${field}" and determine its type and convert it appropriately.

Field: ${field}
Value: "${value.slice(0, 200)}"
${commonType ? `Common type for this field: ${commonType}` : ""}

Canonical field types:
- capital, revenue, profit, totalAssets: number (may need *1000 for some CSV types)
- foundedYear: number (year, e.g., 2023)
- fiscalMonth: number (month, 1-12)

Return JSON only:
{
  "detectedType": "number" | "string" | "unknown",
  "convertedValue": <converted number or original value>,
  "confidence": 0.0-1.0
}
`;

  try {
    const timeoutMs = 5000; // フィールド単位なので短めに

    const res = await Promise.race([
      model.generateContent(prompt),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error("Gemini API timeout")), timeoutMs)
      ),
    ]);

    const text = res.response.text();
    const jsonStr = extractJsonFromText(text);
    if (!jsonStr) return null;

    const parsed = JSON.parse(jsonStr);
    return {
      field,
      value,
      detectedType: parsed.detectedType || "unknown",
      convertedValue: parsed.convertedValue ?? value,
      confidence: parsed.confidence ?? 0.5,
    };
  } catch (e) {
    // エラー時はnullを返して従来の処理にフォールバック
    return null;
  }
}

/**
 * フィールドの値を適切な型に変換（AI + ナレッジストア + ルールベース）
 */
async function convertFieldValue(
  field: CanonicalField,
  value: string,
  csvType: CsvType,
  knowledgeStore: KnowledgeStore
): Promise<any> {
  if (!value || value.trim().length === 0) {
    return undefined;
  }

  // AI判定を試行（失敗してもフォールバック）
  const aiAnalysis = await analyzeFieldTypeWithAI(field, value, knowledgeStore);
  
  if (aiAnalysis && aiAnalysis.confidence >= 0.7) {
    // ナレッジストアを更新
    updateKnowledgeStore(knowledgeStore, field, value, aiAnalysis.detectedType);
    // 財務情報の場合は*1000の処理も適用
    if (shouldMultiplyFinancialBy1000(csvType) && ["capital", "revenue", "profit", "totalAssets"].includes(field)) {
      const num = typeof aiAnalysis.convertedValue === "number" ? aiAnalysis.convertedValue : parseNumberLoose(value);
      return num !== undefined ? Math.round(num * 1000) : undefined;
    }
    return aiAnalysis.convertedValue;
  }

  // フォールバック: ルールベースの変換
  switch (field) {
    case "capital":
    case "employees":
    case "revenue":
    case "profit":
    case "totalAssets":
      return parseFinancialNumber(value, csvType, field);
    case "foundedYear":
      return Number(value.replace(/[^\d]/g, "")) || undefined;
    case "fiscalMonth":
      return Number(value.replace(/[^\d]/g, "")) || undefined;
    case "industryDetail":
      return [value];
    default:
      return value;
  }
}

/**
 * 既存 headerMap(ルール) と Gemini columnToField を統合
 * - Gemini confidence が高いものを優先採用
 * - 既存が空の列は Gemini で補完
 * - 競合時は "confidence差" が小さければ既存を維持（安全寄り）
 * - Gemini失敗時は既存のheaderMapをそのまま返す
 */
function mergeHeaderMaps(
  header: string[],
  base: Map<number, CanonicalField>,
  gemini: GeminiHeaderAnalysis | null
) {
  // Gemini失敗時は既存のheaderMapをそのまま返す
  if (!gemini || gemini.fallbackReason || !gemini.columnToField) {
    return base;
  }

  const out = new Map<number, CanonicalField>(base);

  for (let i = 0; i < header.length; i++) {
    const g = gemini.columnToField[String(i)];
    if (!g) continue;

    const baseField = base.get(i);
    if (!baseField) {
      if (g.confidence >= 0.5) out.set(i, g.field);
      continue;
    }

    if (baseField === g.field) continue;

    // 競合: Gemini が十分に強い時のみ置換
    if (g.confidence >= 0.75) {
      out.set(i, g.field);
    }
  }

  return out;
}

////////////////////////////////////////////////////////////////////////////////
// 7) CSVタイプ判定と財務情報処理
////////////////////////////////////////////////////////////////////////////////

type CsvType = "type_d" | "type_e" | "type_f" | "type_i" | "type_j" | "other";

function detectCsvType(filePath: string): CsvType {
  const fileName = path.basename(filePath);
  
  // タイプD: 1.csv, 2.csv, 3.csv, 4.csv, 5.csv, 6.csv, 111.csv, 112.csv, 113.csv, 114.csv
  if (["1.csv", "2.csv", "3.csv", "4.csv", "5.csv", "6.csv", "111.csv", "112.csv", "113.csv", "114.csv"].includes(fileName)) {
    return "type_d";
  }
  
  // タイプE: 107.csv, 108.csv, 109.csv, 110.csv, 115.csv, 116.csv, 117.csv, 118.csv, 122.csv, 24.csv, 40.csv, 41.csv, 42.csv, 48.csv, 50.csv
  if (["107.csv", "108.csv", "109.csv", "110.csv", "115.csv", "116.csv", "117.csv", "118.csv", "122.csv", "24.csv", "40.csv", "41.csv", "42.csv", "48.csv", "50.csv"].includes(fileName)) {
    return "type_e";
  }
  
  // タイプF: 124.csv, 125.csv, 126.csv
  if (["124.csv", "125.csv", "126.csv"].includes(fileName)) {
    return "type_f";
  }
  
  // タイプI: 132.csv
  if (["132.csv"].includes(fileName)) {
    return "type_i";
  }
  
  // タイプJ: 133.csv, 134.csv, 135.csv, 136.csv
  if (["133.csv", "134.csv", "135.csv", "136.csv"].includes(fileName)) {
    return "type_j";
  }
  
  return "other";
}

function shouldMultiplyFinancialBy1000(csvType: CsvType): boolean {
  return csvType === "type_d" || csvType === "type_e" || csvType === "type_f" || csvType === "type_i" || csvType === "type_j";
}

function parseFinancialNumber(value: string, csvType: CsvType, field: CanonicalField): number | undefined {
  const num = parseNumberLoose(value);
  if (num === undefined) return undefined;
  
  // タイプD、E、F、I、Jの財務情報は*1000
  if (shouldMultiplyFinancialBy1000(csvType)) {
    const financialFields: CanonicalField[] = ["capital", "revenue", "profit", "totalAssets"];
    if (financialFields.includes(field)) {
      return Math.round(num * 1000);
    }
  }
  
  return num;
}

////////////////////////////////////////////////////////////////////////////////
// 8) 行単位での"値→フィールド"推定
////////////////////////////////////////////////////////////////////////////////

function dedupeIndustryTokens(tokens: string[]) {
  const s = new Set<string>();
  const out: string[] = [];
  for (const t of tokens.map(norm).filter(Boolean)) {
    if (s.has(t)) continue;
    s.add(t);
    out.push(t);
  }
  return out;
}

/**
 * 行全体の値解析（列ズレ対策コア）
 */
function analyzeRowByValues(cells: string[], rowIndex: number, rawHeader?: string[], file?: string): Partial<CanonicalCompany> {
  const cleaned = cells.map(norm);

  let corporateNumber: string | undefined;
  let postalCode: string | undefined;
  let tel: string | undefined;
  let homepageUrl: string | undefined;
  let address: string | undefined;
  let name: string | undefined;

  const industryTokens: string[] = [];

  for (const c of cleaned) {
    if (!c) continue;

    if (!corporateNumber && RE.corporateNumber.test(c)) {
      corporateNumber = c;
      continue;
    }
    if (!postalCode && RE.postal.test(c)) {
      postalCode = c.includes("-") ? c : `${c.slice(0, 3)}-${c.slice(3)}`;
      continue;
    }
    if (!tel && RE.tel.test(c)) {
      tel = c;
      continue;
    }
    if (!homepageUrl && RE.url.test(c)) {
      homepageUrl = c;
      continue;
    }
    if (!address) {
      const pref = extractPrefecture(c);
      if (pref) {
        address = c;
        continue;
      }
    }

    if (!name && looksLikeCompanyName(c)) {
      name = c;
      continue;
    }

    if (looksLikeIndustryToken(c)) {
      industryTokens.push(c);
      continue;
    }
  }

  const prefecture = extractPrefecture(address ?? "");

  if (!name) {
    const candidates = cleaned.filter((c) => {
      if (!c) return false;
      if (RE.corporateNumber.test(c) || RE.postal.test(c) || RE.tel.test(c) || RE.url.test(c)) return false;
      if (extractPrefecture(c)) return false;
      return c.length >= 2;
    });
    name = candidates[0];
  }

  const uniqIndustry = dedupeIndustryTokens(industryTokens);
  const industryLarge = uniqIndustry[0];
  const industryMiddle = uniqIndustry[1];
  const industrySmall = uniqIndustry[2];

  const out: Partial<CanonicalCompany> = {
    name,
    corporateNumber,
    postalCode,
    address,
    prefecture,
    tel,
    homepageUrl,
    industryLarge,
    industryMiddle,
    industrySmall,
    industryDetail: uniqIndustry.length ? uniqIndustry : undefined,
    source: { file, row: rowIndex, rawHeader },
  };

  return compactCompany(out);
}

/**
 * ヘッダー利用時の解析:
 * - headerMap に従って取り込み
 * - trustHeader=false の場合はより強めに値解析 fallback を効かせる
 */
async function analyzeRowWithHeader(
  cells: string[],
  header: string[],
  headerMap: Map<number, CanonicalField>,
  rowIndex: number,
  file?: string,
  trustHeader?: boolean,
  csvType?: CsvType,
  knowledgeStore?: KnowledgeStore
): Promise<Partial<CanonicalCompany>> {
  const out: Partial<CanonicalCompany> = { source: { file, row: rowIndex, rawHeader: header } };
  const cleaned = cells.map(norm);

  // forEachではなくfor...ofループでawaitを使用
  for (const [idx, field] of headerMap.entries()) {
    const v = cleaned[idx];
    if (!v || isEmptyCell(v)) continue;

    switch (field) {
      case "name":
      case "corporateNumber":
      case "postalCode":
      case "prefecture":
      case "address":
      case "representativeName":
        // 代表者名から生年月日を抽出して処理
        processRepresentativeNameForGeneric(v, out);
        break;
      case "tel":
      case "homepageUrl":
      case "contactFormUrl":
      case "industryLarge":
      case "industryMiddle":
      case "industrySmall":
        (out as any)[field] = v;
        break;
      case "industryDetail":
        out.industryDetail = [...(out.industryDetail ?? []), v];
        break;
      case "capital":
      case "employees":
      case "revenue":
      case "profit":
      case "totalAssets":
      case "foundedYear":
      case "fiscalMonth":
        // AI + ナレッジストア + ルールベースで型変換
        if (csvType && knowledgeStore) {
          const converted = await convertFieldValue(field, v, csvType, knowledgeStore);
          if (converted !== undefined) {
            (out as any)[field] = converted;
          }
        } else {
          // フォールバック: 従来の処理
          const detectedType = file ? detectCsvType(file) : "other";
          if (field === "capital" || field === "revenue" || field === "profit" || field === "totalAssets") {
            (out as any)[field] = parseFinancialNumber(v, detectedType, field);
          } else if (field === "foundedYear" || field === "fiscalMonth") {
            (out as any)[field] = Number(v.replace(/[^\d]/g, "")) || undefined;
          }
        }
        break;
    }
  }

  // postal列の“値が郵便番号じゃない”なら業種侵食を疑う
  let postalIdx: number | undefined;
  for (const [idx, f] of headerMap.entries()) {
    if (f === "postalCode") {
      postalIdx = idx;
      break;
    }
  }

  const postalCell = postalIdx != null ? cleaned[postalIdx] : undefined;
  const postalLooksValid = postalCell ? RE.postal.test(postalCell) : false;

  const shouldFallbackStrong =
    trustHeader === false || !postalLooksValid;

  if (shouldFallbackStrong) {
    const fallback = analyzeRowByValues(cells, rowIndex, header, file);
    const merged = shallowPrefer(await Promise.resolve(out), fallback);

    merged.industryDetail = dedupeIndustryTokens([
      ...(out.industryDetail ?? []),
      ...(fallback.industryDetail ?? []),
    ]);
    if (merged.industryDetail.length) {
      merged.industryLarge = merged.industryLarge ?? merged.industryDetail[0];
      merged.industryMiddle = merged.industryMiddle ?? merged.industryDetail[1];
      merged.industrySmall = merged.industrySmall ?? merged.industryDetail[2];
    }

    return compactCompany(merged);
  }

  if (!out.prefecture && out.address) out.prefecture = extractPrefecture(out.address);

  if (out.industryDetail?.length) {
    out.industryDetail = dedupeIndustryTokens(out.industryDetail);
    out.industryLarge = out.industryLarge ?? out.industryDetail[0];
    out.industryMiddle = out.industryMiddle ?? out.industryDetail[1];
    out.industrySmall = out.industrySmall ?? out.industryDetail[2];
  }

  return compactCompany(out);
}

////////////////////////////////////////////////////////////////////////////////
// 8) マージ/圧縮ユーティリティ
////////////////////////////////////////////////////////////////////////////////

// 代表者名から生年月日を抽出して処理（汎用インポート用）
function processRepresentativeNameForGeneric(value: string, out: any): void {
  if (!value || typeof value !== "string") return;
  
  const trimmed = value.trim();
  if (!trimmed) return;
  
  // 生年月日パターン（1900-2100年の範囲）
  const birthdatePatterns = [
    /(19\d{2}|20\d{2})[\/年-](\d{1,2})[\/月-](\d{1,2})/g,  // 1977/1/1, 1977-1-1, 1977年1月1日
    /(19\d{2}|20\d{2})\/(\d{1,2})\/(\d{1,2})/g,            // 1977/1/1
  ];
  
  let extractedDate: string | null = null;
  let cleaned = trimmed;
  
  // 生年月日を抽出
  for (const pattern of birthdatePatterns) {
    const match = cleaned.match(pattern);
    if (match) {
      const dateStr = match[0];
      const parts = dateStr.split(/[\/年-]/);
      if (parts.length >= 3) {
        const year = parseInt(parts[0]);
        const month = parseInt(parts[1]);
        const day = parseInt(parts[2]);
        
        // 有効な生年月日かチェック
        if (year >= 1900 && year <= 2100 && month >= 1 && month <= 12 && day >= 1 && day <= 31) {
          extractedDate = dateStr;
          // 生年月日部分を除去
          cleaned = cleaned.replace(pattern, "").trim();
          cleaned = cleaned.replace(/^[\s・、,，\-]/g, "").replace(/[\s・、,，\-]$/g, "").trim();
          break;
        }
      }
    }
  }
  
  // 生年月日を設定（既に値がある場合は上書きしない）
  if (extractedDate && !out.representativeBirthDate) {
    out.representativeBirthDate = extractedDate;
  }
  
  // 個人名（氏名）のみを抽出
  // 役職名を除去
  const titles = [
    "代表取締役", "代表取締役社長", "代表取締役会長", "代表取締役専務",
    "代表取締役常務", "代表取締役副社長", "取締役社長", "取締役会長",
    "社長", "会長", "専務", "常務", "副社長", "代表", "代表者", "CEO", "ceo"
  ];
  
  for (const title of titles) {
    if (cleaned.startsWith(title)) {
      cleaned = cleaned.substring(title.length).trim();
      cleaned = cleaned.replace(/^[\s・、,，]/g, "").trim();
      break;
    }
    const titlePattern = new RegExp(`^${title}[\\s・、,，]`, "i");
    if (titlePattern.test(cleaned)) {
      cleaned = cleaned.replace(titlePattern, "").trim();
      break;
    }
  }
  
  // カッコ内の情報を除去
  cleaned = cleaned.replace(/[（(].*?[）)]/g, "").trim();
  
  // 数字や記号のみの場合はnull
  if (/^[\d\s\-・、,，.。]+$/.test(cleaned)) {
    out.representativeName = null;
  } else if (cleaned && cleaned.length > 0) {
    out.representativeName = cleaned;
  }
}

function compactCompany(c: Partial<CanonicalCompany>): Partial<CanonicalCompany> {
  const out: any = { ...c };

  // 代表者名の処理（生年月日を抽出）
  if (out.representativeName && typeof out.representativeName === "string") {
    processRepresentativeNameForGeneric(out.representativeName, out);
  }

  if (out.industryDetail && Array.isArray(out.industryDetail)) {
    out.industryDetail = out.industryDetail.map(norm).filter(Boolean);
    if (!out.industryDetail.length) delete out.industryDetail;
  }

  if (out.postalCode) {
    const p = norm(out.postalCode).replace(/[^0-9]/g, "");
    if (p.length === 7) out.postalCode = `${p.slice(0, 3)}-${p.slice(3)}`;
  }

  if (!out.prefecture && out.address) out.prefecture = extractPrefecture(out.address);

  Object.keys(out).forEach((k) => {
    const v = out[k];
    if (typeof v === "string" && isEmptyCell(v)) delete out[k];
    if (v == null) delete out[k];
  });

  return out;
}

function shallowPrefer(primary: Partial<CanonicalCompany>, fallback: Partial<CanonicalCompany>) {
  const out: any = { ...fallback, ...primary };
  for (const k of Object.keys(fallback)) {
    if (primary[k as keyof CanonicalCompany] == null) {
      out[k] = (fallback as any)[k];
    }
  }
  return out as Partial<CanonicalCompany>;
}

function mergeForUpdate(existing: any, incoming: Partial<CanonicalCompany>) {
  const out: any = { ...existing };

  function setIf(v: any, key: string) {
    if (v == null) return;
    if (typeof v === "string" && isEmptyCell(v)) return;
    out[key] = v;
  }

  setIf(incoming.name, "name");
  setIf(incoming.corporateNumber, "corporateNumber");
  setIf(incoming.postalCode, "postalCode");
  setIf(incoming.prefecture, "prefecture");
  setIf(incoming.address, "address");
  setIf(incoming.representativeName, "representativeName");
  // representativeBirthDateはoutに含まれている可能性がある（processRepresentativeNameForGenericで設定）
  if ((incoming as any).representativeBirthDate && !out.representativeBirthDate) {
    out.representativeBirthDate = (incoming as any).representativeBirthDate;
  }
  setIf(incoming.tel, "tel");
  setIf(incoming.homepageUrl, "homepageUrl");
  setIf(incoming.contactFormUrl, "contactFormUrl");

  setIf(incoming.industryLarge, "industryLarge");
  setIf(incoming.industryMiddle, "industryMiddle");
  setIf(incoming.industrySmall, "industrySmall");
  if (incoming.industryDetail?.length) {
    const prev = Array.isArray(existing.industryDetail) ? existing.industryDetail : [];
    out.industryDetail = dedupeIndustryTokens([...prev, ...incoming.industryDetail]);
  }

  setIf(incoming.capital, "capital");
  setIf(incoming.employees, "employees");
  setIf(incoming.revenue, "revenue");
  setIf(incoming.profit, "profit");
  setIf(incoming.totalAssets, "totalAssets");
  setIf(incoming.foundedYear, "foundedYear");
  setIf(incoming.fiscalMonth, "fiscalMonth");

  if (incoming.source) out.lastImportSource = incoming.source;

  out.updatedAt = admin.firestore.FieldValue.serverTimestamp();

  return out;
}

////////////////////////////////////////////////////////////////////////////////
// 9) 既存企業の特定ロジック
////////////////////////////////////////////////////////////////////////////////

async function findExistingCompanyRef(
  companiesCol: FirebaseFirestore.CollectionReference,
  incoming: Partial<CanonicalCompany>
) {
  if (incoming.corporateNumber) {
    const snap = await companiesCol.where("corporateNumber", "==", incoming.corporateNumber).limit(1).get();
    if (!snap.empty) return snap.docs[0].ref;
  }

  if (incoming.name && incoming.address) {
    const q = companiesCol
      .where("name", "==", incoming.name)
      .where("address", "==", incoming.address)
      .limit(1);
    const snap = await q.get();
    if (!snap.empty) return snap.docs[0].ref;
  }

  if (incoming.name && incoming.prefecture) {
    const q = companiesCol
      .where("name", "==", incoming.name)
      .where("prefecture", "==", incoming.prefecture)
      .limit(1);
    const snap = await q.get();
    if (!snap.empty) return snap.docs[0].ref;
  }

  if (incoming.name && incoming.representativeName) {
    const q = companiesCol
      .where("name", "==", incoming.name)
      .where("representativeName", "==", incoming.representativeName)
      .limit(1);
    const snap = await q.get();
    if (!snap.empty) return snap.docs[0].ref;
  }

  if (incoming.name) {
    const snap = await companiesCol.where("name", "==", incoming.name).limit(1).get();
    if (!snap.empty) return snap.docs[0].ref;
  }

  return null;
}

////////////////////////////////////////////////////////////////////////////////
// 10) CSV 読み込み
////////////////////////////////////////////////////////////////////////////////

function readFileBuffer(filePath: string): Buffer {
  return fs.readFileSync(filePath);
}

function decodeCsv(buf: Buffer) {
  const enc = (process.env.ENCODING ?? "utf8").toLowerCase();
  if (enc === "sjis" || enc === "shift_jis") {
    // return iconv.decode(buf, "Shift_JIS");
    return buf.toString("binary");
  }
  return buf.toString("utf8");
}

async function parseCsvAllRows(filePath: string): Promise<string[][]> {
  const buf = readFileBuffer(filePath);
  const text = decodeCsv(buf);

  const parsed = Papa.parse<string[]>(text, {
    skipEmptyLines: true,
  });

  if (parsed.errors?.length) {
    console.warn("[CSV] parse warnings:", parsed.errors.slice(0, 3));
  }

  const rows = (parsed.data as any[]).map((r) =>
    Array.isArray(r) ? r.map((c) => (c == null ? "" : String(c))) : []
  );

  return rows;
}

////////////////////////////////////////////////////////////////////////////////
// 11) Upsert 本体（Gemini 併用）
////////////////////////////////////////////////////////////////////////////////

async function upsertCompaniesFromCsv(filePath: string) {
  initAdmin();
  const db = admin.firestore();
  const companiesCol = db.collection("companies_new");

  const store = loadMappingStore();
  const knowledgeStore = loadKnowledgeStore();
  const csvType = detectCsvType(filePath);

  const rows = await parseCsvAllRows(filePath);
  const limit = process.env.LIMIT ? Number(process.env.LIMIT) : Infinity;
  const dryRun = process.env.DRY_RUN === "1";
  const saveMapping = process.env.SAVE_MAPPING === "1";

  // 処理開始ログ
  const geminiEnabled: boolean = canUseGemini();
  console.log(`[CSV Import] 開始: ${path.basename(filePath)} (行数: ${rows.length}, Gemini: ${geminiEnabled ? "有効" : "無効"}, CSVタイプ: ${csvType})`);

  // 先頭数行でヘッダー推定
  const preview = rows.slice(0, Math.min(5, rows.length));
  const headerRowIndex = findHeaderRowIndex(preview);

  let header: string[] | null = null;
  let headerMap: Map<number, CanonicalField> | null = null;
  let geminiAnalysis: GeminiHeaderAnalysis | null = null;

  if (headerRowIndex != null) {
    header = rows[headerRowIndex].map(norm);
    const baseMap = mapHeaderToFields(header, store);

    // Gemini に "ヘッダー整合性 + 列→フィールド推定" を依頼
    const maxSample = process.env.GEMINI_MAX_SAMPLE_ROWS
      ? Number(process.env.GEMINI_MAX_SAMPLE_ROWS)
      : 5;
    const sampleRows = rows
      .filter((_, i) => i !== headerRowIndex)
      .slice(0, Math.max(1, Math.min(maxSample, 10))); // 上限10行

    geminiAnalysis = await callGeminiHeaderAnalysis({
      header,
      sampleRows,
    });

    headerMap = mergeHeaderMaps(header, baseMap, geminiAnalysis);

    if (saveMapping) updateMappingStoreFromHeader(header, headerMap, store);
  }

  let processed = 0;
  let created = 0;
  let updated = 0;
  let skipped = 0;

  const trustHeader = geminiAnalysis?.trustHeader;

  for (let i = 0; i < rows.length && processed < limit; i++) {
    if (headerRowIndex != null && i === headerRowIndex) continue;

    const row = rows[i];
    if (!row || row.every((c) => isEmptyCell(String(c ?? "")))) continue;

    let company: Partial<CanonicalCompany>;

    if (header && headerMap && headerMap.size > 0) {
      company = await analyzeRowWithHeader(
        row,
        header,
        headerMap,
        i + 1,
        path.basename(filePath),
        trustHeader,
        csvType,
        knowledgeStore
      );
    } else {
      company = analyzeRowByValues(row, i + 1, header ?? undefined, path.basename(filePath));
    }

    if (!company.name && !company.address) {
      skipped++;
      continue;
    }

    const ref = await findExistingCompanyRef(companiesCol, company);

    if (dryRun) {
      processed++;
      continue;
    }

    if (ref) {
      const snap = await ref.get();
      const existing = snap.data() ?? {};
      const merged = mergeForUpdate(existing, company);
      await ref.set(merged, { merge: true });
      updated++;
    } else {
      const doc = mergeForUpdate({}, company);
      await companiesCol.add(doc);
      created++;
    }

    processed++;

    if (processed % 200 === 0) {
      console.log(`[progress] processed=${processed} created=${created} updated=${updated} skipped=${skipped}`);
    }
  }

  if (saveMapping) {
    saveMappingStore(store);
  }
  // ナレッジストアは常に保存（学習データの蓄積）
  saveKnowledgeStore(knowledgeStore);

  // 処理終了ログ
  const geminiUsed = geminiAnalysis && !geminiAnalysis.fallbackReason;
  const fallbackReason = geminiAnalysis?.fallbackReason ?? null;
  
  console.log("=== done ===");
  const result = {
    processed,
    created,
    updated,
    skipped,
    headerDetected: headerRowIndex != null,
    geminiUsed,
    trustHeader: trustHeader ?? null,
    headerRowConfidence: geminiAnalysis?.headerRowConfidence ?? null,
    geminiNotes: geminiAnalysis?.notes ?? null,
    geminiFallbackReason: fallbackReason,
  };
  console.log(JSON.stringify(result, null, 2));
  
  // フォールバック理由があれば簡潔に表示
  if (fallbackReason) {
    console.log(`[Gemini] フォールバック理由: ${fallbackReason}`);
  }
}

////////////////////////////////////////////////////////////////////////////////
// 12) CLI
////////////////////////////////////////////////////////////////////////////////

async function main() {
  // 起動時のGemini状態ログ
  const geminiEnabled = canUseGemini();
  const apiKeyPresent = Boolean(process.env.GEMINI_API_KEY && process.env.GEMINI_API_KEY.trim().length > 0);
  console.log(`Gemini enabled: ${geminiEnabled}`);
  console.log(`GEMINI_API_KEY present: ${apiKeyPresent}`);

  // APIキー露出チェック（コマンドライン引数に含まれていないか）
  const argsStr = process.argv.join(" ");
  if (argsStr.includes("GEMINI_API_KEY") && argsStr.match(/AlzaSy[A-Za-z0-9_-]{20,}/)) {
    console.error("❌ エラー: APIキーがコマンドライン引数に含まれている可能性があります。");
    console.error("   APIキーは環境変数で設定してください。");
    process.exit(1);
  }

  const filePath = process.argv[2];
  if (!filePath) {
    console.error("Usage: npx tsx scripts/automation/import_csv_companies_generic.ts <csvPath>");
    process.exit(1);
  }
  if (!fs.existsSync(filePath)) {
    console.error("File not found:", filePath);
    process.exit(1);
  }

  await upsertCompaniesFromCsv(filePath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});