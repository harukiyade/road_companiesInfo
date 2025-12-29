/**
 * 企業のWeb情報を格納する型定義
 */
export interface CompanyWebInfo {
  // 企業リスト表示項目
  /** 上場区分 */
  listingStatus: string | null;
  /** 証券CD */
  securitiesCode: string | null;
  /** HP */
  website: string | null;
  /** 問い合わせフォームのリンク */
  contactFormUrl: string | null;
  /** 経審の評価 */
  constructionEvaluation: string | null;
  /** 資本金 */
  capital: number | null;
  /** 売上（千円） */
  revenue: number | null;
  /** 利益（千円） */
  profit: number | null;
  /** 純資産（千円） */
  netAssets: number | null;
  /** 業種 */
  industry: string | null;
  /** 免許/事業者登録 */
  licenses: string[];
  /** 取引先銀行 */
  banks: string[];
  /** 企業説明 */
  companyDescription: string | null;
  /** 企業概要 */
  companyOverview: string | null;
  /** 取締役 */
  directors: string[];
  /** 社員数 */
  employeeCount: number | null;
  /** オフィス数 */
  officeCount: number | null;
  /** 工場数 */
  factoryCount: number | null;
  /** 店舗数 */
  storeCount: number | null;

  // 企業詳細画面項目
  /** 窓口メールアドレス */
  contactEmail: string | null;
  /** 窓口電話番号 */
  contactPhone: string | null;
  /** FAX */
  fax: string | null;
  /** SNS */
  sns: string[];
  /** 決算月 */
  settlementMonth: string | null;
  /** 代表者 */
  representative: string | null;
  /** 代表者（カナ） */
  representativeKana: string | null;
  /** 代表者住所 */
  representativeAddress: string | null;
  /** 代表者出身校 */
  representativeSchool: string | null;
  /** 代表者生年月日 */
  representativeBirthDate: string | null;
  /** 役員名 */
  officers: string[];
  /** 株主 */
  shareholders: string[];
  /** 自己資本比率 */
  equityRatio: number | null;

  // メタ情報
  /** 情報取得元URL一覧 */
  sourceUrls: string[];
  /** 更新日時（ISO文字列） */
  updatedAt: string;
  /** ステータス */
  status: "pending" | "running" | "success" | "partial" | "failed";
  /** エラーメッセージ */
  errorMessage?: string;
}

