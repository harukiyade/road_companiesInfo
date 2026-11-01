"""
config.py - 全モジュール共通設定・定数

環境変数で上書き可能な値は os.getenv() 経由で読み込む。
APIキーは .env ファイルまたはシェル環境変数で供給すること。
"""
from __future__ import annotations

import os
from pathlib import Path

# ─────────────────────────────────────────────
# パス設定
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).parent.parent
ENRICHMENT_DIR = BASE_DIR / "enrichment"
OUTPUT_DIR = BASE_DIR / "output"
LOGS_DIR = BASE_DIR / "logs" / "enrichment"
DB_PATH = BASE_DIR / "enrichment_state.db"

# ─────────────────────────────────────────────
# API設定（環境変数 or .env から供給）
# ─────────────────────────────────────────────
GBIZ_INFO_API_BASE = "https://info.gbiz.go.jp/hojin/v1/hojin"
GBIZ_INFO_API_KEY: str = os.getenv("GBIZ_INFO_API_KEY", "")

GOOGLE_CSE_API_KEY: str = os.getenv("GOOGLE_CSE_API_KEY", "")
GOOGLE_CSE_CX: str = os.getenv("GOOGLE_CSE_CX", "")

BING_API_KEY: str = os.getenv("BING_API_KEY", "")

# ─────────────────────────────────────────────
# HTTP設定
# ─────────────────────────────────────────────
HTTP_TIMEOUT: int = int(os.getenv("HTTP_TIMEOUT", "15"))          # 秒（一般ページ取得）
HTTP_MAX_RETRIES: int = int(os.getenv("HTTP_MAX_RETRIES", "3"))   # 最大リトライ回数
HTTP_BACKOFF_BASE: float = float(os.getenv("HTTP_BACKOFF_BASE", "2.0"))  # 指数バックオフ基数
HTTP_USER_AGENT: str = (
    "LegatusONE-Enrichment-Bot/1.0 "
    "(+https://legatus.one/enrichment-bot; contact: enrichment@legatus.one)"
)

# ─────────────────────────────────────────────
# Web検索専用（Bing / DuckDuckGo / Google CSE）
# ─────────────────────────────────────────────
SEARCH_TIMEOUT: int = int(os.getenv("SEARCH_TIMEOUT", "30"))           # 秒（検索は長め）
SEARCH_MAX_RETRIES: int = int(os.getenv("SEARCH_MAX_RETRIES", "3"))    # 検索リトライ回数
SEARCH_BACKOFF_BASE: float = float(os.getenv("SEARCH_BACKOFF_BASE", "2.0"))
SEARCH_API_INTERVAL: float = float(os.getenv("SEARCH_API_INTERVAL", "1.5"))  # 秒（リクエスト間隔）

# ─────────────────────────────────────────────
# レート制御
# ─────────────────────────────────────────────
DOMAIN_MIN_INTERVAL: float = float(os.getenv("DOMAIN_MIN_INTERVAL", "1.5"))  # 秒

# ─────────────────────────────────────────────
# PostgreSQL 耐障害性
# ─────────────────────────────────────────────
PG_CONNECT_TIMEOUT: int = int(os.getenv("PG_CONNECT_TIMEOUT", "15"))
PG_MAX_RETRIES: int = int(os.getenv("PG_MAX_RETRIES", "3"))
PG_RETRY_BACKOFF: float = float(os.getenv("PG_RETRY_BACKOFF", "2.0"))
PG_KEEPALIVES: int = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE: int = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL: int = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT: int = int(os.getenv("PG_KEEPALIVES_COUNT", "5"))

# ─────────────────────────────────────────────
# 処理設定
# ─────────────────────────────────────────────
WORKERS: int = int(os.getenv("WORKERS", "4"))
FETCH_BATCH_SIZE: int = int(os.getenv("FETCH_BATCH_SIZE", "50"))   # 1ワーカーあたりの取得バッチ
CSV_FLUSH_ROWS: int = int(os.getenv("CSV_FLUSH_ROWS", "100000"))   # CSVファイル分割単位
PROCESSING_STALL_SEC: int = int(os.getenv("PROCESSING_STALL_SEC", "3600"))  # 停滞判定秒
PG_CHUNK_SIZE: int = int(os.getenv("PG_CHUNK_SIZE", "1000"))       # PostgreSQL 1チャンク件数

# ─────────────────────────────────────────────
# 確信度スコア設定
# ─────────────────────────────────────────────
SCORE_NAME_EXACT = 40       # タイトル/フッターで会社名完全一致
SCORE_NAME_PARTIAL = 20     # テキスト内で会社名完全一致
SCORE_NAME_SHORT = 10       # 短縮名（法人格除外）一致
SCORE_PHONE_MATCH = 30      # 電話番号一致
SCORE_PHONE_MISMATCH = -30  # 電話番号不一致（既存値あり）
SCORE_ADDRESS_MATCH = 20    # 都道府県一致
SCORE_ADDRESS_MISMATCH = -10
SCORE_CORPORATE_NUMBER = 50 # 法人番号一致
SCORE_SSL = 5               # HTTPS
SCORE_GBIZ_PREF_MATCH = 10  # gBizINFO都道府県との一致

CONFIDENCE_HIGH_THRESHOLD: int = 70    # high判定スコア閾値
CONFIDENCE_MEDIUM_THRESHOLD: int = 40  # medium判定スコア閾値

# ─────────────────────────────────────────────
# 除外ドメインリスト（SNS・ポータル・求人・政府等）
# ─────────────────────────────────────────────
EXCLUDED_DOMAINS: frozenset[str] = frozenset({
    # SNS
    "facebook.com", "linkedin.com", "twitter.com", "x.com",
    "instagram.com", "youtube.com", "tiktok.com", "pinterest.com",
    # 採用・求人
    "wantedly.com", "indeed.com", "rikunabi.com", "mynavi.jp",
    "doda.jp", "bizreach.jp", "openwork.jp", "vorkers.com",
    "recruit.co.jp", "en-japan.com", "green-japan.com",
    # 百科事典・情報サイト
    "ja.wikipedia.org", "en.wikipedia.org", "wikimedia.org",
    # ポータル
    "google.com", "yahoo.co.jp", "bing.com", "baidu.com",
    # EC
    "rakuten.co.jp", "amazon.co.jp", "amazon.com",
    "kakaku.com", "price.com",
    # グルメ・地域情報
    "tabelog.com", "hotpepper.jp", "gurunavi.com", "jalan.net",
    # 地図
    "maps.google.com", "mapion.co.jp",
    # ニュース
    "nikkei.com", "asahi.com", "yomiuri.co.jp",
    # 企業DB・信用調査
    "tdb.co.jp", "tsr-net.co.jp", "teikoku-electric.co.jp",
    # 政府・法人番号
    "nta.go.jp", "houjin-bangou.nta.go.jp", "gbiz.go.jp",
    # YouTrust
    "youtrust.jp",
    # プレスリリース
    "prtimes.jp",
    # その他
    "ameblo.jp", "note.com", "qiita.com", "zenn.dev",
})

# ─────────────────────────────────────────────
# HP候補として優先参照する既存カラム名
# ─────────────────────────────────────────────
URL_CANDIDATE_COLUMNS: list[str] = [
    "url", "urls", "homepage_url", "website_url", "company_url", "contact_form_url",
]

# ─────────────────────────────────────────────
# 会社概要系ページのパス候補（日本語サイト向け）
# ─────────────────────────────────────────────
COMPANY_PAGE_PATHS: list[str] = [
    "/company", "/about", "/corporate", "/profile", "/about-us",
    "/company/profile", "/corporate/profile", "/corporate/about",
    "/about/company", "/company/about",
    "/gaisha", "/kaisha", "/annai",
]
COMPANY_PAGE_KEYWORDS: list[str] = [
    "会社概要", "企業情報", "企業概要", "会社情報", "コーポレート",
    "会社案内", "グループ概要", "about", "company", "corporate", "profile",
]

CONTACT_PAGE_KEYWORDS: list[str] = [
    "お問い合わせ", "問い合わせ", "コンタクト", "contact", "inquiry",
]

RECRUIT_PAGE_KEYWORDS: list[str] = [
    "採用", "リクルート", "recruit", "careers", "job",
]

# ─────────────────────────────────────────────
# 出力CSVの固定スキーマ（3-1章対象カラム + メタカラム）
#
# 先頭ブロック「識別子パススルー」: スクレイピング対象外だが
# DB再インポート時のマッチング・検証に必須のため常時出力する列。
# ─────────────────────────────────────────────
OUTPUT_SCHEMA: list[str] = [
    # ===== 識別子パススルー（DB再インポート時のマッチングキー）=====
    "id",
    "corporate_number",        # 法人番号（国税庁）
    "company_name",            # 会社名（主カラム）
    "name",                    # 会社名（別名カラム）
    "prefecture",              # 都道府県（既存）
    # --- URL系 ---
    "url", "homepage_url", "website_url", "company_url", "contact_form_url",
    # --- 連絡先 ---
    "phone_number", "tel", "contact_phone_number", "fax", "email",
    # --- 所在地 ---
    "headquarters_address", "address", "postal_code", "zip_code", "postcode",
    # --- 代表者（公開情報のみ） ---
    "representative_name", "representative_kana", "representative_title",
    # --- 役員（HP「会社概要/役員一覧」ページから） ---
    "executive_name1", "executive_position1",
    "executive_name2", "executive_position2",
    "executive_name3", "executive_position3",
    "executive_name4", "executive_position4",
    "executive_name5", "executive_position5",
    # --- 設立・沿革 ---
    "founded_year", "founding_year", "established",
    "established_date", "establishment_date", "date_of_establishment",
    # --- 事業内容・概要 ---
    "overview", "company_description", "business_summary", "summary",
    "short_description", "description",
    "business_items", "specialties", "business_descriptions",
    # --- 従業員数 ---
    "employee_count", "employees", "number_of_employees",
    # --- 資本金 ---
    "capital_stock", "capital",
    # --- 拠点情報 ---
    "office_count", "number_of_offices",
    "factory_count", "number_of_factories",
    "store_count", "number_of_stores",
    # --- SNS/採用 ---
    "facebook", "linkedin", "wantedly", "youtrust",
    # --- メタ情報 ---
    "meta_description", "meta_keywords",
    # ===== メタカラム =====
    "hp_url_used",
    "hp_match_confidence",
    "enrichment_source",
    "enriched_at",
    "fields_filled",
]
