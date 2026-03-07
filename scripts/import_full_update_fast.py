#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
マルチヘッダーパターン対応・上書き優先インポートスクリプト

fixed_csv_3/unit_million/ および fixed_csv_3/unit_yen/ 配下の全CSVを対象にインポート。
- 財務数値: 1000倍を基本。値に「百万円」「億円」等が含まれる場合は単位に応じて計算
- 強制上書き: transaction_type, sb_flag, nda_flag, ad_flag, latest_revenue, latest_profit, capital_stock
"""

import csv
import json
import logging
import os
import re
import sys
import uuid
from decimal import Decimal
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

# 巨大CSVフィールド対応
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

# --- 設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fast_importer")

# 都道府県リスト（住所からの自動抽出用）
PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)

# =============================================================================
# カラムマッピングの網羅（Mapping Dictionary）
# 基幹: name, corporate_number, transaction_type, sb_flag, nda_flag, ad_flag, is_active
# 連絡先: postal_code, address, prefecture, phone_number, company_url
# 財務: latest_revenue, latest_profit, capital_stock, employee_count, founded_year
# 詳細: representative_name, executives, shareholders, banks, industries, overview, description
# =============================================================================
HEADER_MAP = {
    # --- 基幹・管理フラグ（最優先）---
    "法人番号": "corporate_number",
    "ID": "corporate_number",
    "会社ID": "corporate_number",
    "リストID": "corporate_number",
    "法人番号(先頭)": "corporate_number",
    "会社名": "name",
    "企業名": "name",
    "商号又は名称": "name",
    "商号又は名称かな": "kana",
    "取引種別": "transaction_type",
    "取引区分": "transaction_type",
    "SBフラグ": "sb_flag",
    "NDA": "nda_flag",
    "AD": "ad_flag",
    "状態": "status_for_active",
    "ステータス": "status_for_active",
    # --- 連絡先・所在地 ---
    "郵便番号": "postal_code",
    "会社郵便番号": "postal_code",
    "営業所郵便番号": "postal_code",
    "郵便番号(代表者用でない方)": "postal_code",
    "住所": "address",
    "会社住所": "address",
    "所在地": "address",
    "営業所所在地": "address",
    "都道府県": "prefecture",
    "電話番号": "phone_number",
    "電話番号(窓口)": "phone_number",
    "営業所電話番号": "phone_number",
    "連絡先電話番号": "phone_number",
    "FAX番号": "fax",
    "メールアドレス": "email",
    "URL": "company_url",
    "企業ホームページURL": "company_url",
    "お問い合わせURL": "company_url",
    "website_url": "company_url",
    # --- 財務・規模（100万倍対象）---
    "直近売上": "latest_revenue",
    "売上高": "latest_revenue",
    "法人＿売上高": "latest_revenue",
    "売上規模（百万円）": "latest_revenue",
    "売上高1": "latest_revenue",
    "売上高2": "latest_revenue",
    "売上高3": "latest_revenue",
    "売上高4": "latest_revenue",
    "売上高5": "latest_revenue",
    "直近利益": "latest_profit",
    "法人＿当期純利益(損失)": "latest_profit",
    "当期純利益(損失)": "latest_profit",
    "経常利益": "latest_profit",
    "営業利益": "latest_profit",
    "利益1": "latest_profit",
    "利益2": "latest_profit",
    "利益3": "latest_profit",
    "利益4": "latest_profit",
    "利益5": "latest_profit",
    "資本金": "capital_stock",
    "法人＿資本金": "capital_stock",
    "自己資本": "capital_stock",
    "社員数": "employee_count",
    "従業員数": "employee_count",
    "合計＿計": "employee_count",
    "[実績]": "employee_count",
    "設立": "founding_year",
    "創業": "founding_year",
    "設立年月日": "founding_year",
    "設立年月日(西暦)": "founding_year",
    "区分": "listing",  # 区分ヘッダーの値は 上場/非上場
    "上場": "listing",
    "上場区分": "listing",
    "上場区分（詳細）": "listing",
    # --- 代表者・役員 ---
    "代表者名": "representative_name",
    "代表者": "representative_name",
    "氏名1": "representative_name",
    "代表者住所": "representative_home_address",
    "代表者郵便番号": "representative_postal_code",
    "代表者誕生日": "representative_birth_date",
    "取締役": "executives",
    "役員": "executives",
    "役員名": "executives",
    "株主": "shareholders",
    "主要株主": "shareholders",
    "株式保有率": "shareholders",
    # --- 事業内容 ---
    "業種": "industries",
    "業種1": "industry_large",
    "業種2": "industry_middle",
    "業種3": "industry_small",
    "業種4": "industry_detail",
    "業種5": "industry_detail",
    "業種6": "industry_detail",
    "業種7": "industry_detail",
    "業種-大": "industry_large",
    "業種-中": "industry_middle",
    "業種-小": "industry_small",
    "業種-細": "industry_detail",
    "業種（大）": "industry_large",
    "業種（中）": "industry_middle",
    "業種（小）": "industry_small",
    "業種（細）": "industry_detail",
    "ジャンル": "industry_large",
    "営業種目": "industry_detail",
    "概要": "overview",
    "概況": "overview",
    "説明": "overview",
    "企業概要": "overview",
    "事業内容": "overview",
    "特意分野": "overview",
    "会社情報・備考": "company_description",
    "備考": "company_description",
    "担当者コメント": "company_description",
    "コメント": "company_description",
    "仕入れ先": "suppliers",
    "主要仕入先": "suppliers",
    "取引先": "clients",
    "主要取引先": "clients",
    "[主な取引銀行]": "banks",
    "取引先銀行": "banks",
    "主要取引銀行": "banks",
    "オフィス数": "office_count",
    "[国内の事業所]": "office_count",
    "工場数": "factory_count",
    "店舗数": "store_count",
}

# --- DBカラム一覧（INSERT/UPDATE対象）---
DB_COLS = sorted(
    {
        "name", "kana", "prefecture", "corporate_number", "transaction_type",
        "sb_flag", "nda_flag", "ad_flag", "is_active",
        "postal_code", "address", "phone_number", "fax", "email", "company_url",
        "latest_revenue", "latest_profit", "capital_stock", "employee_count",
        "founding_year", "listing",
        "representative_name", "representative_home_address",
        "representative_postal_code", "representative_birth_date",
        "industry_large", "industry_middle", "industry_small", "industry_detail",
        "overview", "company_description",
        "executives", "shareholders", "suppliers", "clients", "banks", "industries",
        "office_count", "factory_count", "store_count",
    }
)
# 配列型カラム（TEXT[]/JSONB共通。PostgreSQLへは必ず Python list で渡す。文字列は不可）
ARRAY_COLS = {"executives", "suppliers", "clients", "shareholders", "banks", "industries"}
INT_COLS = {"employee_count", "office_count", "factory_count", "store_count", "founding_year"}
BOOL_COLS = {"sb_flag", "nda_flag", "ad_flag", "is_active"}

# 強制上書き対象：CSVの値で常に更新
# 管理フラグ + 財務数値（データ修復モード：誤登録した巨大数値を正しい値で上書き）
FORCE_OVERWRITE_COLS = {
    "transaction_type", "sb_flag", "nda_flag", "ad_flag",
    "latest_revenue", "latest_profit", "capital_stock",
}

# status_for_active は is_active 判定用の中間キー（DBには保存しない）
HEADER_MAP_REV = None

# =============================================================================
# 2. インデックスベース抽出パターン
# =============================================================================
# パターン1: 文字化けファイル（6.csv〜32.csv）
# 0:name, 1:prefecture, 3:corporate_number, 6:transaction_type, 8:nda_flag, 9:ad_flag,
# 22:capital_stock, 25:latest_revenue, 26:latest_profit, 34:employee_count
INDEX_COL_MAP_PATTERN1 = {
    0: "name", 1: "prefecture", 3: "corporate_number", 6: "transaction_type",
    8: "nda_flag", 9: "ad_flag", 22: "capital_stock", 25: "latest_revenue",
    26: "latest_profit", 34: "employee_count",
}

# yuzuri簡易型（グループI）: 4:区分（上場/非上場）-> listing, 5:売上規模（百万円）
YUZURI_INDEX_MAP = {
    0: "name", 1: "address", 2: "representative_name", 3: "industry_large",
    4: "listing", 5: "latest_revenue", 6: "overview",
}

# 財務詳細（5.csv）: 32:売上高, 45:当期純利益, 25:資本金
PATTERN_5_CSV_MAP = {
    32: "latest_revenue", 45: "latest_profit", 25: "capital_stock",
}

cache_corp = {}
cache_name_pref = {}


def load_id_caches(conn):
    """法人番号・名前+都道府県のインメモリキャッシュを構築"""
    logger.info("名寄せキャッシュを構築中... (500万件超のロードには数分かかります)")
    cur = conn.cursor()
    cur.execute(
        "SELECT corporate_number, id FROM companies WHERE corporate_number IS NOT NULL"
    )
    global cache_corp
    cache_corp = dict(cur.fetchall())
    cur.execute(
        "SELECT name, prefecture, id FROM companies WHERE name IS NOT NULL AND prefecture IS NOT NULL"
    )
    global cache_name_pref
    cache_name_pref = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    logger.info(
        f"キャッシュ完了: 法人番号 {len(cache_corp)}件, 名前+都道府県 {len(cache_name_pref)}件"
    )
    cur.close()


def get_encoding(file_path):
    for enc in ["utf-8-sig", "cp932", "utf-8"]:
        try:
            with open(file_path, "r", encoding=enc) as f:
                f.readline()
                return enc
        except Exception:
            continue
    return "utf-8"


def normalize_header(raw):
    """ヘッダー正規化（全角・改行・空白除去）"""
    if not raw:
        return ""
    s = str(raw).strip().strip("\ufeff").replace("\n", " ").replace("\r", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)


def _build_header_map_reverse():
    """DBカラム -> [正規化済みCSV項目名...] の逆引き"""
    rev = {}
    for csv_h, db_col in HEADER_MAP.items():
        if db_col == "status_for_active":
            continue
        n = normalize_header(csv_h)
        if db_col not in rev:
            rev[db_col] = []
        if n and n not in rev[db_col]:
            rev[db_col].append(n)
    return rev


def normalize_val(val):
    if val is None:
        return None
    s = str(val).strip()
    return s if s.lower() not in ["", "nan", "none", "null"] else None


def normalize_array_value(val):
    """
    配列型データを正規化。必ず Python list または None を返す。
    文字列 ["\\u98ef..."] は json.loads() で list に変換（Unicodeデコード済み）。
    文字列のまま psycopg2 に渡すと InvalidTextRepresentation が発生する。
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s or s in ("[]", "nan", "none", "null") or s.lower() in ("nan", "none", "null"):
        return None

    # CSV囲み引用符を除去（繰り返し適用）
    while len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        s = s[1:-1].strip()
        if not s:
            return None

    # [ で始まり ] で終わる場合：json.loads で list に変換
    if s.startswith("[") and s.endswith("]"):
        try:
            lst = json.loads(s)
            if isinstance(lst, list):
                result = [str(x).strip() for x in lst if x is not None and str(x).strip()]
                return result if result else None
            return None
        except (json.JSONDecodeError, TypeError):
            pass
        # 失敗時：[...] の部分を抽出して再試行
        m = re.search(r"\[.*\]", s, re.DOTALL)
        if m:
            try:
                lst = json.loads(m.group(0))
                if isinstance(lst, list):
                    result = [str(x).strip() for x in lst if x is not None and str(x).strip()]
                    return result if result else None
            except (json.JSONDecodeError, TypeError):
                pass

    # カンマ区切り
    result = [x.strip() for x in s.split(",") if x.strip()]
    return result if result else None


def resolve_current_map(headers):
    """ヘッダーからDBカラムへのマッピング（正規化ヘッダー＋生ヘッダー）"""
    global HEADER_MAP_REV
    if HEADER_MAP_REV is None:
        HEADER_MAP_REV = _build_header_map_reverse()
    norm_to_idx = {normalize_header(h): i for i, h in enumerate(headers)}
    current = {}
    for db_col, aliases in HEADER_MAP_REV.items():
        for alias in aliases:
            if alias in norm_to_idx:
                current[norm_to_idx[alias]] = db_col
                break
    for i, h in enumerate(headers):
        if i not in current and h.strip() in HEADER_MAP:
            current[i] = HEADER_MAP[h.strip()]
    return current


# すべての計算終了後の物理限界（900京超は必ず None）
BIGINT_SAFE_MAX = 9_000_000_000_000_000_000
INPUT_CAP = 100_000_000_000_000  # 入力の異常値上限


def parse_revenue_profit(val, file_path=None, apply_1000x=True):
    """
    財務数値のパース。多重変換防止: 「百万円」「億円」等がある場合は
    その単位のみ適用し、1000倍は絶対に適用しない。
    """
    if not val or str(val).lower() == "nan":
        return None
    s_val = str(val).replace(",", "").strip().replace("　", "").replace(" ", "")
    if not s_val:
        return None

    unit_factor = 1
    has_unit = False

    # 単位の判定（百万円・億円がある場合は 1000倍 は適用しない）
    if "億円" in s_val or "億" in s_val:
        unit_factor = 100_000_000
        s_val = re.sub(r"億円?|円", "", s_val)
        has_unit = True
    elif "百万円" in s_val or "百万" in s_val:
        unit_factor = 1_000_000
        s_val = re.sub(r"百万円?|円", "", s_val)
        has_unit = True
    elif ("万円" in s_val or "万" in s_val) and "百万" not in s_val:
        unit_factor = 10_000
        s_val = re.sub(r"万円?|円", "", s_val)
        has_unit = True

    try:
        num_part = re.sub(r"[^\d\.\-eE+]", "", s_val)
        if not num_part:
            return None
        f_val = float(num_part)
        if abs(f_val) > INPUT_CAP:
            return None

        # 単位指定がある場合はそれのみ、ない場合のみ 1000倍
        final_factor = unit_factor if has_unit else (1000 if apply_1000x else 1)
        result = int(f_val * final_factor)

        # 【最終防衛線】すべての倍率計算が終わった直後に必ずチェック
        if abs(result) > BIGINT_SAFE_MAX:
            return None
        return result
    except Exception:
        return None


# エイリアス（parse_financial_value 相当）
parse_financial_value = parse_revenue_profit


def parse_int_safe(val):
    """整数化（7.0 -> 7）"""
    s = normalize_val(val)
    if not s:
        return None
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return int(float(s))
    except (ValueError, OverflowError):
        return None


def parse_bool(val):
    """○、1、あり、Trueなどの値をTrueに、それ以外をFalseに変換"""
    s = normalize_val(val)
    if not s:
        return None
    s_lower = s.lower()
    if s_lower in ("1", "true", "yes", "y", "○", "〇", "あり", "済"):
        return True
    return False


def parse_year(val):
    """文字列から最初の4桁（西暦）を抽出して整数で返す"""
    s = normalize_val(val)
    if not s:
        return None
    match = re.search(r"(\d{4})", s)
    return int(match.group(1)) if match else None


def is_active_logic(status):
    """状態・ステータス列に「解散」「廃業」「休止」「清算」が含まれる場合はFalse、それ以外はTrue"""
    s = normalize_val(status)
    if not s:
        return None
    s_lower = s.lower()
    if any(kw in s_lower for kw in ("解散", "廃業", "休止", "清算")):
        return False
    return True


def extract_prefecture(address):
    """都道府県列が空の場合、住所文字列の先頭から都道府県を抽出する"""
    if not address:
        return None
    s = str(address).strip()
    for pref in PREFECTURES:
        if pref in s:
            return pref
    return None


# transaction_type の期待キーワード（これらを含む値は有効とみなす）
TRANSACTION_TYPE_KEYWORDS = ("譲受", "譲渡", "提携")


def cleanse_transaction_type(val):
    """
    transaction_type の文字化け除去。
    - 置換文字(U+FFFD)・不正バイト列を除去
    - 期待キーワード（譲受・譲渡・提携）を含まない、または記号のみの場合は '未設定' を返す
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    # 置換文字除去
    s = s.replace("\ufffd", "")
    # cp932 で再エンコードして不正バイトを除去
    try:
        s = s.encode("cp932", errors="ignore").decode("cp932")
    except Exception:
        pass
    s = s.strip()
    if not s:
        return "未設定"
    # 期待キーワードを含まない場合は '未設定'
    if not any(kw in s for kw in TRANSACTION_TYPE_KEYWORDS):
        return "未設定"
    # 記号のみ（漢字・かな・英字が残っていない）の場合は '未設定'
    meaningful = re.sub(r"[^\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ffa-zA-Z]", "", s)
    if not meaningful:
        return "未設定"
    return s


def normalize_corp_num(val):
    """指数表記（E+）を解除し、13桁の文字列として正規化する"""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ["nan", "none", "null"]:
        return None
    if "e" in s.lower():
        try:
            d = Decimal(s)
            s = str(int(d))
        except Exception:
            try:
                s = f"{float(s):.0f}"
            except Exception:
                return None
    s = re.sub(r"[^\d]", "", s)
    if len(s) == 12:
        s = "0" + s
    return s if len(s) == 13 and s.isdigit() else None




def detect_file_pattern(file_path, headers, col_map):
    """ファイルパターン判定: yuzuri / 5.csv / 文字化け"""
    name = file_path.name.lower()
    if "yuzuri" in name or detect_yuzuri_type(headers):
        return "yuzuri"
    if name == "5.csv" or "5.fixed" in name:
        return "pattern_5"
    if len(col_map) < 4 and len(headers) >= 30:
        return "pattern1"
    return "normal"


def detect_yuzuri_type(headers):
    """「売上規模（百万円）」を検知"""
    for h in headers or []:
        n = normalize_header(h)
        if "売上規模" in n and "百万円" in n:
            return True
    return False


def apply_value(data, db_col, raw_val, file_path=None):
    """1カラム分の値をパースして data にセット"""
    if db_col == "status_for_active":
        data["is_active"] = is_active_logic(raw_val)
        return
    if db_col in ["latest_revenue", "latest_profit", "capital_stock"]:
        if not raw_val:
            data[db_col] = None
        else:
            data[db_col] = parse_revenue_profit(raw_val, file_path=file_path)
    elif db_col == "corporate_number":
        data[db_col] = normalize_corp_num(raw_val)
    elif db_col in INT_COLS:
        data[db_col] = parse_int_safe(raw_val)
    elif db_col in BOOL_COLS:
        if db_col == "is_active":
            data[db_col] = is_active_logic(raw_val)
        else:
            data[db_col] = parse_bool(raw_val)
    elif db_col == "founding_year":
        data[db_col] = parse_year(raw_val)
    elif db_col in ARRAY_COLS:
        data[db_col] = normalize_array_value(raw_val)
    elif db_col == "transaction_type":
        data[db_col] = cleanse_transaction_type(raw_val)
    elif db_col in ("industry_large", "industry_middle", "industry_small", "industry_detail"):
        data[db_col] = normalize_val(raw_val)
    else:
        data[db_col] = normalize_val(raw_val)


def apply_yuzuri_value(data, db_col, raw_val, file_path=None):
    """yuzuri型: 数値パースは parse_revenue_profit に委譲"""
    apply_value(data, db_col, raw_val, file_path=file_path)


def upsert_batch(conn, batch):
    """
    二段構えの名寄せ + カラム別更新ルール。
    - 強制上書き（transaction_type, sb_flag, nda_flag, ad_flag）: 常にCSVの値で更新
    - 条件付き更新（その他）: DBがNULLの場合のみCSVの値を補完
    新規企業は INSERT。バッチ内の重複は後勝ちで排除。
    """
    cur = conn.cursor()

    for row in batch:
        row["id"] = None
        if row.get("corporate_number"):
            row["id"] = cache_corp.get(row["corporate_number"])
        if not row["id"] and row.get("name"):
            pref = row.get("prefecture") or extract_prefecture(row.get("address"))
            if pref:
                row["id"] = cache_name_pref.get((row["name"], pref))
            if not row["id"] and row.get("prefecture"):
                row["id"] = cache_name_pref.get((row["name"], row["prefecture"]))

    update_map = {}
    insert_map = {}
    for row in batch:
        if row["id"]:
            update_map[row["id"]] = row
        else:
            key = (row.get("name") or "", row.get("prefecture") or "")
            if key[0]:
                insert_map[key] = row

    updates = list(update_map.values())
    inserts = list(insert_map.values())

    cols_to_update = [c for c in DB_COLS if c not in ("corporate_number", "status_for_active")]
    # 強制上書き: EXCLUDED をそのまま代入 / 条件付き: COALESCE(companies.col, EXCLUDED.col)
    set_clauses = []
    for c in cols_to_update:
        if c in FORCE_OVERWRITE_COLS:
            set_clauses.append(f"{c} = EXCLUDED.{c}")
        else:
            set_clauses.append(f"{c} = COALESCE(companies.{c}, EXCLUDED.{c})")
    set_clauses.append("updated_at = NOW()")

    def _normalize_row_arrays(row):
        """配列型カラムをバッチ処理直前に正規化（文字列→list、[ ] を除去）"""
        for col in ARRAY_COLS:
            v = row.get(col)
            if v is not None and not isinstance(v, list):
                row[col] = normalize_array_value(v)
        return row

    def _wrap_val(v, col):
        """配列型は必ず Python list または None で渡す。文字列は絶対に渡さない。"""
        if col in ARRAY_COLS:
            if v is None:
                return None
            if isinstance(v, list):
                return v
            # 文字列の場合は必ず json.loads 等で list に変換
            normalized = normalize_array_value(v)
            return normalized  # list or None
        return v

    # 配列型の最終正規化（文字列が残っていないことを保証）
    for r in updates:
        _normalize_row_arrays(r)
    for r in inserts:
        _normalize_row_arrays(r)

    if updates:
        vals = [
            [r["id"]] + [_wrap_val(r.get(c), c) for c in cols_to_update]
            for r in updates
        ]
        query = f"""INSERT INTO companies (id, {", ".join(cols_to_update)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET {", ".join(set_clauses)}"""
        execute_values(cur, query, vals)

    if inserts:
        for row in inserts:
            row["id"] = row.get("corporate_number") or str(uuid.uuid4())
        insert_cols = ["id"] + cols_to_update
        vals = [
            [r["id"]] + [_wrap_val(r.get(c), c) for c in cols_to_update]
            for r in inserts
        ]
        query = f"""INSERT INTO companies ({", ".join(insert_cols)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET {", ".join(set_clauses)}"""
        execute_values(cur, query, vals)

    conn.commit()
    cur.close()


def process_file(conn, file_path):
    enc = get_encoding(file_path)
    rel_path = f"{file_path.parent.name}/{file_path.name}"
    logger.info(f"処理開始: {rel_path} [財務: 1000倍+単位文字列対応]")
    with open(file_path, "r", encoding=enc, errors="replace") as f:
        reader = csv.reader(f)
        headers = next(reader, None)
        if not headers:
            return

        col_map = resolve_current_map(headers)
        file_pattern = detect_file_pattern(file_path, headers, col_map)

        if file_pattern == "pattern1":
            logger.info(f"  インデックスベース抽出（パターン1・38列）")
        elif file_pattern == "yuzuri":
            logger.info(f"  yuzuri簡易型")
        elif file_pattern == "pattern_5":
            logger.info(f"  財務詳細（5.csv）")

        index_map = INDEX_COL_MAP_PATTERN1
        if file_pattern == "yuzuri":
            index_map = YUZURI_INDEX_MAP
        elif file_pattern == "pattern_5":
            index_map = PATTERN_5_CSV_MAP

        use_index = file_pattern in ("pattern1", "yuzuri")
        use_pattern5 = file_pattern == "pattern_5"

        batch = []
        for row in reader:
            if len(row) < 2:
                continue
            data = {c: None for c in DB_COLS}
            data["status_for_active"] = None

            if use_index:
                for idx, db_col in index_map.items():
                    if idx < len(row):
                        if file_pattern == "yuzuri":
                            apply_yuzuri_value(data, db_col, row[idx], file_path)
                        else:
                            apply_value(data, db_col, row[idx], file_path)
            else:
                for col_idx, db_col in col_map.items():
                    if col_idx < len(row):
                        apply_value(data, db_col, row[col_idx], file_path)

            if use_pattern5:
                for idx, db_col in PATTERN_5_CSV_MAP.items():
                    if idx < len(row) and row[idx]:
                        apply_value(data, db_col, row[idx], file_path)

            if not data.get("prefecture") and data.get("address"):
                data["prefecture"] = extract_prefecture(data["address"])

            if data.get("name"):
                batch.append(data)

            if len(batch) >= 1000:
                upsert_batch(conn, batch)
                batch = []

        if batch:
            upsert_batch(conn, batch)


def main():
    # スクリプト位置基準で fixed_csv_3 を解決（cwd に依存しない）
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    target_dir = project_root / "fixed_csv_3"
    if not target_dir.exists():
        logger.error(f"ディレクトリが存在しません: {target_dir}")
        sys.exit(1)
    # fixed_csv_3 直下や later フォルダを無視し、unit_million / unit_yen のみ対象
    sub_dirs = ["unit_million", "unit_yen"]
    csv_files = []
    for sub in sub_dirs:
        target_path = target_dir / sub
        if target_path.exists():
            csv_files.extend(target_path.glob("*.csv"))
    csv_files = sorted(csv_files)
    logger.info(f"対象CSV: {len(csv_files)}件")
    logger.info("管理フラグ(sb_flag, nda_flag, ad_flag)を使用する場合は backend/sql/add_management_flags.sql を事前実行してください")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )
    try:
        load_id_caches(conn)
        for fp in csv_files:
            process_file(conn, fp)
    finally:
        conn.close()

    logger.info("インポート完了")


if __name__ == "__main__":
    main()
