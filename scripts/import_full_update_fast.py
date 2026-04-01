#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
マルチヘッダーパターン対応・上書き優先インポートスクリプト
※検証用: 指定した特定のCSVファイルのみをインポートするように制限中

設立列は csv.reader により常に文字列として受け取り、日付は backend.api.csv_founding_date で解釈する
（数値推論なし）。pandas を使う場合は merge_pandas_dtype_str_for_founding を参照。
"""

import csv
import hashlib
import json
import logging
import os
import re
import sys
from datetime import date as date_type
from decimal import Decimal
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from backend.api.csv_founding_date import (
    parse_birth_cell_to_date,
    parse_founding_cell_to_date,
    parse_year_from_founding_cell,
)

import psycopg2
from psycopg2.extras import Json, execute_values

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD") or ""
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fast_importer")

PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)

HEADER_MAP = {
    "法人番号": "corporate_number", "ID": "corporate_number", "会社ID": "corporate_number", "リストID": "corporate_number", "法人番号(先頭)": "corporate_number",
    "会社名": "name", "企業名": "name", "商号又は名称": "name", "商号又は名称かな": "kana",
    "取引種別": "transaction_type", "取引区分": "transaction_type",
    "SBフラグ": "sb_flag", "NDA": "nda_flag", "NDA締結": "nda_flag", "ＮＤＡ": "nda_flag", "AD": "ad_flag",
    "状態": "status_for_active", "ステータス": "status_for_active",
    "郵便番号": "postal_code", "会社郵便番号": "postal_code", "営業所郵便番号": "postal_code", "郵便番号(代表者用でない方)": "postal_code",
    "住所": "address", "会社住所": "address", "所在地": "address", "営業所所在地": "address", "都道府県": "prefecture",
    "電話番号": "phone_number", "電話番号(窓口)": "phone_number", "営業所電話番号": "phone_number", "連絡先電話番号": "phone_number",
    "FAX番号": "fax", "メールアドレス": "email",
    "URL": "company_url", "企業ホームページURL": "company_url", "お問い合わせURL": "company_url", "website_url": "company_url",
    "直近売上": "latest_revenue", "売上高": "latest_revenue", "法人＿売上高": "latest_revenue", "売上規模（百万円）": "latest_revenue",
    "売上高1": "latest_revenue", "売上高2": "latest_revenue", "売上高3": "latest_revenue", "売上高4": "latest_revenue", "売上高5": "latest_revenue",
    "直近決算年月": "latest_fiscal_year_month", "決算年月": "latest_fiscal_year_month", "最新決算年月": "latest_fiscal_year_month",
    "直近利益": "latest_profit", "直近純利益": "latest_profit", "純利益": "latest_profit", "法人＿当期純利益(損失)": "latest_profit",
    "当期純利益(損失)": "latest_profit", "経常利益": "latest_profit", "営業利益": "latest_profit",
    "利益1": "latest_profit", "利益2": "latest_profit", "利益3": "latest_profit", "利益4": "latest_profit", "利益5": "latest_profit",
    "資本金": "capital_stock", "法人＿資本金": "capital_stock", "自己資本": "capital_stock",
    "社員数": "employee_count", "従業員数": "employee_count", "合計＿計": "employee_count", "[実績]": "employee_count",
    "設立": "founding_year", "設立年月日": "founding_year", "設立年月日(西暦)": "founding_year", # 「創業」を削除（J列処理で事業詳細に回すため）
    "区分": "listing", "上場": "listing", "上場区分": "listing", "上場区分（詳細）": "listing",
    "代表者名": "representative_name", "代表者": "representative_name", "氏名1": "representative_name",
    "代表者住所": "representative_home_address", "代表者郵便番号": "representative_postal_code", "代表者誕生日": "representative_birth_date",
    "取締役": "executives", "役員": "executives", "役員名": "executives",
    "株主": "shareholders", "主要株主": "shareholders", "株式保有率": "shareholders",
    "業種": "industries", "業種1": "industry_large", "業種2": "industry_middle", "業種3": "industry_small", "業種4": "industry_detail",
    "業種5": "industry_detail", "業種6": "industry_detail", "業種7": "industry_detail",
    "業種-大": "industry_large", "業種-中": "industry_middle", "業種-小": "industry_small", "業種-細": "industry_detail",
    "業種（大）": "industry_large", "業種（中）": "industry_middle", "業種（小）": "industry_small", "業種（細）": "industry_detail",
    "ジャンル": "industry_large", "営業種目": "industry_detail",
    "概要": "overview", "概況": "overview", "説明": "overview", "企業概要": "overview", "事業内容": "overview", "事業詳細": "business_summary", "特意分野": "overview",
    "会社情報・備考": "company_description", "備考": "company_description", "担当者コメント": "company_description", "コメント": "company_description",
    "仕入れ先": "suppliers", "仕入先": "suppliers", "主要仕入先": "suppliers", "主要仕入れ先": "suppliers",
    "取引先": "clients", "主要取引先": "clients",
    "[主な取引銀行]": "banks", "取引先銀行": "banks", "主要取引銀行": "banks",
    "オフィス数": "office_count", "[国内の事業所]": "office_count",
    "工場数": "factory_count", "店舗数": "store_count",
}

DB_COLS = sorted({
    "name", "kana", "prefecture", "corporate_number", "transaction_type",
    "sb_flag", "nda_flag", "ad_flag", "is_active",
    "postal_code", "address", "phone_number", "fax", "email", "company_url",
    "latest_revenue", "latest_profit", "capital_stock", "employee_count",
    "latest_fiscal_year_month",
    "founding_year", "founding", "listing",
    "representative_name", "representative_home_address",
    "representative_postal_code", "representative_birth_date",
    "industry_large", "industry_middle", "industry_small", "industry_detail",
    "overview", "company_description", "business_summary",
    "executives", "shareholders", "suppliers", "clients", "banks", "industries",
    "office_count", "factory_count", "store_count",
})

_list_cols_jsonb = (
    os.getenv("IMPORT_LIST_COLS_AS_JSONB", "").strip().lower() in ("1", "true", "yes")
    or os.getenv("IMPORT_BANKS_AS_JSONB", "").strip().lower() in ("1", "true", "yes")
)

PG_TEXT_ARRAY_COLS_BASE = {"industries", "banks"}
PG_TEXT_ARRAY_COLS = PG_TEXT_ARRAY_COLS_BASE | {"executives", "suppliers", "clients", "shareholders"} if not _list_cols_jsonb else PG_TEXT_ARRAY_COLS_BASE
JSONB_LIST_COLS = {"executives", "suppliers", "clients", "shareholders"} if _list_cols_jsonb else set()
ARRAY_COLS = PG_TEXT_ARRAY_COLS | JSONB_LIST_COLS
INT_COLS = {"employee_count", "office_count", "factory_count", "store_count", "founding_year"}
BOOL_COLS = {"sb_flag", "nda_flag", "ad_flag", "is_active"}
DATE_COLS = {"founding", "representative_birth_date"}

HEADER_MAP_REV = None

INDEX_COL_MAP_PATTERN1 = {
    0: "name", 1: "prefecture", 3: "corporate_number", 6: "transaction_type",
    8: "nda_flag", 9: "ad_flag", 22: "capital_stock", 25: "latest_revenue",
    26: "latest_profit", 34: "employee_count",
}

PATTERN1_38_COL_MAP = {
    0: "name", 1: "prefecture", 2: "representative_name", 3: "corporate_number",
    6: "transaction_type", 7: "status_for_active", 8: "nda_flag", 9: "ad_flag",
    10: "company_description", 11: "company_url", 12: "industry_large",
    13: "industry_middle", 14: "industry_small", 15: "postal_code",
    16: "address", 17: "founding_year", 18: "phone_number",
    19: "representative_postal_code", 20: "representative_home_address",
    21: "representative_birth_date", 22: "capital_stock", 23: "listing",
    24: "latest_fiscal_year_month", 25: "latest_revenue", 26: "latest_profit",
    27: "overview", 28: "overview", 29: "suppliers", 30: "clients",
    31: "banks", 32: "executives", 33: "shareholders", 34: "employee_count",
    35: "office_count", 36: "factory_count", 37: "store_count",
}

YUZURI_INDEX_MAP = {
    0: "name", 1: "address", 2: "representative_name", 3: "industry_large",
    4: "listing", 5: "latest_revenue", 6: "overview",
}

PATTERN_5_CSV_MAP = {
    32: "latest_revenue", 45: "latest_profit", 25: "capital_stock",
}

IMPORT_FIRSTTIME_INDEX_MAP_BASE = {
    0: "name", 1: "phone_number", 2: "postal_code", 3: "address",
    4: "company_url", 5: "representative_name", 6: "representative_postal_code",
    7: "representative_home_address", 11: "shareholders", 12: "executives",
    13: "overview", 14: "industry_large", 15: "industry_detail",
    16: "industry_middle", 17: "industry_small",
}

def import_firsttime_founding_column_index(headers) -> int:
    target = normalize_header("設立")
    for i, h in enumerate(headers or []):
        if normalize_header(h) == target:
            return i
    return 10

def build_import_firsttime_index_map(headers) -> dict:
    m = dict(IMPORT_FIRSTTIME_INDEX_MAP_BASE)
    m[import_firsttime_founding_column_index(headers)] = "founding_year"
    return m

cache_corp = {}
cache_name_pref = {}

def load_id_caches(conn):
    logger.info("名寄せキャッシュを構築中...")
    cur = conn.cursor()
    cur.execute("SELECT corporate_number, id FROM companies WHERE corporate_number IS NOT NULL")
    global cache_corp
    cache_corp = dict(cur.fetchall())
    cur.execute("SELECT name, COALESCE(NULLIF(TRIM(prefecture), ''), '') AS pref, id FROM companies WHERE name IS NOT NULL")
    global cache_name_pref
    cache_name_pref = {(r[0], r[1]): r[2] for r in cur.fetchall()}
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
    if not raw: return ""
    s = str(raw).strip().strip("\ufeff").replace("\n", " ").replace("\r", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)

def _build_header_map_reverse():
    rev = {}
    for csv_h, db_col in HEADER_MAP.items():
        if db_col == "status_for_active": continue
        n = normalize_header(csv_h)
        if db_col not in rev: rev[db_col] = []
        if n and n not in rev[db_col]: rev[db_col].append(n)
    return rev

def normalize_val(val):
    if val is None: return None
    s = str(val).strip()
    return s if s.lower() not in ["", "nan", "none", "null"] else None

def is_blank_csv_cell(val) -> bool:
    if val is None: return True
    if isinstance(val, (int, float)) and not isinstance(val, bool): return False
    s = str(val).strip()
    if not s: return True
    return s.lower() in ("nan", "none", "null")

def normalize_array_value(val):
    if val is None: return None
    s = str(val).strip()
    if not s or s in ("[]", "nan", "none", "null") or s.lower() in ("nan", "none", "null"): return None
    while len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        s = s[1:-1].strip()
        if not s: return None
    if s.startswith("[") and s.endswith("]"):
        try:
            lst = json.loads(s)
            if isinstance(lst, list):
                result = [str(x).strip() for x in lst if x is not None and str(x).strip()]
                return result if result else None
            return None
        except (json.JSONDecodeError, TypeError): pass
        m = re.search(r"\[.*\]", s, re.DOTALL)
        if m:
            try:
                lst = json.loads(m.group(0))
                if isinstance(lst, list):
                    result = [str(x).strip() for x in lst if x is not None and str(x).strip()]
                    return result if result else None
            except (json.JSONDecodeError, TypeError): pass
    for sep in ("\n", "、", "，", ";", "；"):
        s = s.replace(sep, ",")
    result = [x.strip() for x in s.split(",") if x.strip()]
    return result if result else None

def _unwrap_json_array_string_list(val):
    if not isinstance(val, list) or len(val) != 1: return val
    s = val[0]
    if not isinstance(s, str): return val
    t = s.strip()
    if not (t.startswith("[") and t.endswith("]")): return val
    inner = normalize_array_value(s)
    return inner if inner is not None else val

def resolve_current_map(headers):
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

BIGINT_SAFE_MAX = 9_000_000_000_000_000_000
INPUT_CAP = 100_000_000_000_000

def parse_revenue_profit(val, file_path=None, revenue_folder="unit_yen"):
    if not val or str(val).lower() == "nan": return None
    s_val = str(val).replace(",", "").strip().replace("　", "").replace(" ", "")
    if not s_val: return None
    unit_factor = 1
    has_unit = False
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
        if not num_part: return None
        f_val = float(num_part)
        if abs(f_val) > INPUT_CAP: return None
        if has_unit:
            final_factor = unit_factor
        elif revenue_folder == "unit_million":
            final_factor = 1_000_000
        else:
            final_factor = 1000
        result = int(f_val * final_factor)
        if abs(result) > BIGINT_SAFE_MAX: return None
        return result
    except Exception: return None

def parse_capital_stock(val, file_path=None):
    return parse_revenue_profit(val, file_path=file_path, revenue_folder="unit_yen")

parse_financial_value = parse_revenue_profit

def parse_int_safe(val):
    s = normalize_val(val)
    if not s: return None
    s = re.sub(r"[^\d\.\-]", "", s)
    try: return int(float(s))
    except (ValueError, OverflowError): return None

def parse_bool(val):
    s = normalize_val(val)
    if not s: return None
    s_lower = s.lower()
    if "未締結" in s_lower or "未契約" in s_lower: return False
    if s_lower in ("1", "true", "yes", "y", "○", "〇", "あり", "済"): return True
    if "締結済" in s_lower or "契約済" in s_lower or "済み" in s_lower: return True
    if "締結" in s_lower and "未" not in s_lower: return True
    return False

def parse_year(val):
    s = normalize_val(val)
    if not s: return None
    if re.fullmatch(r"\d{5,}", s): return None
    match = re.search(r"(\d{4})", s)
    return int(match.group(1)) if match else None

# 設立・誕生日のパースは backend.api.csv_founding_date（Excel シリアルを設立に使わない）

def normalize_latest_fiscal(val):
    s = normalize_val(val)
    if not s: return None
    # 「2023年8月1日」等から年月だけを抽出して保存
    m = re.search(r"(\d{4})年(\d{1,2})月", s)
    if m:
        return f"{m.group(1)}年{m.group(2)}月"
    return s[:10] if len(s) > 10 else s

def prefecture_key_for_row(row: dict) -> str:
    p = row.get("prefecture") or extract_prefecture(row.get("address")) or ""
    return (p or "").strip()

def deterministic_id_name_pref(name: str, pref: str) -> str:
    raw = f"{(name or '').strip()}\x00{(pref or '').strip()}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"np{h[:32]}"

def postal_jp_ok(p: str) -> bool:
    if not p: return True
    return bool(re.match(r"^\d{3}-\d{4}$", p) or (len(re.sub(r"\D", "", p)) == 7))

def should_skip_corrupt_row(row: list[str], headers: list[str], file_pattern: str, lineno: int, rel_path: str) -> tuple[bool, str]:
    if len(row) != len(headers): return True, "column_count_mismatch"
    for i, h in enumerate(headers):
        if h.strip() == "法人番号":
            if i < len(row):
                cn = normalize_val(row[i])
                if cn and normalize_corp_num(cn) is None: return True, "invalid_corporate_number"
            break
    if file_pattern not in ("import_firsttime", "pattern1", "pattern1_38", "yuzuri", "pattern_5"):
        for i, h in enumerate(headers):
            if h.strip() == "郵便番号" and i < len(row):
                p = normalize_val(row[i])
                if p and (not postal_jp_ok(p)) and len(p) > 15 and any(x in p for x in ("県", "都", "府", "市", "区", "町", "村", "丁目")):
                    return True, "postal_field_looks_like_address"
                break
    return False, ""

def append_import_skip_log(rel_path: str, lineno: int, reason: str, snippet: str):
    env_log = os.getenv("IMPORT_SKIP_LOG")
    if env_log: log_path = Path(env_log)
    else:
        script_dir = Path(__file__).resolve().parent
        log_path = script_dir.parent / "reports" / "import_skip_log.jsonl"
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        rec = json.dumps({"file": rel_path, "line": lineno, "reason": reason, "snippet": snippet[:200]}, ensure_ascii=False)
        with open(log_path, "a", encoding="utf-8") as lf: lf.write(rec + "\n")
    except OSError as e: logger.warning(f"スキップログ書き込み失敗: {e}")

def is_active_logic(status):
    s = normalize_val(status)
    if not s: return None
    s_lower = s.lower()
    if any(kw in s_lower for kw in ("解散", "廃業", "休止", "清算")): return False
    return True

def extract_prefecture(address):
    if not address: return None
    s = str(address).strip()
    for pref in PREFECTURES:
        if pref in s: return pref
    return None

TRANSACTION_TYPE_KEYWORDS = ("譲受", "譲渡", "提携")

def cleanse_transaction_type(val):
    if val is None: return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"): return None
    s = s.replace("\ufffd", "")
    try: s = s.encode("cp932", errors="ignore").decode("cp932")
    except Exception: pass
    s = s.strip()
    if not s: return "未設定"
    if not any(kw in s for kw in TRANSACTION_TYPE_KEYWORDS): return "未設定"
    meaningful = re.sub(r"[^\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ffa-zA-Z]", "", s)
    if not meaningful: return "未設定"
    return s

def normalize_corp_num(val):
    if val is None: return None
    s = str(val).strip()
    if not s or s.lower() in ["nan", "none", "null"]: return None
    if "e" in s.lower():
        try:
            d = Decimal(s)
            s = str(int(d))
        except Exception:
            try: s = f"{float(s):.0f}"
            except Exception: return None
    s = re.sub(r"[^\d]", "", s)
    if len(s) == 12: s = "0" + s
    return s if len(s) == 13 and s.isdigit() else None

def detect_file_pattern(file_path, headers, col_map):
    name = file_path.name.lower()
    if "import_firsttime" in name: return "import_firsttime"
    if "yuzuri" in name or detect_yuzuri_type(headers): return "yuzuri"
    if name == "5.csv" or "5.fixed" in name: return "pattern_5"
    if len(col_map) < 4 and len(headers) >= 30:
        if len(headers) == 38: return "pattern1_38"
        return "pattern1"
    return "normal"

def detect_yuzuri_type(headers):
    for h in headers or []:
        n = normalize_header(h)
        if "売上規模" in n and "百万円" in n: return True
    return False

def resolve_row_name_for_log(row, col_map, use_index, index_map):
    """設立パース失敗ログ用の会社名（取れるときだけ）。"""
    if use_index and row is not None and index_map.get(0) == "name" and len(row) > 0:
        return normalize_val(row[0])
    for idx, dcol in sorted(col_map.items()):
        if dcol == "name" and idx < len(row):
            return normalize_val(row[idx])
    return None

def apply_value(data, db_col, raw_val, file_path=None, revenue_folder="unit_yen", log_context=None):
    if db_col == "status_for_active":
        data["is_active"] = is_active_logic(raw_val)
        return
    if db_col in ["latest_revenue", "latest_profit"]:
        if is_blank_csv_cell(raw_val): return
        parsed = parse_revenue_profit(raw_val, file_path=file_path, revenue_folder=revenue_folder)
        if parsed is not None: data[db_col] = parsed
        return
    if db_col == "capital_stock":
        if is_blank_csv_cell(raw_val): return
        parsed = parse_capital_stock(raw_val, file_path=file_path)
        if parsed is not None: data[db_col] = parsed
        return
    elif db_col == "corporate_number":
        data[db_col] = normalize_corp_num(raw_val)
    elif db_col == "latest_fiscal_year_month":
        data[db_col] = normalize_latest_fiscal(raw_val)
    elif db_col == "representative_birth_date":
        data[db_col] = parse_birth_cell_to_date(raw_val)
    elif db_col == "founding_year":
        if is_blank_csv_cell(raw_val):
            return
        d = parse_founding_cell_to_date(raw_val)
        if d:
            data["founding"] = d
            data["founding_year"] = d.year
        else:
            y = parse_year_from_founding_cell(raw_val) or parse_year(raw_val)
            if y is not None:
                data["founding_year"] = y
            elif log_context and log_context.get("lineno") is not None:
                raw_s = normalize_val(raw_val)
                if raw_s:
                    logger.warning(
                        "設立日をパースできずスキップ（日付・年とも取れず）: path=%s line=%s company=%r value=%r",
                        log_context.get("rel_path"),
                        log_context["lineno"],
                        log_context.get("name"),
                        raw_s,
                    )
    elif db_col in INT_COLS:
        data[db_col] = parse_int_safe(raw_val)
    elif db_col in BOOL_COLS:
        if db_col == "is_active": data[db_col] = is_active_logic(raw_val)
        else: data[db_col] = parse_bool(raw_val)
    elif db_col in ARRAY_COLS:
        data[db_col] = normalize_array_value(raw_val)
    elif db_col == "transaction_type":
        data[db_col] = cleanse_transaction_type(raw_val)
    elif db_col in ("industry_large", "industry_middle", "industry_small", "industry_detail"):
        data[db_col] = normalize_val(raw_val)
    else:
        data[db_col] = normalize_val(raw_val)

def apply_yuzuri_value(data, db_col, raw_val, file_path=None, revenue_folder="unit_yen", log_context=None):
    apply_value(data, db_col, raw_val, file_path=file_path, revenue_folder=revenue_folder, log_context=log_context)

def upsert_batch(conn, batch):
    cur = conn.cursor()
    for row in batch:
        row["id"] = None
        if row.get("corporate_number"): row["id"] = cache_corp.get(row["corporate_number"])
        pref_k = prefecture_key_for_row(row)
        if not row["id"] and row.get("name"): row["id"] = cache_name_pref.get((row["name"], pref_k))
    update_map = {}
    insert_map = {}
    for row in batch:
        if row["id"]: update_map[row["id"]] = row
        else:
            nm = row.get("name") or ""
            key = (nm, prefecture_key_for_row(row))
            if nm: insert_map[key] = row
    updates = list(update_map.values())
    inserts = list(insert_map.values())
    cols_to_update = [c for c in DB_COLS if c not in ("corporate_number", "status_for_active")]
    set_clauses = [f"{c} = COALESCE(EXCLUDED.{c}, companies.{c})" for c in cols_to_update]
    set_clauses.append("updated_at = NOW()")

    def _normalize_row_arrays(row):
        for col in ARRAY_COLS:
            v = row.get(col)
            if v is not None and not isinstance(v, list): row[col] = normalize_array_value(v)
            elif v is not None and col in PG_TEXT_ARRAY_COLS: row[col] = _unwrap_json_array_string_list(v)
        return row

    def _wrap_val(v, col):
        if col in JSONB_LIST_COLS:
            if v is None: return None
            if isinstance(v, list): return Json(v) if v else None
            normalized = normalize_array_value(v)
            return Json(normalized) if normalized else None
        if col in PG_TEXT_ARRAY_COLS:
            if v is None: return None
            if isinstance(v, list): return _unwrap_json_array_string_list(v)
            normalized = normalize_array_value(v)
            return normalized
        if col in DATE_COLS: return v
        return v

    for r in updates: _normalize_row_arrays(r)
    for r in inserts: _normalize_row_arrays(r)

    if updates:
        vals = [[r["id"]] + [_wrap_val(r.get(c), c) for c in cols_to_update] for r in updates]
        query = f"""INSERT INTO companies (id, {", ".join(cols_to_update)}) VALUES %s ON CONFLICT (id) DO UPDATE SET {", ".join(set_clauses)}"""
        execute_values(cur, query, vals)
    if inserts:
        for row in inserts:
            if row.get("corporate_number"): row["id"] = row["corporate_number"]
            else: row["id"] = deterministic_id_name_pref(row.get("name") or "", prefecture_key_for_row(row))
        insert_cols = ["id"] + cols_to_update
        vals = [[r["id"]] + [_wrap_val(r.get(c), c) for c in cols_to_update] for r in inserts]
        query = f"""INSERT INTO companies ({", ".join(insert_cols)}) VALUES %s ON CONFLICT (id) DO UPDATE SET {", ".join(set_clauses)}"""
        execute_values(cur, query, vals)
    for r in list(update_map.values()) + inserts:
        rid = r.get("id")
        if not rid: continue
        cn = r.get("corporate_number")
        if cn: cache_corp[cn] = rid
        if r.get("name"): cache_name_pref[(r["name"], prefecture_key_for_row(r))] = rid
    conn.commit()
    cur.close()

def process_file(conn, file_path):
    enc = get_encoding(file_path)
    rel_path = f"{file_path.parent.name}/{file_path.name}"
    revenue_folder = "unit_million" if file_path.parent.name == "unit_million" else "unit_yen"
    logger.info(f"処理開始: {rel_path} [財務スケール: {revenue_folder}]")
    with open(file_path, "r", encoding=enc, errors="replace") as f:
        reader = csv.reader(f)
        headers = next(reader, None)
        if not headers: return
        col_map = resolve_current_map(headers)
        file_pattern = detect_file_pattern(file_path, headers, col_map)

        index_map = INDEX_COL_MAP_PATTERN1
        if file_pattern == "pattern1_38": index_map = PATTERN1_38_COL_MAP
        elif file_pattern == "yuzuri": index_map = YUZURI_INDEX_MAP
        elif file_pattern == "pattern_5": index_map = PATTERN_5_CSV_MAP
        elif file_pattern == "import_firsttime": index_map = build_import_firsttime_index_map(headers)

        use_index = file_pattern in ("pattern1", "pattern1_38", "yuzuri", "import_firsttime")
        use_pattern5 = file_pattern == "pattern_5"

        batch = []
        for lineno, row in enumerate(reader, start=2):
            if len(row) < 2: continue
            skip, skip_reason = should_skip_corrupt_row(row, headers, file_pattern, lineno, rel_path)
            if skip:
                append_import_skip_log(rel_path, lineno, skip_reason, row[0] if row else "")
                continue

            data = {c: None for c in DB_COLS}
            data["status_for_active"] = None
            log_ctx = {
                "rel_path": rel_path,
                "lineno": lineno,
                "name": resolve_row_name_for_log(row, col_map, use_index, index_map),
            }

            if use_index:
                for idx, db_col in index_map.items():
                    if idx < len(row):
                        if file_pattern == "yuzuri": apply_yuzuri_value(data, db_col, row[idx], file_path, revenue_folder, log_ctx)
                        else: apply_value(data, db_col, row[idx], file_path, revenue_folder, log_ctx)
            else:
                for col_idx, db_col in col_map.items():
                    if col_idx < len(row):
                        apply_value(data, db_col, row[col_idx], file_path, revenue_folder, log_ctx)

            if file_pattern == "pattern1_38" and len(row) > 28:
                bits = []
                for i in (27, 28):
                    if i < len(row):
                        v = normalize_val(row[i])
                        if v: bits.append(v)
                if bits: data["overview"] = "\n\n".join(bits)

            if use_pattern5:
                for idx, db_col in PATTERN_5_CSV_MAP.items():
                    if idx < len(row) and row[idx]:
                        apply_value(data, db_col, row[idx], file_path, revenue_folder, log_ctx)

            if file_pattern == "import_firsttime":
                sogy = normalize_val(row[8]) if len(row) > 8 else None
                biz_mix = normalize_val(row[9]) if len(row) > 9 else None
                base_bs = (data.get("business_summary") or "").strip()
                if not base_bs: base_bs = (data.get("overview") or "").strip()
                chunks = [base_bs] if base_bs else []
                if sogy: chunks.append(f"【創業】{sogy}")
                if biz_mix: chunks.append(f"【事業構成】{biz_mix}")
                if sogy or biz_mix or (base_bs and len(chunks) > 1):
                    data["business_summary"] = "\n\n".join(x for x in chunks if x) or None

            if not data.get("prefecture") and data.get("address"):
                data["prefecture"] = extract_prefecture(data["address"])

            if data.get("name"): batch.append(data)
            if len(batch) >= 1000:
                upsert_batch(conn, batch)
                batch = []
        if batch: upsert_batch(conn, batch)

def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    target_dir = project_root / "fixed_csv_3"
    
    # ■■■ 対象ファイルを「タイプ1」など検証したい特定のCSVだけに絞り込む ■■■
    # ※今回は日付バグ・設立修正の確認と、タイプ1（10.csvなど）の確認を安全に行うため、以下のファイルのみ実行
    target_files = [
        target_dir / "unit_yen" / "10.csv",
        target_dir / "unit_million" / "1.csv",
        target_dir / "unit_yen" / "import_firstTime_105.csv",
        target_dir / "unit_million" / "import_firstTime_1.csv"
    ]
    
    csv_files = [fp for fp in target_files if fp.exists()]
    logger.info(f"対象CSVを {len(csv_files)} 件に制限して実行します。")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
    )
    try:
        load_id_caches(conn)
        for fp in csv_files:
            process_file(conn, fp)
    finally:
        conn.close()

if __name__ == "__main__":
    main()