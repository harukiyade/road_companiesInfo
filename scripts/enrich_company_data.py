#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Legatus ONE 企業DBを完成させるためのリッチデータ投入スクリプト（プロダクション・型安全版）。

【環境・対象】
- DB: PostgreSQL（接続情報は .env の POSTGRES_* を使用）
- 対象: fixed_csv_3 配下の全CSV（パスに /later/ を含むものは除外）

【照合】4段階で1件に特定できた場合のみUPDATE。0件/複数ヒットは skipped_reasons に記録しスキップ。
【マッピング】売上・利益は百万円→円、設立はカレンダー表現のみ（YYYY-MM-DD 等）→日付文字列（Excelシリアルは設立に使わない）、説明→overview/概要→business_descriptions。
  拠点数はbigintへ。
  ★重要: 取締役(executives)はカンマ区切りのtext型へ、株主・取引先銀行はARRAY型(list)として渡す。
"""

import csv
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

# プロジェクトルートの .env を優先して読み込む
_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
from backend.api.csv_founding_date import (
    founding_cell_to_iso_and_year,
    parse_birth_cell_to_date,
    parse_year_from_founding_cell,
)
try:
    from dotenv import load_dotenv
    load_dotenv(_project_root / ".env")
    load_dotenv()  # カレントディレクトリの .env もフォールバック
except ImportError:
    pass

import psycopg2
from psycopg2.extras import execute_batch

# 巨大CSVフィールド対応
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

# --- 設定（.env の POSTGRES_* を参照）---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

ROOT_DIR = "fixed_csv_3"
EXCLUDE_PATH_FRAGMENT = "/later/"

# バッチサイズ
UPDATE_BATCH_SIZE = 500

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("enrich_company_data")

PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)

# 照合用キャッシュ: 法人番号 -> id / (name, phone_norm) -> [id] / (name, pref, addr_norm) -> [id]
cache_corp = {}
cache_name_phone = {}
cache_name_pref_addr = {}

# 特定不可ログ用
skipped_reasons = []


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
    if not raw:
        return ""
    s = str(raw).strip().strip("\ufeff").replace("\n", " ").replace("\r", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)


def normalize_val(val):
    if val is None:
        return None
    s = str(val).strip()
    return s if s and str(s).lower() not in ("", "nan", "none", "null") else None


def normalize_corp_num(val):
    """法人番号を13桁の文字列に正規化"""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    if "e" in s.lower():
        try:
            s = f"{float(s):.0f}"
        except Exception:
            return None
    s = re.sub(r"[^\d]", "", s)
    if len(s) == 12:
        s = "0" + s
    return s if len(s) == 13 and s.isdigit() else None


def normalize_phone(val):
    """電話番号からハイフン・全角記号・空白を除去して比較用に正規化"""
    if not val:
        return None
    s = str(val).strip()
    s = re.sub(r"[\s\-－‐ー−・\uff0d]", "", s)  # ハイフン・全角マイナス・中黒等
    s = re.sub(r"[^\d]", "", s)
    return s if s else None


def address_up_to_street(addr, prefecture=None): # prefecture引数をオプショナルに
    """
    住所から建物名・号室を除いた「番地まで」で正規化。
    丁目・番・号まで含め、ビル・マンション・階・号室は除去。
    """
    if not addr:
        return None
    s = str(addr).strip()
    if not s:
        return None
    # 建物名らしきパターンで切る: ビル、マンション、棟、階、号室、F、○F 等
    for pattern in [
        r"[０-９0-9]*[Ff]+\s*$",
        r"[０-９0-9]+[Ff]",
        r"[\d]+階",
        r"[\d]+号室",
        r"[\d]+号?\s*$",
        r"ビル.*$",
        r"マンション.*$",
        r"建物.*$",
        r"[\u4e00-\u9fff]*棟.*$",
        r"[\u4e00-\u9fff]*館.*$",
    ]:
        s = re.sub(pattern, "", s)
    s = re.sub(r"\s+", " ", s).strip()
    # 番地まで: 数字＋丁目・番・号の並びを維持
    return s if s else None


def extract_prefecture(addr):
    if not addr:
        return None
    s = str(addr).strip()
    for pref in PREFECTURES:
        if pref in s:
            return pref
    return None


def parse_int_safe(val):
    if val is None:
        return None
    s = str(val).replace(",", "").strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    try:
        n = int(float(s))
        return n if n >= 0 else None
    except (ValueError, TypeError):
        return None


def parse_revenue_million_to_yen(val, is_million_unit):
    """直近売上・直近利益: フォルダによって円に変換（×1,000,000）"""
    if val is None:
        return None
    s = str(val).replace(",", "").replace("　", "").strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    try:
        num = float(s)
        if num < 0 or num > 1e15:
            return None
        multiplier = 1000000 if is_million_unit else 1
        return int(num * multiplier)
    except (ValueError, TypeError):
        return None


def to_clean_list(val):
    """カンマ区切り文字列をリストにし、ARRAY用に返す（空はNone）"""
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    # 全角・半角カンマで分割
    parts = re.split(r"[,\s]*[,，、]\s*", s)
    out = [x.strip() for x in parts if x.strip()]
    return out if out else None


def load_id_caches(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT corporate_number, id FROM companies WHERE corporate_number IS NOT NULL AND corporate_number != ''"
    )
    global cache_corp
    cache_corp = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute(
        """SELECT name, phone_number, id FROM companies
           WHERE name IS NOT NULL AND name != '' AND phone_number IS NOT NULL AND phone_number != ''"""
    )
    name_phone_to_ids = {}
    for name, phone, cid in cur.fetchall():
        key = (normalize_val(name), normalize_phone(phone))
        if key[0] and key[1]:
            name_phone_to_ids.setdefault(key, []).append(cid)
    global cache_name_phone
    cache_name_phone = name_phone_to_ids

    cur.execute(
        """SELECT name, prefecture, address, id FROM companies
           WHERE name IS NOT NULL AND name != '' AND prefecture IS NOT NULL AND prefecture != ''"""
    )
    name_pref_addr_to_ids = {}
    for name, pref, addr, cid in cur.fetchall():
        anorm = address_up_to_street(addr)
        key = (normalize_val(name), normalize_val(pref), (anorm or "").strip())
        if key[0] and key[1]:
            name_pref_addr_to_ids.setdefault(key, []).append(cid)
    global cache_name_pref_addr
    cache_name_pref_addr = name_pref_addr_to_ids

    cur.close()
    logger.info(
        f"キャッシュ: 法人番号 {len(cache_corp)}件, 社名+電話 {len(cache_name_phone)}件, "
        f"社名+都道府県+住所 {len(cache_name_pref_addr)}件"
    )


def resolve_company_id(corporate_number, name, phone, prefecture, address, file_path=None, row_info=None):
    """
    4段階照合で1件に特定。複数 or 0件の場合は None を返し、理由を skipped_reasons に追加。
    """
    # 1) 法人番号一致
    if corporate_number and corporate_number in cache_corp:
        return cache_corp[corporate_number]

    phone_norm = normalize_phone(phone)
    addr_norm = address_up_to_street(address) if address else None
    pref_val = normalize_val(prefecture) or extract_prefecture(address)
    name_val = normalize_val(name)

    # 2) 社名 + 電話番号（ハイフン除去）
    if name_val and phone_norm:
        ids = cache_name_phone.get((name_val, phone_norm))
        if ids:
            if len(ids) == 1:
                return ids[0]
            skipped_reasons.append(
                ("複数ヒット: 社名+電話", file_path, row_info, name_val, "name_phone")
            )
            return None

    # 3) 社名 + 都道府県 + 住所（番地まで）
    if name_val and pref_val:
        key_addr = (addr_norm or "").strip()
        key = (name_val, pref_val, key_addr)
        ids = cache_name_pref_addr.get(key)
        if ids:
            if len(ids) == 1:
                return ids[0]
            skipped_reasons.append(
                ("複数ヒット: 社名+都道府県+住所", file_path, row_info, name_val, "name_pref_addr")
            )
            return None

    # 4) 特定不可
    skipped_reasons.append(("特定不可: いずれの照合でも一意に特定できず", file_path, row_info, name_val, "none"))
    return None


def collect_csv_files(project_root):
    root = project_root / ROOT_DIR
    if not root.exists():
        return [], 0
    all_files = []
    excluded = 0
    for fp in root.rglob("*.csv"):
        path_str = str(fp.resolve()).replace("\\", "/")
        if EXCLUDE_PATH_FRAGMENT in path_str:
            excluded += 1
            continue
        all_files.append(fp)
    return sorted(all_files), excluded


# CSV項目名の候補（正規化してマッチ）
CSV_COLUMN_CANDIDATES = {
    "name": ("会社名", "企業名", "name", "商号又は名称"),
    "corporate_number": ("法人番号", "ID", "会社ID", "corporate_number", "リストID"),
    "phone_number": ("電話番号", "電話番号(窓口)", "営業所電話番号", "連絡先電話番号"),
    "prefecture": ("都道府県", "prefecture"),
    "address": ("住所", "所在地", "address", "会社住所", "営業所所在地"),
    "latest_revenue": ("直近売上", "売上高", "売上規模（百万円）"),
    "latest_profit": ("直近利益", "当期純利益(損失)", "経常利益", "営業利益"),
    "established": ("設立", "創業", "設立年月日"),
    "overview": ("説明", "概要", "企業概要", "事業内容"),
    "business_descriptions": ("概要", "事業内容", "会社情報・備考"),
    "office_count": ("オフィス数", "国内の事業所"),
    "factory_count": ("工場数",),
    "store_count": ("店舗数",),
    "representative_name": ("代表者名", "代表者", "氏名1"),
    "representative_birth_date": ("代表者誕生日",),
    "executives": ("取締役", "役員", "役員名"),
    "shareholders": ("株主", "主要株主", "株式保有率"),
    "banks": ("取引先銀行", "主要取引銀行", "[主な取引銀行]"),
}


def build_header_index(headers):
    norm_to_idx = {normalize_header(h): i for i, h in enumerate(headers)}
    index = {}
    for db_key, candidates in CSV_COLUMN_CANDIDATES.items():
        for cand in candidates:
            n = normalize_header(cand)
            if n in norm_to_idx:
                index[db_key] = norm_to_idx[n]
                break
    return index


def row_to_update_payload(row, idx_map, file_path, row_num=0):
    """1行からUPDATE用の辞書を生成。id は含めない。"""
    def get(col):
        i = idx_map.get(col)
        if i is None or i >= len(row):
            return None
        return normalize_val(row[i])

    corporate_number = normalize_corp_num(get("corporate_number"))
    name = get("name")
    phone = get("phone_number")
    prefecture = get("prefecture") or extract_prefecture(get("address"))
    address = get("address")

    company_id = resolve_company_id(
        corporate_number, name, phone, prefecture, address,
        file_path=file_path, row_info=name or f"row_{id(row)}"
    )
    if not company_id:
        return None, False

    established_val = get("established")
    date_str, year_int = founding_cell_to_iso_and_year(established_val)
    if year_int is None and established_val:
        year_int = parse_year_from_founding_cell(established_val)
    if established_val and not date_str and year_int is None:
        logger.warning(
            "設立をパースできません（established は更新しません）: file=%s line=%s company=%r value=%r",
            getattr(file_path, "name", file_path),
            row_num,
            name,
            established_val,
        )

    is_million_unit = "unit_million" in str(file_path)
    latest_revenue = parse_revenue_million_to_yen(get("latest_revenue"), is_million_unit)
    latest_profit = parse_revenue_million_to_yen(get("latest_profit"), is_million_unit)

    overview = get("overview")           # 説明 → overview
    business_descriptions = get("business_descriptions")  # 概要 → business_descriptions
    office_count = parse_int_safe(get("office_count"))
    factory_count = parse_int_safe(get("factory_count"))
    store_count = parse_int_safe(get("store_count"))

    # executives はDB側が text 型のため、リストではなく結合した文字列を渡す
    exec_list = to_clean_list(get("executives"))
    executives_str = ", ".join(exec_list) if exec_list else None

    # shareholders と banks はDB側が ARRAY 型のため、Pythonのリストをそのまま渡す
    shareholders_list = to_clean_list(get("shareholders"))
    banks_list = to_clean_list(get("banks"))

    # birth_dateの処理 (text型)
    birth_raw = get("representative_birth_date")
    bd = parse_birth_cell_to_date(birth_raw) if birth_raw else None
    bd_str = bd.isoformat() if bd else None

    payload = {
        "id": company_id,
        "latest_revenue": latest_revenue,
        "latest_profit": latest_profit,
        "established": date_str,  # text型
        "founding_year": year_int, # int型
        "overview": overview,
        "business_descriptions": business_descriptions,
        "office_count": office_count,
        "factory_count": factory_count,
        "store_count": store_count,
        "representative_name": get("representative_name"),
        "representative_birth_date": bd_str, # text型
        "executives": executives_str, # text型
        "shareholders": shareholders_list, # ARRAY型 (listを渡す)
        "banks": banks_list, # ARRAY型 (listを渡す)
    }
    return payload, True


def process_file(conn, file_path, stats):
    enc = get_encoding(file_path)
    try:
        with open(file_path, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f)
            headers = next(reader, None)
    except Exception as e:
        logger.warning(f"読み込みスキップ: %s - %s", file_path, e)
        stats["errors"] += 1
        return

    if not headers:
        return

    idx_map = build_header_index(headers)
    if not idx_map.get("name") and not idx_map.get("corporate_number"):
        logger.debug("社名・法人番号どちらもないためスキップ: %s", file_path.name)
        return

    updates = []
    row_num = 0
    try:
        with open(file_path, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                row_num += 1
                if len(row) < 2:
                    continue
                payload, resolved = row_to_update_payload(row, idx_map, file_path, row_num)
                if resolved and payload:
                    updates.append(payload)
    except Exception as e:
        logger.warning("処理エラー: %s (行付近) - %s", file_path, e)
        stats["errors"] += 1
        return

    if not updates:
        logger.info("  %s: 更新対象0件", file_path.name)
        return

    # UPDATE を execute_batch で一括実行
    cur = conn.cursor()
    try:
        # DB側の型に合わせてキャストや関数を調整
        # text型の established 等はキャストを削除、ARRAY型にはそのまま %s を渡す
        update_sql = """
            UPDATE companies SET
                latest_revenue = COALESCE(%(latest_revenue)s, latest_revenue),
                latest_profit = COALESCE(%(latest_profit)s, latest_profit),
                established = COALESCE(%(established)s, established),
                founding_year = COALESCE(%(founding_year)s, founding_year),
                overview = COALESCE(NULLIF(TRIM(%(overview)s), ''), overview),
                business_descriptions = COALESCE(NULLIF(TRIM(%(business_descriptions)s), ''), business_descriptions),
                office_count = COALESCE(%(office_count)s, office_count),
                factory_count = COALESCE(%(factory_count)s, factory_count),
                store_count = COALESCE(%(store_count)s, store_count),
                representative_name = COALESCE(NULLIF(TRIM(%(representative_name)s), ''), representative_name),
                representative_birth_date = COALESCE(%(representative_birth_date)s, representative_birth_date),
                executives = COALESCE(NULLIF(TRIM(%(executives)s), ''), executives),
                shareholders = COALESCE(%(shareholders)s, shareholders),
                banks = COALESCE(%(banks)s, banks),
                updated_at = NOW()
            WHERE id = %(id)s
        """
        execute_batch(cur, update_sql, updates, page_size=UPDATE_BATCH_SIZE)
        conn.commit()
        stats["updated"] += len(updates)
        stats["rows_processed"] += len(updates)
        logger.info("  %s: %d件更新", file_path.name, len(updates))
    except Exception as e:
        conn.rollback()
        logger.error("DB更新エラー: %s - %s", file_path.name, e)
        stats["errors"] += 1
    finally:
        cur.close()


def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    csv_files, excluded = collect_csv_files(project_root)
    logger.info("対象ルート: %s/ （%s 除外: %d件）", ROOT_DIR, EXCLUDE_PATH_FRAGMENT.strip("/"), excluded)
    logger.info("対象CSV: %d件", len(csv_files))

    if not csv_files:
        logger.info("対象CSVがありません")
        return

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )

    load_id_caches(conn)

    stats = {"updated": 0, "rows_processed": 0, "errors": 0, "skipped": 0}
    try:
        for fp in csv_files:
            rel = f"{fp.parent.name}/{fp.name}"
            logger.info("処理: %s", rel)
            process_file(conn, fp, stats)
    finally:
        conn.close()

    stats["skipped"] = len(skipped_reasons)
    logger.info("---------- 集計 ----------")
    logger.info("合計更新件数: %d", stats["updated"])
    logger.info("合計スキップ件数: %d", stats["skipped"])
    logger.info("処理行数（更新として反映した数）: %d", stats["rows_processed"])
    logger.info("エラー件数: %d", stats["errors"])

    if skipped_reasons:
        logger.info("--- スキップ理由サンプル（最大20件）---")
        for r in skipped_reasons[:20]:
            fp_display = r[1].name if getattr(r[1], "name", None) else str(r[1])
            logger.info("  [%s] ファイル=%s 社名=%s", r[0], fp_display, r[3] or "(なし)")


if __name__ == "__main__":
    main()