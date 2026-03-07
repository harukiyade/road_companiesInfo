#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3 配下のCSVからSBフラグを読み取り、companies.sb_flag を更新。

- 対象: fixed_csv_3/ 配下（fixed_csv_3/later は除外）
- ストロングバイヤー → sb_flag = true
- ストロングバイヤー以外 → sb_flag = false
"""

import csv
import logging
import os
import re
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import psycopg2

DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

ROOT_DIR = "fixed_csv_3"
EXCLUDE_PATH_FRAGMENT = "/later/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("update_sb_flag")

PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)

cache_corp = {}
cache_name_pref = {}


def get_encoding(fp):
    for enc in ["utf-8-sig", "cp932", "utf-8"]:
        try:
            with open(fp, "r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            pass
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
    return s if s and str(s).lower() not in ("nan", "none", "null") else None


def normalize_corp_num(val):
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


def extract_prefecture(address):
    if not address:
        return None
    s = str(address).strip()
    for pref in PREFECTURES:
        if pref in s:
            return pref
    return None


def parse_sb_flag(val):
    """
    SBフラグの値を bool に変換。
    ストロングバイヤー → True
    ストロングバイヤー以外 → False
    その他 → None（更新しない）
    """
    s = normalize_val(val)
    if not s:
        return None
    if "ストロングバイヤー" in s and "以外" not in s:
        return True
    if "ストロングバイヤー以外" in s or ("ストロングバイヤー" in s and "以外" in s):
        return False
    return None


def load_id_caches(conn):
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
    cur.close()
    logger.info(f"キャッシュ: 法人番号 {len(cache_corp)}件, 名前+都道府県 {len(cache_name_pref)}件")


def resolve_company_id(corporate_number, name, prefecture):
    if corporate_number and corporate_number in cache_corp:
        return cache_corp[corporate_number]
    if name and prefecture:
        return cache_name_pref.get((name, prefecture))
    if name and prefecture is None:
        for (n, p), cid in cache_name_pref.items():
            if n == name:
                return cid
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


def process_file(conn, fp, stats):
    enc = get_encoding(fp)
    try:
        with open(fp, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f)
            headers = next(reader, [])
    except Exception as e:
        logger.warning(f"読み込みスキップ: {fp} - {e}")
        return

    norm_to_idx = {normalize_header(h): i for i, h in enumerate(headers)}
    sb_idx = None
    for norm, idx in norm_to_idx.items():
        if "sb" in norm.lower() or "ストロングバイヤー" in norm or "sbフラグ" in norm:
            sb_idx = idx
            break
    if sb_idx is None:
        return

    name_candidates = ("会社名", "企業名", "name")
    pref_candidates = ("都道府県", "prefecture")
    corp_candidates = ("法人番号", "ID", "会社ID", "corporate_number")
    addr_candidates = ("住所", "所在地", "address")

    name_idx = None
    for k in name_candidates:
        nk = normalize_header(k)
        if nk in norm_to_idx:
            name_idx = norm_to_idx[nk]
            break
    pref_idx = None
    for k in pref_candidates:
        nk = normalize_header(k)
        if nk in norm_to_idx:
            pref_idx = norm_to_idx[nk]
            break
    corp_idx = None
    for k in corp_candidates:
        nk = normalize_header(k)
        if nk in norm_to_idx:
            corp_idx = norm_to_idx[nk]
            break
    addr_idx = None
    for k in addr_candidates:
        nk = normalize_header(k)
        if nk in norm_to_idx:
            addr_idx = norm_to_idx[nk]
            break

    updates = []
    try:
        with open(fp, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if sb_idx >= len(row):
                    continue
                sb_val = parse_sb_flag(row[sb_idx])
                if sb_val is None:
                    continue

                name = normalize_val(row[name_idx]) if name_idx is not None and name_idx < len(row) else None
                prefecture = normalize_val(row[pref_idx]) if pref_idx is not None and pref_idx < len(row) else None
                if not prefecture and addr_idx is not None and addr_idx < len(row):
                    prefecture = extract_prefecture(row[addr_idx])
                corporate_number = normalize_corp_num(row[corp_idx]) if corp_idx is not None and corp_idx < len(row) else None

                company_id = resolve_company_id(corporate_number, name, prefecture)
                if company_id:
                    updates.append((company_id, sb_val))
    except Exception as e:
        logger.warning(f"処理エラー: {fp} - {e}")
        return

    if not updates:
        return

    cur = conn.cursor()
    try:
        for company_id, sb_val in updates:
            cur.execute(
                "UPDATE companies SET sb_flag = %s, updated_at = NOW() WHERE id = %s",
                (sb_val, company_id),
            )
            stats["updated"] += cur.rowcount
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"DB更新エラー: {e}")
    finally:
        cur.close()

    stats["rows_processed"] += len(updates)
    logger.info(f"  {fp.name}: {len(updates)}件更新")


def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    csv_files, excluded = collect_csv_files(project_root)
    sb_files = []
    for fp in csv_files:
        enc = get_encoding(fp)
        try:
            with open(fp, "r", encoding=enc, errors="replace") as f:
                headers = next(csv.reader(f), [])
        except Exception:
            continue
        norm_h = {normalize_header(h) for h in headers}
        if any("sb" in n.lower() or "ストロングバイヤー" in n for n in norm_h):
            sb_files.append(fp)

    logger.info(f"対象ルート: {ROOT_DIR}/")
    logger.info(f"除外ディレクトリ（{EXCLUDE_PATH_FRAGMENT.strip('/')}）: {excluded}件")
    logger.info(f"SBフラグがあるCSV: {len(sb_files)}件")

    if not sb_files:
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

    stats = {"updated": 0, "rows_processed": 0}
    try:
        load_id_caches(conn)
        for fp in sb_files:
            logger.info(f"処理: {fp.parent.name}/{fp.name}")
            process_file(conn, fp, stats)
    finally:
        conn.close()

    logger.info(f"処理完了: 更新レコード数 {stats['updated']}, 処理行数 {stats['rows_processed']}")


if __name__ == "__main__":
    main()
