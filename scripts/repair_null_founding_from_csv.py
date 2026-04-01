#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
from datetime import date as date_type
from decimal import Decimal
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(_project_root / ".env")
    load_dotenv()
except ImportError:
    pass

import psycopg2
from psycopg2.extras import execute_batch

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("repair_null_founding")

DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD") or ""
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

def _date_from_ymd(y: int, m: int, d: int):
    try:
        if y < 1800 or y > 2100 or m < 1 or m > 12 or d < 1 or d > 31: return None
        return date_type(y, m, d)
    except ValueError: return None

def _parse_japanese_date_forms(s: str):
    m = re.search(r"(\d{4})[年/]\s*(\d{1,2})[月/]\s*(\d{1,2})日?", s)
    if m:
        d0 = _date_from_ymd(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if d0: return d0
    m = re.search(r"(\d{1,2})月\s*(\d{1,2})日\s*(\d{4})年", s)
    if m:
        d0 = _date_from_ymd(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        if d0: return d0
    m = re.search(r"(\d{4})年\s*(\d{1,2})月(?!\s*\d{1,2}\s*日)", s)
    if m:
        d0 = _date_from_ymd(int(m.group(1)), int(m.group(2)), 1)
        if d0: return d0
    m = re.search(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m: return _date_from_ymd(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    return None

def parse_flexible_date(val):
    s = normalize_val(val)
    if not s: return None
    d0 = _parse_japanese_date_forms(s)
    if d0: return d0
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", s)
    if m: return _date_from_ymd(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = re.match(r"^(\d{4})[/.](\d{1,2})[/.](\d{1,2})$", s)
    if m: return _date_from_ymd(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", s)
    if m:
        mm, dd, yy = int(m.group(1)), int(m.group(2)), m.group(3)
        if len(yy) == 2:
            yi = int(yy)
            year = 1900 + yi if yi >= 30 else 2000 + yi
        else: year = int(yy)
        return _date_from_ymd(year, mm, dd)
    m = re.search(r"(19|20)\d{2}-\d{1,2}-\d{1,2}", s)
    if m:
        part = m.group(0)
        parts = part.split("-")
        if len(parts) == 3: return _date_from_ymd(int(parts[0]), int(parts[1]), int(parts[2]))
    return None

def parse_year_from_founding_cell(val):
    s = normalize_val(val)
    if not s: return None
    if re.fullmatch(r"\d{5,}", s): return None
    match = re.search(r"(\d{4})", s)
    return int(match.group(1)) if match else None

def _connect_postgres():
    try:
        return psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE)
    except psycopg2.OperationalError as e:
        logger.error("DB 接続に失敗しました: %s", e)
        sys.exit(1)

def normalize_header(raw: str) -> str:
    if not raw: return ""
    s = str(raw).strip().strip("\ufeff").replace("\n", " ").replace("\r", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)

def get_encoding(file_path: Path) -> str:
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            with open(file_path, "r", encoding=enc) as f: f.readline()
            return enc
        except Exception: continue
    return "utf-8"

def normalize_corp_num(val) -> str | None:
    if val is None: return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"): return None
    if "e" in s.lower():
        try: s = str(int(Decimal(s)))
        except Exception:
            try: s = f"{float(s):.0f}"
            except Exception: return None
    s = re.sub(r"[^\d]", "", s)
    if len(s) == 12: s = "0" + s
    return s if len(s) == 13 and s.isdigit() else None

def normalize_val(val) -> str | None:
    if val is None: return None
    s = str(val).strip()
    return s if s.lower() not in ("", "nan", "none", "null") else None

def find_col_indices(headers: list[str]) -> dict[str, int | None]:
    norm = [normalize_header(h) for h in headers]
    aliases = {
        "corp": ("法人番号", "法人番号(先頭)"),
        "founding": ("設立", "設立年月日", "設立年月日(西暦)"),
    }
    out: dict[str, int | None] = {k: None for k in aliases}
    for key, cands in aliases.items():
        for c in cands:
            nc = normalize_header(c)
            if nc in norm:
                out[key] = norm.index(nc)
                break
    return out

def column_exists(cur, col: str) -> bool:
    cur.execute("SELECT 1 FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'companies' AND column_name = %s", (col,))
    return cur.fetchone() is not None

def founding_column_is_date_like(cur) -> bool:
    cur.execute("SELECT data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'companies' AND column_name = 'founding'")
    r = cur.fetchone()
    if not r or not r[0]: return True
    dt = (r[0] or "").lower()
    return dt == "date" or "timestamp" in dt

def sql_founding_is_blank() -> str:
    return """(
        founding IS NULL
        OR TRIM(COALESCE(founding::text, '')) = ''
        OR COALESCE(founding::text, '') > '2030-01-01'
    )"""

def collect_csv_files(root: Path, single: Path | None) -> list[Path]:
    if single: return [single] if single.exists() else []
    return sorted(root.rglob("*.csv"))

def collect_updates_from_csv(files: list[Path]) -> tuple[list[tuple], int]:
    updates: list[tuple] = []
    skipped_parse = 0
    seen_corp: set[str] = set()

    for fp in files:
        enc = get_encoding(fp)
        try:
            with open(fp, "r", encoding=enc, errors="replace") as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers: continue
                idx = find_col_indices(headers)
                ic, ifn = idx["corp"], idx["founding"]
                if ic is None or ifn is None: continue
                
                for lineno, row in enumerate(reader, start=2):
                    if ifn >= len(row) or ic >= len(row): continue
                    raw_f = row[ifn]
                    corp = normalize_corp_num(row[ic])
                    if not corp or corp in seen_corp: continue
                    
                    d = parse_flexible_date(raw_f)
                    y = d.year if d else parse_year_from_founding_cell(raw_f)
                    
                    if not d and y is None:
                        if normalize_val(raw_f): skipped_parse += 1
                        continue
                        
                    iso = d.isoformat() if d else None
                    seen_corp.add(corp)
                    updates.append((d, y, iso, corp))
        except Exception:
            pass
    return updates, skipped_parse

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, default=_project_root / "fixed_csv_3")
    ap.add_argument("--csv", type=Path, default=None)
    args = ap.parse_args()

    files = collect_csv_files(args.root, args.csv)
    files = [p for p in files if "/later/" not in str(p).replace("\\", "/")]
    
    if not files: return

    logger.info("CSVファイルを読み込んで設立日をパース中... (少しお待ちください)")
    updates, skipped_parse = collect_updates_from_csv(files)
    logger.info("CSVから %d 件の有効な日付データを抽出しました。", len(updates))

    logger.info("データベースに接続し、更新が必要なレコードを一括取得中...")
    conn = _connect_postgres()
    cur = conn.cursor()
    
    # 爆速化の鍵：対象の法人番号を1回のクエリで全部持ってくる
    cur.execute(f"SELECT corporate_number FROM companies WHERE corporate_number IS NOT NULL AND {sql_founding_is_blank()}")
    target_corps = {row[0] for row in cur.fetchall() if row[0]}
    logger.info("DB上で設立日がNULLまたはバグっている企業は %d 件です。", len(target_corps))

    has_established = column_exists(cur, "established")
    founding_is_date = founding_column_is_date_like(cur)

    to_apply_date: list[tuple] = []
    to_apply_year: list[tuple] = []
    
    for d, y, iso, corp in updates:
        if corp in target_corps:  # メモリ上で判定（超高速）
            if d: to_apply_date.append((d, y, iso, corp))
            elif y is not None: to_apply_year.append((y, corp))

    if len(to_apply_date) + len(to_apply_year) == 0:
        logger.info("更新が必要なデータはありませんでした。")
        cur.close(); conn.close()
        return

    logger.info("いよいよDBの更新を開始します... (対象: %d件)", len(to_apply_date) + len(to_apply_year))
    
    def _iso(r): return r[2] or (r[0].isoformat() if r[0] else "")
    where_blank = f"WHERE corporate_number = %s AND {sql_founding_is_blank()}"

    if to_apply_date:
        if founding_is_date:
            if has_established:
                sql_d = """UPDATE companies SET founding = %s::date, founding_year = COALESCE(founding_year, %s), established = COALESCE(NULLIF(TRIM(COALESCE(established::text, '')), ''), %s::text) """ + where_blank
                batch_d = [(r[0], r[1], _iso(r), r[3]) for r in to_apply_date]
            else:
                sql_d = """UPDATE companies SET founding = %s::date, founding_year = COALESCE(founding_year, %s) """ + where_blank
                batch_d = [(r[0], r[1], r[3]) for r in to_apply_date]
        else:
            if has_established:
                sql_d = """UPDATE companies SET founding = %s::text, founding_year = COALESCE(founding_year, %s), established = COALESCE(NULLIF(TRIM(COALESCE(established::text, '')), ''), %s::text) """ + where_blank
                batch_d = [(_iso(r), r[1], _iso(r), r[3]) for r in to_apply_date]
            else:
                sql_d = """UPDATE companies SET founding = %s::text, founding_year = COALESCE(founding_year, %s) """ + where_blank
                batch_d = [(_iso(r), r[1], r[3]) for r in to_apply_date]
        execute_batch(cur, sql_d, batch_d, page_size=2000) # まとめてドカンと更新

    if to_apply_year:
        sql_y = "UPDATE companies SET founding_year = COALESCE(founding_year, %s) " + where_blank
        execute_batch(cur, sql_y, to_apply_year, page_size=2000)

    conn.commit()
    logger.info("設立（founding）のパッチ修復が完了しました！ 🎉")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()