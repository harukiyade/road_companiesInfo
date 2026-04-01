#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3 配下の CSV から、財務（千円→円×1000）、上場区分、代表者誕生日を
companies に上書きする。

- 財務のみ例外: CSV が 0（千円換算後も 0）だが DB に非ゼロがある場合は DB を維持
- 上場区分・代表者誕生日は CSV 優先（従来どおり）
- results.json のオフセットで財務・上場・誕生日列にシフト補正
- DRY_RUN がデフォルト（--execute で更新）

実行例:
  cd info_companyDetail
  python3 scripts/apply_contact_established_fix_from_offsets.py \\
    --results reports/financial_fingerprint_20260322_201429/results.json
  python3 scripts/apply_contact_established_fix_from_offsets.py ... --execute
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# プロジェクトルートを先に追加（backend パッケージ解決用）
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from backend.api.csv_founding_date import parse_founding_cell_to_date  # noqa: E402
import import_full_update_fast as iff  # noqa: E402

try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD") or ""
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

EXCLUDE_MARKERS = ("/later/", "/later_2/")


def sniff_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            with open(path, "r", encoding=enc) as f:
                f.readline()
            return enc
        except (UnicodeDecodeError, UnicodeError, OSError):
            continue
    return "utf-8"


def rel_project(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def find_col(headers: list[str], *candidates: str) -> int | None:
    nh = [iff.normalize_header(h) for h in headers]
    for c in candidates:
        nc = iff.normalize_header(c)
        if nc in nh:
            return nh.index(nc)
    return None


def load_results_map(results_path: Path) -> dict[str, dict]:
    data = json.loads(results_path.read_text(encoding="utf-8"))
    out: dict[str, dict] = {}
    for r in data.get("results", []):
        if r.get("error"):
            continue
        p = (r.get("path") or "").replace("\\", "/")
        if p:
            out[p] = r
    return out


def infer_file_offset(meta: dict | None) -> int:
    if not meta:
        return 0
    ol = meta.get("offset_lists") or {}
    merged: Counter[int] = Counter()
    for k in ("revenue", "capital", "profit"):
        for ks, nv in (ol.get(k) or {}).items():
            try:
                merged[int(ks)] += int(nv)
            except (TypeError, ValueError):
                pass
    if merged:
        return merged.most_common(1)[0][0]
    ft = (meta.get("file_type") or "").strip()
    m = re.match(r"^Type-Shift-L(\d+)$", ft)
    if m:
        return -int(m.group(1))
    m = re.match(r"^Type-Shift-R(\d+)$", ft)
    if m:
        return int(m.group(1))
    return 0


def apply_offset(base_idx: int | None, offset: int, row_len: int) -> int | None:
    if base_idx is None:
        return None
    j = base_idx - offset
    if j < 0 or j >= row_len:
        return None
    return j


def to_full_yen(cell: str | None) -> int | None:
    """CSV の千円単位（数値）を円にする: ×1000。空・NaN は None。"""
    if cell is None:
        return None
    s = str(cell).strip()
    if not s or s.lower() in ("nan", "none", "null", "-", "―"):
        return None
    if re.search(r"[億万千兆円]", s):
        return None
    t = s.replace(",", "").replace("，", "").replace("　", "").strip()
    num = re.sub(r"[^\d.\-+eE]", "", t)
    if not num:
        return None
    try:
        v = float(num)
        if abs(v) > 1e15:
            return None
        return int(round(v * 1000))
    except (ValueError, OverflowError):
        return None


def _as_int_fin(v) -> int | None:
    if v is None:
        return None
    try:
        i = int(v)
        return i
    except (TypeError, ValueError):
        return None


def merge_financial_zero_keeps_db(
    csv_cap: int | None,
    csv_rev: int | None,
    csv_prof: int | None,
    db: dict | None,
) -> tuple[int | None, int | None, int | None]:
    """
    CSV が 0 の項目だけ、DB に非ゼロがあれば DB を採用。
    CSV が None（空セル等）→ その列は None（NULL 上書き）。
    """
    d = db or {}

    def cap_merged() -> int | None:
        if csv_cap is None:
            return None
        if csv_cap != 0:
            return csv_cap
        for key in ("capital", "capital_stock"):
            x = _as_int_fin(d.get(key))
            if x is not None and x != 0:
                return x
        return 0

    def rev_merged() -> int | None:
        if csv_rev is None:
            return None
        if csv_rev != 0:
            return csv_rev
        x = _as_int_fin(d.get("latest_revenue"))
        if x is not None and x != 0:
            return x
        return 0

    def prof_merged() -> int | None:
        if csv_prof is None:
            return None
        if csv_prof != 0:
            return csv_prof
        x = _as_int_fin(d.get("latest_profit"))
        if x is not None and x != 0:
            return x
        return 0

    return cap_merged(), rev_merged(), prof_merged()


def fetch_db_financials_by_corp(
    conn, corp_ids: list[str], cols_exist: set[str]
) -> dict[str, dict]:
    if not corp_ids:
        return {}
    fields = ["corporate_number"]
    for c in ("capital", "capital_stock", "latest_revenue", "latest_profit"):
        if c in cols_exist:
            fields.append(c)
    sel = ", ".join(fields)
    out: dict[str, dict] = {}
    chunk = 800
    for i in range(0, len(corp_ids), chunk):
        part = corp_ids[i : i + chunk]
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT {sel} FROM companies WHERE corporate_number = ANY(%s)",
                (part,),
            )
            for r in cur.fetchall():
                out[str(r["corporate_number"])] = dict(r)
    return out


def norm_listing(cell: str | None) -> str:
    """
    「上場」が含まれ、かつ「非上場」が含まれない → 「上場」
    それ以外（長文・空）→ 「非上場」
    """
    if cell is None:
        return "非上場"
    s = str(cell).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return "非上場"
    if "非上場" in s:
        return "非上場"
    if "上場" in s:
        return "上場"
    return "非上場"


def iter_csv_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for p in sorted(root.rglob("*.csv")):
        s = str(p).replace("\\", "/")
        if any(m in s for m in EXCLUDE_MARKERS):
            continue
        if "unit_million" not in s and "unit_yen" not in s:
            continue
        out.append(p)
    return out


def fetch_column_udt(conn, column: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT udt_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'companies'
              AND column_name = %s
            LIMIT 1
            """,
            (column,),
        )
        row = cur.fetchone()
    return row[0] if row else "text"


def build_update_sql(has_capital: bool, has_capital_stock: bool) -> str:
    parts = []
    if has_capital:
        parts.append("capital = %s")
    if has_capital_stock:
        parts.append("capital_stock = %s")
    parts.extend(
        [
            "latest_revenue = %s",
            "latest_profit = %s",
            "listing = %s",
            "representative_birth_date = %s",
        ]
    )
    parts.append("updated_at = NOW()")
    return f"UPDATE companies SET {', '.join(parts)} WHERE corporate_number = %s"


def main() -> None:
    ap = argparse.ArgumentParser(description="CSV から財務・上場・代表誕生日を上書き")
    ap.add_argument("--results", type=Path, required=True)
    ap.add_argument("--root", type=Path, default=PROJECT_ROOT / "fixed_csv_3")
    ap.add_argument("--execute", action="store_true")
    ap.add_argument("--batch-size", type=int, default=500)
    args = ap.parse_args()

    dry_run = not args.execute
    if not args.results.is_file():
        print(f"エラー: results.json が見つかりません: {args.results}", file=sys.stderr)
        sys.exit(2)
    if not dry_run and not DB_PASSWORD:
        print("エラー: POSTGRES_PASSWORD または PGPASSWORD が未設定です。", file=sys.stderr)
        sys.exit(2)

    results_map = load_results_map(args.results)
    files = iter_csv_files(args.root)

    conn = None
    sql = ""
    has_capital = False
    has_capital_stock = True
    rep_birth_is_date = True

    if not dry_run:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            sslmode=DB_SSLMODE,
        )
        conn.autocommit = False

        cols_exist: set[str] = set()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'companies'
                """
            )
            cols_exist = {r[0] for r in cur.fetchall()}

        has_capital = "capital" in cols_exist
        has_capital_stock = "capital_stock" in cols_exist
        rep_udt = fetch_column_udt(conn, "representative_birth_date")
        rep_birth_is_date = rep_udt == "date"

        sql = build_update_sql(has_capital, has_capital_stock)

    stats: Counter[str] = Counter()
    pending: list[tuple] = []

    def flush_batch(cur):
        nonlocal pending
        if not pending:
            return
        execute_batch(cur, sql, pending, page_size=args.batch_size)
        conn.commit()
        stats["batches_committed"] += 1
        stats["rows_updated"] += len(pending)
        print(f"[進捗] {stats['rows_updated']} 件 UPDATE コミット済み", flush=True)
        pending = []

    try:
        for fp in files:
            rel = rel_project(fp)
            meta = results_map.get(rel)
            offset = infer_file_offset(meta)
            enc = sniff_encoding(fp)

            with open(fp, "r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers:
                    continue

                corp_i = find_col(headers, "法人番号", "ID", "会社ID")
                cap_i = find_col(headers, "資本金", "法人＿資本金")
                rev_i = find_col(
                    headers,
                    "直近売上",
                    "売上高",
                    "法人＿売上高",
                    "売上規模（百万円）",
                )
                prof_i = find_col(
                    headers,
                    "直近利益",
                    "直近純利益",
                    "法人＿当期純利益(損失)",
                    "当期純利益(損失)",
                )
                list_i = find_col(headers, "上場区分", "上場")
                bday_i = find_col(headers, "代表者誕生日", "代表者生年月日")

                if corp_i is None:
                    stats["skip_no_corp_col"] += 1
                    continue

                idxs = [i for i in (cap_i, rev_i, prof_i, list_i, bday_i, corp_i) if i is not None]
                max_i = max(idxs) if idxs else corp_i

                row_buf: list[
                    tuple[str, int | None, int | None, int | None, str, object, str, int]
                ] = []

                def drain_row_buf() -> None:
                    nonlocal row_buf, pending
                    if dry_run or not row_buf or conn is None:
                        row_buf.clear()
                        return
                    cns = [r[0] for r in row_buf]
                    db_map = fetch_db_financials_by_corp(conn, cns, cols_exist)
                    for cn, cap, rev, prof, lst, bval, _rel, _lineno in row_buf:
                        dbrow = db_map.get(cn)
                        mcap, mrev, mprof = merge_financial_zero_keeps_db(
                            cap, rev, prof, dbrow
                        )
                        if (mcap, mrev, mprof) != (cap, rev, prof):
                            stats["fin_merged_keep_db"] += 1

                        vals: list = []
                        if has_capital:
                            vals.append(mcap)
                        if has_capital_stock:
                            vals.append(mcap)
                        vals.extend([mrev, mprof, lst, bval, cn])

                        pending.append(tuple(vals))
                        stats["rows_queued"] += 1
                        if len(pending) >= args.batch_size:
                            with conn.cursor() as cur:
                                flush_batch(cur)
                    row_buf.clear()

                for lineno, row in enumerate(reader, start=2):
                    if len(row) != len(headers):
                        stats["row_len_mismatch"] += 1
                        continue
                    if len(row) <= max_i:
                        stats["row_too_short"] += 1
                        continue

                    cn = iff.normalize_corp_num(row[corp_i])
                    if not cn:
                        stats["row_bad_corp"] += 1
                        continue

                    j_cap = apply_offset(cap_i, offset, len(row))
                    j_rev = apply_offset(rev_i, offset, len(row))
                    j_prof = apply_offset(prof_i, offset, len(row))
                    j_list = apply_offset(list_i, offset, len(row))
                    j_bday = apply_offset(bday_i, offset, len(row))

                    cap = to_full_yen(row[j_cap] if j_cap is not None else None)
                    rev = to_full_yen(row[j_rev] if j_rev is not None else None)
                    prof = to_full_yen(row[j_prof] if j_prof is not None else None)
                    lst = norm_listing(row[j_list] if j_list is not None else None)

                    raw_b = row[j_bday] if j_bday is not None else None
                    d_b = parse_founding_cell_to_date(raw_b) if raw_b else None
                    if d_b:
                        bval = d_b if rep_birth_is_date else d_b.isoformat()
                    else:
                        bval = None

                    if dry_run:
                        if stats["dry_log_lines"] < 20:
                            print(
                                f"[予定] corp={cn} file={rel} L{lineno} off={offset} "
                                f"cap={cap} rev={rev} prof={prof} list={lst} bday={bval}"
                            )
                            stats["dry_log_lines"] += 1
                        stats["rows_queued"] += 1
                        continue

                    row_buf.append((cn, cap, rev, prof, lst, bval, rel, lineno))
                    if len(row_buf) >= 500:
                        drain_row_buf()

                drain_row_buf()

        if not dry_run and pending and conn:
            with conn.cursor() as cur:
                flush_batch(cur)

        if dry_run:
            print(f"\n（DRY_RUN）対象行キュー: {stats['rows_queued']} --execute で反映")
        else:
            print(f"\n完了: updated={stats['rows_updated']}, batches={stats['batches_committed']}")

    finally:
        if conn is not None:
            conn.close()

    print("=== 統計 ===")
    for k in sorted(stats.keys()):
        print(f"  {k}: {stats[k]}")


if __name__ == "__main__":
    main()
