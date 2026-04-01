#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
財務指紋レポート（results.json）に基づき、CSV の正しい列から売上・利益・資本金を読み直して DB を上書きする。

- デフォルト DRY_RUN（--execute で実 UPDATE）
- unit_million でも生セルが「円っぽい」大きい整数なら二重に 100 万倍しない
- fixed_csv_3 配下を走査（later/, later_2/ は除外）

使い方:
  export POSTGRES_HOST=... POSTGRES_PASSWORD=...
  python scripts/apply_financial_fix.py --results reports/financial_fingerprint_20260322_201429/results.json
  python scripts/apply_financial_fix.py --results .../results.json --execute
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
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import import_full_update_fast as iff  # noqa: E402

DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD") or ""
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

EXCLUDE_DIR_MARKERS = ("/later/", "/later_2/")
YEN_LIKE_MIN = 1_000_000  # 素数値がこれ以上なら「既に円」とみなす（百万円列の二重換算を防ぐ）
NEIGHBOR_RADIUS = 6
# Type-Normal でも no_match 比率がこれを超えたら行ごとの DB ヒントでオフセット推定を試す
PER_ROW_NO_MATCH_RATIO = 0.35


def sniff_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            with open(path, "r", encoding=enc) as f:
                f.readline()
            return enc
        except (UnicodeDecodeError, UnicodeError):
            continue
    return "utf-8"


def revenue_folder_for_path(path: Path) -> str:
    if "unit_million" in {p.lower() for p in path.parts}:
        return "unit_million"
    return "unit_yen"


def iter_csv_files(root: Path) -> list[Path]:
    out: list[Path] = []
    for p in sorted(root.rglob("*.csv")):
        s = str(p).replace("\\", "/")
        if any(m in s for m in EXCLUDE_DIR_MARKERS):
            continue
        out.append(p)
    return out


def load_results_map(results_path: Path) -> dict[str, dict]:
    data = json.loads(results_path.read_text(encoding="utf-8"))
    m: dict[str, dict] = {}
    for r in data.get("results", []):
        if r.get("error"):
            continue
        p = r.get("path")
        if p:
            m[str(p).replace("\\", "/")] = r
    return m


def dominant_offset(off_map: dict | None) -> int | None:
    """{"0": 100, "-4": 50} から件数最大のオフセットを返す。空なら None。"""
    if not off_map:
        return None
    best_k, best_n = 0, -1
    for ks, nv in off_map.items():
        try:
            k = int(ks)
            n = int(nv)
        except (TypeError, ValueError):
            continue
        if n > best_n:
            best_n = n
            best_k = k
    return best_k if best_n >= 0 else None


def ranked_offset_candidates(
    off_map: dict | None,
    file_type: str | None,
    max_n: int = 8,
    *,
    deprioritize_zero: bool = False,
) -> list[int]:
    """出現回数の多い順にオフセット候補を列挙。file_type のシフトも先頭に入れる。"""
    ft_off = offset_from_file_type(file_type)
    out: list[int] = []
    if ft_off is not None:
        out.append(ft_off)
    if off_map:
        items = sorted(
            [(int(k), int(v)) for k, v in off_map.items()],
            key=lambda x: -x[1],
        )
        for k, _ in items:
            if k not in out:
                out.append(k)
    if 0 not in out:
        out.append(0)
    if deprioritize_zero and 0 in out:
        out = [x for x in out if x != 0] + [0]
    return out[:max_n]


def offset_from_file_type(file_type: str | None) -> int | None:
    if not file_type:
        return None
    m = re.match(r"^Type-Shift-L(\d+)$", file_type.strip())
    if m:
        return -int(m.group(1))
    m = re.match(r"^Type-Shift-R(\d+)$", file_type.strip())
    if m:
        return int(m.group(1))
    return None


def no_match_ratio(meta: dict | None) -> float:
    if not meta:
        return 0.0
    st = meta.get("stats") or {}
    n = st.get("rows_with_db_financial") or 0
    if n <= 0:
        return 0.0
    nm = (
        int(st.get("rev_no_match", 0))
        + int(st.get("cap_no_match", 0))
        + int(st.get("prof_no_match", 0))
    )
    return nm / max(1, 3 * n)


def col_indices_from_headers(headers: list[str]) -> tuple[int | None, int | None, int | None]:
    col_map = iff.resolve_current_map(headers)
    rev = cap = prof = None
    for i, c in col_map.items():
        if c == "latest_revenue" and rev is None:
            rev = i
        elif c == "capital_stock" and cap is None:
            cap = i
        elif c == "latest_profit" and prof is None:
            prof = i
    return rev, cap, prof


def find_corp_col(headers: list[str]) -> int | None:
    for i, h in enumerate(headers):
        if iff.normalize_header(h) == "法人番号":
            return i
    return None


def find_name_col(headers: list[str]) -> int | None:
    for i, h in enumerate(headers):
        nh = iff.normalize_header(h)
        if nh in ("会社名", "企業名"):
            return i
    return None


def exclude_indices(headers: list[str], col_map: dict[int, str]) -> set[int]:
    out: set[int] = set()
    for i, h in enumerate(headers):
        nh = iff.normalize_header(h)
        if nh in ("法人番号", "id", "会社id", "リストid"):
            out.add(i)
        if "社員" in nh or "従業員" in nh:
            out.add(i)
        if nh in ("オフィス数", "工場数", "店舗数"):
            out.add(i)
    for i, c in col_map.items():
        if c in ("employee_count", "office_count", "factory_count", "store_count"):
            out.add(i)
    return out


def plain_numeric_no_unit(s: str) -> float | None:
    """億・万・百万などの単位記号が無いときの素の数値（カンマ可）。単位付きは None。"""
    if not s or not str(s).strip():
        return None
    t = str(s).strip()
    if any(x in t for x in ("億", "万", "百万", "兆", "円")):
        return None
    t = t.replace(",", "").replace("，", "").replace("　", "").replace(" ", "")
    if not t or t.lower() in ("nan", "none", "null", "-"):
        return None
    num_part = re.sub(r"[^\d\.\-eE+]", "", t)
    if not num_part:
        return None
    try:
        f = float(num_part)
        if abs(f) > iff.INPUT_CAP:
            return None
        return f
    except (ValueError, OverflowError):
        return None


def smart_revenue_profit_yen(cell: str, revenue_folder: str) -> int | None:
    """
    インポータ準拠パースに加え、unit_million で素の数値が十分大きい場合は「既に円」とみなす。
    """
    p = iff.parse_revenue_profit(cell, revenue_folder=revenue_folder)
    raw = plain_numeric_no_unit(cell)
    if revenue_folder == "unit_million" and raw is not None and abs(raw) >= YEN_LIKE_MIN:
        v = int(raw)
        if abs(v) <= iff.BIGINT_SAFE_MAX:
            return v
    return p


def smart_capital_yen(cell: str) -> int | None:
    """資本金: 通常は千円×1000。素の数値が円単位っぽい大きさならそのまま円。"""
    p = iff.parse_capital_stock(cell)
    raw = plain_numeric_no_unit(cell)
    if raw is not None and abs(raw) >= YEN_LIKE_MIN:
        v = int(raw)
        if abs(v) <= iff.BIGINT_SAFE_MAX:
            return v
    return p


def bigint_close(a: int | None, b: int | None, rel: float = 1e-7) -> bool:
    if a is None or b is None:
        return False
    if a == b:
        return True
    return abs(a - b) <= max(1, int(abs(b) * rel))


def importer_style_parse_rev(cell: str, revenue_folder: str) -> int | None:
    """列ズレ推定用（DB に載った値と同じ換算ルール）。"""
    return iff.parse_revenue_profit(cell, revenue_folder=revenue_folder)


def find_row_offset_for_metric(
    row: list[str],
    base_idx: int | None,
    db_val: int | None,
    parser,
    exclude: set[int],
) -> int:
    """
    DB に保存されている誤値が、どの列を importer 風パースすると一致するか探索し、
    base_idx からのオフセットを返す。見つからなければ 0。
    """
    if db_val is None or base_idx is None:
        return 0
    best_j: int | None = None
    best_d = 10**9
    for j, cell in enumerate(row):
        if j in exclude:
            continue
        pv = parser(cell)
        if pv is not None and bigint_close(pv, db_val):
            d = abs(j - base_idx)
            if d < best_d:
                best_d = d
                best_j = j
    if best_j is None:
        return 0
    return best_j - base_idx


def read_financial_with_fallback(
    row: list[str],
    base_idx: int | None,
    file_offset: int,
    row_offset: int,
    revenue_folder: str,
    metric: str,
    neighbor_radius: int,
) -> int | None:
    """
    metric: 'revenue' | 'profit' | 'capital'
    読取インデックス = base + file_offset + row_offset を中心に近傍探索。
    """
    if base_idx is None:
        return None
    center = base_idx + file_offset + row_offset
    order = [center]
    for rad in range(1, neighbor_radius + 1):
        for sign in (-1, 1):
            j = center + sign * rad
            if j not in order:
                order.append(j)

    for j in order:
        if j < 0 or j >= len(row):
            continue
        cell = row[j]
        if metric == "capital":
            v = smart_capital_yen(cell)
        else:
            v = smart_revenue_profit_yen(cell, revenue_folder)
        if v is not None:
            return v
    return None


def read_metric_try_offsets(
    row: list[str],
    base_idx: int | None,
    file_offsets: list[int],
    row_offset: int,
    revenue_folder: str,
    metric: str,
    neighbor_radius: int,
) -> int | None:
    for fo in file_offsets:
        v = read_financial_with_fallback(
            row, base_idx, fo, row_offset, revenue_folder, metric, neighbor_radius
        )
        if v is not None:
            return v
    return None


def sql_update_value(new: int | None, old: int | None) -> int | None:
    """変更がなければ None（COALESCE で列を触らない）。"""
    if new is None:
        return None
    if old is not None and bigint_close(new, old):
        return None
    return new


def fmt_yen_human(yen: int | None) -> str:
    if yen is None:
        return "NULL"
    y = int(yen)
    if y == 0:
        return "0円"
    sign = "-" if y < 0 else ""
    y = abs(y)
    oku = y / 1e8
    if oku >= 1.0:
        return f"{sign}{oku:.2f}億円"
    if oku >= 0.01:
        return f"{sign}{oku:.3f}億円"
    man = y / 1e4
    if man >= 1.0:
        return f"{sign}{man:.1f}万円"
    return f"{sign}{y:,}円"


def rel_path_from_project(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve())).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def main() -> None:
    ap = argparse.ArgumentParser(description="CSV 正列から財務を読み直して DB を上書き")
    ap.add_argument(
        "--results",
        type=Path,
        required=True,
        help="analyze_csv_db_financial_fingerprint.py が出力した results.json",
    )
    ap.add_argument(
        "--root",
        type=Path,
        default=PROJECT_ROOT / "fixed_csv_3",
        help="CSV 探索ルート（デフォルト: fixed_csv_3）",
    )
    ap.add_argument(
        "--execute",
        action="store_true",
        help="指定時のみ UPDATE 実行（未指定は DRY_RUN）",
    )
    ap.add_argument("--limit-files", type=int, default=0)
    ap.add_argument("--limit-rows", type=int, default=0, help="ファイルあたり最大処理行数（0=無制限）")
    ap.add_argument(
        "--neighbor-radius",
        type=int,
        default=NEIGHBOR_RADIUS,
        help="空セル時の近傍探索幅",
    )
    args = ap.parse_args()

    dry_run = not args.execute
    if not DB_PASSWORD:
        print("エラー: POSTGRES_PASSWORD または PGPASSWORD が未設定です。", file=sys.stderr)
        sys.exit(2)

    if not args.results.is_file():
        print(f"エラー: results が見つかりません: {args.results}", file=sys.stderr)
        sys.exit(2)

    results_map = load_results_map(args.results)
    files = iter_csv_files(args.root)
    if args.limit_files:
        files = files[: args.limit_files]

    stats: Counter[str] = Counter()
    updates_log: list[tuple[str, str, str, str, str]] = []  # name, field, old, new, corp

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )

    try:
        for fp in files:
            rel = rel_path_from_project(fp)
            meta = results_map.get(rel)
            enc = sniff_encoding(fp)
            revenue_folder = revenue_folder_for_path(fp)

            with open(fp, "r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                try:
                    headers = next(reader)
                except StopIteration:
                    stats["file_empty"] += 1
                    continue

                col_map = iff.resolve_current_map(headers)
                rev_i, cap_i, prof_i = col_indices_from_headers(headers)
                corp_i = find_corp_col(headers)
                name_i = find_name_col(headers)
                exclude = exclude_indices(headers, col_map)

                if corp_i is None:
                    stats["file_no_corp_col"] += 1
                    continue

                ft = (meta or {}).get("file_type") if meta else None
                ol = (meta or {}).get("offset_lists") or {}
                nm_ratio = no_match_ratio(meta)
                expand_offsets = nm_ratio > PER_ROW_NO_MATCH_RATIO or (
                    ft == "Type-Shift-Unknown"
                )
                max_cand = 10 if expand_offsets else 3
                dep0 = expand_offsets
                cand_r = ranked_offset_candidates(
                    ol.get("revenue"), ft, max_n=max_cand, deprioritize_zero=dep0
                )
                cand_c = ranked_offset_candidates(
                    ol.get("capital"), ft, max_n=max_cand, deprioritize_zero=dep0
                )
                cand_p = ranked_offset_candidates(
                    ol.get("profit"), ft, max_n=max_cand, deprioritize_zero=dep0
                )
                use_per_row = ft == "Type-Normal" and nm_ratio > PER_ROW_NO_MATCH_RATIO

                rows: list[tuple[int, list[str]]] = []
                corp_nums: list[str] = []
                for lineno, row in enumerate(reader, start=2):
                    if len(row) != len(headers):
                        stats["row_column_mismatch"] += 1
                        continue
                    cn = iff.normalize_corp_num(row[corp_i] if corp_i < len(row) else "")
                    if not cn:
                        stats["row_invalid_corp"] += 1
                        continue
                    rows.append((lineno, row))
                    corp_nums.append(cn)
                    if args.limit_rows and len(rows) >= args.limit_rows:
                        break

                if not corp_nums:
                    stats["file_no_valid_rows"] += 1
                    continue

                db_map: dict[str, dict] = {}
                chunk = 600
                for i in range(0, len(corp_nums), chunk):
                    part = corp_nums[i : i + chunk]
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute(
                            """
                            SELECT corporate_number, name,
                                   latest_revenue, capital_stock, latest_profit
                            FROM companies
                            WHERE corporate_number = ANY(%s)
                            """,
                            (part,),
                        )
                        for r in cur.fetchall():
                            db_map[str(r["corporate_number"])] = dict(r)

                pending: list[tuple[int | None, int | None, int | None, str]] = []

                for (_lineno, row), cn in zip(rows, corp_nums):
                    dbrow = db_map.get(cn)
                    if not dbrow:
                        stats["row_no_db_match"] += 1
                        continue

                    name = (dbrow.get("name") or "").strip() or (
                        (row[name_i] or "").strip() if name_i is not None else ""
                    )

                    r_off = c_off = p_off = 0
                    if use_per_row:
                        db_r = dbrow.get("latest_revenue")
                        db_c = dbrow.get("capital_stock")
                        db_p = dbrow.get("latest_profit")
                        if db_r is not None:
                            db_r = int(db_r)
                        if db_c is not None:
                            db_c = int(db_c)
                        if db_p is not None:
                            db_p = int(db_p)
                        r_off = find_row_offset_for_metric(
                            row,
                            rev_i,
                            db_r,
                            lambda c: importer_style_parse_rev(c, revenue_folder),
                            exclude,
                        )
                        c_off = find_row_offset_for_metric(
                            row, cap_i, db_c, iff.parse_capital_stock, exclude
                        )
                        p_off = find_row_offset_for_metric(
                            row,
                            prof_i,
                            db_p,
                            lambda c: importer_style_parse_rev(c, revenue_folder),
                            exclude,
                        )

                    new_r = read_metric_try_offsets(
                        row,
                        rev_i,
                        cand_r,
                        r_off,
                        revenue_folder,
                        "revenue",
                        args.neighbor_radius,
                    )
                    new_c = read_metric_try_offsets(
                        row,
                        cap_i,
                        cand_c,
                        c_off,
                        revenue_folder,
                        "capital",
                        args.neighbor_radius,
                    )
                    new_p = read_metric_try_offsets(
                        row,
                        prof_i,
                        cand_p,
                        p_off,
                        revenue_folder,
                        "profit",
                        args.neighbor_radius,
                    )

                    if new_r is None and new_c is None and new_p is None:
                        stats["row_all_finance_empty"] += 1
                        continue

                    old_r = dbrow.get("latest_revenue")
                    old_c = dbrow.get("capital_stock")
                    old_p = dbrow.get("latest_profit")
                    if old_r is not None:
                        old_r = int(old_r)
                    if old_c is not None:
                        old_c = int(old_c)
                    if old_p is not None:
                        old_p = int(old_p)

                    pr = sql_update_value(new_r, old_r)
                    pc = sql_update_value(new_c, old_c)
                    pp = sql_update_value(new_p, old_p)

                    if pr is None and pc is None and pp is None:
                        stats["row_no_change"] += 1
                        continue

                    tag = "UPDATE予定" if dry_run else "UPDATE"
                    if pr is not None:
                        print(
                            f"[{tag}] {name} ({cn}): "
                            f"売上 {fmt_yen_human(old_r)} -> {fmt_yen_human(new_r)}"
                        )
                    if pc is not None:
                        print(
                            f"[{tag}] {name} ({cn}): "
                            f"資本金 {fmt_yen_human(old_c)} -> {fmt_yen_human(new_c)}"
                        )
                    if pp is not None:
                        print(
                            f"[{tag}] {name} ({cn}): "
                            f"利益 {fmt_yen_human(old_p)} -> {fmt_yen_human(new_p)}"
                        )

                    stats["row_will_update"] += 1
                    pending.append((pr, pc, pp, cn))

                if not dry_run and pending:
                    with conn.cursor() as cur:
                        execute_batch(
                            cur,
                            """
                            UPDATE companies
                            SET latest_revenue = COALESCE(%s, latest_revenue),
                                capital_stock = COALESCE(%s, capital_stock),
                                latest_profit = COALESCE(%s, latest_profit)
                            WHERE corporate_number = %s
                            """,
                            pending,
                            page_size=200,
                        )
                    conn.commit()
                    stats["batches_committed"] += 1

            stats["files_processed"] += 1
            if meta is None:
                stats["files_without_results_entry"] += 1

    finally:
        conn.close()

    print()
    print("=== スキップ・統計 ===")
    for k in sorted(stats.keys()):
        print(f"  {k}: {stats[k]}")
    if dry_run:
        print()
        print("（DRY_RUN）--execute を付けると実際に UPDATE します。")


if __name__ == "__main__":
    main()
