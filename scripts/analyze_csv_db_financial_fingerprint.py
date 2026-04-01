#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DB の財務（latest_revenue / capital_stock / latest_profit）と元 CSV を行単位で突合し、
ヘッダー列の値と DB が一致しない場合に「行内別列＋インポート換算」で説明できるか（列ズレの指紋）を調べる。

import_full_update_fast の換算ルールに追従:
  - 売上・利益: unit_million なら素の数値 × 1_000_000、unit_yen なら × 1000（単位記号なし時）
  - 資本金: 常に unit_yen 扱い（素の数値 × 1000）

使い方:
  export POSTGRES_HOST=... POSTGRES_PASSWORD=...  # 他は audit_csv_vs_db_integrity と同様
  python scripts/analyze_csv_db_financial_fingerprint.py
  python scripts/analyze_csv_db_financial_fingerprint.py --root fixed_csv_3 --limit-files 5
  python scripts/analyze_csv_db_financial_fingerprint.py --name-fallback
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

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

EXCLUDE_FRAGMENT = "/later/"


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
    parts = {p.lower() for p in path.parts}
    if "unit_million" in parts:
        return "unit_million"
    return "unit_yen"


def bigint_close(a: int | None, b: int | None, rel: float = 1e-7) -> bool:
    if a is None or b is None:
        return False
    if a == b:
        return True
    return abs(a - b) <= max(1, int(abs(b) * rel))


def col_indices_from_map(col_map: dict[int, str]) -> tuple[int | None, int | None, int | None]:
    rev = cap = prof = None
    for i, c in col_map.items():
        if c == "latest_revenue" and rev is None:
            rev = i
        elif c == "capital_stock" and cap is None:
            cap = i
        elif c == "latest_profit" and prof is None:
            prof = i
    return rev, cap, prof


def header_at(headers: list[str], i: int | None) -> str:
    if i is None or i < 0 or i >= len(headers):
        return ""
    return (headers[i] or "").strip()


def exclude_fingerprint_indices(headers: list[str], col_map: dict[int, str]) -> set[int]:
    """誤一致を減らすため指紋探索から除外する列。"""
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


@dataclass
class MetricScan:
    db_value: int | None
    expected_idx: int | None
    pred_from_header: int | None
    match_idx: int | None
    offset: int | None  # match_idx - expected_idx
    status: str  # no_db | match_header | match_alt | no_match | empty_csv


def scan_metric(
    row: list[str],
    headers: list[str],
    expected_idx: int | None,
    db_value: int | None,
    parser,  # (cell: str) -> int | None
    exclude: set[int],
) -> MetricScan:
    if db_value is None:
        return MetricScan(None, expected_idx, None, None, None, "no_db")
    if expected_idx is None:
        return MetricScan(db_value, None, None, None, None, "empty_csv")

    cell_e = row[expected_idx] if expected_idx < len(row) else ""
    pred_h = parser(cell_e)
    if bigint_close(pred_h, db_value):
        return MetricScan(db_value, expected_idx, pred_h, expected_idx, 0, "match_header")

    best_j: int | None = None
    best_dist = 10**9
    for j, cell in enumerate(row):
        if j in exclude:
            continue
        p = parser(cell)
        if p is not None and bigint_close(p, db_value):
            d = abs(j - expected_idx)
            if d < best_dist:
                best_dist = d
                best_j = j

    if best_j is not None:
        return MetricScan(
            db_value,
            expected_idx,
            pred_h,
            best_j,
            best_j - expected_idx,
            "match_alt",
        )

    return MetricScan(db_value, expected_idx, pred_h, None, None, "no_match")


def fetch_db_map(conn, corp_ids: list[str]) -> dict[str, dict]:
    """corporate_number -> row"""
    if not corp_ids:
        return {}
    out: dict[str, dict] = {}
    chunk = 800
    for i in range(0, len(corp_ids), chunk):
        part = corp_ids[i : i + chunk]
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT corporate_number, latest_revenue, capital_stock, latest_profit, name, prefecture
                FROM companies
                WHERE corporate_number = ANY(%s)
                """,
                (part,),
            )
            for r in cur.fetchall():
                cn = r["corporate_number"]
                if cn:
                    out[str(cn)] = dict(r)
    return out


def fetch_by_name_pref(conn, pairs: list[tuple[str, str]]) -> dict[tuple[str, str], dict]:
    """(name, pref) -> row（複数行ある場合は先頭のみ・警告用）"""
    if not pairs:
        return {}
    out: dict[tuple[str, str], dict] = {}
    seen: set[tuple[str, str]] = set()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        for name, pref in pairs:
            key = (name.strip(), (pref or "").strip())
            if key in seen:
                continue
            seen.add(key)
            cur.execute(
                """
                SELECT corporate_number, latest_revenue, capital_stock, latest_profit, name, prefecture
                FROM companies
                WHERE TRIM(name) = %s AND COALESCE(TRIM(prefecture), '') = %s
                LIMIT 2
                """,
                (key[0], key[1]),
            )
            rows = cur.fetchall()
            if len(rows) == 1:
                r = dict(rows[0])
                out[key] = r
    return out


def classify_file_merged(
    rev_offsets: list[int],
    cap_offsets: list[int],
    prof_offsets: list[int],
    n_analyzed: int,
    threshold: float,
) -> str:
    """売上/資本/利益で得られたオフセットをまとめて多数決（同一行が3票になりうる＝シフトの強いシグナル）。"""
    if n_analyzed == 0:
        return "Type-NoData"
    merged = rev_offsets + cap_offsets + prof_offsets
    if not merged:
        return "Type-NoFinancialMatch"
    c = Counter(merged)
    mode, cnt = c.most_common(1)[0]
    ratio = cnt / len(merged)
    if ratio < threshold:
        return "Type-Shift-Unknown"
    if mode == 0:
        return "Type-Normal"
    if mode < 0:
        return f"Type-Shift-L{abs(mode)}"
    return f"Type-Shift-R{mode}"


def _rel_project(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def iter_csv_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for p in sorted(root.rglob("*.csv")):
        if EXCLUDE_FRAGMENT in str(p).replace("\\", "/"):
            continue
        files.append(p)
    return files


def analyze_file(path: Path, conn, name_fallback: bool) -> dict:
    enc = sniff_encoding(path)
    revenue_folder = revenue_folder_for_path(path)
    with open(path, "r", encoding=enc, newline="") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            return {"error": "empty", "path": str(path)}

        col_map = iff.resolve_current_map(headers)
        rev_i, cap_i, prof_i = col_indices_from_map(col_map)
        corp_i = None
        for i, h in enumerate(headers):
            if iff.normalize_header(h) == "法人番号":
                corp_i = i
                break
        name_i = next((i for i, h in enumerate(headers) if iff.normalize_header(h) in ("会社名", "企業名")), None)
        pref_i = next((i for i, h in enumerate(headers) if iff.normalize_header(h) == "都道府県"), None)

        exclude = exclude_fingerprint_indices(headers, col_map)

        rows_data: list[dict] = []
        corp_keys: list[str] = []
        name_pref_keys: list[tuple[str, str]] = []

        for lineno, row in enumerate(reader, start=2):
            if len(row) != len(headers):
                continue
            cn_raw = row[corp_i] if corp_i is not None and corp_i < len(row) else ""
            cn = iff.normalize_corp_num(cn_raw)
            name = (row[name_i] or "").strip() if name_i is not None else ""
            pref = (row[pref_i] or "").strip() if pref_i is not None else ""

            if cn:
                corp_keys.append(cn)
            elif name_fallback and name:
                name_pref_keys.append((name, pref))

            rows_data.append(
                {
                    "lineno": lineno,
                    "corp": cn,
                    "name": name,
                    "pref": pref,
                    "row": row,
                }
            )

    db_by_corp = fetch_db_map(conn, list(set(corp_keys)))
    db_by_np = fetch_by_name_pref(conn, name_pref_keys) if name_fallback else {}

    rev_offsets: list[int] = []
    cap_offsets: list[int] = []
    prof_offsets: list[int] = []
    stats = Counter()
    samples: list[dict] = []

    def cap_parser(cell: str):
        return iff.parse_capital_stock(cell)

    def rev_parser(cell: str):
        return iff.parse_revenue_profit(cell, revenue_folder=revenue_folder)

    prof_parser = rev_parser

    for rec in rows_data:
        cn = rec["corp"]
        dbrow = None
        if cn and cn in db_by_corp:
            dbrow = db_by_corp[cn]
        elif name_fallback:
            dbrow = db_by_np.get((rec["name"], rec["pref"]))

        if not dbrow:
            stats["no_db_row"] += 1
            continue

        row = rec["row"]
        rv = dbrow.get("latest_revenue")
        cv = dbrow.get("capital_stock")
        pv = dbrow.get("latest_profit")
        if rv is not None:
            rv = int(rv)
        if cv is not None:
            cv = int(cv)
        if pv is not None:
            pv = int(pv)

        has_fin = any(x is not None for x in (rv, cv, pv))
        if not has_fin:
            stats["db_financial_all_null"] += 1
            continue

        stats["rows_with_db_financial"] += 1

        sr = scan_metric(row, headers, rev_i, rv, rev_parser, exclude)
        sc = scan_metric(row, headers, cap_i, cv, cap_parser, exclude)
        sp = scan_metric(row, headers, prof_i, pv, prof_parser, exclude)

        for m, label in ((sr, "rev"), (sc, "cap"), (sp, "prof")):
            stats[f"{label}_{m.status}"] += 1

        if sr.offset is not None and sr.status in ("match_header", "match_alt"):
            rev_offsets.append(sr.offset)
        if sc.offset is not None and sc.status in ("match_header", "match_alt"):
            cap_offsets.append(sc.offset)
        if sp.offset is not None and sp.status in ("match_header", "match_alt"):
            prof_offsets.append(sp.offset)

        interesting = (
            sr.status == "match_alt"
            or sc.status == "match_alt"
            or sp.status == "match_alt"
            or sr.status == "no_match"
            or sc.status == "no_match"
            or sp.status == "no_match"
        )
        if interesting and len(samples) < 12:
            samples.append(
                {
                    "lineno": rec["lineno"],
                    "corporate_number": dbrow.get("corporate_number"),
                    "name": rec["name"][:40],
                    "revenue": asdict(sr),
                    "capital": asdict(sc),
                    "profit": asdict(sp),
                    "header_revenue_col": header_at(headers, rev_i),
                    "header_capital_col": header_at(headers, cap_i),
                    "alt_revenue_header": header_at(headers, sr.match_idx),
                    "alt_capital_header": header_at(headers, sc.match_idx),
                    "alt_profit_header": header_at(headers, sp.match_idx),
                }
            )

    n = stats["rows_with_db_financial"]
    ftype = classify_file_merged(rev_offsets, cap_offsets, prof_offsets, n, threshold=0.72)

    mismatch_alt = (
        stats["rev_match_alt"] + stats["cap_match_alt"] + stats["prof_match_alt"]
    )
    mismatch_none = stats["rev_no_match"] + stats["cap_no_match"] + stats["prof_no_match"]

    return {
        "path": _rel_project(path),
        "revenue_folder": revenue_folder,
        "n_cols": len(headers),
        "indices": {"revenue": rev_i, "capital": cap_i, "profit": prof_i, "corporate_number": corp_i},
        "file_type": ftype,
        "stats": dict(stats),
        "offset_lists": {
            "revenue": dict(Counter(rev_offsets)),
            "capital": dict(Counter(cap_offsets)),
            "profit": dict(Counter(prof_offsets)),
        },
        "priority_score": mismatch_alt + mismatch_none * 2,
        "samples": samples,
    }


def main():
    ap = argparse.ArgumentParser(description="CSV↔DB 財務の列ズレ指紋分析")
    ap.add_argument("--root", type=Path, default=PROJECT_ROOT / "fixed_csv_3")
    ap.add_argument("--out-dir", type=Path, default=None)
    ap.add_argument("--limit-files", type=int, default=0)
    ap.add_argument("--name-fallback", action="store_true", help="法人番号が取れない行を 会社名+都道府県 で突合（重複注意）")
    args = ap.parse_args()

    if not DB_PASSWORD:
        print("エラー: POSTGRES_PASSWORD または PGPASSWORD が未設定です。", file=sys.stderr)
        sys.exit(2)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir or (PROJECT_ROOT / "reports" / f"financial_fingerprint_{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)

    files = iter_csv_files(args.root)
    if args.limit_files:
        files = files[: args.limit_files]

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )
    results: list[dict] = []
    try:
        for fp in files:
            try:
                results.append(analyze_file(fp, conn, args.name_fallback))
            except Exception as e:
                results.append({"path": str(fp), "error": repr(e)})
    finally:
        conn.close()

    # 優先度ソート（影響っぽいスコア）
    def sort_key(r: dict):
        if r.get("error"):
            return (0, r["path"])
        return (-r.get("priority_score", 0), r["path"])

    results_sorted = sorted(results, key=sort_key)

    summary_md = [
        "# CSV ↔ DB 財務「数字の指紋」・列ズレ分析",
        "",
        f"- 対象ルート: `{args.root}`",
        f"- ファイル数: {len(files)}",
        f"- 換算: import_full_update_fast に同じ（資本金×1000円、売上/利益はフォルダで×10^6 or ×1000）",
        "",
        "## 修正優先度（スコア順）",
        "",
        "| 優先 | ファイル | 分類 | alt列一致 | no_match | 売上オフセット分布 |",
        "|---|---|---|---:|---:|---|",
    ]
    for r in results_sorted:
        if r.get("error"):
            summary_md.append(f"| - | `{r['path']}` | ERROR | | | `{r['error']}` |")
            continue
        st = r.get("stats", {})
        alt = st.get("rev_match_alt", 0) + st.get("cap_match_alt", 0) + st.get("prof_match_alt", 0)
        nm = st.get("rev_no_match", 0) + st.get("cap_no_match", 0) + st.get("prof_no_match", 0)
        od = r.get("offset_lists", {}).get("revenue", {})
        od_s = json.dumps(od, ensure_ascii=False) if od else "{}"
        summary_md.append(
            f"| {r.get('priority_score', 0)} | `{r['path']}` | {r.get('file_type')} | {alt} | {nm} | {od_s} |"
        )

    summary_md.extend(
        [
            "",
            "## 分類の意味",
            "",
            "- **Type-Normal**: 支配的オフセットが 0（ヘッダー列のパース値が DB と一致する行が多い）",
            "- **Type-Shift-Lk**: 売上・資本・利益で同じ負のオフセット k が ~72% 以上で一致（データが左にずれている＝本来より左の列に正値）",
            "- **Type-Shift-Unknown**: 行ごとにオフセットがバラバラ、または項目間で食い違い",
            "",
            "詳細 JSON: `results.json`",
        ]
    )

    (out_dir / "README.md").write_text("\n".join(summary_md), encoding="utf-8")
    (out_dir / "results.json").write_text(
        json.dumps(
            {
                "generated_at_utc": ts,
                "root": str(args.root),
                "results": results_sorted,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"Wrote {out_dir / 'README.md'} and results.json")


if __name__ == "__main__":
    main()
