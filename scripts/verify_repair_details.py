#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修復後データの「答え合わせ」検証。

仕様:
- DB: id LIKE 'np%' AND updated_at >= 2026-04-14 14:00:00Z からランダム100件
- CSV: fixed_csv_3 配下を読み込み、法人番号 or 正規化社名+代表者名で紐付け
- 比較: name, corporate_number, phone_number, prefecture, address, representative_name, overview
- 出力: カラム一致率 + 不一致詳細（DB値/CSV期待値）
"""

from __future__ import annotations

import argparse
import csv
import random
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

try:
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import repair_companies_column_shift_from_fixed_csv3 as rep


TARGET_COLUMNS = (
    "name",
    "corporate_number",
    "phone_number",
    "prefecture",
    "address",
    "representative_name",
    "overview",
)

CORP_HEADERS = ["法人番号", "corporate_number", "会社法人等番号", "企業法人番号"]
NAME_HEADERS = ["会社名", "企業名", "name", "法人名", "商号"]
PHONE_HEADERS = ["電話番号", "電話番号(窓口)", "TEL", "tel", "phone_number", "会社電話"]
PREF_HEADERS = ["都道府県", "prefecture", "会社都道府県"]
ADDRESS_HEADERS = ["会社住所", "住所", "address", "所在地"]
REP_HEADERS = ["代表者名", "代表者", "代表取締役"]
OVERVIEW_HEADERS = ["概要", "概況", "overview", "説明"]


@dataclass
class CsvPayload:
    name: str
    corporate_number: str
    phone_number: str
    prefecture: str
    address: str
    representative_name: str
    overview: str
    source_file: str
    source_line: int


def norm_text(v: Optional[str]) -> str:
    if v is None:
        return ""
    return unicodedata.normalize("NFKC", str(v)).strip()


def raw_text(v: Optional[str]) -> str:
    if v is None:
        return ""
    return str(v)


def norm_name_key(v: Optional[str]) -> str:
    return re.sub(r"\s+", "", norm_text(v).lower())


def norm_corp(v: Optional[str]) -> str:
    s = re.sub(r"\D", "", norm_text(v))
    if len(s) == 12:
        s = "0" + s
    return s if len(s) == 13 else ""


def extract_corp_candidates(*vals: Optional[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for v in vals:
        digits = re.sub(r"\D", "", norm_text(v))
        if not digits:
            continue
        if len(digits) == 13:
            candidates = [digits]
        elif len(digits) > 13:
            candidates = [digits[i : i + 13] for i in range(0, len(digits) - 12)]
        else:
            candidates = []
        for c in candidates:
            k = norm_corp(c)
            if k and k not in seen:
                seen.add(k)
                out.append(k)
    return out


def pick_header(headers: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    hs = [str(h) for h in headers]
    for c in candidates:
        if c in hs:
            return c
    return None


def get_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            with path.open("r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "utf-8"


def iter_csv_paths(csv_root: Path) -> List[Path]:
    preferred = [csv_root / "1.csv", csv_root / "2.csv"]
    direct = [p for p in preferred if p.is_file()]
    if direct:
        return direct
    return sorted(csv_root.rglob("*.csv"), key=lambda p: p.stat().st_mtime)


def csv_cell(row: Dict[str, str], key: Optional[str]) -> str:
    if not key:
        return ""
    v = row.get(key)
    return "" if v is None else str(v)


def load_csv_indexes(csv_root: Path) -> Tuple[Dict[str, CsvPayload], Dict[Tuple[str, str], CsvPayload]]:
    by_corp: Dict[str, CsvPayload] = {}
    by_name_rep: Dict[Tuple[str, str], CsvPayload] = {}
    paths = iter_csv_paths(csv_root)
    if not paths:
        raise SystemExit(f"CSV が見つかりません: {csv_root}")

    for fp in paths:
        enc = get_encoding(fp)
        with fp.open("r", encoding=enc, newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                continue
            name_h = pick_header(reader.fieldnames, NAME_HEADERS)
            corp_h = pick_header(reader.fieldnames, CORP_HEADERS)
            phone_h = pick_header(reader.fieldnames, PHONE_HEADERS)
            pref_h = pick_header(reader.fieldnames, PREF_HEADERS)
            addr_h = pick_header(reader.fieldnames, ADDRESS_HEADERS)
            rep_h = pick_header(reader.fieldnames, REP_HEADERS)
            ov_h = pick_header(reader.fieldnames, OVERVIEW_HEADERS)
            if not name_h and not corp_h:
                continue

            for line_no, raw in enumerate(reader, start=2):
                row = {str(k): ("" if v is None else str(v)) for k, v in raw.items()}
                payload = CsvPayload(
                    name=raw_text(csv_cell(row, name_h)),
                    corporate_number=norm_corp(csv_cell(row, corp_h)),
                    phone_number=raw_text(csv_cell(row, phone_h)),
                    prefecture=raw_text(csv_cell(row, pref_h)),
                    address=raw_text(csv_cell(row, addr_h)),
                    representative_name=raw_text(csv_cell(row, rep_h)),
                    overview=raw_text(csv_cell(row, ov_h)),
                    source_file=str(fp),
                    source_line=line_no,
                )
                if payload.corporate_number:
                    by_corp[payload.corporate_number] = payload
                nk = norm_name_key(payload.name)
                rk = norm_name_key(payload.representative_name)
                if nk:
                    by_name_rep[(nk, rk)] = payload

    return by_corp, by_name_rep


def fetch_candidates(cur, since_ts: str) -> List[Tuple]:
    cur.execute(
        """
        SELECT
            id,
            name,
            corporate_number,
            phone_number,
            prefecture,
            address,
            representative_name,
            overview,
            updated_at
        FROM companies
        WHERE id LIKE 'np%%'
          AND updated_at >= %s
        """,
        (since_ts,),
    )
    return cur.fetchall()


def choose_sample(rows: List[Tuple], n: int, seed: int) -> List[Tuple]:
    if len(rows) <= n:
        return rows
    rnd = random.Random(seed)
    return rnd.sample(rows, n)


def link_payload(
    db_row: Tuple,
    by_corp: Dict[str, CsvPayload],
    by_name_rep: Dict[Tuple[str, str], CsvPayload],
) -> Tuple[Optional[CsvPayload], str]:
    db_name = db_row[1]
    db_corp = db_row[2]
    db_phone = db_row[3]
    db_addr = db_row[5]
    db_rep = db_row[6]

    for k in extract_corp_candidates(db_corp, db_phone, db_addr):
        p = by_corp.get(k)
        if p:
            return p, f"corp:{k}"

    nk = norm_name_key(db_name)
    rk = norm_name_key(db_rep)
    if nk:
        p = by_name_rep.get((nk, rk))
        if p:
            return p, "name+rep"
        p = by_name_rep.get((nk, ""))
        if p:
            return p, "name+rep(empty-rep)"
    return None, "unmatched"


def db_col_value(row: Tuple, col: str) -> str:
    idx = {
        "name": 1,
        "corporate_number": 2,
        "phone_number": 3,
        "prefecture": 4,
        "address": 5,
        "representative_name": 6,
        "overview": 7,
    }[col]
    return raw_text(row[idx])


def csv_col_value(payload: CsvPayload, col: str) -> str:
    return raw_text(getattr(payload, col))


def main() -> None:
    ap = argparse.ArgumentParser(description="Verify repaired np records against fixed_csv_3")
    ap.add_argument("--csv-root", type=Path, default=Path("fixed_csv_3"))
    ap.add_argument("--since", default="2026-04-14 14:00:00Z")
    ap.add_argument("--sample-size", type=int, default=100)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--max-diff-lines", type=int, default=500)
    args = ap.parse_args()

    if not args.csv_root.is_dir():
        raise SystemExit(f"CSV root が見つかりません: {args.csv_root}")

    by_corp, by_name_rep = load_csv_indexes(args.csv_root)
    print(f"[INFO] CSV index loaded: corp={len(by_corp)} name+rep={len(by_name_rep)}")

    conn = rep.get_connection()
    try:
        cur = conn.cursor()
        candidates = fetch_candidates(cur, args.since)
    finally:
        conn.close()

    print(f"[INFO] DB candidates: {len(candidates)} rows (id LIKE 'np%', updated_at >= {args.since})")
    sample = choose_sample(candidates, args.sample_size, args.seed)
    print(f"[INFO] Sample size: {len(sample)}")
    if not sample:
        print("[WARN] 対象が 0 件です")
        return

    matched = 0
    unmatched = 0
    success = {c: 0 for c in TARGET_COLUMNS}
    total = {c: 0 for c in TARGET_COLUMNS}
    diffs: List[str] = []

    for row in sample:
        db_id = row[0]
        payload, method = link_payload(row, by_corp, by_name_rep)
        if not payload:
            unmatched += 1
            diffs.append(f"[UNMATCHED] id={db_id} db_name={raw_text(row[1])!r}")
            continue
        matched += 1
        for col in TARGET_COLUMNS:
            dbv = db_col_value(row, col)
            csvv = csv_col_value(payload, col)
            total[col] += 1
            if dbv == csvv:
                success[col] += 1
            else:
                diffs.append(
                    f"[DIFF] id={db_id} match={method} col={col} "
                    f"DB={dbv!r} CSV={csvv!r} src={payload.source_file}:{payload.source_line}"
                )

    print("")
    print("=== 検証サマリ ===")
    print(f"sampled={len(sample)} matched={matched} unmatched={unmatched}")
    print("")
    print("=== カラム一致成功率 ===")
    for col in TARGET_COLUMNS:
        d = total[col]
        rate = (success[col] / d * 100.0) if d else 0.0
        print(f"{col:20s} {success[col]:4d}/{d:<4d}  {rate:6.2f}%")

    print("")
    print("=== 差分詳細 ===")
    if not diffs:
        print("差分なし")
    else:
        for line in diffs[: args.max_diff_lines]:
            print(line)
        if len(diffs) > args.max_diff_lines:
            print(f"... (省略) total_diff_lines={len(diffs)}")


if __name__ == "__main__":
    main()
