#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修復後データの品質検証スクリプト。

要件:
- DB から updated_at が指定時刻以降の np% レコードをランダム取得
- fixed_csv_3 配下 CSV と法人番号 or 社名+代表者名で紐付け
- DB と CSV の対象カラム差分を検証し、一致率と差分詳細を出力
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
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

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
    "address",
    "representative_name",
    "overview",
)

CORP_HEADERS = ["法人番号", "corporate_number", "会社法人等番号", "企業法人番号"]
NAME_HEADERS = ["会社名", "企業名", "name", "法人名", "商号"]
PHONE_HEADERS = ["電話番号", "電話番号(窓口)", "TEL", "tel", "phone_number", "会社電話"]
ADDRESS_HEADERS = ["会社住所", "住所", "address", "所在地"]
REP_HEADERS = ["代表者名", "代表者", "代表取締役"]
OVERVIEW_HEADERS = ["概要", "概況", "overview", "説明"]


@dataclass
class CsvPayload:
    name: str
    corporate_number: str
    phone_number: str
    address: str
    representative_name: str
    overview: str
    source_file: str
    source_line: int


def norm_text(v: Optional[str]) -> str:
    if v is None:
        return ""
    return unicodedata.normalize("NFKC", str(v)).strip()


def norm_name_key(v: Optional[str]) -> str:
    return re.sub(r"\s+", "", norm_text(v).lower())


def norm_corp(v: Optional[str]) -> str:
    d = re.sub(r"\D", "", norm_text(v))
    if len(d) == 12:
        d = "0" + d
    return d if len(d) == 13 else ""


def extract_corp_candidates(*vals: Optional[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for v in vals:
        d = re.sub(r"\D", "", norm_text(v))
        if not d:
            continue
        if len(d) == 13:
            cands = [d]
        elif len(d) > 13:
            cands = [d[i : i + 13] for i in range(0, len(d) - 12)]
        else:
            cands = []
        for c in cands:
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
            addr_h = pick_header(reader.fieldnames, ADDRESS_HEADERS)
            rep_h = pick_header(reader.fieldnames, REP_HEADERS)
            ov_h = pick_header(reader.fieldnames, OVERVIEW_HEADERS)
            if not name_h and not corp_h:
                continue
            for idx, raw in enumerate(reader, start=2):
                row = {str(k): ("" if v is None else str(v)) for k, v in raw.items()}
                payload = CsvPayload(
                    name=norm_text(csv_cell(row, name_h)),
                    corporate_number=norm_corp(csv_cell(row, corp_h)),
                    phone_number=norm_text(csv_cell(row, phone_h)),
                    address=norm_text(csv_cell(row, addr_h)),
                    representative_name=norm_text(csv_cell(row, rep_h)),
                    overview=norm_text(csv_cell(row, ov_h)),
                    source_file=str(fp),
                    source_line=idx,
                )
                ck = payload.corporate_number
                if ck:
                    by_corp[ck] = payload
                nk = norm_name_key(payload.name)
                rk = norm_name_key(payload.representative_name)
                if nk:
                    by_name_rep[(nk, rk)] = payload

    return by_corp, by_name_rep


def fetch_recent_np_rows(cur, since_ts: str) -> List[Tuple]:
    cur.execute(
        """
        SELECT
            id,
            name,
            corporate_number,
            phone_number,
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


def choose_sample(rows: List[Tuple], sample_size: int, seed: int) -> List[Tuple]:
    if len(rows) <= sample_size:
        return rows
    rnd = random.Random(seed)
    return rnd.sample(rows, sample_size)


def link_csv_payload(
    db_row: Tuple,
    by_corp: Dict[str, CsvPayload],
    by_name_rep: Dict[Tuple[str, str], CsvPayload],
) -> Tuple[Optional[CsvPayload], str]:
    db_name = db_row[1]
    db_corp = db_row[2]
    db_phone = db_row[3]
    db_addr = db_row[4]
    db_rep = db_row[5]

    for k in extract_corp_candidates(db_corp, db_phone, db_addr):
        payload = by_corp.get(k)
        if payload:
            return payload, f"corp:{k}"

    nk = norm_name_key(db_name)
    rk = norm_name_key(db_rep)
    if nk:
        payload = by_name_rep.get((nk, rk))
        if payload:
            return payload, "name+rep"
        payload = by_name_rep.get((nk, ""))
        if payload:
            return payload, "name+rep(empty-rep)"

    return None, "unmatched"


def payload_value(payload: CsvPayload, col: str) -> str:
    return getattr(payload, col)


def db_value(db_row: Tuple, col: str) -> str:
    mapping = {
        "name": 1,
        "corporate_number": 2,
        "phone_number": 3,
        "address": 4,
        "representative_name": 5,
        "overview": 6,
    }
    idx = mapping[col]
    return norm_text(db_row[idx])


def main() -> None:
    ap = argparse.ArgumentParser(description="Verify repair quality against fixed_csv_3")
    ap.add_argument("--csv-root", type=Path, default=Path("fixed_csv_3"))
    ap.add_argument("--since", default="2026-04-14 23:00:00")
    ap.add_argument("--sample-size", type=int, default=200)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--max-diff-lines", type=int, default=300)
    args = ap.parse_args()

    if not args.csv_root.is_dir():
        raise SystemExit(f"CSV root が見つかりません: {args.csv_root}")

    by_corp, by_name_rep = load_csv_indexes(args.csv_root)
    print(f"[INFO] CSV index loaded: corp={len(by_corp)} name+rep={len(by_name_rep)}")

    conn = rep.get_connection()
    try:
        cur = conn.cursor()
        candidates = fetch_recent_np_rows(cur, args.since)
    finally:
        conn.close()

    print(f"[INFO] DB candidates: {len(candidates)} rows (updated_at >= {args.since}, id LIKE 'np%')")
    sample_rows = choose_sample(candidates, args.sample_size, args.seed)
    print(f"[INFO] Sample size: {len(sample_rows)}")

    if not sample_rows:
        print("[WARN] 検証対象が 0 件です。")
        return

    compared = 0
    unmatched = 0
    success = {c: 0 for c in TARGET_COLUMNS}
    total = {c: 0 for c in TARGET_COLUMNS}
    diffs: List[str] = []

    for row in sample_rows:
        payload, method = link_csv_payload(row, by_corp, by_name_rep)
        db_id = row[0]
        if not payload:
            unmatched += 1
            diffs.append(f"[UNMATCHED] id={db_id} method={method} db_name={norm_text(row[1])}")
            continue

        compared += 1
        for col in TARGET_COLUMNS:
            dbv = db_value(row, col)
            csvv = payload_value(payload, col)
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
    print(f"sampled={len(sample_rows)} matched={compared} unmatched={unmatched}")
    print("")
    print("=== カラム一致成功率 ===")
    for col in TARGET_COLUMNS:
        denom = total[col]
        rate = (success[col] / denom * 100.0) if denom else 0.0
        print(f"{col:20s} {success[col]:4d}/{denom:<4d}  {rate:6.2f}%")

    print("")
    print("=== 差分詳細（1文字でも不一致） ===")
    if not diffs:
        print("差分なし")
    else:
        for line in diffs[: args.max_diff_lines]:
            print(line)
        if len(diffs) > args.max_diff_lines:
            print(f"... (省略) total_diff_lines={len(diffs)}")


if __name__ == "__main__":
    main()
