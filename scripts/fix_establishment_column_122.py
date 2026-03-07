#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3/unit_million/import_firstTime_122.csv の設立列に住所が入っている行を修正。
該当行では、設立の値（住所）以降を一つ左へずらす。
"""

import csv
import re
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
TARGET_FILE = PROJECT_ROOT / "fixed_csv_3/unit_million/import_firstTime_122.csv"

DATE_PATTERN = re.compile(
    r"^(19|20)\d{2}[\年\/\-]\d{1,2}[\月\/\-]?\d{0,2}日?$|"
    r"^\d{4}[\年\/\-]\d{1,2}[\月\/\-]?\d{0,2}"
)
ADDR_KEYWORDS = ["県", "市", "区", "町", "村", "番地", "丁目", "字", "の"]


def is_date(val):
    if not val or not str(val).strip():
        return True
    return bool(DATE_PATTERN.match(str(val).strip())) or bool(re.match(r"^\d{4}年", str(val).strip()))


def is_address(val):
    if not val or not str(val).strip():
        return False
    v = str(val).strip()
    return any(kw in v for kw in ADDR_KEYWORDS) and not is_date(v)


def fix_file():
    if not TARGET_FILE.exists():
        print(f"ファイルが存在しません: {TARGET_FILE}")
        return 1

    with open(TARGET_FILE, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        print("空ファイル")
        return 0

    headers = rows[0]
    if "住所" not in headers or "設立" not in headers:
        print("住所または設立ヘッダーが見つかりません")
        return 1

    idx_address = headers.index("住所")
    idx_est = headers.index("設立")
    n_cols = len(headers)

    fixed_count = 0
    for i in range(1, len(rows)):
        row = rows[i]
        if len(row) <= idx_est:
            continue
        est_val = row[idx_est] if idx_est < len(row) else ""
        addr_val = row[idx_address] if idx_address < len(row) else ""

        if not est_val:
            continue
        if addr_val and addr_val.strip():
            continue
        if not is_address(est_val) or is_date(est_val):
            continue

        saved_addr = est_val
        row.extend([""] * (n_cols - len(row)))
        for j in range(idx_est, n_cols - 1):
            row[j] = row[j + 1]
        row[n_cols - 1] = ""
        row[idx_address] = saved_addr
        fixed_count += 1

    with open(TARGET_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)

    print(f"修正完了: {fixed_count} 行を左へずらしました")
    return 0


if __name__ == "__main__":
    exit(fix_file())
