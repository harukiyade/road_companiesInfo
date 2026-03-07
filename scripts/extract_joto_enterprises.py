#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3 配下（later除外）から「取引種別」=「譲渡企業」のレコードを抽出し、
異なるファイルから最大10件をピックアップして分析用データを出力する。
"""

import csv
import json
import re
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent / "fixed_csv_3"
EXCLUDE = "/later/"
TARGET_VALUE = "譲渡企業"
MAX_RECORDS = 10
MAX_PER_FILE = 2  # 1ファイルあたり最大2件まで取り、ファイル分散を図る


def normalize_header(h):
    if not h:
        return ""
    s = str(h).strip().strip("\ufeff").replace("\n", " ").replace("\r", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)


def get_encoding(fp):
    for enc in ["utf-8-sig", "cp932", "utf-8"]:
        try:
            with open(fp, "r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            pass
    return "utf-8"


def collect_csv_files():
    if not ROOT.exists():
        return []
    return [fp for fp in sorted(ROOT.rglob("*.csv")) if EXCLUDE not in str(fp).replace("\\", "/")]


def run():
    # ファイルパス -> 取引種別の列インデックス
    results = []  # [{ "file": rel_path, "headers": [], "rows": [ { header: value } ] }]
    seen_files = set()
    total_picked = 0

    for fp in collect_csv_files():
        if total_picked >= MAX_RECORDS:
            break
        enc = get_encoding(fp)
        try:
            with open(fp, "r", encoding=enc, errors="replace") as f:
                reader = csv.reader(f)
                headers = next(reader, [])
        except Exception:
            continue

        norm_to_idx = {normalize_header(h): i for i, h in enumerate(headers)}
        # 取引種別列を探す
        tx_idx = None
        for n, idx in norm_to_idx.items():
            if "取引種別" in n or "取引区分" in n:
                tx_idx = idx
                break
        if tx_idx is None:
            continue

        rel_path = str(fp.relative_to(ROOT.parent))
        picked_this_file = 0
        file_rows = []

        with open(fp, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if total_picked >= MAX_RECORDS:
                    break
                if picked_this_file >= MAX_PER_FILE:
                    break
                if tx_idx >= len(row):
                    continue
                val = str(row[tx_idx]).strip()
                if val != TARGET_VALUE:
                    continue
                # この行をヘッダーと対応付けて辞書に
                rec = {}
                for i, h in enumerate(headers):
                    if i < len(row):
                        rec[h.strip()] = row[i].strip() if i < len(row) else ""
                file_rows.append(rec)
                picked_this_file += 1
                total_picked += 1

        if file_rows:
            results.append({
                "file": rel_path,
                "headers": headers,
                "rows": file_rows,
            })

    return results


if __name__ == "__main__":
    data = run()
    out = Path(__file__).resolve().parent.parent / "docs" / "joto_enterprises_sample.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved: {out}")
    print(f"Files with 譲渡企業: {len(data)}")
    for block in data:
        print(f"  {block['file']}: {len(block['rows'])} 件")
        for r in block["rows"]:
            name = r.get("会社名") or r.get("企業名") or "(不明)"
            print(f"    - {name}")
