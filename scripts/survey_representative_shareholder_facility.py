#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3 配下（later除外）のCSVから
代表者情報・株主情報・拠点・施設数 に関連するカラムを調査し、
カラム名一覧とデータサンプルを出力する。
"""

import csv
import json
import re
import sys
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent / "fixed_csv_3"
EXCLUDE = "/later/"
SAMPLE_ROWS = 5  # 各CSVからサンプルとして取得する行数
MAX_SAMPLES_PER_COL = 8  # カラムあたりの最大サンプル数

# カテゴリ別：正規化後にマッチさせるキーワード（部分一致）
REP_KEYWORDS = [
    "代表者名", "代表者", "代表取締役", "社長", "代表者役職", "代表電話",
    "代表者郵便", "代表者住所", "代表者誕生", "氏名1", "代表",
]
SHAREHOLDER_KEYWORDS = [
    "株主", "主要株主", "大株主", "持株", "出資者", "株式保有",
]
FACILITY_KEYWORDS = [
    "店舗数", "工場数", "拠点", "支店数", "営業所", "オフィス数",
    "事業所", "店舗", "工場", "施設数",
]


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


def match_category(norm_name, keywords):
    for kw in keywords:
        if kw in norm_name or norm_name == kw:
            return True
    return False


def collect_csv_files():
    if not ROOT.exists():
        return []
    files = []
    for fp in sorted(ROOT.rglob("*.csv")):
        if EXCLUDE in str(fp).replace("\\", "/"):
            continue
        files.append(fp)
    return files


def survey():
    # カテゴリ -> カラム名(生) -> { "files": [path], "samples": [values] }
    by_category = {
        "代表者": defaultdict(lambda: {"files": [], "samples": []}),
        "株主": defaultdict(lambda: {"files": [], "samples": []}),
        "拠点・施設数": defaultdict(lambda: {"files": [], "samples": []}),
    }
    # カテゴリ別キーワード
    keywords = {
        "代表者": REP_KEYWORDS,
        "株主": SHAREHOLDER_KEYWORDS,
        "拠点・施設数": FACILITY_KEYWORDS,
    }
    # 該当カラムが1つもないファイル
    files_without_rep = set()
    files_without_shareholder = set()
    files_without_facility = set()
    all_files_set = set()

    for fp in collect_csv_files():
        rel = str(fp.relative_to(ROOT.parent))
        all_files_set.add(rel)
        enc = get_encoding(fp)
        try:
            with open(fp, "r", encoding=enc, errors="replace") as f:
                reader = csv.reader(f)
                headers = next(reader, [])
                rows = []
                for i, row in enumerate(reader):
                    if i >= SAMPLE_ROWS:
                        break
                    rows.append(row)
        except Exception as e:
            continue

        if not headers:
            continue

        found_rep = False
        found_shareholder = False
        found_facility = False

        for col_idx, raw_h in enumerate(headers):
            norm = normalize_header(raw_h)
            if not norm:
                continue

            for cat, kws in keywords.items():
                if not match_category(norm, kws):
                    continue
                if cat == "代表者":
                    found_rep = True
                elif cat == "株主":
                    found_shareholder = True
                else:
                    found_facility = True

                entry = by_category[cat][raw_h.strip()]
                if rel not in entry["files"]:
                    entry["files"].append(rel)
                samples = entry["samples"]
                for row in rows:
                    if col_idx < len(row):
                        val = str(row[col_idx]).strip()
                        if val and val.lower() not in ("nan", "none", "null"):
                            if val not in samples and len(samples) < MAX_SAMPLES_PER_COL:
                                samples.append(val)

        if not found_rep:
            files_without_rep.add(rel)
        if not found_shareholder:
            files_without_shareholder.add(rel)
        if not found_facility:
            files_without_facility.add(rel)

    # 結果をシリアライズ可能な形に
    result = {
        "by_category": {},
        "files_without": {
            "代表者": sorted(files_without_rep),
            "株主": sorted(files_without_shareholder),
            "拠点・施設数": sorted(files_without_facility),
        },
        "total_files": len(all_files_set),
    }
    for cat, col_map in by_category.items():
        result["by_category"][cat] = {}
        for col_name, data in sorted(col_map.items()):
            result["by_category"][cat][col_name] = {
                "files": data["files"],
                "file_count": len(data["files"]),
                "samples": list(data["samples"]),
            }
    return result


def main():
    out_path = Path(__file__).resolve().parent.parent / "docs" / "survey_rep_shareholder_facility_result.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    data = survey()
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved: {out_path}")
    return data


if __name__ == "__main__":
    main()
