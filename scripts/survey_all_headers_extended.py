#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3 配下（later除外）の全CSVから全ヘッダーを抽出し、
拡張キーワード＋AI判断で代表者・株主・拠点関連カラムを徹底再調査する。
"""

import csv
import json
import re
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent / "fixed_csv_3"
EXCLUDE = "/later/"
SAMPLE_ROWS = 8
MAX_SAMPLES_PER_COL = 10

# 前回レポートで既に報告したカラム（新規判定用）
PREV_REP = {"代表者名", "代表者", "代表者住所", "代表者郵便番号", "代表者誕生日"}
PREV_SHAREHOLDER = {"株主", "株式保有率"}
PREV_FACILITY = {"オフィス数", "工場数", "店舗数"}

# 部分一致キーワード（ユーザー指定）
REP_KEYWORDS = ["代表", "社長", "役員", "取締", "責任", "氏名", "個人"]
SHAREHOLDER_KEYWORDS = ["株", "資本", "出資", "所有", "投資", "割合", "比率"]
FACILITY_KEYWORDS = ["店", "工場", "拠点", "支店", "営業所", "支社", "事業所", "施設", "センター", "ビル", "部屋"]

# AI判断：基本企業概要以外で代表・株主・拠点に関連しそうな語
EXTRA_REP_LIKE = ["監査", "執行", "会長", "副社長", "代表取締役", "役職", "取締役", "役員名", "担当", "窓口", "連絡先", "生年月日", "誕生", "郵便", "住所"]
EXTRA_SHAREHOLDER_LIKE = ["持株会", "株主構成", "発行済", "親会社", "子会社", "連結", "単体", "保有", "議決権"]
EXTRA_FACILITY_LIKE = ["本社", "オフィス", "従業員", "人数", "国内", "海外", "店舗", "工場", "オフィス数", "工場数", "店舗数", "[国内", "[主な", "事業所"]


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


def match_any(norm_name, keywords):
    return any(kw in norm_name for kw in keywords)


def collect_csv_files():
    if not ROOT.exists():
        return []
    return [fp for fp in sorted(ROOT.rglob("*.csv")) if EXCLUDE not in str(fp).replace("\\", "/")]


def survey():
    # 全ヘッダー（重複排除）: 生カラム名 -> 正規化名
    all_headers_unique = {}
    # カラム名(生) -> { "files": [rel_path], "samples": [] }
    column_info = defaultdict(lambda: {"files": [], "samples": []})

    # カテゴリ別マッチ: カラム名 -> マッチ理由（キーワード / extra_rep / extra_shareholder / extra_facility）
    matched_rep = defaultdict(list)   # col -> [reason]
    matched_shareholder = defaultdict(list)
    matched_facility = defaultdict(list)

    for fp in collect_csv_files():
        rel = str(fp.relative_to(ROOT.parent))
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
        except Exception:
            continue

        for col_idx, raw_h in enumerate(headers):
            raw_stripped = raw_h.strip() if raw_h else ""
            if not raw_stripped:
                continue
            norm = normalize_header(raw_h)
            all_headers_unique[raw_stripped] = norm

            entry = column_info[raw_stripped]
            if rel not in entry["files"]:
                entry["files"].append(rel)
            for row in rows:
                if col_idx < len(row):
                    val = str(row[col_idx]).strip()
                    if val and val.lower() not in ("nan", "none", "null"):
                        if val not in entry["samples"] and len(entry["samples"]) < MAX_SAMPLES_PER_COL:
                            entry["samples"].append(val)
                        break

            # 代表・役員系
            if match_any(norm, REP_KEYWORDS):
                matched_rep[raw_stripped].append("keyword")
            elif match_any(norm, EXTRA_REP_LIKE):
                matched_rep[raw_stripped].append("extra")

            # 株主・資本系（「資本金」は財務なので除外する場合あり。ここでは「資本」で拾い、後で区別可能に）
            if match_any(norm, SHAREHOLDER_KEYWORDS):
                matched_shareholder[raw_stripped].append("keyword")
            elif match_any(norm, EXTRA_SHAREHOLDER_LIKE):
                matched_shareholder[raw_stripped].append("extra")

            # 拠点・施設系
            if match_any(norm, FACILITY_KEYWORDS):
                matched_facility[raw_stripped].append("keyword")
            elif match_any(norm, EXTRA_FACILITY_LIKE):
                matched_facility[raw_stripped].append("extra")

    # 前回報告分
    prev_all = PREV_REP | PREV_SHAREHOLDER | PREV_FACILITY

    def is_new(col, cat):
        if cat == "代表者":
            return col not in PREV_REP
        if cat == "株主":
            return col not in PREV_SHAREHOLDER
        if cat == "拠点":
            return col not in PREV_FACILITY
        return True

    result = {
        "all_headers_sorted": sorted(all_headers_unique.keys(), key=lambda x: (normalize_header(x), x)),
        "by_category": {
            "代表・役員系": [],
            "株主・資本系": [],
            "拠点・施設系": [],
        },
        "new_columns": {
            "代表・役員系": [],
            "株主・資本系": [],
            "拠点・施設系": [],
        },
        "column_detail": {},
        "total_files": len(collect_csv_files()),
    }

    for col in sorted(matched_rep.keys(), key=lambda x: (normalize_header(x), x)):
        detail = {
            "files": column_info[col]["files"],
            "file_count": len(column_info[col]["files"]),
            "samples": column_info[col]["samples"],
            "match_reason": matched_rep[col],
            "is_new": is_new(col, "代表者"),
        }
        result["by_category"]["代表・役員系"].append(col)
        result["column_detail"][f"rep::{col}"] = detail
        if detail["is_new"]:
            result["new_columns"]["代表・役員系"].append(col)

    for col in sorted(matched_shareholder.keys(), key=lambda x: (normalize_header(x), x)):
        detail = {
            "files": column_info[col]["files"],
            "file_count": len(column_info[col]["files"]),
            "samples": column_info[col]["samples"],
            "match_reason": matched_shareholder[col],
            "is_new": is_new(col, "株主"),
        }
        result["by_category"]["株主・資本系"].append(col)
        result["column_detail"][f"shareholder::{col}"] = detail
        if detail["is_new"]:
            result["new_columns"]["株主・資本系"].append(col)

    for col in sorted(matched_facility.keys(), key=lambda x: (normalize_header(x), x)):
        detail = {
            "files": column_info[col]["files"],
            "file_count": len(column_info[col]["files"]),
            "samples": column_info[col]["samples"],
            "match_reason": matched_facility[col],
            "is_new": is_new(col, "拠点"),
        }
        result["by_category"]["拠点・施設系"].append(col)
        result["column_detail"][f"facility::{col}"] = detail
        if detail["is_new"]:
            result["new_columns"]["拠点・施設系"].append(col)

    return result


def main():
    data = survey()
    out = Path(__file__).resolve().parent.parent / "docs" / "survey_all_headers_extended_result.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved: {out}")
    print(f"Total unique headers: {len(data['all_headers_sorted'])}")
    for cat in ["代表・役員系", "株主・資本系", "拠点・施設系"]:
        print(f"  {cat}: {len(data['by_category'][cat])} columns, new: {len(data['new_columns'][cat])}")
    return data


if __name__ == "__main__":
    main()
