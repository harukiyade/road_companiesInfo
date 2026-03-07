#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3 配下（later除外）の全CSVを走査し、
5カテゴリの項目充実度でランキングし、上位5社を特定する。
"""

import csv
import re
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent.parent / "fixed_csv_3"
EXCLUDE = "/later/"

# カテゴリ別：正規化ヘッダーに含まれるキーワード -> 正規キー
# 1. 代表者・役員
REP_MAP = [
    (["代表者名", "代表者"], "rep_name"),
    (["代表者住所"], "rep_address"),
    (["代表者誕生日"], "rep_birth"),
    (["取締役", "役員"], "executives"),
]
# 2. 株主・資本
SHAREHOLDER_MAP = [
    (["株主", "株式保有率"], "shareholders"),
    (["資本金"], "capital"),
]
# 3. 拠点・施設
FACILITY_MAP = [
    (["オフィス数"], "office_count"),
    (["工場数"], "factory_count"),
    (["店舗数"], "store_count"),
]
# 4. 業績・規模
PERF_MAP = [
    (["直近売上", "売上規模"], "revenue"),
    (["直近利益"], "profit"),
    (["直近決算年月"], "fiscal_end"),
    (["社員数"], "employee_count"),
]
# 5. 事業内容・ネットワーク
BUSINESS_MAP = [
    (["業種1", "業種"], "industry"),  # 業種2,3もあれば概要に使えるがスコアは1項目として
    (["企業概要", "概要", "説明", "概況"], "overview"),
    (["取引先"], "clients"),
    (["取引先銀行"], "banks"),
    (["URL"], "url"),
]

ALL_KEYS = (
    [x[1] for x in REP_MAP]
    + [x[1] for x in SHAREHOLDER_MAP]
    + [x[1] for x in FACILITY_MAP]
    + [x[1] for x in PERF_MAP]
    + [x[1] for x in BUSINESS_MAP]
)
TOTAL_FIELDS = len(ALL_KEYS)

PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)


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


def extract_prefecture(s):
    if not s:
        return ""
    for p in PREFECTURES:
        if p in str(s):
            return p
    return ""


def has_value(v):
    if v is None:
        return False
    s = str(v).strip()
    return bool(s) and s.lower() not in ("nan", "none", "null", "")


def build_header_to_canonical(headers):
    """ヘッダーリストから 正規キー -> col_idx のマッピングを構築。追加で _address, _phone の列番号も返す"""
    norm_to_idx = {}
    for i, h in enumerate(headers):
        n = normalize_header(h)
        if n:
            norm_to_idx[n] = i

    result = {}
    for keywords, key in REP_MAP + SHAREHOLDER_MAP + FACILITY_MAP + PERF_MAP + BUSINESS_MAP:
        for n, idx in norm_to_idx.items():
            if any(kw in n for kw in keywords):
                if key not in result:
                    result[key] = idx
                break
    # 特定キー用（住所・電話）
    for n, idx in norm_to_idx.items():
        if "住所" in n and "代表者" not in n and "address" not in result:
            result["_address"] = idx
            break
    for n, idx in norm_to_idx.items():
        if "電話" in n or "窓口" in n:
            result["_phone"] = idx
            break
    return result


def get_company_key(row, name_idx, pref_idx, addr_idx):
    name = ""
    if name_idx is not None and name_idx < len(row):
        name = str(row[name_idx]).strip()
    if not name:
        return None, None
    pref = ""
    if pref_idx is not None and pref_idx < len(row):
        pref = str(row[pref_idx]).strip()
    if not pref and addr_idx is not None and addr_idx < len(row):
        pref = extract_prefecture(row[addr_idx])
    key = (name, pref) if pref else (name, "")
    return key, name


def collect_csv_files():
    if not ROOT.exists():
        return []
    return [fp for fp in sorted(ROOT.rglob("*.csv")) if EXCLUDE not in str(fp).replace("\\", "/")]


def run_fixed():
    companies = defaultdict(lambda: {"score": 0, "data": {}, "source_file": ""})
    name_aliases = ["会社名", "企業名"]
    pref_aliases = ["都道府県", "prefecture"]
    addr_aliases = ["住所", "所在地", "address"]

    for fp in collect_csv_files():
        enc = get_encoding(fp)
        try:
            with open(fp, "r", encoding=enc, errors="replace") as f:
                reader = csv.reader(f)
                headers = next(reader, [])
        except Exception:
            continue
        if len(headers) < 3:
            continue

        norm_to_idx = {normalize_header(h): i for i, h in enumerate(headers)}
        name_idx = None
        for a in name_aliases:
            n = normalize_header(a)
            if n in norm_to_idx:
                name_idx = norm_to_idx[n]
                break
        pref_idx = None
        for a in pref_aliases:
            n = normalize_header(a)
            if n in norm_to_idx:
                pref_idx = norm_to_idx[n]
                break
        addr_idx = None
        for a in addr_aliases:
            n = normalize_header(a)
            if n in norm_to_idx:
                addr_idx = norm_to_idx[n]
                break
        if name_idx is None:
            continue

        h2c = build_header_to_canonical(headers)
        if len(h2c) < 5:
            continue

        rel_path = str(fp.relative_to(ROOT.parent))
        with open(fp, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) < 2:
                    continue
                key, name = get_company_key(row, name_idx, pref_idx, addr_idx)
                if not key or not name:
                    continue
                data = {}
                score = 0
                for ckey, col_idx in h2c.items():
                    if ckey.startswith("_"):
                        if col_idx < len(row) and has_value(row[col_idx]):
                            data[ckey] = str(row[col_idx]).strip()
                        continue
                    if col_idx < len(row):
                        val = str(row[col_idx]).strip()
                        if has_value(val):
                            data[ckey] = val
                            score += 1
                if score > companies[key]["score"]:
                    companies[key]["score"] = score
                    companies[key]["data"] = data.copy()
                    companies[key]["source_file"] = rel_path

    return companies


if __name__ == "__main__":
    companies = run_fixed()
    sorted_companies = sorted(
        companies.items(),
        key=lambda x: (-x[1]["score"], -len([k for k in x[1]["data"] if not k.startswith("_")])),
    )
    top5 = sorted_companies[:5]
    for i, (key, info) in enumerate(top5, 1):
        name, pref = key
        d = info["data"]
        key_addr = d.get("_address", "")
        key_phone = d.get("_phone", "")
        print(f"--- 第{i}位 (スコア {info['score']}/{TOTAL_FIELDS}) ---")
        print(f"企業名: {name}")
        print(f"特定キー（住所）: {key_addr[:60]}..." if len(key_addr) > 60 else f"特定キー（住所）: {key_addr or '(なし)'}")
        print(f"特定キー（電話）: {key_phone}")
        print(f"都道府県: {pref or '(なし)'}")
        print(f"出典: {info['source_file']}")
        print(f"代表者: {d.get('rep_name','')}")
        print(f"代表者誕生日: {d.get('rep_birth','')}")
        print(f"売上: {d.get('revenue','')}, 利益: {d.get('profit','')}, 決算: {d.get('fiscal_end','')}, 社員数: {d.get('employee_count','')}")
        print(f"株主: {(d.get('shareholders') or '')[:80]}...")
        print(f"店舗数: {d.get('store_count','')}, オフィス: {d.get('office_count','')}, 工場: {d.get('factory_count','')}")
        print(f"URL: {d.get('url','')[:50]}...")
        print()
