#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3 配下の CSV を走査し、「値が別列にずれて入っている」行のみ抽出する監査スクリプト。

対象:
- fixed_csv_3/**/*.csv
- ただし fixed_csv_3/later/, fixed_csv_3/later_2/ は除外

出力:
- reports/csv_integrity_audit.csv
  列: ファイルパス, 行番号, 企業名, 異常種別, 検知された値
"""

from __future__ import annotations

import csv
import re
import sys
from pathlib import Path
from typing import Iterable

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TARGET_ROOT = PROJECT_ROOT / "fixed_csv_3"
REPORT_PATH = PROJECT_ROOT / "reports" / "csv_integrity_audit.csv"
EXCLUDE_DIRS = {"later", "later_2"}

PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)

PHONE_RE = re.compile(r"^(?:0\d{1,4}-\d{1,4}-\d{3,4}|0\d{9,10})$")
POSTAL_RE = re.compile(r"^\d{3}-\d{4}$")
NUMERIC_RE = re.compile(r"^[+-]?\d+(?:\.\d+)?(?:[eE][+-]?\d+)?$")


def normalize_header(raw: str | None) -> str:
    if not raw:
        return ""
    s = str(raw).strip().strip("\ufeff").replace("\n", " ").replace("\r", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)


def normalize_val(raw: str | None) -> str:
    if raw is None:
        return ""
    return str(raw).strip()


def detect_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            with path.open("r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "utf-8"


def collect_target_csv_files() -> list[Path]:
    if not TARGET_ROOT.exists():
        return []
    out: list[Path] = []
    for fp in TARGET_ROOT.rglob("*.csv"):
        rel = fp.relative_to(TARGET_ROOT)
        if rel.parts and rel.parts[0] in EXCLUDE_DIRS:
            continue
        out.append(fp)
    return sorted(out)


def looks_like_address(s: str) -> bool:
    if not s:
        return False
    if any(p in s for p in PREFECTURES):
        return True
    return ("市" in s) or ("区" in s) or ("町" in s) or ("村" in s)


def looks_like_numeric(s: str) -> bool:
    if not s:
        return True
    c = s.replace(",", "").replace("，", "").replace("円", "").replace("百万円", "").strip()
    if c == "":
        return True
    return bool(NUMERIC_RE.match(c))


def header_indices(headers: list[str], patterns: Iterable[str]) -> list[int]:
    out: list[int] = []
    for idx, h in enumerate(headers):
        n = normalize_header(h)
        for p in patterns:
            if p in n:
                out.append(idx)
                break
    return out


def safe_get(row: list[str], idx: int | None) -> str:
    if idx is None or idx < 0 or idx >= len(row):
        return ""
    return normalize_val(row[idx])


def first_existing_idx(idxs: list[int]) -> int | None:
    return idxs[0] if idxs else None


def all_row_values_dump(headers: list[str], row: list[str], max_len: int = 1200) -> str:
    pairs = []
    for i, h in enumerate(headers):
        k = normalize_val(h) or f"col_{i}"
        v = normalize_val(row[i]) if i < len(row) else ""
        pairs.append(f"{k}={v}")
    dump = " | ".join(pairs)
    if len(dump) > max_len:
        return dump[: max_len - 20] + "...(truncated)"
    return dump


def audit_one_file(path: Path, writer: csv.writer) -> int:
    detected = 0
    enc = detect_encoding(path)
    rel_path = str(path.relative_to(PROJECT_ROOT))

    with path.open("r", encoding=enc, newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, None)
        if not headers:
            return 0

        norm_headers = [normalize_header(h) for h in headers]

        company_idx = first_existing_idx(header_indices(headers, ("会社名", "企業名", "商号又は名称")))
        established_idx = first_existing_idx(header_indices(headers, ("設立", "established")))
        capital_idx = first_existing_idx(header_indices(headers, ("資本金", "capital")))
        revenue_idx = first_existing_idx(header_indices(headers, ("直近売上", "売上高", "revenue")))
        email_idx = first_existing_idx(header_indices(headers, ("email", "e-mail", "メール")))

        address_candidates = header_indices(headers, ("住所", "所在地"))
        postal_candidates = header_indices(headers, ("郵便番号", "postal"))
        rep_address_candidates = header_indices(headers, ("代表者住所", "代表住所"))
        rep_postal_candidates = header_indices(headers, ("代表者郵便番号",))

        company_address_idx = first_existing_idx(address_candidates)
        company_postal_idx = first_existing_idx(postal_candidates)
        rep_address_idx = first_existing_idx(rep_address_candidates) if rep_address_candidates else (
            address_candidates[1] if len(address_candidates) > 1 else None
        )
        rep_postal_idx = first_existing_idx(rep_postal_candidates) if rep_postal_candidates else (
            postal_candidates[1] if len(postal_candidates) > 1 else None
        )

        for line_no, row in enumerate(reader, start=2):
            if len(row) < len(headers):
                row = row + [""] * (len(headers) - len(row))

            company = safe_get(row, company_idx) or "(会社名なし)"

            # 1) 列ズレ（設立に電話/郵便/住所）
            established = safe_get(row, established_idx)
            if established:
                if PHONE_RE.match(established):
                    writer.writerow([rel_path, line_no, company, "列ズレ(establishedに電話番号)", established])
                    detected += 1
                elif POSTAL_RE.match(established):
                    writer.writerow([rel_path, line_no, company, "列ズレ(establishedに郵便番号)", established])
                    detected += 1
                elif looks_like_address(established):
                    writer.writerow([rel_path, line_no, company, "列ズレ(establishedに住所)", established])
                    detected += 1

            # 1) 列ズレ（資本金/売上に非数値）
            capital = safe_get(row, capital_idx)
            if capital and not looks_like_numeric(capital):
                writer.writerow([rel_path, line_no, company, "列ズレ(資本金が非数値)", capital])
                detected += 1
            revenue = safe_get(row, revenue_idx)
            if revenue and not looks_like_numeric(revenue):
                writer.writerow([rel_path, line_no, company, "列ズレ(売上が非数値)", revenue])
                detected += 1

            # 1) 列ズレ（email空 + 隣列に@）
            if email_idx is not None:
                email_val = safe_get(row, email_idx)
                if not email_val:
                    left = safe_get(row, email_idx - 1) if email_idx > 0 else ""
                    right = safe_get(row, email_idx + 1) if email_idx + 1 < len(norm_headers) else ""
                    if "@" in left or "@" in right:
                        near_val = left if "@" in left else right
                        writer.writerow([rel_path, line_no, company, "列ズレ(email隣接列にメール値)", near_val])
                        detected += 1

            # 2) 住所逆転（値の入れ違い）
            company_address = safe_get(row, company_address_idx)
            company_postal = safe_get(row, company_postal_idx)
            rep_address = safe_get(row, rep_address_idx)
            rep_postal = safe_get(row, rep_postal_idx)

            if company_postal and company_address:
                if looks_like_address(company_postal) and POSTAL_RE.match(company_address):
                    writer.writerow(
                        [rel_path, line_no, company, "住所逆転(会社郵便番号/住所)", f"郵便番号={company_postal} | 住所={company_address}"]
                    )
                    detected += 1
            if rep_postal and rep_address:
                if looks_like_address(rep_postal) and POSTAL_RE.match(rep_address):
                    writer.writerow(
                        [rel_path, line_no, company, "住所逆転(代表者郵便番号/住所)", f"代表郵便番号={rep_postal} | 代表住所={rep_address}"]
                    )
                    detected += 1

    return detected


def main() -> int:
    csv_files = collect_target_csv_files()
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    total = 0
    with REPORT_PATH.open("w", encoding="utf-8-sig", newline="") as out:
        writer = csv.writer(out)
        writer.writerow(["ファイルパス", "行番号", "企業名", "異常種別", "検知された値"])
        for fp in csv_files:
            total += audit_one_file(fp, writer)

    print(f"監査完了: files={len(csv_files)}, anomalies={total}, report={REPORT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
