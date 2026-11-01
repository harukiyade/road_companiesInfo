#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UIマッピング仕様に沿って、companiesテーブルの表示品質をQAするスクリプト。

実行例:
  python scripts/qa_ui_mapping_check.py
  python scripts/qa_ui_mapping_check.py --sample-size 500 --seed 42
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    import pathlib
    import sys

    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
    import repair_companies_column_shift_from_fixed_csv3 as rep  # type: ignore


INVALID_PLACEHOLDER = "情報未登録"


@dataclass
class QAResult:
    row_id: Any
    ui: Dict[str, str]
    reasons: List[str]

    @property
    def is_perfect(self) -> bool:
        return len(self.reasons) == 0


def first_non_empty(*values: Any) -> Optional[Any]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return value
    return None


def parse_number(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None

    text = str(value).strip()
    if not text:
        return None
    cleaned = text.replace(",", "").replace("¥", "").replace("円", "").strip()
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def format_grouped_int(value: Decimal) -> str:
    as_int = int(value)
    return f"{as_int:,}"


def validate_corporate_number(value: Any) -> Optional[str]:
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if re.fullmatch(r"\d{13}", digits):
        return digits
    return None


def parse_date_value(value: Any) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value

    text = str(value).strip()
    if not text:
        return None

    normalized = text.replace("年", "/").replace("月", "/").replace("日", "").replace("-", "/").replace(".", "/")
    normalized = re.sub(r"\s+", "", normalized)

    for fmt in ("%Y/%m/%d", "%Y/%m", "%Y%m%d", "%Y%m", "%Y"):
        try:
            parsed = dt.datetime.strptime(normalized, fmt)
            if fmt == "%Y":
                return dt.date(parsed.year, 1, 1)
            if fmt in ("%Y/%m", "%Y%m"):
                return dt.date(parsed.year, parsed.month, 1)
            return parsed.date()
        except ValueError:
            continue

    return None


def normalize_url(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if re.match(r"^https?://", text, re.IGNORECASE):
        return text
    return None


def format_money_for_ui(value: Any) -> Optional[str]:
    num = parse_number(value)
    if num is None:
        return None
    if num >= 1000:
        num = num / Decimal("1000")
    return format_grouped_int(num)


def format_capital_for_ui(value: Any) -> Optional[str]:
    num = parse_number(value)
    if num is None:
        return None
    return format_grouped_int(num)


def format_for_ui(row: Dict[str, Any]) -> QAResult:
    reasons: List[str] = []

    company_name_raw = first_non_empty(row.get("company_name"), row.get("name"))
    company_name = str(company_name_raw).strip() if company_name_raw is not None else INVALID_PLACEHOLDER
    if company_name == INVALID_PLACEHOLDER:
        reasons.append("社名が欠損")

    corp_raw = row.get("corporate_number")
    corp_num = validate_corporate_number(corp_raw)
    corporate_number = corp_num if corp_num else INVALID_PLACEHOLDER
    if corp_num is None:
        reasons.append("corporate_numberが13桁不正")

    capital_raw = first_non_empty(row.get("capital_stock"), row.get("capital"))
    capital = format_capital_for_ui(capital_raw) or INVALID_PLACEHOLDER

    revenue_raw = first_non_empty(row.get("latest_revenue"), row.get("revenue"))
    revenue = format_money_for_ui(revenue_raw)
    if revenue is None:
        revenue_view = INVALID_PLACEHOLDER
        reasons.append("売上が計算不可(NULL/非数値)")
    else:
        revenue_view = f"{revenue}千円"

    profit_raw = first_non_empty(row.get("latest_profit"), row.get("profit"))
    profit = format_money_for_ui(profit_raw)
    profit_view = f"{profit}千円" if profit is not None else INVALID_PLACEHOLDER

    postal_code_raw = row.get("postal_code")
    postal_code = str(postal_code_raw).strip() if postal_code_raw is not None and str(postal_code_raw).strip() else INVALID_PLACEHOLDER

    phone_raw = first_non_empty(row.get("phone_number"), row.get("contact_phone_number"))
    phone = str(phone_raw).strip() if phone_raw is not None else INVALID_PLACEHOLDER

    established_raw = first_non_empty(row.get("established"), row.get("established_date"), row.get("founding_year"))
    established_date = parse_date_value(established_raw)
    if established_date is None:
        established = INVALID_PLACEHOLDER
        reasons.append("設立日が不正/欠損")
    else:
        established = established_date.strftime("%Y/%m/%d")

    url_raw = row.get("company_url")
    url = normalize_url(url_raw)
    if url is None:
        company_url = INVALID_PLACEHOLDER
        reasons.append("company_urlが不正(http/httpsなし)")
    else:
        company_url = url

    ui = {
        "社名": company_name,
        "法人番号": corporate_number,
        "資本金": capital,
        "売上（千円）": revenue_view,
        "利益（千円）": profit_view,
        "郵便番号": postal_code,
        "電話番号": phone,
        "設立": established,
        "企業URL": company_url,
    }
    return QAResult(row_id=row.get("id"), ui=ui, reasons=reasons)


def fetch_random_rows(conn: psycopg2.extensions.connection, sample_size: int, seed: Optional[int]) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if seed is not None:
            # PostgreSQLのsetseedは -1.0 ~ 1.0
            normalized_seed = ((seed % 2000000) / 1000000.0) - 1.0
            cur.execute("SELECT setseed(%s);", (normalized_seed,))
        cur.execute(
            """
            SELECT
                id,
                company_name,
                name,
                corporate_number,
                capital_stock,
                capital,
                latest_revenue,
                revenue,
                latest_profit,
                profit,
                postal_code,
                phone_number,
                contact_phone_number,
                established,
                established_date,
                founding_year,
                company_url
            FROM companies
            ORDER BY random()
            LIMIT %s
            """,
            (sample_size,),
        )
        return list(cur.fetchall())


def print_samples(perfect: List[QAResult], imperfect: List[QAResult], sample_n: int) -> None:
    print("=== Perfect サンプル（UI表示シミュレーション） ===")
    if not perfect:
        print("Perfect判定のレコードは見つかりませんでした。")
    for item in perfect[:sample_n]:
        print(
            f"[ID: {item.row_id}] "
            f"社名: {item.ui['社名']} | "
            f"法人番号: {item.ui['法人番号']} | "
            f"売上: {item.ui['売上（千円）']} | "
            f"設立: {item.ui['設立']} | "
            f"URL: {item.ui['企業URL']}"
        )

    print("\n=== Imperfect サンプル（要確認レコード） ===")
    if not imperfect:
        print("Imperfect判定のレコードは見つかりませんでした。")
    for item in imperfect[:sample_n]:
        print(
            f"[ID: {item.row_id}] "
            f"社名: {item.ui['社名']} | "
            f"異常理由: {', '.join(item.reasons)}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="companiesのUIマッピング品質をサンプル検証する")
    parser.add_argument("--sample-size", type=int, default=500, help="ランダム抽出件数（default: 500）")
    parser.add_argument("--seed", type=int, default=None, help="ランダム抽出再現用シード（任意）")
    parser.add_argument("--show", type=int, default=3, help="各カテゴリの表示件数（default: 3）")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.sample_size <= 0:
        raise SystemExit("--sample-size は1以上を指定してください。")
    if args.show <= 0:
        raise SystemExit("--show は1以上を指定してください。")

    conn = rep.get_connection()
    try:
        rows = fetch_random_rows(conn, sample_size=args.sample_size, seed=args.seed)
    finally:
        conn.close()

    results = [format_for_ui(row) for row in rows]
    perfect = [r for r in results if r.is_perfect]
    imperfect = [r for r in results if not r.is_perfect]

    print(f"対象件数: {len(rows)}")
    print(f"Perfect: {len(perfect)}")
    print(f"Imperfect: {len(imperfect)}")
    print_samples(perfect, imperfect, args.show)


if __name__ == "__main__":
    main()
