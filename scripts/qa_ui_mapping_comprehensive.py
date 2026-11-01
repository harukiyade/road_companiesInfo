#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
companiesテーブルのUIマッピング要件を全カラム網羅でQAするスクリプト。

機能:
1) SQLで全件に対するPerfect適合率を高速集計
2) ランダム500件でUI表示シミュレーションし、Perfect/Imperfectサンプルを出力
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

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

RE_CORPORATE = re.compile(r"^\d{13}$")
RE_POSTAL = re.compile(r"^\d{3}-?\d{4}$")
RE_PHONE = re.compile(r"^0\d{1,4}-?\d{1,4}-?\d{3,4}$")
RE_URL = re.compile(r"^https?://", re.IGNORECASE)
RE_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


@dataclass
class QAResult:
    row_id: Any
    ui: Dict[str, str]
    reasons: List[str]

    @property
    def is_perfect(self) -> bool:
        return not self.reasons


def first_non_empty(*values: Any) -> Optional[Any]:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return value
    return None


def parse_date(value: Any) -> Optional[dt.date]:
    if value is None:
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value

    text = str(value).strip()
    if not text:
        return None

    normalized = (
        text.replace("年", "/")
        .replace("月", "/")
        .replace("日", "")
        .replace("-", "/")
        .replace(".", "/")
    )
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


def parse_numeric_strict(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return None

    text = str(value).strip()
    if not text:
        return None
    # 純粋数値のみ許可（カンマ、円、単位などは禁止）
    if not re.fullmatch(r"[+-]?\d+(\.\d+)?", text):
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def format_thousand_unit(value: Any) -> Optional[str]:
    num = parse_numeric_strict(value)
    if num is None:
        return None
    if num >= 1000:
        num = num / Decimal("1000")
    return f"{int(num):,}千円"


def validate_optional_format(
    reasons: List[str],
    label: str,
    value: Any,
    validator,
    required: bool = False,
) -> None:
    if value is None or str(value).strip() == "":
        if required:
            reasons.append(f"{label}が必須だが欠損")
        return
    if not validator(str(value).strip()):
        reasons.append(f"{label}の形式不正({str(value).strip()})")


def aggregate_all_records(conn: psycopg2.extensions.connection) -> Dict[str, Any]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH base AS (
                SELECT
                    id,
                    NULLIF(BTRIM(company_name::text), '') AS company_name_t,
                    NULLIF(BTRIM(name::text), '') AS name_t,
                    NULLIF(BTRIM(address::text), '') AS address_t,
                    NULLIF(BTRIM(representative_name::text), '') AS representative_name_t,
                    NULLIF(BTRIM(executive_name1::text), '') AS executive_name1_t,
                    NULLIF(BTRIM(corporate_number::text), '') AS corporate_number_t,
                    NULLIF(BTRIM(postal_code::text), '') AS postal_code_t,
                    NULLIF(BTRIM(phone_number::text), '') AS phone_number_t,
                    COALESCE(NULLIF(BTRIM(company_url::text), ''), NULLIF(BTRIM(contact_form_url::text), '')) AS ui_url_t,
                    NULLIF(BTRIM(email::text), '') AS email_t,
                    NULLIF(BTRIM(established::text), '') AS established_t,
                    NULLIF(BTRIM(representative_birth_date::text), '') AS representative_birth_date_t,
                    COALESCE(NULLIF(BTRIM(latest_revenue::text), ''), NULLIF(BTRIM(revenue::text), '')) AS revenue_t,
                    COALESCE(NULLIF(BTRIM(latest_profit::text), ''), NULLIF(BTRIM(profit::text), '')) AS profit_t,
                    COALESCE(NULLIF(BTRIM(capital_stock::text), ''), NULLIF(BTRIM(capital::text), '')) AS capital_t,
                    NULLIF(BTRIM(total_assets::text), '') AS total_assets_t,
                    NULLIF(BTRIM(net_assets::text), '') AS net_assets_t,
                    NULLIF(BTRIM(employee_count::text), '') AS employee_count_t,
                    NULLIF(BTRIM(factory_count::text), '') AS factory_count_t,
                    NULLIF(BTRIM(store_count::text), '') AS store_count_t
                FROM companies
            )
            SELECT
                COUNT(*) AS total_count,
                COUNT(*) FILTER (
                    WHERE
                        COALESCE(company_name_t, name_t) IS NOT NULL
                        AND address_t IS NOT NULL
                        AND COALESCE(representative_name_t, executive_name1_t) IS NOT NULL
                        AND (corporate_number_t IS NULL OR corporate_number_t ~ '^[0-9]{13}$')
                        AND (postal_code_t IS NULL OR postal_code_t ~ '^[0-9]{3}-?[0-9]{4}$')
                        AND (phone_number_t IS NULL OR phone_number_t ~ '^0[0-9]{1,4}-?[0-9]{1,4}-?[0-9]{3,4}$')
                        AND (ui_url_t IS NULL OR ui_url_t ~* '^https?://')
                        AND (email_t IS NULL OR email_t ~ '^[^@[:space:]]+@[^@[:space:]]+\\.[^@[:space:]]+$')
                        AND (
                            established_t IS NULL
                            OR established_t ~ '^[0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}$'
                            OR established_t ~ '^[0-9]{4}年[0-9]{1,2}月([0-9]{1,2}日)?$'
                            OR established_t ~ '^[0-9]{4}$'
                        )
                        AND (
                            representative_birth_date_t IS NOT NULL
                            AND (
                                representative_birth_date_t ~ '^[0-9]{4}[-/][0-9]{1,2}[-/][0-9]{1,2}$'
                                OR representative_birth_date_t ~ '^[0-9]{4}年[0-9]{1,2}月([0-9]{1,2}日)?$'
                                OR representative_birth_date_t ~ '^[0-9]{4}$'
                            )
                        )
                        AND (revenue_t IS NULL OR revenue_t ~ '^[+-]?[0-9]+(\\.[0-9]+)?$')
                        AND (profit_t IS NULL OR profit_t ~ '^[+-]?[0-9]+(\\.[0-9]+)?$')
                        AND (capital_t IS NULL OR capital_t ~ '^[+-]?[0-9]+(\\.[0-9]+)?$')
                        AND (total_assets_t IS NULL OR total_assets_t ~ '^[+-]?[0-9]+(\\.[0-9]+)?$')
                        AND (net_assets_t IS NULL OR net_assets_t ~ '^[+-]?[0-9]+(\\.[0-9]+)?$')
                        AND (employee_count_t IS NULL OR employee_count_t ~ '^[+-]?[0-9]+(\\.[0-9]+)?$')
                        AND (factory_count_t IS NULL OR factory_count_t ~ '^[+-]?[0-9]+(\\.[0-9]+)?$')
                        AND (store_count_t IS NULL OR store_count_t ~ '^[+-]?[0-9]+(\\.[0-9]+)?$')
                ) AS perfect_count
            FROM base
            """
        )
        return dict(cur.fetchone())


def fetch_random_rows(conn: psycopg2.extensions.connection, sample_size: int, seed: Optional[int]) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        if seed is not None:
            normalized_seed = ((seed % 2000000) / 1000000.0) - 1.0
            cur.execute("SELECT setseed(%s);", (normalized_seed,))
        cur.execute(
            """
            SELECT
                id,
                company_name,
                name,
                address,
                representative_name,
                executive_name1,
                corporate_number,
                postal_code,
                phone_number,
                company_url,
                contact_form_url,
                email,
                established,
                representative_birth_date,
                latest_revenue,
                revenue,
                latest_profit,
                profit,
                capital_stock,
                capital,
                total_assets,
                net_assets,
                employee_count,
                factory_count,
                store_count
            FROM companies
            ORDER BY random()
            LIMIT %s
            """,
            (sample_size,),
        )
        return list(cur.fetchall())


def format_for_ui(row: Dict[str, Any]) -> QAResult:
    reasons: List[str] = []

    name_raw = first_non_empty(row.get("company_name"), row.get("name"))
    name = str(name_raw).strip() if name_raw is not None else INVALID_PLACEHOLDER
    if name == INVALID_PLACEHOLDER:
        reasons.append("company_name/nameが必須だが欠損")

    address_raw = row.get("address")
    address = str(address_raw).strip() if address_raw is not None and str(address_raw).strip() else INVALID_PLACEHOLDER
    if address == INVALID_PLACEHOLDER:
        reasons.append("addressが必須だが欠損")

    rep_raw = first_non_empty(row.get("representative_name"), row.get("executive_name1"))
    representative = str(rep_raw).strip() if rep_raw is not None else INVALID_PLACEHOLDER
    if representative == INVALID_PLACEHOLDER:
        reasons.append("representative_name/executive_name1が必須だが欠損")

    corporate = str(row.get("corporate_number")).strip() if row.get("corporate_number") is not None else ""
    validate_optional_format(reasons, "corporate_number", corporate, lambda x: bool(RE_CORPORATE.fullmatch(x)))
    corporate_view = corporate if corporate else INVALID_PLACEHOLDER

    postal = str(row.get("postal_code")).strip() if row.get("postal_code") is not None else ""
    validate_optional_format(reasons, "postal_code", postal, lambda x: bool(RE_POSTAL.fullmatch(x)))
    postal_view = postal if postal else INVALID_PLACEHOLDER

    phone = str(row.get("phone_number")).strip() if row.get("phone_number") is not None else ""
    validate_optional_format(reasons, "phone_number", phone, lambda x: bool(RE_PHONE.fullmatch(x)))
    phone_view = phone if phone else INVALID_PLACEHOLDER

    url_raw = first_non_empty(row.get("company_url"), row.get("contact_form_url"))
    url = str(url_raw).strip() if url_raw is not None else ""
    validate_optional_format(reasons, "company_url/contact_form_url", url, lambda x: bool(RE_URL.match(x)))
    url_view = url if url else INVALID_PLACEHOLDER

    email = str(row.get("email")).strip() if row.get("email") is not None else ""
    validate_optional_format(reasons, "email", email, lambda x: bool(RE_EMAIL.fullmatch(x)))
    email_view = email if email else INVALID_PLACEHOLDER

    established_raw = row.get("established")
    established_date = parse_date(established_raw)
    if established_raw is not None and str(established_raw).strip() and established_date is None:
        reasons.append(f"establishedの形式不正({str(established_raw).strip()})")
    established_view = established_date.strftime("%Y/%m/%d") if established_date else INVALID_PLACEHOLDER

    birth_raw = row.get("representative_birth_date")
    birth_date = parse_date(birth_raw)
    if birth_raw is None or str(birth_raw).strip() == "":
        reasons.append("representative_birth_dateが必須だが欠損")
    elif birth_date is None:
        reasons.append(f"representative_birth_dateの形式不正({str(birth_raw).strip()})")
    birth_view = birth_date.strftime("%Y/%m/%d") if birth_date else INVALID_PLACEHOLDER

    numeric_targets = [
        ("latest_revenue/revenue", first_non_empty(row.get("latest_revenue"), row.get("revenue"))),
        ("latest_profit/profit", first_non_empty(row.get("latest_profit"), row.get("profit"))),
        ("capital_stock/capital", first_non_empty(row.get("capital_stock"), row.get("capital"))),
        ("total_assets", row.get("total_assets")),
        ("net_assets", row.get("net_assets")),
        ("employee_count", row.get("employee_count")),
        ("factory_count", row.get("factory_count")),
        ("store_count", row.get("store_count")),
    ]
    for label, val in numeric_targets:
        if val is None or str(val).strip() == "":
            continue
        if parse_numeric_strict(val) is None:
            reasons.append(f"{label}が非数値({str(val).strip()})")

    revenue_view = format_thousand_unit(first_non_empty(row.get("latest_revenue"), row.get("revenue"))) or INVALID_PLACEHOLDER
    profit_view = format_thousand_unit(first_non_empty(row.get("latest_profit"), row.get("profit"))) or INVALID_PLACEHOLDER
    capital_num = parse_numeric_strict(first_non_empty(row.get("capital_stock"), row.get("capital")))
    capital_view = f"{int(capital_num):,}" if capital_num is not None else INVALID_PLACEHOLDER

    ui = {
        "社名": name,
        "住所": address,
        "代表者": representative,
        "法人番号": corporate_view,
        "郵便番号": postal_view,
        "電話番号": phone_view,
        "URL": url_view,
        "メール": email_view,
        "設立": established_view,
        "代表者生年月日": birth_view,
        "売上（千円）": revenue_view,
        "利益（千円）": profit_view,
        "資本金": capital_view,
    }
    return QAResult(row_id=row.get("id"), ui=ui, reasons=reasons)


def print_summary(agg: Dict[str, Any]) -> None:
    total = int(agg.get("total_count") or 0)
    perfect = int(agg.get("perfect_count") or 0)
    pct = (perfect / total * 100.0) if total else 0.0
    print("=== 全件網羅テスト（SQL集計） ===")
    print(f"総レコード数: {total:,} 件")
    print(f"UI要件パーフェクト適合数: {perfect:,} 件 ({pct:.1f}%)")


def print_samples(perfect: List[QAResult], imperfect: List[QAResult], show: int) -> None:
    print("\n=== Perfect サンプル ===")
    if not perfect:
        print("Perfectサンプルは見つかりませんでした。")
    for item in perfect[:show]:
        print(
            f"[ID: {item.row_id}] "
            f"社名: {item.ui['社名']} | "
            f"売上: {item.ui['売上（千円）']} | "
            f"設立: {item.ui['設立']} | "
            f"法人番号: {item.ui['法人番号']} | "
            f"URL: {item.ui['URL']}"
        )

    print("\n=== Imperfect サンプル ===")
    if not imperfect:
        print("Imperfectサンプルは見つかりませんでした。")
    for item in imperfect[:show]:
        print(
            f"[ID: {item.row_id}] "
            f"社名: {item.ui['社名']} | "
            f"異常理由: {', '.join(item.reasons)}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="UIマッピング要件の全カラム網羅QA")
    parser.add_argument("--sample-size", type=int, default=500, help="UIシミュレーション用のランダム抽出件数")
    parser.add_argument("--seed", type=int, default=None, help="ランダム抽出再現用シード")
    parser.add_argument("--show", type=int, default=3, help="Perfect/Imperfect 各表示件数")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.sample_size <= 0:
        raise SystemExit("--sample-size は1以上を指定してください。")
    if args.show <= 0:
        raise SystemExit("--show は1以上を指定してください。")

    conn = rep.get_connection()
    try:
        agg = aggregate_all_records(conn)
        rows = fetch_random_rows(conn, sample_size=args.sample_size, seed=args.seed)
    finally:
        conn.close()

    results = [format_for_ui(row) for row in rows]
    perfect = [r for r in results if r.is_perfect]
    imperfect = [r for r in results if not r.is_perfect]

    print_summary(agg)
    print(f"\nランダム抽出: {len(rows)} 件")
    print(f"サンプル内 Perfect: {len(perfect)} 件")
    print(f"サンプル内 Imperfect: {len(imperfect)} 件")
    print_samples(perfect, imperfect, args.show)


if __name__ == "__main__":
    main()
