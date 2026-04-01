#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DB に誤って入った「あり得ない設立日」をクリーンアップする（任意）。

想定: Excel シリアル誤解釈などで founding が 2200年以降、または極端に古い日付になっている行。
パースに失敗した場合は NULL にし、CSV からの再インポートを推奨。

使い方:
  python scripts/cleanup_absurd_founding_dates.py --dry-run
  python scripts/cleanup_absurd_founding_dates.py
"""
import argparse
import os
import sys
from pathlib import Path

_script_dir = Path(__file__).resolve().parent
_project_root = _script_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

try:
    from dotenv import load_dotenv
    load_dotenv(_project_root / ".env")
    load_dotenv()
except ImportError:
    pass

import psycopg2

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "prefer")


def main():
    p = argparse.ArgumentParser(description="異常な founding / founding_year / established を NULL にする")
    p.add_argument("--dry-run", action="store_true", help="件数のみ表示し UPDATE しない")
    args = p.parse_args()

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )
    cur = conn.cursor()

    # founding が TEXT の場合、date との直接比較は不可。::text と先頭4桁の年で判定（DATE 列でも可）。
    _founding_where = """
            founding IS NOT NULL
            AND TRIM(founding::text) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
            AND (
              SUBSTRING(TRIM(founding::text) FROM 1 FOR 4)::int >= 2200
              OR SUBSTRING(TRIM(founding::text) FROM 1 FOR 4)::int < 1700
            )
    """.strip()
    checks = [
        (
            "founding が YYYY-MM-DD 形式で年が 2200 以上または 1700 未満（TEXT/DATE 列対応）",
            "SELECT COUNT(*) FROM companies WHERE " + _founding_where,
            "UPDATE companies SET founding = NULL WHERE " + _founding_where,
        ),
        (
            "founding_year が 2100 超、または 1700 未満",
            """
            SELECT COUNT(*) FROM companies
            WHERE founding_year IS NOT NULL
              AND (founding_year > 2100 OR founding_year < 1700)
            """,
            """
            UPDATE companies SET founding_year = NULL
            WHERE founding_year IS NOT NULL
              AND (founding_year > 2100 OR founding_year < 1700)
            """,
        ),
        (
            "established (TEXT) が YYYY- で西暦 2200 以上に見える（キャストなし・正規表現）",
            """
            SELECT COUNT(*) FROM companies
            WHERE established IS NOT NULL
              AND established ~ '^(2[2-9][0-9]{2}|[3-9][0-9]{3})-'
            """,
            """
            UPDATE companies SET established = NULL
            WHERE established IS NOT NULL
              AND established ~ '^(2[2-9][0-9]{2}|[3-9][0-9]{3})-'
            """,
        ),
    ]

    total = 0
    for label, count_sql, update_sql in checks:
        cur.execute(count_sql)
        n = cur.fetchone()[0]
        print(f"[{label}] 対象件数: {n}")
        total += n
        if n and not args.dry_run:
            cur.execute(update_sql)
            print(f"  → UPDATE 実行 (影響行数は DB 実装に依存)")

    if not args.dry_run and total:
        conn.commit()
        print("コミット完了")
    elif args.dry_run:
        print("--dry-run のため UPDATE していません")
    else:
        print("対象なし")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
