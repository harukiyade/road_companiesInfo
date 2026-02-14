#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
マッチしなかったCSV行について、companiesテーブルに「法人番号」または「企業名」で
レコードが存在するかを調べ、原因を分類する。

使い方:
  export POSTGRES_PASSWORD='...'
  python scripts/analyze_no_match_transaction.py no_match_still_unmatched.csv
"""

import os
import sys
from urllib.parse import quote_plus
import pandas as pd
import psycopg2

# update_companies_transaction.py と同じ接続設定
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")


def main():
    csv_path = sys.argv[1] if len(sys.argv) > 1 else "no_match_still_unmatched.csv"
    if not os.path.isfile(csv_path):
        print(f"ファイルが見つかりません: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path, encoding="utf-8", dtype=str)
    df = df.where(pd.notna(df), None)

    if "法人番号" not in df.columns or "企業名" not in df.columns:
        print("CSVに「法人番号」「企業名」列が必要です")
        sys.exit(1)

    # 法人番号でユニーク（空は除外）
    corp_nums = [str(x).strip() for x in df["法人番号"].dropna() if str(x).strip()]
    corp_nums = list(dict.fromkeys(corp_nums))

    if not DB_PASSWORD:
        print("POSTGRES_PASSWORD を設定してください")
        sys.exit(1)

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )

    try:
        with conn.cursor() as cur:
            # 1) マッチしなかった行の「法人番号」のうち、companies に存在する数
            if corp_nums:
                cur.execute(
                    "SELECT corporate_number FROM companies WHERE corporate_number = ANY(%s)",
                    (corp_nums,),
                )
                existing_corp = {r[0] for r in cur.fetchall()}
            else:
                existing_corp = set()

            # マッチしなかった「行数」ベースで、法人番号がDBに存在する行数
            csv_corp_to_row = df.dropna(subset=["法人番号"])
            csv_corp_to_row = csv_corp_to_row[csv_corp_to_row["法人番号"].astype(str).str.strip() != ""]
            rows_with_corp = len(csv_corp_to_row)
            rows_corp_exists_in_db = csv_corp_to_row["法人番号"].astype(str).str.strip().isin(existing_corp).sum()
    finally:
        conn.close()

    total_rows = len(df)
    print("=" * 60)
    print("マッチしなかった件の原因分析")
    print(f"入力CSV: {csv_path}（{total_rows} 行）")
    print("=" * 60)
    print()
    print("【法人番号で判定】")
    print(f"  マッチしなかった行のうち、法人番号が空でない行: {rows_with_corp} 行")
    print(f"  そのうち、companies に同じ法人番号が存在する: {rows_corp_exists_in_db} 行")
    print(f"  → これらは「レコードはあるが、企業名・都道府県・代表者名の表記差で一致しなかった」可能性が高い")
    print()
    not_in_db_by_corp = rows_with_corp - rows_corp_exists_in_db
    print(f"  同じ法人番号が companies に存在しない: {not_in_db_by_corp} 行")
    print(f"  → これらは「companies にその法人番号のレコード自体がない」可能性が高い")
    print()
    print("結論:")
    if rows_corp_exists_in_db > 0:
        print(f"  - {rows_corp_exists_in_db} 件は companies にレコードはあるが、名前/都道府県/代表者名の表記差でマッチしなかった。")
    if not_in_db_by_corp > 0:
        print(f"  - {not_in_db_by_corp} 件は companies に同じ法人番号のレコードがない（未登録または別IDの可能性）。")


if __name__ == "__main__":
    main()
