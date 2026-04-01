#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, psycopg2, json

def main():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "34.84.189.233"), port="5432",
        user=os.getenv("POSTGRES_USER", "postgres"), password=os.getenv("POSTGRES_PASSWORD", "Legatus2000/"),
        dbname="postgres", sslmode="disable"
    )
    cur = conn.cursor()

    target_id = '1766038915360006975'

    # 利益を「数値」として明示的に指定したJSON
    fin_data = json.dumps([{
        "term": "直近決算",
        "sales": 404000000,
        "profit": 21000000,  # 文字列ではなく数値
        "totalAssets": None,
        "netAssets": None,
        "equityRatio": None
    }])

    # 念のため latest_profit も数値で更新
    cur.execute("""
        UPDATE companies 
        SET latest_profit = 21000000,
            financials = %s::jsonb
        WHERE id = %s OR name = '山都酒造株式会社'
    """, (fin_data, target_id))

    conn.commit()
    conn.close()
    print("利益データを数値型で強制再上書きしました。UIを確認してください。")

if __name__ == "__main__":
    main()