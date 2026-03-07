#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/compare_duplicate_records.py
特定の2つのIDのレコードを取得し、差分を比較しやすく表示するスクリプト。
"""

import os
import pandas as pd
import psycopg2

# --- 設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

TARGET_IDS = [
    '28b115e8-b3ce-496b-b3a9-cdba8452d5ce',
    'e1e841b8-3b4d-4ca4-b055-bbb49187110e'
]

def main():
    print("=== 対象レコードの取得と状態比較 ===")
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        
        # IDを指定してレコードを取得
        query = "SELECT * FROM companies WHERE id IN %s;"
        
        # Pandasで読み込むと表形式での操作が簡単になります
        df = pd.read_sql_query(query, conn, params=(tuple(TARGET_IDS),))
        
        if df.empty:
            print("指定されたIDのレコードが見つかりませんでした。")
            return
            
        print(f"取得件数: {len(df)}件\n")
        
        # 比較しやすくするため、行と列を入れ替える（転置）
        df_transposed = df.set_index('id').T
        
        # 両方のレコードで値が空（NaNやNone）のカラムは表示から除外して見やすくする
        df_filtered = df_transposed.dropna(how='all')
        
        # ターミナルでの表示幅を広げる設定
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_colwidth', 50)
        
        print(df_filtered)
        
        print("\n【チェックポイント】")
        print("1. corporate_number (法人番号) はどちらかに入っているか？")
        print("2. データの情報量 (overview など) はどちらが多いか？")
        print("3. created_at (作成日時) はどちらが古いか？ (元のデータはどちらか)")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    main()