#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/check_company_db.py
指定した企業名でDBを検索し、現在のテーブル構造と格納されているデータを縦に表示する。
"""

import os
import psycopg2
import sys

# --- 設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

def main():
    if len(sys.argv) < 2:
        print("使い方: python scripts/check_company_db.py '検索したい企業名'")
        return

    search_name = sys.argv[1]
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        cur = conn.cursor()

        # 1. カラム名とデータ型の取得
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'companies'
            ORDER BY ordinal_position;
        """)
        col_info = cur.fetchall()
        colnames = [row[0] for row in col_info]

        # 2. 企業名で部分一致検索（最大1件のみ詳細表示）
        cur.execute("SELECT * FROM companies WHERE name LIKE %s LIMIT 1", (f'%{search_name}%',))
        row = cur.fetchone()

        if not row:
            print(f"「{search_name}」を含む企業はDBに見つかりませんでした。")
            return

        print(f"=== DB格納状況確認: 「{search_name}」 ===\n")
        
        # 3. データを辞書化して縦に表示
        record = dict(zip(colnames, row))
        print(f"{'カラム名':<25} | {'データ型':<12} | {'現在の値'}")
        print("-" * 70)
        
        for col, ctype in col_info:
            val = record[col]
            val_display = "(空)" if val is None or str(val).strip() == "" else str(val)
            # 値が長い場合は改行するか省略（ここでは簡易的に1行表示）
            if len(val_display) > 50:
                val_display = val_display[:47] + "..."
                
            print(f"{col:<25} | {ctype:<12} | {val_display}")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()