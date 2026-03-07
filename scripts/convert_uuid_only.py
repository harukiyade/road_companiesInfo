#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/convert_uuid_only.py
法人番号には手を付けず、IDがUUID形式のレコードのみを数値IDに変換するスクリプト。
"""

import os
import random
import psycopg2

# --- 設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

def generate_numeric_id():
    """19桁の新しい数値IDを生成"""
    return str(random.randint(1000000000000000000, 9999999999999999999))

def main():
    print("=== UUIDレコードの数値ID化処理（法人番号は変更しません） ===")
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        cur = conn.cursor()

        # 1. IDがUUID形式のレコードを特定
        cur.execute("""
            SELECT id FROM companies 
            WHERE (id LIKE '%-%' OR id !~ '^[0-9]+$');
        """)
        uuid_rows = cur.fetchall()
        
        if not uuid_rows:
            print("UUID形式のレコードは見つかりませんでした。すべて数値IDになっています。")
            return

        print(f"対象レコード: {len(uuid_rows)} 件 を変換します...")

        # 2. 1件ずつ数値IDに更新
        for row in uuid_rows:
            old_id = row[0]
            new_id = generate_numeric_id()
            cur.execute("UPDATE companies SET id = %s, updated_at = NOW() WHERE id = %s", (new_id, old_id))

        conn.commit()
        print(f"\n完了: {len(uuid_rows)} 件のIDを数値化しました。")

    except Exception as e:
        if conn: conn.rollback()
        print(f"エラーが発生しました: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()