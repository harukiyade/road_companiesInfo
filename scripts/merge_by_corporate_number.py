#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/merge_by_corporate_number.py
同一の「法人番号」を持つレコードを特定し、
DBの型定義（Schema）に基づき安全にマスターレコードへ集約する最終スクリプト。
"""

import os
import psycopg2
from psycopg2.extras import Json
import logging

DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

DRY_RUN = False  # 本番実行

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def is_empty(val):
    if val is None: return True
    if isinstance(val, str) and str(val).strip() == "": return True
    if isinstance(val, (dict, list)) and not val: return True
    return False

def main():
    print("=== 同一法人番号の重複マージ＆削除処理 ===")
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        cur = conn.cursor()

        # ★DBの設計図（Schema）からカラムのデータ型を完全に取得する
        cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'companies'")
        col_types = {row[0]: row[1].upper() for row in cur.fetchall()}

        # カラム一覧の取得
        cur.execute("SELECT * FROM companies LIMIT 0")
        colnames = [desc[0] for desc in cur.description]

        print("1. 重複している法人番号を調査中...")
        cur.execute("""
            SELECT corporate_number, COUNT(*) 
            FROM companies 
            WHERE corporate_number IS NOT NULL AND corporate_number != ''
            GROUP BY corporate_number 
            HAVING COUNT(*) > 1;
        """)
        duplicates = cur.fetchall()
        print(f" -> 重複を持つ法人番号: {len(duplicates)} 件\n")

        processed_count = 0
        deleted_count = 0

        for corp_num, count in duplicates:
            cur.execute("SELECT * FROM companies WHERE corporate_number = %s ORDER BY updated_at DESC", (corp_num,))
            rows = cur.fetchall()
            records = [dict(zip(colnames, row)) for row in rows]
            
            master = records[0]
            updates = {}

            for rec in records[1:]:
                for col in colnames:
                    if col in ('id', 'created_at', 'updated_at', 'corporate_number'): continue
                    
                    if is_empty(master.get(col)) and not is_empty(rec.get(col)):
                        updates[col] = rec[col]
                        master[col] = rec[col]
                
                if not DRY_RUN:
                    cur.execute("DELETE FROM companies WHERE id = %s", (rec['id'],))
                deleted_count += 1

            if updates and not DRY_RUN:
                set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
                set_clause += ", updated_at = NOW()"
                
                values = []
                # ★DBの型定義に従って完璧にキャストする
                for k, v in updates.items():
                    ctype = col_types.get(k, '')
                    if 'JSON' in ctype:
                        values.append(Json(v))
                    elif ctype == 'ARRAY':
                        values.append(v)
                    else:
                        if isinstance(v, dict): values.append(Json(v))
                        else: values.append(v)
                
                values.append(master['id'])
                cur.execute(f"UPDATE companies SET {set_clause} WHERE id = %s", values)
            
            logger.info(f"[統合] 法人番号 {corp_num}: {count}件のレコードをID({master['id']})に集約しました。")
            processed_count += 1

        if not DRY_RUN:
            conn.commit()
            print(f"\n=======================================================")
            print(f"完了: {processed_count}種類の法人番号を統合し、合計 {deleted_count}件 の重複を削除しました！")
            print(f"=======================================================")

    except Exception as e:
        if conn and not DRY_RUN: conn.rollback()
        logger.error(f"エラーが発生しました: {e}")
        print(f"エラー: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()