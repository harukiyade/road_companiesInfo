#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/merge_by_url_execute.py
URL（ホームページ）をキーにしてUUIDレコードとマスターレコードを紐付けます。
※メモリ＆CPU超最適化版
"""

import os
import re
import psycopg2
import logging
from datetime import datetime

# --- 設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

DRY_RUN = False  # テストモード

# --- ログ設定 ---
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"merge_by_url_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(log_filename, encoding='utf-8')])
logger = logging.getLogger(__name__)

def is_empty(val):
    if val is None: return True
    if isinstance(val, str) and str(val).strip() == "": return True
    return False

def normalize_url(url):
    if not url: return ""
    u = str(url).lower().strip()
    u = re.sub(r'^https?://', '', u)
    u = re.sub(r'^www\.', '', u)
    return u.rstrip('/')

def main():
    print(f"=== URLベースのマージ＆クレンジング {'(テストモード)' if DRY_RUN else '(本番実行)'} ===")
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        cur = conn.cursor()
        
        print("1. マスターレコードのURL一覧を抽出中... (数十秒かかります)")
        cur.execute("""
            SELECT id, corporate_number, url, homepage_url, company_url 
            FROM companies 
            WHERE corporate_number IS NOT NULL AND corporate_number != ''
              AND id ~ '^[0-9]+$'
              AND (url IS NOT NULL OR homepage_url IS NOT NULL OR company_url IS NOT NULL);
        """)
        master_rows = cur.fetchall()
        print(f" -> {len(master_rows)} 件のマスターURL情報をメモリにロードしました。")
        print(" -> URLの正規化とインデックス作成中... (数秒〜十数秒かかります)")
        
        # 辞書作成の超高速化
        master_url_dict = {}
        master_url_seen_ids = {} # 重複チェックをO(1)にするためのSet
        
        for row in master_rows:
            mid, corp_num, u1, u2, u3 = row
            for url_val in (u1, u2, u3):
                norm_u = normalize_url(url_val)
                if norm_u:
                    if norm_u not in master_url_dict:
                        master_url_dict[norm_u] = []
                        master_url_seen_ids[norm_u] = set()
                    
                    if mid not in master_url_seen_ids[norm_u]:
                        master_url_seen_ids[norm_u].add(mid)
                        master_url_dict[norm_u].append({'id': mid, 'corporate_number': corp_num})

        print("2. 対象のUUIDレコードを抽出中...")
        cur.execute("""
            SELECT * FROM companies 
            WHERE (corporate_number IS NULL OR corporate_number = '')
              AND (id LIKE '%-%' OR id !~ '^[0-9]+$')
              AND (url IS NOT NULL OR homepage_url IS NOT NULL OR company_url IS NOT NULL);
        """)
        colnames = [desc[0] for desc in cur.description]
        uuids = [dict(zip(colnames, row)) for row in cur.fetchall()]
        print(f" -> URLを持つUUIDレコードは {len(uuids)} 件です。")
        print("3. マッチング処理を開始します...")

        processed_count = 0
        multiple_match_count = 0

        for index, u_rec in enumerate(uuids, 1):
            if index % 1000 == 0:
                print(f"  ...進捗: {index} / {len(uuids)} 件")
                
            target_url = u_rec.get('url') or u_rec.get('homepage_url') or u_rec.get('company_url')
            norm_u = normalize_url(target_url)
            if not norm_u: continue
                
            matched_masters = master_url_dict.get(norm_u)
            
            if matched_masters:
                if len(matched_masters) == 1:
                    master_basic = matched_masters[0]
                    
                    cur.execute("SELECT * FROM companies WHERE id = %s;", (master_basic['id'],))
                    master_row = cur.fetchone()
                    if not master_row: continue
                    master = dict(zip(colnames, master_row))
                    
                    updates = {}
                    for col in colnames:
                        if col in ('id', 'created_at', 'updated_at', 'corporate_number'): continue
                        if is_empty(master[col]) and not is_empty(u_rec[col]):
                            updates[col] = u_rec[col]
                    
                    logger.info("-" * 50)
                    logger.info(f"【一致】URL: {target_url}")
                    logger.info(f"  [マスター] {master['name']} (ID: {master['id']}, 法人番号: {master['corporate_number']})")
                    logger.info(f"  [消去UUID] {u_rec['name']} (ID: {u_rec['id']})")
                    
                    if not DRY_RUN:
                        if updates:
                            set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
                            set_clause += ", updated_at = NOW()"
                            values = list(updates.values()) + [master['id']]
                            cur.execute(f"UPDATE companies SET {set_clause} WHERE id = %s", values)
                        
                        cur.execute("DELETE FROM companies WHERE id = %s", (u_rec['id'],))
                    
                    processed_count += 1
                else:
                    multiple_match_count += 1

        if not DRY_RUN:
            conn.commit()
            print(f"\n完了: {processed_count} 件のデータをURLをキーにしてマージ＆クレンジングしました。")
        else:
            print(f"\nテスト完了: URLで確実にマージできるレコードは {processed_count} 件でした。")
            print(f"（※複数のマスターに該当して保留した件数: {multiple_match_count} 件）")
            print("問題なければ、スクリプト内の DRY_RUN = False に変更して再実行してください。")

    except Exception as e:
        if conn and not DRY_RUN: conn.rollback()
        print(f"エラーが発生しました: {e}")
        logger.error(f"エラー: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()