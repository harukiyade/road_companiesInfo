#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/merge_by_phone_execute.py
電話番号をキーにしてUUIDレコードとマスターレコードを紐付け、
不足データをマスターにマージした上で、UUIDレコードを削除するスクリプト。
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

# ★ 最初は True（テスト）になっています。問題なければ False にして実行してください。
DRY_RUN = False

# --- ログ設定 ---
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"merge_by_phone_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(log_filename, encoding='utf-8')])
logger = logging.getLogger(__name__)

def is_empty(val):
    if val is None: return True
    if isinstance(val, str) and str(val).strip() == "": return True
    return False

def normalize_phone(phone):
    if not phone: return ""
    return re.sub(r'\D', '', str(phone))

def main():
    print(f"=== 電話番号ベースのマージ＆クレンジング {'(テストモード)' if DRY_RUN else '(本番実行)'} ===")
    print(f"詳細ログ: {log_filename}")
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        cur = conn.cursor()
        
        # 1. マスターの取得
        cur.execute("""
            SELECT * FROM companies 
            WHERE corporate_number IS NOT NULL AND corporate_number != ''
              AND id ~ '^[0-9]+$'
              AND phone_number IS NOT NULL AND phone_number != '';
        """)
        colnames = [desc[0] for desc in cur.description]
        masters = [dict(zip(colnames, row)) for row in cur.fetchall()]
        
        # マスターを電話番号で辞書化
        master_phone_dict = {}
        for m in masters:
            norm_phone = normalize_phone(m['phone_number'])
            if norm_phone:
                if norm_phone not in master_phone_dict:
                    master_phone_dict[norm_phone] = []
                master_phone_dict[norm_phone].append(m)

        # 2. UUIDレコードの取得
        cur.execute("""
            SELECT * FROM companies 
            WHERE (corporate_number IS NULL OR corporate_number = '')
              AND (id LIKE '%-%' OR id !~ '^[0-9]+$')
              AND phone_number IS NOT NULL AND phone_number != '';
        """)
        uuids = [dict(zip(colnames, row)) for row in cur.fetchall()]
        
        processed_count = 0

        # 3. マッチング＆マージ処理
        for u_rec in uuids:
            norm_phone = normalize_phone(u_rec['phone_number'])
            if not norm_phone: continue
                
            matched_masters = master_phone_dict.get(norm_phone)
            
            # 1対1でマッチした場合のみ安全に処理
            if matched_masters and len(matched_masters) == 1:
                master = matched_masters[0]
                updates = {}
                
                # UUIDレコードから不足データを抽出
                for col in colnames:
                    if col in ('id', 'created_at', 'updated_at', 'corporate_number'):
                        continue
                    if is_empty(master[col]) and not is_empty(u_rec[col]):
                        updates[col] = u_rec[col]
                        master[col] = u_rec[col] # ローカル辞書も更新
                
                # ログ出力
                logger.info("-" * 50)
                logger.info(f"【一致】電話番号: {u_rec['phone_number']}")
                logger.info(f"  [マスター] {master['name']} (ID: {master['id']}, 法人番号: {master['corporate_number']})")
                logger.info(f"  [消去UUID] {u_rec['name']} (ID: {u_rec['id']})")
                
                if updates:
                    logger.info("  [マージデータ]")
                    for k, v in updates.items():
                        disp_v = (str(v)[:40] + '...') if len(str(v)) > 40 else str(v)
                        logger.info(f"    - {k}: {disp_v}")
                else:
                    logger.info("  [マージデータ] なし（補完すべき項目なし）")

                # DB更新処理
                if not DRY_RUN:
                    if updates:
                        set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
                        set_clause += ", updated_at = NOW()"
                        values = list(updates.values()) + [master['id']]
                        cur.execute(f"UPDATE companies SET {set_clause} WHERE id = %s", values)
                    
                    cur.execute("DELETE FROM companies WHERE id = %s", (u_rec['id'],))
                
                processed_count += 1
                if processed_count % 1000 == 0:
                    print(f"進捗: {processed_count} 件処理...")

        if not DRY_RUN:
            conn.commit()
            print(f"\n完了: {processed_count} 件のデータを電話番号をキーにしてマージ＆クレンジングしました。")
        else:
            print(f"\nテスト完了: 対象となるレコードは {processed_count} 件でした。")
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