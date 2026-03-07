#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/finalize_companies.py
残存するUUIDレコードに対して、高度な名寄せ（表記揺れ吸収）を行い、
それでも残った新規データには独自の数値IDと仮の法人番号を付与する最終スクリプト。
"""

import os
import re
import unicodedata
import random
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
log_filename = os.path.join(log_dir, f"finalize_companies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(log_filename, encoding='utf-8')])
logger = logging.getLogger(__name__)

def is_empty(val):
    if val is None: return True
    if isinstance(val, str) and str(val).strip() == "": return True
    return False

def normalize_name(name):
    """法人格や記号を削除し、大文字・半角に統一したコア名を取得"""
    if not name: return ""
    n = unicodedata.normalize('NFKC', str(name)).upper()
    n = re.sub(r'(株式会社|有限会社|合同会社|一般社団法人|一般財団法人|医療法人|社団法人|財団法人|\(株\)|\(有\)|\(同\)|K\.K\.|INC\.|CO\.,LTD\.|CO\.,|LTD\.|CORPORATION)', '', n, flags=re.IGNORECASE)
    n = re.sub(r'[\s\.\,\-\_・]', '', n)
    return n

def normalize_addr(addr):
    """住所の丁目番地をハイフンに統一"""
    if not addr: return ""
    n = unicodedata.normalize('NFKC', str(addr))
    n = re.sub(r'\s', '', n)
    n = n.replace('丁目', '-').replace('番地', '-').replace('番', '-').replace('号', '-')
    n = re.sub(r'\-+', '-', n)
    return n

def generate_numeric_id():
    """19桁の新しい数値IDを生成"""
    return str(random.randint(1000000000000000000, 9999999999999999999))

def generate_dummy_corp_num():
    """99から始まる13桁の仮法人番号を生成"""
    return "99" + str(random.randint(10000000000, 99999999999))

def main():
    print(f"=== 最終名寄せ＆クレンジング処理 {'(テストモード)' if DRY_RUN else '(本番実行)'} ===")
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        cur = conn.cursor()
        
        # カラム名の取得
        cur.execute("SELECT * FROM companies LIMIT 0")
        colnames = [desc[0] for desc in cur.description]
        
        print("1. 残存するUUIDレコードを抽出中...")
        cur.execute("""
            SELECT * FROM companies 
            WHERE (corporate_number IS NULL OR corporate_number = '') 
              AND (id LIKE '%-%' OR id !~ '^[0-9]+$')
        """)
        uuids = [dict(zip(colnames, row)) for row in cur.fetchall()]
        print(f" -> 対象UUIDレコード: {len(uuids)} 件")
        
        # UUIDを都道府県ごとにグループ化（処理高速化のため）
        uuid_by_pref = {}
        for u in uuids:
            pref = u.get('prefecture') or 'UNKNOWN'
            if pref not in uuid_by_pref: uuid_by_pref[pref] = []
            uuid_by_pref[pref].append(u)

        merged_count = 0
        converted_count = 0

        print("2. 都道府県ごとにマッチング＆変換処理を開始します...")
        
        for pref, pref_uuids in uuid_by_pref.items():
            master_dict = {}
            if pref != 'UNKNOWN':
                # その都道府県の軽量マスターを取得
                cur.execute("""
                    SELECT id, name, address, corporate_number FROM companies 
                    WHERE prefecture = %s 
                      AND corporate_number IS NOT NULL AND corporate_number != '' 
                      AND id ~ '^[0-9]+$'
                """, (pref,))
                
                for row in cur.fetchall():
                    mid, mname, maddr, mcorp = row
                    nn = normalize_name(mname)
                    if nn:
                        if nn not in master_dict: master_dict[nn] = []
                        master_dict[nn].append({'id': mid, 'name': mname, 'address': maddr, 'corporate_number': mcorp})
            
            for u_rec in pref_uuids:
                nn = normalize_name(u_rec.get('name'))
                na = normalize_addr(u_rec.get('address'))[:10] # 住所の先頭10文字で判定
                
                matched = False
                if nn and nn in master_dict:
                    candidates = master_dict[nn]
                    # コア名が一致し、かつ住所プレフィックスも一致するマスターを抽出
                    valid_masters = [m for m in candidates if normalize_addr(m.get('address'))[:10] == na]
                    
                    if len(valid_masters) == 1:
                        master_basic = valid_masters[0]
                        cur.execute("SELECT * FROM companies WHERE id = %s", (master_basic['id'],))
                        master_row = cur.fetchone()
                        
                        if master_row:
                            master = dict(zip(colnames, master_row))
                            updates = {}
                            for col in colnames:
                                if col in ('id', 'created_at', 'updated_at', 'corporate_number'): continue
                                if is_empty(master[col]) and not is_empty(u_rec[col]):
                                    updates[col] = u_rec[col]
                            
                            logger.info(f"[マージ] {u_rec['name']} -> {master['name']} (法人番号: {master['corporate_number']})")
                            
                            if not DRY_RUN:
                                if updates:
                                    set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
                                    set_clause += ", updated_at = NOW()"
                                    values = list(updates.values()) + [master['id']]
                                    cur.execute(f"UPDATE companies SET {set_clause} WHERE id = %s", values)
                                cur.execute("DELETE FROM companies WHERE id = %s", (u_rec['id'],))
                            
                            merged_count += 1
                            matched = True
                
                if not matched:
                    # マージできなかったものは「完全新規」として数値IDと仮法人番号を付与
                    new_id = generate_numeric_id()
                    new_corp = generate_dummy_corp_num()
                    
                    logger.info(f"[新規変換] {u_rec['name']} -> 新ID: {new_id}, 仮法人番号: {new_corp}")
                    
                    if not DRY_RUN:
                        cur.execute("UPDATE companies SET id = %s, corporate_number = %s, updated_at = NOW() WHERE id = %s", 
                                    (new_id, new_corp, u_rec['id']))
                    converted_count += 1

        if not DRY_RUN:
            conn.commit()
            print("\n=======================================================")
            print(f"完了: すべてのデータが整理されました！")
            print(f" ・既存マスターへマージ＆削除: {merged_count} 件")
            print(f" ・新規企業として数値IDへ変換: {converted_count} 件")
            print("=======================================================")
        else:
            print("\n=======================================================")
            print(f"テスト完了: 以下の処理が予定されています。")
            print(f" ・既存マスターへマージ＆削除: {merged_count} 件")
            print(f" ・新規企業として数値IDへ変換: {converted_count} 件")
            print("=======================================================")
            print("問題なければ、スクリプト内の DRY_RUN = False に変更して再実行してください。")

    except Exception as e:
        if conn and not DRY_RUN: conn.rollback()
        print(f"エラーが発生しました: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()