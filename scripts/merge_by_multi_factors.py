#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/merge_by_multi_factors.py
【超高速版＋JSON完全対応版】住所の「町名」でDBをピンポイント検索し、
「企業名」「住所」「代表者名」の3要素で類似度スコアを算出してマージするスクリプト。
"""

import os
import re
import unicodedata
import difflib
import psycopg2
from psycopg2.extras import Json
import logging
from datetime import datetime

# --- 設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

DRY_RUN = False  # 本番実行モード

# --- ログ設定 ---
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(log_dir, f"merge_by_multi_factors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    handlers=[logging.FileHandler(log_filename, encoding='utf-8')])
logger = logging.getLogger(__name__)

def is_empty(val):
    if val is None: return True
    if isinstance(val, str) and str(val).strip() == "": return True
    if isinstance(val, (dict, list)) and not val: return True
    return False

def get_core_name(name):
    """法人格を削除し、大文字・半角に統一したコア名"""
    if not name: return ""
    n = unicodedata.normalize('NFKC', str(name)).upper()
    n = re.sub(r'(株式会社|有限会社|合同会社|一般社団法人|一般財団法人|医療法人|社団法人|財団法人|\(株\)|\(有\)|\(同\)|K\.K\.|INC\.|CO\.,LTD\.|CO\.,|LTD\.|CORPORATION)', '', n, flags=re.IGNORECASE)
    return re.sub(r'[\s\.\,\-\_・]', '', n)

def normalize_addr(addr):
    """住所から都道府県〜番地までを抽出し、ハイフンに統一"""
    if not addr: return ""
    n = unicodedata.normalize('NFKC', str(addr))
    n = re.sub(r'\s', '', n)
    n = n.replace('丁目', '-').replace('番地', '-').replace('番', '-').replace('号', '-')
    n = re.sub(r'\-+', '-', n)
    match = re.search(r'^([^\d]+[\d\-]+)', n)
    return match.group(1).rstrip('-') if match else n[:15]

def normalize_rep(rep_name):
    """代表者名から空白を削除し統一"""
    if not rep_name: return ""
    n = unicodedata.normalize('NFKC', str(rep_name))
    return re.sub(r'[\s　]', '', n)

def main():
    print(f"=== マルチファクター名寄せ処理 (超高速版) {'(テストモード)' if DRY_RUN else '(本番実行)'} ===")
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        cur = conn.cursor()
        
        # カラム一覧の取得
        cur.execute("SELECT * FROM companies LIMIT 0")
        colnames = [desc[0] for desc in cur.description]
        rep_col = 'representative_name' if 'representative_name' in colnames else 'representative'
        if rep_col not in colnames:
            rep_col = None
        
        print("1. 対象レコード（法人番号空）を抽出中...")
        cur.execute("""
            SELECT * FROM companies 
            WHERE corporate_number IS NULL OR corporate_number = ''
        """)
        targets = [dict(zip(colnames, row)) for row in cur.fetchall()]
        print(f" -> 対象レコード: {len(targets)} 件")
        print("2. 1件ずつ町名ベースでピンポイント照合を開始します...")

        processed_count = 0
        rep_sql = f", {rep_col}" if rep_col else ""

        for index, t in enumerate(targets, 1):
            if index % 100 == 0:
                print(f"  ...進捗: {index} / {len(targets)} 件完了")

            t_addr = t.get('address') or ''
            t_pref = t.get('prefecture') or ''
            if not t_addr or not t_pref:
                continue

            match = re.search(r'^([^\d０-９\-−]+)', t_addr)
            if match:
                search_addr = match.group(1)[:12]
            else:
                search_addr = t_addr[:6]

            if len(search_addr) < 3:
                continue

            search_addr_clean = search_addr.replace('%', '\\%').replace('_', '\\_')

            cur.execute(f"""
                SELECT id, name, address, corporate_number {rep_sql}
                FROM companies 
                WHERE prefecture = %s 
                  AND corporate_number IS NOT NULL AND corporate_number != ''
                  AND address LIKE %s
            """, (t_pref, search_addr_clean + '%'))
            
            candidates = cur.fetchall()
            if not candidates:
                continue

            t_core = get_core_name(t.get('name'))
            t_norm_addr = normalize_addr(t_addr)
            t_rep = normalize_rep(t.get(rep_col)) if rep_col else ""
            
            best_match = None
            highest_score = 0
            
            for row in candidates:
                c_id, c_name, c_addr, c_corp = row[0], row[1], row[2], row[3]
                c_rep = row[4] if rep_col else ""
                
                c_core = get_core_name(c_name)
                c_norm_addr = normalize_addr(c_addr)
                c_norm_rep = normalize_rep(c_rep)
                
                score = 0
                
                if t_core and c_core:
                    name_sim = difflib.SequenceMatcher(None, t_core, c_core).ratio()
                    if t_core in c_core or c_core in t_core: name_sim = max(name_sim, 0.9)
                    score += name_sim * 50
                    
                if t_norm_addr and c_norm_addr:
                    addr_sim = difflib.SequenceMatcher(None, t_norm_addr, c_norm_addr).ratio()
                    if t_norm_addr in c_norm_addr or c_norm_addr in t_norm_addr: addr_sim = max(addr_sim, 0.9)
                    score += addr_sim * 30
                    
                if t_rep and c_norm_rep:
                    if t_rep == c_norm_rep:
                        score += 20
                    else:
                        rep_sim = difflib.SequenceMatcher(None, t_rep, c_norm_rep).ratio()
                        score += rep_sim * 20
                        
                if score > highest_score:
                    highest_score = score
                    best_match = {'id': c_id, 'name': c_name, 'address': c_addr, 'corporate_number': c_corp, 'rep_name': c_rep}
                    
            if highest_score >= 80 and best_match:
                cur.execute("SELECT * FROM companies WHERE id = %s", (best_match['id'],))
                master_row = cur.fetchone()
                if not master_row: continue
                master = dict(zip(colnames, master_row))
                
                updates = {}
                for col in colnames:
                    if col in ('id', 'created_at', 'updated_at', 'corporate_number'): continue
                    if is_empty(master[col]) and not is_empty(t[col]):
                        updates[col] = t[col]
                
                logger.info("-" * 50)
                logger.info(f"【一致検出】スコア: {highest_score:.1f}点")
                logger.info(f"  [マージ元] {t['name']} (住所: {t['address']})")
                logger.info(f"  [マスター] {master['name']} (法人番号: {master['corporate_number']}, 住所: {master['address']})")
                
                if not DRY_RUN:
                    if updates:
                        set_clause = ", ".join([f"{k} = %s" for k in updates.keys()])
                        set_clause += ", updated_at = NOW()"
                        
                        # ★ここが最終修正ポイント：リストの中に辞書があればJSON、文字列だけなら配列
                        values = []
                        for v in updates.values():
                            if isinstance(v, dict):
                                values.append(Json(v))
                            elif isinstance(v, list):
                                if any(isinstance(item, dict) for item in v):
                                    values.append(Json(v))
                                else:
                                    values.append(v)
                            else:
                                values.append(v)
                                
                        values.append(master['id'])
                        cur.execute(f"UPDATE companies SET {set_clause} WHERE id = %s", values)
                    
                    cur.execute("DELETE FROM companies WHERE id = %s", (t['id'],))
                
                processed_count += 1

        if not DRY_RUN:
            conn.commit()
            print(f"\n完了: {processed_count} 件のデータをマージ＆削除しました。")
        else:
            print(f"\nテスト完了: マルチファクター照合でマージ可能なレコードは {processed_count} 件でした。")
            print("問題なければ、DRY_RUN = False に変更して再実行してください。")

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