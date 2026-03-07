#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/match_with_gbiz.py
国税庁のCSVマスタデータと突合し、法人番号を付与します。
DBのカラム型を自動判定し、JSONB/ARRAYの型エラーを完全に防ぎながらマージします。
"""

import os
import csv
import re
import unicodedata
import psycopg2
from psycopg2.extras import Json
import logging
from datetime import datetime

DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

NTA_CSV_PATH = "data/00_zenkoku_all_20260130.csv"
DRY_RUN = False  # 本番モード

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def is_empty(val):
    if val is None: return True
    if isinstance(val, str) and str(val).strip() == "": return True
    if isinstance(val, (dict, list)) and not val: return True
    return False

def get_core_name(name):
    if not name: return ""
    n = unicodedata.normalize('NFKC', str(name)).upper()
    n = re.sub(r'(株式会社|有限会社|合同会社|一般社団法人|一般財団法人|医療法人|社団法人|財団法人|\(株\)|\(有\)|\(同\)|K\.K\.|INC\.|CO\.,LTD\.|CO\.,|LTD\.|CORPORATION)', '', n, flags=re.IGNORECASE)
    return re.sub(r'[\s\.\,\-\_・]', '', n)

def normalize_addr(addr):
    if not addr: return ""
    n = unicodedata.normalize('NFKC', str(addr))
    n = re.sub(r'\s', '', n)
    n = n.replace('丁目', '-').replace('番地', '-').replace('番', '-').replace('号', '-')
    n = re.sub(r'\-+', '-', n)
    match = re.search(r'^([^\d]+[\d\-]+)', n)
    return match.group(1).rstrip('-') if match else n[:15]

def main():
    print(f"=== 国税庁マスタデータ突合＆自動マージ処理 {'(テスト)' if DRY_RUN else '(本番)'} ===")
    
    if not os.path.exists(NTA_CSV_PATH):
        print(f"エラー: マスタファイル '{NTA_CSV_PATH}' が見つかりません。")
        return

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

        print("1. DBから対象レコード（法人番号が空）を読み込み中...")
        cur.execute("SELECT id, name, address FROM companies WHERE corporate_number IS NULL OR corporate_number = ''")
        targets = []
        for row in cur.fetchall():
            targets.append({
                'id': row[0],
                'name': row[1],
                'core_name': get_core_name(row[1]),
                'norm_addr': normalize_addr(row[2])
            })
        print(f" -> 突合対象: {len(targets)} 件")

        remaining_targets = {t['id']: t for t in targets}
        found_updates = {}

        print(f"2. 国税庁データ ({NTA_CSV_PATH}) との突合を開始します...")
        csv.field_size_limit(1000000)

        with open(NTA_CSV_PATH, 'r', encoding='cp932', errors='replace') as f:
            reader = csv.reader(f)
            line_count = 0

            for row in reader:
                line_count += 1
                if line_count % 500000 == 0:
                    print(f"  ...マスタ {line_count} 行目を処理中 (残りターゲット: {len(remaining_targets)}件)")

                if len(row) < 12: continue
                csv_corp = str(row[1]).strip()
                csv_name = str(row[6]).strip()
                csv_addr = str(row[9]).strip() + str(row[10]).strip() + str(row[11]).strip()

                if not csv_corp or not csv_name or not re.match(r'^\d{13}$', csv_corp):
                    continue

                csv_core_name = get_core_name(csv_name)
                csv_norm_addr = normalize_addr(csv_addr)

                matched_ids = []
                for tid, t in list(remaining_targets.items()):
                    if t['core_name'] == csv_core_name and (t['norm_addr'] in csv_norm_addr or csv_norm_addr in t['norm_addr']):
                        matched_ids.append(tid)
                
                for matched_id in matched_ids:
                    t = remaining_targets.pop(matched_id)
                    found_updates[matched_id] = csv_corp
                    
                if not remaining_targets:
                    break

        print(f"\n3. 突合完了: {len(targets)} 件中、{len(found_updates)} 件の法人番号を特定しました。")

        if not DRY_RUN and found_updates:
            print("DBに反映しています（既存の法人番号と重複する場合は自動でマージします）...")
            merged_count = 0
            updated_count = 0

            for tid, corp_num in found_updates.items():
                cur.execute("SELECT * FROM companies WHERE corporate_number = %s", (corp_num,))
                existing_row = cur.fetchone()

                if existing_row:
                    master = dict(zip(colnames, existing_row))
                    
                    cur.execute("SELECT * FROM companies WHERE id = %s", (tid,))
                    target_row = cur.fetchone()
                    if not target_row: continue
                    target = dict(zip(colnames, target_row))

                    updates = {}
                    for col in colnames:
                        if col in ('id', 'created_at', 'updated_at', 'corporate_number'): continue
                        if is_empty(master.get(col)) and not is_empty(target.get(col)):
                            updates[col] = target[col]

                    if updates:
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

                    cur.execute("DELETE FROM companies WHERE id = %s", (tid,))
                    merged_count += 1
                    logger.info(f"[マージ＆削除] {target['name']} -> 既存の法人番号({corp_num})のレコードに統合しました。")

                else:
                    cur.execute("UPDATE companies SET corporate_number = %s, updated_at = NOW() WHERE id = %s", (corp_num, tid))
                    updated_count += 1
                    logger.info(f"[新規付与] ID:{tid} に法人番号({corp_num})を付与しました。")

            conn.commit()
            print("\n=======================================================")
            print(f"DBの更新が完全に終了しました！")
            print(f" ・新規で法人番号を付与した件数: {updated_count} 件")
            print(f" ・既存の法人番号と合致したためマージ＆削除した件数: {merged_count} 件")
            print("=======================================================")

    except Exception as e:
        if conn and not DRY_RUN: conn.rollback()
        logger.error(f"エラーが発生しました: {e}")
        print(f"エラーが発生しました: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()