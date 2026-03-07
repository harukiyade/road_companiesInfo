#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Legatus ONE 高速バルクインポート用スクリプト (完全版 v3 - csv_2全量対応)

【対象】
- csv_2 フォルダ配下のすべてのCSVファイル (再帰的に検索)

【機能】
- DB制約エラーの完全回避: ON CONFLICT (name, prefecture) を廃止。
- 代替検索ロジック: 「会社名+都道府県」でSELECT検索を行い、IDを特定する機能を追加。
- 挙動:
  1. 法人番号でDBを検索 -> あればそのIDで更新
  2. (IDがない場合) 会社名+都道府県でDBを検索 -> あればそのIDで更新
  3. それでもIDが見つからないデータ -> スキップ (重複登録防止のため)
  4. NULL補完: 既存データは保護し、空欄のみ埋める
"""

import csv
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

# ==========================================
# 設定
# ==========================================
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("main")

TABLE_NAME = "companies"

HEADER_MAP = {
    "法人番号": "corporate_number",
    "電話番号": "phone_number",
    "電話番号(窓口)": "phone_number",
    "取引種別": "transaction_type",
    "代表者名": "representative_name",
    "代表者住所": "representative_home_address",
    "代表者誕生日": "representative_birth_date",
    "役員": "executives",
    "仕入れ先": "suppliers",
    "取引先": "clients",
    "直近売上": "latest_revenue",
    "売上高": "latest_revenue",
    "直近利益": "latest_profit",
    "資本金": "capital_stock",
    "会社名": "name",
    "URL": "company_url",
    "都道府県": "prefecture",
    "社員数": "employee_count",
    "設立": "founded_year",
    "上場": "listing",
    "郵便番号": "postal_code",
    "住所": "address",
}

ARRAY_COLS = {"executives", "suppliers", "clients"}

# ==========================================
# クレンジング関数
# ==========================================

def normalize_corporate_number(val: Any) -> Optional[str]:
    if not val: return None
    s = str(val).strip()
    if 'E' in s.upper():
        try:
            s = "{:.0f}".format(float(s))
        except:
            return None
    s = re.sub(r"[^\d]", "", s)
    return s if len(s) == 13 else None

def parse_numeric(val: Any) -> Optional[int]:
    if not val: return None
    s = str(val).strip().replace(",", "").replace(" ", "").replace("円", "")
    try:
        return int(float(s))
    except:
        return None

def parse_founded_year(val: Any) -> Optional[int]:
    if not val: return None
    match = re.search(r'(\d{4})', str(val))
    return int(match.group(1)) if match else None

# ==========================================
# ID解決 (名寄せ) ロジック
# ==========================================

def resolve_existing_ids(conn, batch_rows: List[Dict]) -> List[Dict]:
    """
    1. 法人番号 (corporate_number) でDBを検索し、IDを解決する
    """
    if not batch_rows: return []

    corp_nums = set()
    for row in batch_rows:
        cn = row.get("corporate_number")
        if cn: corp_nums.add(cn)
    
    if corp_nums:
        cur = conn.cursor()
        query = f"SELECT corporate_number, id FROM {TABLE_NAME} WHERE corporate_number = ANY(%s)"
        try:
            cur.execute(query, (list(corp_nums),))
            existing_map = {row[0]: row[1] for row in cur.fetchall()}
            
            # ID書き換え
            for row in batch_rows:
                cn = row.get("corporate_number")
                if cn and cn in existing_map:
                    row["id"] = existing_map[cn]
        except Exception as e:
            logger.warning(f"CorpNum Resolution Warning: {e}")
            conn.rollback()
        finally:
            cur.close()
    
    return batch_rows

def resolve_ids_by_name_pref(conn, batch_rows: List[Dict]) -> List[Dict]:
    """
    2. IDが決まっていないデータについて、(会社名 + 都道府県) でDBを検索し、IDを解決する
       ※ユニーク制約がない環境での代替策
    """
    # IDがまだない行を特定
    target_rows = [r for r in batch_rows if not r.get("id") and r.get("name") and r.get("prefecture")]
    if not target_rows:
        return batch_rows

    # 検索キーのペアを作成 (重複除去)
    search_keys = list(set((r["name"], r["prefecture"]) for r in target_rows))
    
    if search_keys:
        cur = conn.cursor()
        # WHERE (name, prefecture) IN ((a,b), (c,d)...) の形を作る
        query = f"""
            SELECT name, prefecture, id 
            FROM {TABLE_NAME} 
            WHERE (name, prefecture) IN %s
        """
        try:
            execute_values(cur, query, [search_keys], template=None, page_size=1000)
            # マップ作成: キー(name, pref) -> 値(id)
            name_pref_map = {(row[0], row[1]): row[2] for row in cur.fetchall()}
            
            # ID書き換え
            count_resolved = 0
            for row in batch_rows:
                if not row.get("id"):
                    key = (row.get("name"), row.get("prefecture"))
                    if key in name_pref_map:
                        row["id"] = name_pref_map[key]
                        count_resolved += 1
            
        except Exception as e:
            logger.warning(f"Name+Pref Resolution Warning: {e}")
            conn.rollback()
        finally:
            cur.close()

    return batch_rows

# ==========================================
# UPSERT実行エンジン
# ==========================================

def execute_smart_upsert(conn, batch_list):
    if not batch_list: return
    
    # 1. 法人番号で名寄せ
    batch_list = resolve_existing_ids(conn, batch_list)
    # 2. 会社名+都道府県で名寄せ
    batch_list = resolve_ids_by_name_pref(conn, batch_list)

    cur = conn.cursor()
    
    # バッチ内重複排除 (ID単位)
    id_dedup_map = {}
    skipped_count = 0

    for row in batch_list:
        c_num = row.get("id")
        
        # IDが特定できたものだけを更新対象とする
        if c_num:
            id_dedup_map[c_num] = row
        else:
            # IDが見つからず、法人番号も不正なデータはスキップ
            skipped_count += 1

    # --- IDベースのUPSERT実行 ---
    if id_dedup_map:
        unique_rows = list(id_dedup_map.values())
        cols = list(unique_rows[0].keys())
        
        # corporate_number は制約保護のため更新対象から外す
        # 既存がNULLならCSVの値を入れる (COALESCE)
        update_clauses = [
            f"{c}=COALESCE({TABLE_NAME}.{c}, EXCLUDED.{c})" 
            for c in cols if c not in ['id', 'corporate_number']
        ]
        
        query = f"""
            INSERT INTO {TABLE_NAME} ({','.join(cols)}) 
            VALUES %s 
            ON CONFLICT (id) 
            DO UPDATE SET {','.join(update_clauses)}, updated_at = NOW()
        """
        
        try:
            execute_values(cur, query, [tuple(d.values()) for d in unique_rows])
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"ID UPSERT Error: {e}")
            cur = conn.cursor()

    cur.close()

# ==========================================
# メイン処理
# ==========================================

def process_file_bulk(conn, file_path: Path):
    logger.info(f"処理開始: {file_path.name}")
    
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        if not header: return

        col_map = {h: HEADER_MAP[h.strip()] for h in header if h.strip() in HEADER_MAP}
        
        batch_data = []
        for row_num, row in enumerate(reader, 1):
            processed_row = {}
            
            # 生の値を取得
            raw_c_num = row.get("法人番号") or row.get("ID")
            c_num_normalized = normalize_corporate_number(raw_c_num)
            
            # ID候補としてセット
            processed_row["id"] = c_num_normalized
            # corporate_numberは検索用に保持
            processed_row["corporate_number"] = c_num_normalized

            for csv_h, db_col in col_map.items():
                if db_col == "corporate_number": continue 

                val = row[csv_h].strip()
                if val == "":
                    processed_row[db_col] = None
                elif db_col == "founded_year":
                    processed_row[db_col] = parse_founded_year(val)
                elif db_col in ["latest_revenue", "latest_profit", "capital_stock", "employee_count"]:
                    processed_row[db_col] = parse_numeric(val)
                elif db_col in ARRAY_COLS:
                    processed_row[db_col] = [x.strip() for x in val.split(",") if x.strip()]
                else:
                    processed_row[db_col] = val

            batch_data.append(processed_row)

            if len(batch_data) >= 1000:
                execute_smart_upsert(conn, batch_data)
                batch_data = []
                print(f"\r  ... {row_num} 行完了", end="", flush=True)

        if batch_data:
            execute_smart_upsert(conn, batch_data)
        print(f"\n完了: {file_path.name}")

def main():
    # ---------------------------------------------------------
    # 【変更点】対象フォルダを csv_2 (サブフォルダ含む) に変更
    # ---------------------------------------------------------
    target_dir = Path("csv_2")
    
    # 再帰的に全てのCSVを検索 (rglob)
    csv_files = sorted(target_dir.rglob("*.csv"))
    
    if not csv_files:
        logger.error(f"CSVが見つかりません: {target_dir}")
        return

    logger.info(f"対象ファイル数: {len(csv_files)} 件")

    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
    )
    
    try:
        for fp in csv_files:
            process_file_bulk(conn, fp)
    finally:
        conn.close()

if __name__ == "__main__":
    main()