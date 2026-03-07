#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Legatus ONE 最終インポートスクリプト (fixed_csv_3用)

【実装ロジック】
1. 基本: DBの値が存在すれば維持、NULLならCSVの値で補完 (COALESCE(db, csv))
2. 例外: transaction_type は常にCSVの値で上書き (COALESCE(csv, db))
3. 変換: 売上・利益・資本金は数値をパースして「100万倍」する
4. 特定: 法人番号(ID) または 会社名+都道府県 で既存レコードを特定してUPDATE

【対象】
fixed_csv_3 フォルダ配下の全CSVファイル
"""

import csv
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

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

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("main")

TABLE_NAME = "companies"

# ヘッダーとDBカラムのマッピング
HEADER_MAP = {
    "法人番号": "corporate_number",
    "電話番号": "phone_number",
    "電話番号(窓口)": "phone_number",
    "取引種別": "transaction_type",  # ★強制更新対象
    "代表者名": "representative_name",
    "代表者住所": "representative_home_address",
    "代表者誕生日": "representative_birth_date",
    "役員": "executives",
    "仕入れ先": "suppliers",
    "取引先": "clients",
    "直近売上": "latest_revenue",    # ★100万倍対象
    "売上高": "latest_revenue",      # ★100万倍対象
    "直近利益": "latest_profit",     # ★100万倍対象
    "資本金": "capital_stock",       # ★100万倍対象
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
# データ変換・クレンジング関数
# ==========================================

def normalize_corporate_number(val: Any) -> Optional[str]:
    """法人番号の正規化 (指数表記対応)"""
    if not val: return None
    s = str(val).strip()
    if 'E' in s.upper():
        try:
            s = "{:.0f}".format(float(s))
        except:
            return None
    s = re.sub(r"[^\d]", "", s)
    return s if len(s) == 13 else None

def parse_numeric_million(val: Any) -> Optional[int]:
    """
    【100万倍ロジック】
    文字列から数値を抽出し、1,000,000倍して返す。
    例: "100" -> 100,000,000
    """
    if not val: return None
    # カンマや円などを除去し、数字とドット、マイナスのみ残す
    s = str(val).strip()
    s = re.sub(r"[^\d\.\-]", "", s)
    if not s: return None
    
    try:
        # floatにしてから100万倍し、intにキャスト
        return int(float(s) * 1_000_000)
    except:
        return None

def parse_numeric_plain(val: Any) -> Optional[int]:
    """通常の数値変換 (社員数など)"""
    if not val: return None
    s = str(val).strip().replace(",", "").replace(" ", "").replace("円", "")
    s = re.sub(r"[^\d\.\-]", "", s)
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
    """法人番号でDBを検索し、既存レコードのIDを取得"""
    corp_nums = set(r["corporate_number"] for r in batch_rows if r.get("corporate_number"))
    if not corp_nums: return batch_rows

    cur = conn.cursor()
    try:
        query = f"SELECT corporate_number, id FROM {TABLE_NAME} WHERE corporate_number = ANY(%s)"
        cur.execute(query, (list(corp_nums),))
        existing_map = {row[0]: row[1] for row in cur.fetchall()}
        
        for row in batch_rows:
            cn = row.get("corporate_number")
            if cn and cn in existing_map:
                row["id"] = existing_map[cn]
    finally:
        cur.close()
    return batch_rows

def resolve_ids_by_name_pref(conn, batch_rows: List[Dict]) -> List[Dict]:
    """会社名+都道府県でDBを検索し、既存レコードのIDを取得"""
    target_rows = [r for r in batch_rows if not r.get("id") and r.get("name") and r.get("prefecture")]
    if not target_rows: return batch_rows

    search_keys = list(set((r["name"], r["prefecture"]) for r in target_rows))
    
    cur = conn.cursor()
    try:
        query = f"SELECT name, prefecture, id FROM {TABLE_NAME} WHERE (name, prefecture) IN %s"
        # execute_valuesだとSELECTに使えないため単純展開(件数が多い場合は分割推奨だが今回はバッチサイズで制御)
        cur.execute(query, (tuple(search_keys),))
        name_pref_map = {(row[0], row[1]): row[2] for row in cur.fetchall()}
        
        for row in batch_rows:
            if not row.get("id"):
                key = (row.get("name"), row.get("prefecture"))
                if key in name_pref_map:
                    row["id"] = name_pref_map[key]
    finally:
        cur.close()
    return batch_rows

# ==========================================
# UPSERT実行エンジン
# ==========================================

def execute_smart_upsert(conn, batch_list):
    if not batch_list: return
    
    # 1. 名寄せ処理
    batch_list = resolve_existing_ids(conn, batch_list)
    batch_list = resolve_ids_by_name_pref(conn, batch_list)

    # 2. バッチ内での重複排除 (ID基準)
    id_dedup_map = {}
    for row in batch_list:
        c_id = row.get("id")
        if c_id:
            # 後勝ちで上書き (同じIDならデータの新しい方を採用)
            id_dedup_map[c_id] = row
    
    if not id_dedup_map:
        return

    unique_rows = list(id_dedup_map.values())
    cols = list(unique_rows[0].keys())
    cur = conn.cursor()

    # --- SET句の構築 (ここが重要) ---
    update_clauses = []
    for c in cols:
        if c in ['id', 'corporate_number']:
            continue # キー項目は更新しない
        
        if c == 'transaction_type':
            # ★例外: 取引種別は常にCSVの値で上書き (CSVがNULLならDB維持)
            # COALESCE(EXCLUDED, TABLE) = 新しい値優先
            update_clauses.append(f"{c} = COALESCE(EXCLUDED.{c}, {TABLE_NAME}.{c})")
        else:
            # ★基本: DBに値があれば維持、なければCSVの値 (NULL補完)
            # COALESCE(TABLE, EXCLUDED) = 古い値優先
            update_clauses.append(f"{c} = COALESCE({TABLE_NAME}.{c}, EXCLUDED.{c})")
    
    # updated_at は更新
    update_clauses.append("updated_at = NOW()")

    query = f"""
        INSERT INTO {TABLE_NAME} ({','.join(cols)}) 
        VALUES %s 
        ON CONFLICT (id) 
        DO UPDATE SET {','.join(update_clauses)}
    """
    
    try:
        execute_values(cur, query, [tuple(d.values()) for d in unique_rows])
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"UPSERT Error: {e}")

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
            
            # IDの仮設定 (後で名寄せで書き換わる可能性あり)
            raw_c_num = row.get("法人番号") or row.get("ID")
            c_num_normalized = normalize_corporate_number(raw_c_num)
            processed_row["id"] = c_num_normalized
            processed_row["corporate_number"] = c_num_normalized

            for csv_h, db_col in col_map.items():
                if db_col == "corporate_number": continue 

                val = row[csv_h].strip()
                if val == "":
                    processed_row[db_col] = None
                
                # --- 100万倍ロジックの適用 ---
                elif db_col in ["latest_revenue", "latest_profit", "capital_stock"]:
                    processed_row[db_col] = parse_numeric_million(val)
                
                # --- 通常の数値 ---
                elif db_col == "employee_count":
                    processed_row[db_col] = parse_numeric_plain(val)
                
                # --- 年号 ---
                elif db_col == "founded_year":
                    processed_row[db_col] = parse_founded_year(val)
                
                # --- 配列 ---
                elif db_col in ARRAY_COLS:
                    processed_row[db_col] = [x.strip() for x in val.split(",") if x.strip()]
                
                # --- その他文字列 ---
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
    target_dir = Path("fixed_csv_3")
    
    # 処理対象ファイル
    csv_files = sorted(target_dir.glob("*.csv"))
    
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