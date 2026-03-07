import csv
import logging
import os
import re
import sys
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values

# --- 設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("importer")

# マッピング定義
HEADER_MAP = {
    "会社名": "name", "商号又は名称": "name",
    "都道府県": "prefecture",
    "法人番号": "corporate_number",
    "取引種別": "transaction_type",
    "直近売上": "latest_revenue", "売上高": "latest_revenue", "法人＿売上高": "latest_revenue",
    "直近利益": "latest_profit", "法人＿当期純利益(損失)": "latest_profit",
    "資本金": "capital_stock", "法人＿資本金": "capital_stock",
    "電話番号": "phone_number", "電話番号(窓口)": "phone_number",
    "代表者名": "representative_name", "代表者住所": "representative_home_address",
    "役員": "executives", "取締役": "executives",
    "仕入れ先": "suppliers", "取引先": "clients",
    "社員数": "employee_count", "設立": "founded_year",
    "郵便番号": "postal_code", "会社郵便番号": "postal_code",
    "住所": "address", "会社住所": "address"
}

# DBのカラムリスト（ユニークなものだけを抽出）
DB_COLS = sorted(list(set(HEADER_MAP.values())))
ARRAY_COLS = {"executives", "suppliers", "clients"}

def get_encoding(file_path):
    for enc in ['utf-8-sig', 'cp932']:
        try:
            with open(file_path, 'r', encoding=enc) as f:
                f.readline()
                return enc
        except:
            continue
    return 'utf-8'

def normalize_val(val):
    if val is None: return None
    s = str(val).strip()
    if s.lower() in ["", "nan", "none", "null"]: return None
    return s

def parse_num_million(val):
    s = normalize_val(val)
    if not s: return None
    s = re.sub(r"[^\d\.\-]", "", s)
    try: return int(float(s) * 1_000_000)
    except: return None

def normalize_corp_num(val):
    s = normalize_val(val)
    if not s: return None
    if 'E' in s.upper():
        try: s = "{:.0f}".format(float(s))
        except: return None
    s = re.sub(r"[^\d]", "", s)
    return s if len(s) >= 10 else None

def process_file(conn, file_path):
    enc = get_encoding(file_path)
    logger.info(f"処理開始: {file_path.name} (Encoding: {enc})")
    
    with open(file_path, "r", encoding=enc, errors="replace") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        if not headers: return
        
        current_map = {}
        for h in headers:
            clean_h = h.strip()
            if clean_h in HEADER_MAP:
                current_map[h] = HEADER_MAP[clean_h]

        batch = []
        for row in reader:
            data = {v: None for v in DB_COLS}
            for raw_h, db_col in current_map.items():
                val = row.get(raw_h)
                if db_col in ["latest_revenue", "latest_profit", "capital_stock"]:
                    data[db_col] = parse_num_million(val)
                elif db_col == "corporate_number":
                    data[db_col] = normalize_corp_num(val)
                elif db_col in ARRAY_COLS:
                    data[db_col] = [x.strip() for x in val.split(",") if x.strip()] if val else None
                else:
                    data[db_col] = normalize_val(val)
            
            if data["name"]:
                batch.append(data)
                
            if len(batch) >= 500:
                upsert_batch(conn, batch)
                batch = []
        if batch:
            upsert_batch(conn, batch)

def upsert_batch(conn, batch):
    cur = conn.cursor()
    for row in batch:
        row['id'] = None
        if row['corporate_number'] and len(row['corporate_number']) == 13:
            cur.execute("SELECT id FROM companies WHERE corporate_number = %s", (row['corporate_number'],))
            res = cur.fetchone()
            if res: row['id'] = res[0]
            
        if not row['id'] and row['name'] and row['prefecture']:
            cur.execute("SELECT id FROM companies WHERE name = %s AND prefecture = %s", (row['name'], row['prefecture']))
            res = cur.fetchone()
            if res: row['id'] = res[0]

    updates = [r for r in batch if r['id'] is not None]
    inserts = [r for r in batch if r['id'] is None]

    # --- 更新処理 ---
    if updates:
        # 重複を避けるため DB_COLS を使用
        cols_to_update = [c for c in DB_COLS if c != 'corporate_number']
        set_clauses = [f"{c} = COALESCE(companies.{c}, EXCLUDED.{c})" for c in cols_to_update if c != 'transaction_type']
        set_clauses.append("transaction_type = EXCLUDED.transaction_type")
        set_clauses.append("updated_at = NOW()")

        query = f"""
            INSERT INTO companies (id, {', '.join(cols_to_update)})
            VALUES %s
            ON CONFLICT (id) DO UPDATE SET {', '.join(set_clauses)}
        """
        vals = [[r['id']] + [r[c] for c in cols_to_update] for r in updates]
        execute_values(cur, query, vals)

    # --- 新規登録処理 ---
    if inserts:
        query = f"INSERT INTO companies ({', '.join(DB_COLS)}) VALUES %s ON CONFLICT DO NOTHING"
        vals = [[r[c] for c in DB_COLS] for r in inserts]
        execute_values(cur, query, vals)

    conn.commit()
    cur.close()

def main():
    target_dir = Path("fixed_csv_3")
    csv_files = sorted(target_dir.glob("*.csv"))
    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE)
    try:
        for fp in csv_files:
            process_file(conn, fp)
    finally:
        conn.close()

if __name__ == "__main__":
    main()