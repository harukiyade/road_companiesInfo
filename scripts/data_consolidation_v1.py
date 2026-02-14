import os
import pandas as pd
import psycopg2

# --- 設定項目 ---
DB_CONFIG = {
    "host": "34.84.189.233",
    "database": "postgres",
    "user": "postgres",
    "password": "Legatus2000/"
}

# 除外ファイルリスト（指定された36ファイル）
SKIP_FILES = [
    "csv_2/csv/107.csv", "csv_2/csv/109.csv", "csv_2/csv/110.csv", "csv_2/csv/111.csv",
    "csv_2/csv/112.csv", "csv_2/csv/113.csv", "csv_2/csv/114.csv", "csv_2/csv/115.csv",
    "csv_2/csv/116.csv", "csv_2/csv/117.csv", "csv_2/csv/122.csv",
    "csv_2/fixed/107.fixed.csv", "csv_2/fixed/109.fixed.csv", "csv_2/fixed/110.fixed.csv",
    "csv_2/fixed/111.fixed.csv", "csv_2/fixed/112.fixed.csv", "csv_2/fixed/113.fixed.csv",
    "csv_2/fixed/114.fixed.csv", "csv_2/fixed/115.fixed.csv", "csv_2/fixed/116.fixed.csv",
    "csv_2/fixed/117.fixed.csv", "csv_2/fixed/122.fixed.csv",
    "csv_2/import_firstTime/105.csv", "csv_2/import_firstTime/106.csv",
    "csv_2/import_firstTime/107.csv", "csv_2/import_firstTime/110.csv",
    "csv_2/import_firstTime/111.csv", "csv_2/import_firstTime/112.csv",
    "csv_2/import_firstTime/113.csv", "csv_2/import_firstTime/114.csv",
    "csv_2/import_firstTime/115.csv", "csv_2/import_firstTime/116.csv",
    "csv_2/import_firstTime/117.csv", "csv_2/import_firstTime/119.csv",
    "csv_2/import_firstTime/122.csv", "csv_2/import_firstTime/old128.csv"
]

# マッピング定義
MAPPING = {
    'AD': 'ad_flag', 'AD締結': 'ad_flag',
    'NDA': 'nda_flag', 'NDA締結': 'nda_flag',
    'SBフラグ': 'sb_flag',
    '取引種別': 'transaction_type',
    '概況': 'overview', '概要': 'overview', '担当者コメント': 'overview',
    '説明': 'description',
    '株式保有率': 'shareholders',
    '取締役': 'executives', '役員': 'executives',
    '創業': 'established', '設立': 'established',
    '事業年度': 'fiscal_month',
    '資本金': 'capital',
    '直近売上': 'latest_revenue',
    '直近利益': 'latest_profit'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def load_csv(file_path):
    encoding = 'cp932' if 'csv0115' in file_path else 'utf-8-sig'
    try:
        df = pd.read_csv(file_path, encoding=encoding, dtype=str)
        return df.where(pd.notnull(df), None)
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return None

def process_all_csv():
    conn = get_db_connection()
    cur = conn.cursor()
    
    target_files = []
    for root, _, files in os.walk("csv_2"):
        for file in files:
            if not file.endswith(".csv"): continue
            fp = os.path.join(root, file).replace("\\", "/")
            if fp not in SKIP_FILES:
                target_files.append(fp)

    unmatched_records = []

    # --- Phase 1: 法人番号優先マッチング ---
    print("Starting Phase 1: Matching by Corporate Number...")
    for fp in target_files:
        df = load_csv(fp)
        if df is None: continue
        if '法人番号' not in df.columns: continue

        print(f"Processing (Phase 1): {fp}")
        for _, row in df.iterrows():
            corp_num = row.get('法人番号')
            if corp_num:
                corp_num = str(corp_num).split('.')[0].zfill(13)
                update_record(cur, row, corp_num=corp_num)
        conn.commit()

    # --- Phase 2: 名前＋住所マッチング ---
    print("Starting Phase 2: Matching by Name + Address...")
    for fp in target_files:
        df = load_csv(fp)
        if df is None: continue

        print(f"Processing (Phase 2): {fp}")
        for _, row in df.iterrows():
            updated_id = update_record(cur, row, corp_num=None)
            if not updated_id:
                name = row.get('会社名') or row.get('企業名')
                if name:
                    unmatched_records.append({
                        "file": fp,
                        "company_name": name,
                        "corporate_number": row.get('法人番号', '')
                    })
        conn.commit()

    if unmatched_records:
        log_df = pd.DataFrame(unmatched_records)
        log_df.to_csv("unmatched_records_log.csv", index=False, encoding='utf-8-sig')
        print(f"Done. Unmatched records logged. (Count: {len(unmatched_records)})")
    else:
        print("Done. All records matched.")

    cur.close()
    conn.close()

def update_record(cur, csv_row, corp_num=None):
    if corp_num:
        cur.execute("SELECT * FROM companies WHERE corporate_number = %s", (corp_num,))
    else:
        name = csv_row.get('会社名') or csv_row.get('企業名')
        addr = csv_row.get('住所') or csv_row.get('所在地') or csv_row.get('会社住所')
        if not name or not addr: return None
        cur.execute("SELECT * FROM companies WHERE name = %s AND address = %s", (name, addr))
    
    db_row = cur.fetchone()
    if not db_row: return None

    column_names = [desc[0] for desc in cur.description]
    db_data = dict(zip(column_names, db_row))
    db_id = db_data['id']
    
    update_fields = {}
    
    # 1. マッピングに基づく空カラム補填
    for csv_header, db_col in MAPPING.items():
        csv_val = csv_row.get(csv_header)
        if csv_val is None or csv_val == '': continue
        
        # DB側が空(None or '')なら更新対象にする
        if not db_data.get(db_col):
            val_str = str(csv_val).strip()
            # 配列型（shareholders, executives）への対応
            if db_col in ['shareholders', 'executives']:
                update_fields[db_col] = [val_str]
            else:
                update_fields[db_col] = val_str

    # 2. 特殊処理: listing (非上場のみ対象)
    listing_val = csv_row.get('区分')
    if listing_val == '非上場' and not db_data.get('listing'):
        update_fields['listing'] = '非上場'

    # 更新実行
    if update_fields:
        set_clause = ", ".join([f"{k} = %s" for k in update_fields.keys()])
        params = list(update_fields.values())
        params.append(db_id)
        try:
            cur.execute(f"UPDATE companies SET {set_clause}, updated_at = NOW() WHERE id = %s", params)
        except Exception as e:
            print(f"Error updating ID {db_id}: {e}")
            return None
        return db_id
    
    return db_id

if __name__ == "__main__":
    process_all_csv()