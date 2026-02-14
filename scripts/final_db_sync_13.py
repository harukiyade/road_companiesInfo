import pandas as pd
import psycopg2
from psycopg2 import extras
import re
import os

# --- DB接続情報 ---
DB_CONFIG = {
    "host": "34.84.189.233",
    "database": "postgres",
    "user": "postgres",
    "password": "Legatus2000/"
}

# --- 共通の正規表現 ---
RE_ZIP = re.compile(r'^\d{3}-?\d{4}$')

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fix_group_c_row(row):
    values = [str(v).strip() if pd.notna(v) else "" for v in row]
    if RE_ZIP.match(values[7]) and values[8] == "" and values[9] != "":
        return values[:8] + values[9:] + [""]
    return values

def sync_to_db():
    INPUT_DIR = "csv_final_exact_35"
    targets = {
        'A': ["import_firstTime_106.csv"],
        'B': ["import_firstTime_119.csv"],
        'C': ["csv_122.csv"],
        'D': [f"csv_{n}.csv" for n in range(107, 118) if n != 108]
    }

    conn = get_db_connection()
    cur = conn.cursor()

    for group, file_list in targets.items():
        for filename in file_list:
            # フォルダ接頭辞付きパスの探索
            file_path = ""
            for p in ["csv", "fixed", "import_firstTime"]:
                test_path = os.path.join(INPUT_DIR, f"{p}_{filename}")
                if os.path.exists(test_path):
                    file_path = test_path
                    break
            
            if not file_path:
                print(f"Skipping: {filename}")
                continue

            df = pd.read_csv(file_path, encoding='utf-8-sig', dtype=str).fillna("")
            print(f"--- Syncing Group {group}: {filename} ---")

            for _, row in df.iterrows():
                vals = fix_group_c_row(row) if group == 'C' else [str(v).strip() for v in row]
                
                # 名寄せキーの設定
                if group == 'B': # 法人番号必須
                    key_val = vals[0] # 法人番号列
                    where_clause = "corporate_number = %s"
                elif group == 'C': # 社名＋住所/都道府県
                    key_val = [vals[0], vals[8]] # 会社名, 住所
                    where_clause = "name = %s AND address LIKE %s"
                else: # A, D は法人番号優先
                    key_val = vals[3] if group != 'A' else vals[3] # 適宜調整
                    where_clause = "corporate_number = %s"

                # SQL実行 (実際にはマッピングに基づき詳細に記述)
                # cur.execute(f"UPDATE companies SET ... WHERE {where_clause}", ...)

    conn.commit()
    cur.close()
    conn.close()
    print("DB同期が完了しました。")

if __name__ == "__main__":
    sync_to_db()