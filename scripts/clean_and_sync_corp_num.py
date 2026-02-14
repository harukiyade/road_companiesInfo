import pandas as pd
import os
import re
from sqlalchemy import create_engine, text

# --- 設定 ---
CSV_PATH = "data/master_data.csv"
DB_NAME = "postgres" 
DB_USER = "postgres"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"

DB_URL = f"postgresql://{DB_USER}:{os.getenv('POSTGRES_PASSWORD')}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def normalize_name(name):
    if not name or pd.isna(name): return ""
    return re.sub(r'[\[\]「」株式会社有限会社（）\s]', '', str(name))

def normalize_address(addr):
    if not addr or pd.isna(addr): return ""
    match = re.match(r'^[^0-9\-\s－丁番号]+', str(addr))
    return match.group(0) if match else str(addr)[:10]

def run_update():
    if not os.getenv('POSTGRES_PASSWORD'):
        print("エラー: 環境変数 POSTGRES_PASSWORD が設定されていません。")
        return

    try:
        engine = create_engine(DB_URL)
        print(f"CSVを読み込んでいます: {CSV_PATH}")
        master_df = pd.read_csv(CSV_PATH, dtype=str)
    except Exception as e:
        print(f"読み込みエラー: {e}")
        return

    NAME_COL = "商号または名称"
    CORP_COL = "法人番号"
    ADDR_COL = "登記住所"

    print("マスタデータの正規化を行っています...")
    master_df['norm_name'] = master_df[NAME_COL].apply(normalize_name)
    master_df['norm_addr'] = master_df[ADDR_COL].apply(normalize_address)
    
    print(f"DB({DB_NAME})から修正対象を取得しています...")
    with engine.connect() as conn:
        query = text("""
            SELECT id, name, address FROM companies 
            WHERE corporate_number IS NULL OR corporate_number !~ '^\\d{13}$'
        """)
        db_rows = conn.execute(query).fetchall()
    
    total_targets = len(db_rows)
    print(f"修正対象数: {total_targets} 件")
    
    if total_targets == 0:
        return

    updated_count = 0
    skipped_multiple = 0
    
    print("更新処理を開始します。しばらくお待ちください...")
    with engine.begin() as conn:
        for row in db_rows:
            db_norm_name = normalize_name(row.name)
            db_norm_addr = normalize_address(row.address)
            
            # 1. 名前で検索
            matches = master_df[master_df['norm_name'] == db_norm_name]
            
            # 2. 名前が複数ヒットした場合、住所で絞り込み
            if len(matches) > 1 and db_norm_addr:
                matches = matches[matches['norm_addr'].str.contains(db_norm_addr, na=False)]

            # 3. 最終的に1件に特定できれば更新
            if len(matches) == 1:
                new_corp_num = matches.iloc[0][CORP_COL]
                conn.execute(
                    text("UPDATE companies SET corporate_number = :val WHERE id = :id"),
                    {"val": new_corp_num, "id": row.id}
                )
                updated_count += 1
            elif len(matches) > 1:
                skipped_multiple += 1

    print("-" * 30)
    print(f"処理完了！")
    print(f"更新成功: {updated_count} 件")
    print(f"複数候補につき特定不能（スキップ）: {skipped_multiple} 件")
    print("-" * 30)

if __name__ == "__main__":
    run_update()