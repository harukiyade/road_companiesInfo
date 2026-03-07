import pandas as pd
import os
from sqlalchemy import create_engine, text

# --- 【重要】ここを実際のDB設定に合わせて書き換えてください ---
# 例: 'mysql+pymysql://root:password@127.0.0.1:3306/db_name'
DB_URL = 'mysql+pymysql://user:pass@localhost/your_db' 

# パス設定
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULT_DIR = os.path.join(BASE_DIR, 'sqlResultFile')

# ファイル指定
MASTER_FILE = os.path.join(RESULT_DIR, 'unique_companies_list.csv')
RELATION_FILE = os.path.join(RESULT_DIR, 'company_relationships.csv')

def main():
    if not os.path.exists(MASTER_FILE) or not os.path.exists(RELATION_FILE):
        print(f"エラー: sqlResultFile 内にファイルがありません。パスを確認してください: {RESULT_DIR}")
        return

    print("--- データベース更新処理を開始します ---")
    try:
        master_df = pd.read_csv(MASTER_FILE)
        relations_df = pd.read_csv(RELATION_FILE)
        
        engine = create_engine(DB_URL)
        with engine.begin() as conn:
            # 1. 企業マスターの登録
            print(f"企業マスター登録中 ({len(master_df)}件)...")
            for _, row in master_df.iterrows():
                conn.execute(text("""
                    INSERT IGNORE INTO companies (name, created_at, updated_at) 
                    VALUES (:name, NOW(), NOW())
                """), {"name": row['company_name']})

            # 2. IDの取得
            result = conn.execute(text("SELECT id, name FROM companies"))
            name_to_id = {row.name: row.id for row in result}

            # 3. 関連企業（相互）の登録
            print(f"関連付け登録中 ({len(relations_df)}ペア)...")
            for _, row in relations_df.iterrows():
                id_a = name_to_id.get(row['Company'])
                id_b = name_to_id.get(row['Related_Company'])
                if id_a and id_b:
                    conn.execute(text("""
                        INSERT IGNORE INTO company_relations (company_id, related_company_id, created_at) 
                        VALUES (:id_a, :id_b, NOW())
                    """), {"id_a": id_a, "id_b": id_b})

        print("\n--- 完了しました ---")
    except Exception as e:
        print(f"\n【接続エラー】DBに接続できませんでした。DBが起動しているか、DB_URLの設定を確認してください。\n詳細: {e}")

if __name__ == "__main__":
    main()