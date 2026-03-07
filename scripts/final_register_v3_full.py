import psycopg2
import pandas as pd
import re
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": "34.84.189.233", "database": "postgres", "user": "postgres",
    "password": "Legatus2000/", "port": "5432"
}

RAW_DATA_FILE = "relations_raw.csv"
EDINET_LIST_FILE = "/Users/harumacmini/programming/info_companyDetail/edinet/EdinetcodeDlInfo 2.csv"

def run():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ DB接続成功")

        # 1. EDINET対応表の読み込み
        edinet_df = pd.read_csv(EDINET_LIST_FILE, encoding='utf-8-sig', skiprows=1)
        edinet_df.columns = [str(c).strip() for c in edinet_df.columns]
        col_edinet = next(c for c in edinet_df.columns if 'コード' in c)
        col_houjin = next(c for c in edinet_df.columns if '法人番号' in c)
        edinet_map = dict(zip(edinet_df[col_edinet], edinet_df[col_houjin].apply(lambda x: str(int(float(x))) if pd.notnull(x) else None)))

        # 2. 抽出データの読み込み
        relations_df = pd.read_csv(RAW_DATA_FILE)
        print(f"全件登録モードで {len(relations_df)} 件を処理中...")

        final_data = []
        for _, row in relations_df.iterrows():
            parent_edinet = row['parent_edinet_code']
            child_name = row['child_company_name']
            doc_id = row['source_doc_id']
            
            # 親ID特定
            p_h_num = edinet_map.get(parent_edinet)
            cursor.execute("SELECT id FROM companies WHERE corporate_number = %s LIMIT 1", (p_h_num,))
            res_p = cursor.fetchone()
            if not res_p: continue
            p_id = str(res_p[0])

            # 子ID特定（特定できなければNoneのまま）
            cursor.execute("SELECT id FROM companies WHERE name = %s LIMIT 1", (child_name,))
            res_c = cursor.fetchone()
            c_id = str(res_c[0]) if res_c else None

            final_data.append((p_id, c_id, child_name, str(doc_id)))

        # 3. 登録（IDが空でも名前ベースで登録を強行する）
        if final_data:
            unique_data = list(set(final_data))
            print(f"解析完了: {len(unique_data)} 件をDBに流し込みます...")
            
            # ON CONFLICT の動作を確実にするため、child_company_id が NULL の場合も考慮した UPSERT
            insert_query = """
                INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name, source_doc_id)
                VALUES %s 
                ON CONFLICT (parent_company_id, child_company_name) 
                DO UPDATE SET 
                    child_company_id = EXCLUDED.child_company_id,
                    source_doc_id = EXCLUDED.source_doc_id;
            """
            execute_values(cursor, insert_query, unique_data)
            conn.commit()
            
            # 最終的な登録件数を確認
            cursor.execute("SELECT count(*) FROM company_relations")
            total_db = cursor.fetchone()[0]
            print(f"✅ 完了！DB内の総リレーション数: {total_db} 件")

    except Exception as e:
        print(f"❌ エラー: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    run()