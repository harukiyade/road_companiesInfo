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

def clean_phone(phone):
    """電話番号からハイフン等を除去して数字のみにする"""
    if not phone or pd.isna(phone): return None
    return re.sub(r'\D', '', str(phone))

def run():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ DB接続成功")

        # 1. EDINET対応表を読み込み
        edinet_df = pd.read_csv(EDINET_LIST_FILE, encoding='utf-8-sig', skiprows=1)
        edinet_df.columns = [str(c).strip() for c in edinet_df.columns]
        col_edinet = next(c for c in edinet_df.columns if 'コード' in c)
        col_houjin = next(c for c in edinet_df.columns if '法人番号' in c)
        
        # マッピング: {EDINETコード: 法人番号}
        edinet_map = dict(zip(edinet_df[col_edinet], edinet_df[col_houjin].apply(lambda x: str(int(float(x))) if pd.notnull(x) else None)))

        # 2. 抽出データの読み込み（ここに電話番号や住所が入っている想定）
        relations_df = pd.read_csv(RAW_DATA_FILE)
        print(f"電話番号・法人番号クロスチェックモードで {len(relations_df)} 件を処理中...")

        final_data = []
        linked_count = 0

        for _, row in relations_df.iterrows():
            parent_edinet = row['parent_edinet_code']
            child_name = row['child_company_name']
            child_phone = clean_phone(row.get('child_phone')) # CSVに電話番号があれば
            doc_id = row['source_doc_id']
            
            # 親ID特定（法人番号）
            p_h_num = edinet_map.get(parent_edinet)
            cursor.execute("SELECT id FROM companies WHERE corporate_number = %s LIMIT 1", (p_h_num,))
            res_p = cursor.fetchone()
            if not res_p: continue
            p_id = str(res_p[0])

            # --- 子会社の多角的特定ロジック ---
            c_id = None
            
            # (1) 電話番号で検索（最強の救済策）
            if child_phone:
                # DB側の電話番号も数字のみにして比較するようにSQLを書く
                cursor.execute("SELECT id FROM companies WHERE regexp_replace(phone_number, '\D', '', 'g') = %s LIMIT 1", (child_phone,))
                res_phone = cursor.fetchone()
                if res_phone: c_id = str(res_phone[0])

            # (2) 電話番号でヒットしない場合、名前で検索
            if not c_id:
                cursor.execute("SELECT id FROM companies WHERE name = %s", (child_name,))
                matches = cursor.fetchall()
                if len(matches) == 1:
                    c_id = str(matches[0][0])
                elif len(matches) > 1:
                    # 同名が複数いる場合、住所などのサブ情報があればここで絞り込む
                    c_id = None 

            if c_id: linked_count += 1
            final_data.append((p_id, c_id, child_name, str(doc_id)))

        # 3. 登録
        if final_data:
            unique_data = list(set(final_data))
            insert_query = """
                INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name, source_doc_id)
                VALUES %s ON CONFLICT ON CONSTRAINT unique_parent_child_rel 
                DO UPDATE SET child_company_id = EXCLUDED.child_company_id;
            """
            execute_values(cursor, insert_query, unique_data)
            conn.commit()
            print(f"✅ 完了！リンク成功: {linked_count} 件 / テキストのみ: {len(unique_data)-linked_count} 件")

    except Exception as e:
        print(f"❌ エラー: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    run()