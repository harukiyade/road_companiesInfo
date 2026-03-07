import psycopg2
import pandas as pd
import os
from psycopg2.extras import execute_values

DB_CONFIG = {
    "host": "34.84.189.233",
    "database": "postgres",
    "user": "postgres",
    "password": "Legatus2000/",
    "port": "5432"
}

RAW_DATA_FILE = "relations_raw.csv"
EDINET_LIST_FILE = "/Users/harumacmini/programming/info_companyDetail/edinet/EdinetcodeDlInfo 2.csv"

def run():
    conn = None
    try:
        print("DBに接続中...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ 接続成功")

        # 1. テーブル構造の最適化（child_company_nameカラムを許容するように調整）
        print("テーブル構造を最終調整中...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_relations (
                id SERIAL PRIMARY KEY,
                parent_company_id TEXT, 
                child_company_id TEXT,
                child_company_name TEXT, -- このカラムが必須制約を持っているための対応
                source_doc_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            ALTER TABLE company_relations ALTER COLUMN parent_company_id TYPE TEXT;
            ALTER TABLE company_relations ALTER COLUMN child_company_id TYPE TEXT;
            
            -- 重複制約の再構築
            ALTER TABLE company_relations DROP CONSTRAINT IF EXISTS unique_parent_child;
            ALTER TABLE company_relations ADD CONSTRAINT unique_parent_child UNIQUE(parent_company_id, child_company_id);
        """)
        conn.commit()

        # 2. EDINET対応表を読み込み
        print(f"EDINET対応表を読み込み中...")
        edinet_df = pd.read_csv(EDINET_LIST_FILE, encoding='utf-8-sig', skiprows=1)
        edinet_df.columns = [str(c).strip() for c in edinet_df.columns]
        
        col_edinet_code = next(c for c in edinet_df.columns if 'ＥＤＩＮＥＴコード' in c or 'EDINETコード' in c)
        col_houjin_num = next(c for c in edinet_df.columns if '法人番号' in c)
        
        def clean_num(x):
            try:
                if pd.isna(x) or str(x).strip() == "": return None
                return "{:.0f}".format(float(x)) if isinstance(x, (int, float)) else str(x).split('.')[0].strip()
            except: return None

        edinet_map = dict(zip(edinet_df[col_edinet_code], edinet_df[col_houjin_num].apply(clean_num)))

        # 3. 抽出データの読み込みと名寄せ
        relations_df = pd.read_csv(RAW_DATA_FILE)
        print(f"抽出データ {len(relations_df)} 件を照合開始...")

        final_data = []
        for _, row in relations_df.iterrows():
            parent_edinet = row['parent_edinet_code']
            child_name = row['child_company_name']
            doc_id = row['source_doc_id']
            
            parent_corp_num = edinet_map.get(parent_edinet)
            if not parent_corp_num: continue

            # 親ID取得
            cursor.execute("SELECT id FROM companies WHERE corporate_number = %s LIMIT 1", (str(parent_corp_num),))
            res_p = cursor.fetchone()
            if not res_p: continue
            p_id = str(res_p[0])

            # 子ID取得
            cursor.execute("SELECT id FROM companies WHERE name = %s LIMIT 1", (child_name,))
            res_c = cursor.fetchone()
            if res_c:
                c_id = str(res_c[0])
                # (親ID, 子ID, 子会社名, docID) の順でデータをセット
                final_data.append((p_id, c_id, child_name, str(doc_id)))

        # 4. インサート実行
        if final_data:
            unique_data = list(set(final_data))
            print(f"照合成功: {len(unique_data)} 件。DBに登録します...")
            
            # カラムリストに child_company_name を追加
            insert_query = """
                INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name, source_doc_id)
                VALUES %s 
                ON CONFLICT ON CONSTRAINT unique_parent_child DO NOTHING
            """
            execute_values(cursor, insert_query, unique_data)
            conn.commit()
            print(f"✅ ついに完了しました！ {len(unique_data)} 件のリレーションがDBに刻まれました。")
        else:
            print("登録可能なリレーションが見つかりませんでした。")

    except Exception as e:
        print(f"❌ エラー: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run()