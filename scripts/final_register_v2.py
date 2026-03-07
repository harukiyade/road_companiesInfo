import psycopg2
import pandas as pd
import re
import os
from psycopg2.extras import execute_values

# DB接続設定
DB_CONFIG = {
    "host": "34.84.189.233",
    "database": "postgres",
    "user": "postgres",
    "password": "Legatus2000/",
    "port": "5432"
}

RAW_DATA_FILE = "relations_raw.csv"
EDINET_LIST_FILE = "/Users/harumacmini/programming/info_companyDetail/edinet/EdinetcodeDlInfo 2.csv"

def clean_company_name(name):
    """社名から法人格や空白を除去して比較用の『芯』を作る"""
    if not name: return ""
    name = str(name)
    # カッコとその中身、法人格（株式会社、有限会社等）を削除
    name = re.sub(r'[\(（].*?[\)）]', '', name)
    name = re.sub(r'株式会社|有限会社|合同会社|代表取締役|（株）|\(株\)', '', name)
    name = re.sub(r'\s+', '', name)
    return name

def run():
    conn = None
    try:
        print("DBに接続中...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ 接続成功")

        # 1. テーブル構造の最終調整（カラム不足や型エラーを防止）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS company_relations (
                id SERIAL PRIMARY KEY,
                parent_company_id TEXT, 
                child_company_id TEXT,
                child_company_name TEXT NOT NULL,
                source_doc_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(parent_company_id, child_company_id, child_company_name)
            );
            ALTER TABLE company_relations ALTER COLUMN parent_company_id TYPE TEXT;
            ALTER TABLE company_relations ALTER COLUMN child_company_id TYPE TEXT;
            ALTER TABLE company_relations DROP CONSTRAINT IF EXISTS unique_parent_child;
            -- 同名異社や同一親会社内での複数リレーションを考慮したユニーク制約
            ALTER TABLE company_relations ADD CONSTRAINT unique_parent_child_rel UNIQUE(parent_company_id, child_company_name);
        """)
        conn.commit()

        # 2. EDINET対応表の読み込み
        print("EDINET対応表を読み込み中...")
        edinet_df = pd.read_csv(EDINET_LIST_FILE, encoding='utf-8-sig', skiprows=1)
        edinet_df.columns = [str(c).strip() for c in edinet_df.columns]
        col_edinet = next(c for c in edinet_df.columns if 'ＥＤＩＮＥＴコード' in c or 'EDINETコード' in c)
        col_houjin = next(c for c in edinet_df.columns if '法人番号' in c)
        
        def clean_num(x):
            try:
                if pd.isna(x): return None
                return "{:.0f}".format(float(x)) if isinstance(x, (int, float)) else str(x).split('.')[0].strip()
            except: return None
        edinet_map = dict(zip(edinet_df[col_edinet], edinet_df[col_houjin].apply(clean_num)))

        # 3. 抽出データの読み込みと名寄せ処理
        relations_df = pd.read_csv(RAW_DATA_FILE)
        print(f"全 {len(relations_df)} 件の解析を開始します...")

        final_data = []
        stats = {"linked": 0, "text_only": 0, "skipped": 0}

        for _, row in relations_df.iterrows():
            parent_edinet = row['parent_edinet_code']
            child_name = row['child_company_name']
            doc_id = row['source_doc_id']
            
            # 親IDの特定
            parent_corp_num = edinet_map.get(parent_edinet)
            if not parent_corp_num:
                stats["skipped"] += 1
                continue
            
            cursor.execute("SELECT id FROM companies WHERE corporate_number = %s LIMIT 1", (str(parent_corp_num),))
            res_p = cursor.fetchone()
            if not res_p:
                stats["skipped"] += 1
                continue
            p_id = str(res_p[0])

            # --- 子会社の特定ロジック（慎重版） ---
            c_id = None
            
            # (A) 完全一致検索
            cursor.execute("SELECT id FROM companies WHERE name = %s", (child_name,))
            exact_matches = cursor.fetchall()
            
            if len(exact_matches) == 1:
                # 1社のみ発見：確定
                c_id = str(exact_matches[0][0])
            elif len(exact_matches) > 1:
                # 同名が複数：特定不可のため、リンクせずテキスト表示へ
                c_id = None
            else:
                # (B) あいまい検索（社名の『芯』で検索）
                core_name = clean_company_name(child_name)
                if len(core_name) >= 4: # 誤爆防止のため4文字以上
                    cursor.execute("SELECT id FROM companies WHERE name LIKE %s LIMIT 2", (f"%{core_name}%",))
                    fuzzy_matches = cursor.fetchall()
                    if len(fuzzy_matches) == 1:
                        c_id = str(fuzzy_matches[0][0])

            if c_id:
                stats["linked"] += 1
            else:
                stats["text_only"] += 1
            
            final_data.append((p_id, c_id, child_name, str(doc_id)))

        # 4. DBへの一括登録
        if final_data:
            unique_data = list(set(final_data))
            print(f"解析完了: リンク可能={stats['linked']}件 / テキストのみ={stats['text_only']}件")
            
            insert_query = """
                INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name, source_doc_id)
                VALUES %s 
                ON CONFLICT ON CONSTRAINT unique_parent_child_rel 
                DO UPDATE SET 
                    child_company_id = EXCLUDED.child_company_id,
                    source_doc_id = EXCLUDED.source_doc_id;
            """
            execute_values(cursor, insert_query, unique_data)
            conn.commit()
            print(f"✅ 登録が完了しました。")
        else:
            print("登録対象データがありません。")

    except Exception as e:
        print(f"❌ エラー: {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    run()