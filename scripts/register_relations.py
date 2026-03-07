import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# 設定の読み込み
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
RAW_DATA_FILE = "relations_raw.csv"
EDINET_LIST_FILE = "/Users/harumacmini/programming/info_companyDetail/edinet/EdinetcodeDlInfo 2.csv"

def find_column(columns, keywords):
    """キーワードのいずれかを含む列名を探す"""
    for col in columns:
        for key in keywords:
            if key in str(col):
                return col
    return None

def load_edinet_mapping():
    """EDINETコードと法人番号の対応表を読み込む（列名あいまい検索版）"""
    if not os.path.exists(EDINET_LIST_FILE):
        print(f"エラー: {EDINET_LIST_FILE} が見つかりません。")
        return {}

    print(f"[{EDINET_LIST_FILE}] を読み込み中...")
    
    encodings = ['utf-8-sig', 'cp932', 'utf-8']
    df = None
    
    for enc in encodings:
        try:
            # 最初の数行を読み込んでヘッダー行を探す
            preview = pd.read_csv(EDINET_LIST_FILE, encoding=enc, header=None, nrows=20)
            header_row_index = -1
            for i, row in preview.iterrows():
                row_str = "".join(row.astype(str).values)
                if 'ＥＤＩＮＥＴコード' in row_str or 'EDINETコード' in row_str:
                    header_row_index = i
                    break
            
            if header_row_index != -1:
                df = pd.read_csv(EDINET_LIST_FILE, encoding=enc, skiprows=header_row_index)
                break
        except:
            continue
            
    if df is None:
        print("エラー: CSVの読み込みに失敗しました。")
        return {}

    try:
        # 列名をクリーニング
        cols = [str(c).strip() for c in df.columns]
        df.columns = cols
        
        # あいまい検索で列を特定
        col_edinet = find_column(cols, ['ＥＤＩＮＥＴコード', 'EDINETコード', 'コード'])
        col_houjin = find_column(cols, ['法人番号', 'Corporate Number', 'CorporateNumber'])
        
        if not col_edinet or not col_houjin:
            print(f"エラー: 必要な列が見つかりません。")
            print(f"発見された列名一覧: {cols}")
            return {}

        print(f"列特定成功: EDINETコード='{col_edinet}', 法人番号='{col_houjin}'")
        
        mapping_df = df[[col_edinet, col_houjin]].dropna()
        
        # 法人番号を13桁の文字列に変換
        def clean_corp_num(val):
            try:
                # 数値や浮動小数点、文字列が混ざっていても13桁の整数文字列にする
                s = str(val).split('.')[0].replace(' ', '').replace('-', '')
                if len(s) >= 12: # 法人番号は通常13桁
                    return s
                return None
            except:
                return None

        mapping_df['houjin_clean'] = mapping_df[col_houjin].apply(clean_corp_num)
        mapping_df = mapping_df.dropna(subset=['houjin_clean'])
        
        print(f"対応表の展開完了: {len(mapping_df)} 件")
        return dict(zip(mapping_df[col_edinet], mapping_df['houjin_clean']))
    except Exception as e:
        print(f"データ加工中にエラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return {}

def connect_db():
    return psycopg2.connect(DB_URL)

def run():
    if not os.path.exists(RAW_DATA_FILE):
        print(f"エラー: {RAW_DATA_FILE} が見つかりません。")
        return
    
    edinet_map = load_edinet_mapping()
    if not edinet_map:
        return

    relations_df = pd.read_csv(RAW_DATA_FILE)
    relations_df['parent_corporate_number'] = relations_df['parent_edinet_code'].map(edinet_map)
    
    conn = connect_db()
    cursor = conn.cursor()
    
    print(f"DB照合を開始します（対象: {len(relations_df)}件）...")
    
    final_data = []
    success_count = 0
    
    for _, row in relations_df.iterrows():
        parent_corp_num = row['parent_corporate_number']
        child_name = row['child_company_name']
        doc_id = row['source_doc_id']
        
        if pd.isna(parent_corp_num):
            continue

        # 親会社のIDを取得
        cursor.execute("SELECT id FROM companies WHERE corporate_number = %s LIMIT 1", (str(parent_corp_num),))
        res_p = cursor.fetchone()
        if not res_p:
            continue
        parent_id = res_p[0]

        # 子会社のIDを名称で検索
        cursor.execute("SELECT id FROM companies WHERE name = %s LIMIT 1", (child_name,))
        res_c = cursor.fetchone()
        
        if res_c:
            child_id = res_c[0]
            final_data.append((parent_id, child_id, doc_id))
            success_count += 1
            if success_count % 100 == 0:
                print(f"名寄せ成功: {success_count}件経過...")

    if final_data:
        unique_final_data = list(set(final_data))
        print(f"\n照合完了。DBに存在する {len(unique_final_data)} 件のリレーションを登録します...")
        
        insert_query = """
            INSERT INTO company_relations (parent_company_id, child_company_id, source_doc_id)
            VALUES %s
            ON CONFLICT (parent_company_id, child_company_id) DO NOTHING
        """
        execute_values(cursor, insert_query, unique_final_data)
        conn.commit()
        print("✅ DBへの一括登録が正常に完了しました！")
    else:
        print("DBに登録可能な一致データ（子会社名がDB内の企業名と完全一致するもの）が見つかりませんでした。")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print(f"実行中にエラーが発生しました: {e}")