import pandas as pd
import re
import os
from itertools import permutations
from sqlalchemy import create_engine, text

# --- 設定：環境に合わせて変更してください ---
# データベース接続情報 (例: MySQLの場合)
# 'mysql+pymysql://ユーザー名:パスワード@ホスト名:ポート/データベース名'
DB_URL = os.environ.get('DATABASE_URL', 'mysql+pymysql://user:pass@localhost/your_db')

# パス設定
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, 'studio_results_20260222_0031.csv')
RESULT_DIR = os.path.join(BASE_DIR, 'sqlResultFile')

# 出力ファイルパス
OUTPUT_MASTER = os.path.join(RESULT_DIR, 'unique_companies_list.csv')
OUTPUT_RELATIONS = os.path.join(RESULT_DIR, 'company_relationships.csv')
OUTPUT_CLEANED = os.path.join(RESULT_DIR, 'cleaned_companies.csv')

# フォルダがない場合は作成
if not os.path.exists(RESULT_DIR):
    os.makedirs(RESULT_DIR)

def clean_company_name(text):
    if not isinstance(text, str): return None
    noise_patterns = [
        r'の状況について.*', r'につきましても.*', r'については.*', r'につきましては.*',
        r'を存続会社とする.*', r'を吸収合併消滅会社とする.*', r'を吸収合併.*',
        r'の100％子会社である', r'の連結子会社', r'連結子会社であった', r'当社子会社である',
        r'は債務超過.*', r'は設立により.*', r'を取得したことにより.*',
        r'（現', r'）', r'（', r'\(', r'\)', r'^当社', r'並びに', r'および', r'、'
    ]
    clean_text = text
    for pattern in noise_patterns:
        clean_text = re.sub(pattern, '', clean_text)
    
    valid_suffix = r'.*(?:株式会社|有限会社|合同会社|股份有限公司|有限公司|Inc\.|Ltd\.|Corp\.|CO\.,LTD\.)'
    match = re.search(valid_suffix, clean_text, re.IGNORECASE)
    
    if match:
        result = match.group(0).strip()
        result = re.sub(r'^[、。の]+', '', result)
        return result if len(result) > 2 else None
    return None

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"エラー: 元ファイルが見つかりません -> {INPUT_FILE}")
        return

    # 1. データ加工処理
    print(f"--- 1. データ加工中 ---")
    df = pd.read_csv(INPUT_FILE)
    
    def extract_all_from_row(text):
        segments = re.split(r'並びに|および|、', str(text))
        names = [clean_company_name(s) for s in segments]
        return [n for n in names if n]

    df['company_list'] = df['child_company_name'].apply(extract_all_from_row)

    # 企業マスター作成
    all_names = set()
    for names in df['company_list']:
        all_names.update(names)
    master_df = pd.DataFrame(sorted(list(all_names)), columns=['company_name'])
    
    # 相互リレーション作成
    relations = []
    for names in df['company_list']:
        unique_row_names = list(set(names))
        if len(unique_row_names) >= 2:
            for pair in permutations(unique_row_names, 2):
                relations.append({'Company': pair[0], 'Related_Company': pair[1]})
    relations_df = pd.DataFrame(relations).drop_duplicates()

    # ファイル保存 (sqlResultFile配下)
    master_df.to_csv(OUTPUT_MASTER, index=False, encoding='utf-8-sig')
    relations_df.to_csv(OUTPUT_RELATIONS, index=False, encoding='utf-8-sig')
    print(f"CSV出力完了: {RESULT_DIR} 内")

    # 2. データベース更新処理
    print(f"\n--- 2. データベース更新開始 ---")
    try:
        engine = create_engine(DB_URL)
        with engine.begin() as conn:
            # A. 企業マスタの更新 (companiesテーブルを想定)
            print("企業マスタ(companies)を更新中...")
            for name in master_df['company_name']:
                # すでにあればスキップ、なければ挿入のSQL
                # ※MySQLの例。DB種別により適宜調整
                conn.execute(text("""
                    INSERT IGNORE INTO companies (name, created_at, updated_at) 
                    VALUES (:name, NOW(), NOW())
                """), {"name": name})

            # IDマッピングの取得 (名前からIDを引けるようにする)
            result = conn.execute(text("SELECT id, name FROM companies"))
            name_to_id = {row.name: row.id for row in result}

            # B. 関連付け(順方向・逆方向)の更新 (company_relationsテーブルを想定)
            print("関連付け(company_relations)を更新中...")
            for _, row in relations_df.iterrows():
                id_a = name_to_id.get(row['Company'])
                id_b = name_to_id.get(row['Related_Company'])
                
                if id_a and id_b:
                    conn.execute(text("""
                        INSERT IGNORE INTO company_relations (company_id, related_company_id, created_at) 
                        VALUES (:id_a, :id_b, NOW())
                    """), {"id_a": id_a, "id_b": id_b})

        print("\n--- 全ての処理が正常に完了しました ---")
        print(f"登録企業数: {len(master_df)}件")
        print(f"登録関連数: {len(relations_df)}ペア (相互リンク含む)")

    except Exception as e:
        print(f"\n【エラー】DB更新中に問題が発生しました: {e}")

if __name__ == "__main__":
    main()