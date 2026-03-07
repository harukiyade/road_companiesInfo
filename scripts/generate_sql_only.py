import pandas as pd
import os

# --- 【設定】DBの実際のカラム名に完全に準拠 ---
TABLE_COMPANIES = 'companies'
COLUMN_NAME = 'name'

TABLE_RELATIONS = 'company_relations'
COLUMN_PARENT = 'parent_company_id'      # 修正済み
COLUMN_CHILD = 'child_company_id'        # 修正済み
COLUMN_CHILD_NAME = 'child_company_name'  # 追加
# ----------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULT_DIR = os.path.join(BASE_DIR, 'sqlResultFile')
OUTPUT_SQL = os.path.join(RESULT_DIR, 'import_data.sql')

def main():
    if not os.path.exists(os.path.join(RESULT_DIR, 'unique_companies_list.csv')):
        print("エラー: CSVファイルが見つかりません。")
        return

    master_df = pd.read_csv(os.path.join(RESULT_DIR, 'unique_companies_list.csv'))
    relations_df = pd.read_csv(os.path.join(RESULT_DIR, 'company_relationships.csv'))

    with open(OUTPUT_SQL, 'w', encoding='utf-8') as f:
        f.write("-- PostgreSQL用 最終データ移行スクリプト (確定カラム名版)\n\n")
        
        f.write(f"-- 1. {TABLE_COMPANIES} 登録\n")
        for name in master_df['company_name']:
            safe_name = str(name).replace("'", "''")
            f.write(f"INSERT INTO {TABLE_COMPANIES} ({COLUMN_NAME}, created_at, updated_at) "
                    f"SELECT '{safe_name}', NOW(), NOW() "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {TABLE_COMPANIES} WHERE {COLUMN_NAME} = '{safe_name}');\n")

        f.write(f"\n-- 2. {TABLE_RELATIONS} 相互登録\n")
        for _, row in relations_df.iterrows():
            name_a = str(row['Company']).replace("'", "''")
            name_b = str(row['Related_Company']).replace("'", "''")
            
            # parent_company_id, child_company_id に IDを文字列としてキャストして挿入
            # child_company_name もセット
            f.write(f"INSERT INTO {TABLE_RELATIONS} ({COLUMN_PARENT}, {COLUMN_CHILD}, {COLUMN_CHILD_NAME}, created_at) "
                    f"SELECT CAST(c1.id AS TEXT), CAST(c2.id AS TEXT), c2.name, NOW() "
                    f"FROM {TABLE_COMPANIES} c1, {TABLE_COMPANIES} c2 "
                    f"WHERE c1.{COLUMN_NAME} = '{name_a}' AND c2.{COLUMN_NAME} = '{name_b}' "
                    f"AND NOT EXISTS ( "
                    f"  SELECT 1 FROM {TABLE_RELATIONS} "
                    f"  WHERE {COLUMN_PARENT} = CAST(c1.id AS TEXT) AND {COLUMN_CHILD} = CAST(c2.id AS TEXT) "
                    f");\n")
    
    print(f"最終版SQLファイルを生成しました: {OUTPUT_SQL}")
    print(f"反映カラム: {COLUMN_PARENT}, {COLUMN_CHILD}, {COLUMN_CHILD_NAME}")

if __name__ == "__main__":
    main()