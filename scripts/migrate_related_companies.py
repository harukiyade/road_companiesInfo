import pandas as pd
import re
import json
from sqlalchemy import create_engine, text

# --- 設定 ---
DB_URL = 'postgresql://user:pass@localhost/your_db'
INPUT_FILE = 'data/studio_results_20260222_0916.csv'

def extract_names_from_json(raw_text):
    try:
        data = json.loads(raw_text)
        text_content = data.get('name', '')
    except:
        text_content = str(raw_text)
    
    # 文章から会社名（株式会社XXXなど）を抽出するパターン
    patterns = r'[々〇〻\u3400-\u9FFF\u3040-\u309F\u30A0-\u30FF]+(?:株式会社|有限会社|合同会社|股份有限公司|有限公司|Inc\.|Ltd\.)'
    return list(set(re.findall(patterns, text_content)))

def main():
    engine = create_engine(DB_URL)
    df = pd.read_csv(INPUT_FILE)
    
    with engine.begin() as conn:
        # 全企業名とIDのマップをメモリに持つ（高速化のため）
        result = conn.execute(text("SELECT id, name FROM companies"))
        name_to_id = {row.name: row.id for row in result}

        for _, row in df.iterrows():
            parent_name = row['parent_name']
            # 文章から社名を抽出
            extracted_names = extract_names_from_json(row['抽出された名前'])
            
            # 抽出した社名をIDに変換
            child_ids = []
            for name in extracted_names:
                if name in name_to_id:
                    child_ids.append(name_to_id[name])
            
            # IDが見つかった場合のみ、DBの新カラムを更新
            if child_ids:
                conn.execute(text("""
                    UPDATE companies 
                    SET related_company_ids = :ids 
                    WHERE name = :p_name
                """), {"ids": json.dumps(child_ids), "p_name": parent_name})

    print("整理と新カラムの更新が完了しました。")

if __name__ == "__main__":
    main()