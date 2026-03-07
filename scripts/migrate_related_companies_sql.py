import pandas as pd
import re
import json
import os

# --- 設定 ---
INPUT_FILE = 'data/studio_results_20260222_0916.csv'
OUTPUT_SQL = 'sqlResultFile/update_related_company_ids.sql'

def extract_names_from_json(raw_text):
    try:
        data = json.loads(raw_text)
        text_content = data.get('name', '')
    except:
        text_content = str(raw_text)
    
    # 文章から会社名（株式会社XXXなど）を抽出
    patterns = r'[々〇〻\u3400-\u9FFF\u3040-\u309F\u30A0-\u30FF]+(?:株式会社|有限会社|合同会社|股份有限公司|有限公司|Inc\.|Ltd\.)'
    return list(set(re.findall(patterns, text_content)))

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"エラー: {INPUT_FILE} が見つかりません。")
        return

    df = pd.read_csv(INPUT_FILE)
    
    # 出力先フォルダの作成
    os.makedirs(os.path.dirname(OUTPUT_SQL), exist_ok=True)

    with open(OUTPUT_SQL, 'w', encoding='utf-8') as f:
        f.write("-- 関連企業ID集約用SQL\n\n")
        
        for _, row in df.iterrows():
            parent_name = str(row['parent_name']).replace("'", "''")
            extracted_names = extract_names_from_json(row['抽出された名前'])
            
            if extracted_names:
                # 抽出した名前リストを元に、IDを取得してJSON配列としてUPDATEするSQL
                # サブクエリを使用して、名前からIDを引いてJSON配列に変換します
                names_tuple = str(tuple(extracted_names)).replace(",)", ")") if len(extracted_names) > 1 else f"('{extracted_names[0]}')"
                
                sql = f"""
UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN {names_tuple}
)
WHERE name = '{parent_name}';
"""
                f.write(sql)

    print(f"SQLファイルの生成が完了しました: {OUTPUT_SQL}")

if __name__ == "__main__":
    main()