import pandas as pd
import re
import json
import os

# --- 設定 ---
INPUT_FILE = 'data/studio_results_20260222_0916.csv'
OUTPUT_SQL = 'sqlResultFile/insert_company_relations.sql'

def extract_names_precisely(raw_text):
    try:
        data = json.loads(raw_text)
        text_content = data.get('name', '')
    except:
        text_content = str(raw_text)
    
    noise_patterns = [r'^社', r'^および', r'^現', r'^等', r'^主要な', r'^連結子会社', r'^の名称']
    parts = re.split(r'[ \u3000、,;；\n\t・]', text_content)
    extracted = []
    suffix_pattern = r'(?:株式会社|有限会社|合同会社|股份有限公司|有限公司|Inc\.|Ltd\.)'
    
    for p in parts:
        matches = re.findall(r'(.+?' + suffix_pattern + r')', p)
        for m in matches:
            cleaned = m
            for np in noise_patterns:
                cleaned = re.sub(np, '', cleaned)
            cleaned = re.sub(r'^[（(1234567890)）\s]+', '', cleaned).strip()
            if len(cleaned) > 5:
                extracted.append(cleaned)
    return list(set(extracted))

def main():
    df = pd.read_csv(INPUT_FILE)
    os.makedirs(os.path.dirname(OUTPUT_SQL), exist_ok=True)

    with open(OUTPUT_SQL, 'w', encoding='utf-8') as f:
        f.write("-- 1. 中間テーブルへのデータ投入\n")
        
        for _, row in df.iterrows():
            parent_name = str(row['parent_name']).replace("'", "''")
            extracted_names = extract_names_precisely(row['抽出された名前'])
            
            for child_name in extracted_names:
                safe_child = child_name.replace("'", "''")
                # parent_id と child_id を名前から検索してINSERT
                sql = f"""
INSERT INTO company_relations (parent_company_id, child_company_id, child_company_name)
SELECT 
    p.id::text, 
    c.id::text, 
    c.name
FROM companies p, companies c
WHERE p.name = '{parent_name}' AND c.name = '{safe_child}'
ON CONFLICT DO NOTHING;
"""
                f.write(sql)

    print(f"中間テーブル投入用SQLを生成しました: {OUTPUT_SQL}")

if __name__ == "__main__":
    main()