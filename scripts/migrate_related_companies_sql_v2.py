import pandas as pd
import re
import json
import os

# --- 設定 ---
INPUT_FILE = 'data/studio_results_20260222_0916.csv'
OUTPUT_SQL = 'sqlResultFile/update_related_company_ids_v2.sql'

def extract_names_precisely(raw_text):
    try:
        data = json.loads(raw_text)
        text_content = data.get('name', '')
    except:
        text_content = str(raw_text)
    
    # 1. まず「および」「現」「等」「主要な連結子会社」などの不要な言葉を削除・分割のヒントにする
    noise_patterns = [
        r'^社', r'^および', r'^現', r'^等', r'^主要な', r'^連結子会社', r'^の名称', 
        r'^持分法', r'^を適用', r'^した', r'^関係会社', r'^非連結子会社', r'^関連会社', r'^名称等'
    ]
    
    # 区切り文字（空白、句読点）で分割して処理
    parts = re.split(r'[ \u3000、,;；\n\t・]', text_content)
    extracted = []
    
    # 社名の末尾パターン
    suffix_pattern = r'(?:株式会社|有限会社|合同会社|股份有限公司|有限公司|Inc\.|Ltd\.)'
    
    for p in parts:
        # 各断片から社名パターンを探す（非貪欲マッチで複数抽出を試みる）
        matches = re.findall(r'(.+?' + suffix_pattern + r')', p)
        for m in matches:
            cleaned = m
            # 文頭のゴミを掃除
            for np in noise_patterns:
                cleaned = re.sub(np, '', cleaned)
            # 記号の掃除
            cleaned = re.sub(r'^[（(1234567890)）\s]+', '', cleaned)
            cleaned = cleaned.strip()
            
            # 「株式会社」だけのゴミを除外
            if len(cleaned) > 5:
                extracted.append(cleaned)
                
    return list(set(extracted))

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"エラー: {INPUT_FILE} が見つかりません。")
        return

    df = pd.read_csv(INPUT_FILE)
    os.makedirs(os.path.dirname(OUTPUT_SQL), exist_ok=True)

    with open(OUTPUT_SQL, 'w', encoding='utf-8') as f:
        f.write("-- 関連企業ID集約用SQL (高精度クレンジング版)\n\n")
        
        # 親企業名ごとにグループ化して、1つのUPDATE文にまとめる
        grouped = df.groupby('parent_name')
        
        for parent_name, group in grouped:
            all_extracted = []
            for _, row in group.iterrows():
                all_extracted.extend(extract_names_precisely(row['抽出された名前']))
            
            all_extracted = list(set(all_extracted))
            
            if all_extracted:
                safe_parent = str(parent_name).replace("'", "''")
                names_list = "', '".join([n.replace("'", "''") for n in all_extracted])
                
                sql = f"""
UPDATE companies 
SET related_company_ids = (
    SELECT json_agg(id) FROM companies WHERE name IN ('{names_list}')
)
WHERE name = '{safe_parent}';
"""
                f.write(sql)

    print(f"改善版SQLファイルの生成が完了しました: {OUTPUT_SQL}")

if __name__ == "__main__":
    main()