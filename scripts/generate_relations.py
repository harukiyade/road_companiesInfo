import pandas as pd
import re
import os
from itertools import permutations

# --- パス設定 (実行環境に合わせて調整) ---
# スクリプトの場所に関わらず、プロジェクトルートにあるCSVを探す設定
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_FILE = os.path.join(BASE_DIR, 'studio_results_20260222_0031.csv')
OUTPUT_MASTER = os.path.join(BASE_DIR, 'unique_companies_master.csv')
OUTPUT_RELATIONS = os.path.join(BASE_DIR, 'company_relations_bridge.csv')

def clean_company_name(text):
    if not isinstance(text, str): return None
    
    # 除去パターン
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
    
    # 会社名の末尾として有効なもの
    valid_suffix = r'.*(?:株式会社|有限会社|合同会社|股份有限公司|有限公司|Inc\.|Ltd\.|Corp\.|CO\.,LTD\.)'
    match = re.search(valid_suffix, clean_text, re.IGNORECASE)
    
    if match:
        result = match.group(0).strip()
        # 文頭のゴミ取り
        result = re.sub(r'^[、。の]+', '', result)
        return result if len(result) > 2 else None
    return None

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"エラー: ファイルが見つかりません -> {INPUT_FILE}")
        return

    # データ読み込み
    df = pd.read_csv(INPUT_FILE)
    print(f"読み込み成功: {INPUT_FILE}")
    
    # 1. 各行から会社名を抽出
    def extract_all_from_row(text):
        segments = re.split(r'並びに|および|、', text)
        names = [clean_company_name(s) for s in segments]
        return [n for n in names if n]

    df['company_list'] = df['child_company_name'].apply(extract_all_from_row)

    # 2. 企業マスターの作成
    all_names = set()
    for names in df['company_list']:
        all_names.update(names)
    
    # 全量出力（マスタ）
    master_df = pd.DataFrame(sorted(list(all_names)), columns=['company_name'])
    master_df.to_csv(OUTPUT_MASTER, index=False, encoding='utf-8-sig')
    
    # 3. 相互リレーションの生成 (A↔B, B↔C...)
    relations = []
    
    # 同一行内の企業間で相互ペアを作成
    for names in df['company_list']:
        unique_row_names = list(set(names))
        if len(unique_row_names) >= 2:
            for pair in permutations(unique_row_names, 2):
                relations.append({'company_a': pair[0], 'company_b': pair[1]})

    # 全量出力（関連）
    relations_df = pd.DataFrame(relations).drop_duplicates()
    relations_df.to_csv(OUTPUT_RELATIONS, index=False, encoding='utf-8-sig')

    print("\n--- 出力完了 ---")
    print(f"1. 企業マスタ全量: {OUTPUT_MASTER} ({len(master_df)}件)")
    print(f"2. 相互関連全量  : {OUTPUT_RELATIONS} ({len(relations_df)}ペア)")

if __name__ == "__main__":
    main()