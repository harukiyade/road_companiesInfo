import os
import csv
import re
import pandas as pd

# --- 設定：各ファイルの「正解行」の指定 ---
TARGET_CONFIG = {
    "csv_2/csv/107.csv": 2, "csv_2/csv/109.csv": 2, "csv_2/csv/110.csv": 2,
    "csv_2/csv/111.csv": 2, "csv_2/csv/112.csv": 2, "csv_2/csv/113.csv": 2,
    "csv_2/csv/114.csv": 2, "csv_2/csv/115.csv": 2, "csv_2/csv/116.csv": 2,
    "csv_2/csv/117.csv": 2, "csv_2/csv/122.csv": 2, # 122系は一旦2行目
    "csv_2/fixed/107.fixed.csv": 2, "csv_2/fixed/109.fixed.csv": 2,
    "csv_2/fixed/110.fixed.csv": 2, "csv_2/fixed/111.fixed.csv": 2,
    "csv_2/fixed/112.fixed.csv": 2, "csv_2/fixed/113.fixed.csv": 2,
    "csv_2/fixed/114.fixed.csv": 2, "csv_2/fixed/115.fixed.csv": 2,
    "csv_2/fixed/116.fixed.csv": 2, "csv_2/fixed/117.fixed.csv": 2,
    "csv_2/fixed/122.fixed.csv": 2,
    "csv_2/import_firstTime/105.csv": 2, "csv_2/import_firstTime/106.csv": 2,
    "csv_2/import_firstTime/107.csv": 2, "csv_2/import_firstTime/110.csv": 2,
    "csv_2/import_firstTime/111.csv": 2, "csv_2/import_firstTime/112.csv": 2,
    "csv_2/import_firstTime/113.csv": 2, "csv_2/import_firstTime/114.csv": 2,
    "csv_2/import_firstTime/115.csv": 2, "csv_2/import_firstTime/116.csv": 2,
    "csv_2/import_firstTime/117.csv": 2, "csv_2/import_firstTime/119.csv": 3,
    "csv_2/import_firstTime/122.csv": 2
}

OUTPUT_DIR = "修正済み_final_result"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def get_detailed_type(val):
    v = str(val).strip()
    if not v or v.lower() == 'nan': return "EMPTY"
    if re.match(r'^\d{3}-?\d{4}$', v): return "ZIP"
    if v.startswith(('http', 'www')): return "URL"
    if re.match(r'^(東京都|北海道|京都府|大阪府|.{2,3}県)', v): return "ADDR"
    if re.match(r'^0\d{1,4}-?\d{1,4}-?\d{4}$', v): return "TEL"
    if re.match(r'^\d{13}$', v): return "CORP_NUM"
    return "TEXT"

def process_file_with_its_own_header(file_path, correct_row_idx):
    if not os.path.exists(file_path): return
    
    with open(file_path, 'r', encoding='utf-8-sig', errors='replace') as f:
        reader = list(csv.reader(f))
        header = reader[0]
        # 正解行からそのファイル独自の「型パターン」を学習
        template_row = reader[correct_row_idx - 1]
        patterns = [get_detailed_type(v) for v in template_row]
        
        corrected_rows = []
        # 2行目以降（データ行）をループ
        for row in reader[1:]:
            if not any(row): continue
            
            new_row = [None] * len(header)
            # 行の中身をプールに入れ、型が合うものを優先的にスロットへ
            pool = [v.strip() for v in row if v.strip() and v.lower() != 'nan']
            
            # 1次パス：型が完全に一致するものを配置（住所、URL、郵便番号など）
            for i, p_type in enumerate(patterns):
                if p_type == "EMPTY": continue
                for j, val in enumerate(pool):
                    if get_detailed_type(val) == p_type:
                        new_row[i] = pool.pop(j)
                        break
            
            # 2次パス：余ったデータを空いているスロットに順番に流し込む（業種名など）
            for i in range(len(header)):
                if new_row[i] is None and pool:
                    new_row[i] = pool.pop(0)
            
            corrected_rows.append(new_row)

    # 保存（元のファイル名を維持）
    output_path = os.path.join(OUTPUT_DIR, os.path.basename(file_path))
    pd.DataFrame(corrected_rows, columns=header).to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Fixed: {file_path} -> {output_path}")

# --- 実行 ---
for fp, idx in TARGET_CONFIG.items():
    process_file_with_its_own_header(fp, idx)

print("\n全てのファイルの個別修正が完了しました。'修正済み_final_result' フォルダを確認してください。")