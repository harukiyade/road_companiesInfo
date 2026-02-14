import pandas as pd
import re
import os
import glob

# --- 定義 ---
RE_ZIP = re.compile(r'^\d{3}-?\d{4}$')
RE_DATE = re.compile(r'.*[年月].*')
RE_TEL = re.compile(r'^0\d{1,4}-?\d{1,4}-?\d{4}$')

def get_group_id(filename):
    f = filename.lower()
    if any(x in f for x in ["111", "112", "113", "114", "115", "116", "117"]): return 1
    if any(x in f for x in ["107", "109", "110"]): return 2
    if "122" in f or "20251224" in f: return 3
    if "105" in f: return 4
    if "106" in f: return 5
    if "119" in f: return 6
    return None

def anchor_fix_row(group_id, row, original_header):
    values = [str(v).strip() if pd.notna(v) else "" for v in row]
    new_row = [""] * len(original_header)
    found_zip_idx = -1
    for i, val in enumerate(values):
        if RE_ZIP.match(val):
            found_zip_idx = i
            break
    if found_zip_idx == -1: return values[:len(new_row)]
    
    target_zip_idx = 15 if group_id == 6 else (7 if group_id == 3 else 9)
    for i in range(min(target_zip_idx, found_zip_idx)): new_row[i] = values[i]
    if target_zip_idx < len(new_row): new_row[target_zip_idx] = values[found_zip_idx]

    if group_id == 3:
        next_idx = found_zip_idx + 1
        if next_idx < len(values):
            val = values[next_idx]
            if RE_DATE.match(val) or RE_TEL.match(val):
                new_row[8] = ""; remains = values[next_idx:]
            else:
                new_row[8] = val; remains = values[next_idx+1:]
        else: remains = []
        for j, val in enumerate(remains):
            if 9 + j < len(new_row): new_row[9+j] = val
    else:
        remains = values[found_zip_idx + 1:]
        for j, val in enumerate(remains):
            if target_zip_idx + 1 + j < len(new_row): new_row[target_zip_idx+1+j] = val
    return new_row

# --- 実行 ---
TARGET_ROOT = "csv_2"
OUTPUT_ROOT = "csv_final_clean_35"
os.makedirs(OUTPUT_ROOT, exist_ok=True)

# 探索優先順位（同じ番号なら fixed -> import_firstTime -> csv の順で1つ選ぶ）
priority_dirs = ["fixed", "import_firstTime", "csv", "add_20251224"]
processed_numbers = set()

# 対象とするメイン番号
target_numbers = ["105", "106", "107", "109", "110", "111", "112", "113", "114", "115", "116", "117", "119", "122"]
# 20251224系
add_files = [f"{i}_20251224" for i in range(1, 19)]
all_targets = target_numbers + add_files

files_to_process = []
for target in all_targets:
    found = False
    for d in priority_dirs:
        # パスを検索
        matches = glob.glob(os.path.join(TARGET_ROOT, "**", f"*{target}*.csv"), recursive=True)
        for m in matches:
            if d in m and target not in processed_numbers:
                files_to_process.append(m)
                processed_numbers.add(target)
                found = True
                break
        if found: break

print(f"--- 厳選された {len(files_to_process)} ファイルの処理を開始 ---")

for fp in files_to_process:
    filename = os.path.basename(fp)
    gid = get_group_id(filename)
    
    try:
        # 119.csvのエラー対策：on_bad_lines='warn' または 'skip'
        df = pd.read_csv(fp, encoding='utf-8-sig', dtype=str, on_bad_lines='skip')
        
        if gid == 3:
            new_header = list(df.columns[:9])
            new_header += ["設立", "電話番号(窓口)", "代表者郵便番号", "代表者住所", "代表者誕生日", "資本金", "上場", "直近決算年月", "直近売上", "直近利益", "説明", "概要", "仕入れ先", "取引先", "取引先銀行", "取締役", "株主", "社員数", "オフィス数", "工場数", "店舗数"]
            df.columns = (new_header + [""] * len(df.columns))[:len(df.columns)]
        
        fixed_data = [anchor_fix_row(gid, row, df.columns) for _, row in df.iterrows()]
        
        out_path = os.path.join(OUTPUT_ROOT, filename)
        pd.DataFrame(fixed_data, columns=df.columns).to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"Success: {filename}")
    except Exception as e:
        print(f"Error {filename}: {e}")

print(f"\n完了！ '{OUTPUT_ROOT}' フォルダに重複なしのファイルを出力しました。")