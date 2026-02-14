import pandas as pd
import re
import os
import glob

# --- 高精度・補正ロジック ---
RE_ZIP = re.compile(r'^\d{3}-?\d{4}$')

def get_group_id(filename):
    f = filename.lower()
    if any(x in f for x in ["111", "112", "113", "114", "115", "116", "117"]): return 1
    if any(x in f for x in ["107", "109", "110"]): return 2
    if "122" in f or "20251224" in f: return 3 # 20251224系もグループ3として扱う
    if "105" in f: return 4
    if "106" in f: return 5
    if "119" in f: return 6
    return None

def get_target_zip_idx(group_id):
    """各グループの『あるべき』郵便番号の列番号(0開始)"""
    mapping = {1: 9, 2: 9, 3: 7, 4: 2, 5: 2, 6: 15}
    return mapping.get(group_id)

def intelligent_fix(group_id, row):
    """
    指定された列の前後4列をスキャンし、郵便番号を見つけたら
    そこを起点にデータをスライドさせて修正する
    """
    values = list(row)
    target_idx = get_target_zip_idx(group_id)
    if target_idx is None: return values

    # 郵便番号を探索（全列スキャンに拡張して精度向上）
    found_zip_idx = -1
    for i, val in enumerate(values):
        if RE_ZIP.match(str(val).strip()):
            found_zip_idx = i
            break
            
    # 郵便番号が見つかり、かつ本来の位置とズレている場合のみ補正
    if found_zip_idx != -1 and found_zip_idx != target_idx:
        # 郵便番号より前のデータは維持しつつ、郵便番号以降をスライド
        prefix = values[:target_idx]
        suffix = values[found_zip_idx:]
        new_row = prefix + suffix
        # 長さ調整
        if len(new_row) > len(values):
            new_row = new_row[:len(values)]
        else:
            new_row += [''] * (len(values) - len(new_row))
        return new_row
    
    return values

# --- 実行処理 ---
TARGET_ROOT = "csv_2"
OUTPUT_ROOT = "csv_2_fixed_output"
os.makedirs(OUTPUT_ROOT, exist_ok=True)

target_numbers = ["105", "106", "107", "109", "110", "111", "112", "113", "114", "115", "116", "117", "119", "122"]
files = glob.glob(os.path.join(TARGET_ROOT, "**/*.csv"), recursive=True)

print("--- 修正・エクスポート開始 ---")
for fp in files:
    filename = os.path.basename(fp)
    if not any(num in filename for num in target_numbers): continue
    gid = get_group_id(filename)
    if gid is None: continue

    try:
        # 大容量ファイルに対応するためchunksizeを検討しても良いが、まずは通常読み込み
        df = pd.read_csv(fp, encoding='utf-8-sig', dtype=str)
        header = df.columns
        
        fixed_data = [intelligent_fix(gid, row) for _, row in df.iterrows()]
        
        out_path = os.path.join(OUTPUT_ROOT, filename)
        pd.DataFrame(fixed_data, columns=header).to_csv(out_path, index=False, encoding='utf-8-sig')
        print(f"Done: {filename} (Group {gid})")
    except Exception as e:
        print(f"Error {filename}: {e}")

print(f"\n再生成完了: {OUTPUT_ROOT} を確認してください。")