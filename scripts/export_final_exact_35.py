import pandas as pd
import re
import os

# --- 共通ロジック ---
RE_ZIP = re.compile(r'^\d{3}-?\d{4}$')
RE_DATE = re.compile(r'.*[年月].*')
RE_TEL = re.compile(r'^0\d{1,4}-?\d{1,4}-?\d{4}$')

def get_group_id(filename):
    f = filename.lower()
    if any(x in f for x in ["111", "112", "113", "114", "115", "116", "117"]): return 1
    if any(x in f for x in ["107", "109", "110"]): return 2
    if "122" in f: return 3
    if "105" in f: return 4
    if "106" in f: return 5
    if "119" in f: return 6
    return None

def anchor_fix_row(group_id, row, original_header):
    values = [str(v).strip() if pd.notna(v) else "" for v in row]
    new_row = [""] * len(original_header)
    
    # 1. 郵便番号探索
    found_zip_idx = -1
    for i, val in enumerate(values):
        if RE_ZIP.match(val):
            found_zip_idx = i
            break
    
    if found_zip_idx == -1: return values[:len(new_row)]
    
    # 2. ターゲット位置設定
    target_zip_idx = 15 if group_id == 6 else (7 if group_id == 3 else 9)
    
    # 3. 前半部分コピー
    for i in range(min(target_zip_idx, found_zip_idx)):
        new_row[i] = values[i]
    
    # 4. 郵便番号固定
    if target_zip_idx < len(new_row):
        new_row[target_zip_idx] = values[found_zip_idx]

    # 5. 後半部分の精密配置
    if group_id == 3:
        next_idx = found_zip_idx + 1
        if next_idx < len(values):
            val = values[next_idx]
            # 住所が空（日付や電話番号が来ている）なら住所列を飛ばす
            if RE_DATE.match(val) or RE_TEL.match(val):
                new_row[8] = ""
                remains = values[next_idx:]
            else:
                new_row[8] = val
                remains = values[next_idx+1:]
        else:
            remains = []
        for j, val in enumerate(remains):
            if 9 + j < len(new_row): new_row[9+j] = val
    else:
        remains = values[found_zip_idx + 1:]
        for j, val in enumerate(remains):
            if target_zip_idx + 1 + j < len(new_row):
                new_row[target_zip_idx + 1 + j] = val
    return new_row

# --- 対象ファイルリスト (35ファイル) ---
TARGET_FILES = [
    "csv_2/csv/107.csv", "csv_2/csv/109.csv", "csv_2/csv/110.csv", "csv_2/csv/111.csv",
    "csv_2/csv/112.csv", "csv_2/csv/113.csv", "csv_2/csv/114.csv", "csv_2/csv/115.csv",
    "csv_2/csv/116.csv", "csv_2/csv/117.csv", "csv_2/csv/122.csv",
    "csv_2/fixed/107.fixed.csv", "csv_2/fixed/109.fixed.csv", "csv_2/fixed/110.fixed.csv",
    "csv_2/fixed/111.fixed.csv", "csv_2/fixed/112.fixed.csv", "csv_2/fixed/113.fixed.csv",
    "csv_2/fixed/114.fixed.csv", "csv_2/fixed/115.fixed.csv", "csv_2/fixed/116.fixed.csv",
    "csv_2/fixed/117.fixed.csv", "csv_2/fixed/122.fixed.csv",
    "csv_2/import_firstTime/105.csv", "csv_2/import_firstTime/106.csv", "csv_2/import_firstTime/107.csv",
    "csv_2/import_firstTime/110.csv", "csv_2/import_firstTime/111.csv", "csv_2/import_firstTime/112.csv",
    "csv_2/import_firstTime/113.csv", "csv_2/import_firstTime/114.csv", "csv_2/import_firstTime/115.csv",
    "csv_2/import_firstTime/116.csv", "csv_2/import_firstTime/117.csv", "csv_2/import_firstTime/119.csv",
    "csv_2/import_firstTime/122.csv"
]

OUTPUT_ROOT = "csv_final_exact_35"
os.makedirs(OUTPUT_ROOT, exist_ok=True)

print(f"--- 指定35ファイルの処理を開始 ---")

count = 0
for fp in TARGET_FILES:
    if not os.path.exists(fp):
        print(f"Missing file: {fp}")
        continue
    
    filename = os.path.basename(fp)
    # 保存時に重複を避けるためフォルダ名も名前に含める
    dir_hint = fp.split('/')[-2]
    out_name = f"{dir_hint}_{filename}"
    
    gid = get_group_id(filename)
    
    try:
        df = pd.read_csv(fp, encoding='utf-8-sig', dtype=str, on_bad_lines='skip')
        
        # グループ3のヘッダー刷新
        if gid == 3:
            new_header = list(df.columns[:9])
            new_header += ["設立", "電話番号(窓口)", "代表者郵便番号", "代表者住所", "代表者誕生日", "資本金", "上場", "直近決算年月", "直近売上", "直近利益", "説明", "概要", "仕入れ先", "取引先", "取引先銀行", "取締役", "株主", "社員数", "オフィス数", "工場数", "店舗数"]
            df.columns = (new_header + [""] * len(df.columns))[:len(df.columns)]
        
        fixed_data = [anchor_fix_row(gid, row, df.columns) for _, row in df.iterrows()]
        
        out_path = os.path.join(OUTPUT_ROOT, out_name)
        pd.DataFrame(fixed_data, columns=df.columns).to_csv(out_path, index=False, encoding='utf-8-sig')
        count += 1
        print(f"[{count}/35] Processed: {fp}")
    except Exception as e:
        print(f"Error {fp}: {e}")

print(f"\n完了！ '{OUTPUT_ROOT}' フォルダを確認してください。")