import os
import csv
import re
import pandas as pd

# 指定された32カラムのヘッダー定義
TARGET_HEADERS = [
    "会社名", "都道府県", "代表者名", "法人番号", "URL", "業種1", "業種2", "業種3", "業種（細）", 
    "郵便番号", "住所", "設立", "電話番号(窓口)", "代表者郵便番号", "代表者住所", "代表者誕生日", 
    "資本金", "上場", "直近決算年月", "直近売上", "直近利益", "説明", "概要", "仕入れ先", 
    "取引先", "取引先銀行", "取締役", "株主", "社員数", "オフィス数", "工場数", "店舗数"
]

# 対象の36ファイルリスト
TARGET_FILES = [
    "csv_2/csv/107.csv", "csv_2/csv/109.csv", "csv_2/csv/110.csv", "csv_2/csv/111.csv",
    "csv_2/csv/112.csv", "csv_2/csv/113.csv", "csv_2/csv/114.csv", "csv_2/csv/115.csv",
    "csv_2/csv/116.csv", "csv_2/csv/117.csv", "csv_2/csv/122.csv",
    "csv_2/fixed/107.fixed.csv", "csv_2/fixed/109.fixed.csv", "csv_2/fixed/110.fixed.csv",
    "csv_2/fixed/111.fixed.csv", "csv_2/fixed/112.fixed.csv", "csv_2/fixed/113.fixed.csv",
    "csv_2/fixed/114.fixed.csv", "csv_2/fixed/115.fixed.csv", "csv_2/fixed/116.fixed.csv",
    "csv_2/fixed/117.fixed.csv", "csv_2/fixed/122.fixed.csv",
    "csv_2/import_firstTime/105.csv", "csv_2/import_firstTime/106.csv",
    "csv_2/import_firstTime/107.csv", "csv_2/import_firstTime/110.csv",
    "csv_2/import_firstTime/111.csv", "csv_2/import_firstTime/112.csv",
    "csv_2/import_firstTime/113.csv", "csv_2/import_firstTime/114.csv",
    "csv_2/import_firstTime/115.csv", "csv_2/import_firstTime/116.csv",
    "csv_2/import_firstTime/117.csv", "csv_2/import_firstTime/119.csv",
    "csv_2/import_firstTime/122.csv", "csv_2/import_firstTime/old128.csv"
]

OUTPUT_DIR = "修正済み"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 正規表現パターン
re_zip = re.compile(r'^\d{3}-?\d{4}$')
re_corp = re.compile(r'^\d{13}$')
re_url = re.compile(r'^https?://')
re_pref = re.compile(r'^(東京都|北海道|京都府|大阪府|.{2,3}県)')
re_tel = re.compile(r'^0\d{1,4}-?\d{1,4}-?\d{4}$')

def heuristic_reorder(row_values):
    """
    行の各セルの値を見て、適切な列に配置し直す
    """
    new_row = [None] * len(TARGET_HEADERS)
    unassigned = []
    
    # 1次パス：明確なパターン（法人番号、郵便番号、URL、電話）
    for val in row_values:
        val = str(val).strip()
        if not val or val.lower() == 'nan': continue
        
        if re_corp.match(val):
            if not val.startswith('918000'): new_row[3] = val # 法人番号
            continue
        if re_zip.match(val):
            if new_row[9] is None: new_row[9] = val # 郵便番号
            elif new_row[13] is None: new_row[13] = val # 代表者郵便番号
            continue
        if re_url.match(val):
            new_row[4] = val # URL
            continue
        if re_tel.match(val):
            new_row[12] = val # 電話
            continue
        unassigned.append(val)
        
    # 2次パス：文脈判断（住所、会社名、役員、概要）
    for val in unassigned:
        # 住所 or 概要（都道府県から始まる場合）
        if re_pref.match(val):
            if len(val) < 45 and not any(x in val for x in ["を展開", "の名前で", "拠点"]):
                if new_row[10] is None: new_row[10] = val # 住所
                elif new_row[14] is None: new_row[14] = val # 代表者住所
            else:
                new_row[22] = val # 概要
            continue
            
        # 役員情報
        if any(x in val for x in ["（取）", "（監）", "取締役"]):
            new_row[26] = (new_row[26] + " | " + val) if new_row[26] else val
            continue

        # 会社名（特定の接尾辞）
        if any(x in val for x in ["株式会社", "有限会社", "合同会社"]):
            if new_row[0] is None: new_row[0] = val
            continue
            
        # その他、空いている説明・概要欄へ
        for idx in [21, 22, 1, 2, 5, 6, 7, 8, 23, 24, 25, 27, 28, 29, 30, 31]:
            if new_row[idx] is None:
                new_row[idx] = val
                break
    return new_row

# 実行
for i, file_path in enumerate(TARGET_FILES, 1):
    output_path = os.path.join(OUTPUT_DIR, f"{i}.csv")
    if not os.path.exists(file_path):
        print(f"Skipping (Not Found): {file_path}")
        continue
        
    corrected_data = []
    with open(file_path, 'r', encoding='utf-8-sig', errors='replace') as f:
        reader = csv.reader(f)
        next(reader, None) # ヘッダーを飛ばす
        for row in reader:
            if any(row):
                corrected_data.append(heuristic_reorder(row))

    pd.DataFrame(corrected_data, columns=TARGET_HEADERS).to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Fixed and Saved: {output_path}")

print("\nAll tasks completed. Please check the '修正済み' folder.")