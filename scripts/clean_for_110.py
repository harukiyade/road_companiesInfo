import os
import csv
import pandas as pd

# 110.csv系のターゲットヘッダー (38列構成)
TARGET_HEADERS_110 = [
    "会社名", "都道府県", "代表者名", "法人番号", "URL", "業種1", "業種2", "業種3", "業種（細）", 
    "郵便番号", "住所", "設立", "電話番号(窓口)", "代表者郵便番号", "代表者住所", "代表者誕生日", 
    "資本金", "上場", "直近決算年月", "直近売上", "直近利益", "説明", "概要", "仕入れ先", 
    "取引先", "取引先銀行", "取締役", "株主", "社員数", "オフィス数", "工場数", "店舗数",
    "空1", "空2", "空3", "空4", "空5", "空6"
]

def safe_fix_110_style(file_path, output_path):
    corrected_data = []
    
    with open(file_path, 'r', encoding='utf-8-sig', errors='replace') as f:
        # 110.csv系はカンマ問題が少ないため通常のreaderで読み込み
        reader = csv.reader(f)
        next(reader, None) # ヘッダーをスキップ
        
        for row in reader:
            if not any(row): continue
            
            # --- 処理1: 法人番号の無効化 (4列目: index 3) ---
            if len(row) > 3:
                corp_num = str(row[3]).strip()
                if corp_num.startswith('9180') or corp_num.startswith('9.18E'):
                    row[3] = "" # ダミーを消去
            
            # --- 処理2: 列数の補正 (38列に合わせる) ---
            if len(row) < 38:
                row.extend([''] * (38 - len(row)))
            elif len(row) > 38:
                row = row[:38] # 万が一多い場合はカット
                
            corrected_data.append(row)

    # 保存
    df_out = pd.DataFrame(corrected_data, columns=TARGET_HEADERS_110)
    df_out.to_csv(output_path, index=False, encoding='utf-8-sig')

# テスト実行
safe_fix_110_style("csv_2/csv/110.csv", "修正済み_v3_110.csv")
print("110.csvの安全修復が完了しました。")