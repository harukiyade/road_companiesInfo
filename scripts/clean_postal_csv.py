import csv
import os
import re

def clean_postal_codes(target_dir):
    # 郵便番号のパターン: 「3桁-4桁」 または 「ハイフンなし7桁」
    # これに合致しないものは異常値とみなす
    postal_pattern = re.compile(r'^(\d{3}-\d{4}|\d{7})$')

    files = [f for f in os.listdir(target_dir) if f.endswith('.csv')]
    
    for filename in files:
        file_path = os.path.join(target_dir, filename)
        temp_rows = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            # 「郵便番号」列のインデックスを探す
            try:
                post_idx = header.index("郵便番号")
            except ValueError:
                print(f"Skipping {filename}: '郵便番号' column not found.")
                continue

            for row in reader:
                if not row: continue
                
                # 郵便番号列の値をチェック
                postal_val = row[post_idx].strip()
                
                # パターンに合致しない、かつ空でない場合は空欄にする
                if postal_val and not postal_pattern.match(postal_val):
                    row[post_idx] = "" # 空欄に置き換え
                
                temp_rows.append(row)

        # 修正した内容でファイルを上書き保存
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(temp_rows)
        
        print(f"Cleaned: {filename}")

# 実行
clean_postal_codes("csv_final_fixed_12")
print("\nすべてのファイルの郵便番号クレンジングが完了しました。")