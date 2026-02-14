import csv
import os
import re

def auto_align_by_postal_code(target_dir):
    # 郵便番号のパターン (3桁-4桁)
    postal_pattern = re.compile(r'^\d{3}-\d{4}$')
    
    files = [f for f in os.listdir(target_dir) if f.endswith('.csv')]
    
    for filename in files:
        file_path = os.path.join(target_dir, filename)
        refined_rows = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            try:
                # 基準となる「郵便番号」列のインデックス
                base_zip_idx = header.index("郵便番号")
            except ValueError:
                print(f"Skipping {filename}: '郵便番号' column not found.")
                continue

            for row in reader:
                if not row: continue
                
                shift_n = 0
                # 郵便番号列の次（住所列）から順に、どこに郵便番号が紛れ込んでいるか探す
                # 住所(base+1), 設立(base+2), 電話(base+3)... と続く
                for i in range(base_zip_idx + 1, len(row)):
                    val = row[i].strip()
                    if postal_pattern.match(val):
                        # 見つかった場所 i と、本来あるべき場所 base_zip_idx の差分がシフト量
                        shift_n = i - base_zip_idx
                        break
                
                # シフトが必要な場合、その行を再構築する
                if shift_n > 0:
                    # 原理: 郵便番号(base_zip_idx)までのデータ + 
                    #       見つかった郵便番号位置(i)から後ろのデータ
                    # これにより、間に入り込んだ「業種」などのゴミが削除され、左に詰まる
                    new_row = row[:base_zip_idx] + row[base_zip_idx + shift_n:]
                else:
                    new_row = row

                # 行の長さをヘッダーに合わせる（不足は空欄、超過はカット）
                if len(new_row) < len(header):
                    new_row.extend([""] * (len(header) - len(new_row)))
                else:
                    new_row = new_row[:len(header)]
                
                refined_rows.append(new_row)

        # 上書き保存
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(refined_rows)
        
        print(f"Successfully aligned {filename} (Max detected shifts applied)")

# 実行
auto_align_by_postal_code("csv_final_fixed_12")