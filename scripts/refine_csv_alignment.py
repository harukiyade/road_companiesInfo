import csv
import os
import re

def refine_csv_alignment(target_dir):
    # 郵便番号のパターン (3桁-4桁 または 7桁数字)
    postal_pattern = re.compile(r'^(\d{3}-\d{4}|\d{7})$')
    
    files = [f for f in os.listdir(target_dir) if f.endswith('.csv')]
    
    for filename in files:
        file_path = os.path.join(target_dir, filename)
        refined_rows = []
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            # 各列のインデックスを取得
            try:
                idx_zip = header.index("郵便番号")
                idx_addr = header.index("住所")
                idx_est = header.index("設立")
                idx_tel = header.index("電話番号(窓口)")
                idx_rep_zip = header.index("代表者郵便番号")
            except ValueError as e:
                print(f"Skipping {filename}: {e}")
                continue

            for row in reader:
                if not row: continue
                
                # --- ズレの判定ロジック ---
                
                # 1. 代表者郵便番号列まで業種(文字)が入っている場合：左へ5つ移動
                # (住所, 設立, 電話, 代表者郵便番号の4箇所が埋まっている可能性が高いが、代表者郵便番号に着目)
                if len(row) > idx_rep_zip and row[idx_rep_zip] and not postal_pattern.match(row[idx_rep_zip].strip()):
                    # row[idx_zip + 5] 以降を row[idx_zip] の位置へ
                    row = row[:idx_zip] + row[idx_zip + 5:]
                
                # 2. 電話番号列まで業種が入っている場合：左へ4つ移動
                elif len(row) > idx_tel and row[idx_tel] and not re.search(r'\d', row[idx_tel]):
                    row = row[:idx_zip] + row[idx_zip + 4:]
                
                # 3. 設立列まで業種が入っている場合：左へ3つ移動
                elif len(row) > idx_est and row[idx_est] and not re.search(r'\d', row[idx_est]):
                    row = row[:idx_zip] + row[idx_zip + 3:]
                
                # 4. 住所列まで業種が入っている場合：左へ2つ移動
                elif len(row) > idx_addr and row[idx_addr] and not re.search(r'[都道府県]', row[idx_addr]):
                    # 住所列に「都道府県」が含まれず、何らかの文字がある場合
                    row = row[:idx_zip] + row[idx_zip + 2:]
                
                # 5. 住所列に郵便番号が入っている場合：左へ1つ移動
                elif len(row) > idx_addr and postal_pattern.match(row[idx_addr].strip()):
                    # 郵便番号の次に続くべきデータ(本来の住所)を左に詰める
                    row = row[:idx_zip] + row[idx_zip + 1:]

                # 行の長さをヘッダーに合わせる（足りない分は空文字で補填）
                if len(row) < len(header):
                    row.extend([""] * (len(header) - len(row)))
                elif len(row) > len(header):
                    row = row[:len(header)]
                
                refined_rows.append(row)

        # 上書き保存
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(refined_rows)
        
        print(f"Refined alignment: {filename}")

# 実行
refine_csv_alignment("csv_final_fixed_12")