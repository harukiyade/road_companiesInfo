import csv
import os
import shutil

def get_group(header):
    if "法人番号" in header and "ID" in header:
        return "A_B"
    elif "法人番号" not in header and "ID" not in header and "取引種別" in header:
        return "C"
    elif "法人番号" in header and "取引種別" not in header:
        return "D"
    return None

def process_and_cleanup(file_list, output_dir):
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    processed_count = 0
    for file_path in file_list:
        if not os.path.exists(file_path):
            continue
            
        filename = os.path.basename(file_path)
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                continue

            group = get_group(header)
            
            # --- インデックスの再定義 ---
            # Part 1: 開始〜備考の前まで
            # Part 2: URL列
            # Part 3: 郵便番号〜最後まで
            
            if group == "A_B":
                # 0:会社名 ... 9:ステータス | 10:備考(消) | 11:URL | 12:業1...14:業3 | 15:郵便
                p1_end, url_idx, post_start = 10, 11, 15
            elif group == "C":
                # 0:会社名 ... 7:ステータス | 8:備考(消) | 9:URL | 10:業1...12:業3 | 13:郵便
                p1_end, url_idx, post_start = 8, 9, 13
            elif group == "D":
                # 備考なし: 0:会社名 ... 3:法人番号 | 4:URL | 5:業1...7:業3 | 8:郵便
                p1_end, url_idx, post_start = 4, 4, 8
            else:
                continue

            # 新ヘッダーの作成
            if group == "D":
                new_header = header[:p1_end + 1] + header[post_start:]
            else:
                new_header = header[:p1_end] + [header[url_idx]] + header[post_start:]

            corrected_rows = []
            for row in reader:
                if not row: continue
                
                # 業種4が混入している場合（列数がヘッダーより多い）、郵便番号以降をさらに1つ右にずらす
                shift = 1 if len(row) > len(header) else 0
                
                if group == "D":
                    new_row = row[:p1_end + 1] + row[post_start + shift:]
                else:
                    new_row = row[:p1_end] + [row[url_idx]] + row[post_start + shift:]
                
                corrected_rows.append(new_row)

        output_path = os.path.join(output_dir, filename)
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(new_header)
            writer.writerows(corrected_rows)
        
        processed_count += 1
        print(f"[{processed_count}/12] {filename} を整形しました")

target_files = [
    "csv_2/import_firstTime/36.csv", "csv_2/import_firstTime/43.csv",
    "csv_2/import_firstTime/44.csv", "csv_2/import_firstTime/47.csv",
    "csv_2/import_firstTime/49.csv", "csv_2/import_firstTime/108.csv",
    "csv_2/import_firstTime/120.csv", "csv_2/import_firstTime/121.csv",
    "csv_2/import_firstTime/123.csv", "csv_2/import_firstTime/124.csv",
    "csv_2/import_firstTime/42.csv", "csv_2/import_firstTime/48.csv"
]

process_and_cleanup(target_files, "csv_final_fixed_12")