import pandas as pd
import re
import os
import glob

# 郵便番号の正規表現（3桁-4桁）
RE_ZIP_STRICT = re.compile(r'^\d{3}-\d{4}$')

def get_group_id(filename):
    f = filename.lower()
    if any(x in f for x in ["111", "112", "113", "114", "115", "116", "117"]): return 1
    if any(x in f for x in ["107", "109", "110"]): return 2
    if "122" in f: return 3
    if "105" in f: return 4
    if "106" in f: return 5
    if "119" in f: return 6
    return None

def get_zip_column_index(group_id):
    """グループごとの本来の郵便番号列のインデックス(0開始)を返す"""
    if group_id in [1, 2]: return 9   # 10列目
    if group_id == 3: return 7      # 8列目
    if group_id == 4: return 2      # 3列目
    if group_id == 5: return 2      # 3列目
    if group_id == 6: return 15     # 16列目
    return None

def find_invalid_zip_files(target_dir):
    files = glob.glob(os.path.join(target_dir, "*.csv"))
    error_summary = []

    print(f"--- 郵便番号不備のチェック開始: {target_dir} ---")

    for fp in files:
        filename = os.path.basename(fp)
        gid = get_group_id(filename)
        zip_idx = get_zip_column_index(gid)
        
        if zip_idx is None:
            continue

        try:
            df = pd.read_csv(fp, encoding='utf-8-sig', dtype=str)
            # 列名から判定するのではなくインデックスで直接指定
            # (修正済みCSVなので列順は維持されている前提)
            
            # 郵便番号列の値を抽出
            zip_series = df.iloc[:, zip_idx]
            
            # 不備（空、あるいは形式不正）を判定
            is_invalid = zip_series.apply(lambda x: not bool(RE_ZIP_STRICT.match(str(x).strip())))
            invalid_df = df[is_invalid]

            if len(invalid_df) > 0:
                print(f"【不備あり】 {filename}: {len(invalid_df)} 件の不備レコード")
                error_summary.append({
                    "file": filename,
                    "group": gid,
                    "invalid_count": len(invalid_df),
                    "total_rows": len(df),
                    "sample_invalid_value": zip_series[is_invalid].iloc[0] if not invalid_df.empty else "N/A"
                })
        except Exception as e:
            print(f"エラー分析中 {filename}: {e}")

    return pd.DataFrame(error_summary)

if __name__ == "__main__":
    TARGET_DIR = "csv_2_fixed_output"
    report_df = find_invalid_zip_files(TARGET_DIR)
    
    if not report_df.empty:
        report_df.to_csv("zip_error_files_report.csv", index=False, encoding='utf-8-sig')
        print(f"\n報告: {len(report_df)} 個のファイルで不備が見つかりました。")
        print("詳細は 'zip_error_files_report.csv' を確認してください。")
    else:
        print("\n全てのファイルの郵便番号列が正常（3桁-4桁）です！")