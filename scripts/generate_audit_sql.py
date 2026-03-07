import pandas as pd
import os

# 以前取得したカラムリストのCSVを使用します
COLUMNS_CSV = 'data/studio_results_20260222_0302.csv'
OUTPUT_SQL = 'sqlResultFile/check_column_usage.sql'

def main():
    if not os.path.exists(COLUMNS_CSV):
        print(f"エラー: {COLUMNS_CSV} が見つかりません。先にカラム一覧を書き出してください。")
        return

    df_cols = pd.read_csv(COLUMNS_CSV)
    columns = df_cols['column_name'].tolist()

    with open(OUTPUT_SQL, 'w', encoding='utf-8') as f:
        f.write("-- 各カラムのデータ充足数を確認するSQL\n")
        f.write("SELECT\n")
        f.write("    COUNT(*) AS total_rows,\n")
        
        # 各カラムについて、NULLや空でないものをカウントするSQLを生成
        select_lines = []
        for col in columns:
            line = f"    COUNT(NULLIF(CAST({col} AS TEXT), '')) FILTER (WHERE CAST({col} AS TEXT) NOT IN ('[]', '{{}}')) AS {col}_count"
            select_lines.append(line)
        
        f.write(",\n".join(select_lines))
        f.write("\nFROM companies;\n")

    print(f"調査用SQLを生成しました: {OUTPUT_SQL}")
    print("このSQLをDBツールで実行し、結果をCSVで保存してください。")

if __name__ == "__main__":
    main()