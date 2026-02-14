import pandas as pd
import argparse
import os

def analyze_and_filter(input_path, output_path, threshold_year=None):
    print(f"Loading: {input_path} ...")
    
    # 前回の出力（450万件のCSV）を読み込みます
    # もし前回のCSVがない場合は、元のgBizINFO CSVを指定しても動くように設計しています
    try:
        df = pd.read_csv(input_path, dtype=str)
    except:
        print("ファイルが見つかりません。パスを確認してください。")
        return

    total = len(df)
    print(f"現在の件数: {total:,} 社")

    # --- カラム名の特定（前回出力版 or 元データ版） ---
    # 最終更新日のカラムを探す
    date_col = None
    possible_date_cols = ['updated_at', '最終更新日', 'update_date', 'change_date', 'date_of_update']
    for c in possible_date_cols:
        if c in df.columns:
            date_col = c
            break
            
    # URLのカラムを探す
    url_col = None
    possible_url_cols = ['company_url', '企業ホームページ', 'url', 'source_url']
    for c in possible_url_cols:
        if c in df.columns:
            url_col = c
            break

    if not date_col:
        print("エラー: '最終更新日' に相当するカラムが見つかりません。")
        print(f"カラム一覧: {df.columns.tolist()}")
        return

    # --- 分析: 更新年ごとの分布 ---
    print("\n--- [分析] 最終更新年ごとの企業数分布 ---")
    
    # 日付から「年」を抽出 (YYYY-MM-DD -> YYYY)
    # データが空の場合は 'Unknown'
    df['year_temp'] = df[date_col].str[:4].fillna('Unknown')
    
    # 年ごとの集計
    year_counts = df['year_temp'].value_counts().sort_index(ascending=False)
    
    cumulative = 0
    target_found = False
    suggested_year = 2020 # デフォルト
    
    print(f"{'更新年':<10} | {'件数':<10} | {'累積件数 (新しい順)':<15}")
    print("-" * 45)
    
    for year, count in year_counts.items():
        if year == 'Unknown': continue
        cumulative += count
        print(f"{year:<10} | {count:<10,} | {cumulative:<15,}")
        
        # 累積が150万を超えたあたりの年を提案
        if not target_found and cumulative >= 1500000:
            suggested_year = year
            target_found = True

    print("-" * 45)
    
    # --- URL保有数 ---
    if url_col:
        has_url_count = df[df[url_col].notnull() & (df[url_col] != '')].shape[0]
        print(f"URL保有企業数: {has_url_count:,} 社")
    
    # --- フィルタリング実行 ---
    
    # ユーザー指定がない場合は、分析結果から「150万社になるライン」を自動設定
    if threshold_year is None and target_found:
        threshold_year = int(suggested_year)
        print(f"\n★ 推奨フィルタ: {threshold_year}年 以降に更新された企業に絞ると、約150万社になります。")
    elif threshold_year is None:
        threshold_year = 2022 # 安全策
        print(f"\n★ 設定: {threshold_year}年 以降に更新された企業に絞ります。")
    
    print(f"\n処理開始: {threshold_year}年 1月1日以降に更新された企業を抽出します...")
    
    # フィルタ適用
    # 文字列比較でOK ('2023' >= '2022')
    filtered_df = df[df['year_temp'] >= str(threshold_year)].copy()
    
    # 一時カラム削除
    filtered_df.drop(columns=['year_temp'], inplace=True)
    
    final_count = len(filtered_df)
    print(f"抽出結果: {final_count:,} 社 (削減数: {total - final_count:,})")
    
    # 出力
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    filtered_df.to_csv(output_path, index=False)
    print(f"ファイル保存完了: {output_path}")
    print("このファイルをDBインポート用に使ってください。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # デフォルトで前回の出力ファイルを見るようにしています
    parser.add_argument("--input", default="out/active_companies.csv")
    parser.add_argument("--output", default="out/target_150m_companies.csv")
    parser.add_argument("--year", type=int, help="足切りする年 (例: 2023)")
    
    args = parser.parse_args()
    analyze_and_filter(args.input, args.output, args.year)