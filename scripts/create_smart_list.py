import pandas as pd
import argparse
import os

def create_smart_list(input_path, output_path, target_count=1500000):
    print(f"Loading: {input_path} ...")
    
    # 全データを読み込み
    try:
        df = pd.read_csv(input_path, dtype=str)
    except Exception as e:
        print(f"エラー: {e}")
        return

    # カラムマッピング
    cols = {
        'name': '法人名',
        'close_date': '登記記録の閉鎖等年月日',
        'close_reason': '登記記録の閉鎖等の事由',
        'update_date': '最終更新日',
        'url': '企業ホームページ',
        'rep_name': '法人代表者名',
        'address': '本社所在地',
        'capital': '資本金',
        'employees': '従業員数',
        'founded': '設立年月日'
    }

    # --- 1. 基本フィルタ（生きている & 営利法人） ---
    print("基本フィルタ適用中...")
    is_alive = (
        (df[cols['close_date']].isnull() | (df[cols['close_date']] == '')) &
        (df[cols['close_reason']].isnull() | (df[cols['close_reason']] == ''))
    )
    target_keywords = ['株式会社', '有限会社', '合同会社', '合名会社', '合資会社']
    pattern = '|'.join(target_keywords)
    is_commercial = df[cols['name']].str.contains(pattern, na=False)

    active_df = df[is_alive & is_commercial].copy()
    print(f"  -> 候補全件: {len(active_df):,} 社")

    # --- 2. スコアリング（優先度付け） ---
    print("優先度スコアを計算中...")

    # フラグ作成（データがあるかどうか）
    # URLがあるか？
    has_url = active_df[cols['url']].notnull() & (active_df[cols['url']] != '')
    
    # 従業員数があるか？
    has_employees = active_df[cols['employees']].notnull() & (active_df[cols['employees']] != '')
    
    # 更新日（ソート用に日付型へ変換、エラーはNaT）
    active_df['sort_date'] = pd.to_datetime(active_df[cols['update_date']], errors='coerce')

    # 一時的にランク付け用の列を追加
    # True=1, False=0 なので、降順ソートでTrueが上に来ます
    active_df['flag_url'] = has_url
    active_df['flag_emp'] = has_employees

    # --- 3. スマートソート実行 ---
    # 優先順位: 
    #  1. URLがある (最重要)
    #  2. 従業員数がある (実態が確実)
    #  3. 更新日が新しい (最近動いている)
    print(f"スマートソート実行中: URL優先 > 従業員優先 > 更新日順")
    
    sorted_df = active_df.sort_values(
        by=['flag_url', 'flag_emp', 'sort_date'], 
        ascending=[False, False, False]
    )

    # --- 4. 上位抽出 ---
    final_df = sorted_df.head(target_count).copy()

    # 統計情報の表示
    count_url = final_df['flag_url'].sum()
    count_emp = final_df['flag_emp'].sum()
    min_date = final_df['sort_date'].min()

    print("-" * 40)
    print(f"【抽出結果内訳】 合計: {len(final_df):,} 社")
    print(f"  - URLあり企業: {count_url:,} 社 (これらは全て確保しました)")
    print(f"  - 従業員数あり: {count_emp:,} 社")
    print(f"  - リスト尻の更新日: {min_date}")
    print("-" * 40)

    # --- 5. 出力 ---
    output_columns = {
        '法人番号': 'corporate_number',
        cols['name']: 'name',
        cols['address']: 'address',
        cols['rep_name']: 'representative_name',
        cols['capital']: 'capital_stock',
        cols['employees']: 'employee_count',
        cols['founded']: 'founded_year',
        cols['url']: 'company_url',
    }
    
    select_cols = [c for c in output_columns.keys() if c in final_df.columns]
    final_output = final_df[select_cols].rename(columns=output_columns)
    
    # 設立年の整形
    if 'founded_year' in final_output.columns:
        final_output['founded_year'] = final_output['founded_year'].str[:4]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_output.to_csv(output_path, index=False)
    print(f"出力完了: {output_path}")

if __name__ == "__main__":
    INPUT_CSV = "gBizINFO/Kihonjoho_UTF-8.csv"
    OUTPUT_CSV = "out/final_smart_target.csv"
    
    create_smart_list(INPUT_CSV, OUTPUT_CSV)