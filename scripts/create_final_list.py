import pandas as pd
import argparse
import os

def create_final_list(input_path, output_path, target_count=1500000):
    print(f"Loading: {input_path} ...")
    
    # 1. 元データを読み込み
    try:
        df = pd.read_csv(input_path, dtype=str)
    except Exception as e:
        print(f"エラー: {e}")
        return

    # --- カラム名のマッピング ---
    # ユーザー様の環境ヘッダーに合わせています
    cols = {
        'name': '法人名',
        'close_date': '登記記録の閉鎖等年月日',
        'close_reason': '登記記録の閉鎖等の事由',
        'update_date': '最終更新日', # これを使って並び替えます
        'url': '企業ホームページ',
        'rep_name': '法人代表者名',
        'address': '本社所在地',
        'capital': '資本金',
        'founded': '設立年月日'
    }

    # 必須カラムチェック
    for key, col_name in cols.items():
        if col_name not in df.columns:
            # URLなどは無くても続行させますが、更新日は必須
            if key == 'update_date':
                print(f"致命的エラー: '{col_name}' カラムが見つかりません。ヘッダーを確認してください: {df.columns.tolist()}")
                return

    # --- 2. フィルタリング (生きている & 営利法人) ---
    print("フィルタリング中: 閉鎖・非営利を除外...")
    
    # 生存判定
    is_alive = (
        (df[cols['close_date']].isnull() | (df[cols['close_date']] == '')) &
        (df[cols['close_reason']].isnull() | (df[cols['close_reason']] == ''))
    )

    # 営利法人判定
    target_keywords = ['株式会社', '有限会社', '合同会社', '合名会社', '合資会社']
    pattern = '|'.join(target_keywords)
    is_commercial = df[cols['name']].str.contains(pattern, na=False)

    # 抽出
    active_df = df[is_alive & is_commercial].copy()
    print(f"  -> 候補件数: {len(active_df):,} 社")

    # --- 3. 上位150万社への絞り込み (Recency Cut) ---
    print(f"絞り込み中: 最終更新日が新しい順に {target_count:,} 社を抽出...")

    # 更新日で降順ソート (新しい日付が上)
    # 日付形式が混在している可能性があるので、一旦文字列としてソートでも概ねOKですが、
    # 念のため to_datetime で変換してソートします（エラーは無視してNaTにする）
    active_df['sort_date'] = pd.to_datetime(active_df[cols['update_date']], errors='coerce')
    
    # ソート実行
    active_df = active_df.sort_values(by='sort_date', ascending=False)
    
    # 上位N件を取得
    final_df = active_df.head(target_count).copy()
    
    # 最新と最古の日付を表示（確認用）
    latest_date = final_df[cols['update_date']].iloc[0]
    oldest_date = final_df[cols['update_date']].iloc[-1]
    print(f"  -> 抽出範囲: {latest_date} 〜 {oldest_date} までの更新企業")

    # --- 4. 出力用に整形 ---
    output_columns = {
        '法人番号': 'corporate_number',
        cols['name']: 'name',
        cols['address']: 'address',
        cols['rep_name']: 'representative_name',
        cols['capital']: 'capital_stock',
        cols['founded']: 'founded_year',
        cols['url']: 'company_url',
        cols['update_date']: 'update_date' # DBには入れないかもしれませんが、確認用に残します
    }
    
    # 存在するカラムだけリネーム
    select_cols = [c for c in output_columns.keys() if c in final_df.columns]
    final_output = final_df[select_cols].rename(columns=output_columns)

    # 設立年の整形 (YYYY-MM-DD -> YYYY)
    if 'founded_year' in final_output.columns:
        final_output['founded_year'] = final_output['founded_year'].str[:4]

    # ファイル出力
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_output.to_csv(output_path, index=False)
    
    print("-" * 30)
    print(f"完了！")
    print(f"出力ファイル: {output_path}")
    print(f"件数: {len(final_output):,} 社")
    print("-" * 30)

if __name__ == "__main__":
    # 元のgBizINFO CSVを指定
    INPUT_CSV = "gBizINFO/Kihonjoho_UTF-8.csv"
    OUTPUT_CSV = "out/final_target_companies.csv"
    
    create_final_list(INPUT_CSV, OUTPUT_CSV)