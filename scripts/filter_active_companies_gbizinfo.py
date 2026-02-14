import pandas as pd
import argparse
import os

def filter_active_companies(input_path, output_path, dry_run=False):
    print(f"Loading: {input_path}")
    
    # 全カラムを文字列として読み込む
    df = pd.read_csv(input_path, dtype=str)
    
    # --- 1. カラム名のマッピング（お持ちのCSVに合わせて定義） ---
    # CSVのヘッダー名 -> 内部変数名
    cols = {
        'name': '法人名',
        'close_date': '登記記録の閉鎖等年月日',
        'close_reason': '登記記録の閉鎖等の事由',
        'url': '企業ホームページ',           # ここが取れるのはラッキーです！
        'rep_name': '法人代表者名',
        'address': '本社所在地'
    }

    # --- 2. フィルタリング条件 ---

    # A. 「閉鎖」していないこと（生きている）
    # 「閉鎖年月日」が空 AND 「閉鎖事由」が空 の場合を生存とみなす
    is_alive = (
        (df[cols['close_date']].isnull() | (df[cols['close_date']] == '')) &
        (df[cols['close_reason']].isnull() | (df[cols['close_reason']] == ''))
    )

    # B. 「営利法人」であること（法人名から判定）
    # 「法人種別」列がないため、名前の中に以下の文字列が含まれるかチェックします
    target_keywords = ['株式会社', '有限会社', '合同会社', '合名会社', '合資会社']
    
    # 正規表現で一括チェック (株式会社|有限会社|...)
    pattern = '|'.join(target_keywords)
    is_commercial = df[cols['name']].str.contains(pattern, na=False)

    # C. 名前があること
    has_name = df[cols['name']].notnull() & (df[cols['name']] != '')

    # --- 3. 抽出実行 ---
    active_df = df[is_alive & is_commercial & has_name].copy()

    # --- 4. 出力用にカラム名を正規化（DBに入れやすくする） ---
    # DBのカラム名に合わせてリネームして出力します
    # 必要最低限のカラムに絞ります
    output_columns = {
        '法人番号': 'corporate_number',
        cols['name']: 'name',
        cols['address']: 'address',
        cols['rep_name']: 'representative_name',
        '資本金': 'capital_stock',
        '従業員数': 'employee_count',
        '設立年月日': 'founded_year',
        cols['url']: 'company_url' # 企業ホームページ
    }
    
    # 存在するカラムだけを選択してリネーム
    select_cols = [c for c in output_columns.keys() if c in active_df.columns]
    final_df = active_df[select_cols].rename(columns=output_columns)

    # 設立年月日から「年」だけ抽出（YYYY-MM-DD -> YYYY）
    if 'founded_year' in final_df.columns:
        final_df['founded_year'] = final_df['founded_year'].str[:4]

    # 件数表示
    total_count = len(df)
    active_count = len(final_df)
    
    print("-" * 30)
    print(f"全件: {total_count:,}")
    print(f"アクティブ（営利法人）: {active_count:,}")
    print(f"除外された数: {total_count - active_count:,}")
    print("-" * 30)

    if not dry_run:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        final_df.to_csv(output_path, index=False)
        print(f"出力完了: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="out/active_companies.csv")
    parser.add_argument("--dry-run", action="store_true")
    input_csv = "gBizINFO/Kihonjoho_UTF-8.csv" 
    
    args = parser.parse_args()
    filter_active_companies(input_csv, args.output, args.dry_run)