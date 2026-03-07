import pandas as pd
from sqlalchemy import create_engine, text

# --- 設定（接続情報を書き換えてください） ---
DB_URL = 'postgresql://user:pass@localhost/your_db'

def main():
    engine = create_engine(DB_URL)
    print("カラムの使用状況を調査しています...")

    with engine.connect() as conn:
        # カラム一覧を取得
        res = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name = 'companies'"))
        columns = [row[0] for row in res]
        
        # 総行数を取得
        total = conn.execute(text("SELECT count(*) FROM companies")).scalar()
        
        audit_results = []
        for col in columns:
            # 値が入っている（NULLでない、かつ空文字でない）数をカウント
            # 型が様々なのでCASTしてチェック
            count_res = conn.execute(text(f"""
                SELECT count(*) FROM companies 
                WHERE {col} IS NOT NULL 
                AND CAST({col} AS TEXT) NOT IN ('', '[]', '{{}}')
            """))
            filled = count_res.scalar()
            fill_rate = round((filled / total) * 100, 2)
            
            audit_results.append({
                "column": col,
                "filled_count": filled,
                "fill_rate_%": fill_rate
            })
            print(f"調査完了: {col} ({fill_rate}%)")

    # 結果を保存
    df = pd.DataFrame(audit_results).sort_values(by="fill_rate_%", ascending=False)
    df.to_csv('sqlResultFile/column_audit_report.csv', index=False)
    print(f"\n監査完了！レポートを保存しました: sqlResultFile/column_audit_report.csv")

if __name__ == "__main__":
    main()