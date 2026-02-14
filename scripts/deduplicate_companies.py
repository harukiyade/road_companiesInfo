import psycopg2
import os
import time

# --- 設定 ---
DB_NAME = "postgres"
DB_USER = "postgres"
DB_HOST = "127.0.0.1"
DB_PORT = "5432"
# パスワードは環境変数から、なければ直書きでも可
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'Legatus2000/')

def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

def run_deduplication():
    print("重複削除処理を開始します（00% 〜 99%）...")
    
    conn = get_connection()
    # 自動コミットをオフ（手動でコミット制御するため）
    conn.autocommit = False 
    
    try:
        # 00 から 99 までループ
        for i in range(100):
            prefix = f"{i:02d}" # "00", "01", ... "99"
            
            # すでに終わっている '00'~'10' をスキップしたい場合は、
            # if i <= 10: continue  # ←この行のコメントを外してください
            
            print(f"処理中: 法人番号が '{prefix}%' で始まるレコード...")
            start_time = time.time()
            
            with conn.cursor() as cur:
                # SQLの構築（Pythonのf-stringでprefixを埋め込む）
                sql = f"""
                -- 1. 一時テーブル作成（最強レコードの特定）
                CREATE TEMP TABLE best_records_{prefix} AS
                SELECT
                    corporate_number,
                    min(id) as survivor_id,
                    (array_agg(name ORDER BY length(name) DESC) FILTER (WHERE name IS NOT NULL))[1] as best_name,
                    (array_agg(address ORDER BY length(address) DESC) FILTER (WHERE address IS NOT NULL))[1] as best_address,
                    max(url) as best_url,
                    max(representative_name) as best_representative_name,
                    (array_agg(description ORDER BY length(description) DESC) FILTER (WHERE description IS NOT NULL))[1] as best_description,
                    max(employee_count) as best_employee_count,
                    max(revenue) as best_revenue,
                    max(capital) as best_capital
                FROM companies
                WHERE corporate_number LIKE '{prefix}%' 
                  AND corporate_number IS NOT NULL 
                  AND corporate_number != ''
                GROUP BY corporate_number
                HAVING count(*) > 1;

                -- 2. 生存者レコードの更新
                UPDATE companies c
                SET
                    name = b.best_name,
                    address = b.best_address,
                    url = COALESCE(c.url, b.best_url),
                    representative_name = COALESCE(c.representative_name, b.best_representative_name),
                    description = COALESCE(c.description, b.best_description),
                    employee_count = COALESCE(c.employee_count, b.best_employee_count),
                    revenue = COALESCE(c.revenue, b.best_revenue),
                    capital = COALESCE(c.capital, b.best_capital),
                    updated_at = NOW()
                FROM best_records_{prefix} b
                WHERE c.id = b.survivor_id;

                -- 3. 重複レコードの削除
                DELETE FROM companies
                WHERE id IN (
                    SELECT c.id
                    FROM companies c
                    JOIN best_records_{prefix} b ON c.corporate_number = b.corporate_number
                    WHERE c.id != b.survivor_id
                );

                -- 4. 一時テーブルの削除
                DROP TABLE best_records_{prefix};
                """
                
                cur.execute(sql)
                
                # 削除件数を取得できないか試みる（DELETEの結果など）
                # ※ 複数の文を一気に実行しているため、正確な件数は取れない場合がありますが処理は進みます
            
            # 1ループごとにコミット（確定）
            conn.commit()
            
            elapsed = time.time() - start_time
            print(f"  -> 完了 (所要時間: {elapsed:.2f}秒)")

    except Exception as e:
        print(f"エラー発生！ロールバックします: {e}")
        conn.rollback()
    finally:
        conn.close()
        print("全処理が終了しました。")

if __name__ == "__main__":
    run_deduplication()