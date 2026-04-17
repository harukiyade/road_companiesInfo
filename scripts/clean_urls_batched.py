import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def clean_urls():
    # DB接続
    conn = psycopg2.connect(
        host=os.getenv("DATABASE_HOST", "127.0.0.1"),
        port=os.getenv("DATABASE_PORT", "5434"),
        user=os.getenv("DATABASE_USER", "postgres"),
        password=os.getenv("DATABASE_PASS", "Legatus2000"),
        dbname=os.getenv("DATABASE_NAME", "postgres")
    )
    conn.autocommit = True # 1回ずつの処理を即座に確定させる
    cur = conn.cursor()

    batch_size = 50000 # 1回に処理する件数
    total_updated = 0

    print("🚀 URLクレンジングを開始します（577万件規模対応版）...")

    # 修正が必要な件数をあらかじめ把握
    cur.execute("SELECT COUNT(*) FROM companies WHERE homepage_url ~ '[\[\]]';")
    target_count = cur.fetchone()[0]
    print(f"対象件数: {target_count} 件")

    while True:
        # 修正が必要な行を batch_size 分だけ抽出して更新
        # PostgreSQLの正規表現置換機能を活用
        cur.execute(f"""
            WITH targets AS (
                SELECT id FROM companies 
                WHERE homepage_url ~ '[\[\]]' 
                LIMIT {batch_size}
            )
            UPDATE companies
            SET homepage_url = CASE 
                WHEN homepage_url = '[]' THEN NULL
                WHEN homepage_url ~ '^\[".+"\]$' THEN 
                    REPLACE(REPLACE(REPLACE(homepage_url, '["', ''), '"]', ''), '\"', '')
                ELSE REPLACE(REPLACE(homepage_url, '[', ''), ']', '')
            END
            FROM targets
            WHERE companies.id = targets.id;
        """)
        
        updated = cur.rowcount
        if updated == 0:
            break
            
        total_updated += updated
        print(f" [進捗] {total_updated} / {target_count} 件完了...")

    print(f"✨ クレンジング完了！ 合計 {total_updated} 件を修正しました。")
    cur.close()
    conn.close()

if __name__ == "__main__":
    clean_urls()
