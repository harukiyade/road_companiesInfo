import os
import psycopg2
import logging
import getpass

# --- 設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

# 環境変数になければ、後で入力してもらう
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

def add_index():
    global DB_PASSWORD
    
    # パスワードがない場合に入力を求める
    if not DB_PASSWORD:
        print(f"接続先: {DB_HOST}")
        DB_PASSWORD = getpass.getpass("DBパスワードを入力してください: ")

    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        conn.autocommit = True  # インデックス作成にはオートコミットが必要
        cur = conn.cursor()

        logging.info("接続成功。インデックスを作成しています... (数分かかる場合があります)")
        logging.info("※もしここで長時間止まる場合は、別のターミナルで force_unlock.py を実行してください。")

        # 会社名と都道府県の組み合わせに対するインデックスを作成
        cur.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_name_pref 
            ON companies (name, prefecture);
        """)
        logging.info("成功: インデックス 'idx_companies_name_pref' を作成しました！")

    except psycopg2.OperationalError as e:
        logging.error(f"接続エラー: パスワードが間違っているか、接続が拒否されました。\n詳細: {e}")
    except Exception as e:
        logging.error(f"エラー: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    add_index()