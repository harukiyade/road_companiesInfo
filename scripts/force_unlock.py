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

# 環境変数から取得できなければ、実行時に入力を求める
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

def kill_zombies():
    global DB_PASSWORD
    if not DB_PASSWORD:
        print(f"接続先: {DB_HOST}")
        DB_PASSWORD = getpass.getpass("DBパスワードを入力してください: ")

    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        conn.autocommit = True
        cur = conn.cursor()
        
        logging.info("ゾンビプロセス（古い接続）を探しています...")
        
        # 自分以外の、アイドル状態または長時間動いている接続を特定
        cur.execute("""
            SELECT pid, state, query 
            FROM pg_stat_activity 
            WHERE pid <> pg_backend_pid()
            AND (state = 'idle in transaction' OR query LIKE '%INSERT%' OR query LIKE '%UPDATE%');
        """)
        
        rows = cur.fetchall()
        if not rows:
            logging.info("邪魔をしているプロセスはありませんでした。")
        else:
            for pid, state, query in rows:
                q_sample = query[:50] if query else "None"
                logging.warning(f"強制終了対象: PID={pid} State={state} Query={q_sample}...")
                cur.execute(f"SELECT pg_terminate_backend({pid});")
                logging.info(f" -> PID {pid} をキルしました。")
            
            logging.info("すべてのゾンビプロセスを排除しました。")

        cur.close()
        conn.close()

    except psycopg2.OperationalError as e:
        logging.error(f"接続エラー: パスワードが間違っているか、接続が拒否されました。\n詳細: {e}")
    except Exception as e:
        logging.error(f"エラー: {e}")

if __name__ == "__main__":
    kill_zombies()