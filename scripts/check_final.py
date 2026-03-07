import os
import psycopg2
from dotenv import load_dotenv

# .envを読み込む
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def check():
    try:
        print(f"DBに接続を試行中...")
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        print("✅ 接続に成功しました！\n")

        # 1. companies テーブルのカラムを確認
        print("--- [companies] カラム一覧 ---")
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'companies'")
        cols_c = [row[0] for row in cursor.fetchall()]
        print(cols_c)

        # 2. company_relations テーブルのカラムを確認
        print("\n--- [company_relations] カラム一覧 ---")
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'company_relations'")
        cols_r = [row[0] for row in cursor.fetchall()]
        if not cols_r:
            print("⚠️ company_relations テーブルがまだ存在しないようです。")
        else:
            print(cols_r)

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ 接続エラー: {e}")

if __name__ == "__main__":
    check()