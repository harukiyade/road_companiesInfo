import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def check_schema():
    try:
        print(f"接続を試行中... (URL: {DB_URL.split('@')[-1] if DB_URL else 'None'})")
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
        
        # 1. companies テーブルの確認
        print("\n--- [companies] テーブルの構造 ---")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'companies'
        """)
        columns = cursor.fetchall()
        for col in columns:
            print(f"カラム名: {col[0]:<20} | 型: {col[1]}")

        # 2. company_relations テーブルの確認
        print("\n--- [company_relations] テーブルの構造 ---")
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'company_relations'
        """)
        columns = cursor.fetchall()
        if not columns:
            print("⚠️ company_relations テーブルが見つかりません。")
        for col in columns:
            print(f"カラム名: {col[0]:<20} | 型: {col[1]}")

        cursor.close()
        conn.close()
        print("\n✅ 接続と構造確認が完了しました。")

    except Exception as e:
        print(f"\n❌ エラーが発生しました:\n{e}")

if __name__ == "__main__":
    check_schema()