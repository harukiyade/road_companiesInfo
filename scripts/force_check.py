import psycopg2

def check():
    try:
        print("DBへの接続を試行中（個別パラメータ指定）...")
        # URL形式を使わず、個別に指定することで誤認を防ぎます
        conn = psycopg2.connect(
            host="34.84.189.233",
            database="postgres",
            user="postgres",
            password="Legatus2000", # ここにスラッシュは不要です
            port="5432"
        )
        cursor = conn.cursor()
        print("✅ 接続に成功しました！\n")

        # 確認したいテーブル
        target_tables = ['companies', 'company_relations']
        
        for table in target_tables:
            print(f"--- [{table}] の構造 ---")
            cursor.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table}'
            """)
            cols = cursor.fetchall()
            if not cols:
                print(f"⚠️ {table} テーブルが見つかりません。")
            else:
                for col in cols:
                    print(f"カラム名: {col[0]:<25} | 型: {col[1]}")
            print()

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ 接続エラー: {e}")
        print("\nヒント: もし 'timeout' や 'no route to host' が出る場合は、Google CloudのIP制限を再確認してください。")

if __name__ == "__main__":
    check()