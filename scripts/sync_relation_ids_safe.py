import os
import psycopg2
from psycopg2.extras import execute_values
import json

# 環境変数から接続情報を取得（ターミナルで指定したものを使用）
host = os.environ.get('POSTGRES_HOST', '34.84.189.233')
port = os.environ.get('POSTGRES_PORT', '5432')
dbname = os.environ.get('POSTGRES_DB', 'postgres')
user = os.environ.get('POSTGRES_USER', 'postgres')
password = os.environ.get('POSTGRES_PASSWORD', 'Legatus2000/')

def main():
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cur = conn.cursor()

    print("1. 中間テーブルから更新対象のデータを集計中...")
    # 中間テーブルにある親IDごとの子ID配列をすべて取得（数万件程度なのでメモリに乗ります）
    cur.execute("""
        SELECT parent_company_id, jsonb_agg(id) 
        FROM company_relations 
        GROUP BY parent_company_id
    """)
    rows = cur.fetchall()
    total = len(rows)
    print(f" -> 更新対象の親企業数: {total} 件")

    print("2. 1,000件ずつ companies テーブルを更新中...")
    batch_size = 1000
    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        
        # 1件ずつUPDATE文を発行（小分けにすることでタイムアウトを防ぐ）
        for parent_id, agg_ids in batch:
            cur.execute("""
                UPDATE companies 
                SET company_relation_ids = %s 
                WHERE id = %s
            """, (json.dumps(agg_ids), parent_id))
        
        conn.commit()
        print(f" -> {i + len(batch)} / {total} 件完了...")

    print("\nすべての同期が完了しました。")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()