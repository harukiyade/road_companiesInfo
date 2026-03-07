import os
import psycopg2

# DB接続情報
host = os.environ.get('POSTGRES_HOST', '34.84.189.233')
port = os.environ.get('POSTGRES_PORT', '5432')
dbname = os.environ.get('POSTGRES_DB', 'postgres')
user = os.environ.get('POSTGRES_USER', 'postgres')
password = os.environ.get('POSTGRES_PASSWORD', 'Legatus2000/')

def main():
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cur = conn.cursor()

    print("1. 株式会社オプティムの『正しいレコード』を探索中...")
    # 法人番号 1010401083431 または名前で、現在有効なIDを探す
    cur.execute("""
        SELECT id, name, address FROM companies 
        WHERE corporate_number = '1010401083431' 
           OR (name = '株式会社オプティム' AND address LIKE '%港区%')
        ORDER BY (CASE WHEN corporate_number = '1010401083431' THEN 1 ELSE 2 END) ASC
        LIMIT 1
    """)
    res = cur.fetchone()
    
    if not res:
        print(" -> [警告] 本家のオプティムが見つかりませんでした。")
        return
    
    new_parent_id = str(res[0])
    print(f" -> 発見しました。現在の有効ID: {new_parent_id} (住所: {res[2]})")

    print(f"\n2. 中間テーブルの紐付けを ID:{new_parent_id} に更新中...")
    # 旧ID(1010401083431) または 名前での紐付けを現在のIDに付け替える
    cur.execute("""
        UPDATE company_relations 
        SET parent_company_id = %s 
        WHERE parent_company_id = '1010401083431'
           OR (parent_company_id IN (SELECT id::text FROM companies WHERE name = '株式会社オプティム') 
               AND parent_company_id != %s)
    """, (new_parent_id, new_parent_id))
    print(f" -> {cur.rowcount} 件の関連レコードを付け替えました。")

    print("\n3. companiesテーブルの配列カラムを最新状態に同期中...")
    cur.execute("""
        UPDATE companies 
        SET company_relation_ids = sub.agg_ids
        FROM (
            SELECT parent_company_id, jsonb_agg(id) AS agg_ids
            FROM company_relations
            WHERE parent_company_id = %s
            GROUP BY parent_company_id
        ) AS sub
        WHERE id::text = sub.parent_company_id
    """, (new_parent_id,))
    
    conn.commit()
    print("完了！ オプティムの関連会社情報が復旧しました。")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()