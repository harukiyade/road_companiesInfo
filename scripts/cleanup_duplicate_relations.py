import os
import psycopg2
from psycopg2.extras import DictCursor

# DB接続情報
host = os.environ.get('POSTGRES_HOST', '34.84.189.233')
port = os.environ.get('POSTGRES_PORT', '5432')
dbname = os.environ.get('POSTGRES_DB', 'postgres')
user = os.environ.get('POSTGRES_USER', 'postgres')
password = os.environ.get('POSTGRES_PASSWORD', 'Legatus2000/')

def main():
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    cur = conn.cursor(cursor_factory=DictCursor)

    print("1. 重複紐付けが発生している企業名を抽出中...")
    cur.execute("""
        SELECT name FROM companies 
        WHERE company_relation_ids IS NOT NULL AND company_relation_ids != '[]'::jsonb
        GROUP BY name HAVING count(*) > 1
    """)
    target_names = [row['name'] for row in cur.fetchall()]
    total = len(target_names)
    print(f" -> 対象となる重複企業名: {total} 種")

    print("\n2. 企業名ごとにクレンジングを実行中...")
    for i, name in enumerate(target_names):
        # 1つの名前の中で「本命」以外のIDを取得する
        # ルール: 上場 > 法人番号あり > 住所あり > IDが13桁 > 最終更新
        cur.execute("""
            SELECT id FROM (
                SELECT id, 
                ROW_NUMBER() OVER(
                    ORDER BY 
                        (CASE WHEN listing = '上場' THEN 1 ELSE 2 END) ASC,
                        (CASE WHEN corporate_number IS NOT NULL THEN 1 ELSE 2 END) ASC,
                        (CASE WHEN address IS NOT NULL THEN 1 ELSE 2 END) ASC,
                        (CASE WHEN LENGTH(id) = 13 THEN 1 ELSE 2 END) ASC,
                        updated_at DESC
                ) as rank
                FROM companies 
                WHERE name = %s 
                AND company_relation_ids IS NOT NULL 
                AND company_relation_ids != '[]'::jsonb
            ) sub WHERE rank > 1
        """, (name,))
        
        bad_ids = [row['id'] for row in cur.fetchall()]
        
        if bad_ids:
            # 「本命」以外をリセット
            cur.execute("UPDATE companies SET company_relation_ids = '[]'::jsonb WHERE id = ANY(%s)", (bad_ids,))
            # 中間テーブルからも削除
            cur.execute("DELETE FROM company_relations WHERE parent_company_id = ANY(%s)", (bad_ids,))
            
        if i % 100 == 0:
            conn.commit()
            print(f" -> {i}/{total} 件完了 (現在の名前: {name})")

    conn.commit()
    print("\nすべてのクレンジングが完了しました。")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()