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

    # 1. どの子会社が「オプティム」を親に持っていたかを探し、失われた親IDを特定する
    print("1. 中間テーブルから『失われた親ID』を特定中...")
    cur.execute("""
        SELECT DISTINCT parent_company_id 
        FROM company_relations 
        WHERE child_company_name IN (
            'デジタルコンストラクション株式会社', 
            'バンクテクノロジーズ株式会社', 
            'ユラスコア株式会社'
        )
    """)
    lost_parent_ids = [str(r[0]) for r in cur.fetchall()]
    
    if not lost_parent_ids:
        print(" -> 関連会社側からの親特定に失敗しました。名前一致で進めます。")
        lost_parent_ids = ['1010401083431'] # デフォルトの旧ID
    else:
        print(f" -> 特定された旧親ID: {lost_parent_ids}")

    # 2. 現在のDBに残っている『本命』候補を探す
    # 優先：佐賀（ID:2966731）か、豊島区（ID:1764997326527292）
    print("\n2. 現在の『本命』レコードを選択中...")
    cur.execute("""
        SELECT id, address FROM companies 
        WHERE name = '株式会社オプティム'
        ORDER BY 
            (CASE WHEN id = '2966731' THEN 1 -- 佐賀(R&D) 
                  WHEN id = '1764997326527292' THEN 2 -- 豊島区
                  ELSE 3 END) ASC
        LIMIT 1
    """)
    res = cur.fetchone()
    if not res:
        print(" -> [エラー] 本命レコードが見つかりません。")
        return
    
    new_id = str(res[0])
    print(f" -> 本命を ID: {new_id} (住所: {res[1]}) に決定しました。")

    # 3. 中間テーブルの親IDを書き換える
    print(f"\n3. 中間テーブルの親IDを {new_id} に統合中...")
    cur.execute("""
        UPDATE company_relations 
        SET parent_company_id = %s 
        WHERE parent_company_id = ANY(%s)
           OR (parent_company_id IN (SELECT id::text FROM companies WHERE name = '株式会社オプティム'))
    """, (new_id, lost_parent_ids))
    print(f" -> {cur.rowcount} 件の紐付けを修復しました。")

    # 4. 最終同期
    print("\n4. companiesテーブルへのIDリスト同期を実行中...")
    cur.execute("""
        UPDATE companies 
        SET company_relation_ids = sub.agg_ids
        FROM (
            SELECT parent_company_id, jsonb_agg(id) AS agg_ids
            FROM company_relations
            WHERE parent_company_id = %s
            GROUP BY parent_company_id
        ) AS sub
        WHERE id::text = %s
    """, (new_id, new_id))
    
    conn.commit()
    print(f"\n成功！ ID: {new_id} のレコードに関連会社が復旧しました。")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()