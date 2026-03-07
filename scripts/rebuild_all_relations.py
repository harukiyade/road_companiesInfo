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

    print("1. 中間テーブルの child_company_id を最新の企業IDに更新中...")
    # 子会社の名前をキーにして、現在の companies テーブルの正しい ID をセットし直します
    cur.execute("""
        UPDATE company_relations cr
        SET child_company_id = c.id::text
        FROM companies c
        WHERE cr.child_company_name = c.name
        AND (cr.child_company_id IS NULL OR cr.child_company_id != c.id::text);
    """)
    print(f" -> {cur.rowcount} 件の子会社IDを最新化しました。")

    print("\n2. 親会社側の company_relation_ids カラムを一括更新中...")
    # すべての親会社に対して、中間テーブルの最新IDリストを書き戻します
    # タイムアウトを避けるため、データがあるものだけを対象にします
    cur.execute("""
        UPDATE companies c
        SET company_relation_ids = sub.agg_ids
        FROM (
            SELECT parent_company_id, jsonb_agg(id) AS agg_ids
            FROM company_relations
            GROUP BY parent_company_id
        ) AS sub
        WHERE c.id::text = sub.parent_company_id;
    """)
    print(f" -> {cur.rowcount} 件の親企業のリンクを復元しました。")

    conn.commit()
    print("\nすべての関連会社データの反映が完了しました！")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()