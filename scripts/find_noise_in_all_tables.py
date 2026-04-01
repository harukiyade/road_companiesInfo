import os
import re
import psycopg2

def get_db_url():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(current_dir, '..', '.env')
    with open(env_path, 'r', encoding='utf-8') as f:
        content = f.read()
    match = re.search(r'^DATABASE_URL="?(.*?)"?$', content, re.M)
    url = match.group(1).replace('postgresql+psycopg2://', 'postgresql://')
    url = re.sub(r'@[^/:]+:\d+', '@127.0.0.1:5433', url)
    return url

def main():
    conn = psycopg2.connect(get_db_url())
    cur = conn.cursor()

    target = 'お客様の要望をかたちにします。more'
    
    # DB内の全テーブル・全カラムから検索するクエリ（少し時間がかかる場合があります）
    print(f"🔍 DB全体のテーブルから '{target}' を探しています...")
    
    cur.execute("""
        SELECT table_name, column_name 
        FROM information_schema.columns 
        WHERE table_schema = 'public' AND data_type IN ('character varying', 'text');
    """)
    columns = cur.fetchall()
    
    found = False
    for table, column in columns:
        try:
            # 各テーブルのカラムをチェック
            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} = %s", (target,))
            count = cur.fetchone()[0]
            if count > 0:
                print(f"🚩 発見！ テーブル名: 【{table}】 / カラム名: 【{column}】 ({count}件)")
                found = True
        except:
            continue

    if not found:
        print("❌ DB内のどのテーブルにも存在しませんでした。")
        print("👉 ということは、サーバーのプログラム内か、フロントエンドのコード内に直接書かれている可能性があります。")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()