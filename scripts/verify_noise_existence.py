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

    # 特定のノイズ文字で検索
    target = 'お客様の要望をかたちにします。more'
    cur.execute("SELECT id, industry_large, industry_detail FROM companies WHERE industry_detail = %s", (target,))
    
    rows = cur.fetchall()
    if rows:
        print(f"😱 警告: DB内にまだ {len(rows)} 件残っています！")
        for row in rows:
            print(f"ID: {row[0]} | 大分類: {row[1]} | 詳細: {row[2]}")
    else:
        print("✅ DB内にはもう存在しません。DB側はクリーンです。")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()