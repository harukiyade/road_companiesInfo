import os
import re
import psycopg2
from collections import Counter

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

    # 「業種っぽくない」条件で抽出
    # 1. 長すぎる (20文字以上)
    # 2. 文末が「ます」「です」「！」
    # 3. 句読点が含まれる
    # 4. 特定のNGワードが含まれる
    cur.execute("""
        SELECT industry_detail, COUNT(*) as count
        FROM companies 
        WHERE industry_detail ~ '[。！たりますですmore…、]'
           OR LENGTH(industry_detail) > 20
        GROUP BY industry_detail
        ORDER BY count DESC
        LIMIT 100;
    """)
    
    rows = cur.fetchall()
    
    print("🔍 【業種（細）のノイズ候補 上位100件】")
    print("-" * 50)
    for detail, count in rows:
        print(f"[{count:5}件] : {detail}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()