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
    conn.autocommit = True
    cur = conn.cursor()

    print("🧹 業種（細）のノイズを一括クリーニングします...\n")

    # 1. 明らかな宣伝・文章・ニュース文（文章形式）を空にする
    # 「ます」「です」で終わる、句点を含む長い文、moreで終わる、などを対象
    cur.execute("""
        UPDATE companies 
        SET industry_detail = '' 
        WHERE industry_detail ~ '[ますです。]' 
           OR industry_detail LIKE '%%more'
           OR industry_detail ~ '^[A-Za-z]{3}-[0-9]{2}$'; -- Dec-63 形式の排除
    """)
    print(f"✅ 宣伝文句・文章・変換エラーを {cur.rowcount:,} 件削除しました。")

    # 2. 構成比（％）などの余計な数値を削除して、業種名だけを残す
    # 例：「建築工事一式（９９％）」 -> 「建築工事一式」
    # 全角・半角の両方のパーセント、およびカッコ内を対象に除去を試みる
    cur.execute("""
        UPDATE companies 
        SET industry_detail = REGEXP_REPLACE(industry_detail, '[（\(][^（\(]*[0-9０-９．.]{1,}[％%][^）\)]*[）\)]', '', 'g')
        WHERE industry_detail ~ '[％%]';
    """)
    print(f"✅ 構成比（％）表記を {cur.rowcount:,} 件クレンジングしました。")

    # 3. 前後の空白や、ゴミ記号の整理
    cur.execute("""
        UPDATE companies 
        SET industry_detail = TRIM(BOTH ' 　,;、；' FROM industry_detail)
        WHERE industry_detail != '';
    """)

    print("\n🎉 クリーニング完了！")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()