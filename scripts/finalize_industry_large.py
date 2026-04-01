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
    # UIマスターの正解（12種類）
    master_categories = (
        'サービス業', '不動産業', '卸売・小売・飲食業', '建設業', 
        '林業', '漁業', '製造業', '農林・水産', 
        '運輸・通信業', '金融・保険業', '鉱業', '電気・ガス'
    )

    conn = psycopg2.connect(get_db_url())
    conn.autocommit = True
    cur = conn.cursor()

    # 現状の不在件数を確認
    cur.execute("""
        SELECT COUNT(*) FROM companies 
        WHERE industry_large NOT IN %s
    """, (master_categories,))
    remaining_count = cur.fetchone()[0]

    if remaining_count == 0:
        print("✅ すべてのデータが既に標準化されています。")
        return

    print(f"🔥 最終処理：残りの {remaining_count:,} 件を 'サービス業' に集約します...")

    # 一括更新
    cur.execute("""
        UPDATE companies 
        SET industry_large = 'サービス業' 
        WHERE industry_large NOT IN %s
    """, (master_categories,))
    
    print(f"🎉 完了！ 全データをマスター名称に適合させました。")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()