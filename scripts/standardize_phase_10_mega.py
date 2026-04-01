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
    # 3.5万件の「ロングテール」を刈り取るための強力なパターン
    patterns = [
        # サービス業 (教育、医療、専門職、施設、団体など)
        ("%業（%", "サービス業"), ("%所", "サービス業"), ("%園", "サービス業"), ("%学", "サービス業"),
        ("%塾", "サービス業"), ("%クリニック", "サービス業"), ("%医院", "サービス業"), ("%病院", "サービス業"),
        ("%協会", "サービス業"), ("%組合", "サービス業"), ("%法人", "サービス業"), ("%事務所", "サービス業"),
        ("%センター", "サービス業"), ("%コンサル%", "サービス業"), ("%エージェンシー", "サービス業"),
        ("%スタジオ", "サービス業"), ("%サロン", "サービス業"),
        
        # 製造業 (加工系)
        ("%塗装%", "製造業"), ("%メッキ%", "製造業"), ("%プレス%", "製造業"), ("%加工%", "製造業"),
        ("%プリント%", "製造業"), ("%スリット%", "製造業"), ("%板金%", "製造業"),
        
        # 卸売・小売・飲食業 (モノ・食べ物)
        ("%食料品", "卸売・小売・飲食業"), ("%食品", "卸売・小売・飲食業"), ("%飲料", "卸売・小売・飲食業"),
        ("%ストア", "卸売・小売・飲食業"), ("%ショップ", "卸売・小売・飲食業"),
        
        # 運輸・通信業
        ("%ロジスティクス", "運輸・通信業"), ("%デリバリー", "運輸・通信業"),
    ]

    conn = psycopg2.connect(get_db_url())
    conn.autocommit = True
    cur = conn.cursor()

    master_categories = (
        'サービス業', '不動産業', '卸売・小売・飲食業', '建設業', 
        '林業', '漁業', '製造業', '農林・水産', 
        '運輸・通信業', '金融・保険業', '鉱業', '電気・ガス'
    )

    print("🔥 第10弾：最終広域クリーニング（ロングテール一掃）を開始...\n")

    for pat, new in patterns:
        cur.execute("""
            UPDATE companies 
            SET industry_large = %s 
            WHERE industry_large LIKE %s 
              AND industry_large NOT IN %s
        """, (new, pat, master_categories))
        if cur.rowcount > 0:
            print(f"📦 '{pat}' -> '{new}' ({cur.rowcount:,}件)")

    print("\n🎉 第10弾完了！")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()