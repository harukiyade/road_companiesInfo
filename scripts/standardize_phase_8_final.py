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
    # 1. 個別ピンポイント修正（トップ15対策）
    direct_mapping = {
        '不明': 'サービス業',
        '建設': '建設業',
        '農業,林業': '農林・水産',
        '証券・商品': '金融・保険業',
        'ガソリンスタンド': '卸売・小売・飲食業',
        '学習塾': 'サービス業',
        '測量業': 'サービス業',
        '理容業': 'サービス業',
        '美容業': 'サービス業',
    }

    # 2. パターンによる広域修正（残りの3万件対策）
    patterns = [
        ("%整備業", "サービス業"),
        ("%診療所", "サービス業"),
        ("%団体", "サービス業"),
        ("%料理店", "卸売・小売・飲食業"),
        ("%制作業", "運輸・通信業"),
        ("%薬局", "サービス業"),
        ("%こん包業", "運輸・通信業"),
        ("%リース業", "金融・保険業"),
        ("%支援業", "サービス業"),
        ("%案内業", "サービス業"),
        ("%出版業", "運輸・通信業"),
        ("%精米%", "製造業"),
        ("%精麦%", "製造業"),
    ]

    conn = psycopg2.connect(get_db_url())
    conn.autocommit = True
    cur = conn.cursor()

    master_categories = (
        'サービス業', '不動産業', '卸売・小売・飲食業', '建設業', 
        '林業', '漁業', '製造業', '農林・水産', 
        '運輸・通信業', '金融・保険業', '鉱業', '電気・ガス'
    )

    print("🔥 第8弾：最終決戦クリーニングを開始...\n")

    # 個別修正の実行
    for old, new in direct_mapping.items():
        cur.execute("UPDATE companies SET industry_large = %s WHERE industry_large = %s", (new, old))
        if cur.rowcount > 0:
            print(f"🎯 [Direct] '{old}' -> '{new}' ({cur.rowcount:,}件)")

    # パターン修正の実行（マスター以外のものだけを対象にする）
    for pat, new in patterns:
        cur.execute("""
            UPDATE companies 
            SET industry_large = %s 
            WHERE industry_large LIKE %s 
              AND industry_large NOT IN %s
        """, (new, pat, master_categories))
        if cur.rowcount > 0:
            print(f"📦 [Pattern] '{pat}' -> '{new}' ({cur.rowcount:,}件)")

    print("\n🎉 第8弾完了！")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()