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
    mapping = {
        'オフセット印刷業（紙）': '製造業',
        '自動車一般整備業': 'サービス業',
        '総合リース業': '金融・保険業',
        '出版業': '運輸・通信業',
        '保育所': 'サービス業',
        '鉄鋼業': '製造業',
        'ＡＳＰ・ウェブコンテンツ提供業': '運輸・通信業',
        'ガソリンスタンド': '卸売・小売・飲食業',
        '学習塾': 'サービス業',
        '野菜作農業（きのこ類栽培を含む）': '農林・水産',
        '測量業': 'サービス業',
        '職業紹介業': 'サービス業',
        '理容業': 'サービス業',
    }

    conn = psycopg2.connect(get_db_url())
    conn.autocommit = True
    cur = conn.cursor()

    print("🚀 第7弾：個別マッピングと数値データのクリーニングを開始...\n")

    # 1. 個別マッピングの適用
    for old_val, new_val in mapping.items():
        cur.execute("UPDATE companies SET industry_large = %s WHERE industry_large = %s", (new_val, old_val))
        if cur.rowcount > 0:
            print(f"✅ '{old_val}' -> '{new_val}' ({cur.rowcount:,}件) 完了")

    # 2. 数値のみのデータを「不明」に置換（UIを壊さないため）
    # もしUIに「不明」がない場合は ''（空文字）に変えてください
    cur.execute("""
        UPDATE companies 
        SET industry_large = '不明' 
        WHERE industry_large ~ '^[0-9]+$' 
          AND industry_large NOT IN ('サービス業', '不動産業', '卸売・小売・飲食業', '建設業', '林業', '漁業', '製造業', '農林・水産', '運輸・通信業', '金融・保険業', '鉱業', '電気・ガス')
    """)
    if cur.rowcount > 0:
        print(f"🧹 数値のみのゴミデータ {cur.rowcount:,} 件を '不明' に置換しました")

    print("\n🎉 第7弾完了！")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()