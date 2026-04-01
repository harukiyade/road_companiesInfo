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
    # キーワードパターンの定義 (LIKE演算子を使用)
    # 「この文字で終わる/含むなら、この大分類」というルール
    patterns = [
        # 建設業
        ("%工事業", "建設業"), ("%建築業", "建設業"), ("%土木業", "建設業"), ("%設備業", "建設業"),
        # 製造業
        ("%製造業", "製造業"), ("%工業", "製造業"), ("%製造", "製造業"), ("%金型", "製造業"),
        # 卸売・小売・飲食業
        ("%卸売業", "卸売・小売・飲食業"), ("%小売業", "卸売・小売・飲食業"), ("%商店", "卸売・小売・飲食業"), ("%飲食店", "卸売・小売・飲食業"), ("%販売店", "卸売・小売・飲食業"),
        # 運輸・通信業
        ("%運送業", "運輸・通信業"), ("%運輸業", "運輸・通信業"), ("%通信業", "運輸・通信業"), ("%ソフトウェア業", "運輸・通信業"), ("%情報サービス業", "運輸・通信業"),
        # 不動産業
        ("%不動産業", "不動産業"), ("%不動産", "不動産業"), ("%仲介業", "不動産業"),
        # サービス業
        ("%サービス業", "サービス業"), ("%コンサルティング", "サービス業"), ("%メンテナンス業", "サービス業"), ("%警備業", "サービス業"), ("%福祉事業", "サービス業"),
    ]

    conn = psycopg2.connect(get_db_url())
    conn.autocommit = True
    cur = conn.cursor()

    print("🔥 キーワードによる一括クリーニングを開始します...\n")

    total_updated = 0
    for pattern, new_val in patterns:
        # すでに正しい大分類になっているものは除外して更新
        cur.execute("""
            UPDATE companies 
            SET industry_large = %s 
            WHERE industry_large LIKE %s 
              AND industry_large != %s
        """, (new_val, pattern, new_val))
        
        rows = cur.rowcount
        if rows > 0:
            print(f"📦 パターン '{pattern}' -> '{new_val}' : {rows:,} 件を変換しました")
            total_updated += rows

    print(f"\n🎉 クリーニング完了！ 合計 {total_updated:,} 件を標準化しました。")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()