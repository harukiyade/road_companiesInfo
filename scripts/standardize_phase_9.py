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
    # 1. 頻出キーワードに基づく一括マッピング
    patterns = [
        # サービス業
        ("%処分業", "サービス業"), ("%処理業", "サービス業"), ("%エステ%", "サービス業"),
        ("%サロン%", "サービス業"), ("%クラブ", "サービス業"), ("%施術所", "サービス業"),
        ("%写真業", "サービス業"), ("%証明業", "サービス業"), ("%式場業", "サービス業"),
        ("サービス", "サービス業"), ("%洗濯業", "サービス業"), ("%浴場業", "サービス業"),
        
        # 卸売・小売・飲食業
        ("%喫茶店", "卸売・小売・飲食業"), ("%ビヤホール", "卸売・小売・飲食業"),
        ("%バー", "卸売・小売・飲食業"), ("%代理商", "卸売・小売・飲食業"),
        ("%仲立業", "卸売・小売・飲食業"),
        
        # 運輸・通信業
        ("%運営業", "運輸・通信業"), ("%こん包業", "運輸・通信業"),
        ("%プロバイダ%", "運輸・通信業"),
    ]

    conn = psycopg2.connect(get_db_url())
    conn.autocommit = True
    cur = conn.cursor()

    master_categories = (
        'サービス業', '不動産業', '卸売・小売・飲食業', '建設業', 
        '林業', '漁業', '製造業', '農林・水産', 
        '運輸・通信業', '金融・保険業', '鉱業', '電気・ガス'
    )

    print("🚀 第9弾：広域クリーニング（3万件台の解消）を開始...\n")

    for pat, new in patterns:
        cur.execute("""
            UPDATE companies 
            SET industry_large = %s 
            WHERE industry_large LIKE %s 
              AND industry_large NOT IN %s
        """, (new, pat, master_categories))
        if cur.rowcount > 0:
            print(f"📦 '{pat}' -> '{new}' ({cur.rowcount:,}件)")

    print("\n🎉 第9弾完了！")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()