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
    # 第5弾のマッピング定義
    mapping = {
        '一般管工事業': '建設業',
        '教育，学習支援業': 'サービス業',
        '土工・コンクリート工事業': '建設業',
        '電気・ガス・熱供給・ 水道業': '電気・ガス',
        'ごみ収集運搬業': 'サービス業',
        'パルプ・紙・紙加工品製造業': '製造業',
        '金融業，保険業': '金融・保険業',
        'その他の食料・飲料卸売業': '卸売・小売・飲食業',
        '情報提供サービス業': '運輸・通信業',
        '一般電気工事業': '建設業',
        '電気機械器具製造業': '製造業',
        '貸家業': '不動産業',
        '機械器具設置工事業': '建設業',
        'その他の建築材料卸売業': '卸売・小売・飲食業',
        '倉庫業（冷蔵倉庫業を除く）': '運輸・通信業',
    }

    conn = psycopg2.connect(get_db_url())
    conn.autocommit = True
    cur = conn.cursor()

    print("🚀 第5弾の表記ゆれをマスター名称に標準化します...\n")

    for old_val, new_val in mapping.items():
        cur.execute("SELECT COUNT(*) FROM companies WHERE industry_large = %s", (old_val,))
        count = cur.fetchone()[0]
        
        if count == 0:
            continue

        print(f"🔄 '{old_val}' ({count:,}件) -> '{new_val}' へ変換中...")
        
        updated_in_this_category = 0
        while True:
            cur.execute("""
                UPDATE companies 
                SET industry_large = %s 
                WHERE id IN (
                    SELECT id FROM companies 
                    WHERE industry_large = %s 
                    LIMIT 10000
                )
            """, (new_val, old_val))
            
            rows = cur.rowcount
            if rows == 0:
                break
                
            updated_in_this_category += rows
            print(f"  ✅ {updated_in_this_category:,} / {count:,} 件完了")

    print("\n🎉 第5弾の標準化が完了しました！")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()