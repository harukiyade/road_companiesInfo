import os
import re
import csv
import psycopg2

def get_db_url():
    # .envファイルのパスを探索
    current_dir = os.path.dirname(os.path.abspath(__file__))
    potential_env_paths = [
        os.path.join(current_dir, '..', '.env'),
        os.path.join(current_dir, '..', '..', '.env'),
        os.path.join(current_dir, '.env')
    ]
    
    env_path = None
    for p in potential_env_paths:
        if os.path.exists(p):
            env_path = p
            break
            
    if not env_path:
        raise FileNotFoundError(".envファイルが見つかりません。")

    with open(env_path, 'r', encoding='utf-8') as f:
        content = f.read()
    match = re.search(r'^DATABASE_URL="?(.*?)"?$', content, re.M)
    url = match.group(1).replace('postgresql+psycopg2://', 'postgresql://')
    # Cloud SQL Proxyのポートへ置換（5433を使用）
    url = re.sub(r'@[^/:]+:\d+', '@127.0.0.1:5433', url)
    return url

def main():
    # 指摘いただいた通り data/industries.csv を参照
    current_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(current_dir, '..', 'data', 'industries.csv')
    
    if not os.path.exists(csv_path):
        print(f"エラー: industries.csv が見つかりません。パスを確認してください: {csv_path}")
        return

    # 1. UIのマスターを読み込む
    valid_large = set()
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            val = row.get('industryLarge')
            if val:
                valid_large.add(val.strip())

    print(f"✅ UIマスター読み込み完了 (対象: {len(valid_large)} 業種)")
    print(f"📍 参照パス: {csv_path}")
    print("-" * 50)

    # 2. DBに接続
    conn = psycopg2.connect(get_db_url())
    cur = conn.cursor()

    # 大分類(industry_large)の集計
    cur.execute("SELECT industry_large, COUNT(*) FROM companies GROUP BY industry_large;")
    rows = cur.fetchall()

    matches = 0
    mismatches = 0
    mismatch_details = []

    for val, count in rows:
        if not val:
            continue
            
        if val in valid_large:
            matches += count
        else:
            mismatches += count
            mismatch_details.append((val, count))

    # 結果表示
    print(f"\n📊 【大分類 (industry_large) 集計結果】")
    print(f"🟢 UIマスターと一致: {matches:,} 件")
    print(f"🔴 UIマスターに不在: {mismatches:,} 件")
    
    if mismatch_details:
        print(f"\n⚠️ UIマスターに存在しない名称（上位15件）:")
        mismatch_details.sort(key=lambda x: x[1], reverse=True)
        for val, count in mismatch_details[:15]:
            print(f"  - '{val}': {count:,} 件")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()