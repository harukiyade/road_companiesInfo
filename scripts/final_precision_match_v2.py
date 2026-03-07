import psycopg2
import pandas as pd

DB_CONFIG = {
    "host": "34.84.189.233", "database": "postgres", "user": "postgres",
    "password": "Legatus2000/", "port": "5432"
}
MASTER_CSV = "data/00_zenkoku_all_20260130.csv"

def run():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✅ DB接続成功")

    # 1. 未紐付けの子会社と、その「親会社の都道府県」をDBから取得
    # companiesテーブルから親の住所(address)を引っ張ってきます
    cursor.execute("""
        SELECT cr.child_company_name, c.address, cr.id
        FROM company_relations cr
        JOIN companies c ON cr.parent_company_id = c.id
        WHERE cr.child_corporate_number IS NULL
    """)
    rows = cursor.fetchall()
    target_data = {} # {社名: {"parent_pref": 都道府県, "rel_ids": [id1, id2]}}
    for c_name, p_addr, rel_id in rows:
        pref = p_addr[:3] if p_addr else "" # 先頭3文字（東京都、大阪府など）
        if c_name not in target_data:
            target_data[c_name] = {"parent_pref": pref, "ids": []}
        target_data[c_name]["ids"].append(rel_id)

    target_names = set(target_data.keys())
    print(f"解析対象: {len(target_names)} 社")

    # 2. 全国マスターを走査（12列目:商号, 13列目:都道府県, 1列目:法人番号）
    found_map = {}
    reader = pd.read_csv(MASTER_CSV, encoding='cp932', chunksize=200000, header=None)
    for chunk in reader:
        mask = chunk[12].isin(target_names)
        if mask.any():
            for _, row in chunk[mask].iterrows():
                name, h_num, pref = row[12], str(row[1]), str(row[13])
                if name not in found_map: found_map[name] = []
                found_map[name].append({"h_num": h_num, "pref": pref})

    # 3. 住所マッチングで1社に特定
    print("親会社の所在地を元に同名企業を精査中...")
    success_count = 0
    for name, candidates in found_map.items():
        parent_pref = target_data[name]["parent_pref"]
        
        # 戦略A: 全国に1社しかないなら確定
        if len(candidates) == 1:
            target_h_num = candidates[0]['h_num']
        # 戦略B: 複数ある場合、親会社と同じ都道府県のものを探す
        else:
            matched = [c for c in candidates if parent_pref in c['pref']]
            if len(matched) == 1:
                target_h_num = matched[0]['h_num']
            else:
                continue # 特定不能

        # DB更新
        cursor.execute(
            "UPDATE company_relations SET child_corporate_number = %s WHERE child_company_name = %s AND child_corporate_number IS NULL",
            (target_h_num, name)
        )
        success_count += 1

    conn.commit()

    # 4. 最終紐付け
    cursor.execute("""
        UPDATE company_relations cr
        SET child_company_id = c.id
        FROM companies c
        WHERE cr.child_corporate_number = c.corporate_number AND cr.child_company_id IS NULL;
    """)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM company_relations WHERE child_company_id IS NOT NULL")
    print(f"✅ 完了！ リンク済み総数: {cursor.fetchone()[0]} 件 (今回新しく特定: {success_count}件)")
    conn.close()

if __name__ == "__main__":
    run()