import psycopg2
import pandas as pd
import re

DB_CONFIG = {
    "host": "34.84.189.233", "database": "postgres", "user": "postgres",
    "password": "Legatus2000/", "port": "5432"
}
MASTER_CSV = "data/00_zenkoku_all_20260130.csv"

def normalize_name(name):
    """社名から法人格や空白を除去して『芯』だけにする"""
    if not name or pd.isna(name): return ""
    name = str(name)
    name = re.sub(r'[\(（].*?[\)）]', '', name) # (株)などを除去
    name = re.sub(r'株式会社|有限会社|合同会社|代表取締役|（株）|\(株\)', '', name)
    name = re.sub(r'\s+', '', name) # 空白除去
    return name

def run():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✅ DB接続成功")

    # 1. 未紐付け社名を取得し、最初から『芯』に変換して保持
    cursor.execute("""
        SELECT cr.child_company_name, c.address, cr.id
        FROM company_relations cr
        JOIN companies c ON cr.parent_company_id = c.id
        WHERE cr.child_corporate_number IS NULL
    """)
    target_data = {} # {芯の名前: {"original_name": ..., "parent_pref": ..., "ids": []}}
    for c_name, p_addr, rel_id in cursor.fetchall():
        core = normalize_name(c_name)
        if not core: continue
        pref = p_addr[:3] if p_addr else ""
        if core not in target_data:
            target_data[core] = {"parent_pref": pref, "names": set(), "ids": []}
        target_data[core]["names"].add(c_name)
        target_data[core]["ids"].append(rel_id)

    target_cores = set(target_data.keys())
    print(f"解析対象（芯の数）: {len(target_cores)} 件")

    # 2. 全国マスターを走査（12列目も正規化して比較）
    found_map = {} # {芯の名前: [{"h_num": ..., "pref": ...}, ...]}
    reader = pd.read_csv(MASTER_CSV, encoding='cp932', chunksize=200000, header=None)
    
    print("全国マスターを走査中 (芯によるマッチング)...")
    for i, chunk in enumerate(reader):
        # 12列目(商号)を正規化。高速化のためisinは使わず、一時的な列を作って比較
        chunk['core'] = chunk[12].apply(normalize_name)
        mask = chunk['core'].isin(target_cores)
        
        if mask.any():
            for _, row in chunk[mask].iterrows():
                core, h_num, pref = row['core'], str(row[1]), str(row[13])
                if core not in found_map: found_map[core] = []
                found_map[core].append({"h_num": h_num, "pref": pref})
        
        if (i+1) % 5 == 0: print(f"  - {(i+1)*20}万行 走査中...")

    # 3. 精査と反映
    print("特定とDB更新を開始...")
    success_count = 0
    for core, candidates in found_map.items():
        p_pref = target_data[core]["parent_pref"]
        target_h_num = None

        if len(candidates) == 1:
            target_h_num = candidates[0]['h_num']
        else:
            # 複数候補時は都道府県で絞り込み
            matched = [c for c in candidates if p_pref in c['pref']]
            if len(matched) == 1:
                target_h_num = matched[0]['h_num']

        if target_h_num:
            for original_name in target_data[core]["names"]:
                cursor.execute(
                    "UPDATE company_relations SET child_corporate_number = %s WHERE child_company_name = %s AND child_corporate_number IS NULL",
                    (target_h_num, original_name)
                )
            success_count += 1

    conn.commit()
    
    # 最終紐付け
    cursor.execute("""
        UPDATE company_relations cr
        SET child_company_id = c.id
        FROM companies c
        WHERE cr.child_corporate_number = c.corporate_number AND cr.child_company_id IS NULL;
    """)
    conn.commit()

    cursor.execute("SELECT COUNT(*) FROM company_relations WHERE child_company_id IS NOT NULL")
    print(f"✅ 完了！ リンク済み総数: {cursor.fetchone()[0]} 件 (新規救済: {success_count}社)")
    conn.close()

if __name__ == "__main__":
    run()