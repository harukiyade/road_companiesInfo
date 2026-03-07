import psycopg2
import pandas as pd
import re
import unicodedata

DB_CONFIG = {
    "host": "34.84.189.233", "database": "postgres", "user": "postgres",
    "password": "Legatus2000/", "port": "5432"
}
MASTER_CSV = "data/00_zenkoku_all_20260130.csv"

def clean_sentence(name):
    """文章の中から社名らしい部分だけを削り出す"""
    if not name or pd.isna(name): return ""
    name = str(name)
    # 1. 有報特有の末尾フレーズを削除
    phrases = [
        r'については.*', r'ほか\d+社.*', r'他\d+社.*', r'により.*', 
        r'に伴い.*', r'しております.*', r'に社名を変更し.*', r'となりましたが.*',
        r'を連結の範囲から除外.*', r'連結子会社であった'
    ]
    for p in phrases:
        name = re.sub(p, '', name)
    return name.strip()

def normalize_name(name):
    """法人格や記号を除去して比較用の芯を作る"""
    name = clean_sentence(name)
    if not name: return ""
    name = unicodedata.normalize('NFKC', name).lower()
    name = re.sub(r'[\(（\[［].*?[\)）\]］]', '', name)
    name = re.sub(r'株式会社|有限会社|合同会社|（株）|\(株\)|連結子会社|持分法適用.*', '', name)
    name = re.sub(r'[\s　!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>/?？・＆]', '', name)
    return name

def run():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    print("✅ DB接続成功")

    cursor.execute("""
        SELECT cr.child_company_name, c.address, cr.id
        FROM company_relations cr
        JOIN companies c ON cr.parent_company_id = c.id
        WHERE cr.child_company_id IS NULL
    """)
    target_data = {}
    for c_name, p_addr, rel_id in cursor.fetchall():
        core = normalize_name(c_name)
        if not core or len(core) < 2: continue # 短すぎるのはノイズ
        pref = p_addr[:3] if p_addr else ""
        if core not in target_data:
            target_data[core] = {"parent_pref": pref, "names": set()}
        target_data[core]["names"].add(c_name)

    target_cores = set(target_data.keys())
    print(f"再解析対象（文章除去後）: {len(target_cores)} 件")

    found_map = {}
    reader = pd.read_csv(MASTER_CSV, encoding='cp932', chunksize=200000, header=None)
    for i, chunk in enumerate(reader):
        chunk['core'] = chunk[6].apply(normalize_name)
        mask = chunk['core'].isin(target_cores)
        if mask.any():
            for _, row in chunk[mask].iterrows():
                core, h_num, pref = row['core'], str(row[1]), str(row[9])
                if core not in found_map: found_map[core] = []
                if h_num not in [c['h_num'] for c in found_map[core]]:
                    found_map[core].append({"h_num": h_num, "pref": pref})
        if (i+1) % 10 == 0: print(f"  - {(i+1)*20}万行 走査完了...")

    success_count = 0
    for core, candidates in found_map.items():
        p_pref = target_data[core]["parent_pref"]
        target_h_num = None
        if len(candidates) == 1:
            target_h_num = candidates[0]['h_num']
        elif len(candidates) > 1:
            matched = [c for c in candidates if p_pref in c['pref']]
            if len(matched) == 1: target_h_num = matched[0]['h_num']

        if target_h_num:
            for original_name in target_data[core]["names"]:
                cursor.execute("UPDATE company_relations SET child_corporate_number = %s WHERE child_company_name = %s AND child_company_id IS NULL", (target_h_num, original_name))
            success_count += 1
    conn.commit()

    cursor.execute("""
        UPDATE company_relations cr
        SET child_company_id = c.id
        FROM companies c
        WHERE cr.child_corporate_number = c.corporate_number AND cr.child_company_id IS NULL;
    """)
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM company_relations WHERE child_company_id IS NOT NULL")
    print(f"✅ 完了！ リンク済み総数: {cursor.fetchone()[0]} 件 (今回新規に救済: {success_count}社)")
    conn.close()

if __name__ == "__main__":
    run()