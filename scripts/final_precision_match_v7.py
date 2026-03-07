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
    if not name or pd.isna(name): return ""
    name = str(name)
    phrases = [
        r'については.*', r'ほか\d+社.*', r'他\d+社.*', r'により.*', 
        r'に伴い.*', r'しております.*', r'に社名を変更し.*', r'となりましたが.*',
        r'を連結の範囲から除外.*', r'連結子会社であった'
    ]
    for p in phrases:
        name = re.sub(p, '', name)
    return name.strip()

def normalize_name(name):
    name = clean_sentence(name)
    if not name: return ""
    # 全角半角統一、小文字化
    name = unicodedata.normalize('NFKC', name).lower()
    # 括弧内の注釈除去
    name = re.sub(r'[\(（\[［].*?[\)）\]］]', '', name)
    # 日英の法人格・特定キーワードを除去
    corp_suffixes = r'株式会社|有限会社|合同会社|（株）|\(株\)|連結子会社|持分法適用.*|inc\.|corp\.|ltd\.|co\.,ltd\.|corporation'
    name = re.sub(corp_suffixes, '', name)
    # 全ての記号と空白を完全に除去
    name = re.sub(r'[\s　!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>/?？・＆]', '', name)
    return name

def run():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ DB接続成功")

        # 1. 未紐付け社名を取得
        cursor.execute("""
            SELECT cr.child_company_name, c.address, cr.id
            FROM company_relations cr
            JOIN companies c ON cr.parent_company_id = c.id
            WHERE cr.child_company_id IS NULL
        """)
        target_data = {}
        for c_name, p_addr, rel_id in cursor.fetchall():
            core = normalize_name(c_name)
            if not core or len(core) < 2: continue
            pref = p_addr[:3] if p_addr else ""
            if core not in target_data:
                target_data[core] = {"parent_pref": pref, "names": set()}
            target_data[core]["names"].add(c_name)

        target_cores = set(target_data.keys())
        print(f"最終解析対象: {len(target_cores)} 件の『芯』")

        # 2. 全国マスターを走査 (6=日本語名, 24=英語名, 9=都道府県, 1=法人番号)
        found_map = {}
        reader = pd.read_csv(MASTER_CSV, encoding='cp932', chunksize=200000, header=None)
        
        print("全国マスターを最終解析中（日本語・英語 両対応）...")
        for i, chunk in enumerate(reader):
            # 日本語商号(6)と英語商号(24)の両方を正規化
            chunk['core_jp'] = chunk[6].apply(normalize_name)
            # 24番目の列が存在するか確認して処理
            if 24 in chunk.columns:
                chunk['core_en'] = chunk[24].apply(normalize_name)
            else:
                chunk['core_en'] = ""
            
            mask = chunk['core_jp'].isin(target_cores) | chunk['core_en'].isin(target_cores)
            
            if mask.any():
                for _, row in chunk[mask].iterrows():
                    h_num = str(row[1])
                    pref = str(row[9])
                    
                    # 日本語名が一致した場合
                    if row['core_jp'] in target_cores:
                        core = row['core_jp']
                        if core not in found_map: found_map[core] = []
                        if h_num not in [c['h_num'] for c in found_map[core]]:
                            found_map[core].append({"h_num": h_num, "pref": pref})
                    
                    # 英語名が一致した場合
                    if row['core_en'] in target_cores:
                        core = row['core_en']
                        if core not in found_map: found_map[core] = []
                        if h_num not in [c['h_num'] for c in found_map[core]]:
                            found_map[core].append({"h_num": h_num, "pref": pref})
            
            if (i+1) % 10 == 0: print(f"  - {(i+1)*20}万行 走査完了...")

        # 3. DB更新
        print("特定ロジックによる最終更新...")
        success_count = 0
        for core, candidates in found_map.items():
            p_pref = target_data[core]["parent_pref"]
            target_h_num = None
            
            if len(candidates) == 1:
                target_h_num = candidates[0]['h_num']
            elif len(candidates) > 1:
                matched = [c for c in candidates if p_pref in c['pref']]
                if len(matched) == 1:
                    target_h_num = matched[0]['h_num']
            
            if target_h_num:
                for original_name in target_data[core]["names"]:
                    cursor.execute("""
                        UPDATE company_relations 
                        SET child_corporate_number = %s 
                        WHERE child_company_name = %s AND child_company_id IS NULL
                    """, (target_h_num, original_name))
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
        print(f"✅ 完了！ リンク済み総数: {cursor.fetchone()[0]} 件 (今回救済: {success_count}社)")

    except Exception as e:
        print(f"❌ エラー: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    run()