import psycopg2
import pandas as pd
import re
import unicodedata

DB_CONFIG = {
    "host": "34.84.189.233", "database": "postgres", "user": "postgres",
    "password": "Legatus2000/", "port": "5432"
}
MASTER_CSV = "data/00_zenkoku_all_20260130.csv"

def normalize_name(name):
    if not name or pd.isna(name): return ""
    name = str(name)
    # 1. 全角を半角に、大文字を小文字に（英数字・記号の統一）
    name = unicodedata.normalize('NFKC', name).lower()
    # 2. 末尾の（）内注釈や【】などを除去
    name = re.sub(r'[\(（\[［].*?[\)）\]］]$', '', name)
    # 3. 法人格や特定のキーワードを除去
    name = re.sub(r'株式会社|有限会社|合同会社|代表取締役|（株）|\(株\)|連結子会社|持分法適用会社|持分法適用関連会社', '', name)
    # 4. 記号と空白をすべて除去
    name = re.sub(r'[\s　!@#$%^&*()_+\-=\[\]{};\':"\\|,.<>/?？・＆]', '', name)
    return name

def run():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ DB接続成功")

        # 1. 未紐付け社名と親の所在地を取得
        cursor.execute("""
            SELECT cr.child_company_name, c.address, cr.id
            FROM company_relations cr
            JOIN companies c ON cr.parent_company_id = c.id
            WHERE cr.child_company_id IS NULL
        """)
        target_data = {}
        for c_name, p_addr, rel_id in cursor.fetchall():
            core = normalize_name(c_name)
            if not core: continue
            pref = p_addr[:3] if p_addr else ""
            if core not in target_data:
                target_data[core] = {"parent_pref": pref, "names": set()}
            target_data[core]["names"].add(c_name)

        target_cores = set(target_data.keys())
        print(f"救済対象（芯の数）: {len(target_cores)} 件")

        # 2. 全国マスターを走査 (6=商号, 9=都道府県, 1=法人番号)
        found_map = {}
        reader = pd.read_csv(MASTER_CSV, encoding='cp932', chunksize=200000, header=None)
        
        print("全国マスターを解析中（高度な正規化マッチング）...")
        for i, chunk in enumerate(reader):
            # 6番目の列(商号)を正規化
            chunk['core'] = chunk[6].apply(normalize_name)
            mask = chunk['core'].isin(target_cores)
            
            if mask.any():
                for _, row in chunk[mask].iterrows():
                    core, h_num, pref = row['core'], str(row[1]), str(row[9])
                    if core not in found_map: found_map[core] = []
                    # 重複登録防止
                    if h_num not in [c['h_num'] for c in found_map[core]]:
                        found_map[core].append({"h_num": h_num, "pref": pref})
            
            if (i+1) % 10 == 0: print(f"  - {(i+1)*20}万行 走査完了...")

        # 3. 精査と更新
        print("特定ロジック実行中...")
        success_count = 0
        for core, candidates in found_map.items():
            p_pref = target_data[core]["parent_pref"]
            target_h_num = None

            # ロジックA: 全国で1社のみなら、場所に関係なく確定（名前が短すぎる場合を除く）
            if len(candidates) == 1 and len(core) >= 2:
                target_h_num = candidates[0]['h_num']
            # ロジックB: 複数ある場合、親会社と同じ都道府県のものを優先
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
        print("companiesテーブルと最終紐付け...")
        cursor.execute("""
            UPDATE company_relations cr
            SET child_company_id = c.id
            FROM companies c
            WHERE cr.child_corporate_number = c.corporate_number AND cr.child_company_id IS NULL;
        """)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM company_relations WHERE child_company_id IS NOT NULL")
        print(f"✅ 完了！ リンク済み総数: {cursor.fetchone()[0]} 件 (今回新規に救済: {success_count}社)")

    except Exception as e:
        print(f"❌ エラー: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    run()
