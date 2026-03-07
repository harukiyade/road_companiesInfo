import psycopg2
import pandas as pd
import os

DB_CONFIG = {
    "host": "34.84.189.233", "database": "postgres", "user": "postgres",
    "password": "Legatus2000/", "port": "5432"
}
EDINET_LIST_FILE = "/Users/harumacmini/programming/info_companyDetail/edinet/EdinetcodeDlInfo 2.csv"
MASTER_CSV = "data/00_zenkoku_all_20260130.csv"

def run():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ DB接続成功")

        # 1. 未紐付けの子会社名を取得
        cursor.execute("SELECT DISTINCT child_company_name FROM company_relations WHERE child_corporate_number IS NULL")
        target_names = {row[0] for row in cursor.fetchall()}
        print(f"解析対象: {len(target_names)} 社")

        # 2. 全国マスターを走査して【社名: [(法人番号, 住所), ...]】の辞書を作る
        print(f"全国マスターを走査中 (住所情報を保持)...")
        found_map = {}
        # 1列目:法人番号, 12列目:商号, 13列目:都道府県, 14列目:市区町村
        reader = pd.read_csv(MASTER_CSV, encoding='cp932', chunksize=200000, header=None)
        
        for chunk in reader:
            mask = chunk[12].isin(target_names)
            if mask.any():
                for _, row in chunk[mask].iterrows():
                    name = row[12]
                    h_num = str(row[1])
                    address = f"{row[13]}{row[14]}" # 都道府県 + 市区町村
                    if name not in found_map:
                        found_map[name] = []
                    found_map[name].append({"h_num": h_num, "addr": address})

        # 3. 1社に絞り込む（同名が複数ある場合は住所で判定）
        print("同名企業の精査を開始...")
        final_updates = []
        for name, candidates in found_map.items():
            if len(candidates) == 1:
                # 全国に1社だけなら確定
                final_updates.append((candidates[0]['h_num'], name))
            else:
                # 同名が複数ある場合：
                # 本来はここで親会社の住所と比較しますが、まずは「全国に1社」のものを確実に埋めます。
                # 複数候補があるものは安全のため一旦スキップし、リストを出力します。
                print(f"  ⚠️ 同名重複スキップ: {name} ({len(candidates)}社見つかりました)")

        # 4. DB更新
        if final_updates:
            print(f"{len(final_updates)} 件の法人番号を書き込み中...")
            for h_num, name in final_updates:
                cursor.execute(
                    "UPDATE company_relations SET child_corporate_number = %s WHERE child_company_name = %s",
                    (h_num, name)
                )
            conn.commit()

        # 5. 最終紐付け（法人番号 -> companies.id）
        print("companiesテーブルと紐付け実行...")
        cursor.execute("""
            UPDATE company_relations cr
            SET child_company_id = c.id
            FROM companies c
            WHERE cr.child_corporate_number = c.corporate_number
              AND cr.child_company_id IS NULL;
        """)
        conn.commit()

        cursor.execute("SELECT COUNT(*) FROM company_relations WHERE child_company_id IS NOT NULL")
        total = cursor.fetchone()[0]
        print(f"✅ 完了！ リンク済み総数: {total} 件")

    except Exception as e:
        print(f"❌ エラー: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    run()