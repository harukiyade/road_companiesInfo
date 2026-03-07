import psycopg2
import pandas as pd
import os

# DB接続設定
DB_CONFIG = {
    "host": "34.84.189.233",
    "database": "postgres",
    "user": "postgres",
    "password": "Legatus2000/",
    "port": "5432"
}

# 全国法人マスターCSVのパス
MASTER_CSV = "data/00_zenkoku_all_20260130.csv"

def run():
    conn = None
    try:
        # 1. DB接続
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ DB接続成功")

        # 2. child_company_id が空の未紐付け社名リストを取得
        cursor.execute("""
            SELECT DISTINCT child_company_name 
            FROM company_relations 
            WHERE child_company_id IS NULL
        """)
        target_names = {row[0] for row in cursor.fetchall()}
        print(f"解析対象の未紐付け社名: {len(target_names)} 件")

        if not target_names:
            print("再名寄せが必要なデータはありません。")
            return

        # 3. 全国法人マスターを走査 (同名チェック付き)
        print(f"全国マスターを解析中 (cp932)...")
        
        # 社名ごとに見つかった法人番号をリストで保持 {社名: [法人番号1, 法人番号2...]}
        name_to_hnums = {name: [] for name in target_names}
        
        # メモリ節約のため10万行ずつ読み込み
        # 国税庁形式想定: 0列目=法人番号, 11列目=商号
        reader = pd.read_csv(MASTER_CSV, encoding='cp932', chunksize=100000, header=None)

        for i, chunk in enumerate(reader):
            # 対象の社名が含まれる行を抽出
            mask = chunk[11].isin(target_names)
            if mask.any():
                sub_df = chunk[mask]
                for _, row in sub_df.iterrows():
                    name = row[11]
                    h_num = str(row[0])
                    # 法人番号リストに追加（重複は避ける）
                    if h_num not in name_to_hnums[name]:
                        name_to_hnums[name].append(h_num)

            if (i + 1) % 10 == 0:
                print(f"  - { (i + 1) * 10 }万行 走査中...")

        # 4. 1社に絞り込めたものだけをDBに反映
        print("解析完了。DBへの紐付けを開始します...")
        
        success_count = 0
        collision_count = 0 # 同名で絞り込めなかった数

        for name, h_nums in name_to_hnums.items():
            if len(h_nums) == 1:
                # 全国で1社のみ：自信を持って紐付け
                h_num = h_nums[0]
                
                # companiesテーブル側のIDを取得
                cursor.execute("SELECT id FROM companies WHERE corporate_number = %s LIMIT 1", (h_num,))
                res = cursor.fetchone()
                if res:
                    c_id = str(res[0])
                    cursor.execute("""
                        UPDATE company_relations 
                        SET child_company_id = %s, child_corporate_number = %s 
                        WHERE child_company_name = %s AND child_company_id IS NULL
                    """, (c_id, h_num, name))
                    success_count += 1
            elif len(h_nums) > 1:
                # 全国に同名が複数：住所等の証拠がないため、誤爆防止でスキップ（テキスト表示維持）
                collision_count += 1

        conn.commit()
        print("-" * 30)
        print(f"✅ 処理完了レポート")
        print(f"  - 新たにリンク化成功: {success_count} 件")
        print(f"  - 同名のため特定見送り: {collision_count} 件")
        print(f"  - マスター未登録/不一致: {len(target_names) - success_count - collision_count} 件")
        print("-" * 30)

    except Exception as e:
        print(f"❌ エラー: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    run()