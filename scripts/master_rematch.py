import psycopg2
import pandas as pd
import os

DB_CONFIG = {
    "host": "34.84.189.233", "database": "postgres", "user": "postgres",
    "password": "Legatus2000/", "port": "5432"
}

# パス設定
MASTER_CSV = "data/00_zenkoku_all_20260130.csv"

def run():
    conn = None
    try:
        # 1. DB接続
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print("✅ DB接続成功")

        # 2. 現在 child_company_id が空のデータを辞書にロード
        # (名前をキーにして、重複を避けるためにセットで管理)
        cursor.execute("SELECT child_company_name FROM company_relations WHERE child_company_id IS NULL")
        target_names = {row[0] for row in cursor.fetchall()}
        print(f"再名寄せ対象の社名数: {len(target_names)} 件")

        if not target_names:
            print("対象が存在しません。")
            return

        # 3. 全国法人マスターを分割読み込み (Shift-JIS対応)
        print(f"全国法人マスターを解析中 (encoding='cp932')...")
        
        # 見つかった「社名と法人番号」のペアを格納する辞書
        found_map = {}

        # チャンクサイズは10万行ずつ
        chunk_size = 100000
        # headerがない場合や列名が違う場合を考慮し、列番号で指定できるように読み込み
        reader = pd.read_csv(MASTER_CSV, encoding='cp932', chunksize=chunk_size, header=None)

        for i, chunk in enumerate(reader):
            # 全国マスターCSVの標準的な形式（国税庁形式）を想定
            # 1列目(0): 法人番号, 12列目(11): 商号
            # ※もし列が違う場合はここを調整してください
            sub_df = chunk[chunk[11].isin(target_names)]
            
            for _, row in sub_df.iterrows():
                h_num = str(row[0])
                name = row[11]
                # 同名が複数ある場合は、上書きせずにリスト化して後で検証も可能ですが、
                # まずは最初に見つかった法人番号を候補に入れます
                if name not in found_map:
                    found_map[name] = h_num

            if (i + 1) % 10 == 0:
                print(f"  - { (i + 1) * chunk_size / 10000 }万行 走査完了...")

        # 4. 特定できた法人番号をDBに反映
        print(f"解析完了。{len(found_map)} 件の社名に法人番号がヒットしました。")
        
        success_count = 0
        for name, h_num in found_map.items():
            # companiesテーブルからIDを取得
            cursor.execute("SELECT id FROM companies WHERE corporate_number = %s LIMIT 1", (h_num,))
            res = cursor.fetchone()
            if res:
                new_id = str(res[0])
                cursor.execute(
                    "UPDATE company_relations SET child_company_id = %s WHERE child_company_name = %s AND child_company_id IS NULL",
                    (new_id, name)
                )
                success_count += 1

        conn.commit()
        print(f"✅ 更新完了！ 新たに {success_count} 件を companies テーブルと紐付けました。")

    except UnicodeDecodeError:
        print("❌ 文字コードエラー: cp932 (Shift-JIS) でも読み込めませんでした。utf-8-sig を試してください。")
    except Exception as e:
        print(f"❌ エラー: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    run()