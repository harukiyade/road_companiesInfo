#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/verify_actis_data.py
「アクティスジャパン株式会社」のデータがCSVの内容通りにDBに入っているか検証する。
"""

import os
import psycopg2
import json
from datetime import date, datetime

# --- DB接続設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

# --- CSVから読み取った期待値 (Expected Values) ---
EXPECTED = {
    "name": "アクティスジャパン株式会社",
    "prefecture": "愛知県",
    "representative_name": "菅井泰伸",
    "corporate_number": "7180000000000",
    "address": "愛知県名古屋市中区新栄１－７－７名古屋センターステージ",
    "phone_number": "052-241-4477",
    "url": "http://www.acts-group.co.jp/index.html",
    # DBのカラム名は推測を含みます
    "overview": "システム開発や「Pepper」を使用したロボットアプリの開発を行う会社",
    "business_descriptions": "既存先を中心に安定した受注を得ていると聞く。近年は人員数が増加傾向にあり、売上増加とともに費用面での増加も大きい。",
    "suppliers": "カタナコーポレーション，ビーユーシージャパン",
    "clients": "村田機械（１０％），三菱電機インフォメーションシステムズ，日立システムズ，富士通，ＳＣＳＫ，日本システム技術",
    "banks": "三菱ＵＦＪ（大津町）愛知（東郊通）商工組合中央金庫（名古屋）日本政策金融公庫名古屋（名古屋駅前）十六（今池）",
    "executives": "（取）安斎義彦，西原邦明，菅井孝昇",
    "shareholders": "菅井泰伸（３９％），従業員持株会，菅井貴美子，安斎義彦",
    "employee_count": "185",
    "office_count": "3",
    "factory_count": "0",
    "store_count": "0",
    "capital": "95000",
    "latest_revenue": "1681000",  # DBカラム名が revenue かも？
    "latest_profit": "22917",     # DBカラム名が profit かも？
    "representative_address": "愛知県名古屋市東区泉１－２２－１ザ・センチュリーステイツ１５０５",
    "representative_birth_date": "1949-08-24 00:00:00"
}

# カラム名のゆらぎ対応用マッピング (DBカラム名 -> 期待値キー)
# ※ 実際のDBカラム名に合わせて自動で照合します
COLUMN_MAPPING = {
    "company_url": "url",
    "homepage_url": "url",
    "revenue": "latest_revenue",
    "profit": "latest_profit",
    "employees": "employee_count",
    "representative_home_address": "representative_address",
    "main_banks": "banks",
    "major_shareholders": "shareholders",
    "officers": "executives",
    # その他のカラムはそのままの名前でチェック
}

def normalize(val):
    if val is None:
        return ""
    return str(val).strip()

def main():
    print("=== データ検証レポート: アクティスジャパン株式会社 ===")
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            sslmode=DB_SSLMODE,
        )
        cur = conn.cursor()
        
        # データ取得
        cur.execute("SELECT * FROM companies WHERE name = %s", (EXPECTED["name"],))
        row = cur.fetchone()
        
        if not row:
            print("【エラー】DBに「アクティスジャパン株式会社」が見つかりません。")
            return

        # カラム名取得
        colnames = [desc[0] for desc in cur.description]
        row_dict = dict(zip(colnames, row))
        
        # 検証実行
        match_count = 0
        mismatch_count = 0
        missing_col_count = 0

        print(f"\nDBレコードID: {row_dict.get('id', '不明')}")
        print("-" * 60)
        print(f"{'項目':<20} | {'判定':<6} | {'DBの値 (抜粋)':<30} | {'CSVの期待値'}")
        print("-" * 60)

        # EXPECTEDにあるキーについてチェック
        for key, expected_val in EXPECTED.items():
            # DBカラム名を探す
            db_col = key
            if key not in row_dict:
                # マッピングを確認
                found = False
                for db_k, map_k in COLUMN_MAPPING.items():
                    if map_k == key and db_k in row_dict:
                        db_col = db_k
                        found = True
                        break
                if not found:
                    # カラム自体がDBに見つからない場合
                    # よくあるカラム名のパターンで再検索 (例: suppliers -> supplier?)
                    if key in row_dict: found = True
                    
            if db_col not in row_dict:
                 print(f"{key:<20} | 不明   | (DBにカラムなし)               | {expected_val}")
                 missing_col_count += 1
                 continue

            db_val = normalize(row_dict[db_col])
            exp_val = normalize(expected_val)
            
            # 比較（部分一致も含めて柔軟に判定）
            is_match = False
            if db_val == exp_val:
                is_match = True
            elif exp_val in db_val: # DBの方が情報量が多い場合もOKとする（追記されている場合など）
                is_match = True
            elif db_val in exp_val and len(db_val) > 0: # CSVの方が詳しい場合（未更新？）
                is_match = False # 厳密には不一致
            
            # 日付などのフォーマット差分を吸収 (簡易)
            if "00:00:00" in exp_val and db_val == exp_val.split(" ")[0]:
                is_match = True

            status = "OK" if is_match else "NG"
            if is_match:
                match_count += 1
            else:
                mismatch_count += 1
            
            # 表示用に値を短縮
            disp_db = (db_val[:28] + "..") if len(db_val) > 30 else db_val
            disp_exp = (exp_val[:28] + "..") if len(exp_val) > 30 else exp_val
            
            print(f"{key:<20} | {status:<6} | {disp_db:<30} | {disp_exp}")

        print("-" * 60)
        print(f"検証結果: 一致 {match_count} 件 / 不一致 {mismatch_count} 件 / カラム不明 {missing_col_count} 件")
        
        if mismatch_count > 0:
            print("\n※ NGの項目は、インポート漏れか、DBカラム名のマッピング違いの可能性があります。")
            print("   特に `overview` や `business_descriptions` が空の場合はインポートスクリプトの確認が必要です。")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()