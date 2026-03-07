#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/match_by_phone.py
電話番号をキーにして、UUIDレコードとマスターレコード（法人番号あり）をマッチングするスクリプト。
"""

import os
import re
import psycopg2

DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

def normalize_phone(phone):
    """電話番号から数字以外（ハイフンやスペース）を取り除いて正規化"""
    if not phone:
        return ""
    return re.sub(r'\D', '', str(phone))

def main():
    print("=== 電話番号ベースの名寄せマッチング (テストモード) ===")
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, user=DB_USER,
            password=DB_PASSWORD, dbname=DB_NAME, sslmode=DB_SSLMODE
        )
        cur = conn.cursor()
        
        # 1. マスターレコード（法人番号あり、数値ID）の電話番号を取得
        print("マスターレコードを読み込み中...")
        cur.execute("""
            SELECT id, name, corporate_number, phone_number 
            FROM companies 
            WHERE corporate_number IS NOT NULL AND corporate_number != ''
              AND id ~ '^[0-9]+$'
              AND phone_number IS NOT NULL AND phone_number != '';
        """)
        masters = cur.fetchall()
        
        # マスターの電話番号を正規化して辞書化
        master_phone_dict = {}
        for mid, mname, corp_num, phone in masters:
            norm_phone = normalize_phone(phone)
            if norm_phone:
                if norm_phone not in master_phone_dict:
                    master_phone_dict[norm_phone] = []
                master_phone_dict[norm_phone].append({
                    'id': mid, 'name': mname, 'corporate_number': corp_num
                })

        # 2. 対象となるUUIDレコード（法人番号なし）を取得
        print("対象のUUIDレコードを読み込み中...")
        cur.execute("""
            SELECT id, name, phone_number 
            FROM companies 
            WHERE (corporate_number IS NULL OR corporate_number = '')
              AND (id LIKE '%-%' OR id !~ '^[0-9]+$')
              AND phone_number IS NOT NULL AND phone_number != '';
        """)
        uuids = cur.fetchall()
        
        match_count = 0
        multiple_match_count = 0
        
        print(f"\n電話番号を持つUUIDレコード: {len(uuids)} 件")
        print("-" * 50)
        
        # 3. マッチング処理
        for uid, uname, uphone in uuids:
            norm_phone = normalize_phone(uphone)
            if not norm_phone:
                continue
                
            matched_masters = master_phone_dict.get(norm_phone)
            
            if matched_masters:
                if len(matched_masters) == 1:
                    match_count += 1
                    # 最初の5件だけサンプル表示
                    if match_count <= 5:
                        m = matched_masters[0]
                        print(f"[マッチ成功] {uname} (UUID)")
                        print(f"  -> マスター: {m['name']} (法人番号: {m['corporate_number']})")
                        print(f"  -> キー(電話番号): {uphone}")
                        print()
                else:
                    # 同じ電話番号を持つマスターが複数いる（支店違いなど）
                    multiple_match_count += 1

        print("-" * 50)
        print(f"【結果】")
        print(f"・電話番号で確実にマージできそうな件数: {match_count} 件")
        print(f"・複数のマスターに該当して保留する件数: {multiple_match_count} 件")
        print(f"・電話番号が一致しなかった件数        : {len(uuids) - match_count - multiple_match_count} 件")
        print("-" * 50)
        
        if match_count > 0:
            print("これらのデータは、電話番号を信頼して安全に統合（マージ＆UUID削除）できそうです。")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    main()