#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import csv
import argparse
import psycopg2
from pathlib import Path
from psycopg2.extras import RealDictCursor

TARGET_RECORDS = [
    {"path": "fixed_csv_3/unit_million/4.csv", "row_idx": 1393, "name": "栄伸開発株式会社"},
    {"path": "fixed_csv_3/unit_million/import_firstTime_107.csv", "row_idx": 6977, "name": "山都酒造株式会社"},
    {"path": "fixed_csv_3/unit_million/import_firstTime_107.csv", "row_idx": 6977, "name": "山都酒造株式会社"},
    {"path": "fixed_csv_3/unit_million/import_firstTime_111.csv", "row_idx": 1977, "name": "山都酒造株式会社"},
    {"path": "fixed_csv_3/unit_million/import_firstTime_111.csv", "row_idx": 1977, "name": "山都酒造株式会社"},
    {"path": "fixed_csv_3/unit_million/import_firstTime_4.csv", "row_idx": 712, "name": "株式会社桐井製作所"},
    {"path": "fixed_csv_3/unit_yen/19.csv", "row_idx": 207, "name": "株式会社インフィニティエージェント"},
    {"path": "fixed_csv_3/unit_yen/30.csv", "row_idx": 169, "name": "株式会社インフィニティエージェント"},
    {"path": "fixed_csv_3/unit_yen/import_firstTime_3.csv", "row_idx": 52, "name": "新英金属株式会社"},
    {"path": "fixed_csv_3/unit_yen/import_firstTime_40.csv", "row_idx": 52, "name": "新英金属株式会社"},
    {"path": "fixed_csv_3/unit_yen/import_firstTime_40.csv", "row_idx": 161, "name": "吉野電化工業株式会社"},
    {"path": "fixed_csv_3/unit_yen/import_firstTime_5.csv", "row_idx": 617, "name": "株式会社桐井製作所"},
    {"path": "fixed_csv_3/unit_million/111.fixed.csv", "row_idx": 1977, "name": "山都酒造株式会社"}
]

MAPPING = {
    'name': '会社名',
    'prefecture': '都道府県',
    'representative_name': '代表者名',
    # 'corporate_number': '法人番号',  <- DBの完璧な値を守るため、更新対象から完全に除外
    'url': 'URL',
    'postal_code': '郵便番号',
    'address': '住所',
    'established': '設立',
    'phone_number': '電話番号(窓口)',
    'representative_postal_code': '代表者郵便番号',
    'representative_address': '代表者住所',
    'representative_birthday': '代表者誕生日',
    'capital': '資本金',
    'latest_fiscal_year_month': '直近決算年月',
    'latest_revenue': '直近売上',
    'latest_profit': '直近利益',
    'description': '説明',
    'overview': '概要',
    'suppliers': '仕入れ先',
    'clients': '取引先',
    'banks': '取引先銀行',
    'shareholders': '株主',
    'employee_count': '社員数',
    'office_count': 'オフィス数',
    'factory_count': '工場数',
    'store_count': '店舗数'
}

def clean_int(val, factor=1):
    if not val or str(val).strip() == '': return None
    try:
        f_val = float(str(val).replace(',', '').strip())
        return int(f_val * factor)
    except (ValueError, TypeError):
        return None

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        sslmode="disable"
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true', help='実行フラグ')
    args = parser.parse_args()

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    for target in TARGET_RECORDS:
        path = Path(target['path'])
        if not path.exists():
            print(f"Skipping: {path} (Not Found)")
            continue

        is_million = 'unit_million' in str(path)
        factor = 1000000 if is_million else 1

        with open(path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            found_row = None
            for i, row in enumerate(reader, start=2):
                if i == target['row_idx']:
                    found_row = row
                    break
            
            if not found_row:
                continue

            data = {}
            for db_col, csv_col in MAPPING.items():
                val = found_row.get(csv_col)
                if db_col in ['capital', 'latest_revenue', 'latest_profit']:
                    data[db_col] = clean_int(val, factor)
                elif db_col in ['employee_count', 'office_count', 'factory_count', 'store_count']:
                    data[db_col] = clean_int(val, 1)
                elif db_col in ['suppliers', 'clients', 'banks', 'shareholders']:
                    if val and str(val).strip():
                        cleaned_str = str(val).replace('，', ',').replace('、', ',')
                        data[db_col] = [x.strip() for x in cleaned_str.split(',') if x.strip()]
                    else:
                        data[db_col] = []
                else:
                    data[db_col] = val if val else None

            # 検索キーは「会社名」と「都道府県」を絶対使用
            where_clause = "name = %s AND prefecture = %s"
            where_val = [target['name'], found_row.get('都道府県')]

            cols = list(data.keys())
            vals = [data[c] for c in cols]

            update_stmt = ", ".join([f'"{c}" = %s' for c in cols])
            sql = f"UPDATE companies SET {update_stmt} WHERE {where_clause}"

            if args.execute:
                cur.execute(sql, vals + where_val)
                if cur.rowcount == 0:
                    print(f"  -> Record not found in DB for Key: {target['name']} ({found_row.get('都道府県')}).")
                else:
                    print(f"Success: Updated {target['name']} (Row {target['row_idx']})")

    if args.execute:
        conn.commit()
        print("\nAll updates committed successfully.")
    
    conn.close()

if __name__ == "__main__":
    main()