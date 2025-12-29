#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
127.csv/128.csvのbanksフィールドから銀行名のみを抽出
"""

import csv
import json
import sys

csv.field_size_limit(sys.maxsize)

def extract_bank_names_from_json(bank_json_str):
    """banksのJSON形式から銀行名のみを抽出"""
    if not bank_json_str:
        return ''
    
    # 既にJSON形式でない（プレーンテキスト）場合はそのまま返す
    if not bank_json_str.strip().startswith('{'):
        return bank_json_str
    
    try:
        data = json.loads(bank_json_str)
        bank_names = set()
        
        # tables配列から銀行名を抽出
        if isinstance(data, dict) and 'tables' in data:
            for table in data['tables']:
                if 'title' in table:
                    title = table['title']
                    # "借入先金融機関名 借入金総合計 北海道銀行" から銀行名を抽出
                    # 不要な文字列を除去
                    title = title.replace('借入先金融機関名', '')
                    title = title.replace('借入金総合計', '')
                    title = title.replace('金融機関別借入金', '')
                    title = title.replace('不明分', '')
                    
                    # 残った部分をスペースで分割
                    parts = title.split()
                    for part in parts:
                        # 銀行、信用金庫、信用組合などのキーワードを含む
                        if any(kw in part for kw in ['銀行', '信金', '信組', 'バンク', '金庫', '農協', 'JA']):
                            part = part.strip()
                            if part and len(part) > 2:
                                bank_names.add(part)
        
        if bank_names:
            return ', '.join(sorted(bank_names))
        
    except:
        pass
    
    return ''

def fix_banks_field(input_file, output_file):
    """banksフィールドを銀行名のみに修正"""
    
    rows_processed = 0
    banks_extracted = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for row in reader:
            rows_processed += 1
            
            # banksフィールドを処理
            if row.get('banks'):
                extracted = extract_bank_names_from_json(row['banks'])
                if extracted:
                    row['banks'] = extracted
                    banks_extracted += 1
                else:
                    row['banks'] = ''
            
            writer.writerow(row)
    
    print(f"✅ {input_file} → {output_file}")
    print(f"   処理行数: {rows_processed}")
    print(f"   銀行名抽出成功: {banks_extracted}")

if __name__ == '__main__':
    print("🏦 127.csv/128.csv の banks フィールド修正")
    print("   JSON形式 → 銀行名のみ")
    print("")
    
    # 127.csv
    print("📄 127.csv を処理中...")
    fix_banks_field('./csv/127.csv', './csv/127_fixed_banks.csv')
    print("")
    
    # 128.csv
    print("📄 128.csv を処理中...")
    fix_banks_field('./csv/128.csv', './csv/128_fixed_banks.csv')
    print("")
    
    print("🎉 完了！")
    print("   修正後: csv/127_fixed_banks.csv, csv/128_fixed_banks.csv")
    print("")
    print("📌 次のステップ:")
    print("   mv csv/127_fixed_banks.csv csv/127.csv")
    print("   mv csv/128_fixed_banks.csv csv/128.csv")

