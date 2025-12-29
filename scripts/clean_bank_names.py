#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
127_fixed_banks.csv/128_fixed_banks.csvの銀行名をクリーンにする
['株式会社北洋銀行'] → 株式会社北洋銀行
['8399 琉球銀行'] → 琉球銀行
"""

import csv
import re

def clean_bank_name(bank_str):
    """銀行名をクリーンにする"""
    if not bank_str:
        return ''
    
    # リスト形式の文字列を処理 ['銀行名'] → 銀行名
    s = bank_str.strip()
    s = s.replace('[', '').replace(']', '').replace("'", '').replace('"', '')
    
    # 複数銀行がある場合
    banks = []
    for bank in s.split(','):
        bank = bank.strip()
        if not bank:
            continue
        
        # 銀行コード（数字4桁）を除去
        bank = re.sub(r'^\d{4}\s+', '', bank)
        
        # 株式会社を除去（オプション - 銀行名のみにする場合）
        # bank = bank.replace('株式会社', '').strip()
        
        banks.append(bank)
    
    return ', '.join(banks)

def clean_banks_in_csv(input_file, output_file):
    """CSVのbanksフィールドをクリーンにする"""
    
    rows_processed = 0
    banks_cleaned = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        
        for row in reader:
            rows_processed += 1
            
            # banksフィールドをクリーン
            if row.get('banks'):
                original = row['banks']
                cleaned = clean_bank_name(original)
                if cleaned != original:
                    row['banks'] = cleaned
                    banks_cleaned += 1
            
            writer.writerow(row)
    
    print(f"✅ {input_file} → {output_file}")
    print(f"   処理行数: {rows_processed}")
    print(f"   銀行名クリーン: {banks_cleaned}")

if __name__ == '__main__':
    print("🧹 banks フィールドクリーニング")
    print("")
    
    # 127_fixed_banks.csv
    print("📄 127_fixed_banks.csv をクリーン中...")
    clean_banks_in_csv('./csv/127_fixed_banks.csv', './csv/127_final.csv')
    print("")
    
    # 128_fixed_banks.csv
    print("📄 128_fixed_banks.csv をクリーン中...")
    clean_banks_in_csv('./csv/128_fixed_banks.csv', './csv/128_final.csv')
    print("")
    
    print("🎉 完了！")
    print("   最終版: csv/127_final.csv, csv/128_final.csv")
    print("")
    print("📌 次のステップ:")
    print("   mv csv/127_final.csv csv/127.csv")
    print("   mv csv/128_final.csv csv/128.csv")

