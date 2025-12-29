#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
127.csv/128.csvを統一修正
1. ヘッダーを128.csvに統一（idフィールドを除外）
2. banksフィールドから銀行名のみを抽出、複数ある場合は・で連結
3. contactUrlの https://valuesearch.nikkei で始まるURLを削除
"""

import csv
import json
import re
import sys
import ast

csv.field_size_limit(sys.maxsize)

def extract_bank_names(banks_str):
    """banksフィールドから銀行名のみを抽出"""
    if not banks_str or banks_str.strip() == '':
        return ''
    
    banks_str = banks_str.strip()
    
    # JSON/dict形式の場合
    if banks_str.startswith('{'):
        try:
            # まずJSONとして試す
            try:
                data = json.loads(banks_str)
            except:
                # JSONでダメならPythonのdict形式として解析
                data = ast.literal_eval(banks_str)
            bank_names = []  # setではなくlistで順序を保持
            
            # tables配列からtitleを解析
            if isinstance(data, dict) and 'tables' in data:
                for table in data['tables']:
                    if 'title' in table:
                        title = table['title']
                        # 不要な文字列を除去
                        title = re.sub(r'借入先金融機関名', '', title)
                        title = re.sub(r'借入金総合計', '', title)
                        title = re.sub(r'金融機関別借入金', '', title)
                        title = re.sub(r'不明分', '', title)
                        title = title.strip()
                        
                        # 残った文字列から銀行名を抽出
                        # スペースで分割
                        words = re.split(r'\s+', title)
                        for word in words:
                            # 銀行名のキーワードを含むか
                            if any(kw in word for kw in ['銀行', '信金', '信組', 'バンク', '金庫', '農協', 'JA', '信用金庫', '信用組合']):
                                # 銀行コード（数字4桁）を除去
                                word = re.sub(r'^\d{4}\s*', '', word)
                                word = word.strip()
                                
                                # 最低3文字以上で、重複しない場合のみ追加
                                if word and len(word) >= 3 and word not in bank_names:
                                    bank_names.append(word)
            
            if bank_names:
                # ・で連結
                return '・'.join(bank_names)
        except Exception as e:
            # JSON パースエラーの場合は空にする
            return ''
    
    # リスト形式の文字列の場合 ['銀行名']
    if banks_str.startswith('['):
        banks_str = banks_str.replace('[', '').replace(']', '').replace("'", '').replace('"', '')
        # 複数銀行がある場合
        banks = []
        for bank in banks_str.split(','):
            bank = bank.strip()
            if not bank:
                continue
            # 銀行コード除去
            bank = re.sub(r'^\d{4}\s+', '', bank)
            bank = bank.strip()
            if bank and len(bank) >= 3:
                banks.append(bank)
        return '・'.join(banks)
    
    # その他の場合（プレーンテキスト）
    # 銀行コード除去
    banks_str = re.sub(r'^\d{4}\s+', '', banks_str)
    return banks_str.strip()

def clean_contact_url(url_str):
    """contactUrlから valuesearch.nikkei のURLを削除"""
    if not url_str:
        return ''
    
    url = url_str.strip()
    
    # https://valuesearch.nikkei で始まる場合は削除
    if url.startswith('https://valuesearch.nikkei'):
        return ''
    
    return url

def unify_csv(input_file, output_file, target_headers):
    """CSVを統一形式に変換"""
    
    rows_processed = 0
    banks_cleaned = 0
    urls_cleaned = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=target_headers, extrasaction='ignore')
        writer.writeheader()
        
        for row in reader:
            rows_processed += 1
            
            new_row = {}
            
            # 全フィールドをコピー（idフィールドは除外）
            for field in target_headers:
                new_row[field] = row.get(field, '')
            
            # banksフィールドをクリーン
            if new_row.get('banks'):
                original = new_row['banks']
                cleaned = extract_bank_names(original)
                # JSON形式が残っている場合は空にする
                if cleaned.startswith('{'):
                    cleaned = ''
                if cleaned != original:
                    new_row['banks'] = cleaned
                    banks_cleaned += 1
            
            # contactUrlをクリーン
            if new_row.get('contactUrl'):
                original = new_row['contactUrl']
                cleaned = clean_contact_url(original)
                if cleaned != original:
                    new_row['contactUrl'] = cleaned
                    urls_cleaned += 1
            
            writer.writerow(new_row)
    
    print(f"✅ {input_file} → {output_file}")
    print(f"   処理行数: {rows_processed}")
    print(f"   banks修正: {banks_cleaned}")
    print(f"   contactUrl削除: {urls_cleaned}")

if __name__ == '__main__':
    print("🔧 127.csv/128.csv 統一修正スクリプト")
    print("   1. ヘッダーを128.csv形式に統一（idなし）")
    print("   2. banksを銀行名のみに（・で連結）")
    print("   3. contactUrlの valuesearch.nikkei を削除")
    print("")
    
    # 128.csvのヘッダーを基準とする（idなし）
    target_headers = [
        'name', 'nameEn', 'corporateNumber', 'prefecture', 'address', 'industry',
        'capitalStock', 'revenue', 'latestProfit', 'employeeCount', 'issuedShares',
        'established', 'fiscalMonth', 'listing', 'representativeName', 'businessDescriptions',
        'companyUrl', 'contactUrl', 'banks', 'affiliations', 'overview', 'history',
        'totalAssets', 'totalLiabilities', 'netAssets', 'revenueFromStatements', 'operatingIncome'
    ]
    
    # 127.csv
    print("📄 127.csv を統一形式に変換中...")
    unify_csv('./csv/127.csv', './csv/127_unified.csv', target_headers)
    print("")
    
    # 128.csv
    print("📄 128.csv を統一形式に変換中...")
    unify_csv('./csv/128.csv', './csv/128_unified.csv', target_headers)
    print("")
    
    print("🎉 完了！")
    print("   統一後: csv/127_unified.csv, csv/128_unified.csv")
    print("")
    print("📌 次のステップ:")
    print("   mv csv/127_unified.csv csv/127.csv")
    print("   mv csv/128_unified.csv csv/128.csv")

