#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
127_expanded.csv/128_expanded.csvをクリーンアップ
- 不要なJSONフィールドを削除
- 変な記号（〒など）を除去
- 両ファイルを同じ構造に統一
"""

import csv
import json
import re
import sys

# CSVフィールドサイズ制限を拡張
csv.field_size_limit(sys.maxsize)

def clean_value(value):
    """値から変な記号を除去"""
    if not value:
        return ''
    
    s = str(value).strip()
    
    # 〒記号を除去
    s = re.sub(r'〒\s*', '', s)
    
    # 全角スペースを半角に
    s = s.replace('　', ' ')
    
    # 連続スペースを1つに
    s = re.sub(r'\s+', ' ', s)
    
    # その他の特殊記号を除去
    s = s.replace('◆', '')
    s = s.replace('※', '')
    s = s.replace('■', '')
    s = s.replace('●', '')
    
    return s.strip()

def normalize_financial_number(value_str):
    """財務数値を正規化（百万円単位 → 円単位）"""
    if not value_str or value_str in ['-', '']:
        return ''
    
    s = str(value_str).strip().replace(',', '')
    
    # 数値のみ抽出
    match = re.search(r'([\d.]+)', s)
    if not match:
        return ''
    
    num = float(match.group(1))
    
    # 百万円単位として処理
    num *= 1_000_000
    
    return str(int(num))

def extract_from_statements_json(statements_json_str):
    """statementsJsonから財務情報を抽出（百万円→円に変換）"""
    if not statements_json_str:
        return {}
    
    try:
        data = json.loads(statements_json_str)
        kv = data.get('kv', {})
        
        result = {}
        
        # 資産（百万円単位）
        if kv.get('資産合計'):
            result['totalAssets'] = normalize_financial_number(kv['資産合計'])
        
        # 負債（百万円単位）
        if kv.get('負債合計'):
            result['totalLiabilities'] = normalize_financial_number(kv['負債合計'])
        
        # 純資産（百万円単位）
        if kv.get('純資産'):
            result['netAssets'] = normalize_financial_number(kv['純資産'])
        
        # 売上高（営業収益）（百万円単位）
        if kv.get('売上高・営業収益'):
            result['revenueFromStatements'] = normalize_financial_number(kv['売上高・営業収益'])
        
        # 営業利益（百万円単位）
        if kv.get('営業利益'):
            result['operatingIncome'] = normalize_financial_number(kv['営業利益'])
        
        return result
    except:
        return {}

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
                    # パターン: "借入先金融機関名" の後の部分から銀行名を取得
                    parts = title.split()
                    for part in parts:
                        # 銀行、信用金庫、信用組合などのキーワードを含む
                        if any(kw in part for kw in ['銀行', '信金', '信組', 'バンク', '金庫']):
                            # 余計な文字を除去
                            part = part.replace('借入先金融機関名', '')
                            part = part.replace('借入金総合計', '')
                            part = part.replace('金融機関別借入金', '')
                            part = part.strip()
                            if part and len(part) > 2:
                                bank_names.add(part)
        
        if bank_names:
            return ', '.join(sorted(bank_names))
        
    except:
        pass
    
    return ''

def extract_from_bank_borrowings_json(bank_json_str, current_banks):
    """bankBorrowingsJsonから取引先銀行を抽出してbanksに統合"""
    extracted_banks = extract_bank_names_from_json(bank_json_str)
    
    if extracted_banks:
        if current_banks:
            # 既存の銀行名と統合（重複除去）
            all_banks = set()
            for bank in current_banks.split(','):
                bank = bank.strip()
                if bank:
                    all_banks.add(bank)
            for bank in extracted_banks.split(','):
                bank = bank.strip()
                if bank:
                    all_banks.add(bank)
            return ', '.join(sorted(all_banks))
        else:
            return extracted_banks
    
    return current_banks

def cleanup_csv(input_file, output_file):
    """CSVをクリーンアップ + statementsJson展開"""
    
    # 必要なフィールド（statementsJsonから展開した財務情報を追加）
    keep_fieldnames = [
        'id', 'name', 'nameEn', 'corporateNumber', 'nikkeiCode', 'prefecture',
        'address', 'industry', 'capitalStock', 'revenue', 'latestProfit',
        'employeeCount', 'issuedShares', 'established', 'fiscalMonth', 'listing',
        'representativeName', 'businessDescriptions', 'companyUrl', 'contactUrl',
        'detailUrl', 'banks', 'affiliations', 'overview', 'history',
        # statementsJsonから展開
        'totalAssets', 'totalLiabilities', 'netAssets', 'revenueFromStatements', 'operatingIncome'
    ]
    
    rows_processed = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=keep_fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        for row in reader:
            rows_processed += 1
            
            # 各値をクリーンアップ
            cleaned_row = {}
            for field in keep_fieldnames:
                cleaned_row[field] = clean_value(row.get(field, ''))
            
            # statementsJsonから財務情報を抽出
            statements_data = extract_from_statements_json(row.get('statementsJson', ''))
            for key, value in statements_data.items():
                cleaned_row[key] = clean_value(value)
            
            # bankBorrowingsJsonをbanksに統合
            if row.get('bankBorrowingsJson'):
                cleaned_row['banks'] = extract_from_bank_borrowings_json(
                    row.get('bankBorrowingsJson', ''),
                    cleaned_row.get('banks', '')
                )
            
            writer.writerow(cleaned_row)
    
    print(f"✅ {input_file} → {output_file}")
    print(f"   処理行数: {rows_processed}")
    print(f"   保持フィールド: {len(keep_fieldnames)}")

if __name__ == '__main__':
    print("🧹 127.csv/128.csv クリーンアップスクリプト")
    print("   - 不要なJSONフィールド削除")
    print("   - 変な記号除去")
    print("")
    
    # 127_expanded.csv
    print("📄 127_expanded.csv をクリーンアップ中...")
    cleanup_csv('./csv/127_expanded.csv', './csv/127_clean.csv')
    print("")
    
    # 128_expanded.csv
    print("📄 128_expanded.csv をクリーンアップ中...")
    cleanup_csv('./csv/128_expanded.csv', './csv/128_clean.csv')
    print("")
    
    print("🎉 完了！")
    print("   クリーン後: csv/127_clean.csv, csv/128_clean.csv")
    print("")
    print("📌 次のステップ:")
    print("   mv csv/127_clean.csv csv/127.csv")
    print("   mv csv/128_clean.csv csv/128.csv")

