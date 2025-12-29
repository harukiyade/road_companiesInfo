#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
130.csv/131.csv（タイプI）を修正するスクリプト
rawTextフィールドから情報を抽出して適切なカラムに分離
"""

import csv
import re
import sys

def extract_from_rawtext(rawtext):
    """rawTextフィールドから情報を抽出"""
    if not rawtext:
        return {}
    
    data = {}
    
    # 代表者名
    match = re.search(r'代表者名\s+([^\s]+)', rawtext)
    if match:
        data['representative'] = match.group(1).strip()
    
    # 住所
    match = re.search(r'住所\s+([^\s]+(?:県|都|府|道)[^売従資設上決業電URL]+)', rawtext)
    if match:
        data['address'] = match.group(1).strip()
    
    # 売上
    match = re.search(r'売上\s+([^\s]+)', rawtext)
    if match:
        data['sales'] = match.group(1).strip()
    
    # 従業員数
    match = re.search(r'従業員数\s+([^\s]+)', rawtext)
    if match:
        data['employees'] = match.group(1).strip()
    
    # 資本金
    match = re.search(r'資本金\s+([^\s]+)', rawtext)
    if match:
        data['capital'] = match.group(1).strip()
    
    # 設立
    match = re.search(r'設立\s+(\d{4}年\d{1,2}月\d{1,2}日)', rawtext)
    if match:
        data['founded'] = match.group(1).strip()
    
    # 上場区分
    match = re.search(r'上場区分\s+([^\s]+)', rawtext)
    if match:
        data['listing'] = match.group(1).strip()
    
    # 決算月
    match = re.search(r'決算月\s+([^\s]+)', rawtext)
    if match:
        data['fiscalMonth'] = match.group(1).strip()
    
    # URL
    match = re.search(r'URL\s+(https?://[^\s]+)', rawtext)
    if match:
        data['url'] = match.group(1).strip()
    
    return data

def extract_representative_from_people(people):
    """peopleフィールドから代表者名を抽出"""
    if not people:
        return None
    
    # パターン1: "代表者; 頭取　　佐　藤　　稔"
    match = re.search(r'代表者;\s*[^\s;]+\s+([^;]+)', people)
    if match:
        name = match.group(1).strip()
        # 空白を除去
        name = re.sub(r'\s+', '', name)
        return name
    
    # パターン2: "代表者; 佐藤稔"
    match = re.search(r'代表者;\s*([^;]+)', people)
    if match:
        name = match.group(1).strip()
        # 余計な文字を除去
        name = re.sub(r'\s+', '', name)
        return name
    
    return None

def process_csv(input_file, output_file):
    """CSVを処理して修正"""
    rows_processed = 0
    rows_with_data = 0
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        
        # 元のヘッダーを維持
        fieldnames = reader.fieldnames
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row in reader:
            rows_processed += 1
            
            # rawTextから情報を抽出
            extracted = extract_from_rawtext(row.get('rawText', ''))
            
            # peopleから代表者名を抽出
            rep_from_people = extract_representative_from_people(row.get('people', ''))
            
            # 空のフィールドに抽出した情報を埋める
            if not row['representative'] and rep_from_people:
                row['representative'] = rep_from_people
            elif not row['representative'] and extracted.get('representative'):
                row['representative'] = extracted['representative']
            
            if not row['address'] and extracted.get('address'):
                row['address'] = extracted['address']
            
            if not row['sales'] and extracted.get('sales'):
                row['sales'] = extracted['sales']
            
            if not row['employees'] and extracted.get('employees'):
                row['employees'] = extracted['employees']
            
            if not row['capital'] and extracted.get('capital'):
                row['capital'] = extracted['capital']
            
            if not row['founded'] and extracted.get('founded'):
                row['founded'] = extracted['founded']
            
            if not row['listing'] and extracted.get('listing'):
                row['listing'] = extracted['listing']
            
            if not row['fiscalMonth'] and extracted.get('fiscalMonth'):
                row['fiscalMonth'] = extracted['fiscalMonth']
            
            if not row['url'] and extracted.get('url'):
                row['url'] = extracted['url']
            
            if extracted:
                rows_with_data += 1
            
            writer.writerow(row)
    
    print(f"✅ 処理完了: {input_file} → {output_file}")
    print(f"   総行数: {rows_processed}")
    print(f"   情報抽出成功: {rows_with_data}")

if __name__ == '__main__':
    print("🔧 130.csv/131.csv 修正スクリプト")
    print("")
    
    # 130.csv
    print("📄 130.csv を処理中...")
    process_csv('./csv/130.csv', './csv/130_fixed.csv')
    print("")
    
    # 131.csv
    print("📄 131.csv を処理中...")
    process_csv('./csv/131.csv', './csv/131_fixed.csv')
    print("")
    
    print("🎉 完了！")
    print("   修正後: csv/130_fixed.csv, csv/131_fixed.csv")
    print("")
    print("📌 次のステップ:")
    print("   1. 修正後のCSVを確認")
    print("   2. 問題なければ元のファイルと置き換え:")
    print("      mv csv/130_fixed.csv csv/130.csv")
    print("      mv csv/131_fixed.csv csv/131.csv")

