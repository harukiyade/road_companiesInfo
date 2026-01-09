#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
127.csv/128.csv（タイプJ1）のJSON展開スクリプト
summaryJson、basicJson、financeJsonなどから情報を抽出
"""

import csv
import json
import re
import sys

# CSVフィールドサイズ制限を拡張
csv.field_size_limit(sys.maxsize)

def normalize_number(value_str):
    """数値を正規化（カンマ区切りと百万円単位に対応）"""
    if not value_str or value_str in ['-', '']:
        return ''
    
    s = str(value_str).strip()
    
    # 百万円フラグ
    has_million = '(百万円)' in s or '百万円' in s
    
    # 括弧内と期表記を除去
    s = re.sub(r'\([^)]+\)', '', s)
    s = re.sub(r'\(.*期\)', '', s)
    
    # 範囲表記（~）は最小値を取る
    if '~' in s:
        s = s.split('~')[0].strip()
    
    # カンマを除去
    s = s.replace(',', '')
    
    # 数値と単位を抽出
    match = re.search(r'([\d.]+)\s*(兆|億|万|千)?', s)
    if not match:
        return ''
    
    num = float(match.group(1))
    unit = match.group(2)
    
    # 百万円の場合は百万倍
    if has_million:
        num *= 1_000_000
    
    # 単位に応じて変換
    if unit == '兆':
        num *= 1_000_000_000_000
    elif unit == '億':
        num *= 100_000_000
    elif unit == '万':
        num *= 10_000
    elif unit == '千':
        num *= 1_000
    
    return str(int(num))

def extract_from_summary_json(summary_json_str):
    """summaryJsonから情報を抽出（全フィールド展開）"""
    if not summary_json_str:
        return {}
    
    try:
        data = json.loads(summary_json_str)
        kv = data.get('kv', {})
        
        result = {}
        
        # 会社名
        if kv.get('会社名'):
            result['name'] = kv['会社名']
        
        # 英文名
        if kv.get('英文名'):
            result['nameEn'] = kv['英文名']
        
        # 業種
        if kv.get('業種'):
            result['industry'] = kv['業種']
        
        # 本社住所
        if kv.get('本社住所'):
            addr = kv['本社住所']
            # 〒 を除去
            addr = re.sub(r'^〒\s*[\d\-]+\s*', '', addr)
            result['address'] = addr
        
        # 設立年月日
        if kv.get('設立年月日'):
            result['established'] = kv['設立年月日']
        
        # 法人番号
        if kv.get('法人番号'):
            result['corporateNumber'] = kv['法人番号']
        
        # 日経会社コード
        if kv.get('日経会社コード'):
            result['nikkeiCode'] = kv['日経会社コード']
        
        # 資本金（数値に正規化）
        if kv.get('資本金'):
            result['capitalStock'] = normalize_number(kv['資本金'])
        
        # 決算月
        if kv.get('決算月'):
            result['fiscalMonth'] = kv['決算月']
        
        # 従業員数（数値に正規化）
        if kv.get('従業員数'):
            result['employeeCount'] = normalize_number(kv['従業員数'])
        
        # 所属団体
        if kv.get('所属団体'):
            result['affiliations'] = kv['所属団体']
        
        # URL
        if kv.get('URL'):
            result['companyUrl'] = kv['URL']
        
        # 代表者名
        if kv.get('代表者名'):
            result['representativeName'] = kv['代表者名']
        
        # 売上高（単独）
        if kv.get('売上高（単独）'):
            result['revenue'] = normalize_number(kv['売上高（単独）'])
        elif kv.get('売上高'):
            result['revenue'] = normalize_number(kv['売上高'])
        
        # 当期利益
        if kv.get('当期利益'):
            result['latestProfit'] = normalize_number(kv['当期利益'])
        elif kv.get('経常利益'):
            result['latestProfit'] = normalize_number(kv['経常利益'])
        
        # 発行済株式数
        if kv.get('発行済株式数'):
            result['issuedShares'] = normalize_number(kv['発行済株式数'])
        
        # 事業内容
        if kv.get('事業内容'):
            result['businessDescriptions'] = kv['事業内容']
        
        return result
    except Exception as e:
        return {}

def process_type_j1_csv(input_file, output_file):
    """タイプJ1のCSVを展開"""
    rows_processed = 0
    
    # 新しいヘッダー（summaryJsonから展開した項目を含む）
    new_fieldnames = [
        'id',
        'name',              # 会社名
        'nameEn',            # 英文名
        'corporateNumber',   # 法人番号
        'nikkeiCode',        # 日経会社コード
        'prefecture',        # 都道府県
        'address',           # 住所
        'industry',          # 業種
        'capitalStock',      # 資本金（数値）
        'revenue',           # 売上高（数値）
        'latestProfit',      # 当期利益（数値）
        'employeeCount',     # 従業員数（数値）
        'issuedShares',      # 発行済株式数（数値）
        'established',       # 設立
        'fiscalMonth',       # 決算月
        'listing',           # 上場
        'representativeName', # 代表者名
        'businessDescriptions', # 事業内容
        'companyUrl',        # URL
        'contactUrl',        # 問い合わせURL
        'detailUrl',         # 詳細URL
        'banks',             # 取引先銀行
        'affiliations',      # 所属団体
        'overview',          # 概要
        'history',           # 沿革
        # JSONフィールドは元のまま保持（将来の拡張用）
        'topTabsJson',
        'leftNavJson',
        'summaryJson',
        'overviewTabJson',
        'orgJson',
        'basicJson',
        'financeJson',
        'compareMAJson',
        'shareholdersJson',
        'shareholdersMeetingJson',
        'esgJson',
        'statementsJson',
        'notesJson',
        'analysisJson',
        'segmentsJson',
        'bankBorrowingsJson',
        'forecastJson',
    ]
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=new_fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        for row in reader:
            rows_processed += 1
            
            new_row = {}
            
            # summaryJsonから情報を抽出（最優先）
            summary_data = extract_from_summary_json(row.get('summaryJson', ''))
            for key, value in summary_data.items():
                new_row[key] = value
            
            # 基本フィールド（summaryJsonにない場合のフォールバック）
            if not new_row.get('id'):
                new_row['id'] = row.get('id', '')
            if not new_row.get('name'):
                new_row['name'] = row.get('name', '')
            if not new_row.get('corporateNumber'):
                new_row['corporateNumber'] = row.get('corporateNumber', '')
            if not new_row.get('prefecture'):
                new_row['prefecture'] = row.get('prefecture', '')
            if not new_row.get('listing'):
                new_row['listing'] = row.get('listed', '')
            if not new_row.get('companyUrl'):
                new_row['companyUrl'] = row.get('url', '')
            
            # 元のフィールドで値がない項目を補完
            new_row['contactUrl'] = row.get('contactUrl', '')
            new_row['detailUrl'] = row.get('detailUrl', '')
            if not new_row.get('overview'):
                new_row['overview'] = row.get('overview', '')
            if not new_row.get('history'):
                new_row['history'] = row.get('history', '')
            if not new_row.get('banks'):
                new_row['banks'] = row.get('banks', '')
            
            # JSONフィールドは元のまま保持
            for json_field in ['topTabsJson', 'leftNavJson', 'summaryJson', 'overviewTabJson',
                              'orgJson', 'basicJson', 'financeJson', 'compareMAJson',
                              'shareholdersJson', 'shareholdersMeetingJson', 'esgJson',
                              'statementsJson', 'notesJson', 'analysisJson', 'segmentsJson',
                              'bankBorrowingsJson', 'forecastJson']:
                new_row[json_field] = row.get(json_field, '')
            
            writer.writerow(new_row)
    
    print(f"✅ 処理完了: {input_file} → {output_file}")
    print(f"   総行数: {rows_processed}")

if __name__ == '__main__':
    print("🔧 127.csv/128.csv（タイプJ1）展開スクリプト")
    print("")
    
    # 127.csv
    print("📄 127.csv を展開中...")
    process_type_j1_csv('./csv/127.csv', './csv/127_expanded.csv')
    print("")
    
    # 128.csv
    print("📄 128.csv を展開中...")
    process_type_j1_csv('./csv/128.csv', './csv/128_expanded.csv')
    print("")
    
    print("🎉 完了！")
    print("   展開後: csv/127_expanded.csv, csv/128_expanded.csv")
    print("")
    print("📌 次のステップ:")
    print("   1. 展開後のCSVを確認")
    print("   2. 問題なければ元のファイルと置き換え:")
    print("      mv csv/127_expanded.csv csv/127.csv")
    print("      mv csv/128_expanded.csv csv/128.csv")

