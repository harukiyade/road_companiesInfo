#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
130.csv/131.csv（タイプI）を完全展開するスクリプト
- industriesを業種1～4に分割
- departmentsを部署情報に分割
- rawTextから追加情報抽出
- 英語ヘッダーを日本語（companies_newフィールド名）に変換
"""

import csv
import re
import sys

def normalize_number(value_str):
    """
    数値を正規化（単位を除去して数値のみに）
    例: "1兆円" → "1000000000000"
        "235億円" → "23500000000"
        "2820人" → "2820"
        "1.2万人" → "12000"
    """
    if not value_str or value_str in ['-', '']:
        return ''
    
    s = value_str.strip()
    
    # 単位の変換表
    # 兆 = 10^12, 億 = 10^8, 万 = 10^4, 千 = 10^3
    
    # "1兆円 ~" のような範囲表記は最小値を取る
    if '~' in s:
        s = s.split('~')[0].strip()
    
    # 数値と単位を抽出
    match = re.search(r'([\d.]+)\s*(兆|億|万|千)?', s)
    if not match:
        return ''
    
    num = float(match.group(1))
    unit = match.group(2)
    
    # 単位に応じて変換
    if unit == '兆':
        num *= 1_000_000_000_000
    elif unit == '億':
        num *= 100_000_000
    elif unit == '万':
        num *= 10_000
    elif unit == '千':
        num *= 1_000
    
    # 整数に変換
    return str(int(num))

def parse_people(people_str):
    """
    people フィールドをパースして役員名を抽出
    パターン: "職種; 人名; 職種; 人名; ..."
    """
    if not people_str:
        return {}
    
    result = {}
    
    # セミコロンで分割
    parts = [p.strip() for p in people_str.split(';') if p.strip()]
    
    # 無視するキーワード
    skip_keywords = ['企業詳細', '基本情報', '人物', '部署', '企業メモ', '追加', '総人数', '総件数', 'は追加データ']
    
    executives = []
    i = 0
    
    while i < len(parts) and len(executives) < 10:
        part = parts[i]
        
        # スキップキーワード
        if any(kw in part for kw in skip_keywords):
            i += 1
            continue
        
        # 法人番号パターン
        if '法人番号' in part or re.match(r'^\d{13}$', part):
            i += 1
            continue
        
        # 会社名パターン（セミコロン前後に同じ会社名が出る）
        if '株式会社' in part or '有限会社' in part or '合同会社' in part:
            i += 1
            continue
        
        # 職種パターン（次が人名の可能性）
        position_keywords = ['代表者', 'プロジェクトマネージャー', '職制', '設計エンジニア', 
                            '従業員', '一般', '会社員', '製品開発', 'Assistant Manager',
                            '製造作業', '担当職', 'Staff', '主担当員', '銀行員',
                            'チーフスタッフ', '部長', '広報部長', '経営企画部']
        
        if any(kw in part for kw in position_keywords):
            # 次が人名の可能性
            if i + 1 < len(parts):
                next_part = parts[i + 1]
                # 人名っぽいか判定（2-20文字、職種キーワードを含まない）
                if 2 <= len(next_part) <= 20 and not any(kw in next_part for kw in position_keywords):
                    # 会社名や法人番号でない
                    if '株式会社' not in next_part and not re.match(r'^\d{13}$', next_part):
                        executives.append({
                            'position': part,
                            'name': next_part
                        })
                        i += 1  # 人名を読んだのでスキップ
        
        i += 1
    
    # 役員名1～10として展開
    for idx, exec_info in enumerate(executives[:10], 1):
        result[f'executiveName{idx}'] = exec_info['name']
        result[f'executivePosition{idx}'] = exec_info['position']
    
    return result

def parse_industries(industries_str):
    """
    industries フィールドをパースして業種1～4に分割
    例: "大分類; 金融業・保険業; 中分類; 銀行業; 小分類; 銀行（中央銀行を除く）; 細分類; 普通銀行"
    """
    if not industries_str:
        return {}
    
    result = {}
    parts = [p.strip() for p in industries_str.split(';') if p.strip()]
    
    large = []
    middle = []
    small = []
    detail = []
    
    current = None
    for part in parts:
        if part == '大分類':
            current = large
        elif part == '中分類':
            current = middle
        elif part == '小分類':
            current = small
        elif part == '細分類':
            current = detail
        elif current is not None and part:
            current.append(part)
    
    # 最大4つまで取得
    if large:
        result['industryLarge'] = '; '.join(large[:4])
    if middle:
        result['industryMiddle'] = '; '.join(middle[:4])
    if small:
        result['industrySmall'] = '; '.join(small[:4])
    if detail:
        result['industryDetail'] = '; '.join(detail[:4])
    
    return result

def parse_departments(departments_str):
    """
    departments フィールドをパースして部署情報に分割
    パターン: 部署名; 住所; (電話番号); (カテゴリ); 部署名; 住所; ...
    """
    if not departments_str:
        return {}
    
    result = {}
    
    # セミコロンで分割
    parts = [p.strip() for p in departments_str.split(';') if p.strip()]
    
    # 無視するキーワード
    skip_keywords = ['企業詳細', '基本情報', '人物', '条件に一致', '企業メモ', '追加', '総件数', '総人数']
    
    # 法人番号を抽出
    for part in parts:
        if re.match(r'法人番号[：:]\d+', part):
            corp_num = re.search(r'(\d{13})', part)
            if corp_num:
                result['bankCorporateNumber'] = corp_num.group(1)
            break
    
    # 部署情報を抽出
    departments = []
    i = 0
    
    while i < len(parts) and len(departments) < 7:
        part = parts[i]
        
        # スキップキーワードチェック
        if any(kw in part for kw in skip_keywords):
            i += 1
            continue
        
        # 法人番号パターンはスキップ
        if '法人番号' in part:
            i += 1
            continue
        
        # カテゴリっぽいもの（営業、経営企画など）はスキップ
        categories = ['営業', '経営企画', '総務管理', '人事', '研究', '製造工場', 'その他', '法務リスク', '経理', '購買資材', '環境CSR', '広報IR', '海外', 'システム']
        if part in categories:
            i += 1
            continue
        
        # 電話番号だけのパターンはスキップ
        if re.match(r'^[\d\-()]+$', part) and len(part) >= 9:
            i += 1
            continue
        
        # 住所っぽいか判定
        is_address = any(pref in part for pref in ['県', '都', '府', '道']) or '市' in part or '区' in part or '町' in part
        
        # 住所でも電話番号でもない → 部署名の可能性
        if not is_address and not re.match(r'^[\d\-()]+$', part):
            dept_info = {'name': part}
            
            # 次が住所か確認
            if i + 1 < len(parts):
                next_part = parts[i + 1]
                if any(pref in next_part for pref in ['県', '都', '府', '道']) or '市' in next_part or '区' in next_part:
                    dept_info['address'] = next_part
                    i += 1
                    
                    # さらに次が電話番号か確認
                    if i + 1 < len(parts):
                        tel_part = parts[i + 1]
                        if re.match(r'^[\d\-]+$', tel_part) and 9 <= len(tel_part.replace('-', '')) <= 11:
                            dept_info['phone'] = tel_part
                            i += 1
            
            # 部署名が意味のあるものだけ追加
            if len(dept_info['name']) > 2 and '|' not in dept_info['name']:
                departments.append(dept_info)
        
        i += 1
    
    # 最大7部署まで
    for idx, dept in enumerate(departments[:7], 1):
        if 'name' in dept:
            result[f'departmentName{idx}'] = dept['name']
        if 'address' in dept:
            result[f'departmentAddress{idx}'] = dept['address']
        if 'phone' in dept:
            result[f'departmentPhone{idx}'] = dept['phone']
    
    return result

def extract_from_rawtext_detailed(rawtext):
    """rawTextフィールドから詳細情報を抽出（数値は正規化）"""
    if not rawtext:
        return {}
    
    data = {}
    
    # 代表者名（既に値がある場合は上書きしない）
    match = re.search(r'代表者名\s+([^\s]+)', rawtext)
    if match and match.group(1).strip() != '-':
        data['representativeName'] = match.group(1).strip()
    
    # 住所（既に値がある場合は上書きしない）
    match = re.search(r'住所\s+([^\s]+(?:県|都|府|道)[^売従資設上決業電URL]+)', rawtext)
    if match:
        data['address'] = match.group(1).strip()
    
    # 売上（数値に正規化）
    match = re.search(r'売上\s+([^\s]+)', rawtext)
    if match:
        data['revenue'] = normalize_number(match.group(1))
    
    # 従業員数（数値に正規化）
    match = re.search(r'従業員数\s+([^\s]+)', rawtext)
    if match:
        data['employeeCount'] = normalize_number(match.group(1))
    
    # 資本金（数値に正規化）
    match = re.search(r'資本金\s+([^\s]+)', rawtext)
    if match:
        data['capitalStock'] = normalize_number(match.group(1))
    
    # 設立
    match = re.search(r'設立\s+(\d{4}年\d{1,2}月\d{1,2}日)', rawtext)
    if match:
        data['established'] = match.group(1).strip()
    
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
        data['companyUrl'] = match.group(1).strip()
    
    # 電話番号
    match = re.search(r'電話番号\s+([\d\-]+)', rawtext)
    if match:
        data['phoneNumber'] = match.group(1).strip()
    
    return data

def process_csv_expanded(input_file, output_file):
    """CSVを完全展開して処理"""
    rows_processed = 0
    
    # 新しいヘッダー（companies_newフィールド名 - 日本語コメント付き）
    # ヘッダーは英語のまま（backfill_companies_from_csv.tsでマッピングしやすいため）
    new_fieldnames = [
        'name',                 # 会社名
        'corporateNumber',      # 法人番号
        'representativeName',   # 代表者名
        'revenue',              # 売上高（数値）
        'capitalStock',         # 資本金（数値）
        'listing',              # 上場区分
        'address',              # 住所
        'employeeCount',        # 従業員数（数値）
        'established',          # 設立
        'fiscalMonth',          # 決算月
        'industryLarge',        # 業種-大
        'industryMiddle',       # 業種-中
        'industrySmall',        # 業種-小
        'industryDetail',       # 業種-細
        'phoneNumber',          # 電話番号
        'companyUrl',           # URL
        'bankCorporateNumber',  # 取引先銀行法人番号
        'departmentName1', 'departmentAddress1', 'departmentPhone1',
        'departmentName2', 'departmentAddress2', 'departmentPhone2',
        'departmentName3', 'departmentAddress3', 'departmentPhone3',
        'departmentName4', 'departmentAddress4', 'departmentPhone4',
        'departmentName5', 'departmentAddress5', 'departmentPhone5',
        'departmentName6', 'departmentAddress6', 'departmentPhone6',
        'departmentName7', 'departmentAddress7', 'departmentPhone7',
        'executiveName1', 'executivePosition1',
        'executiveName2', 'executivePosition2',
        'executiveName3', 'executivePosition3',
        'executiveName4', 'executivePosition4',
        'executiveName5', 'executivePosition5',
        'executiveName6', 'executivePosition6',
        'executiveName7', 'executivePosition7',
        'executiveName8', 'executivePosition8',
        'executiveName9', 'executivePosition9',
        'executiveName10', 'executivePosition10',
        'departments',          # 元の部署情報（参考）
        'people',               # 元の人物情報（参考）
        'rawText'               # 元の生データ（参考）
    ]
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=new_fieldnames, extrasaction='ignore')
        writer.writeheader()
        
        for row in reader:
            rows_processed += 1
            
            new_row = {}
            
            # 基本フィールド（英語フィールド名を保持、数値は正規化）
            new_row['name'] = row.get('name', '')
            new_row['corporateNumber'] = row.get('corporateNumber', '')
            new_row['representativeName'] = row.get('representative', '')
            
            # 数値フィールドは単位を除去
            new_row['revenue'] = normalize_number(row.get('sales', ''))
            new_row['capitalStock'] = normalize_number(row.get('capital', ''))
            new_row['employeeCount'] = normalize_number(row.get('employees', ''))
            
            new_row['listing'] = row.get('listing', '')
            new_row['address'] = row.get('address', '')
            new_row['established'] = row.get('founded', '')
            new_row['fiscalMonth'] = row.get('fiscalMonth', '')
            new_row['phoneNumber'] = row.get('tel', '')
            new_row['companyUrl'] = row.get('url', '')
            
            # industries を分割
            industries_data = parse_industries(row.get('industries', ''))
            new_row['industryLarge'] = industries_data.get('industryLarge', '')
            new_row['industryMiddle'] = industries_data.get('industryMiddle', '')
            new_row['industrySmall'] = industries_data.get('industrySmall', '')
            new_row['industryDetail'] = industries_data.get('industryDetail', '')
            
            # departments を分割
            dept_data = parse_departments(row.get('departments', ''))
            for key, value in dept_data.items():
                new_row[key] = value
            
            # people から役員名を抽出
            people_data = parse_people(row.get('people', ''))
            for key, value in people_data.items():
                new_row[key] = value
            
            # rawText から追加情報を抽出（空のフィールドのみ補完）
            rawtext_data = extract_from_rawtext_detailed(row.get('rawText', ''))
            for key, value in rawtext_data.items():
                if not new_row.get(key):
                    new_row[key] = value
            
            # departments と rawText からも役員情報を探す
            # （peopleで見つからなかった場合の補完）
            if not any(new_row.get(f'executiveName{i}') for i in range(1, 11)):
                # departmentsからも探す
                dept_people = parse_people(row.get('departments', ''))
                for key, value in dept_people.items():
                    if not new_row.get(key):
                        new_row[key] = value
                
                # rawTextからも探す
                rawtext_people = parse_people(row.get('rawText', ''))
                for key, value in rawtext_people.items():
                    if not new_row.get(key):
                        new_row[key] = value
            
            # 元のフィールドも保持
            new_row['departments'] = row.get('departments', '')
            new_row['people'] = row.get('people', '')
            new_row['rawText'] = row.get('rawText', '')
            
            writer.writerow(new_row)
    
    print(f"✅ 処理完了: {input_file} → {output_file}")
    print(f"   総行数: {rows_processed}")

if __name__ == '__main__':
    print("🔧 130.csv/131.csv 完全展開スクリプト")
    print("")
    
    # 現在のCSVファイルから処理
    # 130.csv
    print("📄 130.csv を展開中...")
    process_csv_expanded('./csv/130.csv', './csv/130_expanded.csv')
    print("")
    
    # 131.csv
    print("📄 131.csv を展開中...")
    process_csv_expanded('./csv/131.csv', './csv/131_expanded.csv')
    print("")
    
    print("🎉 完了！")
    print("   展開後: csv/130_expanded.csv, csv/131_expanded.csv")
    print("")
    print("📌 次のステップ:")
    print("   1. 展開後のCSVを確認")
    print("   2. 問題なければ元のファイルと置き換え:")
    print("      mv csv/130_expanded.csv csv/130.csv")
    print("      mv csv/131_expanded.csv csv/131.csv")

