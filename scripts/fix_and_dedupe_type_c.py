#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
107.csvタイプ（タイプC）の修正と重複削除
1. 業種4が空の行のデータズレを修正
2. ID, 取引種別, SBフラグ, NDA, AD, ステータス, 備考を削除
3. 重複企業を統合（法人番号または会社名+住所で判定）
"""

import csv
import sys

def is_postal_code(value):
    """郵便番号らしいか判定"""
    if not value:
        return False
    v = str(value).strip()
    # 3桁-4桁または7桁
    return (len(v) == 8 and '-' in v) or (len(v) == 7 and v.isdigit())

def is_address(value):
    """住所らしいか判定"""
    if not value:
        return False
    prefectures = ['北海道', '青森県', '岩手県', '宮城県', '秋田県', '山形県', '福島県',
                   '茨城県', '栃木県', '群馬県', '埼玉県', '千葉県', '東京都', '神奈川県',
                   '新潟県', '富山県', '石川県', '福井県', '山梨県', '長野県', '岐阜県',
                   '静岡県', '愛知県', '三重県', '滋賀県', '京都府', '大阪府', '兵庫県',
                   '奈良県', '和歌山県', '鳥取県', '島根県', '岡山県', '広島県', '山口県',
                   '徳島県', '香川県', '愛媛県', '高知県', '福岡県', '佐賀県', '長崎県',
                   '熊本県', '大分県', '宮崎県', '鹿児島県', '沖縄県']
    return any(pref in str(value) for pref in prefectures)

def fix_alignment(row, headers):
    """データのズレを修正"""
    # 業種（細）の位置（16番目、インデックス15）
    industry_detail_idx = 15
    postal_code_idx = 16
    
    # 業種（細）が郵便番号っぽい場合はズレている
    if len(row) > industry_detail_idx:
        industry_detail_value = row[industry_detail_idx]
        
        # 業種（細）が郵便番号または住所の場合、データを1つ右にシフト
        if is_postal_code(industry_detail_value) or is_address(industry_detail_value):
            # 業種（細）位置に空を挿入、データを右にシフト
            new_row = row[:industry_detail_idx] + [''] + row[industry_detail_idx:]
            return new_row
    
    return row

def remove_internal_fields(row, headers):
    """内部管理フィールドを削除"""
    # 削除するフィールドのインデックス
    remove_indices = set()
    
    for i, h in enumerate(headers):
        if h in ['ID', '取引種別', 'SBフラグ', 'NDA', 'AD', 'ステータス', '備考']:
            remove_indices.add(i)
    
    # 削除するインデックス以外を保持
    new_row = [row[i] for i in range(len(row)) if i not in remove_indices]
    new_headers = [headers[i] for i in range(len(headers)) if i not in remove_indices]
    
    return new_row, new_headers

def process_type_c_file(input_file, output_file):
    """タイプCファイルを処理"""
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.reader(infile)
        headers = next(reader)
        
        # 内部管理フィールドを削除したヘッダー
        _, clean_headers = remove_internal_fields([], headers)
        
        writer = csv.writer(outfile)
        writer.writerow(clean_headers)
        
        fixed_count = 0
        total_count = 0
        
        for row in reader:
            total_count += 1
            
            # アライメント修正
            fixed_row = fix_alignment(row, headers)
            if len(fixed_row) != len(row):
                fixed_count += 1
            
            # 内部管理フィールド削除
            clean_row, _ = remove_internal_fields(fixed_row, headers)
            
            writer.writerow(clean_row)
        
        print(f"✅ {input_file} → {output_file}")
        print(f"   総行数: {total_count}")
        print(f"   ズレ修正: {fixed_count}行")

if __name__ == '__main__':
    print("🔧 タイプC（107含む）修正スクリプト")
    print("   1. データのズレ修正")
    print("   2. 内部管理フィールド削除")
    print("")
    
    files = [
        ('csv/105.csv', 'csv/105_fixed.csv'),
        ('csv/106.csv', 'csv/106_fixed.csv'),
        ('csv/107.csv', 'csv/107_fixed.csv'),
        ('csv/109.csv', 'csv/109_fixed.csv'),
        ('csv/110.csv', 'csv/110_fixed.csv'),
        ('csv/122.csv', 'csv/122_fixed.csv'),
    ]
    
    for input_f, output_f in files:
        try:
            process_type_c_file(input_f, output_f)
        except Exception as e:
            print(f"エラー: {input_f} - {e}")
    
    print("")
    print("🎉 完了！")
    print("")
    print("📌 次のステップ:")
    print("   1. 修正ファイル確認")
    print("   2. 元ファイルと置き換え:")
    print("      for f in 105 106 107 109 110 122; do mv csv/${f}_fixed.csv csv/${f}.csv; done")

