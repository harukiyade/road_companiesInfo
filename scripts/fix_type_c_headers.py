#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
タイプC（107,109,110,122）のヘッダー修正
業種3の後に業種4（業種（細））を追加
"""

import csv

def fix_headers(input_file, output_file):
    """ヘッダーを修正してCSVを再構築"""
    
    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.reader(infile)
        headers = next(reader)
        
        # 業種3の後（16番目の前）に業種（細）を挿入
        new_headers = headers[:15] + ['業種（細）'] + headers[15:]
        
        writer = csv.writer(outfile)
        writer.writerow(new_headers)
        
        # データ行はそのまま書き込み
        for row in reader:
            writer.writerow(row)
    
    print(f"✅ {input_file} → {output_file}")

if __name__ == '__main__':
    print("🔧 タイプC ヘッダー修正")
    print("   業種3の後に業種（細）を追加")
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
            fix_headers(input_f, output_f)
        except Exception as e:
            print(f"エラー: {input_f} - {e}")
    
    print("")
    print("🎉 完了！")
    print("")
    print("📌 確認:")
    print("   head -1 csv/107_fixed.csv | tr ',' '\\n' | grep -n 業種")

