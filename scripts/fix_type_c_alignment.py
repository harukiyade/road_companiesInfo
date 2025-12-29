#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
タイプC（107,109,110）のヘッダーズレを修正
1-3行目を正として、他の行でズレている箇所を修正
"""

import csv
import sys

def analyze_alignment(file_path, correct_rows=3):
    """最初のN行を正として、ヘッダーとの対応を分析"""
    
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        print(f"\n=== {file_path} ===")
        print(f"ヘッダー数: {len(headers)}")
        
        # 最初の3行のカラム数を確認
        col_counts = []
        for idx in range(correct_rows):
            try:
                row = next(reader)
                col_counts.append(len(row))
                print(f"行{idx+1}: {len(row)}列")
            except StopIteration:
                break
        
        if col_counts:
            expected_cols = max(col_counts)
            print(f"期待カラム数: {expected_cols}")
            print(f"ヘッダーとの差: {expected_cols - len(headers)}")

if __name__ == '__main__':
    print("🔍 タイプC ヘッダーズレ分析")
    
    files = ['csv/105.csv', 'csv/106.csv', 'csv/107.csv', 'csv/109.csv', 'csv/110.csv', 'csv/122.csv']
    
    for file_path in files:
        try:
            analyze_alignment(file_path)
        except Exception as e:
            print(f"エラー: {file_path} - {e}")

