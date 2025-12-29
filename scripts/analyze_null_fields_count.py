#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import csv
from pathlib import Path
from collections import defaultdict

def analyze_null_fields():
    base_dir = Path(__file__).parent.parent / "null_fields_detailed"
    
    if not base_dir.exists():
        print(f"❌ ディレクトリが見つかりません: {base_dir}")
        return
    
    csv_files = sorted(base_dir.glob("null_fields_detailed_*.csv"))
    
    if not csv_files:
        print(f"❌ CSVファイルが見つかりません: {base_dir}")
        return
    
    print(f"📁 {len(csv_files)} 個のCSVファイルを分析中...")
    
    unique_companies = set()
    total_null_fields = 0
    field_counts = defaultdict(int)
    company_field_counts = defaultdict(int)
    
    for csv_file in csv_files:
        print(f"  処理中: {csv_file.name}")
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                company_id = row.get('companyId', '').strip()
                field_name = row.get('nullFieldName', '').strip()
                
                if company_id and field_name:
                    unique_companies.add(company_id)
                    total_null_fields += 1
                    field_counts[field_name] += 1
                    company_field_counts[company_id] += 1
    
    print(f"\n✅ 分析結果:")
    print(f"  総CSVファイル数: {len(csv_files)} 個")
    print(f"  更新対象企業数: {len(unique_companies):,} 社")
    print(f"  総nullフィールド数: {total_null_fields:,} 件")
    print(f"  企業あたり平均nullフィールド数: {total_null_fields / len(unique_companies):.2f} 件")
    
    print(f"\n📊 フィールド別null件数トップ10:")
    sorted_fields = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)
    for i, (field, count) in enumerate(sorted_fields[:10], 1):
        percentage = (count / len(unique_companies) * 100) if unique_companies else 0
        print(f"  {i}. {field}: {count:,} 件 ({percentage:.2f}%)")
    
    print(f"\n📊 企業別nullフィールド数分布:")
    null_count_distribution = defaultdict(int)
    for count in company_field_counts.values():
        null_count_distribution[count] += 1
    
    sorted_dist = sorted(null_count_distribution.items())
    for null_count, company_count in sorted_dist[:10]:
        print(f"  {null_count}個のnullフィールド: {company_count:,} 社")
    
    if len(sorted_dist) > 10:
        print(f"  ... (他 {len(sorted_dist) - 10} パターン)")

if __name__ == "__main__":
    analyze_null_fields()

