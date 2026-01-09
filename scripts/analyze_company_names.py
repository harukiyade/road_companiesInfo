#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSVファイル内の会社名を分析し、問題のある行を特定するスクリプト

問題のパターン:
1. 会社名が個人名のように見える（「株式会社」「有限会社」などの法人格がない）
2. 会社名が事業内容の説明になっている
3. 会社名が空または不正な形式
"""

import csv
import re
import sys
from pathlib import Path
from typing import List, Dict, Tuple

# 法人格のパターン
CORPORATE_SUFFIXES = [
    '株式会社', '有限会社', '合資会社', '合名会社', '合同会社',
    '一般社団法人', '一般財団法人', '公益社団法人', '公益財団法人',
    '学校法人', '医療法人', '社会福祉法人', '宗教法人',
    '特定非営利活動法人', 'NPO法人', '協同組合', '農業協同組合',
    '生活協同組合', '信用金庫', '信用組合', '労働金庫',
    '相互会社', '特殊会社', '地方公共団体', '独立行政法人'
]

# 個人名の可能性が高いパターン（カタカナのみ、漢字のみの短い名前など）
PERSON_NAME_PATTERNS = [
    re.compile(r'^[ァ-ヶー]+$'),  # カタカナのみ
    re.compile(r'^[一-龯]{2,4}$'),  # 漢字2-4文字（個人名の可能性）
]

# 事業内容を示す可能性が高いキーワード
BUSINESS_DESCRIPTION_KEYWORDS = [
    '事務', '業務', '代行', '製造', '販売', '卸売', '小売', '運送', '建設',
    '工事', '設計', '開発', '管理', '運営', 'サービス', '事業', '業',
    '調達', 'メンテナンス', '製造・', '販売、', '運送、', '工事、',
    'の運営', 'を行う', 'を手掛ける', 'を担当', 'を提供'
]


def is_valid_company_name(name: str) -> bool:
    """会社名が有効かどうかを判定"""
    if not name or not name.strip():
        return False
    
    name = name.strip()
    
    # 法人格を含む場合は有効とみなす
    for suffix in CORPORATE_SUFFIXES:
        if suffix in name:
            return True
    
    return False


def is_likely_person_name(name: str) -> bool:
    """個人名の可能性が高いかどうかを判定"""
    if not name or not name.strip():
        return False
    
    name = name.strip()
    
    # 法人格を含む場合は個人名ではない
    for suffix in CORPORATE_SUFFIXES:
        if suffix in name:
            return False
    
    # パターンマッチング
    for pattern in PERSON_NAME_PATTERNS:
        if pattern.match(name):
            return True
    
    # 短い名前（2-4文字）で法人格がない場合は個人名の可能性
    if 2 <= len(name) <= 4 and not any(suffix in name for suffix in CORPORATE_SUFFIXES):
        return True
    
    return False


def is_likely_business_description(name: str) -> bool:
    """事業内容の説明の可能性が高いかどうかを判定"""
    if not name or not name.strip():
        return False
    
    name = name.strip()
    
    # 法人格を含む場合は事業内容ではない
    for suffix in CORPORATE_SUFFIXES:
        if suffix in name:
            return False
    
    # キーワードを含む場合は事業内容の可能性
    for keyword in BUSINESS_DESCRIPTION_KEYWORDS:
        if keyword in name:
            return True
    
    # カンマ区切りで複数の事業内容が列挙されている場合
    if ',' in name and len(name.split(',')) >= 2:
        return True
    
    # 長い説明文（30文字以上）の場合は事業内容の可能性
    if len(name) >= 30:
        return True
    
    return False


def analyze_csv_file(csv_path: Path) -> Dict:
    """CSVファイルを分析して問題のある行を特定"""
    results = {
        'total_rows': 0,
        'valid_names': 0,
        'invalid_names': 0,
        'person_names': [],
        'business_descriptions': [],
        'empty_names': [],
        'other_issues': []
    }
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            # ヘッダー行を読み込む
            reader = csv.reader(f)
            header = next(reader)
            
            # 会社名の列インデックスを探す
            company_name_index = None
            for i, col in enumerate(header):
                if col in ['会社名', '社名', '企業名', 'name', 'companyName']:
                    company_name_index = i
                    break
            
            if company_name_index is None:
                print(f"⚠️ 警告: {csv_path.name} に会社名の列が見つかりません")
                return results
            
            print(f"📊 {csv_path.name} を分析中...")
            print(f"   会社名の列: {header[company_name_index]} (インデックス: {company_name_index})")
            
            # 各行を分析
            for row_num, row in enumerate(reader, start=2):  # ヘッダーを除いて2行目から
                results['total_rows'] += 1
                
                if len(row) <= company_name_index:
                    continue
                
                company_name = row[company_name_index].strip() if company_name_index < len(row) else ''
                
                if not company_name:
                    results['empty_names'].append({
                        'row': row_num,
                        'name': company_name
                    })
                    results['invalid_names'] += 1
                elif is_valid_company_name(company_name):
                    results['valid_names'] += 1
                elif is_likely_person_name(company_name):
                    results['person_names'].append({
                        'row': row_num,
                        'name': company_name,
                        'full_row': row[:min(5, len(row))]  # 最初の5列のみ保存
                    })
                    results['invalid_names'] += 1
                elif is_likely_business_description(company_name):
                    results['business_descriptions'].append({
                        'row': row_num,
                        'name': company_name,
                        'full_row': row[:min(5, len(row))]  # 最初の5列のみ保存
                    })
                    results['invalid_names'] += 1
                else:
                    results['other_issues'].append({
                        'row': row_num,
                        'name': company_name,
                        'full_row': row[:min(5, len(row))]
                    })
                    results['invalid_names'] += 1
    
    except Exception as e:
        print(f"❌ エラー: {csv_path.name} の読み込みに失敗しました: {e}")
        return results
    
    return results


def print_report(results: Dict, csv_path: Path):
    """分析結果をレポートとして出力"""
    print(f"\n{'='*60}")
    print(f"📋 分析レポート: {csv_path.name}")
    print(f"{'='*60}")
    print(f"総行数: {results['total_rows']}")
    print(f"有効な会社名: {results['valid_names']} ({results['valid_names']/max(results['total_rows'], 1)*100:.1f}%)")
    print(f"問題のある会社名: {results['invalid_names']} ({results['invalid_names']/max(results['total_rows'], 1)*100:.1f}%)")
    print(f"\n詳細:")
    print(f"  - 空の会社名: {len(results['empty_names'])}")
    print(f"  - 個人名の可能性: {len(results['person_names'])}")
    print(f"  - 事業内容の可能性: {len(results['business_descriptions'])}")
    print(f"  - その他の問題: {len(results['other_issues'])}")
    
    if results['person_names']:
        print(f"\n🔍 個人名の可能性がある行 (最初の10件):")
        for item in results['person_names'][:10]:
            print(f"  行 {item['row']}: {item['name']}")
    
    if results['business_descriptions']:
        print(f"\n🔍 事業内容の可能性がある行 (最初の10件):")
        for item in results['business_descriptions'][:10]:
            print(f"  行 {item['row']}: {item['name'][:50]}...")
    
    if results['other_issues']:
        print(f"\n🔍 その他の問題がある行 (最初の10件):")
        for item in results['other_issues'][:10]:
            print(f"  行 {item['row']}: {item['name'][:50]}...")


def main():
    """メイン処理"""
    if len(sys.argv) < 2:
        print("使用方法: python analyze_company_names.py <csv-file> [csv-file2] ...")
        print("例: python analyze_company_names.py csv/36.csv")
        sys.exit(1)
    
    csv_files = [Path(f) for f in sys.argv[1:]]
    
    all_results = {}
    
    for csv_path in csv_files:
        if not csv_path.exists():
            print(f"❌ エラー: ファイルが見つかりません: {csv_path}")
            continue
        
        results = analyze_csv_file(csv_path)
        all_results[str(csv_path)] = results
        print_report(results, csv_path)
    
    # サマリー
    if len(all_results) > 1:
        print(f"\n{'='*60}")
        print("📊 全体サマリー")
        print(f"{'='*60}")
        total_rows = sum(r['total_rows'] for r in all_results.values())
        total_valid = sum(r['valid_names'] for r in all_results.values())
        total_invalid = sum(r['invalid_names'] for r in all_results.values())
        print(f"総行数: {total_rows}")
        print(f"有効な会社名: {total_valid} ({total_valid/max(total_rows, 1)*100:.1f}%)")
        print(f"問題のある会社名: {total_invalid} ({total_invalid/max(total_rows, 1)*100:.1f}%)")


if __name__ == '__main__':
    main()
