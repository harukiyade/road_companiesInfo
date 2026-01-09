#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSVファイルの会社名を修正するスクリプト

修正方法:
1. 法人番号から企業名を取得（推奨）
2. 説明列から企業名を抽出
3. 手動修正用のレポートを生成
"""

import csv
import re
import sys
from pathlib import Path
from typing import List, Dict, Optional
import json

# 法人格のパターン
CORPORATE_SUFFIXES = [
    '株式会社', '有限会社', '合資会社', '合名会社', '合同会社',
    '一般社団法人', '一般財団法人', '公益社団法人', '公益財団法人',
    '学校法人', '医療法人', '社会福祉法人', '宗教法人',
    '特定非営利活動法人', 'NPO法人', '協同組合', '農業協同組合',
    '生活協同組合', '信用金庫', '信用組合', '労働金庫',
    '相互会社', '特殊会社', '地方公共団体', '独立行政法人',
    '税理士法人', '司法書士法人', '弁理士法人', '行政書士法人',
    '土地家屋調査士法人', '社会保険労務士法人',
    '国立大学法人', '公立大学法人', '私立大学法人',
    '国立研究開発法人', '地方独立行政法人'
]


def extract_company_name_from_description(description: str) -> Optional[str]:
    """説明文から企業名を抽出"""
    if not description:
        return None
    
    # 「株式会社XXX」のようなパターンを探す
    patterns = [
        r'([^、,，。\n]*?株式会社[^、,，。\n]*)',
        r'([^、,，。\n]*?有限会社[^、,，。\n]*)',
        r'([^、,，。\n]*?合同会社[^、,，。\n]*)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, description)
        if match:
            name = match.group(1).strip()
            # 長すぎる場合は除外
            if len(name) <= 50:
                return name
    
    return None


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


def generate_fix_report(csv_path: Path, output_path: Optional[Path] = None) -> List[Dict]:
    """修正が必要な行のレポートを生成"""
    issues = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            # 列インデックスを探す
            company_name_idx = None
            corporate_number_idx = None
            description_idx = None
            representative_idx = None
            
            for i, col in enumerate(header):
                if col in ['会社名', '社名', '企業名', 'name', 'companyName']:
                    company_name_idx = i
                elif col in ['法人番号', 'corporateNumber']:
                    corporate_number_idx = i
                elif col in ['説明', '概要', 'description']:
                    description_idx = i
                elif col in ['代表者名', 'representative']:
                    representative_idx = i
            
            if company_name_idx is None:
                print(f"⚠️ 警告: {csv_path.name} に会社名の列が見つかりません")
                return issues
            
            # 各行をチェック
            for row_num, row in enumerate(reader, start=2):
                if len(row) <= company_name_idx:
                    continue
                
                company_name = row[company_name_idx].strip() if company_name_idx < len(row) else ''
                corporate_number = row[corporate_number_idx].strip() if corporate_number_idx and corporate_number_idx < len(row) else ''
                description = row[description_idx].strip() if description_idx and description_idx < len(row) else ''
                representative = row[representative_idx].strip() if representative_idx and representative_idx < len(row) else ''
                
                # 問題がある場合
                if not is_valid_company_name(company_name):
                    issue = {
                        'row': row_num,
                        'current_name': company_name,
                        'corporate_number': corporate_number,
                        'description': description[:100] if description else '',
                        'representative': representative,
                        'suggested_name': None
                    }
                    
                    # 説明から企業名を推測
                    if description:
                        suggested = extract_company_name_from_description(description)
                        if suggested:
                            issue['suggested_name'] = suggested
                    
                    issues.append(issue)
    
    except Exception as e:
        print(f"❌ エラー: {csv_path.name} の読み込みに失敗しました: {e}")
        return issues
    
    # レポートを出力
    if output_path:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(issues, f, ensure_ascii=False, indent=2)
        print(f"📄 レポートを出力しました: {output_path}")
    else:
        # コンソールに出力
        print(f"\n{'='*60}")
        print(f"📋 修正が必要な行: {len(issues)}件")
        print(f"{'='*60}")
        for issue in issues[:20]:  # 最初の20件のみ表示
            print(f"行 {issue['row']}: {issue['current_name']}")
            if issue['suggested_name']:
                print(f"  → 推奨: {issue['suggested_name']}")
    
    return issues


def main():
    """メイン処理"""
    if len(sys.argv) < 2:
        print("使用方法: python fix_company_names.py <csv-file> [--report output.json]")
        print("例: python fix_company_names.py csv/36.csv --report report.json")
        sys.exit(1)
    
    csv_path = Path(sys.argv[1])
    output_path = None
    
    if '--report' in sys.argv:
        idx = sys.argv.index('--report')
        if idx + 1 < len(sys.argv):
            output_path = Path(sys.argv[idx + 1])
    
    if not csv_path.exists():
        print(f"❌ エラー: ファイルが見つかりません: {csv_path}")
        sys.exit(1)
    
    issues = generate_fix_report(csv_path, output_path)
    
    if issues:
        print(f"\n✅ 分析完了: {len(issues)}件の問題を検出しました")
        if output_path:
            print(f"   詳細は {output_path} を確認してください")
    else:
        print("\n✅ 問題は見つかりませんでした")


if __name__ == '__main__':
    main()
