#!/usr/bin/env python3
"""
CSVファイルのヘッダーとデータ行の整合性をチェックするスクリプト
ヘッダー通りに値が入っていないファイルを洗い出します
"""

import csv
import os
import sys
from pathlib import Path
from collections import defaultdict

def check_csv_file(csv_path: Path) -> dict:
    """CSVファイルの整合性をチェック"""
    issues = {
        'file': str(csv_path),
        'header_count': 0,
        'data_row_count': 0,
        'mismatched_rows': [],
        'empty_rows': [],
        'errors': []
    }
    
    try:
        with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
            # まずファイル全体を読み込んで文字コードを確認
            content = f.read()
            
        # CSVパーサーで読み込み
        with open(csv_path, 'r', encoding='utf-8', errors='ignore') as f:
            reader = csv.reader(f)
            
            # ヘッダー行を取得
            try:
                header_row = next(reader)
                header_count = len([col for col in header_row if col.strip()])
                issues['header_count'] = header_count
            except StopIteration:
                issues['errors'].append('ヘッダー行が存在しません')
                return issues
            
            if header_count == 0:
                issues['errors'].append('ヘッダー行が空です')
                return issues
            
            # データ行をチェック
            row_num = 1  # ヘッダー行の次から
            for row in reader:
                row_num += 1
                data_cols = [col for col in row if col.strip()]
                actual_count = len(row)  # 空の列も含む
                
                if len(data_cols) == 0:
                    issues['empty_rows'].append(row_num)
                    continue
                
                issues['data_row_count'] += 1
                
                # 列数が一致しない場合
                if actual_count != header_count:
                    issues['mismatched_rows'].append({
                        'row': row_num,
                        'expected': header_count,
                        'actual': actual_count,
                        'data_cols': len(data_cols)
                    })
                    
    except Exception as e:
        issues['errors'].append(f'読み込みエラー: {str(e)}')
    
    return issues

def main():
    csv_dir = Path(__file__).parent.parent / 'csv'
    
    if not csv_dir.exists():
        print(f'❌ CSVディレクトリが見つかりません: {csv_dir}')
        sys.exit(1)
    
    # すべてのCSVファイルを取得
    csv_files = sorted(csv_dir.glob('*.csv'))
    
    if not csv_files:
        print('❌ CSVファイルが見つかりません')
        sys.exit(1)
    
    print(f'📊 {len(csv_files)}個のCSVファイルをチェック中...\n')
    
    problematic_files = []
    all_issues = []
    
    for csv_file in csv_files:
        issues = check_csv_file(csv_file)
        all_issues.append(issues)
        
        has_issues = (
            len(issues['mismatched_rows']) > 0 or
            len(issues['errors']) > 0 or
            issues['data_row_count'] == 0
        )
        
        if has_issues:
            problematic_files.append(issues)
    
    # 結果を表示
    print('=' * 80)
    print('🔍 チェック結果')
    print('=' * 80)
    
    if not problematic_files:
        print('✅ すべてのCSVファイルでヘッダーとデータ行の整合性が確認できました')
    else:
        print(f'\n⚠️  問題があるファイル: {len(problematic_files)}個\n')
        
        for issues in problematic_files:
            print(f"\n📄 {Path(issues['file']).name}")
            print(f"   ヘッダー列数: {issues['header_count']}")
            print(f"   データ行数: {issues['data_row_count']}")
            
            if issues['errors']:
                print(f"   ❌ エラー:")
                for error in issues['errors']:
                    print(f"      - {error}")
            
            if issues['mismatched_rows']:
                print(f"   ⚠️  列数不一致の行: {len(issues['mismatched_rows'])}行")
                # 最初の5行だけ表示
                for mismatch in issues['mismatched_rows'][:5]:
                    print(f"      行{mismatch['row']}: 期待{mismatch['expected']}列 / 実際{mismatch['actual']}列 (データ列: {mismatch['data_cols']})")
                if len(issues['mismatched_rows']) > 5:
                    print(f"      ... 他{len(issues['mismatched_rows']) - 5}行")
            
            if issues['empty_rows']:
                print(f"   ⚠️  空の行: {len(issues['empty_rows'])}行")
                if len(issues['empty_rows']) <= 10:
                    print(f"      行番号: {', '.join(map(str, issues['empty_rows']))}")
                else:
                    print(f"      行番号: {', '.join(map(str, issues['empty_rows'][:10]))} ... 他{len(issues['empty_rows']) - 10}行")
    
    # サマリーを表示
    print('\n' + '=' * 80)
    print('📊 サマリー')
    print('=' * 80)
    
    total_files = len(all_issues)
    problem_count = len(problematic_files)
    ok_count = total_files - problem_count
    
    print(f'総ファイル数: {total_files}')
    print(f'✅ 正常: {ok_count}')
    print(f'⚠️  問題あり: {problem_count}')
    
    # 問題があるファイルのリストを出力
    if problematic_files:
        print('\n問題があるファイル一覧:')
        for issues in problematic_files:
            filename = Path(issues['file']).name
            problems = []
            if issues['errors']:
                problems.append('エラー')
            if issues['mismatched_rows']:
                problems.append(f"列数不一致({len(issues['mismatched_rows'])}行)")
            if issues['data_row_count'] == 0:
                problems.append('データ行なし')
            print(f'  - {filename}: {", ".join(problems)}')
    
    # 詳細レポートをファイルに出力
    report_file = Path(__file__).parent.parent / 'csv_header_consistency_report.txt'
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write('CSVヘッダー整合性チェックレポート\n')
        f.write('=' * 80 + '\n\n')
        
        for issues in problematic_files:
            f.write(f"\n{'=' * 80}\n")
            f.write(f"ファイル: {Path(issues['file']).name}\n")
            f.write(f"ヘッダー列数: {issues['header_count']}\n")
            f.write(f"データ行数: {issues['data_row_count']}\n\n")
            
            if issues['errors']:
                f.write("エラー:\n")
                for error in issues['errors']:
                    f.write(f"  - {error}\n")
                f.write("\n")
            
            if issues['mismatched_rows']:
                f.write(f"列数不一致の行 ({len(issues['mismatched_rows'])}行):\n")
                for mismatch in issues['mismatched_rows']:
                    f.write(f"  行{mismatch['row']}: 期待{mismatch['expected']}列 / 実際{mismatch['actual']}列 (データ列: {mismatch['data_cols']})\n")
                f.write("\n")
            
            if issues['empty_rows']:
                f.write(f"空の行 ({len(issues['empty_rows'])}行):\n")
                f.write(f"  行番号: {', '.join(map(str, issues['empty_rows']))}\n")
                f.write("\n")
    
    print(f'\n📝 詳細レポートを保存しました: {report_file}')

if __name__ == '__main__':
    main()

