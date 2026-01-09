#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全8要件対応 CSV修正スクリプト

要件1: 12.csvグループの重複統合 → Firestore側で実施
要件2: 法人番号バリデーション → backfill_companies_from_csv.tsで実施済み
要件3: 111.csv, 116.csvタイプの修正
要件4: 127.csvタイプのヘッダー日本語化
要件5: 130.csv, 131.csvのヘッダー日本語化 → 既に実施済み
要件6: 132.csvタイプの大量フィールド削除
要件7, 8: 133.csvタイプの修正
"""

import csv
import os
import sys
import re

def is_postal_code(value):
    """郵便番号判定（3桁-4桁）"""
    if not value:
        return False
    v = str(value).strip()
    # 3桁-4桁 または 7桁
    return bool(re.match(r'^\d{3}-\d{4}$', v) or re.match(r'^\d{7}$', v))

def is_probably_address(value):
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

def fix_type_d_e(input_file, output_file):
    """
    要件3: タイプD（111-115.csv）、タイプE（116-117.csv）の修正
    
    1. 内部管理フィールド削除: ID, 取引種別, SBフラグ, NDA, AD, ステータス, 備考
    2. 業種4ヘッダー追加（業種3と郵便番号の間）
    3. 郵便番号判定修正
    4. 住所フィールド修正
    """
    print(f"\n📝 {os.path.basename(input_file)} を修正中...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        # 削除するフィールドのインデックス
        remove_indices = set()
        remove_fields = ['ID', '取引種別', 'SBフラグ', 'NDA', 'AD', 'ステータス', '備考']
        
        for i, h in enumerate(headers):
            if h in remove_fields:
                remove_indices.add(i)
        
        # 新しいヘッダー作成
        new_headers = []
        for i, h in enumerate(headers):
            if i not in remove_indices:
                new_headers.append(h)
        
        # 業種4の位置を特定して挿入
        if '業種3' in new_headers and '郵便番号' in new_headers:
            idx_industry3 = new_headers.index('業種3')
            idx_postal = new_headers.index('郵便番号')
            
            # 業種3と郵便番号の間に業種4がなければ追加
            if idx_postal == idx_industry3 + 1:
                new_headers.insert(idx_postal, '業種4')
                print(f"  ✅ 業種4ヘッダーを追加しました（位置: {idx_postal}）")
        
        # データ行を処理
        rows = []
        fixed_count = 0
        
        for row in reader:
            # 削除フィールドを除去
            new_row = [row[i] for i in range(len(row)) if i not in remove_indices]
            
            # 業種4挿入位置を決定
            if '業種4' in new_headers:
                idx_industry4 = new_headers.index('業種4')
                idx_postal = new_headers.index('郵便番号')
                idx_address = new_headers.index('住所') if '住所' in new_headers else -1
                
                # 現在の業種4位置の値をチェック
                if len(new_row) > idx_industry4:
                    current_value = new_row[idx_industry4] if idx_industry4 < len(new_row) else ''
                    
                    # 郵便番号が入っている場合は業種4を空に、次の値を郵便番号に
                    if is_postal_code(current_value):
                        # 業種4を空に
                        new_row.insert(idx_industry4, '')
                        # 郵便番号フィールドは現在の値を使用
                        # 住所を右隣の値に修正
                        if idx_address > 0 and idx_address + 1 < len(new_row):
                            if is_postal_code(new_row[idx_address]):
                                new_row[idx_address] = new_row[idx_address + 1] if idx_address + 1 < len(new_row) else ''
                                fixed_count += 1
                    else:
                        # 業種4らしい値が入っている
                        # そのまま使用
                        pass
            
            rows.append(new_row)
        
        # 出力
        with open(output_file, 'w', encoding='utf-8', newline='') as out:
            writer = csv.writer(out)
            writer.writerow(new_headers)
            writer.writerows(rows)
        
        print(f"  ✅ 完了: {len(rows)}行処理、{fixed_count}行修正")

def fix_type_g(input_file, output_file):
    """
    要件4: タイプG（127.csv, 128.csv）のヘッダー日本語化
    """
    print(f"\n📝 {os.path.basename(input_file)} を修正中...")
    
    # ヘッダーマッピング
    header_mapping = {
        'name': '会社名',
        'nameEn': '会社名（英語）',
        'corporateNumber': '法人番号',
        'prefecture': '都道府県',
        'address': '住所',
        'industry': '業種',
        'capitalStock': '資本金',
        'revenue': '売上',
        'latestRevenue': '直近売上',
        'latestProfit': '直近利益',
        'employeeCount': '従業員数',
        'issuedShares': '発行株式数',
        'established': '設立',
        'fiscalMonth': '決算月',
        'listing': '上場',
        'representativeName': '代表者名',
        'representativeTitle': '代表者役職',
        'banks': '銀行',
        'phoneNumber': '電話番号',
        'companyUrl': 'URL',
        'contactFormUrl': '問い合わせURL',
        'email': 'メールアドレス',
        'fax': 'FAX',
        'postalCode': '郵便番号',
    }
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        # ヘッダーを日本語化
        new_headers = [header_mapping.get(h, h) for h in headers]
        
        rows = list(reader)
    
    with open(output_file, 'w', encoding='utf-8', newline='') as out:
        writer = csv.writer(out)
        writer.writerow(new_headers)
        writer.writerows(rows)
    
    print(f"  ✅ 完了: ヘッダーを日本語化、{len(rows)}行処理")

def fix_type_i(input_file, output_file):
    """
    要件6: タイプI（132.csv）の大量フィールド削除
    """
    print(f"\n📝 {os.path.basename(input_file)} を修正中...")
    
    remove_fields = [
        'ID', '取引種別', 'SBフラグ', 'NDA', 'AD', 'ステータス', '備考',
        '売DM最終送信日時', '買DM最終送信日時', '売手紙最終送付日時',
        '買手最終荷電日時', '社長手紙最終送付日時', 'SDS手紙最終送付日時',
        'SDS社長手紙最終送付日時'
    ]
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        # 削除するフィールドのインデックス
        remove_indices = set()
        for i, h in enumerate(headers):
            if h in remove_fields:
                remove_indices.add(i)
        
        # 新しいヘッダー
        new_headers = [headers[i] for i in range(len(headers)) if i not in remove_indices]
        
        # データ行を処理
        rows = []
        for row in reader:
            new_row = [row[i] for i in range(len(row)) if i < len(headers) and i not in remove_indices]
            rows.append(new_row)
    
    with open(output_file, 'w', encoding='utf-8', newline='') as out:
        writer = csv.writer(out)
        writer.writerow(new_headers)
        writer.writerows(rows)
    
    print(f"  ✅ 完了: {len(remove_indices)}個のフィールド削除、{len(rows)}行処理")

def fix_type_j(input_file, output_file):
    """
    要件7, 8: タイプJ（133-136.csv）の修正
    
    1. 会社ID削除
    2. 代表者名フィールド確認・追加
    """
    print(f"\n📝 {os.path.basename(input_file)} を修正中...")
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        
        # 会社ID削除
        remove_indices = set()
        for i, h in enumerate(headers):
            if h == '会社ID':
                remove_indices.add(i)
        
        # 新しいヘッダー
        new_headers = [headers[i] for i in range(len(headers)) if i not in remove_indices]
        
        # 代表者名フィールドの確認
        if '代表者名' not in new_headers and '代表名' in new_headers:
            # 代表名を代表者名に変更
            new_headers = ['代表者名' if h == '代表名' else h for h in new_headers]
            print(f"  ✅ '代表名' → '代表者名' に変更")
        
        # データ行を処理
        rows = []
        for row in reader:
            new_row = [row[i] for i in range(len(row)) if i < len(headers) and i not in remove_indices]
            rows.append(new_row)
    
    with open(output_file, 'w', encoding='utf-8', newline='') as out:
        writer = csv.writer(out)
        writer.writerow(new_headers)
        writer.writerows(rows)
    
    print(f"  ✅ 完了: 会社ID削除、{len(rows)}行処理")

def main():
    print("\n🔧 全要件対応 CSV修正スクリプト")
    print("="*60)
    
    csv_dir = 'csv'
    
    # 要件3: タイプD, E（111-117.csv）
    print("\n【要件3】タイプD, E の修正")
    type_de_files = ['111.csv', '112.csv', '113.csv', '114.csv', '115.csv', '116.csv', '117.csv']
    for filename in type_de_files:
        input_path = os.path.join(csv_dir, filename)
        output_path = os.path.join(csv_dir, filename.replace('.csv', '_fixed.csv'))
        if os.path.exists(input_path):
            try:
                fix_type_d_e(input_path, output_path)
            except Exception as e:
                print(f"  ❌ エラー: {e}")
    
    # 要件4: タイプG（127-128.csv）
    print("\n【要件4】タイプG の修正")
    type_g_files = ['127.csv', '128.csv']
    for filename in type_g_files:
        input_path = os.path.join(csv_dir, filename)
        output_path = os.path.join(csv_dir, filename.replace('.csv', '_fixed.csv'))
        if os.path.exists(input_path):
            try:
                fix_type_g(input_path, output_path)
            except Exception as e:
                print(f"  ❌ エラー: {e}")
    
    # 要件6: タイプI（132.csv）
    print("\n【要件6】タイプI の修正")
    input_path = os.path.join(csv_dir, '132.csv')
    output_path = os.path.join(csv_dir, '132_fixed.csv')
    if os.path.exists(input_path):
        try:
            fix_type_i(input_path, output_path)
        except Exception as e:
            print(f"  ❌ エラー: {e}")
    
    # 要件7, 8: タイプJ（133-136.csv）
    print("\n【要件7, 8】タイプJ の修正")
    type_j_files = ['133.csv', '134.csv', '135.csv', '136.csv']
    for filename in type_j_files:
        input_path = os.path.join(csv_dir, filename)
        output_path = os.path.join(csv_dir, filename.replace('.csv', '_fixed.csv'))
        if os.path.exists(input_path):
            try:
                fix_type_j(input_path, output_path)
            except Exception as e:
                print(f"  ❌ エラー: {e}")
    
    print("\n" + "="*60)
    print("🎉 CSV修正完了！")
    print("\n📌 次のステップ:")
    print("   1. 修正ファイル確認")
    print("   2. 元ファイルと置き換え:")
    print("      cd csv")
    print("      for f in *_fixed.csv; do mv \"$f\" \"${f/_fixed/}\"; done")
    print("      cd ..")
    print("   3. バックフィル実行:")
    print("      bash scripts/run_backfill_by_type.sh")
    print("")

if __name__ == '__main__':
    main()

