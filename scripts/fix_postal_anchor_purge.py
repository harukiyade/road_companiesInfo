#!/usr/bin/env python3
"""
【重要・再修正】CSVの列ズレ補正：郵便番号をアンカーとした特定列（業種4）の動的削除

対象: csv_2/import_firstTime/ の 36, 42, 43, 44, 47, 48, 49, 108, 119, 120, 121, 123, 124.csv
出力: fixed_csv_2/import_firstTime/ に同名で保存
"""

import csv
import re
from pathlib import Path

# 郵便番号パターン（3桁-4桁、厳密にハイフン必須）
POSTAL_PATTERN = re.compile(r'^\d{3}-\d{4}$')

BASE_DIR = Path(__file__).resolve().parent.parent
TARGET_FILES = [
    '36.csv', '42.csv', '43.csv', '44.csv', '47.csv', '48.csv', '49.csv',
    '108.csv', '119.csv', '120.csv', '121.csv', '123.csv', '124.csv'
]


def find_postal_column_index(header):
    """ヘッダーから「郵便番号」列のインデックスを取得。"""
    for i, col in enumerate(header):
        col_clean = str(col).strip().strip('\ufeff')
        if col_clean == '郵便番号':
            return i
    return -1


def is_valid_postal(val):
    """3桁-4桁形式の郵便番号か判定"""
    if not val or not isinstance(val, str):
        return False
    return bool(POSTAL_PATTERN.match(val.strip()))


def fix_row(row, base_zip_idx, header_len):
    """
    1行のデータを補正する。

    異常行の判定: 郵便番号列の値が3桁-4桁形式でない場合
    動的削除: 右にスキャンして最初の3桁-4桁を「本来の郵便番号」とし、
              その左側の余計なデータ（業種4等）を削除して左詰め
    正常行: 郵便番号列が既に正しい形式なら一切変更しない
    """
    if base_zip_idx >= len(row):
        # 列が足りない行はパディングのみ
        pad = [''] * (header_len - len(row))
        return row + pad if len(row) < header_len else row[:header_len]

    val_at_postal = (row[base_zip_idx] or '').strip()

    # 正常行：郵便番号列が既に3桁-4桁形式 → 一切変更しない
    if is_valid_postal(val_at_postal):
        if len(row) < header_len:
            return row + [''] * (header_len - len(row))
        if len(row) > header_len:
            return row[:header_len]
        return row

    # 異常行：郵便番号列に業種名などが入っている
    # 右に向かってスキャンし、最初に出現する3桁-4桁を「本来の郵便番号」として特定
    found_idx = -1
    for i in range(base_zip_idx, len(row)):
        v = (row[i] or '').strip()
        if is_valid_postal(v):
            found_idx = i
            break

    if found_idx < 0:
        # 郵便番号が見つからない場合は行をそのまま（長さのみ調整）
        if len(row) < header_len:
            return row + [''] * (header_len - len(row))
        return row[:header_len]

    # 余計なデータ（base_zip_idx ～ found_idx-1）を削除し、左に詰め直す
    # new_row = row[0:base_zip_idx] + row[found_idx:]
    new_row = row[:base_zip_idx] + row[found_idx:]

    # ヘッダー列数に合わせる
    if len(new_row) < header_len:
        new_row = new_row + [''] * (header_len - len(new_row))
    else:
        new_row = new_row[:header_len]

    return new_row


def process_file(input_path, output_path):
    """1ファイルを処理。戻り値: (成功, パージ適用行数)"""
    try:
        with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f)
            header = next(reader)
            base_zip_idx = find_postal_column_index(header)
            if base_zip_idx < 0:
                return False, 0

            header_len = len(header)
            refined_rows = []
            purge_count = 0

            for row in reader:
                if not row:
                    continue
                fixed = fix_row(row, base_zip_idx, header_len)
                if row != fixed:
                    purge_count += 1
                refined_rows.append(fixed)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(refined_rows)

        return True, purge_count

    except Exception as e:
        print(f"  エラー: {e}")
        return False, 0


def main():
    input_dir = BASE_DIR / 'csv_2' / 'import_firstTime'
    output_dir = BASE_DIR / 'fixed_csv_2' / 'import_firstTime'
    output_dir.mkdir(parents=True, exist_ok=True)

    print("【郵便番号アンカー補正】対象13ファイル")
    print("-" * 50)

    total_purge = 0
    for name in TARGET_FILES:
        input_path = input_dir / name
        if not input_path.exists():
            print(f"スキップ（存在なし）: {name}")
            continue

        output_path = output_dir / name
        ok, purge = process_file(input_path, output_path)
        if ok:
            total_purge += purge
            if purge > 0:
                print(f"補正: {name} (パージ適用 {purge} 行)")
            else:
                print(f"処理: {name} (変更なし)")
        else:
            print(f"失敗: {name}")

    print("-" * 50)
    print(f"パージ適用行数合計: {total_purge}")
    print(f"出力先: {output_dir}")


if __name__ == '__main__':
    main()
