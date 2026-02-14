#!/usr/bin/env python3
"""
csv_2フォルダ配下の全CSVに対する列ズレ補正スクリプト

目的: 業種4等の混入により右にズレたデータを左シフトして正しい位置に戻す
対象: csv_2 およびそのサブフォルダ内の .csv ファイル全て
出力: fixed_csv_2 に同じディレクトリ構造で保存（元ファイルは上書きしない）
"""

import csv
import os
import re
import shutil
from pathlib import Path

# 郵便番号パターン（3桁-4桁）
POSTAL_PATTERN = re.compile(r'^\d{3}-?\d{4}$')

BASE_DIR = Path(__file__).resolve().parent.parent
TARGET_DIR = BASE_DIR / 'csv_2'
OUTPUT_DIR = BASE_DIR / 'fixed_csv_2'


def find_postal_column_index(header):
    """ヘッダーから「郵便番号」列のインデックスを取得。複数ある場合は最初のものを返す。"""
    for i, col in enumerate(header):
        col_clean = str(col).strip().strip('\ufeff')
        if col_clean == '郵便番号':
            return i
    return -1


def fix_row(row, base_zip_idx, header_len):
    """
    1行のデータを補正する。
    郵便番号列(base_zip_idx)から右方向に郵便番号パターンを探し、
    見つかった位置に応じて左シフト量を決定して補正する。
    """
    shift_n = 0
    for i in range(base_zip_idx, min(len(row), base_zip_idx + 10)):
        val = (row[i] or '').strip()
        if POSTAL_PATTERN.match(val):
            shift_n = i - base_zip_idx
            break

    if shift_n > 0:
        new_row = row[:base_zip_idx] + row[base_zip_idx + shift_n:]
    else:
        new_row = row

    # ヘッダー列数に合わせる（不足は空欄、超過はカット）
    if len(new_row) < header_len:
        new_row = new_row + [''] * (header_len - len(new_row))
    else:
        new_row = new_row[:header_len]

    return new_row


def copy_unchanged(input_path, output_path):
    """ファイルをそのままコピーする（郵便番号列なしの場合など）。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(input_path, output_path)


def process_csv(input_path, output_path):
    """
    1つのCSVファイルを処理する。
    戻り値: (成功可否, シフト適用行数, スキップ理由: None=通常, 'no_postal'=郵便番号列なし)
    """
    try:
        with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f)
            header = next(reader)
            base_zip_idx = find_postal_column_index(header)
            if base_zip_idx < 0:
                return False, 0, 'no_postal'

            header_len = len(header)
            refined_rows = []
            shift_count = 0

            for row in reader:
                if not row:
                    continue
                fixed = fix_row(row, base_zip_idx, header_len)
                if row != fixed:
                    shift_count += 1
                refined_rows.append(fixed)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(refined_rows)

        return True, shift_count, None

    except Exception as e:
        print(f"  エラー: {e}")
        return False, 0, None


def main():
    target = Path(TARGET_DIR)
    if not target.exists():
        print(f"対象ディレクトリが存在しません: {target}")
        return

    output_base = Path(OUTPUT_DIR)
    output_base.mkdir(parents=True, exist_ok=True)

    csv_files = list(target.rglob('*.csv'))
    print(f"対象CSV数: {len(csv_files)}")
    print("-" * 50)

    ok_count = 0
    skip_count = 0
    err_count = 0
    total_shifts = 0

    for input_path in sorted(csv_files):
        rel = input_path.relative_to(target)
        output_path = output_base / rel

        success, shifts, skip_reason = process_csv(input_path, output_path)
        if success:
            ok_count += 1
            total_shifts += shifts
            if shifts > 0:
                print(f"補正: {rel} (シフト適用 {shifts} 行)")
        elif skip_reason == 'no_postal':
            skip_count += 1
            copy_unchanged(input_path, output_path)
        else:
            err_count += 1
            print(f"失敗: {rel}")

    print("-" * 50)
    print(f"処理完了: 成功={ok_count}, スキップ(郵便番号列なし)={skip_count}, エラー={err_count}")
    print(f"シフト補正を適用した行数合計: {total_shifts}")
    print(f"出力先: {output_base}")


if __name__ == '__main__':
    main()
