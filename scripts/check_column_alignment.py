#!/usr/bin/env python3
"""列のずれを検出：ヘッダー列数とデータ行の列数が一致するかチェック"""
import csv
from pathlib import Path

def check_file(filepath: str, encodings=None):
    encodings = encodings or ['utf-8', 'utf-8-sig', 'cp932', 'shift_jis']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc, errors='replace') as f:
                reader = csv.reader(f)
                header = next(reader)
                header_len = len(header)
                mismatches = []
                for i, row in enumerate(reader):
                    if len(row) != header_len:
                        mismatches.append((i + 2, len(row), header_len))  # +2 for 1-indexed and header
                return header_len, mismatches, enc
        except Exception:
            continue
    return None, [], None

def main():
    csv2_dir = Path(__file__).parent.parent / 'csv_2'
    results = []
    for fp in sorted(csv2_dir.rglob('*.csv')):
        rel = str(fp.relative_to(csv2_dir))
        hlen, mismatches, enc = check_file(str(fp))
        if hlen is None:
            results.append((rel, "読取失敗", [], None))
        elif mismatches:
            results.append((rel, hlen, mismatches[:20], enc))
    for rel, hlen, mismatches, enc in results:
        if isinstance(hlen, str):
            print(f"{rel}: {hlen}")
        elif mismatches:
            total = len(mismatches)
            sample = mismatches[:5]
            print(f"{rel}: ヘッダー{hlen}列 vs 不一致行{total}件 (例: 行{sample[0][0]}は{sample[0][1]}列)")

if __name__ == '__main__':
    main()
