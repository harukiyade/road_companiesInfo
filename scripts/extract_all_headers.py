#!/usr/bin/env python3
"""csv_2配下の全CSVのヘッダー（列名）を洗い出す"""
import csv
from pathlib import Path
from collections import defaultdict

def get_headers(filepath: str, encodings=None):
    encodings = encodings or ['utf-8', 'utf-8-sig', 'cp932', 'shift_jis']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc, errors='replace') as f:
                reader = csv.reader(f)
                header = next(reader)
                return [h.strip() if h else '(空)' for h in header], enc
        except Exception:
            continue
    return None, None

def main():
    csv2_dir = Path(__file__).parent.parent / 'csv_2'
    # 列名 -> 出現したファイル一覧
    col_to_files = defaultdict(list)
    # 全列名のユニークリスト（出現順をある程度保持）
    seen_cols = []
    col_set = set()
    failed = []

    for fp in sorted(csv2_dir.rglob('*.csv')):
        rel = str(fp.relative_to(csv2_dir))
        headers, enc = get_headers(str(fp))
        if headers is None:
            failed.append(rel)
            continue
        for col in headers:
            if col not in col_set:
                col_set.add(col)
                seen_cols.append(col)
            col_to_files[col].append(rel)

    # 出力: 1. 全ユニーク列名一覧（出現頻度順）
    print("=" * 80)
    print("【1】全ユニーク列名一覧（出現ファイル数順）")
    print("=" * 80)
    sorted_cols = sorted(col_to_files.items(), key=lambda x: -len(x[1]))
    for i, (col, files) in enumerate(sorted_cols, 1):
        count = len(files)
        print(f"{i:3}. {col!r}  ({count}ファイル)")

    # 出力: 2. 列名ごとの出現ファイル
    print("\n" + "=" * 80)
    print("【2】列名ごとの出現ファイル一覧")
    print("=" * 80)
    for col, files in sorted_cols:
        # ファイルが多い場合は最初の5件＋他N件
        if len(files) <= 10:
            file_list = ", ".join(files)
        else:
            file_list = ", ".join(files[:5]) + f", ... 他{len(files)-5}件"
        print(f"\n{col!r}:")
        print(f"  → {file_list}")

    # 出力: 3. CSVテキストとして全列名を出力（分析用）
    print("\n" + "=" * 80)
    print("【3】全ユニーク列名（カンマ区切り・分析用コピー）")
    print("=" * 80)
    print(",".join(sorted_cols[i][0] for i in range(len(sorted_cols))))

    if failed:
        print("\n読取失敗:", failed)

if __name__ == '__main__':
    main()
