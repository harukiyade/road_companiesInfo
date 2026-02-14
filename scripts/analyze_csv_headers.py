#!/usr/bin/env python3
"""
csv_2配下の全CSVファイルのヘッダーを分析し、種類・並び順でグループ分けするスクリプト
"""
import csv
import os
from collections import defaultdict
from pathlib import Path

def get_csv_header(filepath: str, encodings: list[str] = None) -> tuple[str | None, str | None]:
    """CSVの1行目（ヘッダー）を取得。複数エンコーディングで試行"""
    encodings = encodings or ['utf-8', 'utf-8-sig', 'cp932', 'shift_jis', 'iso-2022-jp']
    for enc in encodings:
        try:
            with open(filepath, 'r', encoding=enc, errors='replace') as f:
                reader = csv.reader(f)
                header = next(reader)
                # 空のカラム名を正規化
                header = [h.strip() if h else '' for h in header]
                return header, enc
        except Exception as e:
            continue
    return None, None

def normalize_header_for_comparison(header: list[str]) -> str:
    """比較用にヘッダーを正規化（Unnamed系を除去して統一）"""
    cleaned = [h for h in header if h and not h.startswith('Unnamed')]
    return '|'.join(cleaned)

def main():
    csv2_dir = Path(__file__).parent.parent / 'csv_2'
    if not csv2_dir.exists():
        print(f"ディレクトリが存在しません: {csv2_dir}")
        return

    # ヘッダー文字列 -> ファイル一覧
    header_groups: dict[str, list[str]] = defaultdict(list)
    header_to_raw: dict[str, list[str]] = {}  # 代表的な生ヘッダー
    encoding_info: dict[str, str] = {}  # ファイル -> 検出エンコーディング

    csv_files = sorted(csv2_dir.rglob('*.csv'))
    failed_files = []

    for fp in csv_files:
        rel_path = str(fp.relative_to(csv2_dir))
        header, enc = get_csv_header(str(fp))
        if header is None:
            failed_files.append(rel_path)
            continue
        encoding_info[rel_path] = enc or 'unknown'

        key = normalize_header_for_comparison(header)
        header_groups[key].append(rel_path)
        if key not in header_to_raw:
            header_to_raw[key] = header

    # 結果を出力
    print("=" * 80)
    print("CSV_2 ヘッダー分析レポート")
    print("=" * 80)
    print(f"\n総CSVファイル数: {len(csv_files)}")
    print(f"分析成功: {len(csv_files) - len(failed_files)}")
    print(f"分析失敗: {len(failed_files)}")
    if failed_files:
        print("\n失敗したファイル:")
        for f in failed_files[:20]:
            print(f"  - {f}")
        if len(failed_files) > 20:
            print(f"  ... 他 {len(failed_files) - 20} 件")

    print("\n" + "=" * 80)
    print("グループ別一覧（ヘッダー種類・並び順で分類）")
    print("=" * 80)

    for i, (key, files) in enumerate(sorted(header_groups.items(), key=lambda x: -len(x[1])), 1):
        raw = header_to_raw.get(key, [])
        col_count = len([h for h in raw if h and not h.startswith('Unnamed')])
        print(f"\n--- グループ {i} (ファイル数: {len(files)}, 列数: {col_count}) ---")
        print("ヘッダー:")
        print("  " + ", ".join(raw[:15]))
        if len(raw) > 15:
            print("  ... " + ", ".join(raw[15:20]))
            if len(raw) > 20:
                print(f"  ... 他 {len(raw) - 20} 列 (Unnamed含む)")
        print("\n該当ファイル（先頭10件）:")
        for f in sorted(files)[:10]:
            enc = encoding_info.get(f, '')
            print(f"  - {f} (enc: {enc})")
        if len(files) > 10:
            print(f"  ... 他 {len(files) - 10} 件")

    # エンコーディング別サマリ
    enc_counts: dict[str, int] = defaultdict(int)
    for enc in encoding_info.values():
        enc_counts[enc] += 1
    print("\n" + "=" * 80)
    print("エンコーディング別ファイル数")
    print("=" * 80)
    for enc, cnt in sorted(enc_counts.items(), key=lambda x: -x[1]):
        print(f"  {enc}: {cnt}")

if __name__ == '__main__':
    main()
