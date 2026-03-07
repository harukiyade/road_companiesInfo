#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
csv_2 配下のCSVを CP932/Shift-JIS から UTF-8 に変換して上書き保存する。
"""

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CSV2_DIR = PROJECT_ROOT / "csv_2"


def detect_encoding(file_path):
    """utf-8 で読めるか、cp932 が必要かを判定"""
    for enc in ["utf-8-sig", "utf-8", "cp932"]:
        try:
            with open(file_path, "r", encoding=enc) as f:
                content = f.read()
            # UTF-8 で読めて日本語が含まれる
            if enc.startswith("utf-8") and "\ufffd" not in content:
                return "utf-8"
            if enc == "cp932":
                return "cp932"
        except (UnicodeDecodeError, UnicodeError):
            continue
    return None


def convert_to_utf8(target_dir, dry_run=False):
    """対象ディレクトリ配下のCSVを UTF-8 に変換"""
    if not target_dir.exists():
        print(f"ディレクトリが存在しません: {target_dir}", file=sys.stderr)
        return 1

    files = sorted(target_dir.rglob("*.csv"))
    converted = 0
    skipped = 0

    for fp in files:
        enc = detect_encoding(fp)
        if enc == "utf-8":
            skipped += 1
            continue
        if enc != "cp932":
            print(f"  スキップ（エンコーディング不明）: {fp.relative_to(target_dir)}", file=sys.stderr)
            continue

        try:
            with open(fp, "r", encoding="cp932") as f:
                content = f.read()
            if not dry_run:
                with open(fp, "w", encoding="utf-8", newline="") as f:
                    f.write(content)
            rel = fp.relative_to(target_dir)
            print(f"  {'[dry-run] ' if dry_run else ''}変換: {rel}")
            converted += 1
        except Exception as e:
            print(f"  エラー {fp.name}: {e}", file=sys.stderr)

    print(f"\n変換: {converted}件, スキップ(既にUTF-8): {skipped}件")
    return 0


def main():
    import argparse
    parser = argparse.ArgumentParser(description="csv_2 配下のCSVを UTF-8 に変換")
    parser.add_argument("--dry-run", action="store_true", help="実際には保存せずログのみ")
    parser.add_argument("--dir", default=None, help="対象ディレクトリ（省略時: csv_2）")
    args = parser.parse_args()
    target = Path(args.dir) if args.dir else CSV2_DIR
    if args.dry_run:
        print("【dry-run モード】実際には保存しません\n")
    return convert_to_utf8(target, dry_run=args.dry_run)


if __name__ == "__main__":
    sys.exit(main())
