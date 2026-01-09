#!/usr/bin/env python3
import sys
from pathlib import Path
import shutil

import pandas as pd
import chardet

"""
使い方:
  python convert_to_csv.py /path/to/root

/root 配下を再帰的に探索し、
- .xlsx / .xls: CSV に変換
- .csv      : 文字コードを判別して読み込み、UTF-8 の CSV として書き出し直し
を ./output_csv/ 以下に同じフォルダ構造で保存します。
"""

def main(root_dir: Path):
    if not root_dir.exists():
        print(f"❌ 指定ディレクトリが存在しません: {root_dir}")
        sys.exit(1)

    output_root = root_dir / "output_csv"
    output_root.mkdir(exist_ok=True)
    print(f"出力先: {output_root}")

    exts_excel = {".xlsx", ".xls", ".xlsm"}
    exts_csv = {".csv"}

    count_excel = 0
    count_csv_conv = 0

    for path in root_dir.rglob("*"):
        if not path.is_file():
            continue

        suffix = path.suffix.lower()

        # 出力先フォルダ自身は無視
        if output_root in path.parents:
            continue

        rel = path.relative_to(root_dir)
        out_path = output_root / rel
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # すでに CSV のもの → 文字コードを判定して UTF-8 で書き出し直す
        if suffix in exts_csv:
            out_csv_path = out_path  # 拡張子はそのまま .csv を想定
            try:
                # まずバイト列から文字コードを推定
                with open(path, "rb") as fb:
                    raw = fb.read()  # 全体を読む
                detected = chardet.detect(raw) or {}
                enc = detected.get("encoding") or "cp932"

                print(f"[CSV->CSV] {rel} (encoding={enc}) -> {out_csv_path.relative_to(root_dir)}")
                # 推定したエンコーディングでデコードし、UTF-8 で書き出し直す
                text = raw.decode(enc, errors="ignore")
                # 改行コードはそのままでもよいが、気になる場合は \r\n を \n に揃えるなど調整可能
                with open(out_csv_path, "w", encoding="utf-8", newline="") as fw:
                    fw.write(text)
                count_csv_conv += 1
            except Exception as e:
                # 変換に失敗した場合は念のためコピーだけしておく
                print(f"⚠️ CSV 変換失敗: {rel} ({e}) -> コピーのみ")
                shutil.copy2(path, out_csv_path)
                count_csv_conv += 1
            continue

        # Excel ファイル → CSV に変換
        if suffix in exts_excel:
            # .xlsx → .csv に拡張子変更
            out_csv_path = out_path.with_suffix(".csv")
            try:
                print(f"[XLSX->CSV] {rel} -> {out_csv_path.relative_to(root_dir)}")
                df = pd.read_excel(path)
                df.to_csv(out_csv_path, index=False)
                count_excel += 1
            except Exception as e:
                print(f"⚠️ 変換失敗: {rel} ({e})")
            continue

        # それ以外の拡張子は無視
        # print(f"[SKIP] {rel}")

    print("========== サマリ ==========")
    print(f"Excel を CSV に変換      : {count_excel} 件")
    print(f"既存 CSV を再エンコード : {count_csv_conv} 件")
    print(f"出力先フォルダ           : {output_root}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("使い方: python convert_to_csv.py /path/to/root")
        sys.exit(1)
    root = Path(sys.argv[1]).expanduser().resolve()
    main(root)