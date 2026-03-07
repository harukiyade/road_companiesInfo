#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/list_representative_headers.py
fixed_csv_3配下（later除く）のCSVをスキャンし、
「代表者」に関連するヘッダー（列名）を全て洗い出して一覧化する。
"""

import os
from pathlib import Path
import pandas as pd

# --- 設定 ---
ROOT_DIR_NAME = "fixed_csv_3"
EXCLUDE_PATH_FRAGMENT = "/later/"

# 検索キーワード（これらの文字が含まれるヘッダーを抽出）
KEYWORDS = ["代表", "社長", "CEO", "氏名", "住所", "誕生日", "生年月日", "郵便番号", "年齢", "出身"]

def get_encoding(file_path):
    for enc in ["utf-8-sig", "cp932", "utf-8"]:
        try:
            with open(file_path, "r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "utf-8"

def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    root = project_root / ROOT_DIR_NAME

    if not root.exists():
        print(f"ディレクトリが見つかりません: {root}")
        return

    print(f"=== 代表者関連ヘッダーの洗い出し開始: {ROOT_DIR_NAME} (laterを除く) ===")
    
    found_headers = set()
    header_samples = {}  # ヘッダーごとのサンプル値

    # ファイル探索
    for fp in root.rglob("*.csv"):
        path_str = str(fp.resolve()).replace("\\", "/")
        if EXCLUDE_PATH_FRAGMENT in path_str:
            continue
        
        # 129.csvは特殊なので今回は除外
        if fp.name == "129.csv":
            continue

        try:
            enc = get_encoding(fp)
            # ヘッダーのみ読み込み
            df = pd.read_csv(fp, encoding=enc, dtype=str, nrows=5)
            cols = list(df.columns)

            for c in cols:
                # キーワードが含まれるかチェック
                if any(k in c for k in KEYWORDS):
                    found_headers.add(c)
                    
                    # サンプル値を保持（まだなければ）
                    if c not in header_samples:
                        sample = df[c].dropna().iloc[0] if not df[c].dropna().empty else "(空)"
                        header_samples[c] = sample

        except Exception:
            continue

    print("\n【検出された代表者関連ヘッダー一覧】")
    print("-" * 60)
    print(f"{'ヘッダー名':<30} | サンプル値")
    print("-" * 60)
    
    # ソートして表示
    for h in sorted(list(found_headers)):
        print(f"{h:<30} | {header_samples.get(h, '')[:30]}")

    print("-" * 60)
    print("このリストを元に、DBカラムとのマッピングを行います。")

if __name__ == "__main__":
    main()