#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/find_richest_csv.py
fixed_csv_3配下（later除く）から、以下の項目が最も充実しているCSVファイルを探します。
・代表者情報
・仕入先
・取引先
・取引先銀行
"""

import os
import re
from pathlib import Path
import pandas as pd

# --- 設定 ---
ROOT_DIR_NAME = "fixed_csv_3"
EXCLUDE_PATH_FRAGMENT = "/later/"

# ターゲットとするキーワード（列名に含まれるべき文字）
TARGET_KEYWORDS = ["代表", "仕入", "取引先", "銀行"]

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

    print(f"=== 探索開始: {ROOT_DIR_NAME} (laterを除く) ===")
    
    file_scores = []

    # ファイル探索
    for fp in root.rglob("*.csv"):
        path_str = str(fp.resolve()).replace("\\", "/")
        if EXCLUDE_PATH_FRAGMENT in path_str:
            continue
        
        # 129.csvは今回対象外（特殊フォーマットのため）
        if fp.name == "129.csv":
            continue

        try:
            enc = get_encoding(fp)
            # ヘッダーのみ読み込み
            df_head = pd.read_csv(fp, encoding=enc, dtype=str, nrows=1)
            cols = list(df_head.columns)

            # キーワードにマッチする列があるか確認
            matched_cols = []
            for kw in TARGET_KEYWORDS:
                found = [c for c in cols if kw in c]
                if found:
                    matched_cols.extend(found)
            
            # マッチした列が少なければスキップ（高速化）
            if len(set(matched_cols)) < 2:
                continue

            # 全行読み込んでデータ充実度（埋まっている率）を計算
            df = pd.read_csv(fp, encoding=enc, dtype=str, low_memory=False)
            
            score = 0
            details = []
            
            for kw in TARGET_KEYWORDS:
                target_cols = [c for c in df.columns if kw in c]
                if not target_cols:
                    continue
                
                # そのキーワードに関連する列の非NULL数をカウント
                # 複数の列がある場合は合算（例: 取引先1, 取引先2...）
                non_null_count = df[target_cols].notna().sum().sum()
                score += non_null_count
                details.append(f"{kw}:{non_null_count}")

            if score > 0:
                file_scores.append({
                    "file": f"{fp.parent.name}/{fp.name}",
                    "score": score,
                    "details": ", ".join(details),
                    "total_rows": len(df)
                })

        except Exception:
            continue

    # スコア順（充実度順）にソート
    file_scores.sort(key=lambda x: x["score"], reverse=True)

    print("\n【結果】情報充実度ランキング TOP5")
    print("-" * 60)
    for i, item in enumerate(file_scores[:5], 1):
        print(f"{i}. {item['file']}")
        print(f"   スコア: {item['score']} (行数: {item['total_rows']})")
        print(f"   内訳: {item['details']}")
        print("-" * 60)

if __name__ == "__main__":
    main()