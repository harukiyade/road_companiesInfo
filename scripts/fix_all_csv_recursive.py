#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV全量補正・集約スクリプト

【目的】
csv_2 フォルダ配下にある全てのCSVファイルを対象に、
1. 郵便番号をアンカーとした列ズレ補正 (パターンA対策)
2. ヘッダーと列数が合わない行のパディング (パターンB対策)
を行い、結果を全て fixed_csv_3 フォルダにフラットに出力する。

【解決する問題】
- 前回のスクリプトで対象外だったフォルダのファイルも全て処理する
- ファイル数の欠損 (187 -> 121) を解消し、全量を移動させる
"""

import csv
import os
import re
import sys
import shutil
from pathlib import Path

# --- 設定 ---
INPUT_ROOT = Path("csv_2")
OUTPUT_DIR = Path("fixed_csv_3")

# 郵便番号の正規表現 (3桁-4桁 または 7桁ハイフンなし)
POSTAL_REGEX = re.compile(r'^\d{3}-?\d{4}$')

def is_postal_code(val):
    if not val: return False
    # 全角数字→半角、全角ハイフン→半角
    s = str(val).strip().translate(str.maketrans({chr(0xFF10 + i): chr(0x30 + i) for i in range(10)}))
    s = s.replace("ー", "-").replace("−", "-")
    return bool(POSTAL_REGEX.match(s))

def process_and_save(file_path, output_dir):
    try:
        # ファイル読み込み
        with open(file_path, "r", encoding="utf-8", errors="replace") as f_in:
            reader = csv.reader(f_in)
            try:
                header = next(reader)
            except StopIteration:
                print(f"SKIP: {file_path.name} (空ファイル)")
                return False

            header_len = len(header)
            
            # ヘッダーから「郵便番号」の位置を探す
            postal_col_idx = -1
            for i, col in enumerate(header):
                if "郵便番号" in col:
                    postal_col_idx = i
                    break
            
            fixed_rows = []
            fix_count_postal = 0  # 郵便番号ズレ補正数
            fix_count_pad = 0     # パディング補正数

            for row in reader:
                # ---------------------------------------------------
                # 1. 郵便番号アンカーによるズレ補正 (Postal Anchor)
                # ---------------------------------------------------
                if postal_col_idx != -1:
                    # 行の長さが足りず、郵便番号列にアクセスできない場合は一旦パディング
                    if len(row) <= postal_col_idx:
                         row += [""] * (postal_col_idx - len(row) + 1)

                    current_val = row[postal_col_idx]
                    
                    if not is_postal_code(current_val):
                        # 異常検知：右側をスキャン
                        found_idx = -1
                        for search_idx in range(postal_col_idx + 1, len(row)):
                            if is_postal_code(row[search_idx]):
                                found_idx = search_idx
                                break
                        
                        if found_idx != -1:
                            # 補正実行: 本来の位置〜見つかった位置の前までを削除
                            new_row = row[:postal_col_idx] + row[found_idx:]
                            row = new_row
                            fix_count_postal += 1

                # ---------------------------------------------------
                # 2. 列数不整合の補正 (Padding / Trimming)
                # ---------------------------------------------------
                original_len = len(row)
                if original_len < header_len:
                    # 足りない -> 空文字で埋める
                    row += [""] * (header_len - original_len)
                    fix_count_pad += 1
                elif original_len > header_len:
                    # 多すぎる -> 切り詰める
                    row = row[:header_len]
                    fix_count_pad += 1

                fixed_rows.append(row)

        # ファイル書き出し (出力先フォルダに保存)
        out_path = output_dir / file_path.name
        
        # もし同名ファイルが既に存在する場合の警告 (上書きします)
        if out_path.exists():
            print(f"WARNING: 同名ファイルが上書きされます -> {file_path.name}")

        with open(out_path, "w", encoding="utf-8", newline="") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)
            writer.writerows(fixed_rows)
        
        # ログ出力
        msg = []
        if fix_count_postal > 0: msg.append(f"郵便番号ズレ修正:{fix_count_postal}件")
        if fix_count_pad > 0: msg.append(f"列数パディング:{fix_count_pad}件")
        
        if msg:
            print(f"FIXED: {file_path.name} -> {', '.join(msg)}")
        else:
            # 変更がなくてもコピー完了として表示
            print(f"COPY : {file_path.name} (修正なし)")
            
        return True

    except Exception as e:
        print(f"ERROR: {file_path.name} - {e}")
        return False

def main():
    if not INPUT_ROOT.exists():
        print(f"エラー: 入力フォルダ {INPUT_ROOT} が見つかりません。")
        return

    # 出力先を初期化 (再作成)
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir()

    # 再帰的に全てのCSVファイルを検索
    csv_files = sorted(list(INPUT_ROOT.rglob("*.csv")))
    total_files = len(csv_files)
    
    print(f"対象ルート: {INPUT_ROOT}")
    print(f"発見ファイル数: {total_files} 件")
    print("-" * 50)

    processed_count = 0
    for fp in csv_files:
        if process_and_save(fp, OUTPUT_DIR):
            processed_count += 1

    print("-" * 50)
    print(f"処理完了: {processed_count} / {total_files} ファイルを {OUTPUT_DIR} に出力しました。")
    
    # 最終チェック
    output_count = len(list(OUTPUT_DIR.glob("*.csv")))
    print(f"出力先ファイル数: {output_count}")
    
    if output_count == total_files:
        print("✅ ファイル数は一致しています。")
    else:
        print(f"⚠️ 差分があります: {total_files - output_count} 件不足 (同名ファイルの上書き等が原因の可能性があります)")

if __name__ == "__main__":
    main()