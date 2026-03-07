#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV列ズレ補正スクリプト (Postal Anchor Strategy v3)

【対象】
csv_2/import_firstTime/ 配下のCSVファイル

【ロジック】
1. ヘッダーの「郵便番号」列のインデックス(idx)を取得
2. 各行の idx の値を確認
   - 郵便番号形式(^\d{3}-?\d{4}$)ならOK
   - 違う場合、idx + 1 以降をスキャンして郵便番号を探す
3. 郵便番号が見つかった場合
   - 本来の位置(idx)から、見つかった位置(found_idx)の手前までを「ゴミデータ(業種混入)」とみなして削除
   - 列を左詰めする
4. 行末の列数不整合を空文字パディングで修正
"""

import csv
import os
import re
import sys
import shutil
from pathlib import Path

# --- 設定 ---
TARGET_DIR = Path("csv_2/import_firstTime")
OUTPUT_DIR = Path("fixed_csv_3")  # 確認用に別フォルダに出力

# 郵便番号の正規表現 (3桁-4桁 または 7桁ハイフンなし)
POSTAL_REGEX = re.compile(r'^\d{3}-?\d{4}$')

def is_postal_code(val):
    if not val: return False
    # 全角数字を半角に、全角ハイフンを半角に正規化してチェック
    s = str(val).strip().translate(str.maketrans({chr(0xFF10 + i): chr(0x30 + i) for i in range(10)}))
    s = s.replace("ー", "-").replace("−", "-")
    return bool(POSTAL_REGEX.match(s))

def process_file(file_path, output_path):
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f_in:
            reader = csv.reader(f_in)
            try:
                header = next(reader)
            except StopIteration:
                return False # 空ファイル

            # ヘッダーから「郵便番号」の位置を探す
            postal_col_idx = -1
            for i, col in enumerate(header):
                if "郵便番号" in col:
                    postal_col_idx = i
                    break
            
            # 郵便番号列がない場合は、そのままコピーして終了（またはスキップ）
            if postal_col_idx == -1:
                print(f"SKIP: {file_path.name} (郵便番号列なし)")
                return False

            fixed_rows = []
            fixed_count = 0
            header_len = len(header)

            for i, row in enumerate(reader, 1):
                # 行の長さが足りない場合は一旦パディング
                if len(row) < header_len:
                    row += [""] * (header_len - len(row))
                
                # ターゲットの値を取得
                current_val = row[postal_col_idx] if postal_col_idx < len(row) else ""

                if is_postal_code(current_val):
                    # 正常：何もしない
                    pass
                else:
                    # 異常：右側をスキャンして郵便番号を探す
                    found_idx = -1
                    # postal_col_idx + 1 から 行末まで探索
                    for search_idx in range(postal_col_idx + 1, len(row)):
                        if is_postal_code(row[search_idx]):
                            found_idx = search_idx
                            break
                    
                    if found_idx != -1:
                        # 発見！ -> 間の列（ゴミ）を削除して左詰め
                        # 例: [..., 本来, ゴミ1, ゴミ2, 実郵便, ...]
                        # 削除範囲: postal_col_idx 〜 found_idx - 1
                        
                        # スライス操作:
                        # 0〜本来の前 + 実郵便以降
                        new_row = row[:postal_col_idx] + row[found_idx:]
                        row = new_row
                        fixed_count += 1

                # 最終的な列数合わせ (ヘッダー長に合わせる)
                if len(row) > header_len:
                    row = row[:header_len] # 多すぎる場合は切る
                elif len(row) < header_len:
                    row += [""] * (header_len - len(row)) # 足りない場合は埋める

                fixed_rows.append(row)

        # ファイル書き出し
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8", newline="") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)
            writer.writerows(fixed_rows)
        
        if fixed_count > 0:
            print(f"FIXED: {file_path.name} -> {fixed_count} 行を補正しました")
        return True

    except Exception as e:
        print(f"ERROR: {file_path.name} - {e}")
        return False

def main():
    if not TARGET_DIR.exists():
        print(f"エラー: ディレクトリ {TARGET_DIR} が見つかりません。")
        return

    # 出力先をクリーンアップ
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir()

    csv_files = sorted(TARGET_DIR.glob("*.csv"))
    print(f"対象ファイル数: {len(csv_files)}")
    print("-" * 40)

    total_files_processed = 0
    
    for fp in csv_files:
        # 出力先のパス
        out_fp = OUTPUT_DIR / fp.name
        process_file(fp, out_fp)
        total_files_processed += 1

    print("-" * 40)
    print("処理完了。結果は fixed_csv_3/ に保存されました。")
    print("問題がなければ、以下のコマンドで適用してください：")
    print(f"cp -r {OUTPUT_DIR}/* {TARGET_DIR}/")

if __name__ == "__main__":
    main()