#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSV全量補正・フラット化スクリプト (決定版)

【機能】
1. 列ズレ補正: 郵便番号を基準に、混入したゴミデータを削除して左詰め
2. 列数補正: ヘッダーと列数が合わない行をパディング/トリミング
3. 重複回避: 「親フォルダ名_ファイル名.csv」の形式にリネームしてフラット化
4. ログ出力: ファイル名の変更履歴と補正数を出力
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

def get_encoding(file_path):
    """UTF-8 で正しく読めるか、CP932 が必要かを判定"""
    for enc in ["utf-8-sig", "utf-8", "cp932"]:
        try:
            with open(file_path, "r", encoding=enc) as f:
                content = f.read(10000)
            if enc.startswith("utf-8") and "\ufffd" in content:
                continue  # 置換文字＝文字化け
            if enc.startswith("utf-8") and "会社" not in content and "法人" not in content:
                continue  # 日本語なし＝CP932の可能性
            return enc
        except (UnicodeDecodeError, UnicodeError):
            continue
    return "utf-8"  # フォールバック


def is_postal_code(val):
    if not val: return False
    # 全角数字→半角、全角ハイフン→半角
    s = str(val).strip().translate(str.maketrans({chr(0xFF10 + i): chr(0x30 + i) for i in range(10)}))
    s = s.replace("ー", "-").replace("−", "-")
    return bool(POSTAL_REGEX.match(s))

def get_unique_filename(input_path, root_dir):
    """
    パス構造を元にユニークなファイル名を生成する
    例: csv_2/import_firstTime/1.csv -> import_firstTime_1.csv
    """
    try:
        # root_dir からの相対パスを取得
        rel_path = input_path.relative_to(root_dir)
        # フォルダ区切りをアンダースコアに置換
        parts = list(rel_path.parts)
        # 拡張子を除いた部分を結合
        name_body = "_".join(parts[:-1] + [parts[-1]])
        return name_body
    except ValueError:
        return input_path.name

def process_and_save(file_path, output_dir, root_dir):
    try:
        # 入力ファイル読み込み（CP932混在にも対応）
        enc = get_encoding(file_path)
        with open(file_path, "r", encoding=enc, errors="replace") as f_in:
            reader = csv.reader(f_in)
            try:
                header = next(reader)
            except StopIteration:
                print(f"SKIP (空ファイル): {file_path}")
                return False

            header_len = len(header)
            
            # ヘッダーから「郵便番号」の位置を探す
            postal_col_idx = -1
            for i, col in enumerate(header):
                if "郵便番号" in col:
                    postal_col_idx = i
                    break
            
            fixed_rows = []
            fix_count_postal = 0
            fix_count_pad = 0

            for row in reader:
                # ---------------------------------------------------
                # 1. 郵便番号アンカーによるズレ補正
                # ---------------------------------------------------
                if postal_col_idx != -1:
                    # 行の長さが足りない場合のガード
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
                            # 例: [A, B, ゴミ, ゴミ, 〒, C] -> [A, B, 〒, C]
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

        # ユニークなファイル名を生成
        new_filename = get_unique_filename(file_path, root_dir)
        out_path = output_dir / new_filename
        
        # 保存
        with open(out_path, "w", encoding="utf-8", newline="") as f_out:
            writer = csv.writer(f_out)
            writer.writerow(header)
            writer.writerows(fixed_rows)
        
        # ログ作成
        status_msgs = []
        if fix_count_postal > 0: status_msgs.append(f"ズレ補正:{fix_count_postal}件")
        if fix_count_pad > 0: status_msgs.append(f"列数調整:{fix_count_pad}件")
        
        status_str = f" -> {', '.join(status_msgs)}" if status_msgs else " (修正なし)"
        
        # 元のファイル名 -> 新しいファイル名 の履歴を出力
        print(f"RENAME: {file_path.relative_to(root_dir)} -> {new_filename}{status_str}")
            
        return True

    except Exception as e:
        print(f"ERROR: {file_path} - {e}")
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
    print("ファイルを検索中...")
    csv_files = sorted(list(INPUT_ROOT.rglob("*.csv")))
    total_files = len(csv_files)
    
    print(f"対象ルート: {INPUT_ROOT}")
    print(f"発見ファイル数: {total_files} 件")
    print("-" * 60)

    processed_count = 0
    for fp in csv_files:
        if process_and_save(fp, OUTPUT_DIR, INPUT_ROOT):
            processed_count += 1

    print("-" * 60)
    
    # 最終チェック
    output_count = len(list(OUTPUT_DIR.glob("*.csv")))
    
    print(f"  入力ファイル数: {total_files}")
    print(f"  出力ファイル数: {output_count}")
    
    if output_count == total_files:
        print("\n✅ 成功: すべてのファイルが重複なく fixed_csv_3 に格納されました。")
    else:
        print(f"\n❌ 警告: ファイル数が一致しません ({total_files - output_count} 件不足)。ログを確認してください。")

if __name__ == "__main__":
    main()