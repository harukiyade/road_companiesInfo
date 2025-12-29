#!/usr/bin/env python3
# parse_industries.py
# 複数フォーマットに対応した業種分類パーサー
# 使い方:
#   python3 parse_industries.py industries.txt industries.csv [--debug]

import sys
import csv
import re
import argparse

def detect_format(lines):
    """ファイル形式を自動検出"""
    markdown_count = 0
    numbered_list_count = 0
    tsv_count = 0
    csv_count = 0
    
    for line in lines[:100]:  # 先頭100行をサンプル
        stripped = line.strip()
        if not stripped:
            continue
            
        # Markdown階層形式: ## / ### / -
        if stripped.startswith("## "):
            markdown_count += 1
        elif stripped.startswith("### "):
            markdown_count += 1
        elif stripped.startswith("- "):
            markdown_count += 1
        
        # 番号付きリスト形式: 数字. / タブ+数字. / タブ+• 
        if re.match(r'^\d+\.\s+', stripped):
            numbered_list_count += 1
        elif re.match(r'^\t+\d+\.\s+', stripped):
            numbered_list_count += 1
        elif re.match(r'^\t+[•\*]\s*\d+\s+', stripped) or re.match(r'^\t+[•\*]\t', stripped):
            numbered_list_count += 1
        
        # TSV形式: タブ区切り
        parts = stripped.split('\t')
        if len(parts) == 3 and all(p.strip() for p in parts):
            tsv_count += 1
        
        # CSV形式: カンマ区切り
        parts = stripped.split(',')
        if len(parts) == 3 and all(p.strip() for p in parts):
            csv_count += 1
    
    # 最も多くマッチした形式を選択
    scores = {
        'markdown': markdown_count,
        'numbered_list': numbered_list_count,
        'tsv': tsv_count,
        'csv': csv_count,
    }
    
    max_score = max(scores.values())
    if max_score == 0:
        return 'unknown', scores
    
    for fmt, score in scores.items():
        if score == max_score:
            return fmt, scores
    
    return 'unknown', scores


def parse_markdown(lines):
    """Markdown階層形式をパース: ## 大分類 / ### 中分類 / - 小分類"""
    large = None
    middle = None
    rows = []
    
    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        
        # 大分類
        if line.startswith("## "):
            large = line[3:].strip()
            middle = None
            continue
        
        # 中分類
        if line.startswith("### "):
            middle = line[4:].strip()
            continue
        
        # 小分類（リスト）
        if line.startswith("- "):
            small = line[2:].strip()
            # 空や「（—）」をスキップ
            if small in ("（—）", "(—)", "—", "（ー）", "-"):
                continue
            if large and middle and small:
                rows.append((large, middle, small))
            continue
    
    return rows


def parse_numbered_list(lines):
    """番号付きリスト形式をパース: 数字. 大分類 / タブ+数字. 中分類 / タブ+• 小分類"""
    large = None
    middle = None
    rows = []
    prev_was_large = False  # 前の行が大分類だったかどうか
    prev_was_empty = False  # 前の行が空行だったかどうか
    large_number = None  # 現在の大分類の番号
    
    for i, raw in enumerate(lines):
        stripped = raw.rstrip('\n\r')
        is_empty = not stripped
        prev_was_empty_current = prev_was_empty
        prev_was_empty = is_empty
        
        if is_empty:
            prev_was_large = False
            continue
        
        # タブでインデントされているかチェック
        is_indented = stripped.startswith('\t')
        content = stripped.lstrip('\t ')
        
        # 小分類: タブ + • または * で始まる
        bullet_match = re.match(r'^[•\*]\s*(\d+)?\s*(.+)$', content)
        if bullet_match:
            small = bullet_match.group(2).strip()
            # 空や「（—）」をスキップ
            if small in ("（—）", "(—)", "—", "（ー）", "-"):
                continue
            # 大分類が設定されている場合
            if large:
                # 中分類が設定されている場合はそれを使用、なければ大分類と同じ
                effective_middle = middle if middle else large
                rows.append((large, effective_middle, small))
            prev_was_large = False
            continue
        
        # 数字で始まる行
        numbered_match = re.match(r'^(\d+)\.\s*(.+)$', content)
        if numbered_match:
            number = int(numbered_match.group(1))
            name = numbered_match.group(2).strip()
            
            if is_indented:
                # タブあり = 中分類
                middle = name
                prev_was_large = False
            else:
                # タブなし
                # 次の行が小分類かどうかをチェック
                next_is_small = False
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line.startswith('\t') and re.match(r'^\t+[•\*]', next_line):
                        next_is_small = True
                
                # 次の行が空行で、その次の行が数字で始まる場合は大分類の可能性が高い
                next_is_numbered_after_empty = False
                if i + 2 < len(lines) and not lines[i + 1].strip():
                    next_next_line = lines[i + 2].strip()
                    if re.match(r'^\d+\.', next_next_line):
                        next_is_numbered_after_empty = True
                
                if large is None:
                    # 大分類がまだ設定されていない = 新しい大分類
                    large = name
                    middle = None
                    large_number = number
                    prev_was_large = True
                elif prev_was_empty_current and large and large_number is not None:
                    # 空行の後で、大分類が設定されている場合
                    if next_is_small:
                        # 次の行が小分類 = 中分類
                        middle = name
                        prev_was_large = False
                    elif next_is_numbered_after_empty:
                        # 次の行が空行で、その次が数字 = 新しい大分類
                        large = name
                        middle = None
                        large_number = number
                        prev_was_large = True
                    elif number < large_number:
                        # 番号が大分類より小さい = 新しい大分類
                        large = name
                        middle = None
                        large_number = number
                        prev_was_large = True
                    else:
                        # その他 = 中分類
                        middle = name
                        prev_was_large = False
                elif prev_was_large:
                    # 前の行が大分類だった = 中分類
                    middle = name
                    prev_was_large = False
                else:
                    # それ以外 = 新しい大分類
                    large = name
                    middle = None
                    large_number = number
                    prev_was_large = True
            
            continue
    
    return rows


def parse_tsv(lines):
    """TSV形式をパース: タブ区切り"""
    rows = []
    for raw in lines:
        stripped = raw.strip()
        if not stripped or stripped.startswith('#'):
            continue
        parts = stripped.split('\t')
        if len(parts) >= 3:
            large = parts[0].strip()
            middle = parts[1].strip()
            small = parts[2].strip()
            if large and middle and small:
                rows.append((large, middle, small))
    return rows


def parse_csv(lines):
    """CSV形式をパース: カンマ区切り"""
    rows = []
    for raw in lines:
        stripped = raw.strip()
        if not stripped or stripped.startswith('#'):
            continue
        # 簡易的なCSVパース（カンマで分割）
        parts = [p.strip().strip('"') for p in stripped.split(',')]
        if len(parts) >= 3:
            large = parts[0]
            middle = parts[1]
            small = parts[2]
            if large and middle and small:
                rows.append((large, middle, small))
    return rows


def remove_duplicates(rows):
    """重複行を除外（順序は維持）"""
    seen = set()
    result = []
    for row in rows:
        if row not in seen:
            seen.add(row)
            result.append(row)
    return result


def main():
    parser = argparse.ArgumentParser(description='業種分類ファイルをパースしてCSVに変換')
    parser.add_argument('input', help='入力ファイルパス')
    parser.add_argument('output', help='出力CSVファイルパス')
    parser.add_argument('--debug', action='store_true', help='デバッグ情報を表示')
    args = parser.parse_args()
    
    # ファイル読み込み
    try:
        with open(args.input, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません: {args.input}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"エラー: ファイル読み込み失敗: {e}", file=sys.stderr)
        sys.exit(1)
    
    # 形式検出
    detected_format, scores = detect_format(lines)
    
    if args.debug:
        print(f"[DEBUG] 検出されたフォーマット: {detected_format}")
        print(f"[DEBUG] スコア: {scores}")
    
    # パース
    if detected_format == 'markdown':
        rows = parse_markdown(lines)
    elif detected_format == 'numbered_list':
        rows = parse_numbered_list(lines)
    elif detected_format == 'tsv':
        rows = parse_tsv(lines)
    elif detected_format == 'csv':
        rows = parse_csv(lines)
    else:
        # 自動検出に失敗した場合、番号付きリスト形式を試す
        if args.debug:
            print("[DEBUG] 形式が不明なため、番号付きリスト形式でパースを試みます")
        rows = parse_numbered_list(lines)
        if not rows:
            # それでも失敗したらMarkdown形式を試す
            if args.debug:
                print("[DEBUG] 番号付きリスト形式で失敗したため、Markdown形式でパースを試みます")
            rows = parse_markdown(lines)
    
    # 重複除去
    original_count = len(rows)
    rows = remove_duplicates(rows)
    removed_count = original_count - len(rows)
    
    if args.debug:
        print(f"[DEBUG] パース行数: {original_count} 行")
        print(f"[DEBUG] 重複除外: {removed_count} 行")
        print(f"[DEBUG] 最終行数: {len(rows)} 行")
        print(f"[DEBUG] 先頭10件のサンプル:")
        for i, (large, middle, small) in enumerate(rows[:10], 1):
            print(f"  {i}. 大分類={large}, 中分類={middle}, 小分類={small}")
    
    # CSV出力
    try:
        with open(args.output, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(["industryLarge", "industryMiddle", "industrySmall"])
            w.writerows(rows)
    except Exception as e:
        print(f"エラー: CSV書き込み失敗: {e}", file=sys.stderr)
        sys.exit(1)
    
    print(f"OK: {len(rows)} rows -> {args.output}")


if __name__ == "__main__":
    main()
