#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3 配下のCSVを財務単位に基づき unit_million / unit_yen へ仕分けするスクリプト。

判定ロジック:
- unit_yen: ヘッダーに「円」「（円）」、または数値が1億超
- unit_million: ヘッダーに「百万円」、または全数値が1億未満

処理: shutil.move でファイルを移動。ログ出力。

使い方:
  python scripts/organize_csv_units.py [--dry-run]
"""

import argparse
import csv
import logging
import re
import shutil
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# スクリプト位置基準で fixed_csv_3 を解決（cwd に依存しない）
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
SOURCE_DIR = PROJECT_ROOT / "fixed_csv_3"
UNIT_MILLION_DIR = SOURCE_DIR / "unit_million"  # 百万単位（100万倍が必要）
UNIT_YEN_DIR = SOURCE_DIR / "unit_yen"          # 円単位（そのまま）

YEN_THRESHOLD = 100_000_000  # 1億（これを超えると円単位と判断）
SAMPLE_ROWS = 200  # サンプリング行数

# 財務列のヘッダー候補（revenue, profit, capital_stock）
FIN_HEADERS_REVENUE = ["売上高", "直近売上", "法人＿売上高", "売上高1", "売上規模（百万円）", "売上規模(百万円)"]
FIN_HEADERS_PROFIT = ["直近利益", "当期純利益(損失)", "法人＿当期純利益(損失)", "経常利益", "営業利益", "利益1"]
FIN_HEADERS_CAPITAL = ["資本金", "法人＿資本金", "自己資本"]

# パターン1（文字化け）: 22:資本金, 25:売上, 26:利益
FIN_INDICES_PATTERN1 = [22, 25, 26]
# yuzuri: 5:売上規模（百万円）
FIN_INDICES_YUZURI = [5]
# 5.csv: 25:資本金, 32:売上, 45:利益
FIN_INDICES_5CSV = [25, 32, 45]


def get_encoding(file_path):
    for enc in ["utf-8-sig", "cp932", "utf-8"]:
        try:
            with open(file_path, "r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "utf-8"


def normalize_header(raw):
    if not raw:
        return ""
    s = str(raw).strip().strip("\ufeff").replace("\n", " ").replace("\r", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)


def parse_num(val):
    """数値に変換。失敗時はNone。"""
    if val is None:
        return None
    s = str(val).strip().replace(",", "").replace(" ", "")
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    s = re.sub(r"[^\d\.\-eE+]", "", s)
    if not s:
        return None
    try:
        return float(s)
    except (ValueError, OverflowError):
        return None


def has_yen_unit_in_header(headers):
    """ヘッダーに「円」または「（円）」があり、かつ「百万円」ではない"""
    for h in headers or []:
        n = normalize_header(h)
        if "百万円" in n:
            continue
        if "円" in n or "（円）" in n or "(円)" in n:
            return True
    return False


def has_million_unit_in_header(headers):
    """ヘッダーに「百万円」または「（百万円）」を含む"""
    for h in headers or []:
        n = normalize_header(h)
        if "百万円" in n or "百万円" in n.replace("(", "").replace(")", ""):
            return True
    return False


def find_financial_indices(headers):
    """財務列のインデックスを取得。見つからなければ空リスト。"""
    indices = []
    all_fin = FIN_HEADERS_REVENUE + FIN_HEADERS_PROFIT + FIN_HEADERS_CAPITAL
    norm_to_idx = {normalize_header(h): i for i, h in enumerate(headers)}
    for fin_h in all_fin:
        n = normalize_header(fin_h)
        if n in norm_to_idx:
            indices.append(norm_to_idx[n])
    # 部分一致も試す
    for i, h in enumerate(headers):
        n = normalize_header(h)
        for fin_h in all_fin:
            if fin_h in n or n in fin_h:
                indices.append(i)
                break
    return list(set(indices))


def detect_pattern(file_path, headers, fin_indices):
    """パターン判定（文字化け / yuzuri / 5.csv）"""
    name = file_path.name.lower()
    if "yuzuri" in name or (headers and any("売上規模" in normalize_header(h) and "百万円" in normalize_header(h) for h in headers)):
        return "yuzuri"
    if name == "5.csv" or "5.fixed" in name:
        return "pattern_5"
    if len(fin_indices) < 2 and len(headers or []) >= 30:
        return "pattern1"
    return "normal"


def sample_financial_values(file_path, enc, headers, fin_indices, pattern):
    """財務列から数値をサンプリング"""
    values = []
    indices = fin_indices
    if pattern == "pattern1":
        indices = FIN_INDICES_PATTERN1
    elif pattern == "yuzuri":
        indices = FIN_INDICES_YUZURI
    elif pattern == "pattern_5":
        indices = FIN_INDICES_5CSV

    if not indices:
        return values

    try:
        with open(file_path, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f)
            next(reader, None)  # skip header
            for i, row in enumerate(reader):
                if i >= SAMPLE_ROWS:
                    break
                for idx in indices:
                    if idx < len(row) and row[idx]:
                        v = parse_num(row[idx])
                        if v is not None and abs(v) < 1e20:  # 明らかなゴミ除外
                            values.append(v)
    except Exception as e:
        logger.warning(f"  {file_path.name} サンプリング失敗: {e}")
    return values


def classify_csv_unit(file_path):
    """
    1ファイルの単位を判定。'unit_yen' | 'unit_million' | 'unknown'
    """
    enc = get_encoding(file_path)
    try:
        with open(file_path, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f)
            headers = next(reader, None)
    except Exception as e:
        logger.warning(f"  {file_path.name} ヘッダー読込失敗: {e}")
        return "unknown", "read_error"

    if not headers:
        return "unknown", "no_headers"

    fin_indices = find_financial_indices(headers)
    pattern = detect_pattern(file_path, headers, fin_indices)

    # 1. ヘッダーによる判定
    if has_million_unit_in_header(headers):
        return "unit_million", "header_百万円"
    if has_yen_unit_in_header(headers):
        # 「円」のみで「百万円」がない場合（法人＿売上高などは百万円ではない表記もある）
        # より明確な「（円）」があれば unit_yen
        for h in headers or []:
            n = normalize_header(h)
            if "（円）" in n or "(円)" in n:
                return "unit_yen", "header_円"

    # 2. サンプル値による判定
    values = sample_financial_values(file_path, enc, headers, fin_indices, pattern)

    if not values:
        return "unit_million", "no_values_default"  # 不明時は百万単位をデフォルト

    has_large = any(abs(v) >= YEN_THRESHOLD for v in values)
    if has_large:
        return "unit_yen", "value_1億超"
    return "unit_million", "value_1億未満"


def main():
    parser = argparse.ArgumentParser(description="CSVを財務単位で unit_million / unit_yen へ仕分け")
    parser.add_argument("--dry-run", action="store_true", help="実際には移動せずログのみ")
    args = parser.parse_args()
    dry_run = args.dry_run

    if not SOURCE_DIR.exists():
        logger.error(f"ディレクトリが存在しません: {SOURCE_DIR}")
        return 1

    if dry_run:
        logger.info("【dry-run モード】実際には移動しません")

    # 直下のCSVのみ対象（サブフォルダ内は既に仕分け済みのため除外）
    csv_files = sorted([f for f in SOURCE_DIR.glob("*.csv") if f.is_file()])
    logger.info(f"対象CSV: {len(csv_files)}件 (対象: {SOURCE_DIR.resolve()})")
    if not csv_files and not dry_run:
        logger.warning("仕分け対象のCSVがありません。unit_million/unit_yen 内に既に移動済みの可能性があります。")

    UNIT_MILLION_DIR.mkdir(parents=True, exist_ok=True)
    UNIT_YEN_DIR.mkdir(parents=True, exist_ok=True)

    moved_million = []
    moved_yen = []
    moved_unknown = []

    for fp in csv_files:
        unit, reason = classify_csv_unit(fp)
        dest_dir = UNIT_MILLION_DIR if unit == "unit_million" else (UNIT_YEN_DIR if unit == "unit_yen" else None)
        if dest_dir is None:
            dest_dir = UNIT_MILLION_DIR  # 不明時は unit_million へ
            moved_unknown.append((fp.name, reason))
        dest_path = dest_dir / fp.name

        try:
            if not dry_run:
                shutil.move(str(fp), str(dest_path))
            if unit == "unit_million":
                moved_million.append((fp.name, reason))
            elif unit == "unit_yen":
                moved_yen.append((fp.name, reason))
        except Exception as e:
            logger.error(f"  {fp.name} 移動失敗: {e}")

    # ログ出力
    if dry_run:
        logger.info("【dry-run】上記は実際の移動結果です")
    logger.info("===== 仕分け結果 =====")
    logger.info(f"unit_million ({UNIT_MILLION_DIR}): {len(moved_million)}件")
    for name, reason in moved_million:
        logger.info(f"  [million] {name} (理由: {reason})")
    logger.info(f"unit_yen ({UNIT_YEN_DIR}): {len(moved_yen)}件")
    for name, reason in moved_yen:
        logger.info(f"  [yen]     {name} (理由: {reason})")
    if moved_unknown:
        logger.info(f"不明→unit_million にデフォルト: {len(moved_unknown)}件")
        for name, reason in moved_unknown:
            logger.info(f"  [default] {name} (理由: {reason})")

    logger.info("完了")
    if not dry_run and (moved_million or moved_yen or moved_unknown):
        logger.info("")
        logger.info("確認コマンド:")
        logger.info(f"  ls -l {UNIT_MILLION_DIR} | wc -l")
        logger.info(f"  ls -l {UNIT_YEN_DIR} | wc -l")
    return 0


if __name__ == "__main__":
    exit(main())
