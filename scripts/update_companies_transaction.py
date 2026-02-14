#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CSVの「取引種別」を companies テーブルの transaction_type に反映するスクリプト。

companies テーブルを 法人番号(corporate_number) のみで検索し、
一致した行の transaction_type を更新する。企業名・都道府県・代表者名は条件に含めない。
法人番号は12/13桁の数値の場合のみ有効（それ以外の行はマッチしなかったとしてスキップ）。

対象ディレクトリ: csv_2, csv0108, csv0115。取引種別列が無いCSVは無視。
取引種別が「譲受企業」または「譲渡企業」の行のみ更新し、未設定・その他は無視（DBはnullのまま）。

前提:
  - companies テーブルに transaction_type カラムが存在すること。
    未定義の場合は backend/sql/add_transaction_type_column.sql を実行してください。
"""

import argparse
import os
import re
import sys
import glob
import logging
from typing import Optional, List, Tuple
from urllib.parse import quote_plus

import pandas as pd
import psycopg2

# ==========================================
# 設定（環境変数で上書き可能）
# ==========================================
CSV_DIRECTORY_PATHS = [
    "/Users/harumacmini/programming/info_companyDetail/csv_2",
    "/Users/harumacmini/programming/info_companyDetail/csv0108",
    "/Users/harumacmini/programming/info_companyDetail/csv0115",
]

DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "Legatus2000/")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

# 対象テーブル・カラム（変数化して変更しやすくする）
TABLE_NAME = "companies"
COL_NAME = "name"                    # 企業名
COL_PREF = "prefecture"              # 都道府県
COL_REP = "representative_name"     # 代表者名
COL_CORP = "corporate_number"        # 法人番号
COL_TARGET = "transaction_type"      # 更新対象

# CSV列名の候補（日本語）
CSV_COL_COMPANY = ("企業名", "会社名")
CSV_COL_PREF = "都道府県"
CSV_COL_REP = "代表者名"
CSV_COL_CORP = "法人番号"
CSV_COL_TRANSACTION = "取引種別"

# 取引種別: 「譲受企業」「譲渡企業」のみ更新する。未設定・その他は無視（DBはnullのまま）
TRANSACTION_TYPE_TARGETS = ("譲受企業", "譲渡企業")

# 法人番号: 12桁または13桁かつ数値のみの場合のみWHEREに含める（日本の法人番号は13桁）
CORP_NUMBER_VALID_LENS = (12, 13)
CORP_NUMBER_DIGITS_ONLY = re.compile(r"^\d+$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def _get_connection():
    """PostgreSQL接続を確立する。SSL必須。"""
    encoded_password = quote_plus(DB_PASSWORD) if DB_PASSWORD else ""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )
    return conn


def _resolve_csv_columns(df: pd.DataFrame) -> Optional[dict]:
    """
    CSVの列名を解決する。
    戻り値: { 'name', 'prefecture', 'representative', 'corporate_number', 'transaction' } のキーを持つ辞書。
    取引種別列が無いCSVは無視（None を返す）。必須列（企業名・都道府県・代表者名・取引種別）が揃っている場合のみ返す。
    """
    cols = {}
    for cand in CSV_COL_COMPANY:
        if cand in df.columns:
            cols["name"] = cand
            break
    if "name" not in cols:
        return None
    if CSV_COL_PREF not in df.columns:
        return None
    cols["prefecture"] = CSV_COL_PREF
    if CSV_COL_REP not in df.columns:
        return None
    cols["representative"] = CSV_COL_REP
    cols["corporate_number"] = CSV_COL_CORP if CSV_COL_CORP in df.columns else None
    if CSV_COL_TRANSACTION not in df.columns:
        return None
    cols["transaction"] = CSV_COL_TRANSACTION
    return cols


def _is_target_transaction_type(val: Optional[str]) -> bool:
    """取引種別が「譲受企業」または「譲渡企業」ならTrue。未設定・空・その他はFalse。"""
    if val is None:
        return False
    return str(val).strip() in TRANSACTION_TYPE_TARGETS


def _normalize_cell(val) -> Optional[str]:
    """セルを文字列にし、NaN/空はNoneに。"""
    if pd.isna(val) or val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _is_valid_corporate_number(val) -> bool:
    """法人番号が「12桁または13桁かつ数値のみ」ならTrue。"""
    if val is None:
        return False
    s = str(val).strip()
    if len(s) not in CORP_NUMBER_VALID_LENS:
        return False
    return bool(CORP_NUMBER_DIGITS_ONLY.match(s))


def _collect_csv_files(directory_paths: List[str]) -> List[str]:
    """指定ディレクトリ配下の全CSVファイルパスを返す。"""
    paths = []
    for base in directory_paths:
        if not os.path.isdir(base):
            logger.warning("ディレクトリが存在しません: %s", base)
            continue
        for path in sorted(glob.glob(os.path.join(base, "*.csv"))):
            paths.append(path)
    return paths


def _build_where_corp_only(corporate_number: str, col_corp: str) -> Tuple[str, list]:
    """法人番号のみでマッチするWHERE句とパラメータを返す。"""
    return "{} = %s".format(col_corp), [corporate_number]


def run_update(
    report_no_match_path: Optional[str] = None,
    input_csv_path: Optional[str] = None,
):
    """メイン処理: CSVを読み、companies の transaction_type を更新する。"""
    stats = {"files_processed": 0, "rows_read": 0, "updated": 0, "no_match": 0, "error": 0, "skipped_no_transaction_col": 0}
    no_match_rows: List[dict] = []  # --report-no-match 用

    if input_csv_path:
        if not os.path.isfile(input_csv_path):
            logger.warning("指定ファイルが存在しません: %s", input_csv_path)
            return stats
        csv_paths = [input_csv_path]
        logger.info("入力CSV: %s（1 件）", input_csv_path)
    else:
        csv_paths = _collect_csv_files(CSV_DIRECTORY_PATHS)
        if not csv_paths:
            logger.warning("CSVファイルが1件も見つかりませんでした。")
            return stats
        logger.info("対象CSV: %s 件", len(csv_paths))

    try:
        conn = _get_connection()
    except Exception as e:
        logger.exception("DB接続エラー: %s", e)
        raise

    try:
        with conn.cursor() as cur:
            for file_idx, path in enumerate(csv_paths, 1):
                logger.info("(%s/%s) %s", file_idx, len(csv_paths), path)
                sys.stdout.flush()
                sys.stderr.flush()
                try:
                    df = pd.read_csv(path, encoding="utf-8", dtype=str)
                except Exception as e:
                    try:
                        df = pd.read_csv(path, encoding="cp932", dtype=str)
                    except Exception as e2:
                        logger.warning("読み込みスキップ %s: %s", path, e2)
                        stats["error"] += 1
                        continue

                df = df.where(pd.notna(df), None)
                col_map = _resolve_csv_columns(df)
                if col_map is None:
                    logger.info("取引種別列または必須列がないためスキップ: %s", path)
                    stats["skipped_no_transaction_col"] += 1
                    continue

                stats["files_processed"] += 1
                n_rows = len(df)
                logger.info("  取引種別あり: %s 行を走査します", n_rows)
                sys.stdout.flush()
                name_col = col_map["name"]
                pref_col = col_map["prefecture"]
                rep_col = col_map["representative"]
                corp_col = col_map["corporate_number"]
                trans_col = col_map["transaction"]

                row_in_file = 0
                for idx, row in df.iterrows():
                    stats["rows_read"] += 1
                    row_in_file += 1
                    if row_in_file % 5000 == 0:
                        logger.info("  行進捗: %s/%s (更新 %s 件)", row_in_file, n_rows, stats["updated"])
                        sys.stdout.flush()
                    name = _normalize_cell(row.get(name_col))
                    prefecture = _normalize_cell(row.get(pref_col))
                    representative = _normalize_cell(row.get(rep_col))
                    corporate_number = _normalize_cell(row.get(corp_col)) if corp_col else None
                    transaction_value = _normalize_cell(row.get(trans_col))

                    # 譲受企業 or 譲渡企業 のみ更新。未設定・その他は無視（nullのまま）
                    if not _is_target_transaction_type(transaction_value):
                        continue

                    # 法人番号のみでマッチ: 12/13桁数値でない行はスキップ
                    if not _is_valid_corporate_number(corporate_number):
                        stats["no_match"] += 1
                        if report_no_match_path is not None:
                            no_match_rows.append({
                                "企業名": name or "",
                                "都道府県": prefecture or "",
                                "代表者名": representative or "",
                                "法人番号": corporate_number or "",
                                "取引種別": transaction_value or "",
                                "ソースファイル": path,
                            })
                        continue

                    where_sql, params = _build_where_corp_only(corporate_number, COL_CORP)
                    update_params = [transaction_value] + params
                    update_sql = "UPDATE {} SET {} = %s WHERE {}".format(
                        TABLE_NAME, COL_TARGET, where_sql
                    )
                    try:
                        cur.execute(update_sql, update_params)
                        if cur.rowcount > 0:
                            stats["updated"] += cur.rowcount
                        else:
                            stats["no_match"] += 1
                            if report_no_match_path is not None:
                                no_match_rows.append({
                                    "企業名": name or "",
                                    "都道府県": prefecture or "",
                                    "代表者名": representative or "",
                                    "法人番号": corporate_number or "",
                                    "取引種別": transaction_value or "",
                                    "ソースファイル": path,
                                })
                    except Exception as e:
                        logger.warning("UPDATE失敗 (行%s): %s", idx + 2, e)
                        stats["error"] += 1

            conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception("処理中にエラー: %s", e)
        raise
    finally:
        conn.close()

    if report_no_match_path and no_match_rows:
        try:
            pd.DataFrame(no_match_rows).to_csv(
                report_no_match_path, index=False, encoding="utf-8-sig"
            )
            logger.info("マッチしなかった件の一覧を出力: %s (%s 件)", report_no_match_path, len(no_match_rows))
        except Exception as e:
            logger.warning("マッチしなかった一覧の出力失敗: %s", e)

    return stats


def main():
    parser = argparse.ArgumentParser(description="CSVの取引種別を companies.transaction_type に反映する")
    parser.add_argument(
        "--report-no-match",
        metavar="CSV_PATH",
        default=None,
        help="マッチしなかった行の一覧をこのCSVに出力する（原因調査用）",
    )
    parser.add_argument(
        "--input-csv",
        metavar="CSV_PATH",
        default=None,
        help="このCSVのみを入力として更新する（例: no_match_transaction.csv）",
    )
    args = parser.parse_args()

    logger.info("接続先: %s:%s/%s (user=%s, sslmode=%s)", DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_SSLMODE)
    stats = run_update(
        report_no_match_path=args.report_no_match,
        input_csv_path=args.input_csv,
    )
    logger.info("--- 集計 ---")
    logger.info("処理したCSVファイル数: %s", stats["files_processed"])
    logger.info("読み込み行数: %s", stats["rows_read"])
    logger.info("更新成功件数: %s", stats["updated"])
    logger.info("マッチしなかった件数: %s", stats["no_match"])
    logger.info("エラー件数: %s", stats["error"])
    logger.info("取引種別列なしでスキップしたファイル数: %s", stats["skipped_no_transaction_col"])
    if stats["no_match"] > 0:
        logger.info("--- マッチしなかった主な原因 ---")
        logger.info("  1. 法人番号が12/13桁の数値でない（空欄・桁数違い・数値以外含む）")
        logger.info("  2. 該当法人番号が companies テーブルに存在しない")
        logger.info("  詳細: --report-no-match で一覧CSVを出力可能")


if __name__ == "__main__":
    main()
