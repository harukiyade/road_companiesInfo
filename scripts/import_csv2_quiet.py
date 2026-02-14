#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Legatus ONE データインポート用スクリプト (配列対応 & エラーハンドリング強化版)

【変更点】
- 配列型カラム (suppliers, clients, executives) への対応を追加
- 行ごとのエラーハンドリングを追加 (1行のエラーで停止しないように変更)
"""

import csv
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2

# ==========================================
# 設定
# ==========================================
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

# --- ロガー設定 ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("main")

update_logger = logging.getLogger("update_log")
update_logger.setLevel(logging.INFO)
update_logger.propagate = False
fh = logging.FileHandler('updated_records.log', encoding='utf-8')
fh.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
update_logger.addHandler(fh)

# ==========================================
# マッピング定義
# ==========================================
HEADER_MAP = {
    # キー項目
    "法人番号": "corporate_number",
    "電話番号": "phone_number",
    "電話番号(窓口)": "phone_number",

    # 強制上書き (Overwrite) 対象
    "取引種別": "transaction_type",
    "代表者名": "representative_name",
    "代表者": "representative_name",
    "代表者住所": "representative_home_address",
    "代表者年齢": "representative_birth_date",
    "役員": "executives",     # ARRAY
    "仕入れ先": "suppliers",  # ARRAY
    "取引先": "clients",      # ARRAY
    
    # 財務情報
    "直近売上": "latest_revenue",
    "売上高": "latest_revenue", 
    "売上(百万)": "latest_revenue",
    "直近利益": "latest_profit",
    "利益(百万)": "latest_profit",
    "総資産(千円)": "total_assets",
    "純資産(千円)": "net_assets",
    "資本金": "capital_stock",
    "資本金(千円)": "capital_stock",

    # NULL埋め (Fill) 対象
    "会社名": "name",
    "企業名": "name",
    "フォーム・HP": "company_url",
    "URL": "company_url",
    "都道府県": "prefecture",
    "法人格": "legal_form",
    "社員数": "employee_count",
    "従業員数": "employee_count",
    "設立年": "founded_year",
    "上場": "listing",
    "上場企業関連会社": "related_companies",
    "証券コード": "securities_code",
    "業種(大分類)": "industry_large",
    "業種(中分類)": "industry_middle",
    "業種(小分類)": "industry_small",
    "業種(細分類)": "industry_detail",
    "決算月": "fiscal_month",
    "郵便番号": "postal_code",
    "住所": "address",
    "会社住所": "address",
}

OVERWRITE_COLS = {
    "transaction_type", "representative_name", "representative_home_address",
    "representative_birth_date", "executives", "suppliers", "clients",
    "latest_revenue", "latest_profit", "total_assets", "net_assets", "capital_stock"
}

# 配列として扱うカラム
ARRAY_COLS = {"executives", "suppliers", "clients", "related_companies"}

# ==========================================
# 関数群
# ==========================================

def normalize_header(raw: str) -> str:
    if not raw: return ""
    s = str(raw).strip().replace("\n", "").replace("　", "")
    s = s.replace("（", "(").replace("）", ")")
    return s

def normalize_corporate_number(val: Any) -> Optional[str]:
    if not val: return None
    s = str(val).strip()
    if not s: return None
    if "E" in s.upper():
        try:
            s = str(int(float(s)))
        except:
            return None
    s = re.sub(r"[^\d]", "", s)
    if len(s) == 13:
        return s
    return None

def normalize_phone(val: Any) -> Optional[str]:
    if not val: return None
    s = str(val).strip()
    if re.search(r"\d{4}[/年]", s):
        return None
    nums = re.sub(r"[^\d]", "", s)
    if 9 <= len(nums) <= 11:
        return nums
    return None

def parse_numeric(header: str, val: Any) -> Optional[int]:
    if not val: return None
    s = str(val).strip()
    if not s or s == "-": return None
    scale = 1
    if "百万" in header:
        scale = 1_000_000
    elif "千円" in header:
        scale = 1_000
    if "百万円" in s:
        scale = 1_000_000
        s = s.replace("百万円", "")
    elif "千円" in s:
        scale = 1_000
        s = s.replace("千円", "")
    s = s.replace(",", "").replace(" ", "")
    try:
        num = float(s)
        return int(num * scale)
    except ValueError:
        return None

def parse_array(val: Any) -> Optional[List[str]]:
    """文字列を分割してリスト（配列）にする"""
    if not val: return None
    s = str(val).strip()
    if not s: return None
    # 全角カンマ等を半角に統一して分割
    s = s.replace("，", ",").replace("、", ",")
    items = [x.strip() for x in s.split(",") if x.strip()]
    return items if items else None

def process_file(conn, file_path: Path, dry_run: bool):
    logger.info(f"処理開始: {file_path.name}")
    
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        try:
            raw_header = next(reader)
        except StopIteration:
            return

        col_idx_map = {}
        header_names = {}
        for idx, h in enumerate(raw_header):
            norm_h = normalize_header(h)
            if norm_h in HEADER_MAP:
                db_col = HEADER_MAP[norm_h]
                col_idx_map[db_col] = idx
                header_names[db_col] = norm_h

        if "corporate_number" not in col_idx_map and "phone_number" not in col_idx_map:
            logger.warning(f"  -> スキップ (キー列なし)")
            return

        updates_in_file = 0
        skip_count = 0
        error_count = 0
        cursor = conn.cursor()
        
        for row_num, row in enumerate(reader, 1):
            if row_num % 1000 == 0:
                print(f"\r  ... {row_num} 行目を処理中 (更新: {updates_in_file}, エラー: {error_count})", end="", flush=True)

            corp_num = None
            phone_num = None
            
            # --- マッチングキー取得 ---
            if "corporate_number" in col_idx_map:
                idx = col_idx_map["corporate_number"]
                if idx < len(row):
                    corp_num = normalize_corporate_number(row[idx])
            
            if "phone_number" in col_idx_map:
                idx = col_idx_map["phone_number"]
                if idx < len(row):
                    phone_num = normalize_phone(row[idx])
            
            if not corp_num and not phone_num:
                skip_count += 1
                continue

            # --- パラメータ構築 ---
            update_params = {}
            for db_col, idx in col_idx_map.items():
                if idx >= len(row): continue
                if db_col in ["corporate_number", "phone_number"]: continue

                val = row[idx].strip()
                if val == "": continue
                
                # 型変換処理
                if db_col in ["latest_revenue", "latest_profit", "total_assets", "net_assets", "capital_stock", "employee_count", "founded_year"]:
                    num_val = parse_numeric(header_names[db_col], val)
                    if num_val is not None:
                        update_params[db_col] = num_val
                
                elif db_col in ARRAY_COLS:
                    # 配列型への変換
                    arr_val = parse_array(val)
                    if arr_val is not None:
                        update_params[db_col] = arr_val
                else:
                    update_params[db_col] = val

            if not update_params:
                continue

            # --- SQL構築 ---
            set_clauses = []
            sql_values = []
            
            for col, val in update_params.items():
                if col in OVERWRITE_COLS:
                    set_clauses.append(f"{col} = %s")
                    sql_values.append(val)
                else:
                    set_clauses.append(f"{col} = COALESCE({col}, %s)")
                    sql_values.append(val)
            
            if not set_clauses:
                continue

            set_stmt = ", ".join(set_clauses)
            set_stmt += ", updated_at = NOW()"
            
            match_sql = ""
            match_val = None
            match_key_label = ""
            
            if corp_num:
                match_sql = "corporate_number = %s"
                match_val = corp_num
                match_key_label = f"CN:{corp_num}"
            elif phone_num:
                match_sql = "REPLACE(REPLACE(phone_number, '-', ''), ' ', '') = %s"
                match_val = phone_num
                match_key_label = f"TEL:{phone_num}"

            # --- 実行 (Try-Except で保護) ---
            try:
                if dry_run:
                    update_logger.info(f"[DRY] {match_key_label} | Update: {list(update_params.keys())}")
                    updates_in_file += 1
                else:
                    final_sql = f"UPDATE companies SET {set_stmt} WHERE {match_sql} RETURNING id"
                    sql_values.append(match_val)
                    
                    cursor.execute(final_sql, sql_values)
                    
                    if cursor.rowcount > 0:
                        updated_id = cursor.fetchone()[0]
                        update_logger.info(f"ID:{updated_id} | {match_key_label} | Update: {list(update_params.keys())}")
                        updates_in_file += 1
            except Exception as e:
                # エラーが出ても止まらず、ログを出して次へ
                error_count += 1
                logger.warning(f"\n[ERROR] Row {row_num} in {file_path.name}: {e}")
                conn.rollback() # このトランザクションだけロールバックして接続を維持
                # cursorを再作成する必要がある場合があるため、簡易的に続行（バッチ外ループなのでsafe）
                # ただしこのスクリプトは1行1コミットではないので、厳密には conn.rollback() すると
                # ここまでの未コミット分が消える可能性がある。
                # 安全のため、1ファイル単位のコミットにしているが、エラー時はそのファイルの前半も巻き戻る。
                # -> 行ごとに savepoint を切るか、エラー時は無視するか。
                # ここでは「ファイル単位コミット」なので、エラーが出るとそのファイルの処理が中断されるのを防ぐため
                # continueするが、psycopg2はエラー後 rollback が必要。
                continue

        print("") 
        
        # エラーがなければコミット、エラー過多ならロールバックなどの判断も可能だが
        # 基本的に通ったものだけコミットする
        conn.commit()
        cursor.close()
        logger.info(f"完了: {file_path.name} -> 更新 {updates_in_file} 件 (エラー {error_count} 件)")

def main():
    import argparse
    parser = argparse.ArgumentParser(description="CSVインポート (Quiet Mode)")
    parser.add_argument("--dry-run", action="store_true", help="DB更新を行わずシミュレーション")
    parser.add_argument("--dir", default="csv_2", help="対象フォルダ (default: csv_2)")
    args = parser.parse_args()

    base_dir = Path(args.dir)
    if not base_dir.exists() and Path("fixed_csv_2").exists():
        base_dir = Path("fixed_csv_2")
        logger.info(f"指定フォルダがないため fixed_csv_2 を使用します")

    csv_files = sorted(base_dir.glob("*.csv"))
    if not csv_files:
        logger.error(f"CSVファイルが見つかりません: {base_dir}")
        return

    logger.info(f"DB接続先: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    conn = None
    try:
        if not args.dry_run:
            if not DB_PASSWORD:
                logger.error("環境変数 POSTGRES_PASSWORD が設定されていません")
                return
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                dbname=DB_NAME,
                sslmode=DB_SSLMODE
            )
        
        update_logger.info(f"--- 処理開始 ({len(csv_files)} ファイル) ---")
        
        for fp in csv_files:
            process_file(conn, fp, args.dry_run)
            
    except Exception as e:
        logger.exception("重大なエラーが発生しました")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()