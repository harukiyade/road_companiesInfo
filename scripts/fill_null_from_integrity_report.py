#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NULL補完スクリプト（安全対策・個別コミット付き）

integrity_report.csv または report_null_only.csv を読み、
「DB側がNULL（または空文字）の場合のみ」CSVの値で companies を UPDATE する。
既存の非NULLデータは一切上書きしない。

前提:
  - レポートの 不一致の種類 が「CSVに値あり・DBがNULL/空」の行のみを対象とする。
  - 同一 (ID, カラム名) が複数ある場合は、最初に出現した値を使用する。
  - ★法人番号（corporate_number）の更新は重複エラー防止のためスキップする。
  - ★1件ごとに更新を確定（またはエラー時はその件だけロールバック）し、全体が止まるのを防ぐ。

使い方:
  # ドライラン（実際には更新しない）
  POSTGRES_PASSWORD=xxx python scripts/fill_null_from_integrity_report.py --report integrity_report.csv --dry-run

  # 本実行（report_null_only.csv を使用する場合）
  POSTGRES_PASSWORD=xxx python scripts/fill_null_from_integrity_report.py --report report_null_only.csv
"""

import csv
import os
import re
import sys
from pathlib import Path

import psycopg2

# 対象とする不一致の種類
NULL_FILL_KIND = "CSVに値あり・DBがNULL/空"

DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

COLUMN_SPEC = {
    "name": ("str", 500),
    "kana": ("str", 500),
    "corporate_number": ("str", 13),
    "prefecture": ("str", 50),
    "address": ("str", None),
    "postal_code": ("str", 10),
    "phone_number": ("str", 50),
    "fax": ("str", 50),
    "email": ("str", 255),
    "company_url": ("str", None),
    "representative_name": ("str", 200),
    "representative_home_address": ("str", None),
    "representative_postal_code": ("str", 10),
    "industry_large": ("str", 200),
    "industry_middle": ("str", 200),
    "industry_small": ("str", 200),
    "industry_detail": ("str", 200),
    "overview": ("str", None),
    "company_description": ("str", None),
    "listing": ("str", 100),
    "transaction_type": ("str", 100),
    "founding_year": ("int", None),
    "employee_count": ("int", None),
    "office_count": ("int", None),
    "factory_count": ("int", None),
    "store_count": ("int", None),
    "latest_revenue": ("bigint", None),
    "latest_profit": ("bigint", None),
    "capital_stock": ("bigint", None),
    "nda_flag": ("bool", None),
    "ad_flag": ("bool", None),
    "sb_flag": ("bool", None),
}

BIGINT_SAFE_MAX = 9_000_000_000_000_000_000
INPUT_CAP = 100_000_000_000_000


def _norm_val(val):
    if val is None:
        return None
    s = str(val).strip()
    return s if s.lower() not in ("", "nan", "none", "null") else None


def _parse_year(val):
    s = _norm_val(val)
    if not s:
        return None
    m = re.search(r"(\d{4})", s)
    return int(m.group(1)) if m else None


def _parse_int_safe(val):
    s = _norm_val(val)
    if not s:
        return None
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return int(float(s))
    except (ValueError, OverflowError):
        return None


def _parse_revenue_profit(val, apply_1000x=True):
    s = _norm_val(val)
    if not s:
        return None
    s_val = s.replace(",", "").replace("　", "").replace(" ", "")
    unit_factor = 1
    has_unit = False
    if "億円" in s_val or "億" in s_val:
        unit_factor = 100_000_000
        s_val = re.sub(r"億円?|円", "", s_val)
        has_unit = True
    elif "百万円" in s_val or "百万" in s_val:
        unit_factor = 1_000_000
        s_val = re.sub(r"百万円?|円", "", s_val)
        has_unit = True
    elif ("万円" in s_val or "万" in s_val) and "百万" not in s_val:
        unit_factor = 10_000
        s_val = re.sub(r"万円?|円", "", s_val)
        has_unit = True
    try:
        num_part = re.sub(r"[^\d\.\-eE+]", "", s_val)
        if not num_part:
            return None
        f_val = float(num_part)
        if abs(f_val) > INPUT_CAP:
            return None
        final_factor = unit_factor if has_unit else (1000 if apply_1000x else 1)
        result = int(f_val * final_factor)
        if abs(result) > BIGINT_SAFE_MAX:
            return None
        return result
    except Exception:
        return None


def _parse_bool(val):
    s = _norm_val(val)
    if not s:
        return None
    s_lower = s.lower()
    if s_lower in ("1", "true", "yes", "y", "○", "〇", "あり", "済", "締結済", "契約済"):
        return True
    if "ストロングバイヤー" in s and "以外" not in s:
        return True
    return False


def _convert_value(col, raw_val):
    spec = COLUMN_SPEC.get(col)
    if not spec:
        s = _norm_val(raw_val)
        return s[:2000] if s else None
    typ, max_len = spec
    if typ == "str":
        s = _norm_val(raw_val)
        if not s:
            return None
        if max_len and len(s) > max_len:
            return s[:max_len]
        return s
    if typ == "int":
        if col == "founding_year":
            return _parse_year(raw_val)
        return _parse_int_safe(raw_val)
    if typ == "bigint":
        return _parse_revenue_profit(raw_val)
    if typ == "bool":
        return _parse_bool(raw_val)
    return _norm_val(raw_val)


def _is_text_column(col):
    spec = COLUMN_SPEC.get(col)
    if not spec:
        return True
    return spec[0] == "str"


def load_null_fill_rows(report_path):
    path = Path(report_path)
    if not path.exists():
        raise FileNotFoundError(f"レポートが見つかりません: {report_path}")

    seen = set()
    rows = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            kind = r.get("不一致の種類", "").strip()
            if kind != NULL_FILL_KIND:
                continue
            id_ = (r.get("ID") or "").strip()
            col = (r.get("カラム名") or "").strip()
            if not id_ or not col:
                continue
                
            # ★ 修正ポイント1: corporate_number は除外する
            if col == "corporate_number":
                continue
                
            key = (id_, col)
            if key in seen:
                continue
            seen.add(key)
            csv_val = r.get("CSVの値", "").strip()
            if not csv_val:
                continue
            rows.append({"id": id_, "column": col, "csv_value": csv_val})
    return rows


ALLOWED_COLUMNS = frozenset(COLUMN_SPEC.keys())


def run_fill(conn, rows, dry_run=False, limit=None):
    cur = conn.cursor()
    updated = 0
    skipped = 0
    errors = 0
    to_process = rows[:limit] if limit else rows

    for r in to_process:
        id_, col, raw_val = r["id"], r["column"], r["csv_value"]

        if col not in ALLOWED_COLUMNS:
            skipped += 1
            continue

        try:
            converted = _convert_value(col, raw_val)
            if converted is None:
                skipped += 1
                continue
        except Exception as e:
            errors += 1
            if not dry_run:
                print(f"  [変換エラー] id={id_}, col={col}: {e}", file=sys.stderr)
            continue

        if dry_run:
            try:
                cur.execute(f'SELECT "{col}" FROM companies WHERE id = %s', (id_,))
                row = cur.fetchone()
                current = row[0] if row else None
                is_null_or_empty = current is None or (isinstance(current, str) and current.strip() == "")
                if is_null_or_empty:
                    updated += 1
                    # print(f"  [DRY-RUN] id={id_}, {col} = {str(converted)[:50]!r}")
                else:
                    skipped += 1
            except Exception as e:
                errors += 1
                conn.rollback() # エラー時はロールバックして次へ
                print(f"  [DRY-RUN 確認エラー] id={id_}, col={col}: {e}", file=sys.stderr)
            continue

        # 本実行
        try:
            # ★ 修正ポイント2: 1件ごとにトランザクションを張る（SAVEPOINTの活用）
            cur.execute("SAVEPOINT my_savepoint")
            
            if _is_text_column(col):
                stmt = f'UPDATE companies SET "{col}" = %s WHERE id = %s AND ("{col}" IS NULL OR "{col}" = \'\')'
            else:
                stmt = f'UPDATE companies SET "{col}" = %s WHERE id = %s AND "{col}" IS NULL'
            
            cur.execute(stmt, (converted, id_))
            
            if cur.rowcount > 0:
                updated += 1
                # 成功したらセーブポイントを解放（確定は後でまとめて行う）
                cur.execute("RELEASE SAVEPOINT my_savepoint")
            else:
                skipped += 1
                cur.execute("RELEASE SAVEPOINT my_savepoint")
                
        except Exception as e:
            errors += 1
            # エラーが起きたら、この1件のUPDATEだけを無かったことにする（全体のドミノ倒しを防ぐ）
            cur.execute("ROLLBACK TO SAVEPOINT my_savepoint")
            print(f"  [UPDATEエラー] id={id_}, col={col}: {e}", file=sys.stderr)

    cur.close()
    return updated, skipped, errors


def main():
    import argparse
    parser = argparse.ArgumentParser(description="整合性レポートからNULL補完（安全対策・個別コミット版）")
    parser.add_argument("--report", required=True, help="integrity_report.csv または report_null_only.csv")
    parser.add_argument("--dry-run", action="store_true", help="更新せず対象のみ表示")
    parser.add_argument("--limit", type=int, default=None, help="処理する最大件数（テスト用）")
    args = parser.parse_args()

    if not DB_PASSWORD:
        print("エラー: POSTGRES_PASSWORD を設定してください。", file=sys.stderr)
        sys.exit(1)

    rows = load_null_fill_rows(args.report)
    print(f"補完対象レコード数（corporate_number除外後）: {len(rows)} 件")
    
    if args.dry_run:
        print("--- ドライランモード（DBは更新しません） ---")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )

    try:
        updated, skipped, errors = run_fill(conn, rows, dry_run=args.dry_run, limit=args.limit)
        
        if args.dry_run:
            conn.rollback()
        else:
            # ★ 全ての行の処理が終わってから、正常に処理できたものだけをDBに反映する
            conn.commit()
            
        print(f"更新: {updated} 件, スキップ: {skipped} 件, エラー: {errors} 件")
    except Exception as e:
        conn.rollback()
        print(f"致命的なエラー: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main()