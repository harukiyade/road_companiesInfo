#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
csv_2 フォルダ配下のCSVを companies テーブルにインポート（更新）するスクリプト。

分析結果（docs/csv_2_analysis_report.md）に基づき:
- ヘッダー正規化（全角/半角、会社名/企業名のエイリアス）
- 法人番号の指数表記→13桁整数変換
- 数値カラムのパース（百万円、カンマ除去、日付混入はnull）
- マッチング: 法人番号 優先 → 電話番号 フォールバック

使い方:
  python scripts/import_csv2_to_companies.py [--dry-run] [--source csv_2|fixed_csv_2]
"""

import argparse
import csv
import logging
import sys

# 巨大なCSVフィールド対応
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import psycopg2

# ==========================================
# 設定
# ==========================================
BASE_DIR = Path(__file__).resolve().parent.parent
SOURCE_PRIORITY = ["fixed_csv_2", "csv_2"]  # 優先順

DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

# CSV → DB カラムマッピング（複数エイリアス対応）
HEADER_TO_DB = {
    "name": ["会社名", "企業名", "商号又は名称", "法人名"],
    "corporate_number": ["法人番号", "corporateNumber"],
    "phone_number": ["電話番号(窓口)", "電話番号", "TEL", "代表電話"],
    "prefecture": ["都道府県", "県"],
    "address": ["住所", "所在地", "本社住所", "会社住所"],
    "postal_code": ["郵便番号", "会社郵便番号"],
    "representative_name": ["代表者名", "代表者", "代表"],
    "capital_stock": ["資本金"],
    "latest_revenue": ["直近売上"],
    "latest_profit": ["直近利益"],
    "revenue": ["売上高", "売上", "直近売上"],
    "company_url": ["URL", "企業ホームページURL", "HP"],
    "established": ["設立", "設立年月日"],
    "employee_count": ["社員数", "従業員数", "従業員"],
    "industry": ["業種1", "業種", "業種(大)"],
    "industry_large": ["業種1", "業種(大)", "業種-大"],
    "industry_middle": ["業種2", "業種(中)", "業種-中"],
    "industry_small": ["業種3", "業種(小)", "業種-小"],
    "industry_detail": ["業種4", "業種(細)", "業種-細"],
    "overview": ["概要", "概況", "説明", "企業概要"],
}

# 数値パターン
RE_DATE_JP = re.compile(r"^\d{4}年\d{1,2}月\d{1,2}日$")
RE_DATE_SLASH = re.compile(r"^\d{4}/\d{1,2}/\d{1,2}")
RE_MAN_YEN = re.compile(r"([\d,]+)\s*百万円?")
RE_CORP_DIGITS = re.compile(r"^\d{12,13}$")
RE_PHONE_DIGITS = re.compile(r"^[\d\-\(\)]+$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def normalize_header(raw: str) -> str:
    """ヘッダーを正規化（全角→半角、空白除去）"""
    s = str(raw).strip().strip("\ufeff").replace("\n", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)


def resolve_header_map(headers: List[str]) -> Dict[str, int]:
    """CSVヘッダーからDBカラム名へのインデックスマップを構築"""
    result: Dict[str, int] = {}
    norm_to_idx = {}
    for i, h in enumerate(headers):
        n = normalize_header(h)
        norm_to_idx[n] = i
    for db_col, aliases in HEADER_TO_DB.items():
        for alias in aliases:
            na = normalize_header(alias)
            if na in norm_to_idx:
                result[db_col] = norm_to_idx[na]
                break
            if alias in headers:
                result[db_col] = headers.index(alias)
                break
    return result


def normalize_corporate_number(val: Any) -> Optional[str]:
    """法人番号を13桁文字列に正規化。無効ならNone。"""
    if val is None or (isinstance(val, float) and (val != val or val == 0)):
        return None
    s = str(val).strip()
    if not s:
        return None
    if "E" in s.upper() or "e" in s:
        try:
            n = int(float(s))
            s = str(n)
        except (ValueError, OverflowError):
            return None
    s = re.sub(r"[^\d]", "", s)
    if len(s) == 12:
        s = "0" + s
    if len(s) != 13 or not s.isdigit():
        return None
    return s


def normalize_phone(val: Any) -> Optional[str]:
    """電話番号を正規化。数字・ハイフン・括弧のみ有効。"""
    if val is None:
        return None
    s = str(val).strip()
    if not s or len(s) < 10:
        return None
    if RE_DATE_JP.match(s) or RE_DATE_SLASH.match(s):
        return None
    if "都道府県" in s or "市" in s or "区" in s:
        return None
    digits = re.sub(r"[^\d]", "", s)
    if len(digits) < 9:
        return None
    return s


def parse_numeric(val: Any) -> Optional[int]:
    """数値をパース。百万円、カンマ、日付混入を処理。"""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if RE_DATE_JP.match(s) or RE_DATE_SLASH.match(s):
        return None
    m = RE_MAN_YEN.search(s)
    if m:
        try:
            n = int(m.group(1).replace(",", ""))
            return n * 1_000_000
        except ValueError:
            return None
    s = s.replace(",", "")
    try:
        if "E" in s.upper():
            return int(float(s))
        return int(float(s))
    except (ValueError, OverflowError):
        return None


def get_cell(row: list, idx: Optional[int]) -> Optional[str]:
    if idx is None or idx < 0 or idx >= len(row):
        return None
    v = row[idx]
    if v is None or (isinstance(v, float) and (v != v)):
        return None
    s = str(v).strip()
    return s if s else None


def collect_csv_files(base_dir: Path) -> List[Path]:
    """base_dir配下の全CSVを再帰的に収集"""
    return sorted(base_dir.rglob("*.csv"))


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )


def run_import(dry_run: bool = False, source: str = "fixed_csv_2") -> Dict[str, int]:
    """メイン処理"""
    base = BASE_DIR / source
    if not base.exists():
        base = BASE_DIR / "csv_2"
        logger.info("fixed_csv_2 が存在しないため csv_2 を使用: %s", base)

    files = collect_csv_files(base)
    logger.info("対象CSV: %s 件 (%s)", len(files), base)

    stats = {"files": 0, "rows": 0, "updated_corp": 0, "updated_phone": 0, "no_match": 0, "skip": 0}

    if dry_run:
        logger.info("[DRY RUN] 書き込みは行いません")

    conn = None
    if not dry_run:
        try:
            conn = get_connection()
        except Exception as e:
            logger.exception("DB接続エラー: %s", e)
            raise

    from contextlib import contextmanager

    @contextmanager
    def maybe_cursor():
        if conn:
            with conn.cursor() as c:
                yield c
        else:
            yield None

    try:
        with maybe_cursor() as cur:
            for fp in files:
                try:
                    with open(fp, "r", encoding="utf-8", errors="replace") as f:
                        reader = csv.reader(f)
                        header = next(reader)
                except Exception as e:
                    logger.warning("読み込みスキップ %s: %s", fp, e)
                    continue

                hmap = resolve_header_map(header)
                if "name" not in hmap and "corporate_number" not in hmap and "phone_number" not in hmap:
                    stats["skip"] += 1
                    continue

                stats["files"] += 1
                rel = fp.relative_to(BASE_DIR)

                with open(fp, "r", encoding="utf-8", errors="replace") as f:
                    reader = csv.reader(f)
                    next(reader)
                    for row_num, row in enumerate(reader, start=2):
                        if not row:
                            continue
                        stats["rows"] += 1

                        name = get_cell(row, hmap.get("name"))
                        corp_raw = get_cell(row, hmap.get("corporate_number"))
                        phone_raw = get_cell(row, hmap.get("phone_number"))
                        corp = normalize_corporate_number(corp_raw)
                        phone = normalize_phone(phone_raw)

                        if not corp and not phone:
                            stats["no_match"] += 1
                            continue

                        updates: Dict[str, Any] = {}
                        if name:
                            updates["name"] = name
                        pref = get_cell(row, hmap.get("prefecture"))
                        if pref:
                            updates["prefecture"] = pref
                        addr = get_cell(row, hmap.get("address"))
                        if addr:
                            updates["address"] = addr
                        postal = get_cell(row, hmap.get("postal_code"))
                        if postal:
                            updates["postal_code"] = postal
                        rep = get_cell(row, hmap.get("representative_name"))
                        if rep:
                            updates["representative_name"] = rep
                        if phone:
                            updates["phone_number"] = phone
                        url = get_cell(row, hmap.get("company_url"))
                        if url:
                            updates["company_url"] = url

                        cap = parse_numeric(get_cell(row, hmap.get("capital_stock")))
                        if cap is not None:
                            updates["capital_stock"] = cap
                        rev = parse_numeric(get_cell(row, hmap.get("latest_revenue")))
                        if rev is None:
                            rev = parse_numeric(get_cell(row, hmap.get("revenue")))
                        if rev is not None:
                            updates["latest_revenue"] = rev
                        prof = parse_numeric(get_cell(row, hmap.get("latest_profit")))
                        if prof is not None:
                            updates["latest_profit"] = prof
                        emp = parse_numeric(get_cell(row, hmap.get("employee_count")))
                        if emp is not None:
                            updates["employee_count"] = emp

                        ind1 = get_cell(row, hmap.get("industry_large")) or get_cell(row, hmap.get("industry"))
                        if ind1:
                            updates["industry"] = ind1
                            updates["industry_large"] = ind1
                        ind2 = get_cell(row, hmap.get("industry_middle"))
                        if ind2:
                            updates["industry_middle"] = ind2
                        ind3 = get_cell(row, hmap.get("industry_small"))
                        if ind3:
                            updates["industry_small"] = ind3
                        over = get_cell(row, hmap.get("overview"))
                        if over:
                            updates["overview"] = over

                        if not updates:
                            continue

                        if dry_run:
                            if stats["rows"] <= 10:
                                logger.info("  [DRY] %s L%d: corp=%s phone=%s → %s", rel, row_num, corp, phone, list(updates.keys()))
                            if corp:
                                stats["updated_corp"] += 1
                            else:
                                stats["updated_phone"] += 1
                            continue

                        set_parts = []
                        params: List[Any] = []
                        for k, v in updates.items():
                            set_parts.append(f'"{k}" = %s')
                            params.append(v)

                        if corp:
                            params.append(corp)
                            where = "corporate_number = %s"
                        else:
                            params.append(phone)
                            where = "phone_number = %s"

                        sql = f"UPDATE companies SET {', '.join(set_parts)} WHERE {where}"
                        try:
                            if cur:
                                cur.execute(sql, params)
                            if cur and cur.rowcount > 0:
                                if corp:
                                    stats["updated_corp"] += 1
                                else:
                                    stats["updated_phone"] += 1
                            else:
                                stats["no_match"] += 1
                        except Exception as e:
                            logger.warning("UPDATE失敗 %s L%d: %s", rel, row_num, e)

                if stats["files"] % 20 == 0:
                    logger.info("進捗: %s ファイル, 更新 corp=%s phone=%s", stats["files"], stats["updated_corp"], stats["updated_phone"])

        if conn:
            conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        logger.exception("処理エラー: %s", e)
        raise
    finally:
        if conn:
            conn.close()

    return stats


def main():
    parser = argparse.ArgumentParser(description="csv_2 のCSVを companies にインポート")
    parser.add_argument("--dry-run", action="store_true", help="書き込みを行わずに確認")
    parser.add_argument("--source", choices=["csv_2", "fixed_csv_2"], default="fixed_csv_2", help="データソース")
    args = parser.parse_args()

    if not args.dry_run and not DB_PASSWORD:
        logger.error("本番実行時は POSTGRES_PASSWORD 環境変数を設定してください")
        sys.exit(1)

    logger.info("接続先: %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)
    stats = run_import(dry_run=args.dry_run, source=args.source)
    logger.info("--- 集計 ---")
    logger.info("処理ファイル: %s, 行数: %s", stats["files"], stats["rows"])
    logger.info("更新(法人番号): %s, 更新(電話番号): %s", stats["updated_corp"], stats["updated_phone"])
    logger.info("マッチなし: %s, スキップ: %s", stats["no_match"], stats["skip"])


if __name__ == "__main__":
    main()
