#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3/unit_million・unit_yen 配下の CSV から財務データを読み取り、
companies.financials（JSONB 配列）および latest_revenue / latest_profit を更新する。

金額列はいずれのディレクトリも「千円」単位の数値として扱い、円に直すために 1000 倍する。

名寄せ: (1) 法人番号一致（列がなく値が取れない場合は次へ） (2) 会社名＋代表者名＋電話の複合一致。
法人番号が有効な値だが DB に無い場合は複合キーにはフォールバックしない。
同一キーで DB に複数行ある場合は、ヒットしたすべての id に同じ財務データを反映する。

起動時に companies を全件メモリへ読み込み辞書で O(1) 照合。本番時は UPDATE を
execute_batch で 1000 件単位で実行する。

使い方:
  python3 scripts/migrate_financials_from_fixed_csv3.py              # ドライラン（更新なし）
  python3 scripts/migrate_financials_from_fixed_csv3.py --execute   # 本番更新

接続: DATABASE_URL または POSTGRES_*（run_db_migrations.py と同様）。
プロジェクト直下の .env があれば未設定の環境変数のみ読み込む。
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
from pathlib import Path
from collections import defaultdict
from typing import Any, Optional

import psycopg2
from psycopg2.extras import Json, RealDictCursor, execute_batch

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BATCH_UPDATE_SIZE = 1000

SQL_UPDATE_FINANCIALS = """
UPDATE companies
SET financials = %s,
    latest_revenue = COALESCE(%s, latest_revenue),
    latest_profit = COALESCE(%s, latest_profit),
    updated_at = NOW()
WHERE id = %s
"""

TARGET_DIRS = (
    PROJECT_ROOT / "fixed_csv_3" / "unit_million",
    PROJECT_ROOT / "fixed_csv_3" / "unit_yen",
)

DEFAULT_TERM = "直近決算"

# (論理名, ヘッダー候補の優先順)
COLUMN_ALIASES: list[tuple[str, list[str]]] = [
    ("corporate_number", ["法人番号", "corporate_number"]),
    # 複合キー用（法人番号が空の行のみ使用。いずれか1列でも値が取れればよい）
    ("match_company_name", ["会社名", "企業名", "name"]),
    ("match_representative_name", ["代表者名", "代表者"]),
    ("match_phone", ["電話番号", "電話番号(窓口)", "TEL", "tel"]),
    ("term", ["直近決算年月", "決算年月"]),
    ("sales_kyen", ["直近売上", "売上", "売上高"]),
    ("profit_kyen", ["直近利益", "利益", "当期純利益"]),
    ("total_assets_kyen", ["総資産"]),
    ("net_assets_kyen", ["純資産"]),
    ("equity_ratio", ["自己資本比率"]),
]

RE_DATE_JP = re.compile(r"^\d{4}年\d{1,2}月\d{1,2}日$")
RE_DATE_SLASH = re.compile(r"^\d{4}/\d{1,2}/\d{1,2}")
RE_MAN_YEN = re.compile(r"([\d,]+)\s*百万円?")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_dotenv(p: Path) -> None:
    if not p.is_file():
        return
    for raw in p.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip()
        if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
            v = v[1:-1]
        os.environ.setdefault(k, v)


def connect():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)
    password = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD") or ""
    if not password:
        logger.error("DATABASE_URL または POSTGRES_PASSWORD を設定してください。")
        sys.exit(1)
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1").strip(),
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=password,
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        sslmode=os.getenv("POSTGRES_SSLMODE", "require").strip(),
    )


def normalize_header(raw: str) -> str:
    s = str(raw).strip().strip("\ufeff").replace("\n", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)


def build_header_map(headers: list[str]) -> dict[str, int]:
    norm_to_idx: dict[str, int] = {}
    for i, h in enumerate(headers):
        norm_to_idx[normalize_header(h)] = i
    out: dict[str, int] = {}
    for logical, aliases in COLUMN_ALIASES:
        for alias in aliases:
            na = normalize_header(alias)
            if na in norm_to_idx:
                out[logical] = norm_to_idx[na]
                break
    return out


def detect_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            with path.open("r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "utf-8"


def normalize_corporate_number(val: Any) -> Optional[str]:
    if val is None or (isinstance(val, float) and (val != val)):
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


def parse_thousand_yen_to_yen(val: Any) -> Optional[int]:
    """千円単位のセルを整数（円）に変換。日付・百万円表記は除外。"""
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
            n = int(float(s))
        else:
            n = int(float(s))
    except (ValueError, OverflowError):
        return None
    return n * 1000


def parse_equity_ratio(val: Any) -> Any:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    t = s.replace("%", "").strip().replace(",", "")
    try:
        return float(t)
    except ValueError:
        return s


def get_cell(row: list[str], idx: Optional[int]) -> Optional[str]:
    if idx is None or idx < 0 or idx >= len(row):
        return None
    v = row[idx]
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def normalize_matching_name(val: Optional[str]) -> str:
    """比較用: 前後・文字間の空白（半角・全角）を除去。"""
    if not val:
        return ""
    s = str(val).strip()
    return re.sub(r"[\s\u3000]+", "", s)


def normalize_matching_phone_digits(val: Optional[str]) -> str:
    """比較用: ハイフン（- / ー / － 等）と空白を除き数字のみ。"""
    if not val:
        return ""
    s = str(val).strip()
    for ch in (
        "-",
        "ー",
        "－",
        "—",
        "–",
        "−",
    ):
        s = s.replace(ch, "")
    s = re.sub(r"[\s\u3000]+", "", s)
    return re.sub(r"[^\d]", "", s)


def composite_key_string(nn: str, nr: str, nd: str) -> str:
    """オンメモリ辞書用の複合キー（例: 株式会社テスト_山田太郎_0312345678）。"""
    return f"{nn}_{nr}_{nd}"


def _unique_ordered_ids(ids: list[str]) -> list[str]:
    """同一キーに同一 id が重複して積まれた場合（例: 電話2列が同一正規化）に除重。"""
    seen: set[str] = set()
    out: list[str] = []
    for i in ids:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


def load_companies_lookup(cur: Any) -> tuple[
    dict[str, list[str]],
    dict[str, list[str]],
    dict[str, Any],
]:
    """
    companies を全件読み込み、法人番号辞書・複合キー辞書・id→financials を構築する。
    各キーに対応する id はリスト（同一法人番号・同一複合キーの重複行はすべて含む）。
    """
    logger.info("companies テーブルを全件ロードしてインデックスを構築しています…")
    cur.execute(
        """
        SELECT id, corporate_number, name, representative_name,
               phone_number, contact_phone_number, financials
        FROM companies
        """
    )
    rows = cur.fetchall()

    corp_buckets: defaultdict[str, list[str]] = defaultdict(list)
    composite_buckets: defaultdict[str, list[str]] = defaultdict(list)
    id_to_financials: dict[str, Any] = {}

    for r in rows:
        rid = str(r["id"])
        id_to_financials[rid] = r["financials"]

        cn = normalize_corporate_number(r.get("corporate_number"))
        if cn:
            corp_buckets[cn].append(rid)

        nn = normalize_matching_name(r.get("name"))
        nr = normalize_matching_name(r.get("representative_name"))
        for phone in (r.get("phone_number"), r.get("contact_phone_number")):
            nd = normalize_matching_phone_digits(phone)
            ck = composite_key_string(nn, nr, nd) if nn and nr and nd else ""
            if ck:
                composite_buckets[ck].append(rid)

    dict_by_corp_num: dict[str, list[str]] = {
        cn: _unique_ordered_ids(ids) for cn, ids in corp_buckets.items()
    }
    dict_by_composite: dict[str, list[str]] = {
        ck: _unique_ordered_ids(ids) for ck, ids in composite_buckets.items()
    }

    logger.info(
        "インデックス構築完了: 企業行=%s, 法人番号キー=%s, 複合キー=%s",
        len(rows),
        len(dict_by_corp_num),
        len(dict_by_composite),
    )
    return dict_by_corp_num, dict_by_composite, id_to_financials


def resolve_match(
    hmap: dict[str, int],
    row: list[str],
    dict_by_corp_num: dict[str, list[str]],
    dict_by_composite: dict[str, list[str]],
) -> tuple[Optional[list[str]], str]:
    """
    第一候補: 法人番号（列があり有効な値のときのみ。インデックス未登録時は複合にフォールバックしない）。
    第二候補: 会社名＋代表者名＋電話。
    戻り値: (マッチした id のリスト, reason)
    """
    has_corp_col = "corporate_number" in hmap
    corp: Optional[str] = None
    if has_corp_col:
        corp = normalize_corporate_number(get_cell(row, hmap["corporate_number"]))
    if corp:
        ids = dict_by_corp_num.get(corp)
        if not ids:
            return None, "no_match"
        return ids, "corporate"

    cname = get_cell(row, hmap.get("match_company_name"))
    rname = get_cell(row, hmap.get("match_representative_name"))
    pcell = get_cell(row, hmap.get("match_phone"))
    nn = normalize_matching_name(cname)
    nr = normalize_matching_name(rname)
    nd = normalize_matching_phone_digits(pcell)
    if not nn or not nr or not nd:
        return None, "skip_incomplete_composite"

    ck = composite_key_string(nn, nr, nd)
    ids = dict_by_composite.get(ck)
    if not ids:
        return None, "no_match"
    return ids, "composite"


def flush_update_batch(
    cur: Any,
    conn: Any,
    batch: list[tuple[Any, Any, Any, str]],
    execute: bool,
) -> None:
    if not batch or not execute:
        batch.clear()
        return
    n = len(batch)
    execute_batch(cur, SQL_UPDATE_FINANCIALS, batch, page_size=min(n, BATCH_UPDATE_SIZE))
    conn.commit()
    logger.debug("execute_batch: %s 行をコミット", n)
    batch.clear()


def financials_to_list(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    if isinstance(raw, dict):
        return [raw]
    return []


def merge_financials_array(
    existing: Any,
    new_entry: dict[str, Any],
) -> list[dict[str, Any]]:
    arr = [dict(x) for x in financials_to_list(existing)]
    term = new_entry.get("term") or DEFAULT_TERM
    new_entry = dict(new_entry)
    new_entry["term"] = term

    for i, item in enumerate(arr):
        t = item.get("term")
        if t is None or str(t).strip() == "":
            t = DEFAULT_TERM
        if str(t).strip() == str(term).strip():
            arr[i] = new_entry
            return arr
    arr.append(new_entry)
    return arr


def collect_csv_files() -> list[Path]:
    out: list[Path] = []
    for d in TARGET_DIRS:
        if not d.is_dir():
            logger.warning("ディレクトリがありません（スキップ）: %s", d)
            continue
        for fp in sorted(d.rglob("*.csv")):
            if fp.is_file():
                out.append(fp)
    return sorted(set(out))


def row_to_financial_entry(hmap: dict[str, int], row: list[str]) -> dict[str, Any]:
    term_raw = get_cell(row, hmap.get("term"))
    term = (term_raw or "").strip() or DEFAULT_TERM

    sales_yen = parse_thousand_yen_to_yen(get_cell(row, hmap.get("sales_kyen")))
    profit_yen = parse_thousand_yen_to_yen(get_cell(row, hmap.get("profit_kyen")))
    total_assets_yen = parse_thousand_yen_to_yen(get_cell(row, hmap.get("total_assets_kyen")))
    net_assets_yen = parse_thousand_yen_to_yen(get_cell(row, hmap.get("net_assets_kyen")))

    eq_raw = get_cell(row, hmap.get("equity_ratio"))
    equity_ratio = parse_equity_ratio(eq_raw) if eq_raw is not None else None

    return {
        "term": term,
        "sales": sales_yen,
        "profit": profit_yen,
        "totalAssets": total_assets_yen,
        "netAssets": net_assets_yen,
        "equityRatio": equity_ratio,
    }


def run(*, execute: bool, verbose: bool) -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    files = collect_csv_files()
    if not files:
        logger.error("対象 CSV がありません: %s / %s", TARGET_DIRS[0], TARGET_DIRS[1])
        return

    stats = {
        "files": 0,
        "rows": 0,
        "match_corporate": 0,
        "match_composite": 0,
        "no_match": 0,
        "skip_incomplete_composite": 0,
        "db_record_updates": 0,
    }

    conn = connect()
    try:
        conn.autocommit = False
        cur = conn.cursor(cursor_factory=RealDictCursor)

        dict_by_corp_num, dict_by_composite, id_to_financials = load_companies_lookup(cur)
        update_batch: list[tuple[Any, Any, Any, str]] = []

        for fp in files:
            try:
                rel = fp.relative_to(PROJECT_ROOT)
            except ValueError:
                rel = fp
            enc = detect_encoding(fp)
            with fp.open("r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                try:
                    headers = next(reader)
                except StopIteration:
                    logger.warning("空ファイルをスキップ: %s", rel)
                    continue

                hmap = build_header_map(headers)
                stats["files"] += 1
                if "corporate_number" not in hmap:
                    logger.info(
                        "%s: 法人番号列なし。複合キー（会社名・代表者・電話）のみで照合します。",
                        rel,
                    )

                for row_no, row in enumerate(reader, start=2):
                    stats["rows"] += 1
                    entry = row_to_financial_entry(hmap, row)
                    sales_yen = entry["sales"]
                    profit_yen = entry["profit"]

                    matched_ids, reason = resolve_match(
                        hmap, row, dict_by_corp_num, dict_by_composite
                    )
                    if reason == "no_match":
                        stats["no_match"] += 1
                        logger.debug("DB 未一致: %s 行 %s", rel, row_no)
                        continue
                    if reason == "skip_incomplete_composite":
                        stats["skip_incomplete_composite"] += 1
                        continue

                    if reason == "corporate":
                        stats["match_corporate"] += 1
                    else:
                        stats["match_composite"] += 1

                    for row_id in matched_ids:
                        merged = merge_financials_array(id_to_financials[row_id], entry)
                        id_to_financials[row_id] = merged

                        if execute:
                            update_batch.append(
                                (Json(merged), sales_yen, profit_yen, row_id)
                            )
                            if len(update_batch) >= BATCH_UPDATE_SIZE:
                                flush_update_batch(cur, conn, update_batch, True)
                        elif verbose:
                            logger.info(
                                "[DRY-RUN] %s match=%s id=%s ids_all=%s -> latest_revenue=%s latest_profit=%s financials=%s",
                                rel,
                                reason,
                                row_id,
                                matched_ids,
                                sales_yen,
                                profit_yen,
                                json.dumps(merged, ensure_ascii=False)[:240],
                            )

                        stats["db_record_updates"] += 1

        if execute:
            flush_update_batch(cur, conn, update_batch, True)
        else:
            conn.rollback()

        cur.close()
    finally:
        conn.close()

    csv_matched_rows = stats["match_corporate"] + stats["match_composite"]
    logger.info(
        "完了: ファイル=%s, CSV処理行数=%s, CSVでマッチした行数=%s, "
        "DBレコード更新対象件数(id数)=%s, 法人番号一致行=%s, 複合キー一致行=%s, "
        "DB未一致行=%s, 複合キー欠損スキップ行=%s, (%s)",
        stats["files"],
        stats["rows"],
        csv_matched_rows,
        stats["db_record_updates"],
        stats["match_corporate"],
        stats["match_composite"],
        stats["no_match"],
        stats["skip_incomplete_composite"],
        "本番でコミット済みの UPDATE 件数" if execute else "ドライラン（DB未更新）",
    )


def main() -> None:
    p = argparse.ArgumentParser(description="fixed_csv_3 財務 CSV → companies.financials 移行")
    p.add_argument(
        "--execute",
        action="store_true",
        help="指定時のみ DB を更新（未指定はドライラン）",
    )
    p.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="ドライラン時に行ごとの詳細ログを出す（件数が多いと冗長）",
    )
    args = p.parse_args()
    if not args.execute:
        logger.info("ドライラン（--execute なし）。financials はマージ結果を計算しますが COMMIT しません。")
    run(execute=args.execute, verbose=args.verbose)


if __name__ == "__main__":
    main()
