#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
companies のうち「カラムズレが明らかな行」のみを id キーセットで走査し、
fixed_csv_3 の CSV から法人番号または正規化社名で正解行を逆引きして、
指定カラムのみ CSV の値で強制上書きする（一時テーブルに全件コピーしない）。

抽出条件（壊れ行のみ）:
  - 電話列（DB に存在する方を自動選択: phone_number を優先し、無ければ contact_phone_number）
    が NULL でなく、かつ電話として許容した文字以外を含む（!~ '^[0-9+() -]*$'）
  - または address がちょうど 13 桁の数字のみ（法人番号混入想定）

上書き対象（この5列のみ）:
  上記で選んだ電話列（値は CSV の電話と同じ）, prefecture, address, representative_name, overview

name / id / corporate_number / 業種関連など他カラムは一切 UPDATE しない。
--apply が無い場合は差分件数のみ表示（DB は変更しない）。
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

try:
    from psycopg2 import sql
    from psycopg2.extras import execute_values
except ImportError as e:
    raise SystemExit("psycopg2-binary が必要です") from e

_FW_BASE = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=_FW_BASE / ".env", override=True)
    load_dotenv()
except ImportError:
    pass

try:
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import repair_companies_column_shift_from_fixed_csv3 as rep

LOG = logging.getLogger("force_overwrite_csv")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CSV_ROOT = BASE_DIR / "fixed_csv_3"

# DB によっては phone_number が無く contact_phone_number のみ、という環境がある
_PHONE_COL_PRIORITY: Tuple[str, ...] = ("phone_number", "contact_phone_number")


def resolve_companies_schema_and_phone_column(cur) -> Tuple[str, str]:
    """実テーブル上の companies のスキーマ名と、電話番号として使う列名を決定する。"""
    cur.execute(
        """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_name = 'companies'
        ORDER BY (table_schema = 'public') DESC, table_schema
        LIMIT 1
        """
    )
    row = cur.fetchone()
    if not row:
        raise SystemExit("information_schema に companies テーブルが見つかりません。")
    schema = str(row[0])
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = 'companies'
          AND column_name = ANY(%s)
        """,
        (schema, list(_PHONE_COL_PRIORITY)),
    )
    found = {r[0] for r in cur.fetchall()}
    for col in _PHONE_COL_PRIORITY:
        if col in found:
            return schema, col
    raise SystemExit(
        f"{schema}.companies に {_PHONE_COL_PRIORITY} のいずれもありません。"
        "create_companies_table.sql 相当の DDL で列を追加するか、接続先 DB を確認してください。"
    )


def keyset_select_corrupted_query(schema: str, phone_col: str) -> sql.Composed:
    """キーセット抽出 SQL（c. 修飾・壊れ行条件）。phone_col は resolve の戻りのみ渡す。"""
    c_phone = sql.SQL("c.{}").format(sql.Identifier(phone_col))
    tbl = sql.Identifier(schema, "companies")
    return sql.SQL(
        """
SELECT
    c.id,
    c.name,
    c.corporate_number,
    {c_phone},
    c.prefecture,
    c.address,
    c.representative_name,
    c.overview
FROM {tbl} c
WHERE (
    ({c_phone} !~ '^[0-9+() -]*$' AND {c_phone} IS NOT NULL)
    OR (c.address ~ '^\\d{{13}}$')
)
  AND c.id > %s
ORDER BY c.id
LIMIT %s
"""
    ).format(c_phone=c_phone, tbl=tbl)


CORP_HEADERS = ["法人番号", "corporate_number", "会社法人等番号", "企業法人番号"]
NAME_HEADERS = ["会社名", "企業名", "name", "法人名", "商号"]
PHONE_HEADERS = ["電話番号", "電話番号(窓口)", "TEL", "tel", "phone_number", "会社電話"]
PREF_HEADERS = ["都道府県", "prefecture", "会社都道府県"]
ADDRESS_HEADERS = ["会社住所", "住所", "address", "所在地"]
REP_HEADERS = ["代表者名", "代表者", "代表取締役"]
OVERVIEW_HEADERS = ["概要", "概況", "overview", "説明"]


@dataclass
class OverwritePayload:
    """CSV 1行分（上書き用・生文字列・5カラムのみ）。"""
    phone_number: str
    prefecture: str
    address: str
    representative_name: str
    overview: str
    source_file: str
    source_line: int

    def as_tuple(self) -> Tuple[str, str, str, str, str]:
        return (
            self.phone_number,
            self.prefecture,
            self.address,
            self.representative_name,
            self.overview,
        )


def norm_text(v: Optional[str]) -> str:
    if v is None:
        return ""
    return unicodedata.normalize("NFKC", str(v)).strip()


def raw_text(v: Optional[str]) -> str:
    if v is None:
        return ""
    return str(v)


def norm_name_key(v: Optional[str]) -> str:
    return re.sub(r"\s+", "", norm_text(v).lower())


def norm_corp(v: Optional[str]) -> str:
    s = re.sub(r"\D", "", norm_text(v))
    if len(s) == 12:
        s = "0" + s
    return s if len(s) == 13 else ""


def extract_corp_keys(*vals: Optional[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for v in vals:
        digits = re.sub(r"\D", "", norm_text(v))
        if not digits:
            continue
        if len(digits) == 13:
            cands = [digits]
        elif len(digits) > 13:
            cands = [digits[i : i + 13] for i in range(0, len(digits) - 12)]
        else:
            cands = []
        for c in cands:
            k = norm_corp(c)
            if k and k not in seen:
                seen.add(k)
                out.append(k)
    return out


def pick_header(headers: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    hs = [str(h) for h in headers]
    for c in candidates:
        if c in hs:
            return c
    return None


def get_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            with path.open("r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "utf-8"


def iter_csv_paths(csv_root: Path) -> List[Path]:
    preferred = [csv_root / "1.csv", csv_root / "2.csv"]
    direct = [p for p in preferred if p.is_file()]
    if direct:
        return direct
    return sorted(csv_root.rglob("*.csv"), key=lambda p: p.stat().st_mtime)


def csv_cell(row: Dict[str, str], key: Optional[str]) -> str:
    if not key:
        return ""
    v = row.get(key)
    return "" if v is None else str(v)


def load_csv_indexes(csv_root: Path) -> Tuple[Dict[str, OverwritePayload], Dict[str, OverwritePayload]]:
    """13桁法人番号キー・正規化社名キー（後勝ち）。"""
    by_corp: Dict[str, OverwritePayload] = {}
    by_norm_name: Dict[str, OverwritePayload] = {}
    paths = iter_csv_paths(csv_root)
    if not paths:
        raise SystemExit(f"CSV が見つかりません: {csv_root}")

    for fp in paths:
        enc = get_encoding(fp)
        with fp.open("r", encoding=enc, newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                continue
            name_h = pick_header(reader.fieldnames, NAME_HEADERS)
            corp_h = pick_header(reader.fieldnames, CORP_HEADERS)
            phone_h = pick_header(reader.fieldnames, PHONE_HEADERS)
            pref_h = pick_header(reader.fieldnames, PREF_HEADERS)
            addr_h = pick_header(reader.fieldnames, ADDRESS_HEADERS)
            rep_h = pick_header(reader.fieldnames, REP_HEADERS)
            ov_h = pick_header(reader.fieldnames, OVERVIEW_HEADERS)
            if not name_h and not corp_h:
                continue

            for line_no, raw in enumerate(reader, start=2):
                row = {str(k): ("" if v is None else str(v)) for k, v in raw.items()}
                pl = OverwritePayload(
                    phone_number=raw_text(csv_cell(row, phone_h)),
                    prefecture=raw_text(csv_cell(row, pref_h)),
                    address=raw_text(csv_cell(row, addr_h)),
                    representative_name=raw_text(csv_cell(row, rep_h)),
                    overview=raw_text(csv_cell(row, ov_h)),
                    source_file=str(fp),
                    source_line=line_no,
                )
                ck = norm_corp(csv_cell(row, corp_h) if corp_h else "")
                if ck:
                    by_corp[ck] = pl
                nk = norm_name_key(csv_cell(row, name_h) if name_h else "")
                if nk:
                    by_norm_name[nk] = pl

    LOG.info("CSV indexes: corp=%d norm_name=%d files=%d", len(by_corp), len(by_norm_name), len(paths))
    return by_corp, by_norm_name


def lookup_payload(
    db_id: str,
    db_name: str,
    db_corp: Optional[str],
    db_phone: Optional[str],
    db_addr: Optional[str],
    by_corp: Dict[str, OverwritePayload],
    by_norm_name: Dict[str, OverwritePayload],
) -> Tuple[Optional[OverwritePayload], str]:
    for k in extract_corp_keys(db_corp, db_phone, db_addr):
        p = by_corp.get(k)
        if p:
            return p, f"corp:{k}"
    nk = norm_name_key(db_name)
    if nk:
        p = by_norm_name.get(nk)
        if p:
            return p, "name"
    return None, "unmatched"


def row_differs(
    db_row: Tuple,
    pl: OverwritePayload,
) -> bool:
    """DB 行と CSV 期待が1つでも違えば True（corporate_number の差分は無視）。"""
    _, _, d_corp, d_phone, d_pref, d_addr, d_rep, d_ov = db_row
    return (
        raw_text(d_phone) != pl.phone_number
        or raw_text(d_pref) != pl.prefecture
        or raw_text(d_addr) != pl.address
        or raw_text(d_rep) != pl.representative_name
        or raw_text(d_ov) != pl.overview
    )


def ensure_staging(cur) -> None:
    cur.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS _force_csv (
            id TEXT NOT NULL PRIMARY KEY,
            phone_number TEXT,
            prefecture TEXT,
            address TEXT,
            representative_name TEXT,
            overview TEXT
        ) ON COMMIT DROP
        """
    )


def flush_updates(
    cur,
    batch: List[Tuple],
    chunk_insert: int,
    schema: str,
    db_phone_column: str,
) -> int:
    if not batch:
        return 0
    ensure_staging(cur)
    cur.execute("TRUNCATE _force_csv")
    for i in range(0, len(batch), chunk_insert):
        chunk = batch[i : i + chunk_insert]
        execute_values(
            cur,
            """
            INSERT INTO _force_csv (
                id, phone_number, prefecture, address,
                representative_name, overview
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                phone_number = EXCLUDED.phone_number,
                prefecture = EXCLUDED.prefecture,
                address = EXCLUDED.address,
                representative_name = EXCLUDED.representative_name,
                overview = EXCLUDED.overview
            """,
            chunk,
        )
    # SET 句の左辺にはテーブルエイリアス（c.）を付けられない。WHERE 等の参照は c. のまま。
    target_phone_col = sql.Identifier(db_phone_column)
    tbl = sql.Identifier(schema, "companies")
    cur.execute(
        sql.SQL(
            """
        UPDATE {tbl} c SET
            {target_phone_col} = s.phone_number,
            prefecture = s.prefecture,
            address = s.address,
            representative_name = s.representative_name,
            overview = s.overview,
            updated_at = CURRENT_TIMESTAMP
        FROM _force_csv s
        WHERE c.id = s.id
        """
        ).format(tbl=tbl, target_phone_col=target_phone_col)
    )
    n = cur.rowcount
    cur.execute("TRUNCATE _force_csv")
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description="Force overwrite selected columns from fixed_csv_3 CSV")
    ap.add_argument("--csv-root", type=Path, default=DEFAULT_CSV_ROOT)
    ap.add_argument(
        "--fetch-chunk",
        type=int,
        default=5000,
        help="companies キーセット 1 ページあたりの行数",
    )
    ap.add_argument("--apply-batch", type=int, default=2000, help="UPDATE バッチの staging 行数")
    ap.add_argument(
        "--log-every",
        type=int,
        default=5_000,
        help="走査件数をこの間隔で INFO ログ（0 で無効）",
    )
    ap.add_argument("--apply", action="store_true", help="DB を更新する（未指定は dry-run）")
    args = ap.parse_args()

    if not args.csv_root.is_dir():
        raise SystemExit(f"CSV root が見つかりません: {args.csv_root}")

    by_corp, by_norm_name = load_csv_indexes(args.csv_root)

    conn = rep.get_connection()
    scanned = 0
    csv_matched = 0
    would_change = 0
    updated_total = 0
    pending: List[Tuple] = []
    next_log_at = args.log_every if args.log_every else 0

    try:
        cur = conn.cursor()

        # 接続先とカラムの実在確認（キーセット SELECT 直前）
        cur.execute("SELECT current_schema(), current_database();")
        schema, db = cur.fetchone()
        LOG.info("接続情報: DB=%s, Schema=%s", db, schema)

        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'companies'
              AND table_schema = current_schema();
            """
        )
        columns = [r[0] for r in cur.fetchall()]
        LOG.info("発見されたカラム一覧: %s", columns)

        if "phone_number" not in columns:
            LOG.error(
                "致命的: 現在のスキーマの companies テーブルに phone_number が存在しません。"
            )

        schema, db_phone_col = resolve_companies_schema_and_phone_column(cur)
        LOG.info("companies 参照: schema=%s phone_column=%s", schema, db_phone_col)
        keyset_q = keyset_select_corrupted_query(schema, db_phone_col)
        last_id = ""
        while True:
            cur.execute(keyset_q, (last_id, args.fetch_chunk))
            chunk = cur.fetchall()
            if not chunk:
                break
            last_id = str(chunk[-1][0])

            for row in chunk:
                scanned += 1
                db_id = str(row[0])
                db_name = str(row[1]) if row[1] is not None else ""
                pl, _method = lookup_payload(
                    db_id,
                    db_name,
                    row[2],
                    row[3],
                    row[5],
                    by_corp,
                    by_norm_name,
                )
                if not pl:
                    continue
                csv_matched += 1
                if row_differs(row, pl):
                    would_change += 1
                if not args.apply:
                    continue
                if not row_differs(row, pl):
                    continue
                pending.append((db_id,) + pl.as_tuple())
                if len(pending) >= args.apply_batch:
                    updated_total += flush_updates(
                        cur, pending, args.apply_batch, schema, db_phone_col
                    )
                    pending.clear()

            while args.log_every and next_log_at and scanned >= next_log_at:
                LOG.info(
                    "progress scanned=%d csv_matched=%d rows_needing_overwrite=%d rows_updated=%d last_id=%s",
                    scanned,
                    csv_matched,
                    would_change,
                    updated_total,
                    (last_id[:64] + ("..." if len(last_id) > 64 else "")),
                )
                next_log_at += args.log_every

        if args.apply and pending:
            updated_total += flush_updates(
                cur, pending, args.apply_batch, schema, db_phone_col
            )
            pending.clear()

        if args.apply:
            conn.commit()
            LOG.info(
                "APPLIED: scanned=%d csv_matched=%d would_have_diff=%d rows_updated=%d",
                scanned,
                csv_matched,
                would_change,
                updated_total,
            )
        else:
            conn.rollback()
            LOG.info(
                "DRY-RUN: scanned=%d csv_matched=%d rows_needing_overwrite=%d (use --apply)",
                scanned,
                csv_matched,
                would_change,
            )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()