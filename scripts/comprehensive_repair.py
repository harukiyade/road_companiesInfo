#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
companies の壊れ行をキーセットで走査し、次の 3 段照合で修復候補を引く。

1) fixed_csv_3/1,2 のメインCSV: 法人番号一致
2) fixed_csv_3/1,2 のメインCSV: 正規化社名 + 正規化電話一致
3) fixed_csv_3/gbizinfo/Kihonjoho_UTF-8 2.csv: 法人番号一致

上書き対象:
  phone_number(または contact_phone_number), prefecture, address, representative_name, overview

抽出条件:
  - 電話番号異常
  - address が 13 桁数字のみ
  - address に adsbygoogle を含む
  - revenue_growth::text が 13 桁数字のみ（列が存在する場合）
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

try:
    from psycopg2 import sql
    from psycopg2.extras import execute_values
except ImportError as e:
    raise SystemExit("psycopg2-binary が必要です") from e

_BASE = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=_BASE / ".env", override=True)
    load_dotenv()
except ImportError:
    pass

try:
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import repair_companies_column_shift_from_fixed_csv3 as rep

LOG = logging.getLogger("comprehensive_repair")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CSV_ROOT = BASE_DIR / "fixed_csv_3"
DEFAULT_GBIZ_PATH = DEFAULT_CSV_ROOT / "gbizinfo" / "Kihonjoho_UTF-8 2.csv"
_PHONE_COL_PRIORITY: Tuple[str, ...] = ("phone_number", "contact_phone_number")

CORP_HEADERS = ["法人番号", "corporate_number", "会社法人等番号", "企業法人番号"]
NAME_HEADERS = ["会社名", "企業名", "name", "法人名", "商号", "商号又は名称"]
PHONE_HEADERS = ["電話番号", "電話番号(窓口)", "TEL", "tel", "phone_number", "会社電話"]
PREF_HEADERS = ["都道府県", "prefecture", "会社都道府県"]
ADDRESS_HEADERS = ["会社住所", "住所", "address", "所在地", "本社所在地", "本店又は主たる事務所の所在地"]
REP_HEADERS = ["代表者名", "代表者", "代表取締役", "法人代表者名"]
OVERVIEW_HEADERS = ["概要", "概況", "overview", "説明", "事業概要", "事業内容"]

# GBizInfo 側の代表的な列名
GBIZ_CORP_HEADERS = ["法人番号", "corporate_number"]
GBIZ_NAME_HEADERS = ["法人名", "商号", "商号又は名称", "会社名"]
GBIZ_PREF_HEADERS = ["都道府県", "都道府県名"]
GBIZ_ADDRESS_HEADERS = ["本社所在地", "住所", "所在地", "本店又は主たる事務所の所在地"]
GBIZ_REP_HEADERS = ["法人代表者名", "代表者名", "代表者"]
GBIZ_OVERVIEW_HEADERS = ["事業概要", "概要", "事業内容"]
GBIZ_PHONE_HEADERS = ["電話番号", "電話", "代表電話", "TEL"]


@dataclass
class OverwritePayload:
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


def norm_name(v: Optional[str]) -> str:
    return re.sub(r"\s+", "", norm_text(v).lower())


def norm_phone(v: Optional[str]) -> str:
    return re.sub(r"\D", "", norm_text(v))


def norm_corp(v: Optional[str]) -> str:
    s = re.sub(r"\D", "", norm_text(v))
    if len(s) == 12:
        s = "0" + s
    return s if len(s) == 13 else ""


def make_name_phone_key(name: Optional[str], phone: Optional[str]) -> Tuple[str, str]:
    n = norm_name(name)
    p = norm_phone(phone)
    if not n or not p:
        return ("", "")
    return (n, p)


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


def iter_main_csv_paths(csv_root: Path) -> List[Path]:
    paths: List[Path] = []
    for d in (csv_root / "1", csv_root / "2"):
        if d.is_dir():
            paths.extend(sorted(d.rglob("*.csv"), key=lambda p: p.stat().st_mtime))
    return paths


def csv_cell(row: Dict[str, str], key: Optional[str]) -> str:
    if not key:
        return ""
    v = row.get(key)
    return "" if v is None else str(v)


def load_main_indexes(
    csv_root: Path,
) -> Tuple[Dict[str, OverwritePayload], Dict[Tuple[str, str], OverwritePayload]]:
    dict_corp: Dict[str, OverwritePayload] = {}
    dict_name_phone: Dict[Tuple[str, str], OverwritePayload] = {}
    files = iter_main_csv_paths(csv_root)
    if not files:
        raise SystemExit(f"fixed_csv_3/1,2 の CSV が見つかりません: {csv_root}")

    for fp in files:
        enc = get_encoding(fp)
        with fp.open("r", encoding=enc, newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                continue
            corp_h = pick_header(reader.fieldnames, CORP_HEADERS)
            name_h = pick_header(reader.fieldnames, NAME_HEADERS)
            phone_h = pick_header(reader.fieldnames, PHONE_HEADERS)
            pref_h = pick_header(reader.fieldnames, PREF_HEADERS)
            addr_h = pick_header(reader.fieldnames, ADDRESS_HEADERS)
            rep_h = pick_header(reader.fieldnames, REP_HEADERS)
            ov_h = pick_header(reader.fieldnames, OVERVIEW_HEADERS)
            if not corp_h and not (name_h and phone_h):
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
                    dict_corp[ck] = pl
                nk = make_name_phone_key(csv_cell(row, name_h), csv_cell(row, phone_h))
                if nk[0] and nk[1]:
                    dict_name_phone[nk] = pl

    LOG.info(
        "main CSV indexes: corp=%d name_phone=%d files=%d",
        len(dict_corp),
        len(dict_name_phone),
        len(files),
    )
    return dict_corp, dict_name_phone


def load_gbiz_index(gbiz_csv_path: Path) -> Dict[str, OverwritePayload]:
    if not gbiz_csv_path.is_file():
        raise SystemExit(f"GBiz CSV が見つかりません: {gbiz_csv_path}")
    dict_gbiz: Dict[str, OverwritePayload] = {}
    enc = get_encoding(gbiz_csv_path)
    with gbiz_csv_path.open("r", encoding=enc, newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return dict_gbiz
        corp_h = pick_header(reader.fieldnames, GBIZ_CORP_HEADERS)
        name_h = pick_header(reader.fieldnames, GBIZ_NAME_HEADERS)
        pref_h = pick_header(reader.fieldnames, GBIZ_PREF_HEADERS)
        addr_h = pick_header(reader.fieldnames, GBIZ_ADDRESS_HEADERS)
        rep_h = pick_header(reader.fieldnames, GBIZ_REP_HEADERS)
        ov_h = pick_header(reader.fieldnames, GBIZ_OVERVIEW_HEADERS)
        phone_h = pick_header(reader.fieldnames, GBIZ_PHONE_HEADERS)
        if not corp_h:
            raise SystemExit(
                f"GBiz CSV に法人番号列がありません: {gbiz_csv_path} headers={reader.fieldnames[:20]}"
            )

        for line_no, raw in enumerate(reader, start=2):
            row = {str(k): ("" if v is None else str(v)) for k, v in raw.items()}
            ck = norm_corp(csv_cell(row, corp_h))
            if not ck:
                continue
            # GBiz の列を companies 側上書き対象に合わせる
            payload = OverwritePayload(
                phone_number=raw_text(csv_cell(row, phone_h)),
                prefecture=raw_text(csv_cell(row, pref_h)),
                address=raw_text(csv_cell(row, addr_h)),
                representative_name=raw_text(csv_cell(row, rep_h)),
                overview=raw_text(csv_cell(row, ov_h)),
                source_file=str(gbiz_csv_path),
                source_line=line_no,
            )
            # 法人名は今回はキー照合に使わないが、overview が空で name があれば補助情報として入れる
            if not payload.overview:
                payload.overview = raw_text(csv_cell(row, name_h))
            dict_gbiz[ck] = payload
    LOG.info("gbiz index: corp=%d file=%s", len(dict_gbiz), gbiz_csv_path)
    return dict_gbiz


def lookup_payload(
    db_corp: Optional[str],
    db_name: Optional[str],
    db_phone: Optional[str],
    dict_corp: Dict[str, OverwritePayload],
    dict_name_phone: Dict[Tuple[str, str], OverwritePayload],
    dict_gbiz: Dict[str, OverwritePayload],
) -> Tuple[Optional[OverwritePayload], str]:
    # 1. メインCSVから法人番号で検索
    corp_key = norm_corp(db_corp)
    if corp_key and corp_key in dict_corp:
        return dict_corp[corp_key], "main_csv_corp"

    # 2. メインCSVから社名+電話で検索
    name_phone_key = make_name_phone_key(db_name, db_phone)
    if name_phone_key[0] and name_phone_key[1] and name_phone_key in dict_name_phone:
        return dict_name_phone[name_phone_key], "main_csv_name_phone"

    # 3. GBizInfoから法人番号で検索
    if corp_key and corp_key in dict_gbiz:
        return dict_gbiz[corp_key], "gbizinfo"

    return None, "unmatched"


def row_differs(db_row: Tuple, pl: OverwritePayload) -> bool:
    _, _, _, d_phone, d_pref, d_addr, d_rep, d_ov = db_row
    return (
        raw_text(d_phone) != pl.phone_number
        or raw_text(d_pref) != pl.prefecture
        or raw_text(d_addr) != pl.address
        or raw_text(d_rep) != pl.representative_name
        or raw_text(d_ov) != pl.overview
    )


def resolve_companies_schema_and_columns(cur) -> Tuple[str, str, bool]:
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
        raise SystemExit("companies テーブルが見つかりません。")
    schema = str(row[0])
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = 'companies'
        """,
        (schema,),
    )
    cols = {r[0] for r in cur.fetchall()}
    phone_col = ""
    for c in _PHONE_COL_PRIORITY:
        if c in cols:
            phone_col = c
            break
    if not phone_col:
        raise SystemExit(f"{schema}.companies に phone_number/contact_phone_number がありません。")
    has_revenue_growth = "revenue_growth" in cols
    if not has_revenue_growth:
        LOG.warning("revenue_growth 列が無いため、revenue_growth 条件はスキップします。")
    return schema, phone_col, has_revenue_growth


def keyset_select_query(schema: str, phone_col: str, has_revenue_growth: bool) -> sql.Composed:
    c_phone = sql.SQL("c.{}").format(sql.Identifier(phone_col))
    tbl = sql.Identifier(schema, "companies")
    conditions: List[sql.Composable] = [
        sql.SQL("({c_phone} !~ '^[0-9+() -]*$' AND {c_phone} IS NOT NULL)").format(c_phone=c_phone),
        sql.SQL("(c.address ~ '^\\d{{13}}$')"),
        sql.SQL("(c.address ~* 'adsbygoogle')"),
    ]
    if has_revenue_growth:
        conditions.append(sql.SQL("(c.revenue_growth::text ~ '^\\d{{13}}$')"))
    where_or = sql.SQL(" OR ").join(conditions)
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
WHERE ({where_or})
  AND c.id > %s
ORDER BY c.id
LIMIT %s
"""
    ).format(c_phone=c_phone, tbl=tbl, where_or=where_or)


def ensure_staging(cur) -> None:
    cur.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS _comprehensive_stage (
            id TEXT NOT NULL PRIMARY KEY,
            phone_number TEXT,
            prefecture TEXT,
            address TEXT,
            representative_name TEXT,
            overview TEXT
        ) ON COMMIT DROP
        """
    )


def flush_updates(cur, batch: List[Tuple], chunk_insert: int, schema: str, phone_col: str) -> int:
    if not batch:
        return 0
    ensure_staging(cur)
    cur.execute("TRUNCATE _comprehensive_stage")
    for i in range(0, len(batch), chunk_insert):
        chunk = batch[i : i + chunk_insert]
        execute_values(
            cur,
            """
            INSERT INTO _comprehensive_stage (
                id, phone_number, prefecture, address, representative_name, overview
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
    tbl = sql.Identifier(schema, "companies")
    target_phone = sql.Identifier(phone_col)
    cur.execute(
        sql.SQL(
            """
            UPDATE {tbl} c
               SET {target_phone} = s.phone_number,
                   prefecture = s.prefecture,
                   address = s.address,
                   representative_name = s.representative_name,
                   overview = s.overview,
                   updated_at = CURRENT_TIMESTAMP
              FROM _comprehensive_stage s
             WHERE c.id = s.id
            """
        ).format(tbl=tbl, target_phone=target_phone)
    )
    n = cur.rowcount
    cur.execute("TRUNCATE _comprehensive_stage")
    return n


def main() -> None:
    ap = argparse.ArgumentParser(description="3段照合による companies 修復")
    ap.add_argument("--csv-root", type=Path, default=DEFAULT_CSV_ROOT)
    ap.add_argument("--gbiz-csv", type=Path, default=DEFAULT_GBIZ_PATH)
    ap.add_argument("--fetch-chunk", type=int, default=5000)
    ap.add_argument("--apply-batch", type=int, default=2000)
    ap.add_argument("--log-every", type=int, default=5000)
    ap.add_argument("--apply", action="store_true", help="DB更新を実行（未指定は dry-run）")
    args = ap.parse_args()

    if not args.csv_root.is_dir():
        raise SystemExit(f"CSV root が見つかりません: {args.csv_root}")
    if not args.gbiz_csv.is_file():
        raise SystemExit(f"GBiz CSV が見つかりません: {args.gbiz_csv}")

    dict_corp, dict_name_phone = load_main_indexes(args.csv_root)
    dict_gbiz = load_gbiz_index(args.gbiz_csv)

    conn = rep.get_connection()
    scanned = 0
    matched = 0
    would_change = 0
    updated_total = 0
    pending: List[Tuple] = []
    next_log_at = args.log_every if args.log_every else 0
    matched_methods: Counter[str] = Counter()

    try:
        cur = conn.cursor()
        schema, phone_col, has_revenue_growth = resolve_companies_schema_and_columns(cur)
        q = keyset_select_query(schema, phone_col, has_revenue_growth)
        LOG.info(
            "scan target: schema=%s phone_col=%s revenue_growth_condition=%s",
            schema,
            phone_col,
            has_revenue_growth,
        )
        last_id = ""
        while True:
            cur.execute(q, (last_id, args.fetch_chunk))
            chunk = cur.fetchall()
            if not chunk:
                break
            last_id = str(chunk[-1][0])

            for row in chunk:
                scanned += 1
                payload, method = lookup_payload(
                    row[2],
                    row[1],
                    row[3],
                    dict_corp,
                    dict_name_phone,
                    dict_gbiz,
                )
                if not payload:
                    continue
                matched += 1
                matched_methods[method] += 1
                if row_differs(row, payload):
                    would_change += 1
                if not args.apply:
                    continue
                if not row_differs(row, payload):
                    continue
                pending.append((str(row[0]),) + payload.as_tuple())
                if len(pending) >= args.apply_batch:
                    updated_total += flush_updates(cur, pending, args.apply_batch, schema, phone_col)
                    pending.clear()

            while args.log_every and next_log_at and scanned >= next_log_at:
                LOG.info(
                    "progress scanned=%d matched=%d would_change=%d updated=%d last_id=%s methods=%s",
                    scanned,
                    matched,
                    would_change,
                    updated_total,
                    last_id[:64] + ("..." if len(last_id) > 64 else ""),
                    dict(matched_methods),
                )
                next_log_at += args.log_every

        if args.apply and pending:
            updated_total += flush_updates(cur, pending, args.apply_batch, schema, phone_col)
            pending.clear()

        if args.apply:
            conn.commit()
            LOG.info(
                "APPLIED scanned=%d matched=%d would_change=%d updated=%d methods=%s",
                scanned,
                matched,
                would_change,
                updated_total,
                dict(matched_methods),
            )
        else:
            conn.rollback()
            LOG.info(
                "DRY-RUN scanned=%d matched=%d would_change=%d methods=%s",
                scanned,
                matched,
                would_change,
                dict(matched_methods),
            )
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()

