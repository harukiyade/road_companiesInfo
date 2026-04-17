#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DB 起点で companies の列ズレを修復する。

1) DB から「ズレ疑い」レコードを id キーセットでバッチ取得（主に PK / id 順で進む）。
2) fixed_csv_3 の 1.csv / 2.csv（なければ 1/・2/ 配下）をメモリに載せ、法人番号キーと
   (正規化社名, 正規化代表) キーで逆引き。
3) 優先1: DB の corporate_number / phone_number / address から 13 桁候補を抽出し CSV を引く。
   優先2: 正規化 name + representative_name の組で CSV を引く。
4) ヒットした行のみ CSV 正値で UPDATE（バッチ staging）。
   業種4列（industry_large〜detail）は DB 現状を維持し更新しない。

idx_companies_norm_name と同一の式で DB 側の正規化比較に使う（インデックス利用可能な形）。
デフォルト dry-run。--apply でコミット。
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
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError as e:
    raise SystemExit("psycopg2-binary が必要です") from e

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

try:
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import repair_companies_column_shift_from_fixed_csv3 as rep

LOG = logging.getLogger("repair_np_csv3")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_CSV_ROOT = BASE_DIR / "fixed_csv_3"

# 都道府県名が phone_number に入っている典型ズレ
PREFECTURES_PHONE: Tuple[str, ...] = (
    "北海道",
    "青森県",
    "岩手県",
    "宮城県",
    "秋田県",
    "山形県",
    "福島県",
    "茨城県",
    "栃木県",
    "群馬県",
    "埼玉県",
    "千葉県",
    "東京都",
    "神奈川県",
    "新潟県",
    "富山県",
    "石川県",
    "福井県",
    "山梨県",
    "長野県",
    "岐阜県",
    "静岡県",
    "愛知県",
    "三重県",
    "滋賀県",
    "京都府",
    "大阪府",
    "兵庫県",
    "奈良県",
    "和歌山県",
    "鳥取県",
    "島根県",
    "岡山県",
    "広島県",
    "山口県",
    "徳島県",
    "香川県",
    "愛媛県",
    "高知県",
    "福岡県",
    "佐賀県",
    "長崎県",
    "熊本県",
    "大分県",
    "宮崎県",
    "鹿児島県",
    "沖縄県",
)

# representative_name に混入しがちな属性フラグ（部分一致）
REP_ATTR_SUBSTRINGS: Tuple[str, ...] = (
    "ストロングバイヤー",
    "バイヤー以外",
    "以外",
    "フラグ",
    "属性",
)

CORP_HEADERS = ["法人番号", "corporate_number", "会社法人等番号", "企業法人番号"]
NAME_HEADERS = ["会社名", "企業名", "name", "法人名", "商号"]
PHONE_HEADERS = ["電話番号", "電話番号(窓口)", "TEL", "tel", "phone_number", "会社電話"]
ADDRESS_HEADERS = ["会社住所", "住所", "address", "所在地"]
PREF_HEADERS = ["都道府県", "prefecture", "会社都道府県"]
REP_HEADERS = ["代表者名", "代表者", "代表取締役"]
OVERVIEW_HEADERS = ["概要", "概況", "overview", "説明"]


def sql_name_norm_expr(alias: str) -> str:
    """idx_companies_norm_name と同一式（修飾子のみ alias）。"""
    return f"regexp_replace(lower(COALESCE({alias}.name, '')), '\\s+', '', 'g')"


def sql_rep_norm_expr(alias: str) -> str:
    return f"regexp_replace(lower(COALESCE({alias}.representative_name, '')), '\\s+', '', 'g')"


def pick_header(headers: Sequence[str], candidates: Sequence[str]) -> Optional[str]:
    hs = [str(h) for h in headers]
    for c in candidates:
        if c in hs:
            return c
    return None


def norm_text(v: Optional[str]) -> str:
    if v is None:
        return ""
    return unicodedata.normalize("NFKC", str(v)).strip()


def norm_name_key(v: Optional[str]) -> str:
    s = norm_text(v).lower()
    return re.sub(r"\s+", "", s)


def norm_corp_digits(v: Optional[str]) -> str:
    s = re.sub(r"\D", "", norm_text(v))
    if len(s) == 12:
        s = "0" + s
    return s if len(s) == 13 else ""


def cell(row: Dict[str, str], key: Optional[str]) -> str:
    if not key:
        return ""
    v = row.get(key)
    return "" if v is None else str(v)


def corp_keys_from_text(text: Optional[str]) -> List[str]:
    """テキスト中の連続数字から 13 桁法人番号候補を列挙（重複除去・順序維持）。"""
    d = re.sub(r"\D", "", norm_text(text))
    if not d:
        return []
    seen: set[str] = set()
    out: List[str] = []
    for i in range(0, max(0, len(d) - 12)):
        chunk = d[i : i + 13]
        if len(chunk) != 13:
            break
        k = norm_corp_digits(chunk)
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def cell_or_none(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s = str(s).strip()
    return None if s == "" else s


@dataclass
class RepairPayload:
    name: Optional[str]
    corporate_number: Optional[str]
    phone_number: Optional[str]
    prefecture: Optional[str]
    address: Optional[str]
    representative_name: Optional[str]
    overview: Optional[str]

    def as_tuple(self) -> Tuple:
        return (
            self.name,
            self.corporate_number,
            self.phone_number,
            self.prefecture,
            self.address,
            self.representative_name,
            self.overview,
        )


def row_to_payload(row: Dict[str, str], headers: Dict[str, Optional[str]]) -> RepairPayload:
    def nz(s: str) -> Optional[str]:
        s = (s or "").strip()
        return None if s == "" else s

    return RepairPayload(
        name=nz(cell(row, headers["name"])),
        corporate_number=nz(cell(row, headers["corp"])),
        phone_number=nz(cell(row, headers["phone"])),
        prefecture=nz(cell(row, headers["pref"])),
        address=nz(cell(row, headers["addr"])),
        representative_name=nz(cell(row, headers["rep"])),
        overview=nz(cell(row, headers["overview"])),
    )


def resolve_headers(fieldnames: Sequence[str]) -> Optional[Dict[str, Optional[str]]]:
    if not fieldnames:
        return None
    name_k = pick_header(fieldnames, NAME_HEADERS)
    corp_k = pick_header(fieldnames, CORP_HEADERS)
    if not name_k and not corp_k:
        return None
    return {
        "name": name_k,
        "corp": corp_k,
        "phone": pick_header(fieldnames, PHONE_HEADERS),
        "addr": pick_header(fieldnames, ADDRESS_HEADERS),
        "pref": pick_header(fieldnames, PREF_HEADERS),
        "rep": pick_header(fieldnames, REP_HEADERS),
        "overview": pick_header(fieldnames, OVERVIEW_HEADERS),
    }


def get_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            with path.open("r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "utf-8"


def iter_fallback_csv_paths(csv_root: Path) -> List[Path]:
    paths: List[Path] = []
    for sub in ("1", "2"):
        d = csv_root / sub
        if d.is_dir():
            paths.extend(sorted(d.rglob("*.csv"), key=lambda p: p.stat().st_mtime))
    return sorted(set(paths), key=lambda p: p.stat().st_mtime)


def csv_load_paths(csv_root: Path) -> List[Path]:
    """優先: fixed_csv_3/1.csv, 2.csv。無ければ 1/・2/ 配下の全 CSV。"""
    preferred = [csv_root / "1.csv", csv_root / "2.csv"]
    paths = [p for p in preferred if p.is_file()]
    if paths:
        return paths
    fb = iter_fallback_csv_paths(csv_root)
    if not fb:
        raise SystemExit(f"CSV が見つかりません: {csv_root} に 1.csv/2.csv または 1/・2/ 配下の .csv を配置してください")
    LOG.info("1.csv/2.csv が無いためフォールバック: %d ファイル", len(fb))
    return fb


def load_csv_indexes(csv_paths: Sequence[Path]) -> Tuple[Dict[str, RepairPayload], Dict[str, RepairPayload], Dict[Tuple[str, str], RepairPayload]]:
    """
    後勝ちで上書き（リスト後ろほど新しい想定）。
    - by_corp: 13 桁法人番号 -> payload
    - by_norm_name: 正規化社名 -> payload（単独キー・衝突時は後勝ち）
    - by_name_rep: (正規化社名, 正規化代表) -> payload（代表空は "" キー）
    """
    by_corp: Dict[str, RepairPayload] = {}
    by_norm_name: Dict[str, RepairPayload] = {}
    by_name_rep: Dict[Tuple[str, str], RepairPayload] = {}
    n_files = 0
    for fp in csv_paths:
        n_files += 1
        enc = get_encoding(fp)
        with fp.open("r", encoding=enc, newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                continue
            hdr = resolve_headers(reader.fieldnames)
            if not hdr:
                LOG.warning("skip (no usable headers): %s", fp)
                continue
            for raw in reader:
                row = {str(k): ("" if v is None else str(v)) for k, v in raw.items()}
                pl = row_to_payload(row, hdr)
                ck = norm_corp_digits(cell(row, hdr["corp"]) if hdr["corp"] else "")
                if ck:
                    by_corp[ck] = pl
                nk = norm_name_key(cell(row, hdr["name"]) if hdr["name"] else "")
                if nk:
                    by_norm_name[nk] = pl
                rk = norm_name_key(cell(row, hdr["rep"]) if hdr["rep"] else "")
                if nk:
                    by_name_rep[(nk, rk)] = pl
    LOG.info(
        "CSV indexes: corp=%d norm_name=%d name_rep=%d (files=%d)",
        len(by_corp),
        len(by_norm_name),
        len(by_name_rep),
        n_files,
    )
    return by_corp, by_norm_name, by_name_rep


def suspect_where_sql() -> str:
    """ズレ疑い: np~ ID、電話が都道府県名、電話が 13 桁純数字、代表に属性語。"""
    pref_sql = ", ".join(["%s"] * len(PREFECTURES_PHONE))
    attr_conds = " OR ".join(["COALESCE(representative_name,'') LIKE %s" for _ in REP_ATTR_SUBSTRINGS])
    return f"""
    (
        id LIKE 'np%%'
        OR BTRIM(COALESCE(phone_number, '')) IN ({pref_sql})
        OR (regexp_replace(COALESCE(phone_number, ''), '[^0-9]', '', 'g') ~ '^[0-9]{{13}}$')
        OR ({attr_conds})
    )
    """


def suspect_where_params() -> List[str]:
    params: List[str] = list(PREFECTURES_PHONE)
    for s in REP_ATTR_SUBSTRINGS:
        params.append(f"%{s}%")
    return params


def fetch_suspect_batch(cur, last_id: Optional[str], limit: int) -> List[Tuple]:
    """キーセット (id > last_id) で suspect 行を取得。idx 利用は主に id LIKE 'np%%' 部分に寄与。"""
    base_params = suspect_where_params()
    if last_id is None:
        q = f"""
            SELECT
                id,
                name,
                corporate_number,
                phone_number,
                prefecture,
                address,
                representative_name,
                overview
            FROM companies
            WHERE {suspect_where_sql()}
            ORDER BY id ASC
            LIMIT %s
            """
        cur.execute(q, (*base_params, limit))
    else:
        q = f"""
            SELECT
                id,
                name,
                corporate_number,
                phone_number,
                prefecture,
                address,
                representative_name,
                overview
            FROM companies
            WHERE {suspect_where_sql()}
              AND id > %s
            ORDER BY id ASC
            LIMIT %s
            """
        cur.execute(q, (*base_params, last_id, limit))
    return cur.fetchall()


def lookup_csv_payload(
    row: Tuple,
    by_corp: Dict[str, RepairPayload],
    by_name_rep: Dict[Tuple[str, str], RepairPayload],
) -> Optional[RepairPayload]:
    """
    row: SELECT 順と同じ。
    優先1: corp / phone / address から 13 桁候補で by_corp。
    優先2: DB 上の name + rep を idx と同式の正規化でキー化し by_name_rep、次に (nn,'')。
    """
    name = cell_or_none(row[1])
    corp = cell_or_none(row[2])
    phone = cell_or_none(row[3])
    addr = cell_or_none(row[5])
    rep = cell_or_none(row[6])

    for field in (corp, phone, addr):
        if not field:
            continue
        for k in corp_keys_from_text(field):
            pl = by_corp.get(k)
            if pl:
                return pl

    nn = norm_name_key(name)
    nr = norm_name_key(rep)
    if nn:
        pl = by_name_rep.get((nn, nr))
        if pl:
            return pl
        pl = by_name_rep.get((nn, ""))
        if pl:
            return pl
    return None


def ensure_repair_staging(cur) -> None:
    cur.execute(
        """
        CREATE TEMP TABLE IF NOT EXISTS _repair_final (
            id TEXT NOT NULL PRIMARY KEY,
            name TEXT,
            corporate_number TEXT,
            phone_number TEXT,
            prefecture TEXT,
            address TEXT,
            representative_name TEXT,
            overview TEXT
        ) ON COMMIT DROP
        """
    )


def flush_repairs(cur, rows: List[Tuple], values_chunk: int) -> int:
    if not rows:
        return 0
    ensure_repair_staging(cur)
    for i in range(0, len(rows), values_chunk):
        chunk = rows[i : i + values_chunk]
        execute_values(
            cur,
            """
            INSERT INTO _repair_final (
                id, name, corporate_number, phone_number, prefecture, address,
                representative_name, overview
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                corporate_number = EXCLUDED.corporate_number,
                phone_number = EXCLUDED.phone_number,
                prefecture = EXCLUDED.prefecture,
                address = EXCLUDED.address,
                representative_name = EXCLUDED.representative_name,
                overview = EXCLUDED.overview
            """,
            chunk,
        )
    cur.execute(
        """
        UPDATE companies c SET
            name = u.name,
            corporate_number = u.corporate_number,
            phone_number = u.phone_number,
            prefecture = u.prefecture,
            address = u.address,
            representative_name = u.representative_name,
            overview = u.overview,
            updated_at = CURRENT_TIMESTAMP
        FROM _repair_final u
        WHERE c.id = u.id
          AND NOT EXISTS (
              SELECT 1
              FROM companies existing
              WHERE existing.corporate_number = u.corporate_number
                AND existing.id <> u.id
          )
          AND (
              COALESCE(BTRIM(u.corporate_number), '') = ''
              OR u.id = (
                  SELECT MIN(u2.id)
                  FROM _repair_final u2
                  WHERE u2.corporate_number = u.corporate_number
              )
          )
        """
    )
    n = cur.rowcount
    cur.execute("TRUNCATE _repair_final")
    return n


def run_db_driven_repair(
    cur,
    by_corp: Dict[str, RepairPayload],
    by_name_rep: Dict[Tuple[str, str], RepairPayload],
    db_batch: int,
    apply_chunk: int,
    max_rows: Optional[int],
    do_apply: bool,
) -> Tuple[int, int, int]:
    """
    Returns (suspects_scanned, csv_matched, db_rows_updated)
    """
    last_id: Optional[str] = None
    scanned = 0
    matched = 0
    updated_total = 0
    pending: List[Tuple] = []

    while True:
        batch = fetch_suspect_batch(cur, last_id, db_batch)
        if not batch:
            break
        last_id = str(batch[-1][0])
        scanned += len(batch)
        for row in batch:
            pl = lookup_csv_payload(row, by_corp, by_name_rep)
            if not pl:
                continue
            matched += 1
            if not do_apply:
                continue
            cid = str(row[0])
            pending.append((cid,) + pl.as_tuple())
            if len(pending) >= apply_chunk:
                updated_total += flush_repairs(cur, pending, apply_chunk)
                pending.clear()
        if max_rows is not None and scanned >= max_rows:
            LOG.info("--max-rows に達したため DB 走査を打ち切ります")
            break

    if pending:
        if do_apply:
            updated_total += flush_repairs(cur, pending, apply_chunk)
        pending.clear()

    return scanned, matched, updated_total


def explain_index_usage_note() -> None:
    LOG.info(
        "ヒント: suspect 条件のうち id LIKE 'np%%' は id B-tree で絞り込みやすいです。"
        "その他条件は部分インデックス（例: (regexp_replace(phone...))）を検討してください。"
        "社名側は CSV 逆引きで %s と同一式を Python で使用しています。",
        sql_name_norm_expr("c"),
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="DB起点: 列ズレ疑い行を CSV で逆引き修復")
    ap.add_argument("--csv-root", type=Path, default=DEFAULT_CSV_ROOT, help="fixed_csv_3 ルート")
    ap.add_argument("--db-batch", type=int, default=1000, help="DB から suspect を読む行数")
    ap.add_argument("--apply-chunk", type=int, default=5000, help="UPDATE 単位の staging 行数")
    ap.add_argument("--max-rows", type=int, default=None, help="テスト用: 走査する suspect 上限（未指定で無制限）")
    ap.add_argument("--apply", action="store_true", help="DB を更新する（未指定は dry-run）")
    args = ap.parse_args()

    if not args.csv_root.is_dir():
        raise SystemExit(f"CSV root not found: {args.csv_root}")

    paths = csv_load_paths(args.csv_root)
    by_corp, by_norm_name, by_name_rep = load_csv_indexes(paths)
    LOG.info(
        "正規化社名单独キー索引: %d 件（マッチは法人番号および社名+代表ペアのみ。単独キーは拡張用）",
        len(by_norm_name),
    )
    explain_index_usage_note()

    conn = rep.get_connection()
    try:
        cur = conn.cursor()
        if args.apply:
            ensure_repair_staging(cur)
        scanned, matched, updated = run_db_driven_repair(
            cur,
            by_corp,
            by_name_rep,
            args.db_batch,
            args.apply_chunk,
            args.max_rows,
            do_apply=args.apply,
        )
        LOG.info(
            "suspects_scanned=%d csv_matched=%d updated_rows=%d apply=%s",
            scanned,
            matched,
            updated,
            args.apply,
        )
        if args.apply:
            conn.commit()
            LOG.info("COMMIT 完了")
        else:
            conn.rollback()
            LOG.info("DRY-RUN: 変更なし（--apply で反映）")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
