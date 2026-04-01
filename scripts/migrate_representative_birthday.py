#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3/unit_million & unit_yen 配下の CSV から代表者誕生日を companies にインポートする。

安全装置（最重要）:
- DB 側の対象カラムに「NULL ではない値（または空文字）」が既に入っている場合は絶対に上書きしない。
- DB 側が空（NULL/空文字）で、CSV 側に有効な誕生日がある場合のみ更新。

名寄せロジック（前回踏襲）:
1. 法人番号（CSV: 法人番号 または corporate_number）で corporate_number 一致
2. 法人番号が取れない/空の場合は複合キー（会社名＋代表者名＋電話番号）
   - 会社名/代表者名: 前後空白や全半角スペース除去して比較
   - 電話番号: - や ー 等のハイフン系と空白を除去し数字のみ比較
   - 3項目のいずれかが欠損ならその行はスキップ

実行:
  python3 scripts/migrate_representative_birthday.py          # ドライラン
  python3 scripts/migrate_representative_birthday.py --execute # 本番更新
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import logging
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor, execute_batch

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BATCH_UPDATE_SIZE = 1000

TARGET_DIRS = (
    PROJECT_ROOT / "fixed_csv_3" / "unit_million",
    PROJECT_ROOT / "fixed_csv_3" / "unit_yen",
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


def connect() -> Any:
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


def normalize_matching_name(val: Optional[str]) -> str:
    if not val:
        return ""
    s = str(val).strip()
    return re.sub(r"[\s\u3000]+", "", s)


def normalize_matching_phone_digits(val: Optional[str]) -> str:
    if not val:
        return ""
    s = str(val).strip()
    for ch in ("-", "ー", "－", "—", "–", "−"):
        s = s.replace(ch, "")
    s = re.sub(r"[\s\u3000]+", "", s)
    return re.sub(r"[^\d]", "", s)


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


def parse_birth_date_to_date(val: Optional[str]) -> Optional[dt.date]:
    """
    CSV の日付文字列を DATE(YYYY-MM-DD) に変換。
    例: 1980/1/1, 1980年1月1日, 1980-01-01, 1980-1-1
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if s.startswith("0000"):
        return None

    # 1980年1月1日
    m = re.match(r"^(?P<y>\d{4})年(?P<m>\d{1,2})月(?P<d>\d{1,2})日$", s)
    if m:
        y = int(m.group("y"))
        mo = int(m.group("m"))
        d = int(m.group("d"))
    else:
        # YYYY/MM/DD or YYYY-MM-DD or YYYY/M/D
        m2 = re.match(r"^(?P<y>\d{4})[\/\-\.](?P<m>\d{1,2})[\/\-\.](?P<d>\d{1,2})", s)
        if not m2:
            # 末尾に時刻が付いているケース等（先頭で YYYY-MM-DD を抽出）
            m3 = re.match(r"^(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})", s)
            if not m3:
                return None
            y = int(m3.group("y"))
            mo = int(m3.group("m"))
            d = int(m3.group("d"))
        else:
            y = int(m2.group("y"))
            mo = int(m2.group("m"))
            d = int(m2.group("d"))

    try:
        if y <= 0 or mo <= 0 or d <= 0:
            return None
        return dt.date(y, mo, d)
    except ValueError:
        return None


def composite_key_string(nn: str, nr: str, nd: str) -> str:
    return f"{nn}_{nr}_{nd}"


def _unique_ordered_ids(ids: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for i in ids:
        if i in seen:
            continue
        seen.add(i)
        out.append(i)
    return out


def detect_target_column(conn: Any) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'companies'
              AND column_name IN ('representative_birthday', 'representative_birth_date')
            """
        )
        cols = [r[0] for r in cur.fetchall()]
    if "representative_birthday" in cols:
        return "representative_birthday"
    if "representative_birth_date" in cols:
        return "representative_birth_date"
    raise RuntimeError("companies テーブルに representative_birthday または representative_birth_date が見つかりません。")


def load_companies_lookup(
    cur: Any,
    rep_col: str,
) -> tuple[
    dict[str, list[str]],
    dict[str, list[str]],
    dict[str, Any],
]:
    """
    companies を全件ロードしてインデックスを作る。
    - dict_by_corp_num: corporate_number -> [id...]
    - dict_by_composite: composite_key -> [id...]
    - id_to_existing_rep_birth: id -> 既存値（date か None）
    """
    logger.info("companies テーブルを全件ロードしています（インデックス構築）…")
    cur.execute(
        f"""
        SELECT id, corporate_number, name, representative_name,
               phone_number, contact_phone_number, {rep_col}
        FROM companies
        """
    )
    rows = cur.fetchall()

    corp_buckets: defaultdict[str, list[str]] = defaultdict(list)
    composite_buckets: defaultdict[str, list[str]] = defaultdict(list)
    id_to_existing_rep_birth: dict[str, Any] = {}

    for r in rows:
        rid = str(r["id"])
        id_to_existing_rep_birth[rid] = r[rep_col]

        cn = normalize_corporate_number(r.get("corporate_number"))
        if cn:
            corp_buckets[cn].append(rid)

        nn = normalize_matching_name(r.get("name"))
        nr = normalize_matching_name(r.get("representative_name"))
        if not nn or not nr:
            continue

        for phone in (r.get("phone_number"), r.get("contact_phone_number")):
            nd = normalize_matching_phone_digits(phone)
            if not nd:
                continue
            ck = composite_key_string(nn, nr, nd)
            composite_buckets[ck].append(rid)

    dict_by_corp_num = {cn: _unique_ordered_ids(ids) for cn, ids in corp_buckets.items()}
    dict_by_composite = {ck: _unique_ordered_ids(ids) for ck, ids in composite_buckets.items()}

    logger.info(
        "インデックス構築完了: 企業行=%s, 法人番号キー=%s, 複合キー=%s",
        len(rows),
        len(dict_by_corp_num),
        len(dict_by_composite),
    )
    return dict_by_corp_num, dict_by_composite, id_to_existing_rep_birth


def get_cell(row: list[str], idx: Optional[int]) -> Optional[str]:
    if idx is None or idx < 0 or idx >= len(row):
        return None
    v = row[idx]
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def build_csv_header_map(headers: list[str]) -> dict[str, int]:
    norm_to_idx: dict[str, int] = {}
    for i, h in enumerate(headers):
        norm_to_idx[normalize_header(h)] = i

    def pick(*aliases: str) -> Optional[int]:
        for a in aliases:
            na = normalize_header(a)
            if na in norm_to_idx:
                return norm_to_idx[na]
        return None

    out: dict[str, int] = {}
    corp = pick("法人番号", "corporate_number")
    if corp is not None:
        out["corporate_number"] = corp

    comp = pick("会社名", "企業名", "name")
    if comp is not None:
        out["match_company_name"] = comp

    rep = pick("代表者名", "代表者")
    if rep is not None:
        out["match_representative_name"] = rep

    phone = pick("電話番号", "電話番号(窓口)", "TEL", "tel")
    if phone is not None:
        out["match_phone"] = phone

    birth = pick("代表者誕生日", "代表者生年月日", "生年月日")
    if birth is not None:
        out["birth_date"] = birth

    return out


def resolve_match_ids(
    hmap: dict[str, int],
    row: list[str],
    dict_by_corp_num: dict[str, list[str]],
    dict_by_composite: dict[str, list[str]],
) -> tuple[list[str], str]:
    # 第一候補: 法人番号
    if "corporate_number" in hmap:
        corp = normalize_corporate_number(get_cell(row, hmap.get("corporate_number")))
        if corp and corp in dict_by_corp_num:
            return dict_by_corp_num[corp], "corporate"

    # 第二候補: 複合キー
    cname = get_cell(row, hmap.get("match_company_name"))
    rname = get_cell(row, hmap.get("match_representative_name"))
    pcell = get_cell(row, hmap.get("match_phone"))

    if not cname or not rname or not pcell:
        return [], "skip_incomplete_composite"

    nn = normalize_matching_name(cname)
    nr = normalize_matching_name(rname)
    nd = normalize_matching_phone_digits(pcell)
    if not nn or not nr or not nd:
        return [], "skip_incomplete_composite"

    ck = composite_key_string(nn, nr, nd)
    ids = dict_by_composite.get(ck, [])
    if not ids:
        return [], "no_match"
    return ids, "composite"


def collect_csv_files() -> list[Path]:
    out: list[Path] = []
    for d in TARGET_DIRS:
        if not d.is_dir():
            continue
        for fp in sorted(d.rglob("*.csv")):
            if fp.is_file():
                out.append(fp)
    return sorted(set(out))


def run(*, execute: bool) -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    files = collect_csv_files()
    if not files:
        logger.error("対象 CSV がありません: %s / %s", TARGET_DIRS[0], TARGET_DIRS[1])
        return

    stats = {
        "files": 0,
        "csv_rows": 0,
        "matched_rows": 0,  # CSV 行（key一致があったもの）
        "matched_ids": 0,  # DB レコード id 数
        "new_writes": 0,  # 実際に更新する id 数
        "skip_existing": 0,  # key一致だが DB に既存値ありの id 数
        "skip_incomplete_composite": 0,
        "no_match": 0,
        "invalid_birth_date_rows": 0,
    }

    conn = connect()
    try:
        conn.autocommit = False
        cur = conn.cursor()
        # マッチング辞書を作るには RealDictCursor の方が便利だが、
        # ここでは fetchall 後にキーアクセスが必要ないので通常カーソルでも動く。
        # ただし id_to_existing 取得のため rep_col を動的に使う必要があるので、
        # 便宜上 cursor_factory ではなく SELECT で順序固定。

        # 情報スキーマで更新対象カラムを確定
        # （psycopg2 の辞書カーソルを使わない構成に合わせて動的 SQL を避ける）
        rep_col = detect_target_column(conn)

        # companies 全件ロード
        cur2 = conn.cursor(cursor_factory=RealDictCursor)
        dict_by_corp_num, dict_by_composite, id_to_existing = load_companies_lookup(cur2, rep_col)
        cur2.close()

        update_sql = f"UPDATE companies SET {rep_col} = %s, updated_at = NOW() WHERE id = %s"
        update_batch: list[tuple[Any, str]] = []

        for fp in files:
            enc = "utf-8-sig"
            try:
                enc = "utf-8-sig"
                with fp.open("r", encoding=enc, newline="") as f:
                    reader = csv.reader(f)
                    try:
                        headers = next(reader)
                    except StopIteration:
                        continue

                    hmap = build_csv_header_map(headers)
                    if "birth_date" not in hmap:
                        # 列が無いファイルはスキップ
                        continue

                    stats["files"] += 1

                    for row in reader:
                        stats["csv_rows"] += 1

                        ids, reason = resolve_match_ids(
                            hmap, row, dict_by_corp_num, dict_by_composite
                        )
                        if reason == "no_match" or not ids:
                            stats["no_match"] += 1
                            continue
                        if reason == "skip_incomplete_composite":
                            stats["skip_incomplete_composite"] += 1
                            continue

                        stats["matched_rows"] += 1
                        stats["matched_ids"] += len(ids)

                        birth_raw = get_cell(row, hmap.get("birth_date"))
                        birth_date = parse_birth_date_to_date(birth_raw)
                        if birth_date is None:
                            stats["invalid_birth_date_rows"] += 1
                            continue

                        for row_id in ids:
                            existing = id_to_existing.get(row_id)
                            if existing is not None and not (
                                isinstance(existing, str) and existing.strip() == ""
                            ):
                                stats["skip_existing"] += 1
                                continue

                            stats["new_writes"] += 1
                            if execute:
                                update_batch.append((birth_date, row_id))
                                if len(update_batch) >= BATCH_UPDATE_SIZE:
                                    execute_batch(cur, update_sql, update_batch, page_size=len(update_batch))
                                    conn.commit()
                                    update_batch.clear()

            except Exception as e:
                logger.warning("CSV 読み込み失敗: %s (%s)", fp, e)

        if execute and update_batch:
            execute_batch(cur, update_sql, update_batch, page_size=len(update_batch))
            conn.commit()

        if execute:
            conn.commit()
        else:
            conn.rollback()

        cur.close()
    finally:
        conn.close()

    logger.info(
        "完了: マッチしたCSV行=%s, マッチID=%s, 新規書き込み=%s, 既存ありスキップ=%s, "  # noqa: E501
        "複合キー欠損スキップ=%s, DB未一致=%s, 誕生日不正行=%s",
        stats["matched_rows"],
        stats["matched_ids"],
        stats["new_writes"],
        stats["skip_existing"],
        stats["skip_incomplete_composite"],
        stats["no_match"],
        stats["invalid_birth_date_rows"],
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    p = argparse.ArgumentParser(description="fixed_csv_3 代表者誕生日 → companies へインポート")
    p.add_argument(
        "--execute",
        action="store_true",
        help="指定時のみ DB を更新（未指定はドライラン）",
    )
    args = p.parse_args()

    if not args.execute:
        logger.info("ドライラン（--execute なし）: DB は更新しません。")
    run(execute=args.execute)


if __name__ == "__main__":
    main()

