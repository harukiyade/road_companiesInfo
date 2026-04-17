#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3/1, fixed_csv_3/2 の全CSVを解析し、代表者名を更新しつつ np レコードを統合する。

処理概要:
1) CSVから representative_name 候補を抽出（代表者列を自動判定）
2) DB の companies を corporate_number または name+phone でマッチして representative_name を最新値で更新
3) id が np... のレコードを、同一 corporate_number または name+phone の正規IDへ統合
   - 正規側の空欄を np 側で補完
   - 統合後に np 側を削除
4) representative_name 末尾の助詞（は/が/の）をクリーンアップ

デフォルトは dry-run（DB変更なし）。--apply で反映。
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sys
import unicodedata
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import repair_companies_column_shift_from_fixed_csv3 as rep

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

try:
    from psycopg2.extras import execute_values
except Exception as e:
    raise SystemExit("psycopg2-binary が必要です") from e


LOG = logging.getLogger("sync_rep_np")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

REP_HEADER_PRIORITY = [
    "代表者名",
    "代表者",
    "代表取締役",
    "社長名",
    "社長",
]
REP_HEADER_CONTAINS = ["代表", "社長"]
REP_HEADER_EXCLUDE = ["住所", "郵便", "電話", "生年", "誕生", "年齢", "カナ", "フリガナ", "役職"]

CORP_HEADERS = ["法人番号", "corporate_number", "会社法人等番号", "企業法人番号"]
NAME_HEADERS = ["会社名", "企業名", "name", "法人名", "商号"]
PHONE_HEADERS = ["電話番号", "TEL", "tel", "phone_number", "会社電話"]

NP_ID_RE = r"^np[0-9a-f]{32}$"


# ---------------------------------------------------------------------------
# DB 上の idx_companies_norm_name（式インデックス）と字句一致させること。
# 正規化は必ず sql_name_norm 経由。CREATE INDEX の式と 1 文字でも違うと
# フルスキャンになるため、運用 DB で次を実行し indexdef と突き合わせること:
#   SELECT indexdef FROM pg_indexes
#   WHERE indexname = 'idx_companies_norm_name';
# 以下の式は上記インデックスの index 対象式と同一（テーブル修飾子のみ {alias}. を付与）。
#   regexp_replace(lower(COALESCE(name, '')), '\s+', '', 'g')
# ---------------------------------------------------------------------------
def sql_corp_digits(alias: str) -> str:
    return f"regexp_replace(COALESCE({alias}.corporate_number,''), '[^0-9]', '', 'g')"


def sql_name_norm(alias: str) -> str:
    return f"regexp_replace(lower(COALESCE({alias}.name, '')), '\\s+', '', 'g')"


def sql_phone_digits(alias: str) -> str:
    return f"regexp_replace(COALESCE({alias}.phone_number,''), '[^0-9]', '', 'g')"


def get_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            with path.open("r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "utf-8"


def norm_text(v: Optional[str]) -> str:
    if v is None:
        return ""
    s = unicodedata.normalize("NFKC", str(v)).strip()
    return s


def norm_name(v: Optional[str]) -> str:
    s = norm_text(v).lower()
    s = re.sub(r"\s+", "", s)
    return s


def norm_phone(v: Optional[str]) -> str:
    return re.sub(r"\D", "", norm_text(v))


def norm_corp(v: Optional[str]) -> str:
    s = re.sub(r"\D", "", norm_text(v))
    if len(s) == 12:
        s = "0" + s
    return s if len(s) == 13 else ""


def clean_rep(v: Optional[str]) -> str:
    s = norm_text(v)
    if not s:
        return ""
    # 末尾助詞の簡易除去
    s = re.sub(r"[はがの]+$", "", s).strip()
    return s


def pick_header(headers: Sequence[str], priority: Sequence[str], contains: Sequence[str] = ()) -> Optional[str]:
    hs = [str(h) for h in headers]
    for p in priority:
        for h in hs:
            if h == p:
                return h
    for p in priority:
        for h in hs:
            if p in h:
                return h
    for c in contains:
        for h in hs:
            if c in h:
                return h
    return None


def detect_rep_header(headers: Sequence[str]) -> Optional[str]:
    hs = [str(h) for h in headers]
    for p in REP_HEADER_PRIORITY:
        for h in hs:
            if h == p:
                return h
    for h in hs:
        if any(k in h for k in REP_HEADER_CONTAINS) and not any(x in h for x in REP_HEADER_EXCLUDE):
            return h
    return None


def iter_csv_paths(root: Path) -> List[Path]:
    paths: List[Path] = []
    for sub in ("1", "2"):
        base = root / sub
        if not base.exists():
            continue
        paths.extend(base.rglob("*.csv"))
    # mtime 昇順で処理し、後ろのファイルを「最新」とみなす
    return sorted(paths, key=lambda p: p.stat().st_mtime)


def read_csv_rows(path: Path) -> Iterable[Dict[str, str]]:
    enc = get_encoding(path)
    with path.open("r", encoding=enc, newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return
        for row in reader:
            yield {str(k): ("" if v is None else str(v)) for k, v in row.items()}


def build_staging_rows(csv_root: Path) -> List[Tuple[str, str, str, str]]:
    rows: List[Tuple[str, str, str, str]] = []
    files = iter_csv_paths(csv_root)
    LOG.info("CSV files: %d", len(files))
    for i, fp in enumerate(files, 1):
        try:
            sample_iter = read_csv_rows(fp)
            first = next(iter(sample_iter), None)
            if first is None:
                continue
            headers = list(first.keys())
            rep_h = detect_rep_header(headers)
            if not rep_h:
                continue
            corp_h = pick_header(headers, CORP_HEADERS)
            name_h = pick_header(headers, NAME_HEADERS)
            phone_h = pick_header(headers, PHONE_HEADERS)

            def push(rec: Dict[str, str]) -> None:
                rep_val = clean_rep(rec.get(rep_h))
                if not rep_val:
                    return
                corp = norm_corp(rec.get(corp_h) if corp_h else None)
                name = norm_name(rec.get(name_h) if name_h else None)
                phone = norm_phone(rec.get(phone_h) if phone_h else None)
                if not corp and not (name and phone):
                    return
                rows.append((corp, name, phone, rep_val))

            push(first)
            for rec in sample_iter:
                push(rec)
        except Exception as e:
            LOG.warning("skip csv due to parse error: %s (%s)", fp, e)
        if i % 20 == 0:
            LOG.info("parsed files: %d/%d staging rows=%d", i, len(files), len(rows))
    return rows


def _execute_safe_rollback(conn) -> None:
    try:
        if not getattr(conn, "closed", 1):
            conn.rollback()
    except Exception:
        pass


def main() -> None:
    ap = argparse.ArgumentParser(description="Sync representative_name and merge np records.")
    ap.add_argument("--apply", action="store_true", help="DBに反映する（未指定はdry-run）")
    ap.add_argument("--csv-root", default="fixed_csv_3", help="fixed_csv_3 のルート")
    ap.add_argument("--staging-batch", type=int, default=10000)
    args = ap.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    csv_root = project_root / args.csv_root
    if not csv_root.exists():
        raise SystemExit(f"CSV root not found: {csv_root}")

    staging_rows = build_staging_rows(csv_root)
    LOG.info("staging rows collected: %d", len(staging_rows))

    conn = rep.get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TEMP TABLE _rep_staging (
                seq BIGSERIAL PRIMARY KEY,
                corp_key TEXT,
                name_key TEXT,
                phone_key TEXT,
                representative_name TEXT
            ) ON COMMIT DROP
            """
        )
        if staging_rows:
            for i in range(0, len(staging_rows), args.staging_batch):
                chunk = staging_rows[i : i + args.staging_batch]
                execute_values(
                    cur,
                    "INSERT INTO _rep_staging (corp_key, name_key, phone_key, representative_name) VALUES %s",
                    chunk,
                )
        # _rep_staging は ON COMMIT DROP のため、ここで commit すると消える。
        # 最後にまとめて commit / rollback する。

        # representative_name 更新（最新 seq を採用）
        cur = conn.cursor()
        cur.execute(
            f"""
            WITH latest AS (
                SELECT DISTINCT ON (corp_key) corp_key, representative_name
                FROM _rep_staging
                WHERE corp_key IS NOT NULL AND corp_key <> ''
                ORDER BY corp_key, seq DESC
            )
            UPDATE companies c
            SET representative_name = l.representative_name
            FROM latest l
            WHERE {sql_corp_digits("c")} = l.corp_key
              AND COALESCE(c.representative_name, '') <> l.representative_name
            """
        )
        rep_by_corp = cur.rowcount

        cur.execute(
            f"""
            WITH latest AS (
                SELECT DISTINCT ON (name_key, phone_key) name_key, phone_key, representative_name
                FROM _rep_staging
                WHERE (corp_key IS NULL OR corp_key = '')
                  AND name_key IS NOT NULL AND name_key <> ''
                  AND phone_key IS NOT NULL AND phone_key <> ''
                ORDER BY name_key, phone_key, seq DESC
            )
            UPDATE companies c
            SET representative_name = l.representative_name
            FROM latest l
            WHERE {sql_name_norm("c")} = l.name_key
              AND {sql_phone_digits("c")} = l.phone_key
              AND COALESCE(c.representative_name, '') <> l.representative_name
            """
        )
        rep_by_name_phone = cur.rowcount
        LOG.info("representative updates: by_corp=%d by_name_phone=%d", rep_by_corp, rep_by_name_phone)

        # np -> canonical mapping（LATERAL 禁止: 法人番号 / 社名+電話 を分割 INSERT）
        cur.execute(
            """
            CREATE TEMP TABLE _np_merge_map (
                np_id TEXT NOT NULL PRIMARY KEY,
                canon_id TEXT NOT NULL
            ) ON COMMIT DROP
            """
        )
        # 1) 法人番号一致（正規化式は _rep_staging / 代表者 UPDATE と同一＝関数インデックス想定）
        cur.execute(
            f"""
            INSERT INTO _np_merge_map (np_id, canon_id)
            SELECT DISTINCT ON (t.np_id) t.np_id, t.canon_id
            FROM (
                SELECT np.id AS np_id, c.id AS canon_id
                FROM companies np
                INNER JOIN companies c
                    ON NOT (c.id ~ '{NP_ID_RE}')
                   AND {sql_corp_digits("np")} <> ''
                   AND {sql_corp_digits("c")} <> ''
                   AND {sql_corp_digits("c")} = {sql_corp_digits("np")}
                WHERE np.id ~ '{NP_ID_RE}'
            ) t
            ORDER BY t.np_id, t.canon_id
            """
        )
        inserted_corp = cur.rowcount
        # 2) 社名+電話（正規化式はインデックス定義と同一。未マージの np のみ）
        cur.execute(
            f"""
            INSERT INTO _np_merge_map (np_id, canon_id)
            SELECT DISTINCT ON (t.np_id) t.np_id, t.canon_id
            FROM (
                SELECT np.id AS np_id, c.id AS canon_id
                FROM companies np
                INNER JOIN companies c
                    ON NOT (c.id ~ '{NP_ID_RE}')
                   AND (np.corporate_number IS NULL OR BTRIM(np.corporate_number) = '')
                   AND {sql_name_norm("c")} = {sql_name_norm("np")}
                   AND {sql_phone_digits("c")} = {sql_phone_digits("np")}
                   AND {sql_name_norm("np")} <> ''
                   AND {sql_phone_digits("np")} <> ''
                WHERE np.id ~ '{NP_ID_RE}'
                  AND NOT EXISTS (SELECT 1 FROM _np_merge_map m WHERE m.np_id = np.id)
            ) t
            ORDER BY t.np_id, t.canon_id
            """
        )
        inserted_name_phone = cur.rowcount
        LOG.info(
            "np merge map: corp_inserts=%d name_phone_inserts=%d",
            inserted_corp,
            inserted_name_phone,
        )
        cur.execute("SELECT COUNT(*) FROM _np_merge_map")
        merge_targets = int(cur.fetchone()[0] or 0)
        LOG.info("np merge targets: %d", merge_targets)

        # canonical を空欄補完
        cur.execute(
            """
            UPDATE companies c
            SET
              representative_name = CASE WHEN COALESCE(BTRIM(c.representative_name),'') = '' THEN np.representative_name ELSE c.representative_name END,
              email               = CASE WHEN COALESCE(BTRIM(c.email),'') = '' THEN np.email ELSE c.email END,
              postal_code         = CASE WHEN COALESCE(BTRIM(c.postal_code),'') = '' THEN np.postal_code ELSE c.postal_code END,
              address             = CASE WHEN COALESCE(BTRIM(c.address),'') = '' THEN np.address ELSE c.address END,
              phone_number        = CASE WHEN COALESCE(BTRIM(c.phone_number),'') = '' THEN np.phone_number ELSE c.phone_number END,
              overview            = CASE WHEN COALESCE(BTRIM(c.overview),'') = '' THEN np.overview ELSE c.overview END,
              business_descriptions = CASE WHEN COALESCE(BTRIM(c.business_descriptions),'') = '' THEN np.business_descriptions ELSE c.business_descriptions END,
              industry_large      = CASE WHEN COALESCE(BTRIM(c.industry_large),'') = '' THEN np.industry_large ELSE c.industry_large END,
              industry_middle     = CASE WHEN COALESCE(BTRIM(c.industry_middle),'') = '' THEN np.industry_middle ELSE c.industry_middle END,
              industry_small      = CASE WHEN COALESCE(BTRIM(c.industry_small),'') = '' THEN np.industry_small ELSE c.industry_small END,
              industry_detail     = CASE WHEN COALESCE(BTRIM(c.industry_detail),'') = '' THEN np.industry_detail ELSE c.industry_detail END
            FROM _np_merge_map m
            JOIN companies np ON np.id = m.np_id
            WHERE c.id = m.canon_id
            """
        )
        merged_fill_updates = cur.rowcount

        # np 削除
        cur.execute(
            """
            DELETE FROM companies d
            USING _np_merge_map m
            WHERE d.id = m.np_id
            """
        )
        deleted_np = cur.rowcount

        # 末尾助詞クリーンアップ
        cur.execute(
            """
            UPDATE companies
            SET representative_name = NULLIF(BTRIM(REGEXP_REPLACE(representative_name, '[はがの]+$', '', 'g')), '')
            WHERE representative_name IS NOT NULL
              AND representative_name ~ '[はがの]+$'
            """
        )
        cleaned_particles = cur.rowcount

        if args.apply:
            conn.commit()
            LOG.info(
                "APPLIED: rep_updates(corp=%d,name_phone=%d), np_fill=%d, np_deleted=%d, rep_particle_clean=%d",
                rep_by_corp,
                rep_by_name_phone,
                merged_fill_updates,
                deleted_np,
                cleaned_particles,
            )
        else:
            _execute_safe_rollback(conn)
            LOG.info(
                "DRY-RUN: rep_updates(corp=%d,name_phone=%d), np_fill=%d, np_deleted=%d, rep_particle_clean=%d",
                rep_by_corp,
                rep_by_name_phone,
                merged_fill_updates,
                deleted_np,
                cleaned_particles,
            )
    except Exception:
        _execute_safe_rollback(conn)
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

