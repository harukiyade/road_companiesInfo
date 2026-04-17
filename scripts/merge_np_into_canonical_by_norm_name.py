#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
正規化社名が一致する np 行（id LIKE 'np%'）から、非 np の正規行へ値を移植し、
移植後に np 行を削除する。

- デフォルト: 正規側が NULL または空文字のカラムのみ np 側で埋める
- --force-newer: np 側の updated_at が正規側より新しいときは、そのカラムは
  np の非空値を優先（空なら正規側を維持）

社名正規化は idx_companies_norm_name と同一式:
  regexp_replace(lower(COALESCE(name, '')), '\\s+', '', 'g')

デフォルト dry-run。--apply でコミット。
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

try:
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import repair_companies_column_shift_from_fixed_csv3 as rep

LOG = logging.getLogger("merge_np_canonical")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def sql_name_norm(alias: str) -> str:
    return f"regexp_replace(lower(COALESCE({alias}.name, '')), '\\s+', '', 'g')"


def build_merge_sql(force_newer: bool) -> str:
    force_expr = "TRUE" if force_newer else "FALSE"
    n_s = sql_name_norm("s")
    n_t = sql_name_norm("t")
    n_c = sql_name_norm("c")

    def col_expr(col: str) -> str:
        # BTRIM で空白のみも空扱い
        return f"""
            {col} = CASE
              WHEN ({force_expr})
                   AND (p.src_upd IS NOT NULL AND (p.tgt_upd IS NULL OR p.src_upd > p.tgt_upd))
                THEN COALESCE(NULLIF(BTRIM(s.{col}), ''), BTRIM(c.{col}))
              ELSE COALESCE(NULLIF(BTRIM(c.{col}), ''), NULLIF(BTRIM(s.{col}), ''))
            END""".strip()

    cols = [
        "corporate_number",
        "phone_number",
        "address",
        "representative_name",
        "overview",
        "prefecture",
        "email",
    ]
    set_clause = ",\n  ".join(col_expr(c) for c in cols)

    return f"""
WITH pair AS (
    SELECT DISTINCT ON (s.id)
        s.id AS sid,
        t.id AS tid,
        s.updated_at AS src_upd,
        t.updated_at AS tgt_upd
    FROM companies s
    INNER JOIN companies t
        ON t.id NOT LIKE 'np%%'
       AND s.id LIKE 'np%%'
       AND {n_s} = {n_t}
    ORDER BY s.id, t.id ASC
)
UPDATE companies c
SET
  {set_clause},
  updated_at = CURRENT_TIMESTAMP
FROM pair p
INNER JOIN companies s ON s.id = p.sid
WHERE c.id = p.tid
""".strip()


DELETE_NP_SQL = f"""
DELETE FROM companies c
WHERE c.id LIKE 'np%%'
  AND EXISTS (
      SELECT 1
      FROM companies t
      WHERE t.id NOT LIKE 'np%%'
        AND {sql_name_norm("t")} = {sql_name_norm("c")}
  )
""".strip()


def main() -> None:
    ap = argparse.ArgumentParser(description="Merge np companies into canonical by normalized name")
    ap.add_argument(
        "--force-newer",
        action="store_true",
        help="np.updated_at が正規側より新しいときは非空の np 値で上書き",
    )
    ap.add_argument("--apply", action="store_true", help="コミットする（未指定はロールバック）")
    args = ap.parse_args()

    merge_sql = build_merge_sql(args.force_newer)
    conn = rep.get_connection()
    try:
        cur = conn.cursor()
        LOG.info("Running MERGE (force_newer=%s)...", args.force_newer)
        cur.execute(merge_sql)
        merged = cur.rowcount
        LOG.info("MERGE updated rows: %d", merged)

        LOG.info("Running DELETE np with canonical twin...")
        cur.execute(DELETE_NP_SQL)
        deleted = cur.rowcount
        LOG.info("DELETE np rows: %d", deleted)

        if args.apply:
            conn.commit()
            LOG.info("COMMIT: merged=%d deleted_np=%d", merged, deleted)
        else:
            conn.rollback()
            LOG.info("DRY-RUN rolled back (use --apply). merged would=%d deleted_np would=%d", merged, deleted)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
