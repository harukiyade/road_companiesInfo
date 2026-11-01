"""
db_writer.py - エンリッチ結果の PostgreSQL UPSERT（オプション）

仕様書方針:
- 既存値が NULL/空文字のカラムのみ更新（上書きしない）
- デフォルトは CSV 出力のみ。--write-db 指定時に本モジュールを使用
- メタカラム（hp_url_used 等）は companies に存在しないため書き込まない
- OperationalError 時は自動再接続して再実行
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set

from psycopg2 import OperationalError, InterfaceError

from .db_loader import ResilientPgConnection

logger = logging.getLogger(__name__)

_SKIP_WRITE_COLS: frozenset[str] = frozenset({
    "id",
    "hp_url_used",
    "hp_match_confidence",
    "enrichment_source",
    "enriched_at",
    "fields_filled",
})


class DbWriter:
    """
    差分エンリッチ行を companies へ条件付き UPDATE する。
    長時間バッチ向けに ResilientPgConnection で再接続する。
    """

    def __init__(self) -> None:
        self._rpc: Optional[ResilientPgConnection] = None
        self._writable_cols: Optional[Set[str]] = None
        self._updated = 0
        self._skipped = 0

    def _ensure_conn(self) -> ResilientPgConnection:
        if self._rpc is None:
            self._rpc = ResilientPgConnection()
            self._writable_cols = self._load_writable_columns()
            logger.info(
                f"[db_writer] Connected. writable_cols={len(self._writable_cols or [])}"
            )
        else:
            self._rpc.ensure_alive()
        return self._rpc

    def _load_writable_columns(self) -> Set[str]:
        assert self._rpc is not None

        def _fetch(c):
            with c.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name
                      FROM information_schema.columns
                     WHERE table_schema = 'public'
                       AND table_name = 'companies'
                    """
                )
                return {r[0] for r in cur.fetchall()} - _SKIP_WRITE_COLS

        return self._rpc.run(_fetch, label="load writable columns")

    def upsert_enrichment_row(self, row: Dict) -> bool:
        """
        エンリッチ差分行を UPDATE。
        各カラムは「現在値が NULL/空」の場合のみセット。
        """
        company_id = str(row.get("id", "")).strip()
        if not company_id:
            self._skipped += 1
            return False

        rpc = self._ensure_conn()
        writable = self._writable_cols or set()

        set_parts: List[str] = []
        params: List = []
        for col, val in row.items():
            if col in _SKIP_WRITE_COLS or col not in writable:
                continue
            if val is None or str(val).strip() in ("", "nan", "None", "null"):
                continue
            set_parts.append(
                f'"{col}" = CASE '
                f'WHEN COALESCE(NULLIF(TRIM(COALESCE("{col}"::text, \'\')), \'\'), NULL) IS NULL '
                f'THEN %s ELSE "{col}" END'
            )
            params.append(str(val).strip())

        if not set_parts:
            self._skipped += 1
            return False

        set_parts.append("updated_at = CURRENT_TIMESTAMP")
        params.append(company_id)
        sql = f'UPDATE companies SET {", ".join(set_parts)} WHERE id = %s'
        field_count = len(set_parts) - 1

        def _update(c):
            with c.cursor() as cur:
                cur.execute(sql, params)
            c.commit()
            return True

        try:
            rpc.run(_update, label=f"UPSERT {company_id}")
            self._updated += 1
            logger.debug(f"[db_writer] Updated {company_id} ({field_count} fields)")
            return True
        except (OperationalError, InterfaceError) as e:
            logger.error(f"[db_writer] UPSERT reconnect-exhausted for {company_id}: {e}")
            self._skipped += 1
            return False
        except Exception as e:
            try:
                rpc.raw.rollback()
            except Exception:
                pass
            logger.error(f"[db_writer] UPSERT failed for {company_id}: {e}")
            self._skipped += 1
            return False

    def flush_stats(self) -> Dict[str, int]:
        return {"updated": self._updated, "skipped": self._skipped}

    def close(self) -> None:
        if self._rpc is not None:
            self._rpc.close()
            logger.info(
                f"[db_writer] Closed. updated={self._updated} skipped={self._skipped}"
            )
        self._rpc = None
