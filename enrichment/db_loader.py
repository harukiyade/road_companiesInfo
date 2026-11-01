"""
db_loader.py - PostgreSQL companies テーブルからのチャンク取得

設計方針:
- 500万件を一括ロードせず、キーセットページネーション（id > last_id）で取得
- OFFSET は大規模では遅いため原則使わない
- 進捗SQLite（state_manager）で done/not_found/low_confidence はスキップ
- 長時間バッチ向け: OperationalError 時に自動再接続＋クエリ再実行
"""
from __future__ import annotations

import logging
import os
import time
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Set, Tuple, TypeVar
from urllib.parse import unquote, urlparse

import psycopg2
from psycopg2 import OperationalError, InterfaceError
from psycopg2.extras import RealDictCursor

from .config import (
    PG_CONNECT_TIMEOUT,
    PG_MAX_RETRIES,
    PG_RETRY_BACKOFF,
    PG_KEEPALIVES,
    PG_KEEPALIVES_IDLE,
    PG_KEEPALIVES_INTERVAL,
    PG_KEEPALIVES_COUNT,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# エンリッチ対象として SELECT するカラム（スキーマ準拠・必要最小）
COMPANY_SELECT_COLUMNS: tuple[str, ...] = (
    "id",
    "name",
    "company_name",
    "corporate_number",
    "prefecture",
    # URL系
    "url", "urls", "homepage_url", "website_url", "company_url", "contact_form_url",
    # 連絡先
    "phone_number", "tel", "contact_phone_number", "fax", "email",
    # 所在地
    "headquarters_address", "address", "postal_code", "zip_code", "postcode",
    # 代表者
    "representative_name", "representative_kana", "representative_title",
    # 役員（1〜5）
    "executive_name1", "executive_position1",
    "executive_name2", "executive_position2",
    "executive_name3", "executive_position3",
    "executive_name4", "executive_position4",
    "executive_name5", "executive_position5",
    # 設立
    "founded_year", "founding_year", "established",
    "established_date", "establishment_date", "date_of_establishment",
    # 概要
    "overview", "company_description", "business_summary", "summary",
    "short_description", "description",
    "business_items", "specialties", "business_descriptions",
    # 従業員・資本金
    "employee_count", "employees", "number_of_employees",
    "capital_stock", "capital",
    # 拠点
    "office_count", "number_of_offices",
    "factory_count", "number_of_factories",
    "store_count", "number_of_stores",
    # SNS / メタ
    "facebook", "linkedin", "wantedly", "youtrust",
    "meta_description", "meta_keywords",
)

# 再接続対象の例外
_TRANSIENT_DB_ERRORS = (OperationalError, InterfaceError)


def get_pg_dsn() -> Dict[str, str]:
    """
    .env から PostgreSQL 接続パラメータを構築。

    優先順位:
      1. 個別変数（DATABASE_HOST 等）— パスワードに / @ 等を含んでも安全
      2. DATABASE_URL
    """
    host = os.getenv("DATABASE_HOST", "").strip()
    if host:
        return {
            "host": host,
            "port": os.getenv("DATABASE_PORT", "5432").strip() or "5432",
            "dbname": os.getenv("DATABASE_NAME", "postgres").strip() or "postgres",
            "user": os.getenv("DATABASE_USER", "postgres").strip() or "postgres",
            "password": os.getenv("DATABASE_PASS", ""),
        }

    url = os.getenv("DATABASE_URL", "").strip()
    if url and not url.startswith("#"):
        parsed = urlparse(url)
        if parsed.scheme.startswith("postgres"):
            return {
                "host": parsed.hostname or "127.0.0.1",
                "port": str(parsed.port or 5432),
                "dbname": (parsed.path or "/postgres").lstrip("/") or "postgres",
                "user": unquote(parsed.username or "postgres"),
                "password": unquote(parsed.password or ""),
            }

    return {
        "host": os.getenv("DATABASE_HOST", "127.0.0.1"),
        "port": os.getenv("DATABASE_PORT", "5432"),
        "dbname": os.getenv("DATABASE_NAME", "postgres"),
        "user": os.getenv("DATABASE_USER", "postgres"),
        "password": os.getenv("DATABASE_PASS", ""),
    }


def _connect(connect_timeout: Optional[int] = None):
    """keepalive 付きで新規接続を開く"""
    dsn = get_pg_dsn()
    timeout = connect_timeout if connect_timeout is not None else PG_CONNECT_TIMEOUT
    logger.info(
        f"[db_loader] Connecting to PostgreSQL "
        f"{dsn['user']}@{dsn['host']}:{dsn['port']}/{dsn['dbname']}"
    )
    return psycopg2.connect(
        host=dsn["host"],
        port=int(dsn["port"]),
        dbname=dsn["dbname"],
        user=dsn["user"],
        password=dsn["password"],
        connect_timeout=timeout,
        keepalives=PG_KEEPALIVES,
        keepalives_idle=PG_KEEPALIVES_IDLE,
        keepalives_interval=PG_KEEPALIVES_INTERVAL,
        keepalives_count=PG_KEEPALIVES_COUNT,
    )


class ResilientPgConnection:
    """
    長時間バッチ向けの再接続可能な接続ラッパー。

    - execute / fetch 系で OperationalError / InterfaceError が出たら
      コネクションを張り直して同じ操作を再実行（最大 PG_MAX_RETRIES 回）
    - keepalive でアイドル切断を抑制
    """

    def __init__(self, connect_timeout: Optional[int] = None) -> None:
        self._connect_timeout = connect_timeout
        self._conn = _connect(connect_timeout)
        self._reconnect_count = 0

    @property
    def raw(self):
        return self._conn

    def close(self) -> None:
        try:
            if self._conn and not self._conn.closed:
                self._conn.close()
        except Exception:
            pass
        logger.debug(
            f"[db_loader] Connection closed "
            f"(reconnects={self._reconnect_count})"
        )

    def _safe_close(self) -> None:
        try:
            if self._conn and not self._conn.closed:
                self._conn.close()
        except Exception:
            pass

    def reconnect(self) -> None:
        """強制再接続"""
        self._safe_close()
        wait = PG_RETRY_BACKOFF ** min(self._reconnect_count, 4)
        if wait > 0:
            logger.warning(
                f"[db_loader] Reconnecting in {wait:.1f}s "
                f"(attempt reconnect #{self._reconnect_count + 1})..."
            )
            time.sleep(wait)
        self._conn = _connect(self._connect_timeout)
        self._reconnect_count += 1
        logger.info(
            f"[db_loader] Reconnected OK (total_reconnects={self._reconnect_count})"
        )

    def ensure_alive(self) -> None:
        """死活確認。ダメなら再接続"""
        try:
            if self._conn is None or self._conn.closed:
                self.reconnect()
                return
            with self._conn.cursor() as cur:
                cur.execute("SELECT 1")
        except _TRANSIENT_DB_ERRORS as e:
            logger.warning(f"[db_loader] Connection health check failed: {e}")
            self.reconnect()

    def run(self, fn: Callable[[Any], T], label: str = "query") -> T:
        """
        fn(conn) を実行。切断系エラーなら再接続して再試行。
        """
        last_err: Optional[BaseException] = None
        for attempt in range(PG_MAX_RETRIES + 1):
            try:
                if self._conn is None or self._conn.closed:
                    self.reconnect()
                return fn(self._conn)
            except _TRANSIENT_DB_ERRORS as e:
                last_err = e
                logger.warning(
                    f"[db_loader] {label} failed ({type(e).__name__}: {e}) "
                    f"→ retry {attempt + 1}/{PG_MAX_RETRIES}"
                )
                if attempt >= PG_MAX_RETRIES:
                    break
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                self.reconnect()
            except Exception:
                # 非一時エラーは巻き戻して上位へ
                try:
                    self._conn.rollback()
                except Exception:
                    pass
                raise
        raise OperationalError(
            f"[db_loader] {label} failed after {PG_MAX_RETRIES} reconnects: {last_err}"
        )


@contextmanager
def pg_connection(connect_timeout: Optional[int] = None):
    """psycopg2 コネクションのコンテキストマネージャ（再接続対応ラッパー）"""
    wrapper = ResilientPgConnection(connect_timeout=connect_timeout)
    try:
        yield wrapper
    finally:
        wrapper.close()


def probe_connection() -> Tuple[bool, str]:
    """接続可否と companies 件数を返す（パイロット前のヘルスチェック）"""
    try:
        with pg_connection() as conn:
            def _count(c):
                with c.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM companies")
                    return cur.fetchone()[0]
            count = conn.run(_count, label="probe COUNT(*)")
        return True, f"OK (companies={count:,})"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _resolve_available_columns(conn: ResilientPgConnection) -> List[str]:
    """
    companies テーブルに実在するカラムのうち、SELECT 対象と交差する列だけ返す。
    """
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
            return {r[0] for r in cur.fetchall()}

    existing = conn.run(_fetch, label="resolve columns")
    selected = [c for c in COMPANY_SELECT_COLUMNS if c in existing]
    if "id" not in selected:
        raise RuntimeError("companies テーブルに id カラムがありません")
    missing = [c for c in COMPANY_SELECT_COLUMNS if c not in existing]
    if missing:
        logger.warning(
            f"[db_loader] Skipping {len(missing)} missing columns "
            f"(sample: {missing[:8]})"
        )
    logger.info(f"[db_loader] SELECT columns: {len(selected)}")
    return selected


def _incomplete_where_sql(available: Set[str]) -> Optional[str]:
    """情報が不足している企業を優先取得するための WHERE 断片。"""
    url_cols = [c for c in ("url", "homepage_url", "website_url", "company_url") if c in available]
    parts: List[str] = []

    if url_cols:
        url_nulls = " AND ".join(
            f"COALESCE(NULLIF(TRIM(COALESCE(\"{c}\"::text, '')), ''), NULL) IS NULL"
            for c in url_cols
        )
        parts.append(f"({url_nulls})")

    for col in ("phone_number", "headquarters_address", "email"):
        if col in available:
            parts.append(
                f"COALESCE(NULLIF(TRIM(COALESCE(\"{col}\"::text, '')), ''), NULL) IS NULL"
            )

    if not parts:
        return None
    return "(" + " OR ".join(parts) + ")"


def _row_to_dict(row: Dict) -> Dict:
    """DB行をエンリッチパイプライン用 dict に正規化"""
    out: Dict = {}
    for k, v in dict(row).items():
        if v is None:
            out[k] = ""
        elif isinstance(v, (list, tuple)):
            out[k] = ",".join(str(x) for x in v) if v else ""
        else:
            out[k] = str(v)
    if not out.get("company_name") and out.get("name"):
        out["company_name"] = out["name"]
    return out


def fetch_companies_chunk(
    conn: ResilientPgConnection,
    columns: List[str],
    last_id: Optional[str],
    chunk_size: int,
    incomplete_only: bool = False,
) -> List[Dict]:
    """キーセットページネーションで1チャンク取得（切断時は自動再試行）。"""
    col_sql = ", ".join(f'"{c}"' for c in columns)
    clauses: List[str] = []
    params: List = []

    if last_id is not None:
        clauses.append("id > %s")
        params.append(last_id)

    if incomplete_only:
        incomplete = _incomplete_where_sql(set(columns))
        if incomplete:
            clauses.append(incomplete)
        else:
            logger.warning("[db_loader] incomplete_only requested but no filter columns")

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    sql = f"SELECT {col_sql} FROM companies{where} ORDER BY id LIMIT %s"
    params.append(chunk_size)

    def _fetch(c):
        with c.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    rows = conn.run(_fetch, label=f"chunk last_id={last_id}")
    return [_row_to_dict(r) for r in rows]


def fetch_companies_by_ids(
    conn: ResilientPgConnection,
    columns: List[str],
    company_ids: Iterable[str],
) -> Dict[str, Dict]:
    """IDリストで企業行を取得（切断時は自動再試行）"""
    ids = [str(i) for i in company_ids if i]
    if not ids:
        return {}
    col_sql = ", ".join(f'"{c}"' for c in columns)

    def _fetch(c):
        with c.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                f"SELECT {col_sql} FROM companies WHERE id = ANY(%s)",
                (ids,),
            )
            return cur.fetchall()

    rows = conn.run(_fetch, label=f"fetch_by_ids n={len(ids)}")
    return {str(r["id"]): _row_to_dict(r) for r in rows}


def iter_company_chunks(
    chunk_size: int = 1000,
    limit: Optional[int] = None,
    incomplete_only: bool = False,
    skip_ids: Optional[Set[str]] = None,
) -> Generator[List[Dict], None, None]:
    """
    companies をチャンク単位で yield するジェネレータ。
    長時間実行中の接続断は ResilientPgConnection が吸収する。
    """
    skip_ids = skip_ids or set()
    yielded_total = 0
    last_id: Optional[str] = None

    with pg_connection() as conn:
        columns = _resolve_available_columns(conn)

        def _count(c):
            with c.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM companies")
                return cur.fetchone()[0]

        total = conn.run(_count, label="COUNT(*)")
        logger.info(f"[db_loader] companies total≈{total:,}, chunk_size={chunk_size}")

        while True:
            if limit is not None and yielded_total >= limit:
                break

            # 定期的な死活確認（アイドル切断の早期検知）
            if yielded_total > 0 and yielded_total % (chunk_size * 10) == 0:
                conn.ensure_alive()

            rows = fetch_companies_chunk(
                conn, columns, last_id, chunk_size, incomplete_only=incomplete_only
            )
            if not rows:
                logger.info("[db_loader] No more rows (end of table)")
                break

            last_id = rows[-1]["id"]
            filtered = [r for r in rows if r["id"] not in skip_ids]

            if not filtered:
                logger.debug(
                    f"[db_loader] Chunk all skipped (last_id={last_id}, size={len(rows)})"
                )
                continue

            if limit is not None:
                remain = limit - yielded_total
                if len(filtered) > remain:
                    filtered = filtered[:remain]

            yielded_total += len(filtered)
            logger.info(
                f"[db_loader] Yield chunk: {len(filtered)} rows "
                f"(raw={len(rows)}, last_id={last_id}, total_yielded={yielded_total})"
            )
            yield filtered

            if len(rows) < chunk_size:
                break

    logger.info(f"[db_loader] Iteration complete. total_yielded={yielded_total}")
