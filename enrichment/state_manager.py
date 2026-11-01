"""
state_manager.py - SQLiteを用いた企業単位の進捗管理

仕様書 10章:
- company_id 単位で status(pending/processing/done/not_found/error/low_confidence) を管理
- プロセスクラッシュ後、processing 状態が STALL_SEC 以上更新されなければ pending に戻す
- retry_count >= MAX_RETRY の error は再試行しない
- スレッドセーフな設計（threading.local でコネクションを分離）
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

logger = logging.getLogger(__name__)

MAX_RETRY = 3
_VALID_STATUSES = frozenset({"pending", "processing", "done", "not_found", "error", "low_confidence"})


class StateManager:
    """
    SQLite ベースの企業処理状態管理クラス。
    スレッドセーフ: threading.local() でコネクションをスレッド分離。
    """

    def __init__(self, db_path: Path, stall_sec: int = 3600) -> None:
        self.db_path = db_path
        self.stall_sec = stall_sec
        self._local = threading.local()
        self._init_lock = threading.Lock()
        self._initialize_schema()

    # ─────────────────────────────────────────────
    # 内部: コネクション管理
    # ─────────────────────────────────────────────
    def _conn(self) -> sqlite3.Connection:
        """スレッドローカルなコネクションを返す"""
        if not getattr(self._local, "conn", None):
            conn = sqlite3.connect(str(self.db_path), check_same_thread=False)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")   # 並列書き込み性能向上
            conn.execute("PRAGMA synchronous=NORMAL")
            self._local.conn = conn
        return self._local.conn

    def _initialize_schema(self) -> None:
        """DBスキーマの初期化（冪等）"""
        with self._init_lock:
            conn = sqlite3.connect(str(self.db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS company_state (
                    company_id          TEXT PRIMARY KEY,
                    status              TEXT NOT NULL DEFAULT 'pending',
                    hp_url              TEXT,
                    confidence          TEXT,
                    enrichment_source   TEXT,
                    retry_count         INTEGER NOT NULL DEFAULT 0,
                    last_attempted_at   TEXT,
                    error_message       TEXT,
                    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
                    updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cs_status ON company_state(status)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cs_updated ON company_state(updated_at)"
            )
            conn.execute("""
                CREATE TABLE IF NOT EXISTS enrichment_log (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_id      TEXT NOT NULL,
                    event           TEXT NOT NULL,
                    detail          TEXT,
                    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_el_company ON enrichment_log(company_id)"
            )
            conn.commit()
            conn.close()
        logger.info(f"[StateManager] DB initialized: {self.db_path}")

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # ─────────────────────────────────────────────
    # 一括登録
    # ─────────────────────────────────────────────
    def bulk_register_pending(self, company_ids: List[str]) -> int:
        """
        IDリストを pending として一括登録。既存レコードはスキップ（INSERT OR IGNORE）。
        Returns: 新規挿入件数
        """
        conn = self._conn()
        now = self._now_iso()
        cursor = conn.executemany(
            "INSERT OR IGNORE INTO company_state (company_id, status, created_at, updated_at) "
            "VALUES (?, 'pending', ?, ?)",
            [(cid, now, now) for cid in company_ids],
        )
        conn.commit()
        inserted = cursor.rowcount
        logger.debug(f"[StateManager] bulk_register_pending: {inserted} inserted")
        return inserted

    # ─────────────────────────────────────────────
    # バッチ取得
    # ─────────────────────────────────────────────
    def get_pending_batch(self, size: int = 50) -> List[str]:
        """
        pending または retry可能 error のIDをバッチ取得。
        取得前に停滞処理中を pending に戻す。
        """
        self._reset_stalled()
        conn = self._conn()
        rows = conn.execute(
            """
            SELECT company_id FROM company_state
            WHERE status = 'pending'
               OR (status = 'error' AND retry_count < ?)
            ORDER BY company_id
            LIMIT ?
            """,
            (MAX_RETRY, size),
        ).fetchall()
        return [r["company_id"] for r in rows]

    def get_terminal_ids(self, company_ids: List[str]) -> Set[str]:
        """
        渡された ID のうち、再処理不要（done/not_found/low_confidence）の集合を返す。
        DBチャンク取得後のスキップ判定用。
        """
        if not company_ids:
            return set()
        conn = self._conn()
        placeholders = ",".join("?" * len(company_ids))
        rows = conn.execute(
            f"""
            SELECT company_id FROM company_state
             WHERE company_id IN ({placeholders})
               AND status IN ('done', 'not_found', 'low_confidence')
            """,
            company_ids,
        ).fetchall()
        return {r["company_id"] for r in rows}

    def filter_processable(self, company_ids: List[str]) -> List[str]:
        """完了済みを除いた処理対象 ID リストを返す"""
        terminal = self.get_terminal_ids(company_ids)
        return [cid for cid in company_ids if cid not in terminal]

    def _reset_stalled(self) -> None:
        """
        status='processing' で last_attempted_at が stall_sec 以上古いレコードを
        pending に戻す（クラッシュ後のレジューム対応）。
        """
        threshold = datetime.now(timezone.utc)
        from datetime import timedelta
        cutoff = (threshold - timedelta(seconds=self.stall_sec)).strftime("%Y-%m-%dT%H:%M:%SZ")
        now = self._now_iso()
        conn = self._conn()
        cur = conn.execute(
            """
            UPDATE company_state
               SET status = 'pending', updated_at = ?
             WHERE status = 'processing'
               AND last_attempted_at < ?
            """,
            (now, cutoff),
        )
        conn.commit()
        if cur.rowcount > 0:
            logger.warning(
                f"[StateManager] Reset {cur.rowcount} stalled 'processing' → 'pending'"
            )

    # ─────────────────────────────────────────────
    # 状態遷移
    # ─────────────────────────────────────────────
    def mark_processing(self, company_id: str) -> None:
        now = self._now_iso()
        self._conn().execute(
            "UPDATE company_state SET status='processing', last_attempted_at=?, updated_at=? "
            "WHERE company_id=?",
            (now, now, company_id),
        )
        self._conn().commit()

    def mark_done(
        self,
        company_id: str,
        hp_url: str,
        confidence: str,
        enrichment_source: str,
    ) -> None:
        now = self._now_iso()
        self._conn().execute(
            """UPDATE company_state
               SET status='done', hp_url=?, confidence=?, enrichment_source=?, updated_at=?
             WHERE company_id=?""",
            (hp_url, confidence, enrichment_source, now, company_id),
        )
        self._conn().commit()

    def mark_not_found(self, company_id: str, reason: str = "") -> None:
        now = self._now_iso()
        self._conn().execute(
            "UPDATE company_state SET status='not_found', error_message=?, updated_at=? "
            "WHERE company_id=?",
            (reason, now, company_id),
        )
        self._conn().commit()

    def mark_error(self, company_id: str, error_msg: str) -> None:
        now = self._now_iso()
        self._conn().execute(
            """UPDATE company_state
               SET status='error', retry_count=retry_count+1, error_message=?, updated_at=?
             WHERE company_id=?""",
            (error_msg[:1000], now, company_id),
        )
        self._conn().commit()

    def mark_low_confidence(
        self,
        company_id: str,
        hp_url: str,
        confidence: str,
        reason: str,
    ) -> None:
        now = self._now_iso()
        self._conn().execute(
            """UPDATE company_state
               SET status='low_confidence', hp_url=?, confidence=?, error_message=?, updated_at=?
             WHERE company_id=?""",
            (hp_url, confidence, reason[:500], now, company_id),
        )
        self._conn().commit()

    # ─────────────────────────────────────────────
    # 照会・統計
    # ─────────────────────────────────────────────
    def is_completed(self, company_id: str) -> bool:
        """done / not_found / low_confidence は完了扱い（再処理しない）"""
        row = self._conn().execute(
            "SELECT status FROM company_state WHERE company_id=?", (company_id,)
        ).fetchone()
        return row is not None and row["status"] in ("done", "not_found", "low_confidence")

    def get_stats(self) -> Dict[str, int]:
        rows = self._conn().execute(
            "SELECT status, COUNT(*) as cnt FROM company_state GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}

    def get_total(self) -> int:
        row = self._conn().execute(
            "SELECT COUNT(*) as cnt FROM company_state"
        ).fetchone()
        return row["cnt"] if row else 0

    # ─────────────────────────────────────────────
    # ログ記録
    # ─────────────────────────────────────────────
    def log_event(self, company_id: str, event: str, detail: str = "") -> None:
        try:
            self._conn().execute(
                "INSERT INTO enrichment_log (company_id, event, detail) VALUES (?, ?, ?)",
                (company_id, event, detail[:2000]),
            )
            self._conn().commit()
        except Exception as e:
            logger.debug(f"[StateManager] log_event failed: {e}")

    # ─────────────────────────────────────────────
    # not_found / low_confidence の取得（ログ出力用）
    # ─────────────────────────────────────────────
    def iter_not_found(self, batch: int = 1000):
        """not_found レコードをバッチ返しジェネレータ"""
        offset = 0
        while True:
            rows = self._conn().execute(
                "SELECT company_id, error_message FROM company_state "
                "WHERE status='not_found' LIMIT ? OFFSET ?",
                (batch, offset),
            ).fetchall()
            if not rows:
                break
            yield from rows
            offset += batch

    def iter_low_confidence(self, batch: int = 1000):
        """low_confidence レコードをバッチ返しジェネレータ"""
        offset = 0
        while True:
            rows = self._conn().execute(
                "SELECT company_id, hp_url, confidence, error_message FROM company_state "
                "WHERE status='low_confidence' LIMIT ? OFFSET ?",
                (batch, offset),
            ).fetchall()
            if not rows:
                break
            yield from rows
            offset += batch

    def iter_errors(self, batch: int = 1000):
        """error レコードをバッチ返しジェネレータ"""
        offset = 0
        while True:
            rows = self._conn().execute(
                "SELECT company_id, error_message, retry_count FROM company_state "
                "WHERE status='error' LIMIT ? OFFSET ?",
                (batch, offset),
            ).fetchall()
            if not rows:
                break
            yield from rows
            offset += batch
