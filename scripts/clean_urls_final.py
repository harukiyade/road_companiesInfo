#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
companies.homepage_url の商用グレード・バッチクレンジング。

- [] / JSON 配列文字列 / スキーム欠落 / 明らかなゴミ を正規化
- FOR UPDATE SKIP LOCKED + 5000 件単位コミット + id 昇順
- デッドロック等は指数バックオフで再試行
- --dry-run で LIMIT 10 プレビュー（DB 不変更）

  python scripts/clean_urls_final.py --dry-run
  python scripts/clean_urls_final.py --batch-size 5000
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

try:
    from psycopg2 import OperationalError, InterfaceError
    from psycopg2 import errors as pg_errors
    from psycopg2.extras import execute_values
except ImportError as e:
    raise SystemExit("psycopg2-binary が必要です") from e

try:
    import tqdm  # type: ignore
except ImportError:
    tqdm = None  # type: ignore[misc, assignment]

try:
    from dotenv import load_dotenv
    from pathlib import Path

    _root = Path(__file__).resolve().parent.parent
    load_dotenv(dotenv_path=_root / ".env", override=True)
    load_dotenv()
except ImportError:
    pass

try:
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))
    import repair_companies_column_shift_from_fixed_csv3 as rep  # type: ignore

LOG = logging.getLogger("clean_urls_final")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

_RETRIABLE = (OperationalError, InterfaceError)
try:
    _DEADLOCK = (pg_errors.DeadlockDetected,)
except Exception:  # pragma: no cover
    _DEADLOCK = ()


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, _DEADLOCK):
        return True
    if isinstance(exc, OperationalError):
        msg = str(exc).lower()
        if "deadlock" in msg or "timeout" in msg or "could not serialize" in msg:
            return True
    return False


def normalize_homepage_url(raw: Any) -> Tuple[Optional[str], str]:
    """
    正規化後の値（NULL のときは None）と、分類タグを返す。
    入力と出力が同じでもタグは「already_ok」または「unchanged」にできるが、
    ここでは「適用すべき新値」を返し、呼び出し側で比較する。
    """
    if raw is None:
        return None, "null_input"

    s0 = str(raw).strip()
    if not s0:
        return None, "empty_string"

    low = s0.lower()
    if low in ("[]", "{}", "null", "none", "undefined"):
        return None, "empty_literal"

    s = s0

    # JSON 配列形式 ["https://..."] 等
    if s.startswith("["):
        parsed_ok = False
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                parsed_ok = True
                if len(parsed) == 0:
                    return None, "json_empty_array"
                first = parsed[0]
                if isinstance(first, str) and first.strip():
                    s = first.strip()
                else:
                    return None, "json_array_no_string"
        except json.JSONDecodeError:
            parsed_ok = False
        if not parsed_ok:
            m = re.match(r'^\[\s*"(.+)"\s*\]\s*$', s, re.DOTALL)
            if m:
                s = m.group(1).replace('\\"', '"').strip()
            else:
                inner = s.strip().lstrip("[").rstrip("]").strip().strip('"').strip()
                if not inner or inner == s0:
                    return None, "bracket_garbage"
                s = inner

    s = s.strip().strip('"').strip("'")

    if not s or s.lower() in ("[]", "null"):
        return None, "stripped_empty"

    if re.fullmatch(r"\d+", s):
        return None, "digits_only"

    if len(s) < 4:
        return None, "too_short"

    # スキーム付与
    if not re.match(r"^https?://", s, re.IGNORECASE):
        sl = s.lower()
        if sl.startswith("www.") or ("." in s and " " not in s and "/" not in s.split(".")[0]):
            s = "https://" + s
        else:
            return None, "not_url_like"

    try:
        p = urlparse(s)
        if not p.netloc or len(p.netloc) < 3:
            return None, "no_netloc"
        if ".." in p.netloc:
            return None, "invalid_netloc"
    except Exception:
        return None, "urlparse_error"

    return s, "normalized"


def _sql_dirty_predicate() -> str:
    """Python 側と揃えた粗い事前フィルタ（インデックス無くても動く想定）。"""
    # psycopg2 はクエリ内の % をプレースホルダと解釈するため、LIKE/ILIKE の % は %% にする
    return """
    homepage_url IS NOT NULL
    AND BTRIM(homepage_url::text) <> ''
    AND (
        BTRIM(homepage_url::text) IN ('[]', '{}', 'null', 'None', 'undefined')
        OR BTRIM(homepage_url::text) ILIKE '[%%'
        OR BTRIM(homepage_url::text) ~* '^www\\.'
        OR BTRIM(homepage_url::text) ~ '^[0-9]+$'
        OR (
            BTRIM(homepage_url::text) NOT ILIKE 'http%%'
            AND BTRIM(homepage_url::text) LIKE '%%.%%'
        )
    )
    """


@dataclass
class RunStats:
    rows_selected: int = 0
    rows_updated: int = 0
    batches_skipped_fatal: int = 0
    by_tag: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def add_tag(self, tag: str, n: int = 1) -> None:
        self.by_tag[tag] += n


def _retry_sleep(attempt: int, base: float, cap: float) -> None:
    t = min(cap, base * (2**attempt))
    t = t * (0.8 + 0.4 * random.random())
    time.sleep(t)


def _fetch_batch(cur, last_id: str, batch_size: int) -> List[Tuple[str, Any]]:
    cur.execute(
        f"""
        SELECT id, homepage_url
        FROM companies
        WHERE id > %s
          AND ({_sql_dirty_predicate()})
        ORDER BY id ASC
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        """,
        (last_id, batch_size),
    )
    return list(cur.fetchall())


def _apply_updates(cur, pairs: List[Tuple[str, Optional[str]]]) -> int:
    if not pairs:
        return 0
    execute_values(
        cur,
        """
        UPDATE companies AS c
        SET homepage_url = v.nu
        FROM (VALUES %s) AS v(id, nu)
        WHERE c.id = v.id
        """,
        pairs,
        template="(%s, %s)",
        page_size=len(pairs),
    )
    return len(pairs)


def run_preview(conn, limit: int) -> None:
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT id, homepage_url
        FROM companies
        WHERE ({_sql_dirty_predicate()})
        ORDER BY id ASC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    LOG.info("--- dry-run プレビュー（最大 %d 件）---", limit)
    for rid, old in rows:
        new, tag = normalize_homepage_url(old)
        old_s = None if old is None else str(old)[:120]
        new_s = None if new is None else str(new)[:120]
        LOG.info("id=%s tag=%s\n  old: %r\n  new: %r", rid, tag, old_s, new_s)
    cur.close()


def run_main(conn: Any, args: argparse.Namespace) -> int:
    stats = RunStats()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM companies WHERE {_sql_dirty_predicate()};")
        total_dirty = int(cur.fetchone()[0] or 0)
    finally:
        cur.close()
        conn.commit()

    LOG.info("dirty 推定件数（SQL 事前フィルタ）: %d", total_dirty)

    last_id = ""
    processed = 0
    pbar = None
    if tqdm is not None and total_dirty > 0:
        pbar = tqdm.tqdm(total=total_dirty, desc="homepage_url", unit="row")

    max_attempts = 12
    while True:
        rows: List[Tuple[str, Any]] = []
        exhausted_no_more_rows = False
        attempt = 0
        while attempt < max_attempts:
            cur = conn.cursor()
            try:
                if args.lock_timeout_ms:
                    cur.execute("SET LOCAL lock_timeout = %s", (f"{int(args.lock_timeout_ms)}ms",))
                if args.statement_timeout_ms:
                    cur.execute("SET LOCAL statement_timeout = %s", (f"{int(args.statement_timeout_ms)}ms",))

                rows = _fetch_batch(cur, last_id, args.batch_size)
                if not rows:
                    cur.close()
                    conn.commit()
                    exhausted_no_more_rows = True
                    break

                pairs: List[Tuple[str, Optional[str]]] = []
                for rid, old in rows:
                    new, tag = normalize_homepage_url(old)
                    old_norm = None if old is None else str(old).strip()
                    new_norm = new
                    if (old_norm or "") == ("" if new_norm is None else str(new_norm).strip()):
                        stats.add_tag("no_change_" + tag, 1)
                        continue
                    pairs.append((str(rid), new_norm))
                    stats.add_tag(tag, 1)

                if pairs:
                    _apply_updates(cur, pairs)
                    stats.rows_updated += len(pairs)
                last_id = str(rows[-1][0])
                processed += len(rows)
                cur.close()
                conn.commit()

                if pbar is not None:
                    pbar.update(len(rows))
                else:
                    pct = (100.0 * processed / total_dirty) if total_dirty else 0.0
                    lid = last_id[:48] if last_id else ""
                    LOG.info(
                        "進捗 %d / %d 件 [%.2f%%] last_id=%s batch_updates=%d",
                        processed,
                        total_dirty,
                        pct,
                        lid,
                        len(pairs),
                    )

                break

            except Exception as e:
                try:
                    cur.close()
                except Exception:
                    pass
                conn.rollback()
                if _is_retryable(e) and attempt < max_attempts - 1:
                    LOG.warning("再試行 (%s) attempt=%d: %s", type(e).__name__, attempt + 1, e)
                    _retry_sleep(attempt, args.retry_base_sec, args.retry_cap_sec)
                    attempt += 1
                    continue
                LOG.exception("バッチ失敗（再試行打ち切り）: %s", e)
                if rows:
                    LOG.critical(
                        "当該バッチは未更新のまま id を進めます（後で再実行してください）: %d 行",
                        len(rows),
                    )
                    last_id = str(rows[-1][0])
                    processed += len(rows)
                    stats.batches_skipped_fatal += 1
                    if pbar is not None:
                        pbar.update(len(rows))
                else:
                    LOG.error("行未取得のまま失敗しました。10秒待機して同じ位置から再試行します。")
                    time.sleep(10.0)
                attempt += 1
                break

        if exhausted_no_more_rows:
            break

    if pbar is not None:
        pbar.close()

    LOG.info("=== 完了サマリ ===")
    LOG.info("選択行累計（バッチ取得）: %d", processed)
    LOG.info("UPDATE 行数（実際に値が変わったもの）: %d", stats.rows_updated)
    LOG.info("致命エラーでスキップしたバッチ数: %d", stats.batches_skipped_fatal)
    for k in sorted(stats.by_tag.keys(), key=lambda x: (-stats.by_tag[x], x)):
        LOG.info("  %-28s %d", k, stats.by_tag[k])
    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="companies.homepage_url バッチクレンジング")
    ap.add_argument("--dry-run", action="store_true", help="LIMIT 10 でプレビューのみ（DB 不変更）")
    ap.add_argument("--preview-limit", type=int, default=10, help="--dry-run 時の件数")
    ap.add_argument("--batch-size", type=int, default=5000, help="1 トランザクションあたりの最大行数")
    ap.add_argument("--retry-base-sec", type=float, default=0.5, help="指数バックオフ初期秒")
    ap.add_argument("--retry-cap-sec", type=float, default=30.0, help="指数バックオフ上限秒")
    ap.add_argument("--lock-timeout-ms", type=int, default=5000, help="SET LOCAL lock_timeout（0 で無効）")
    ap.add_argument("--statement-timeout-ms", type=int, default=0, help="SET LOCAL statement_timeout（0 で無効）")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    rc = 1
    try:
        conn = rep.get_connection()
        try:
            if args.dry_run:
                run_preview(conn, args.preview_limit)
                rc = 0
            else:
                rc = run_main(conn, args)
        finally:
            conn.close()
    except KeyboardInterrupt:
        LOG.warning("中断されました（KeyboardInterrupt）")
        rc = 130
    except Exception as e:
        LOG.exception("致命的エラー（終了）: %s", e)
        rc = 1
    sys.exit(rc)


if __name__ == "__main__":
    main()
