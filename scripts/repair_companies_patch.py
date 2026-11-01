#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
companies の残存インテグリティ異常を追加修復するパッチバッチ。

対象:
1) 財務カラムに混入した法人番号（13桁）を corporate_number へ移送し、元カラムを NULL 化
   - revenue, capital_stock, latest_profit, latest_revenue
2) 郵便番号カラムの不正値を NULL 化
   - postal_code, representative_postal_code
3) company_url の残存異常を正規化（https 付与 / ゴミは NULL）

要件:
- 5000件バッチ
- FOR UPDATE SKIP LOCKED
- デッドロック等の指数バックオフ再試行
- --dry-run プレビュー対応
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
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

try:
    from psycopg2 import InterfaceError, OperationalError
    from psycopg2 import errors as pg_errors
    from psycopg2.extras import execute_values
except ImportError as exc:
    raise SystemExit("psycopg2-binary が必要です") from exc

try:
    import tqdm  # type: ignore
except ImportError:
    tqdm = None  # type: ignore[misc, assignment]

try:
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))
    import repair_companies_column_shift_from_fixed_csv3 as rep  # type: ignore

LOG = logging.getLogger("repair_companies_patch")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

_RETRIABLE = (OperationalError, InterfaceError)
try:
    _DEADLOCK = (pg_errors.DeadlockDetected,)
except Exception:
    _DEADLOCK = ()

RE_CORP_13 = re.compile(r"^\d{13}$")
RE_POSTAL_ALLOWED = re.compile(r"^[0-9-]+$")


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, _DEADLOCK):
        return True
    if isinstance(exc, _RETRIABLE):
        msg = str(exc).lower()
        if "deadlock" in msg or "timeout" in msg or "could not serialize" in msg:
            return True
    return False


def _retry_sleep(attempt: int, base: float, cap: float) -> None:
    sec = min(cap, base * (2**attempt))
    sec *= 0.8 + 0.4 * random.random()
    time.sleep(sec)


def _to_text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _is_empty(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def normalize_company_url(raw: Any) -> tuple[Optional[str], str]:
    if raw is None:
        return None, "null_input"

    s0 = str(raw).strip()
    if not s0:
        return None, "empty_string"

    low = s0.lower()
    if low in ("[]", "{}", "null", "none", "undefined"):
        return None, "empty_literal"

    s = s0
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
    return """
    (
        (revenue IS NOT NULL AND BTRIM(revenue::text) ~ '^\\d{13}$')
        OR (capital_stock IS NOT NULL AND BTRIM(capital_stock::text) ~ '^\\d{13}$')
        OR (latest_profit IS NOT NULL AND BTRIM(latest_profit::text) ~ '^\\d{13}$')
        OR (latest_revenue IS NOT NULL AND BTRIM(latest_revenue::text) ~ '^\\d{13}$')
        OR (postal_code IS NOT NULL AND BTRIM(postal_code::text) <> '' AND BTRIM(postal_code::text) !~ '^[0-9-]+$')
        OR (representative_postal_code IS NOT NULL AND BTRIM(representative_postal_code::text) <> '' AND BTRIM(representative_postal_code::text) !~ '^[0-9-]+$')
        OR (
            company_url IS NOT NULL
            AND BTRIM(company_url::text) <> ''
            AND (
                BTRIM(company_url::text) IN ('[]', '{}', 'null', 'None', 'undefined')
                OR BTRIM(company_url::text) ILIKE '[%%'
                OR BTRIM(company_url::text) ~* '^www\\.'
                OR BTRIM(company_url::text) ~ '^[0-9]+$'
                OR BTRIM(company_url::text) NOT ILIKE 'http%%'
            )
        )
    )
    """


@dataclass
class RowPatch:
    id: str
    set_corporate_number: bool = False
    corporate_number: Optional[str] = None
    set_revenue: bool = False
    revenue: Optional[int] = None
    set_capital_stock: bool = False
    capital_stock: Optional[int] = None
    set_latest_profit: bool = False
    latest_profit: Optional[int] = None
    set_latest_revenue: bool = False
    latest_revenue: Optional[int] = None
    set_postal_code: bool = False
    postal_code: Optional[str] = None
    set_representative_postal_code: bool = False
    representative_postal_code: Optional[str] = None
    set_company_url: bool = False
    company_url: Optional[str] = None
    reasons: List[str] = field(default_factory=list)

    def changed(self) -> bool:
        return any(
            [
                self.set_corporate_number,
                self.set_revenue,
                self.set_capital_stock,
                self.set_latest_profit,
                self.set_latest_revenue,
                self.set_postal_code,
                self.set_representative_postal_code,
                self.set_company_url,
            ]
        )


@dataclass
class RunStats:
    selected_rows: int = 0
    updated_rows: int = 0
    skipped_batches_fatal: int = 0
    by_reason: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def add(self, key: str) -> None:
        self.by_reason[key] += 1


def _mark_financial_pollution(
    patch: RowPatch,
    row: Dict[str, Any],
    field_name: str,
    stats: RunStats,
) -> Optional[str]:
    raw = _to_text(row.get(field_name))
    if raw and RE_CORP_13.fullmatch(raw):
        if field_name == "revenue":
            patch.set_revenue = True
            patch.revenue = None
        elif field_name == "capital_stock":
            patch.set_capital_stock = True
            patch.capital_stock = None
        elif field_name == "latest_profit":
            patch.set_latest_profit = True
            patch.latest_profit = None
        elif field_name == "latest_revenue":
            patch.set_latest_revenue = True
            patch.latest_revenue = None
        tag = f"{field_name}_corp13_cleared"
        patch.reasons.append(tag)
        stats.add(tag)
        return raw
    return None


def build_patch(row: Dict[str, Any], stats: RunStats) -> RowPatch:
    patch = RowPatch(id=str(row["id"]))

    corp_candidate = None
    for col in ("revenue", "capital_stock", "latest_profit", "latest_revenue"):
        hit = _mark_financial_pollution(patch, row, col, stats)
        if hit and corp_candidate is None:
            corp_candidate = hit

    current_corp = _to_text(row.get("corporate_number"))
    if corp_candidate and not current_corp:
        patch.set_corporate_number = True
        patch.corporate_number = corp_candidate
        patch.reasons.append("corporate_number_filled_from_financial")
        stats.add("corporate_number_filled_from_financial")

    postal = _to_text(row.get("postal_code"))
    if postal and not RE_POSTAL_ALLOWED.fullmatch(postal):
        patch.set_postal_code = True
        patch.postal_code = None
        patch.reasons.append("postal_code_invalid_cleared")
        stats.add("postal_code_invalid_cleared")

    rep_postal = _to_text(row.get("representative_postal_code"))
    if rep_postal and not RE_POSTAL_ALLOWED.fullmatch(rep_postal):
        patch.set_representative_postal_code = True
        patch.representative_postal_code = None
        patch.reasons.append("representative_postal_code_invalid_cleared")
        stats.add("representative_postal_code_invalid_cleared")

    company_url = row.get("company_url")
    if not _is_empty(company_url):
        normalized, tag = normalize_company_url(company_url)
        old_norm = _to_text(company_url)
        new_norm = "" if normalized is None else normalized.strip()
        if old_norm != new_norm:
            patch.set_company_url = True
            patch.company_url = normalized
            patch.reasons.append("company_url_" + tag)
            stats.add("company_url_" + tag)

    return patch


def _fetch_batch(cur, last_id: str, batch_size: int) -> List[Dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
            id,
            corporate_number,
            revenue,
            capital_stock,
            latest_profit,
            latest_revenue,
            postal_code,
            representative_postal_code,
            company_url
        FROM companies
        WHERE id > %s
          AND ({_sql_dirty_predicate()})
        ORDER BY id ASC
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        """,
        (last_id, batch_size),
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _apply_updates(cur, patches: List[RowPatch]) -> int:
    if not patches:
        return 0

    payload = [
        (
            p.id,
            p.set_corporate_number, p.corporate_number,
            p.set_revenue, p.revenue,
            p.set_capital_stock, p.capital_stock,
            p.set_latest_profit, p.latest_profit,
            p.set_latest_revenue, p.latest_revenue,
            p.set_postal_code, p.postal_code,
            p.set_representative_postal_code, p.representative_postal_code,
            p.set_company_url, p.company_url,
        )
        for p in patches
    ]

    execute_values(
        cur,
        """
        UPDATE companies AS c
        SET
            corporate_number = CASE WHEN v.set_corporate_number THEN v.corporate_number ELSE c.corporate_number END,
            revenue = CASE WHEN v.set_revenue THEN v.revenue::bigint ELSE c.revenue END,
            capital_stock = CASE WHEN v.set_capital_stock THEN v.capital_stock::bigint ELSE c.capital_stock END,
            latest_profit = CASE WHEN v.set_latest_profit THEN v.latest_profit::bigint ELSE c.latest_profit END,
            latest_revenue = CASE WHEN v.set_latest_revenue THEN v.latest_revenue::bigint ELSE c.latest_revenue END,
            postal_code = CASE WHEN v.set_postal_code THEN v.postal_code ELSE c.postal_code END,
            representative_postal_code = CASE WHEN v.set_representative_postal_code THEN v.representative_postal_code ELSE c.representative_postal_code END,
            company_url = CASE WHEN v.set_company_url THEN v.company_url ELSE c.company_url END
        FROM (VALUES %s) AS v(
            id,
            set_corporate_number, corporate_number,
            set_revenue, revenue,
            set_capital_stock, capital_stock,
            set_latest_profit, latest_profit,
            set_latest_revenue, latest_revenue,
            set_postal_code, postal_code,
            set_representative_postal_code, representative_postal_code,
            set_company_url, company_url
        )
        WHERE c.id = v.id
        """,
        payload,
        template="""(
            %s::text,
            %s::boolean, %s::text,
            %s::boolean, %s::bigint,
            %s::boolean, %s::bigint,
            %s::boolean, %s::bigint,
            %s::boolean, %s::bigint,
            %s::boolean, %s::text,
            %s::boolean, %s::text,
            %s::boolean, %s::text
        )""",
        page_size=len(payload),
    )
    return len(payload)


def run_preview(conn, limit: int) -> int:
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            id,
            corporate_number,
            revenue,
            capital_stock,
            latest_profit,
            latest_revenue,
            postal_code,
            representative_postal_code,
            company_url
        FROM companies
        WHERE {_sql_dirty_predicate()}
        ORDER BY id ASC
        LIMIT %s
        """,
        (limit,),
    )
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()

    stats = RunStats()
    LOG.info("--- dry-run プレビュー（最大 %d 件）---", limit)
    for row in rows:
        patch = build_patch(row, stats)
        if not patch.changed():
            continue
        after = dict(row)
        if patch.set_corporate_number:
            after["corporate_number"] = patch.corporate_number
        if patch.set_revenue:
            after["revenue"] = patch.revenue
        if patch.set_capital_stock:
            after["capital_stock"] = patch.capital_stock
        if patch.set_latest_profit:
            after["latest_profit"] = patch.latest_profit
        if patch.set_latest_revenue:
            after["latest_revenue"] = patch.latest_revenue
        if patch.set_postal_code:
            after["postal_code"] = patch.postal_code
        if patch.set_representative_postal_code:
            after["representative_postal_code"] = patch.representative_postal_code
        if patch.set_company_url:
            after["company_url"] = patch.company_url

        LOG.info("id=%s reasons=%s", row["id"], ",".join(patch.reasons))
        LOG.info("  before=%s", json.dumps(row, ensure_ascii=False, default=str))
        LOG.info("  after =%s", json.dumps(after, ensure_ascii=False, default=str))

    for k in sorted(stats.by_reason.keys(), key=lambda x: (-stats.by_reason[x], x)):
        LOG.info("reason %-44s %d", k, stats.by_reason[k])
    return 0


def run_main(conn: Any, args: argparse.Namespace) -> int:
    stats = RunStats()
    conn.autocommit = False

    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM companies WHERE {_sql_dirty_predicate()}")
        total_dirty = int(cur.fetchone()[0] or 0)
    finally:
        cur.close()
        conn.commit()

    LOG.info("dirty 推定件数: %d", total_dirty)
    pbar = tqdm.tqdm(total=total_dirty, desc="companies-patch", unit="row") if (tqdm is not None and total_dirty > 0) else None

    last_id = ""
    max_attempts = args.max_retries
    while True:
        rows: List[Dict[str, Any]] = []
        exhausted = False
        attempt = 0

        while attempt < max_attempts:
            cur = conn.cursor()
            try:
                if args.lock_timeout_ms > 0:
                    cur.execute("SET LOCAL lock_timeout = %s", (f"{int(args.lock_timeout_ms)}ms",))
                if args.statement_timeout_ms > 0:
                    cur.execute("SET LOCAL statement_timeout = %s", (f"{int(args.statement_timeout_ms)}ms",))

                rows = _fetch_batch(cur, last_id, args.batch_size)
                if not rows:
                    cur.close()
                    conn.commit()
                    exhausted = True
                    break

                stats.selected_rows += len(rows)
                patches: List[RowPatch] = []
                for row in rows:
                    patch = build_patch(row, stats)
                    if patch.changed():
                        patches.append(patch)

                if patches:
                    stats.updated_rows += _apply_updates(cur, patches)

                last_id = str(rows[-1]["id"])
                cur.close()
                conn.commit()

                if pbar is not None:
                    pbar.update(len(rows))
                    top = sorted(stats.by_reason.items(), key=lambda kv: (-kv[1], kv[0]))[:4]
                    if top:
                        pbar.set_postfix_str(" ".join([f"{k}:{v}" for k, v in top]))
                else:
                    LOG.info(
                        "processed=%d updated=%d last_id=%s batch_rows=%d batch_updates=%d",
                        stats.selected_rows,
                        stats.updated_rows,
                        last_id[:48],
                        len(rows),
                        len(patches),
                    )
                break
            except Exception as exc:
                try:
                    cur.close()
                except Exception:
                    pass
                conn.rollback()
                if _is_retryable(exc) and attempt < max_attempts - 1:
                    LOG.warning("再試行 attempt=%d/%d err=%s", attempt + 1, max_attempts, exc)
                    _retry_sleep(attempt, args.retry_base_sec, args.retry_cap_sec)
                    attempt += 1
                    continue
                LOG.exception("バッチ失敗（打ち切り）: %s", exc)
                if rows:
                    last_id = str(rows[-1]["id"])
                    stats.skipped_batches_fatal += 1
                    if pbar is not None:
                        pbar.update(len(rows))
                else:
                    time.sleep(5.0)
                attempt += 1
                break

        if exhausted:
            break

    if pbar is not None:
        pbar.close()

    LOG.info("=== 完了 ===")
    LOG.info("処理対象行数（取得ベース）: %d", stats.selected_rows)
    LOG.info("更新行数: %d", stats.updated_rows)
    LOG.info("致命スキップバッチ数: %d", stats.skipped_batches_fatal)
    for k in sorted(stats.by_reason.keys(), key=lambda x: (-stats.by_reason[x], x)):
        LOG.info("  %-44s %d", k, stats.by_reason[k])
    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="companies 残存インテグリティ修復パッチ")
    ap.add_argument("--dry-run", action="store_true", help="先頭N件の変更内容プレビューのみ")
    ap.add_argument("--preview-limit", type=int, default=10, help="--dry-run 件数")
    ap.add_argument("--batch-size", type=int, default=5000, help="1トランザクションの行数")
    ap.add_argument("--lock-timeout-ms", type=int, default=5000, help="SET LOCAL lock_timeout")
    ap.add_argument("--statement-timeout-ms", type=int, default=0, help="SET LOCAL statement_timeout (0=無効)")
    ap.add_argument("--max-retries", type=int, default=12, help="リトライ上限")
    ap.add_argument("--retry-base-sec", type=float, default=0.5, help="指数バックオフ初期秒")
    ap.add_argument("--retry-cap-sec", type=float, default=30.0, help="指数バックオフ上限秒")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    rc = 1
    try:
        conn = rep.get_connection()
        try:
            if args.dry_run:
                rc = run_preview(conn, args.preview_limit)
            else:
                rc = run_main(conn, args)
        finally:
            conn.close()
    except KeyboardInterrupt:
        LOG.warning("中断されました")
        rc = 130
    except Exception as exc:
        LOG.exception("致命的エラー: %s", exc)
        rc = 1
    sys.exit(rc)


if __name__ == "__main__":
    main()
