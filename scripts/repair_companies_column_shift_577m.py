#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
companies テーブルのカラムズレ（玉突き）を商用グレードで自動修復する。

修復対象パターン:
1) postal_code に漢字混入
   - 代表者らしき文字列 -> representative_name へ移送
   - 業種らしき文字列   -> industry_middle / industry_small / industry_detail へ移送
2) phone_number が XXX-XXXX（7桁ハイフン） -> postal_code へ移送し phone_number を NULL 化
3) revenue / profit が13桁数値 -> corporate_number（空なら補完）+ 汚染元を NULL 化
4) established が JSON 文字列（{"企業サマリ": ...}） -> 企業サマリ.kv.設立 を抽出して再格納

実行要件:
- 5000件単位コミット
- FOR UPDATE SKIP LOCKED
- Deadlock/OperationalError は指数バックオフで再試行
- tqdm による ETA + パターン別件数表示
- --dry-run で先頭10件の before/after を表示して終了
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
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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

LOG = logging.getLogger("repair_companies_column_shift_577m")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

KANJI_RE = re.compile(r"[一-龥々〆ヵヶ]")
POSTAL_RE = re.compile(r"^\d{3}-\d{4}$")
PHONE_JP_RE = re.compile(r"^0\d[\d\-]{7,}$")
CORP_13_RE = re.compile(r"^\d{13}$")
REP_HINT_RE = re.compile(
    r"(代表|取締役|社長|会長|専務|常務|執行役|CEO|CFO|氏|さん|様)$|^[一-龥々ぁ-んァ-ヶー・\s]{2,32}$"
)
INDUSTRY_HINT_RE = re.compile(
    r"(業|販売|製造|建設|工事|運送|運輸|物流|サービス|小売|卸売|不動産|広告|IT|情報|開発|人材|食品|医療|介護)"
)

_RETRIABLE = (OperationalError, InterfaceError)
try:
    _DEADLOCK = (pg_errors.DeadlockDetected,)
except Exception:
    _DEADLOCK = ()


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


def _is_empty(v: Any) -> bool:
    return v is None or (isinstance(v, str) and not v.strip())


def _norm_text(v: Any) -> str:
    return "" if v is None else str(v).strip()


def _is_representative_like(text: str) -> bool:
    s = text.strip()
    if not s:
        return False
    if any(ch.isdigit() for ch in s):
        return False
    return bool(REP_HINT_RE.search(s))


def _is_industry_like(text: str) -> bool:
    s = text.strip()
    if not s:
        return False
    if len(s) > 120:
        return False
    return bool(INDUSTRY_HINT_RE.search(s))


def _normalize_established(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    if not s:
        return None

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    if re.fullmatch(r"\d{4}/\d{1,2}/\d{1,2}", s):
        dt = datetime.strptime(s, "%Y/%m/%d")
        return dt.strftime("%Y-%m-%d")
    m = re.fullmatch(r"(\d{4})年(\d{1,2})月(\d{1,2})日", s)
    if m:
        y, mo, d = map(int, m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"
    m = re.fullmatch(r"(\d{4})年(\d{1,2})月", s)
    if m:
        y, mo = map(int, m.groups())
        return f"{y:04d}年{mo:02d}月"
    return s


def _extract_established_from_json(raw: str) -> Optional[str]:
    s = (raw or "").strip()
    if not s.startswith('{"企業サマリ"'):
        return None
    try:
        data = json.loads(s)
    except json.JSONDecodeError:
        return None
    value = (
        data.get("企業サマリ", {})
        .get("kv", {})
        .get("設立")
    )
    if value is None:
        return None
    return _normalize_established(str(value))


def _sql_dirty_predicate() -> str:
    return """
    (
        (postal_code IS NOT NULL AND BTRIM(postal_code::text) ~ '[一-龥々〆ヵヶ]')
        OR (phone_number IS NOT NULL AND BTRIM(phone_number::text) ~ '^\\d{3}-\\d{4}$')
        OR (revenue IS NOT NULL AND BTRIM(revenue::text) ~ '^\\d{13}$')
        OR (profit IS NOT NULL AND BTRIM(profit::text) ~ '^\\d{13}$')
        OR (established IS NOT NULL AND BTRIM(established::text) LIKE '{"企業サマリ"%%')
    )
    """


@dataclass
class RowPatch:
    id: str
    set_postal_code: bool = False
    postal_code: Optional[str] = None
    set_phone_number: bool = False
    phone_number: Optional[str] = None
    set_representative_name: bool = False
    representative_name: Optional[str] = None
    set_industry_middle: bool = False
    industry_middle: Optional[str] = None
    set_industry_small: bool = False
    industry_small: Optional[str] = None
    set_industry_detail: bool = False
    industry_detail: Optional[str] = None
    set_corporate_number: bool = False
    corporate_number: Optional[str] = None
    set_revenue: bool = False
    revenue: Optional[str] = None
    set_profit: bool = False
    profit: Optional[str] = None
    set_capital_stock: bool = False
    capital_stock: Optional[str] = None
    set_established: bool = False
    established: Optional[str] = None
    reasons: List[str] = field(default_factory=list)

    def is_empty(self) -> bool:
        return not any(
            [
                self.set_postal_code,
                self.set_phone_number,
                self.set_representative_name,
                self.set_industry_middle,
                self.set_industry_small,
                self.set_industry_detail,
                self.set_corporate_number,
                self.set_revenue,
                self.set_profit,
                self.set_capital_stock,
                self.set_established,
            ]
        )


@dataclass
class RunStats:
    selected_rows: int = 0
    updated_rows: int = 0
    skipped_batches_fatal: int = 0
    by_reason: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def add_reason(self, reason: str) -> None:
        self.by_reason[reason] += 1


def _move_industry_slot(
    patch: RowPatch,
    row: Dict[str, Any],
    value: str,
) -> bool:
    if _is_empty(row.get("industry_middle")):
        patch.set_industry_middle = True
        patch.industry_middle = value
        return True
    if _is_empty(row.get("industry_small")):
        patch.set_industry_small = True
        patch.industry_small = value
        return True
    if _is_empty(row.get("industry_detail")):
        patch.set_industry_detail = True
        patch.industry_detail = value
        return True
    return False


def build_patch_for_row(row: Dict[str, Any], stats: RunStats) -> RowPatch:
    patch = RowPatch(id=str(row["id"]))

    postal_raw = _norm_text(row.get("postal_code"))
    if postal_raw and KANJI_RE.search(postal_raw):
        if _is_representative_like(postal_raw):
            patch.set_postal_code = True
            patch.postal_code = None
            if _is_empty(row.get("representative_name")):
                patch.set_representative_name = True
                patch.representative_name = postal_raw
                patch.reasons.append("postal_kanji_to_representative")
                stats.add_reason("postal_kanji_to_representative")
            else:
                patch.reasons.append("postal_kanji_representative_target_not_empty")
                stats.add_reason("postal_kanji_representative_target_not_empty")
        elif _is_industry_like(postal_raw):
            moved = _move_industry_slot(patch, row, postal_raw)
            patch.set_postal_code = True
            patch.postal_code = None
            if moved:
                patch.reasons.append("postal_kanji_to_industry")
                stats.add_reason("postal_kanji_to_industry")
            else:
                patch.reasons.append("postal_kanji_industry_target_not_empty")
                stats.add_reason("postal_kanji_industry_target_not_empty")
        else:
            patch.reasons.append("postal_kanji_unclassified")
            stats.add_reason("postal_kanji_unclassified")

    phone_raw = _norm_text(row.get("phone_number"))
    if phone_raw and POSTAL_RE.fullmatch(phone_raw):
        if _is_empty(row.get("postal_code")) or not POSTAL_RE.fullmatch(_norm_text(row.get("postal_code"))):
            patch.set_postal_code = True
            patch.postal_code = phone_raw
            patch.reasons.append("phone_7digits_to_postal")
            stats.add_reason("phone_7digits_to_postal")
        patch.set_phone_number = True
        patch.phone_number = None
        patch.reasons.append("phone_7digits_clear_phone")
        stats.add_reason("phone_7digits_clear_phone")

    corporate_raw = _norm_text(row.get("corporate_number"))
    corp_candidate: Optional[str] = None
    revenue_raw = _norm_text(row.get("revenue"))
    profit_raw = _norm_text(row.get("profit"))

    if revenue_raw and CORP_13_RE.fullmatch(revenue_raw):
        corp_candidate = corp_candidate or revenue_raw
        patch.set_revenue = True
        patch.revenue = None
        patch.reasons.append("revenue_13digits_clear")
        stats.add_reason("revenue_13digits_clear")
    if profit_raw and CORP_13_RE.fullmatch(profit_raw):
        corp_candidate = corp_candidate or profit_raw
        patch.set_profit = True
        patch.profit = None
        patch.reasons.append("profit_13digits_clear")
        stats.add_reason("profit_13digits_clear")

    if corp_candidate and not corporate_raw:
        patch.set_corporate_number = True
        patch.corporate_number = corp_candidate
        patch.reasons.append("corporate_number_from_financial")
        stats.add_reason("corporate_number_from_financial")

    established_raw = _norm_text(row.get("established"))
    if established_raw.startswith('{"企業サマリ"'):
        parsed = _extract_established_from_json(established_raw)
        if parsed:
            patch.set_established = True
            patch.established = parsed
            patch.reasons.append("established_from_json_summary")
            stats.add_reason("established_from_json_summary")
        else:
            patch.reasons.append("established_json_parse_failed")
            stats.add_reason("established_json_parse_failed")

    return patch


def _fetch_batch(cur, last_id: str, batch_size: int) -> List[Dict[str, Any]]:
    cur.execute(
        f"""
        SELECT
            id, postal_code, representative_name,
            industry_middle, industry_small, industry_detail,
            phone_number, revenue, profit, capital_stock, corporate_number, established
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
            p.set_postal_code, p.postal_code,
            p.set_phone_number, p.phone_number,
            p.set_representative_name, p.representative_name,
            p.set_industry_middle, p.industry_middle,
            p.set_industry_small, p.industry_small,
            p.set_industry_detail, p.industry_detail,
            p.set_corporate_number, p.corporate_number,
            p.set_revenue, p.revenue,
            p.set_profit, p.profit,
            p.set_capital_stock, p.capital_stock,
            p.set_established, p.established,
        )
        for p in patches
    ]

    execute_values(
        cur,
        """
        UPDATE companies AS c
        SET
            postal_code = CASE WHEN v.set_postal_code THEN v.postal_code ELSE c.postal_code END,
            phone_number = CASE WHEN v.set_phone_number THEN v.phone_number ELSE c.phone_number END,
            representative_name = CASE WHEN v.set_representative_name THEN v.representative_name ELSE c.representative_name END,
            industry_middle = CASE WHEN v.set_industry_middle THEN v.industry_middle ELSE c.industry_middle END,
            industry_small = CASE WHEN v.set_industry_small THEN v.industry_small ELSE c.industry_small END,
            industry_detail = CASE WHEN v.set_industry_detail THEN v.industry_detail ELSE c.industry_detail END,
            corporate_number = CASE WHEN v.set_corporate_number THEN v.corporate_number ELSE c.corporate_number END,
            revenue = CASE WHEN v.set_revenue THEN v.revenue::bigint ELSE c.revenue END,
            profit = CASE WHEN v.set_profit THEN v.profit::bigint ELSE c.profit END,
            capital_stock = CASE WHEN v.set_capital THEN v.capital_stock::bigint ELSE c.capital_stock END,
            established = CASE WHEN v.set_established THEN v.established ELSE c.established END
        FROM (
            VALUES %s
        ) AS v(
            id,
            set_postal_code, postal_code,
            set_phone_number, phone_number,
            set_representative_name, representative_name,
            set_industry_middle, industry_middle,
            set_industry_small, industry_small,
            set_industry_detail, industry_detail,
            set_corporate_number, corporate_number,
            set_revenue, revenue,
            set_profit, profit,
            set_capital, capital_stock,
            set_established, established
        )
        WHERE c.id = v.id
        """,
        payload,
        template="""(
            %s::text,
            %s::boolean, %s::text,
            %s::boolean, %s::text,
            %s::boolean, %s::text,
            %s::boolean, %s::text,
            %s::boolean, %s::text,
            %s::boolean, %s::text,
            %s::boolean, %s::text,
            %s::boolean, %s::bigint,
            %s::boolean, %s::bigint,
            %s::boolean, %s::bigint,
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
            id, postal_code, representative_name,
            industry_middle, industry_small, industry_detail,
            phone_number, revenue, profit, capital_stock, corporate_number, established
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

    LOG.info("--- dry-run プレビュー（最大 %d 件） ---", limit)
    preview_stats = RunStats()
    for row in rows:
        patch = build_patch_for_row(row, preview_stats)
        if patch.is_empty():
            continue
        after = dict(row)
        if patch.set_postal_code:
            after["postal_code"] = patch.postal_code
        if patch.set_phone_number:
            after["phone_number"] = patch.phone_number
        if patch.set_representative_name:
            after["representative_name"] = patch.representative_name
        if patch.set_industry_middle:
            after["industry_middle"] = patch.industry_middle
        if patch.set_industry_small:
            after["industry_small"] = patch.industry_small
        if patch.set_industry_detail:
            after["industry_detail"] = patch.industry_detail
        if patch.set_corporate_number:
            after["corporate_number"] = patch.corporate_number
        if patch.set_revenue:
            after["revenue"] = patch.revenue
        if patch.set_profit:
            after["profit"] = patch.profit
        if patch.set_capital_stock:
            after["capital_stock"] = patch.capital_stock
        if patch.set_established:
            after["established"] = patch.established

        LOG.info("id=%s reasons=%s", row["id"], ",".join(patch.reasons))
        LOG.info("  before=%s", json.dumps(row, ensure_ascii=False, default=str))
        LOG.info("  after =%s", json.dumps(after, ensure_ascii=False, default=str))

    for k in sorted(preview_stats.by_reason.keys(), key=lambda x: (-preview_stats.by_reason[x], x)):
        LOG.info("reason %-36s %d", k, preview_stats.by_reason[k])
    return 0


def run_main(conn, args: argparse.Namespace) -> int:
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

    pbar = tqdm.tqdm(total=total_dirty, desc="column-shift-repair", unit="row") if (tqdm is not None and total_dirty > 0) else None

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
                    patch = build_patch_for_row(row, stats)
                    if not patch.is_empty():
                        patches.append(patch)

                if patches:
                    updated = _apply_updates(cur, patches)
                    stats.updated_rows += updated

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
                        "processed=%d updated=%d last_id=%s batch_rows=%d batch_patches=%d",
                        stats.selected_rows,
                        stats.updated_rows,
                        last_id[:36],
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
                    LOG.warning("再試行します attempt=%d/%d err=%s", attempt + 1, max_attempts, exc)
                    _retry_sleep(attempt, args.retry_base_sec, args.retry_cap_sec)
                    attempt += 1
                    continue

                LOG.exception("バッチ失敗（打ち切り）: %s", exc)
                if rows:
                    last_id = str(rows[-1]["id"])
                    stats.skipped_batches_fatal += 1
                    stats.selected_rows += len(rows)
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
        LOG.info("  %-36s %d", k, stats.by_reason[k])
    return 0


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="companies カラムズレ修復（577万件対応）")
    ap.add_argument("--dry-run", action="store_true", help="先頭 N 件の before/after 比較のみ表示して終了")
    ap.add_argument("--preview-limit", type=int, default=10, help="--dry-run 時に比較表示する件数")
    ap.add_argument("--batch-size", type=int, default=5000, help="1コミットあたりの行数")
    ap.add_argument("--lock-timeout-ms", type=int, default=5000, help="SET LOCAL lock_timeout")
    ap.add_argument("--statement-timeout-ms", type=int, default=0, help="SET LOCAL statement_timeout（0で無効）")
    ap.add_argument("--max-retries", type=int, default=12, help="リトライ最大回数")
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
