"""
main.py - バッチオーケストレーション（エントリポイント）

仕様書 10章に準拠した設計:
- デフォルトは PostgreSQL `companies` からキーセットページネーションで取得
- 企業単位パイプライン（Step1→7）を ThreadPoolExecutor で並列実行
- SQLite state_manager による中断再開（レジューム）対応
- 差分CSV出力 + オプションで DB UPSERT（--write-db）
- --input 指定時は従来どおり CSV フォールバック

Usage:
  # DB直読み（推奨）: パイロット 10件
  python -m enrichment.main --limit 10 --dry-run --debug

  # 本番バッチ（情報不足企業優先）
  python -m enrichment.main --workers 4 --incomplete-only

  # 差分CSVに加え DB へ条件付き UPSERT
  python -m enrichment.main --write-db --limit 1000

  # CSVフォールバック
  python -m enrichment.main --input data/companies_rows.csv --limit 100
"""
from __future__ import annotations

import argparse
import csv
import logging
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Set

# ─────────────────────────────────────────────
# ロギング設定（最初に設定）
# ─────────────────────────────────────────────

def _setup_logging(log_dir: Path, debug: bool = False) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"enrichment_{datetime.now().strftime('%Y%m%d')}.log"
    level = logging.DEBUG if debug else logging.INFO
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
    for noisy in ("urllib3", "requests", "charset_normalizer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 内部モジュール
# ─────────────────────────────────────────────

from .config import (
    OUTPUT_DIR, LOGS_DIR, DB_PATH,
    WORKERS, FETCH_BATCH_SIZE, CSV_FLUSH_ROWS,
    PROCESSING_STALL_SEC, PG_CHUNK_SIZE,
)
from .state_manager import StateManager
from .url_finder import find_hp_candidates
from .url_validator import find_best_url
from .scraper import scrape_company
from .normalizer import normalize_scraped_data
from .csv_exporter import (
    CSVExporter, build_enrichment_row, has_meaningful_data,
    flush_all_logs,
)
from .db_loader import iter_company_chunks, probe_connection
from .db_writer import DbWriter

# ─────────────────────────────────────────────
# グローバルシャットダウンフラグ
# ─────────────────────────────────────────────
_shutdown = False


def _handle_signal(signum, frame) -> None:
    global _shutdown
    logger.warning(f"Signal {signum} received. Shutting down gracefully...")
    _shutdown = True


# ─────────────────────────────────────────────
# CSV入力ローダー（--input フォールバック用）
# ─────────────────────────────────────────────

_ID_COLUMN_CANDIDATES: tuple[str, ...] = (
    "id", "Id", "ID", "company_id", "companyId", "document_id", "doc_id",
)
_SCHEMA_HEADER_MARKERS: frozenset[str] = frozenset({
    "テーブル名", "カラム名", "データ型", "table_name", "column_name", "data_type",
})


def _normalize_fieldnames(fieldnames: Optional[List[str]]) -> List[str]:
    if not fieldnames:
        return []
    return [(name or "").lstrip("\ufeff").strip() for name in fieldnames]


def _detect_id_column(fieldnames: List[str]) -> Optional[str]:
    normalized = {f.lower(): f for f in fieldnames if f}
    for candidate in _ID_COLUMN_CANDIDATES:
        if candidate.lower() in normalized:
            return normalized[candidate.lower()]
    return None


def _is_schema_definition_csv(fieldnames: List[str]) -> bool:
    return len(set(fieldnames) & _SCHEMA_HEADER_MARKERS) >= 2


def _normalize_company_row(row: Dict, id_col: str) -> Dict:
    cleaned: Dict = {}
    for k, v in row.items():
        if k is None:
            continue
        key = str(k).lstrip("\ufeff").strip()
        cleaned[key] = v if v is not None else ""
    cleaned["id"] = str(cleaned.get(id_col, "")).strip()
    if not str(cleaned.get("company_name", "")).strip() and cleaned.get("name"):
        cleaned["company_name"] = str(cleaned["name"]).strip()
    return cleaned


def _open_csv_reader(csv_path: Path):
    encodings = ("utf-8-sig", "utf-8", "cp932")
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            f = open(csv_path, "r", encoding=enc, newline="")
            reader = csv.DictReader(f)
            fields = _normalize_fieldnames(list(reader.fieldnames or []))
            if not fields:
                f.close()
                raise ValueError(f"CSV header is empty (encoding={enc})")
            reader.fieldnames = fields
            return f, reader, enc, fields
        except Exception as e:
            last_err = e
            try:
                f.close()  # type: ignore[name-defined]
            except Exception:
                pass
    raise ValueError(f"Failed to open CSV {csv_path}: {last_err}")


def _load_csv_rows(csv_path: Path, limit: Optional[int] = None) -> Iterator[Dict]:
    fh, reader, enc, fields = _open_csv_reader(csv_path)
    logger.info(f"[CSV] Opened {csv_path.name} with encoding={enc}")
    logger.info(f"[CSV] Headers ({len(fields)} cols): {fields[:12]}{'...' if len(fields) > 12 else ''}")

    if _is_schema_definition_csv(fields):
        fh.close()
        raise ValueError(
            f"入力ファイルは企業データではなく「カラム定義（スキーマ）CSV」です: {csv_path}\n"
            f"  → --input を外して DB 直読みにするか、companies 行データCSVを指定してください。"
        )

    id_col = _detect_id_column(fields)
    if id_col is None:
        fh.close()
        raise ValueError(f"CSVに id カラムが見つかりません: {fields[:30]}")
    logger.info(f"[CSV] Using id column: '{id_col}'")

    skipped = 0
    yielded = 0
    try:
        for row_num, raw_row in enumerate(reader, start=2):
            if limit is not None and yielded >= limit:
                break
            if raw_row is None or all(
                (v is None or str(v).strip() == "") for v in raw_row.values()
            ):
                logger.debug(f"[CSV] Row {row_num} skipped: empty row")
                skipped += 1
                continue
            row = _normalize_company_row(dict(raw_row), id_col)
            if not row.get("id"):
                logger.debug(f"[CSV] Row {row_num} skipped: missing '{id_col}'")
                skipped += 1
                continue
            yielded += 1
            yield row
    finally:
        fh.close()
    logger.info(f"[CSV] Parsed done: yielded={yielded}, skipped={skipped}")


def _iter_csv_chunks(
    csv_path: Path,
    chunk_size: int,
    limit: Optional[int] = None,
) -> Iterator[List[Dict]]:
    """CSVをチャンク単位で yield（DB経路とインターフェースを揃える）"""
    buf: List[Dict] = []
    total = 0
    for row in _load_csv_rows(csv_path, limit=None):
        if limit is not None and total >= limit:
            break
        buf.append(row)
        total += 1
        if len(buf) >= chunk_size:
            yield buf
            buf = []
            if limit is not None and total >= limit:
                break
    if buf and (limit is None or total <= limit):
        if limit is not None and total > limit:
            # 既に break 済みなのでここには来ない想定
            pass
        yield buf


# ─────────────────────────────────────────────
# 企業1件のパイプライン
# ─────────────────────────────────────────────

def process_company(
    company_row: Dict,
    state: StateManager,
    exporter: CSVExporter,
    used_urls: Set[str],
    dry_run: bool = False,
    write_db: bool = False,
    db_writer: Optional[DbWriter] = None,
    not_found_buf: Optional[List[Dict]] = None,
    low_conf_buf: Optional[List[Dict]] = None,
    error_buf: Optional[List[Dict]] = None,
) -> str:
    """企業1件の完全パイプライン。Returns: done|not_found|low_confidence|error|skip"""
    company_id = str(company_row.get("id", "?"))
    company_name = str(company_row.get("company_name", company_row.get("name", "")))

    if state.is_completed(company_id):
        return "skip"

    state.mark_processing(company_id)

    try:
        if dry_run:
            logger.info(f"[{company_id}] [DRY-RUN] Step1–6 skip")
            state.mark_done(company_id, "dry-run://", "high", "dry_run")
            return "done"

        candidates, source_method, gbiz_info = find_hp_candidates(company_row)

        if not candidates:
            reason = "no_candidates_found"
            logger.info(f"[{company_id}] not_found: {reason}")
            state.mark_not_found(company_id, reason)
            if not_found_buf is not None:
                not_found_buf.append({
                    "id": company_id,
                    "corporate_number": company_row.get("corporate_number", ""),
                    "company_name": company_name,
                    "reason": reason,
                })
            return "not_found"

        best_url, confidence, score_reason = find_best_url(
            candidates, company_row, gbiz_info, used_urls
        )
        logger.info(f"[{company_id}] Step2 → url={best_url} confidence={confidence}")

        if confidence == "not_found" or best_url is None:
            state.mark_not_found(company_id, f"score_too_low: {score_reason}")
            if not_found_buf is not None:
                not_found_buf.append({
                    "id": company_id,
                    "corporate_number": company_row.get("corporate_number", ""),
                    "company_name": company_name,
                    "reason": f"score_too_low: {score_reason}",
                })
            return "not_found"

        if confidence == "low":
            logger.info(f"[{company_id}] low_confidence → review queue")
            state.mark_low_confidence(company_id, best_url, confidence, score_reason)
            if low_conf_buf is not None:
                low_conf_buf.append({
                    "id": company_id,
                    "corporate_number": company_row.get("corporate_number", ""),
                    "company_name": company_name,
                    "hp_url_candidate": best_url,
                    "confidence": confidence,
                    "score_reason": score_reason,
                })
            return "low_confidence"

        used_urls.add(best_url)

        raw_scraped = scrape_company(best_url, company_id)
        normalized = normalize_scraped_data(raw_scraped)
        enrichment_row = build_enrichment_row(
            company_row, normalized, best_url, confidence, source_method
        )

        state.mark_done(company_id, best_url, confidence, source_method)
        state.log_event(
            company_id, "done",
            f"fields_filled={enrichment_row.get('fields_filled','')} url={best_url}"
        )

        if has_meaningful_data(enrichment_row):
            exporter.add_row(enrichment_row)
            if write_db and db_writer is not None:
                db_writer.upsert_enrichment_row(enrichment_row)

        fields_count = (
            len(enrichment_row.get("fields_filled", "").split(","))
            if enrichment_row.get("fields_filled") else 0
        )
        logger.info(
            f"[{company_id}] ✓ done | url={best_url} | "
            f"confidence={confidence} | fields_filled={fields_count}"
        )
        return "done"

    except KeyboardInterrupt:
        raise
    except Exception as e:
        err_msg = f"{type(e).__name__}: {e}"
        logger.error(f"[{company_id}] error: {err_msg}", exc_info=True)
        state.mark_error(company_id, err_msg)
        if error_buf is not None:
            error_buf.append({
                "id": company_id,
                "corporate_number": company_row.get("corporate_number", ""),
                "company_name": company_name,
                "error_type": type(e).__name__,
                "error_message": str(e)[:500],
                "retry_count": 0,
            })
        return "error"


# ─────────────────────────────────────────────
# オーケストレーション
# ─────────────────────────────────────────────

def run_enrichment(
    output_dir: Path = OUTPUT_DIR,
    db_path: Path = DB_PATH,
    workers: int = WORKERS,
    limit: Optional[int] = None,
    dry_run: bool = False,
    log_flush_interval: int = 500,
    chunk_size: int = PG_CHUNK_SIZE,
    incomplete_only: bool = False,
    write_db: bool = False,
    csv_path: Optional[Path] = None,
) -> None:
    """
    メインのバッチ処理ループ。

    1. PostgreSQL（または CSV）からチャンク取得
    2. state_manager で完了済みをスキップし、未処理を pending 登録
    3. ThreadPoolExecutor で並列エンリッチ
    4. 差分CSV出力（+ オプションで DB UPSERT）
    """
    source = f"CSV:{csv_path}" if csv_path else "PostgreSQL:companies"
    logger.info("=" * 60)
    logger.info("Legatus ONE Company Enrichment Batch - Start")
    logger.info(f"  source:           {source}")
    logger.info(f"  output:           {output_dir}")
    logger.info(f"  state_db:         {db_path}")
    logger.info(f"  workers:          {workers}")
    logger.info(f"  chunk_size:       {chunk_size}")
    logger.info(f"  limit:            {limit or 'ALL'}")
    logger.info(f"  incomplete_only:  {incomplete_only}")
    logger.info(f"  write_db:         {write_db}")
    logger.info(f"  dry_run:          {dry_run}")
    logger.info("=" * 60)

    # DB接続ヘルスチェック（CSVモード以外）
    if csv_path is None:
        ok, msg = probe_connection()
        if not ok:
            logger.error(
                f"PostgreSQL に接続できません: {msg}\n"
                f"  .env の DATABASE_* / DATABASE_URL を確認し、DBが起動しているか確認してください。\n"
                f"  CSVフォールバック: --input path/to/companies_rows.csv"
            )
            raise SystemExit(2)
        logger.info(f"[db_loader] Connection probe: {msg}")

    state = StateManager(db_path, stall_sec=PROCESSING_STALL_SEC)
    exporter = CSVExporter(output_dir=output_dir, flush_rows=CSV_FLUSH_ROWS)
    db_writer: Optional[DbWriter] = DbWriter() if write_db and not dry_run else None

    used_urls: Set[str] = set()
    counts = {"done": 0, "not_found": 0, "low_confidence": 0, "error": 0, "skip": 0}
    total_fields_filled = 0
    not_found_buf: List[Dict] = []
    low_conf_buf: List[Dict] = []
    error_buf: List[Dict] = []
    processed = 0
    batch_start_time = time.monotonic()

    # チャンクソース選択
    if csv_path is not None:
        try:
            chunk_iter = _iter_csv_chunks(csv_path, chunk_size, limit)
        except ValueError as e:
            logger.error(f"CSV load aborted: {e}")
            raise SystemExit(2) from e
    else:
        chunk_iter = iter_company_chunks(
            chunk_size=chunk_size,
            limit=limit,
            incomplete_only=incomplete_only,
        )

    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="enricher") as pool:
        for chunk in chunk_iter:
            if _shutdown:
                break

            # 完了済みスキップ + pending 登録
            ids = [r["id"] for r in chunk]
            processable_ids = state.filter_processable(ids)
            if not processable_ids:
                logger.info(
                    f"[chunk] All {len(ids)} already terminal — skip chunk"
                )
                continue

            state.bulk_register_pending(processable_ids)
            processable_set = set(processable_ids)
            rows = [r for r in chunk if r["id"] in processable_set]

            if limit is not None:
                remain = limit - processed
                if remain <= 0:
                    break
                rows = rows[:remain]

            logger.info(
                f"[chunk] Processing {len(rows)} companies "
                f"(chunk_raw={len(chunk)}, skipped_terminal={len(ids)-len(processable_ids)})"
            )

            futures = {
                pool.submit(
                    process_company,
                    row, state, exporter, used_urls, dry_run,
                    write_db, db_writer,
                    not_found_buf, low_conf_buf, error_buf,
                ): row["id"]
                for row in rows
            }

            for future in as_completed(futures):
                if _shutdown:
                    break
                cid = futures[future]
                try:
                    result = future.result()
                    counts[result] = counts.get(result, 0) + 1
                except Exception as e:
                    logger.error(f"[{cid}] Unhandled future exception: {e}", exc_info=True)
                    counts["error"] += 1

                processed += 1

                if processed % 100 == 0:
                    elapsed = time.monotonic() - batch_start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    logger.info(
                        f"[Progress] processed={processed} | "
                        f"done={counts['done']} not_found={counts['not_found']} "
                        f"low={counts['low_confidence']} error={counts['error']} "
                        f"skip={counts['skip']} | rate={rate:.1f}/s"
                    )
                    logger.info(f"[StateDB] {state.get_stats()}")

                if processed % log_flush_interval == 0:
                    flush_all_logs(output_dir, not_found_buf, low_conf_buf, error_buf)
                    not_found_buf.clear()
                    low_conf_buf.clear()
                    error_buf.clear()

            if limit is not None and processed >= limit:
                logger.info(f"Reached --limit={limit}")
                break

    # 終了処理
    logger.info("Finalizing outputs...")
    exporter.flush()
    flush_all_logs(output_dir, not_found_buf, low_conf_buf, error_buf)

    if db_writer is not None:
        logger.info(f"[db_writer] stats: {db_writer.flush_stats()}")
        db_writer.close()

    final_stats = state.get_stats()
    total_done = final_stats.get("done", 0)
    total_nf = final_stats.get("not_found", 0) + final_stats.get("low_confidence", 0)

    manifest = exporter.finalize_manifest(
        total_processed=processed,
        total_hp_found=total_done,
        total_not_found=total_nf,
        total_fields_filled=total_fields_filled,
    )

    logger.info("=" * 60)
    logger.info("Batch Complete")
    logger.info(f"  processed:      {processed}")
    logger.info(f"  done:           {counts['done']}")
    logger.info(f"  not_found:      {counts['not_found']}")
    logger.info(f"  low_confidence: {counts['low_confidence']}")
    logger.info(f"  error:          {counts['error']}")
    logger.info(f"  skip:           {counts['skip']}")
    logger.info(f"  manifest:       {manifest}")
    logger.info(f"  final StateDB:  {final_stats}")
    logger.info("=" * 60)
    _post_run_check(output_dir, state)


def _post_run_check(output_dir: Path, state: StateManager) -> None:
    logger.info("[Step7] Post-run integrity check...")
    stats = state.get_stats()
    remaining_pending = stats.get("pending", 0)
    remaining_error = stats.get("error", 0)

    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        import json
        with open(manifest_path) as f:
            m = json.load(f)
        logger.info(f"[Step7] manifest total_written={m.get('total_written', 0)}")

    if remaining_pending > 0:
        logger.warning(
            f"[Step7] {remaining_pending} still PENDING → re-run to resume"
        )
    if remaining_error > 0:
        logger.warning(
            f"[Step7] {remaining_error} in ERROR → check error_log.csv"
        )
    if remaining_pending == 0 and remaining_error == 0:
        logger.info("[Step7] All tracked companies processed. ✓")


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Legatus ONE Company Enrichment Batch (PostgreSQL / CSV)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input", "-i", type=Path, default=None,
        help="任意: CSVフォールバック入力。未指定時は PostgreSQL companies を直読み",
    )
    parser.add_argument(
        "--output", "-o", type=Path, default=OUTPUT_DIR,
        help="差分CSV出力ディレクトリ",
    )
    parser.add_argument(
        "--db", type=Path, default=DB_PATH,
        help="進捗管理SQLiteファイルパス",
    )
    parser.add_argument(
        "--workers", "-w", type=int, default=WORKERS,
        help="並列ワーカー数",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=PG_CHUNK_SIZE,
        help="PostgreSQL/CSV から1回に取得する件数",
    )
    parser.add_argument(
        "--limit", "-l", type=int, default=None,
        help="処理件数上限（パイロット用）",
    )
    parser.add_argument(
        "--incomplete-only", action="store_true",
        help="URL/電話/住所/メールが不足している企業のみ取得（DBモード）",
    )
    parser.add_argument(
        "--write-db", action="store_true",
        help="差分CSVに加え、空欄カラムのみ PostgreSQL へ条件付き UPDATE",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="HTTPアクセスなしでパイプラインをテスト実行",
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="デバッグログを出力",
    )
    parser.add_argument(
        "--log-flush-interval", type=int, default=500,
        help="ログCSVをフラッシュする間隔（処理件数）",
    )
    return parser.parse_args()


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).parent.parent / ".env"
        if env_path.exists():
            load_dotenv(env_path)
            logger.info(f".env loaded from {env_path}")
    except ImportError:
        pass


def main() -> None:
    args = _parse_args()
    _setup_logging(LOGS_DIR, debug=args.debug)
    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)
    _load_dotenv()

    if args.input is not None and not args.input.exists():
        logger.error(f"Input CSV not found: {args.input}")
        sys.exit(1)

    if args.write_db and args.dry_run:
        logger.warning("--write-db is ignored in --dry-run mode")

    run_enrichment(
        output_dir=args.output,
        db_path=args.db,
        workers=args.workers,
        limit=args.limit,
        dry_run=args.dry_run,
        log_flush_interval=args.log_flush_interval,
        chunk_size=args.chunk_size,
        incomplete_only=args.incomplete_only,
        write_db=args.write_db,
        csv_path=args.input,
    )


if __name__ == "__main__":
    main()
