"""
csv_exporter.py - 差分CSVおよびmanifest.json・各種ログの出力管理（Step6）

仕様書 9章:
- 固定スキーマ差分CSV（100,000件ごとに分割）
- manifest.json（生成日時・件数・ID範囲）
- not_found_log.csv
- low_confidence_review.csv
- error_log.csv

- 文字コード: UTF-8 BOM付き（Excelで開ける）
- 改行: LF、RFC4180準拠
- 命名: companies_enrichment_part_{4桁連番}.csv
"""
from __future__ import annotations

import csv
import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import OUTPUT_SCHEMA, CSV_FLUSH_ROWS, OUTPUT_DIR

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 出力ディレクトリ初期化
# ─────────────────────────────────────────────

def ensure_output_dirs(output_dir: Path = OUTPUT_DIR) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"[csv_exporter] Output directory: {output_dir}")


# ─────────────────────────────────────────────
# スレッドセーフ CSVExporter
# ─────────────────────────────────────────────

class CSVExporter:
    """
    差分CSVの分割書き出しを管理するクラス。
    複数ワーカースレッドから安全に呼び出せる（Lock使用）。

    Usage:
        exporter = CSVExporter(output_dir)
        exporter.add_row(enriched_row_dict)
        ...
        exporter.flush()   # 残りを書き出し
        exporter.finalize_manifest(state_manager)
    """

    def __init__(
        self,
        output_dir: Path = OUTPUT_DIR,
        flush_rows: int = CSV_FLUSH_ROWS,
    ) -> None:
        self.output_dir = output_dir
        self.flush_rows = flush_rows
        self._lock = threading.Lock()
        self._buffer: List[Dict] = []
        self._part_index = 1
        self._total_written = 0
        self._part_meta: List[Dict] = []  # manifest用
        ensure_output_dirs(output_dir)
        # 既存パートファイルがあれば連番を進める
        self._part_index = self._detect_next_part()

    def _detect_next_part(self) -> int:
        """既存ファイルを確認して次の連番を返す"""
        idx = 1
        while (self.output_dir / f"companies_enrichment_part_{idx:04d}.csv").exists():
            idx += 1
        return idx

    def _part_path(self, idx: int) -> Path:
        return self.output_dir / f"companies_enrichment_part_{idx:04d}.csv"

    def add_row(self, row: Dict) -> None:
        """
        エンリッチ済み行を追加。flush_rows に達したら自動書き出し。
        row: OUTPUT_SCHEMA に対応した dict（不足キーは空文字補完）
        """
        with self._lock:
            self._buffer.append(row)
            if len(self._buffer) >= self.flush_rows:
                self._flush_locked()

    def flush(self) -> None:
        """バッファに残った行を書き出す"""
        with self._lock:
            if self._buffer:
                self._flush_locked()

    def _flush_locked(self) -> None:
        """ロックを保持した状態で呼ぶこと"""
        if not self._buffer:
            return
        path = self._part_path(self._part_index)
        rows = self._buffer
        self._buffer = []

        first_id = rows[0].get("id", "")
        last_id = rows[-1].get("id", "")

        _write_enrichment_csv(path, rows)
        logger.info(
            f"[csv_exporter] Wrote part {self._part_index:04d}: "
            f"{len(rows)} rows → {path.name}"
        )
        self._part_meta.append({
            "file": path.name,
            "row_count": len(rows),
            "id_range": [str(first_id), str(last_id)],
            "written_at": datetime.now(timezone.utc).isoformat(),
        })
        self._total_written += len(rows)
        self._part_index += 1

    def finalize_manifest(
        self,
        total_processed: int,
        total_hp_found: int,
        total_not_found: int,
        total_fields_filled: int,
    ) -> Path:
        """manifest.json を生成して書き出す"""
        manifest_path = self.output_dir / "manifest.json"
        manifest = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "parts": self._part_meta,
            "total_written": self._total_written,
            "total_processed": total_processed,
            "total_hp_found": total_hp_found,
            "total_not_found": total_not_found,
            "total_fields_filled": total_fields_filled,
        }
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        logger.info(f"[csv_exporter] manifest.json written: {manifest_path}")
        return manifest_path


# ─────────────────────────────────────────────
# CSV書き出し関数群
# ─────────────────────────────────────────────

def _write_enrichment_csv(path: Path, rows: List[Dict]) -> None:
    """
    差分CSVを書き出し。
    - UTF-8 BOM付き
    - 固定スキーマ（OUTPUT_SCHEMA）
    - 不足フィールドは空文字
    """
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=OUTPUT_SCHEMA,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL,
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            # 欠損フィールドを空文字で補完
            filled = {col: row.get(col, "") for col in OUTPUT_SCHEMA}
            writer.writerow(filled)


def write_not_found_log(
    output_dir: Path,
    rows: List[Dict],
    append: bool = True,
) -> Path:
    """
    not_found_log.csv を書き出し（or 追記）。
    columns: id, corporate_number, company_name, reason, logged_at
    """
    path = output_dir / "not_found_log.csv"
    fieldnames = ["id", "corporate_number", "company_name", "reason", "logged_at"]
    mode = "a" if append and path.exists() else "w"
    with open(path, mode, encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames,
            extrasaction="ignore",   # 余分なキーは無視
            quoting=csv.QUOTE_ALL, lineterminator="\n",
        )
        if mode == "w":
            writer.writeheader()
        for row in rows:
            writer.writerow({
                "id": row.get("id", ""),
                "corporate_number": row.get("corporate_number", ""),
                "company_name": row.get("company_name", row.get("name", "")),
                "reason": row.get("reason", ""),
                "logged_at": datetime.now(timezone.utc).isoformat(),
            })
    return path


def write_low_confidence_review(
    output_dir: Path,
    rows: List[Dict],
    append: bool = True,
) -> Path:
    """
    low_confidence_review.csv を書き出し（or 追記）。
    columns: id, corporate_number, company_name, hp_url_candidate, confidence, score_reason, logged_at
    """
    path = output_dir / "low_confidence_review.csv"
    fieldnames = [
        "id", "corporate_number", "company_name",
        "hp_url_candidate", "confidence", "score_reason", "logged_at",
    ]
    mode = "a" if append and path.exists() else "w"
    with open(path, mode, encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL, lineterminator="\n",
        )
        if mode == "w":
            writer.writeheader()
        for row in rows:
            writer.writerow({
                "id": row.get("id", ""),
                "corporate_number": row.get("corporate_number", ""),
                "company_name": row.get("company_name", row.get("name", "")),
                "hp_url_candidate": row.get("hp_url_candidate", ""),
                "confidence": row.get("confidence", ""),
                "score_reason": row.get("score_reason", ""),
                "logged_at": datetime.now(timezone.utc).isoformat(),
            })
    return path


def write_error_log(
    output_dir: Path,
    rows: List[Dict],
    append: bool = True,
) -> Path:
    """
    error_log.csv を書き出し（or 追記）。
    columns: id, corporate_number, company_name, error_type, error_message, retry_count, logged_at
    """
    path = output_dir / "error_log.csv"
    fieldnames = [
        "id", "corporate_number", "company_name",
        "error_type", "error_message", "retry_count", "logged_at",
    ]
    mode = "a" if append and path.exists() else "w"
    with open(path, mode, encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=fieldnames,
            extrasaction="ignore",
            quoting=csv.QUOTE_ALL, lineterminator="\n",
        )
        if mode == "w":
            writer.writeheader()
        for row in rows:
            writer.writerow({
                "id": row.get("id", ""),
                "corporate_number": row.get("corporate_number", ""),
                "company_name": row.get("company_name", row.get("name", "")),
                "error_type": row.get("error_type", ""),
                "error_message": str(row.get("error_message", ""))[:500],
                "retry_count": row.get("retry_count", 0),
                "logged_at": datetime.now(timezone.utc).isoformat(),
            })
    return path


# ─────────────────────────────────────────────
# エンリッチ結果 → 差分行の生成
# ─────────────────────────────────────────────

def build_enrichment_row(
    company_row: Dict,
    scraped: Dict,
    hp_url: str,
    confidence: str,
    enrichment_source: str,
) -> Dict:
    """
    元の会社行 + スクレイピング結果 → 差分出力行を構築。

    仕様書 8章: 既存値がある場合は上書きしない（空欄のみ補完）。
    仕様書 9章: fields_filled に埋まったカラム名を列挙。

    出力カラムの種別:
      A) 識別子パススルー: corporate_number, company_name, name, prefecture 等
         → 元行から無条件でコピー。fields_filled には含めない。
      B) エンリッチ対象: スクレイピング値で空欄のみ補完。
         → 埋まったカラム名を fields_filled に列挙。
      C) メタカラム: hp_url_used, hp_match_confidence 等
         → 処理ごとに付与。

    Returns:
        OUTPUT_SCHEMA に沿った差分行dict。
    """
    from .config import OUTPUT_SCHEMA

    # ─── A) 識別子パススルー列（DB再インポート時のマッチングキー）───
    PASSTHROUGH_COLS: frozenset = frozenset({
        "corporate_number", "company_name", "name", "prefecture",
    })

    # ─── C) メタカラム（書き込みループ対象外）───
    META_COLS: frozenset = frozenset({
        "id", "hp_url_used", "hp_match_confidence",
        "enrichment_source", "enriched_at", "fields_filled",
    })

    # id は必ず先頭
    out: Dict = {"id": str(company_row.get("id", ""))}
    fields_filled: List[str] = []

    # OUTPUT_SCHEMA の順で各列を処理
    for col in OUTPUT_SCHEMA:
        if col in META_COLS:
            continue

        raw_val = company_row.get(col)
        existing = str(raw_val).strip() if raw_val is not None else ""
        is_empty = existing in ("", "nan", "None", "null")

        if col in PASSTHROUGH_COLS:
            # A) パススルー: 元行の値を無条件コピー（空でも出力してOK）
            out[col] = existing if not is_empty else ""
            continue

        # B) エンリッチ対象
        if not is_empty:
            # 既存値あり → 上書きしない
            continue

        # スクレイピング結果に値があれば採用
        scraped_val = scraped.get(col)
        if scraped_val is None or str(scraped_val).strip() == "":
            continue

        out[col] = str(scraped_val).strip()
        fields_filled.append(col)

    # 役員情報の書き込み（_executives リスト）
    executives = scraped.get("_executives", [])
    if executives:
        from .column_mapping import get_executive_slots
        slots = get_executive_slots(company_row)
        for (name_col, pos_col), (name, pos) in zip(slots, executives):
            if name:
                out[name_col] = name
                fields_filled.append(name_col)
            if pos:
                out[pos_col] = pos
                fields_filled.append(pos_col)

    # C) メタカラム付与
    out["hp_url_used"] = hp_url
    out["hp_match_confidence"] = confidence
    out["enrichment_source"] = enrichment_source
    out["enriched_at"] = datetime.now(timezone.utc).isoformat()
    out["fields_filled"] = ",".join(fields_filled)

    return out


def has_meaningful_data(row: Dict) -> bool:
    """
    差分行にエンリッチされたデータがあるか判定（出力要否）。
    識別子パススルー・メタカラムはカウントしない。
    """
    NON_ENRICHMENT_COLS: frozenset = frozenset({
        "id", "corporate_number", "company_name", "name", "prefecture",
        "hp_url_used", "hp_match_confidence", "enrichment_source",
        "enriched_at", "fields_filled",
    })
    return any(
        v and str(v).strip() not in ("", "nan", "None", "null")
        for k, v in row.items()
        if k not in NON_ENRICHMENT_COLS
    )


# ─────────────────────────────────────────────
# 全ログ一括書き出し（バッチ終了時）
# ─────────────────────────────────────────────

def flush_all_logs(
    output_dir: Path,
    not_found_rows: List[Dict],
    low_confidence_rows: List[Dict],
    error_rows: List[Dict],
) -> None:
    """not_found / low_confidence / error のログを一括追記"""
    if not_found_rows:
        p = write_not_found_log(output_dir, not_found_rows)
        logger.info(f"[csv_exporter] not_found_log: +{len(not_found_rows)} → {p}")
    if low_confidence_rows:
        p = write_low_confidence_review(output_dir, low_confidence_rows)
        logger.info(f"[csv_exporter] low_confidence_review: +{len(low_confidence_rows)} → {p}")
    if error_rows:
        p = write_error_log(output_dir, error_rows)
        logger.info(f"[csv_exporter] error_log: +{len(error_rows)} → {p}")
