#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
companies テーブル全件のデータ品質をキーセットページネーションで調査し、JSON + テキストサマリを出力する。

  python scripts/companies_db_integrity_scan.py
  python scripts/generate_full_integrity_report.py --companies-db-integrity

出力: reports/companies_db_integrity_YYYYMMDD_HHMMSS/{report.json,summary.txt}
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from psycopg2 import sql
except ImportError as e:
    raise SystemExit("psycopg2-binary が必要です") from e

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
REPORTS_BASE = PROJECT_ROOT / "reports"

try:
    sys.path.insert(0, str(SCRIPT_DIR))
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    import repair_companies_column_shift_from_fixed_csv3 as rep  # type: ignore

PREFECTURES_FULL = frozenset(
    "北海道 青森県 岩手県 宮城県 秋田県 山形県 福島県 茨城県 栃木県 群馬県 埼玉県 千葉県 東京都 神奈川県 "
    "新潟県 富山県 石川県 福井県 山梨県 長野県 岐阜県 静岡県 愛知県 三重県 滋賀県 京都府 大阪府 兵庫県 "
    "奈良県 和歌山県 鳥取県 島根県 岡山県 広島県 山口県 徳島県 香川県 愛媛県 高知県 福岡県 佐賀県 長崎県 "
    "熊本県 大分県 宮崎県 鹿児島県 沖縄県".split()
)

_DIGIT_13 = re.compile(r"\d{13}")
# 改行・タブ・復帰以外の制御類（ASCII C0 + DELブロック、ゼロ幅等）
_BAD_CTRL_RE = re.compile(
    r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f\u200b-\u200f\ufeff\u2028\u2029]"
)


def _is_prefecture_ok(val: Optional[str]) -> bool:
    if val is None or not str(val).strip():
        return True
    t = unicodedata.normalize("NFKC", str(val).strip())
    return t in PREFECTURES_FULL


_URLISH = frozenset(
    {
        "company_url",
        "contact_form_url",
        "profile_url",
        "external_detail_url",
        "facebook",
        "linkedin",
        "wantedly",
        "youtrust",
    }
)


def _is_url_column(name: str) -> bool:
    n = name.lower()
    if n in _URLISH:
        return True
    if n.endswith("_url") and "nikkei" not in n:
        return True
    return False


@dataclass
class ColumnStats:
    ctrl_chars: int = 0
    digit_13: int = 0
    strip_mismatch: int = 0
    max_len: int = 0
    max_len_id: Optional[str] = None
    max_len_preview: str = ""
    # カラム別
    postal_invalid: int = 0
    email_invalid: int = 0
    url_not_http: int = 0
    prefecture_invalid: int = 0
    samples: Dict[str, List[Dict[str, str]]] = field(default_factory=lambda: defaultdict(list))

    def to_dict(self) -> Dict[str, Any]:
        def pack_samples() -> Dict[str, Any]:
            out: Dict[str, Any] = {}
            for k, v in self.samples.items():
                if v:
                    out[k] = v[:2]
            return out

        base: Dict[str, Any] = {
            "control_chars_except_newline_tab": self.ctrl_chars,
            "contains_13_digit_substring": self.digit_13,
            "leading_trailing_whitespace": self.strip_mismatch,
            "max_character_length": {
                "length": self.max_len,
                "id": self.max_len_id,
                "preview": (self.max_len_preview or "")[:300],
            },
        }
        if self.postal_invalid:
            base["postal_code_invalid_chars_or_format"] = self.postal_invalid
        if self.email_invalid:
            base["email_at_sign_invalid"] = self.email_invalid
        if self.url_not_http:
            base["url_not_starting_with_http"] = self.url_not_http
        if self.prefecture_invalid:
            base["prefecture_not_in_47"] = self.prefecture_invalid
        s = pack_samples()
        if s:
            base["samples_by_anomaly"] = s
        return base


def _fetch_text_columns(cur, schema: str, table: str) -> List[Tuple[str, str]]:
    cur.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    out: List[Tuple[str, str]] = []
    for name, dtype in cur.fetchall():
        if name == "id":
            continue
        out.append((name, str(dtype or "")))
    return out


def _select_exprs(cols_meta: Sequence[Tuple[str, str]]) -> sql.Composed:
    parts: List[Any] = [sql.SQL("id")]
    for col_name, dtype in cols_meta:
        ident = sql.Identifier(col_name)
        if dtype and "json" in dtype.lower():
            parts.append(
                sql.SQL("LEFT(COALESCE({i}::text, ''), 12000) AS {i}").format(i=ident)
            )
        else:
            parts.append(sql.SQL("COALESCE({i}::text, '') AS {i}").format(i=ident))
    return sql.SQL(", ").join(parts)


def _process_cell(
    col: str,
    text: str,
    rid: str,
    st: ColumnStats,
) -> None:
    if text is None:
        text = ""
    L = len(text)
    if L > st.max_len:
        st.max_len = L
        st.max_len_id = rid
        st.max_len_preview = text

    if not text:
        return

    if _BAD_CTRL_RE.search(text):
        st.ctrl_chars += 1
        sk = "control_chars"
        if len(st.samples[sk]) < 2:
            st.samples[sk].append({"id": rid, "preview": text[:400]})

    if _DIGIT_13.search(text):
        st.digit_13 += 1
        sk = "digit_13"
        if len(st.samples[sk]) < 2:
            st.samples[sk].append({"id": rid, "preview": text[:400]})

    if text != text.strip():
        st.strip_mismatch += 1
        sk = "strip_mismatch"
        if len(st.samples[sk]) < 2:
            st.samples[sk].append({"id": rid, "preview": repr(text[:200])})

    if col in ("postal_code", "representative_postal_code"):
        s = text.strip()
        if s:
            ok_form = bool(
                re.fullmatch(r"\d{3}-\d{4}", s)
                or re.fullmatch(r"\d{7}", s)
            )
            only_digits_hyphen = bool(re.fullmatch(r"[\d\-]+", s))
            if (not only_digits_hyphen) or (not ok_form):
                st.postal_invalid += 1
                sk = "postal_invalid"
                if len(st.samples[sk]) < 2:
                    st.samples[sk].append({"id": rid, "preview": text[:200]})

    if col == "email":
        s = text.strip()
        if s:
            at = s.count("@")
            if at != 1:
                st.email_invalid += 1
                sk = "email_invalid"
                if len(st.samples[sk]) < 2:
                    st.samples[sk].append({"id": rid, "preview": text[:200]})

    if col == "prefecture":
        if not _is_prefecture_ok(text):
            st.prefecture_invalid += 1
            sk = "prefecture_invalid"
            if len(st.samples[sk]) < 2:
                st.samples[sk].append({"id": rid, "preview": text[:200]})

    if _is_url_column(col):
        s = text.strip().lower()
        if s and not (s.startswith("http://") or s.startswith("https://")):
            st.url_not_http += 1
            sk = "url_not_http"
            if len(st.samples[sk]) < 2:
                st.samples[sk].append({"id": rid, "preview": text[:200]})


def run_companies_db_integrity_report(argv: Optional[Sequence[str]] = None) -> Path:
    ap = argparse.ArgumentParser(description="companies テーブル全件データ品質レポート")
    ap.add_argument("--schema", default="public", help="テーブルスキーマ")
    ap.add_argument("--table", default="companies", help="テーブル名")
    ap.add_argument("--fetch-chunk", type=int, default=3000, help="キーセット 1 ページの行数")
    ap.add_argument("--log-every-chunks", type=int, default=50, help="このチャンク数ごとに進捗を表示")
    ap.add_argument("--max-rows", type=int, default=0, help="走査行数の上限（0=全件。試行用）")
    ap.add_argument("--out-dir", type=Path, default=None, help="出力先（未指定は reports 下にタイムスタンプ）")
    if argv is not None:
        args, unknown = ap.parse_known_args(list(argv))
    else:
        args, unknown = ap.parse_known_args()
    if unknown:
        print(f"[警告] 未認識の引数を無視します: {unknown}", flush=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir or (REPORTS_BASE / f"companies_db_integrity_{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = rep.get_connection()
    cur = conn.cursor()
    cols_meta = _fetch_text_columns(cur, args.schema, args.table)
    col_names = [c[0] for c in cols_meta]
    if not col_names:
        raise SystemExit("対象カラムがありません（id 以外）")

    select_list = _select_exprs(cols_meta)
    tbl = sql.Identifier(args.schema, args.table)
    base_q = sql.SQL("SELECT {cols} FROM {tbl} AS c WHERE c.id > %s ORDER BY c.id LIMIT %s").format(
        cols=select_list,
        tbl=tbl,
    )

    per_col: Dict[str, ColumnStats] = {c: ColumnStats() for c in col_names}
    total_rows = 0
    last_id = ""
    chunk_i = 0

    try:
        while True:
            cur.execute(base_q, (last_id, args.fetch_chunk))
            rows = cur.fetchall()
            if not rows:
                break
            desc = [d[0] for d in cur.description]
            id_idx = desc.index("id")
            for row in rows:
                rid = str(row[id_idx])
                for j, name in enumerate(desc):
                    if name == "id":
                        continue
                    val = row[j]
                    _process_cell(name, val if val is not None else "", rid, per_col[name])
                total_rows += 1
            last_id = str(rows[-1][id_idx])
            chunk_i += 1
            if args.log_every_chunks and chunk_i % args.log_every_chunks == 0:
                print(
                    f"[進捗] chunks={chunk_i} rows_scanned={total_rows} last_id={last_id[:48]}…",
                    flush=True,
                )
            if args.max_rows and total_rows >= args.max_rows:
                break
    finally:
        cur.close()
        conn.close()

    report: Dict[str, Any] = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "schema": args.schema,
        "table": args.table,
        "rows_scanned": total_rows,
        "fetch_chunk": args.fetch_chunk,
        "columns": {c: per_col[c].to_dict() for c in col_names},
    }

    json_path = out_dir / "report.json"
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [
        f"companies DB integrity report",
        f"generated: {report['generated_at']}",
        f"rows_scanned: {total_rows}",
        f"json: {json_path}",
        "",
        "=== サマリ（件数 > 0 のみ） ===",
    ]
    for c in col_names:
        d = per_col[c].to_dict()
        parts = [f"[{c}]"]
        for k, v in d.items():
            if k in ("samples_by_anomaly", "max_character_length"):
                continue
            if isinstance(v, int) and v > 0:
                parts.append(f"  {k}: {v}")
        ml = d.get("max_character_length", {})
        if isinstance(ml, dict) and ml.get("length", 0) > 0:
            parts.append(
                f"  max_character_length: len={ml.get('length')} id={ml.get('id')}"
            )
        if len(parts) > 1:
            lines.extend(parts)
            lines.append("")
    summary_path = out_dir / "summary.txt"
    summary_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"完了: rows_scanned={total_rows}", flush=True)
    print(f"JSON: {json_path}", flush=True)
    print(f"TXT:  {summary_path}", flush=True)
    return out_dir


if __name__ == "__main__":
    run_companies_db_integrity_report()
