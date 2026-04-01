# -*- coding: utf-8 -*-
"""
CSV の設立日・代表者誕生日を DB 向けにパースする。

設立（founding / established 相当）の厳格な優先順:
  1. **年4桁が先頭** … ``YYYY-MM-DD`` / ``YYYY/MM/DD`` / ``YYYY.MM.DD``
     （まず ``datetime.strptime``、単桁月日は正規表現 + ``date`` でフォールバック）
  2. **MM/DD/YYYY**（月/日/年）… ``strptime`` のあと柔軟な ``/`` パターン
  3. 日本語表記・その他フォールバック（任意）
  4. セルに ``/`` または ``-`` を含む場合のみ ``pandas.to_datetime(..., format='mixed')`` を最終手段として試行

禁止事項:
  - ハイフン・スラッシュを除去して ``19380301`` のような整数にまとめる処理は行わない。
  - 設立として **純粋数値のみ**のセル（Excel シリアル等）は受理しない（4桁の「年のみ」文字列は例外）。
  - シリアル日数への変換は設立では行わない（誕生日のフォールバックのみ別経路）。

pandas で読み込む場合は ``merge_pandas_dtype_str_for_founding()`` または ``dtype=str`` 全列で、
設立列を数値推論させないこと。
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

DATE_YEAR_MIN = 1800
DATE_YEAR_MAX = 2100

# Excel (Windows) 日付シリアル: 1899-12-30 を day 0 とする慣習（誕生日フォールバックのみ）
EXCEL_EPOCH = datetime(1899, 12, 30)

# 設立として NULL 扱いするプレースホルダー（大文字小文字は ASCII のみ lower）
_FOUNDING_PLACEHOLDER_EXACT = frozenset(
    {
        "不明",
        "未定",
        "なし",
        "無",
        "該当なし",
        "非開示",
        "****",
        "*****",
        "-",
        "―",
        "ー",
        "－",
        "…",
        "...",
    }
)
_FOUNDING_PLACEHOLDER_LOWER = frozenset({"unknown", "n/a", "na", "none", "null", "nil"})

# pandas.read_csv 用: 設立系ヘッダー名（正規化前の表記揺れ）
FOUNDRING_CSV_HEADER_NAMES = (
    "設立",
    "設立年月日",
    "設立年月日(西暦)",
)


def merge_pandas_dtype_str_for_founding(
    dtype: Optional[Dict[str, Any]] = None,
    columns: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    ``pd.read_csv(..., dtype=...)`` 用に、設立関連列を必ず str にマージする。
    ``columns`` を渡すと存在する列名だけキーを足す。
    """
    out: Dict[str, Any] = dict(dtype) if dtype else {}
    for name in FOUNDRING_CSV_HEADER_NAMES:
        if columns is not None and name not in columns:
            continue
        out.setdefault(name, str)
    return out


def _date_from_ymd(y: int, m: int, d: int) -> Optional[date]:
    try:
        if y < DATE_YEAR_MIN or y > DATE_YEAR_MAX or m < 1 or m > 12 or d < 1 or d > 31:
            return None
        return date(y, m, d)
    except ValueError:
        return None


def _normalize_csv_cell(val) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null", ""):
        return None
    return s


def _is_founding_placeholder(s: str) -> bool:
    t = s.strip()
    if t in _FOUNDING_PLACEHOLDER_EXACT:
        return True
    if t.lower() in _FOUNDING_PLACEHOLDER_LOWER:
        return True
    return False


def founding_numeric_cell_reject(s: str) -> bool:
    """
    True → 設立パース対象外（純数値・指数・Excel風小数のみのセル等）。
    4 桁だけの文字列（例: 1938）は年のみ解釈のため False（拒否しない）。
    """
    t = s.strip()
    if not t:
        return True
    if re.search(r"[eE]", t):
        return True
    if re.search(r"[-/年月]", t):
        return False
    # 年のみ 4 桁（数字のみ）
    if re.fullmatch(r"\d{4}", t):
        try:
            y = int(t)
            return not (DATE_YEAR_MIN <= y <= DATE_YEAR_MAX)
        except ValueError:
            return True
    m = re.fullmatch(r"(\d+)\.0+", t)
    if m:
        t = m.group(1)
    if re.fullmatch(r"-?\d+\.?\d*$", t):
        return True
    return False


def _try_priority1_year_first(s: str) -> Optional[date]:
    """
    優先1: 先頭が 4 桁年のカレンダー（中間を数値シリアルにしない）。
    strptime → 単桁月日のフォールバック。
    """
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d"):
        try:
            d = datetime.strptime(s, fmt).date()
            if DATE_YEAR_MIN <= d.year <= DATE_YEAR_MAX:
                return d
        except ValueError:
            continue
    m = re.match(r"^(\d{4})[./-](\d{1,2})[./-](\d{1,2})$", s)
    if m:
        return _date_from_ymd(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def _try_priority2_mm_dd_yyyy(s: str) -> Optional[date]:
    """優先2: MM/DD/YYYY（strptime 後、1〜2 桁の月日を許容する正規表現）。"""
    # 4 桁年のみ（%y は 2000年代へ誤解釈しうるため使わない）
    try:
        d = datetime.strptime(s, "%m/%d/%Y").date()
        if DATE_YEAR_MIN <= d.year <= DATE_YEAR_MAX:
            return d
    except ValueError:
        pass
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        mm, dd, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return _date_from_ymd(y, mm, dd)
    return None


def _parse_japanese_date_forms(s: str) -> Optional[date]:
    m = re.search(r"(\d{4})[年/]\s*(\d{1,2})[月/]\s*(\d{1,2})日?", s)
    if m:
        d0 = _date_from_ymd(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if d0:
            return d0
    m = re.search(r"(\d{1,2})月\s*(\d{1,2})日\s*(\d{4})年", s)
    if m:
        d0 = _date_from_ymd(int(m.group(3)), int(m.group(1)), int(m.group(2)))
        if d0:
            return d0
    m = re.search(r"(\d{4})年\s*(\d{1,2})月(?!\s*\d{1,2}\s*日)", s)
    if m:
        d0 = _date_from_ymd(int(m.group(1)), int(m.group(2)), 1)
        if d0:
            return d0
    return None


def _try_m_d_yy_slash(s: str) -> Optional[date]:
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})$", s)
    if not m:
        return None
    mm, dd, yy = int(m.group(1)), int(m.group(2)), m.group(3)
    if len(yy) == 4:
        return None
    yi = int(yy)
    year = 1900 + yi if yi >= 30 else 2000 + yi
    return _date_from_ymd(year, mm, dd)


def _try_embedded_iso(s: str) -> Optional[date]:
    m = re.search(r"(19|20)\d{2}-\d{1,2}-\d{1,2}", s)
    if m:
        parts = m.group(0).split("-")
        if len(parts) == 3:
            return _date_from_ymd(int(parts[0]), int(parts[1]), int(parts[2]))
    return None


def _try_pandas_mixed_safe(s: str) -> Optional[date]:
    if not re.search(r"[-/]", s):
        return None
    try:
        import pandas as pd
    except ImportError:
        return None
    try:
        ts = pd.to_datetime(s, format="mixed", errors="coerce")
    except (ValueError, TypeError):
        ts = pd.to_datetime(s, errors="coerce")
    if ts is None or pd.isna(ts):
        return None
    try:
        d = ts.date() if hasattr(ts, "date") else None
    except (ValueError, OSError, OverflowError):
        return None
    if d is None or not (DATE_YEAR_MIN <= d.year <= DATE_YEAR_MAX):
        return None
    return d


def _founding_try_parsers() -> List[Callable[[str], Optional[date]]]:
    return [
        _try_priority1_year_first,
        _try_priority2_mm_dd_yyyy,
        _parse_japanese_date_forms,
        _try_m_d_yy_slash,
        _try_embedded_iso,
        _try_pandas_mixed_safe,
    ]


def parse_founding_cell_to_date(val) -> Optional[date]:
    """設立列専用。空・プレースホルダー・純数値（4桁年のみ除く）は None。"""
    s = _normalize_csv_cell(val)
    if not s:
        return None
    if _is_founding_placeholder(s):
        return None
    if founding_numeric_cell_reject(s):
        return None
    for fn in _founding_try_parsers():
        d = fn(s)
        if d:
            return d
    return None


def founding_cell_to_iso_and_year(val) -> Tuple[Optional[str], Optional[int]]:
    d = parse_founding_cell_to_date(val)
    if not d:
        return None, None
    return d.isoformat(), d.year


def _excel_serial_to_date_if_sane(val) -> Optional[date]:
    s = _normalize_csv_cell(val)
    if not s:
        return None
    num_part = re.sub(r"[^\d.\-]", "", s)
    if not num_part:
        return None
    try:
        serial = float(num_part)
    except (ValueError, OverflowError):
        return None
    if not (1 <= serial <= 2958465):
        return None
    dt = EXCEL_EPOCH + timedelta(days=int(serial))
    if not (DATE_YEAR_MIN <= dt.year <= DATE_YEAR_MAX):
        return None
    return dt.date()


def parse_birth_cell_to_date(val) -> Optional[date]:
    d = parse_founding_cell_to_date(val)
    if d:
        return d
    s = _normalize_csv_cell(val)
    if not s:
        return None
    if re.search(r"[-/年月]", s):
        return None
    return _excel_serial_to_date_if_sane(s)


def parse_year_from_founding_cell(val) -> Optional[int]:
    s = _normalize_csv_cell(val)
    if not s or _is_founding_placeholder(s):
        return None
    if founding_numeric_cell_reject(s):
        return None
    m = re.search(r"(\d{4})年?", s)
    if not m:
        return None
    y = int(m.group(1))
    if DATE_YEAR_MIN <= y <= DATE_YEAR_MAX:
        return y
    return None
