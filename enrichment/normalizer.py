"""
normalizer.py - 抽出値のクレンジング・データ型変換（Step4）

仕様書 7-2章:
- 電話番号: 全角→半角、ハイフン統一
- 郵便番号: 7桁ハイフン付き統一
- 日付（設立年月等）: YYYY-MM-DD or YYYY年M月 → 統一形式
- 金額（資本金等）: 億/万 を含む日本語表記 → 円単位 int
- 従業員数: 約/名/人 等の表記ゆれ → int
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


# ─────────────────────────────────────────────
# 電話番号
# ─────────────────────────────────────────────

_FULL_DIGITS = str.maketrans("０１２３４５６７８９", "0123456789")
_FULL_HYPHEN = str.maketrans("ー－―‐−", "-----")


def normalize_phone(phone: Any) -> Optional[str]:
    """
    電話番号を正規化。
    - 全角→半角数字・ハイフン
    - 余分な記号除去
    - 国際番号プレフィックス(+81)を0に変換
    Returns: "XX-XXXX-XXXX" 形式 or None
    """
    if not phone:
        return None
    s = str(phone).strip()
    s = s.translate(_FULL_DIGITS).translate(_FULL_HYPHEN)
    # 国際番号 +81 → 0
    s = re.sub(r"^\+81[-\s]?", "0", s)
    # 数字とハイフンのみ残す
    s = re.sub(r"[^\d\-]", "", s)
    # ハイフンなしの場合、市外局番を推定して挿入は行わない（そのまま返す）
    digits = re.sub(r"\D", "", s)
    if len(digits) < 10 or len(digits) > 11:
        return None
    return s if "-" in s else digits


# ─────────────────────────────────────────────
# FAX
# ─────────────────────────────────────────────

def normalize_fax(fax: Any) -> Optional[str]:
    return normalize_phone(fax)


# ─────────────────────────────────────────────
# 郵便番号
# ─────────────────────────────────────────────

def normalize_postal_code(postal: Any) -> Optional[str]:
    """
    郵便番号を 7桁ハイフン付き（NNN-NNNN）に統一。
    """
    if not postal:
        return None
    s = str(postal).strip()
    s = s.translate(_FULL_DIGITS)
    s = re.sub(r"[^0-9]", "", s)
    if len(s) == 7:
        return f"{s[:3]}-{s[3:]}"
    return None


# ─────────────────────────────────────────────
# 日付（設立年月等）
# ─────────────────────────────────────────────

_ERA_MAP = {
    "明治": 1868, "大正": 1912, "昭和": 1926, "平成": 1989, "令和": 2019,
}


def _era_to_year(era: str, year: int) -> int:
    """元号年を西暦に変換"""
    base = _ERA_MAP.get(era, 0)
    return base + year - 1


def normalize_date(date_str: Any) -> Optional[str]:
    """
    設立年月日を YYYY-MM-DD（または YYYY-MM, YYYY）に統一。
    """
    if not date_str:
        return None
    s = str(date_str).strip()
    s = s.translate(_FULL_DIGITS)

    # --- 元号パターン（「元年」対応）---
    s = re.sub(r"元年", "1年", s)   # 元年 → 1年 に正規化
    # 年月日あり
    m = re.match(
        r"(明治|大正|昭和|平成|令和)\s*(\d{1,2})年\s*(\d{1,2})月\s*(?:(\d{1,2})日)?",
        s,
    )
    if m:
        year = _era_to_year(m.group(1), int(m.group(2)))
        month = int(m.group(3))
        day = int(m.group(4)) if m.group(4) else None
        if day:
            return f"{year:04d}-{month:02d}-{day:02d}"
        return f"{year:04d}-{month:02d}"
    # 年のみ（例: 昭和55年、令和1年）
    m = re.match(r"(明治|大正|昭和|平成|令和)\s*(\d{1,2})年?$", s)
    if m:
        year = _era_to_year(m.group(1), int(m.group(2)))
        return f"{year:04d}"

    # --- 西暦 YYYY年M月D日 ---
    m = re.match(r"(\d{4})年\s*(\d{1,2})月\s*(?:(\d{1,2})日)?", s)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        day = int(m.group(3)) if m.group(3) else None
        if 1800 <= year <= 2100:
            if day:
                return f"{year:04d}-{month:02d}-{day:02d}"
            return f"{year:04d}-{month:02d}"

    # --- ISO / スラッシュ形式 ---
    m = re.match(r"(\d{4})[-/](\d{1,2})(?:[-/](\d{1,2}))?", s)
    if m:
        year, month = int(m.group(1)), int(m.group(2))
        day = int(m.group(3)) if m.group(3) else None
        if 1800 <= year <= 2100 and 1 <= month <= 12:
            if day and 1 <= day <= 31:
                return f"{year:04d}-{month:02d}-{day:02d}"
            return f"{year:04d}-{month:02d}"

    # --- 年のみ ---
    m = re.match(r"(\d{4})年?$", s)
    if m:
        year = int(m.group(1))
        if 1800 <= year <= 2100:
            return f"{year:04d}"

    return None


def normalize_founded_year(date_str: Any) -> Optional[str]:
    """
    founded_year/founding_year 用に年だけ返す。
    """
    d = normalize_date(date_str)
    if d:
        return d[:4]
    return None


# ─────────────────────────────────────────────
# 金額（資本金）
# ─────────────────────────────────────────────

def normalize_capital(capital: Any) -> Optional[int]:
    """
    資本金を円単位の int に変換。
    例: "1億2000万円" → 120_000_000
        "5,000万円" → 50_000_000
        "100万" → 1_000_000
    """
    if not capital:
        return None
    s = str(capital).strip()
    s = s.translate(_FULL_DIGITS)
    # カンマ除去
    s = s.replace(",", "").replace("，", "").replace("、", "")

    total = 0

    # 億
    m = re.search(r"([\d.]+)億", s)
    if m:
        total += int(float(m.group(1)) * 100_000_000)

    # 万
    m = re.search(r"([\d,]+)万", s)
    if m:
        val = m.group(1).replace(",", "")
        total += int(float(val) * 10_000)

    # 千
    m = re.search(r"([\d]+)千", s)
    if m and "万" not in s:
        total += int(m.group(1)) * 1_000

    if total > 0:
        return total

    # 円単位の数値
    m = re.search(r"([\d]+)", re.sub(r"[億万千]", "", s))
    if m:
        val = int(m.group(1))
        if val > 0:
            return val

    return None


# ─────────────────────────────────────────────
# 従業員数
# ─────────────────────────────────────────────

def normalize_employee_count(count: Any) -> Optional[int]:
    """
    従業員数を int に変換。
    例: "約300名" → 300, "1,200人（パート含む）" → 1200
    """
    if not count:
        return None
    s = str(count).strip()
    s = s.translate(_FULL_DIGITS)
    # カンマ除去
    s = s.replace(",", "").replace("，", "")
    # 「約」「以上」「以下」「名」「人」「名様」等の除去
    s = re.sub(r"[約以上以下名様人スタッフ\s（\(）\)パートアルバイト含む]", "", s)
    # 最初の数字列を取得
    m = re.search(r"(\d+)", s)
    if m:
        val = int(m.group(1))
        if 0 < val < 10_000_000:
            return val
    return None


# ─────────────────────────────────────────────
# メールアドレス
# ─────────────────────────────────────────────

def normalize_email(email: Any) -> Optional[str]:
    """メールアドレスの正規化（全角→半角、クエリパラメータ除去）"""
    if not email:
        return None
    s = str(email).strip()
    s = s.translate(str.maketrans(
        "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ"
        "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ"
        "０１２３４５６７８９＠．",
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789@.",
    ))
    s = s.split("?")[0].split(" ")[0]
    m = re.match(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", s)
    if m:
        return m.group(0).lower()
    return None


# ─────────────────────────────────────────────
# 代表者名
# ─────────────────────────────────────────────

def normalize_representative_name(name: Any) -> Optional[str]:
    """
    代表者名から不要な役職・記号を除去して氏名だけ返す。
    """
    if not name:
        return None
    s = str(name).strip()
    # 役職除去
    s = re.sub(
        r"(代表取締役|取締役|監査役|社長|会長|副社長|専務|常務|執行役員"
        r"|CEO|CFO|COO|CTO|代表|Chairman|President)\s*",
        "",
        s,
    ).strip()
    # 括弧内除去
    s = re.sub(r"[（(].*?[）)]", "", s).strip()
    # 空白正規化
    s = re.sub(r"\s+", " ", s).strip()
    # 2〜20文字の範囲が人名として妥当
    if 2 <= len(s) <= 20:
        return s
    return None


# ─────────────────────────────────────────────
# テキスト（概要・事業内容）
# ─────────────────────────────────────────────

def normalize_description(text: Any, max_len: int = 500) -> Optional[str]:
    """
    概要・事業内容テキストのクレンジング。
    著作権懸念を避けるため max_len 文字で切り捨て。
    """
    if not text:
        return None
    s = str(text).strip()
    # 過度な空白・改行を圧縮
    s = re.sub(r"[ 　\t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    s = s[:max_len]
    if len(s) < 5:
        return None
    return s


# ─────────────────────────────────────────────
# 所在地
# ─────────────────────────────────────────────

def normalize_address(address: Any) -> Optional[str]:
    """住所の基本クレンジング（全角スペース統一等）"""
    if not address:
        return None
    s = str(address).strip()
    s = re.sub(r"[ 　\t]+", " ", s)
    # 郵便番号が先頭にある場合は除去
    s = re.sub(r"^〒?\s*\d{3}-?\d{4}\s*", "", s).strip()
    return s if s else None


# ─────────────────────────────────────────────
# 統合正規化
# ─────────────────────────────────────────────

_FIELD_NORMALIZERS = {
    "phone_number": normalize_phone,
    "tel": normalize_phone,
    "contact_phone_number": normalize_phone,
    "fax": normalize_fax,
    "email": normalize_email,
    "postal_code": normalize_postal_code,
    "zip_code": normalize_postal_code,
    "postcode": normalize_postal_code,
    "headquarters_address": normalize_address,
    "address": normalize_address,
    "established_date": normalize_date,
    "established": normalize_date,
    "establishment_date": normalize_date,
    "date_of_establishment": normalize_date,
    "founded_year": normalize_founded_year,
    "founding_year": normalize_founded_year,
    "representative_name": normalize_representative_name,
    "capital_stock": lambda v: str(normalize_capital(v)) if normalize_capital(v) is not None else None,
    "capital": lambda v: str(normalize_capital(v)) if normalize_capital(v) is not None else None,
    "employee_count": lambda v: str(normalize_employee_count(v)) if normalize_employee_count(v) is not None else None,
    "employees": lambda v: str(normalize_employee_count(v)) if normalize_employee_count(v) is not None else None,
    "number_of_employees": lambda v: str(normalize_employee_count(v)) if normalize_employee_count(v) is not None else None,
    "overview": lambda v: normalize_description(v, 500),
    "company_description": lambda v: normalize_description(v, 500),
    "business_summary": lambda v: normalize_description(v, 300),
    "summary": lambda v: normalize_description(v, 300),
    "description": lambda v: normalize_description(v, 500),
    "short_description": lambda v: normalize_description(v, 200),
    "business_items": lambda v: normalize_description(v, 300),
    "specialties": lambda v: normalize_description(v, 300),
    "business_descriptions": lambda v: normalize_description(v, 500),
    "meta_description": lambda v: normalize_description(v, 300),
    "meta_keywords": lambda v: normalize_description(v, 200),
    "office_count": lambda v: str(normalize_employee_count(v)) if normalize_employee_count(v) is not None else None,
    "factory_count": lambda v: str(normalize_employee_count(v)) if normalize_employee_count(v) is not None else None,
    "store_count": lambda v: str(normalize_employee_count(v)) if normalize_employee_count(v) is not None else None,
    "number_of_offices": lambda v: str(normalize_employee_count(v)) if normalize_employee_count(v) is not None else None,
}


def normalize_scraped_data(raw: Dict) -> Dict:
    """
    スクレイピング結果 dict を各フィールドの正規化関数で処理して返す。
    役員情報（_executives）は別途処理されるのでここではスキップ。
    """
    result: Dict = {}
    for field, value in raw.items():
        if field.startswith("_"):
            result[field] = value
            continue
        normalizer = _FIELD_NORMALIZERS.get(field)
        if normalizer:
            try:
                normalized = normalizer(value)
                if normalized is not None:
                    result[field] = normalized
            except Exception:
                # 正規化失敗は元の値を使用
                if value:
                    result[field] = str(value)
        else:
            # 未定義フィールドはそのまま（URLなど）
            if value:
                result[field] = str(value)
    return result
