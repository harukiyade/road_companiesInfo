"""
column_mapping.py - 代表カラムマッピング管理

同一意味を持つ複数カラムをグループ化し、エンリッチ処理での
「代表カラム」（書き込み先）を一元管理する。

仕様書 4章:「代表カラム」優先順位
1. 最も汎用的でNULL率が高い列名
2. 既存データが入っている列がある場合はその列と同じ列にのみ追記

CANONICAL_MAP: {代表カラム名: [同義カラムリスト（優先順）]}
"""
from __future__ import annotations

from typing import Dict, List, Optional

# 代表カラム → 同義カラム群のマッピング
# リストの先頭が代表カラム（自分自身も含む）
CANONICAL_MAP: Dict[str, List[str]] = {
    # URL系
    "url": ["url", "urls", "homepage_url", "website_url", "company_url"],
    "contact_form_url": ["contact_form_url"],
    # 連絡先
    "phone_number": ["phone_number", "tel", "contact_phone_number"],
    "fax": ["fax"],
    "email": ["email"],
    # 所在地
    "postal_code": ["postal_code", "zip_code", "postcode"],
    "headquarters_address": ["headquarters_address", "address"],
    # 代表者
    "representative_name": ["representative_name"],
    "representative_kana": ["representative_kana"],
    "representative_title": ["representative_title"],
    # 設立
    "founded_year": ["founded_year", "founding_year"],
    "established_date": ["established_date", "established", "establishment_date", "date_of_establishment"],
    # 事業内容
    "company_description": ["company_description", "overview", "business_summary", "summary",
                             "short_description", "description"],
    "business_items": ["business_items", "specialties", "business_descriptions"],
    # 従業員数
    "employee_count": ["employee_count", "employees", "number_of_employees"],
    # 資本金
    "capital_stock": ["capital_stock", "capital"],
    # 拠点
    "office_count": ["office_count", "number_of_offices"],
    "factory_count": ["factory_count", "number_of_factories"],
    "store_count": ["store_count", "number_of_stores"],
    # SNS
    "facebook": ["facebook"],
    "linkedin": ["linkedin"],
    "wantedly": ["wantedly"],
    "youtrust": ["youtrust"],
    # メタ
    "meta_description": ["meta_description"],
    "meta_keywords": ["meta_keywords"],
}

# 逆引き: カラム名 → 代表カラム名
_ALIAS_TO_CANONICAL: Dict[str, str] = {}
for _canonical, _aliases in CANONICAL_MAP.items():
    for _alias in _aliases:
        _ALIAS_TO_CANONICAL[_alias] = _canonical


def get_canonical(column_name: str) -> str:
    """カラム名から代表カラム名を返す（マッピングになければそのまま）"""
    return _ALIAS_TO_CANONICAL.get(column_name, column_name)


def get_aliases(canonical: str) -> List[str]:
    """代表カラム名から同義カラムリストを返す"""
    return CANONICAL_MAP.get(canonical, [canonical])


def find_existing_value(row: Dict, canonical: str) -> Optional[str]:
    """
    行データから代表カラムまたは同義カラムのいずれかに値があれば返す。
    ルール: 既存データが入っている列がある場合はその列に追記（上書きしない）
    """
    for col in get_aliases(canonical):
        val = row.get(col)
        if val is not None and str(val).strip() not in ("", "nan", "None", "null"):
            return str(val).strip()
    return None


def find_write_target(row: Dict, canonical: str) -> Optional[str]:
    """
    書き込み先カラム名を返す。
    - 同義カラムの中に既存値があるカラムを優先（そのカラムに書く）
    - 全て空の場合は代表カラム（canonical）に書く
    - 全て既存値がある場合は None（書かない）
    """
    first_empty: Optional[str] = None
    for col in get_aliases(canonical):
        val = row.get(col)
        is_empty = val is None or str(val).strip() in ("", "nan", "None", "null")
        if is_empty and first_empty is None:
            first_empty = col
    if first_empty is None:
        # 全カラムに値がある → 書き込まない
        return None
    return first_empty


# 役員カラム（動的生成）
EXECUTIVE_SLOTS = 5  # HP取得対象の役員スロット数

def get_executive_slots(row: Dict) -> List[tuple[str, str]]:
    """
    空いている役員スロット(name/position)を返す。
    Returns: [(name_col, position_col), ...]
    """
    available = []
    for i in range(1, EXECUTIVE_SLOTS + 1):
        name_col = f"executive_name{i}"
        pos_col = f"executive_position{i}"
        name_val = row.get(name_col, "")
        pos_val = row.get(pos_col, "")
        if (not name_val or str(name_val).strip() in ("", "nan", "None")) and \
           (not pos_val or str(pos_val).strip() in ("", "nan", "None")):
            available.append((name_col, pos_col))
    return available
