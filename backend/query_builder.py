"""
企業検索用の SQLAlchemy 条件ビルダー。

業種（細）: industryDetail が指定された場合は industry_detail 列のみで一致させる。
"""

from __future__ import annotations

from typing import Any, List

from sqlalchemy import text
from sqlalchemy.sql import and_, or_


def build_company_search_conditions(Company: Any, params: Any) -> List[Any]:
    """Company ORM と SearchParams 相当オブジェクトから AND 結合する条件リストを返す。"""
    conditions: List[Any] = []

    if params.prefecture:
        conditions.append(Company.prefecture == params.prefecture)

    if params.revenue_min is not None:
        conditions.append(Company.revenue >= params.revenue_min)

    if params.revenue_max is not None:
        conditions.append(Company.revenue <= params.revenue_max)

    if params.industry_tags:
        tag_conditions = []
        for tag in params.industry_tags:
            tag_conditions.append(
                text(":tag = ANY(companies.industries)").bindparams(tag=tag)
            )
        if tag_conditions:
            conditions.append(or_(*tag_conditions))

    if params.listing:
        conditions.append(Company.listing == params.listing)

    if params.capital_stock_min is not None:
        conditions.append(Company.capital_stock >= params.capital_stock_min)

    if params.employee_count_min is not None:
        conditions.append(Company.employee_count >= params.employee_count_min)

    if params.industry:
        conditions.append(Company.industry == params.industry)

    if params.industry_large:
        conditions.append(Company.industry_large == params.industry_large)

    if params.industry_middle:
        conditions.append(Company.industry_middle == params.industry_middle)

    if getattr(params, "industry_small", None):
        conditions.append(Company.industry_small == params.industry_small)

    industry_detail_val = getattr(params, "industry_detail", None)
    if industry_detail_val:
        conditions.append(Company.industry_detail == industry_detail_val)

    if params.name:
        conditions.append(Company.name.ilike(f"%{params.name}%"))

    if params.corporate_number:
        conditions.append(Company.corporate_number == params.corporate_number)

    return conditions


def apply_search_filters(query: Any, Company: Any, params: Any) -> Any:
    """query に業種・所在地などのフィルタを適用し、企業名ソートを付与する。"""
    conditions = build_company_search_conditions(Company, params)
    if conditions:
        query = query.filter(and_(*conditions))
    return query.order_by(Company.name)
