"""
業種カスケード API（companies の実在値のみ）。

DB は industry_large / industry_middle / industry_small / industry_detail の4カラムのみ使用。
"""

from __future__ import annotations

import os
from typing import List, Optional

from fastapi import APIRouter, Query
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

router = APIRouter(prefix="/industries", tags=["industries"])

_engine: Optional[Engine] = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        url = os.getenv("DATABASE_URL")
        if not url:
            raise RuntimeError("DATABASE_URL が未設定です")
        _engine = create_engine(url, pool_pre_ping=True)
    return _engine


@router.get("/large", response_model=List[str])
def list_large() -> List[str]:
    sql = text(
        """
        SELECT DISTINCT industry_large AS v
        FROM companies
        WHERE industry_large IS NOT NULL
          AND btrim(industry_large) <> ''
        ORDER BY v
        """
    )
    with get_engine().connect() as conn:
        return [row[0] for row in conn.execute(sql)]


@router.get("/middle", response_model=List[str])
def list_middle(
    large: str = Query(..., description="大分類（例: 建設業）"),
) -> List[str]:
    sql = text(
        """
        SELECT DISTINCT industry_middle AS v
        FROM companies
        WHERE industry_large = :large
          AND industry_middle IS NOT NULL
          AND btrim(industry_middle) <> ''
        ORDER BY v
        """
    )
    with get_engine().connect() as conn:
        return [row[0] for row in conn.execute(sql, {"large": large})]


@router.get("/small", response_model=List[str])
def list_small(
    large: str = Query(...),
    middle: str = Query(...),
) -> List[str]:
    sql = text(
        """
        SELECT DISTINCT industry_small AS v
        FROM companies
        WHERE industry_large = :large
          AND industry_middle = :middle
          AND industry_small IS NOT NULL
          AND btrim(industry_small) <> ''
        ORDER BY v
        """
    )
    with get_engine().connect() as conn:
        return [row[0] for row in conn.execute(sql, {"large": large, "middle": middle})]


@router.get("/detail", response_model=List[str])
def list_detail(
    large: str = Query(...),
    middle: str = Query(...),
    small: str = Query(...),
) -> List[str]:
    sql = text(
        """
        SELECT DISTINCT btrim(industry_detail) AS v
        FROM companies
        WHERE industry_large = :large
          AND industry_middle = :middle
          AND industry_small = :small
          AND industry_detail IS NOT NULL
          AND btrim(industry_detail) <> ''
        ORDER BY v
        """
    )
    with get_engine().connect() as conn:
        return [row[0] for row in conn.execute(sql, {"large": large, "middle": middle, "small": small})]
