"""
FastAPI エントリ: 業種カスケード API などをマウントする。

起動例（backend ディレクトリで）:
  pip install -r requirements-api.txt
  uvicorn main:app --reload --host 0.0.0.0 --port 8000

フロント既定 URL: http://localhost:8000/api/industries
環境変数 CORS_ORIGINS に Next のオリジンをカンマ区切りで指定。
"""

from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import industries as industries_router

app = FastAPI(title="info_companyDetail API")

_origins = [
    o.strip()
    for o in os.getenv(
        "CORS_ORIGINS",
        "http://localhost:3000,http://127.0.0.1:3000",
    ).split(",")
    if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(industries_router.router, prefix="/api")
