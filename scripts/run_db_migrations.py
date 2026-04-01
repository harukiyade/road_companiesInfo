#!/usr/bin/env python3
"""
PostgreSQL にマイグレーション SQL を順に実行する（import_full_update_fast 前用）。

環境変数: DATABASE_URL または
  POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB, POSTGRES_SSLMODE

プロジェクト直下の .env があれば先頭で読み込み（既存の環境変数は上書きしない）。

例:
  python3 scripts/run_db_migrations.py
  python3 scripts/run_db_migrations.py --only backend/sql/add_management_flags.sql
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_FILES = [
    ROOT / "backend/sql/migration_20260323_import_fast_columns.sql",
    ROOT / "backend/sql/add_management_flags.sql",
]

VERIFY_COLUMNS = {
    "latest_fiscal_year_month",
    "founding",
    "founding_year",
    "business_summary",
    "representative_birth_date",
    "shareholders",
    "suppliers",
    "clients",
    "overview",
    "company_description",
    "latest_revenue",
    "latest_profit",
    "capital_stock",
    "sb_flag",
    "nda_flag",
    "ad_flag",
    "is_active",
}


def load_dotenv(p: Path) -> None:
    if not p.is_file():
        return
    for raw in p.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip()
        if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
            v = v[1:-1]
        os.environ.setdefault(k, v)


def strip_line_comments(sql: str) -> str:
    lines = []
    for line in sql.splitlines():
        if "--" in line:
            line = line[: line.index("--")]
        if line.strip():
            lines.append(line)
    return "\n".join(lines)


def split_statements(sql: str):
    body = strip_line_comments(sql)
    for part in body.split(";"):
        s = part.strip()
        if s:
            yield s


def connect():
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)
    password = os.getenv("POSTGRES_PASSWORD", "")
    if not password:
        print(
            "エラー: DATABASE_URL または POSTGRES_PASSWORD を設定してください。"
            " プロジェクト直下に .env を置くか、export してください。",
            file=sys.stderr,
        )
        sys.exit(1)
    host = os.getenv("POSTGRES_HOST", "34.84.189.233").strip()
    if host in ("...", "your_host", "localhost.example"):
        print(
            "エラー: POSTGRES_HOST がプレースホルダのままです。"
            " 実際の IP またはホスト名に置き換えてください"
            "（例: 34.84.189.233 または Cloud SQL プロキシなら 127.0.0.1）。",
            file=sys.stderr,
        )
        sys.exit(1)
    sslmode = os.getenv("POSTGRES_SSLMODE", "require").strip()
    if "python" in sslmode.lower():
        print(
            "エラー: POSTGRES_SSLMODE に誤ってコマンドがくっついています。"
            " 例: export POSTGRES_SSLMODE=require のあと改行して python3 を実行してください。",
            file=sys.stderr,
        )
        sys.exit(1)
    return psycopg2.connect(
        host=host,
        port=os.getenv("POSTGRES_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=password,
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        sslmode=sslmode,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SQL migration files against PostgreSQL.")
    parser.add_argument(
        "files",
        nargs="*",
        type=Path,
        help="SQL paths (default: migration_20260323 + add_management_flags)",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip information_schema column check",
    )
    args = parser.parse_args()

    load_dotenv(ROOT / ".env")

    paths = [Path(p).resolve() for p in args.files] if args.files else DEFAULT_FILES
    for p in paths:
        if not p.is_file():
            print(f"エラー: ファイルがありません: {p}", file=sys.stderr)
            sys.exit(1)

    conn = connect()
    try:
        cur = conn.cursor()
        for fp in paths:
            try:
                rel = fp.relative_to(ROOT)
            except ValueError:
                rel = fp
            print(f"--- 実行: {rel} ---")
            sql = fp.read_text(encoding="utf-8")
            for i, stmt in enumerate(split_statements(sql), 1):
                cur.execute(stmt + ";")
                print(f"  OK 文 {i}")
        conn.commit()
        cur.close()

        if not args.no_verify:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'companies'
                """
            )
            have = {r[0] for r in cur.fetchall()}
            missing = sorted(VERIFY_COLUMNS - have)
            cur.close()
            if missing:
                print("警告: 次の列が companies に見つかりません:", ", ".join(missing))
            else:
                print(
                    f"検証 OK: 期待する {len(VERIFY_COLUMNS)} 列はすべて public.companies に存在します。"
                )
    finally:
        conn.close()

    print("マイグレーション完了。")


if __name__ == "__main__":
    main()
