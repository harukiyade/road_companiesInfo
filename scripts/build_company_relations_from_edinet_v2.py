#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import os
import re
from collections import defaultdict
from urllib.parse import quote_plus

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


def get_db_config() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "34.84.189.233"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "Legatus2000/"),
        "dbname": os.getenv("POSTGRES_DB", "postgres"),
        "sslmode": os.getenv("POSTGRES_SSLMODE", "require"),
        "connect_timeout": int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10")),
    }


def clean_company_name(name: str) -> str:
    if not name:
        return ""
    s = str(name)
    s = re.sub(r"[\(（].*?[\)）]", "", s)
    s = re.sub(r"株式会社|有限会社|合同会社|（株）|\(株\)|㈱", "", s)
    s = re.sub(r"\s+", "", s)
    return s.strip()


def ensure_schema(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_relations (
            id SERIAL PRIMARY KEY,
            parent_company_id TEXT,
            child_company_id TEXT,
            child_company_name TEXT NOT NULL,
            source_doc_id TEXT,
            relation_category TEXT,
            voting_rights_ratio NUMERIC,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    cur.execute(
        """
        ALTER TABLE company_relations
        ADD COLUMN IF NOT EXISTS relation_category TEXT;
        """
    )
    cur.execute(
        """
        ALTER TABLE company_relations
        ADD COLUMN IF NOT EXISTS voting_rights_ratio NUMERIC;
        """
    )
    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_constraint
                WHERE conname = 'unique_parent_child_rel'
            ) THEN
                ALTER TABLE company_relations
                ADD CONSTRAINT unique_parent_child_rel UNIQUE(parent_company_id, child_company_name);
            END IF;
        END $$;
        """
    )


def resolve_unique_id(candidates: list[str] | None) -> str | None:
    if not candidates:
        return None
    uniq = list(dict.fromkeys(candidates))
    return uniq[0] if len(uniq) == 1 else None


def normalize_ratio(v) -> float | None:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().replace("%", "").replace("％", "")
    if not s:
        return None
    m = re.search(r"\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None


def run(input_csv: str, fuzzy_match: bool = False) -> None:
    if not os.path.isfile(input_csv):
        raise FileNotFoundError(f"入力CSVが見つかりません: {input_csv}")

    df = pd.read_csv(input_csv, encoding="utf-8-sig", dtype=str).fillna("")
    required = ["親会社法人番号", "子会社名", "関係区分", "議決権所有比率", "出典docID"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"入力CSVに必要列がありません: {c}")

    cfg = get_db_config()
    masked = cfg.copy()
    masked["password"] = quote_plus(masked["password"]) if masked["password"] else ""
    print(f"DB接続: {cfg['host']}:{cfg['port']}/{cfg['dbname']} user={cfg['user']} sslmode={cfg['sslmode']}")

    conn = psycopg2.connect(**cfg)
    try:
        with conn.cursor() as cur:
            ensure_schema(cur)
            conn.commit()

            # 事前ロードでDB往復を削減（親会社ID・子会社名の完全一致）
            cur.execute("SELECT id, corporate_number, name FROM companies")
            company_rows = cur.fetchall()
            parent_map: dict[str, str] = {}
            fuzzy_core_map: dict[str, list[str]] = defaultdict(list)
            for cid, corp, name in company_rows:
                cid_s = str(cid)
                corp_s = (str(corp).strip() if corp is not None else "")
                if corp_s and corp_s not in parent_map:
                    parent_map[corp_s] = cid_s
                n = (str(name).strip() if name is not None else "")
                if not n:
                    continue
                core = clean_company_name(n)
                if len(core) >= 4:
                    fuzzy_core_map[core].append(cid_s)

            # CSV上の子会社名を一括抽出し、IN句でまとめて照合候補を取得
            child_names = {
                str(v).strip()
                for v in df["子会社名"].tolist()
                if str(v).strip()
            }
            exact_name_ids: dict[str, list[str]] = defaultdict(list)
            if child_names:
                cur.execute(
                    "SELECT id, name FROM companies WHERE name = ANY(%s)",
                    (list(child_names),),
                )
                for cid, name in cur.fetchall():
                    n = (str(name).strip() if name is not None else "")
                    if n:
                        exact_name_ids[n].append(str(cid))

            records = []
            stats = {"total": 0, "parent_missing": 0, "insertable": 0, "child_linked": 0}
            for _, row in df.iterrows():
                stats["total"] += 1
                if stats["total"] % 2000 == 0:
                    print(
                        f"progress: {stats['total']}/{len(df)} insertable={stats['insertable']} child_linked={stats['child_linked']}",
                        flush=True,
                    )
                parent_corp = str(row["親会社法人番号"]).strip()
                child_name = str(row["子会社名"]).strip()
                relation_category = str(row["関係区分"]).strip() or None
                voting_ratio = normalize_ratio(row["議決権所有比率"])
                source_doc_id = str(row["出典docID"]).strip() or None

                if not parent_corp or not child_name:
                    continue

                parent_company_id = parent_map.get(parent_corp)
                if not parent_company_id:
                    stats["parent_missing"] += 1
                    continue

                child_company_id = resolve_unique_id(exact_name_ids.get(child_name))
                if child_company_id is None and fuzzy_match:
                    core = clean_company_name(child_name)
                    if len(core) >= 4:
                        child_company_id = resolve_unique_id(fuzzy_core_map.get(core))
                if child_company_id:
                    stats["child_linked"] += 1

                records.append(
                    (
                        parent_company_id,
                        child_company_id,
                        child_name,
                        source_doc_id,
                        relation_category,
                        voting_ratio,
                    )
                )
                stats["insertable"] += 1

            if records:
                # UPSERT前に (parent_company_id, child_company_id) の重複を除去
                seen_pairs = set()
                unique_records = []
                for rec in records:
                    key = (rec[0], rec[1])
                    if key in seen_pairs:
                        continue
                    seen_pairs.add(key)
                    unique_records.append(rec)

                q = """
                    INSERT INTO company_relations
                    (parent_company_id, child_company_id, child_company_name, source_doc_id, relation_category, voting_rights_ratio)
                    VALUES %s
                    ON CONFLICT ON CONSTRAINT unique_parent_child_rel
                    DO UPDATE SET
                        child_company_id = EXCLUDED.child_company_id,
                        source_doc_id = EXCLUDED.source_doc_id,
                        relation_category = EXCLUDED.relation_category,
                        voting_rights_ratio = EXCLUDED.voting_rights_ratio;
                """
                execute_values(cur, q, unique_records, page_size=1000)
                conn.commit()

            print(
                "完了:",
                f"入力={stats['total']}",
                f"登録対象={stats['insertable']}",
                f"親未解決={stats['parent_missing']}",
                f"子リンク成功={stats['child_linked']}",
            )
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="EDINET抽出CSVをcompany_relationsへ登録")
    parser.add_argument("--input-csv", required=True, help="extract_relations_from_edinet_xbrl.py が出力したCSV")
    parser.add_argument("--fuzzy-match", action="store_true", help="子会社名の曖昧一致検索も実施（遅くなる可能性あり）")
    args = parser.parse_args()
    run(args.input_csv, fuzzy_match=args.fuzzy_match)


if __name__ == "__main__":
    main()
