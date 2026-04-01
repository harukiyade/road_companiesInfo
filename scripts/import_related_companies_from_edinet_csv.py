#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values


@dataclass
class Candidate:
    parent_company_id: str
    child_company_id: Optional[str]
    child_company_name: str
    child_name_normalized: str
    relation_type: Optional[str]
    voting_rights_ratio: Optional[Decimal]
    source_doc_id: Optional[str]
    child_corporate_number: Optional[str]


@dataclass(frozen=True)
class IndustryTriple:
    primary: str
    secondary: str
    tertiary: str


def db_config() -> dict:
    return {
        "host": os.getenv("POSTGRES_HOST", "34.84.189.233"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.getenv("POSTGRES_DB", "postgres"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "Legatus2000/"),
        "sslmode": os.getenv("POSTGRES_SSLMODE", "require"),
        "connect_timeout": int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "10")),
    }


def normalize_child_name(name: str) -> str:
    s = (name or "").strip().replace('"', "").replace("“", "").replace("”", "")
    s = re.sub(r"\s+", " ", s)
    # （東京都...） / (東京都...) 形式の住所ノイズを丸ごと除去
    s = re.sub(r"[（(]\s*(東京都|北海道|(?:京都|大阪)府|.{2,3}県)[^）)]*[）)]", "", s)
    s = re.sub(r"[、,]\s*(東京都|北海道|(?:京都|大阪)府|.{2,3}県).*$", "", s)
    s = re.sub(r"\s*(東京都|北海道|(?:京都|大阪)府|.{2,3}県).*$", "", s)
    return s.strip()


def normalize_key(name: str) -> str:
    s = normalize_child_name(name).lower()
    s = re.sub(r"\s+", "", s)
    return s


def normalize_ratio(raw: str) -> Optional[Decimal]:
    t = (raw or "").strip().replace("%", "").replace("％", "")
    if not t:
        return None
    m = re.search(r"\d+(?:\.\d+)?", t)
    if not m:
        return None
    try:
        return Decimal(m.group(0))
    except InvalidOperation:
        return None


def load_industry_master(industries_csv: str) -> list[IndustryTriple]:
    with open(industries_csv, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    out: list[IndustryTriple] = []
    for row in rows:
        large = (row.get("industryLarge") or "").strip()
        middle = (row.get("industryMiddle") or "").strip()
        small = (row.get("industrySmall") or "").strip()
        if large and middle and small:
            out.append(IndustryTriple(primary=large, secondary=middle, tertiary=small))
    if not out:
        raise RuntimeError(f"industries.csv が空、または必須列が不足: {industries_csv}")
    return out


def pick_default_industry(master: list[IndustryTriple]) -> IndustryTriple:
    for item in master:
        if "その他" in item.tertiary:
            return item
    return master[0]


def guess_industry(name: str, parent_industry_info: Optional[IndustryTriple], industry_master: list[IndustryTriple]) -> IndustryTriple:
    n = (name or "").strip()
    keyword_map: list[tuple[list[str], tuple[str, str]]] = [
        (["銀行", "信用金庫", "信託"], ("金融業,保険業", "銀行業")),
        (["証券", "投信", "アセットマネジメント"], ("金融業,保険業", "証券業")),
        (["保険", "損保", "生命"], ("金融業,保険業", "保険業")),
        (["建設", "工務店", "土木"], ("建設業", "総合工事業")),
        (["食品", "フーズ", "飲料", "菓子"], ("製造業", "食料品製造業")),
        (["システム", "ソフト", "IT", "情報処理", "テック"], ("情報通信業", "情報サービス業")),
        (["運輸", "物流", "倉庫", "配送"], ("運輸業", "道路貨物運送業")),
        (["電力", "ガス", "エネルギー"], ("電気・ガス・熱供給・水道業", "")),
        (["不動産", "リアルティ", "住宅"], ("不動産業", "")),
        (["小売", "ストア", "ショップ"], ("卸売業,小売業", "小売業")),
    ]
    for keywords, (p, s) in keyword_map:
        if not any(k in n for k in keywords):
            continue
        for item in industry_master:
            if item.primary != p:
                continue
            if s and item.secondary != s:
                continue
            return item

    if parent_industry_info is not None:
        return parent_industry_info
    return pick_default_industry(industry_master)


def should_register_master(relation_type: Optional[str], voting_rights_ratio: Optional[Decimal]) -> bool:
    relation = (relation_type or "").strip()
    if relation == "連結子会社":
        return True
    if voting_rights_ratio is not None and voting_rights_ratio >= Decimal("50"):
        return True
    return False


def first_present(row: dict[str, str], keys: list[str]) -> str:
    for k in keys:
        v = (row.get(k) or "").strip()
        if v:
            return v
    return ""


def pick_better(prev: Candidate, cur: Candidate) -> Candidate:
    # docIDの新しさ（文字列比較）を優先、それ以外は新データ側を採用
    pdoc = prev.source_doc_id or ""
    cdoc = cur.source_doc_id or ""
    return cur if cdoc >= pdoc else prev


def load_parent_map(cur, parent_numbers: list[str], chunk_size: int) -> dict[str, str]:
    out: dict[str, str] = {}
    for i in range(0, len(parent_numbers), chunk_size):
        chunk = parent_numbers[i : i + chunk_size]
        if not chunk:
            continue
        placeholders = ",".join(["%s"] * len(chunk))
        cur.execute(f"SELECT id, corporate_number FROM companies WHERE corporate_number IN ({placeholders})", chunk)
        for cid, corp in cur.fetchall():
            if corp is not None:
                out[str(corp).strip()] = str(cid)
    return out


def load_parent_industry_map(cur, parent_ids: list[str], chunk_size: int) -> dict[str, IndustryTriple]:
    out: dict[str, IndustryTriple] = {}
    for i in range(0, len(parent_ids), chunk_size):
        chunk = parent_ids[i : i + chunk_size]
        if not chunk:
            continue
        placeholders = ",".join(["%s"] * len(chunk))
        cur.execute(
            f"""
            SELECT id, primary_industry, secondary_industry, tertiary_industry
            FROM companies
            WHERE id IN ({placeholders})
            """,
            chunk,
        )
        for cid, p, s, t in cur.fetchall():
            if p and s and t:
                out[str(cid)] = IndustryTriple(primary=str(p), secondary=str(s), tertiary=str(t))
    return out


def load_child_maps(cur, child_names: list[str], child_corps: list[str], chunk_size: int) -> tuple[dict[str, str | None], dict[str, str]]:
    name_to_id: dict[str, str | None] = {}
    corp_to_id: dict[str, str] = {}

    for i in range(0, len(child_names), chunk_size):
        chunk = child_names[i : i + chunk_size]
        if not chunk:
            continue
        placeholders = ",".join(["%s"] * len(chunk))
        cur.execute(f"SELECT id, name FROM companies WHERE name IN ({placeholders})", chunk)
        for cid, name in cur.fetchall():
            if not name:
                continue
            key = normalize_child_name(str(name))
            cid_s = str(cid)
            if key in name_to_id and name_to_id[key] != cid_s:
                name_to_id[key] = None
            elif key not in name_to_id:
                name_to_id[key] = cid_s

    for i in range(0, len(child_corps), chunk_size):
        chunk = child_corps[i : i + chunk_size]
        if not chunk:
            continue
        placeholders = ",".join(["%s"] * len(chunk))
        cur.execute(f"SELECT id, corporate_number FROM companies WHERE corporate_number IN ({placeholders})", chunk)
        for cid, corp in cur.fetchall():
            if corp:
                corp_to_id[str(corp).strip()] = str(cid)

    return name_to_id, corp_to_id


def load_existing_relations(cur, parent_ids: list[str], chunk_size: int) -> dict[tuple[str, str], dict]:
    existing: dict[tuple[str, str], dict] = {}
    for i in range(0, len(parent_ids), chunk_size):
        chunk = parent_ids[i : i + chunk_size]
        if not chunk:
            continue
        placeholders = ",".join(["%s"] * len(chunk))
        cur.execute(
            f"""
            SELECT
                parent_company_id,
                child_company_name,
                relation_type,
                voting_rights_ratio,
                source_doc_id,
                child_company_id,
                child_corporate_number
            FROM company_relations
            WHERE parent_company_id IN ({placeholders})
            """,
            chunk,
        )
        for row in cur.fetchall():
            parent_id = str(row[0]) if row[0] is not None else ""
            child_name = str(row[1]) if row[1] is not None else ""
            key = (parent_id, normalize_key(child_name))
            existing[key] = {
                "child_company_name": child_name,
                "relation_type": row[2],
                "voting_rights_ratio": row[3],
                "source_doc_id": row[4],
                "child_company_id": row[5],
                "child_corporate_number": row[6],
            }
    return existing


def resolve_industries_path(user_path: Optional[str]) -> str:
    if user_path:
        p = Path(user_path)
        if not p.exists():
            raise RuntimeError(f"industries.csv が見つかりません: {user_path}")
        return str(p)
    candidates = [
        Path(__file__).resolve().parent / "industries.csv",
        Path(__file__).resolve().parent.parent / "data" / "industries.csv",
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    raise RuntimeError("industries.csv が見つかりません (scripts/industries.csv または data/industries.csv)")


def run(input_csv: str, chunk_size: int = 1000, industries_csv: Optional[str] = None) -> None:
    with open(input_csv, "r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    industries_path = resolve_industries_path(industries_csv)
    industry_master = load_industry_master(industries_path)

    parent_numbers = sorted({(r.get("親会社法人番号") or "").strip() for r in rows if (r.get("親会社法人番号") or "").strip()})
    child_names = sorted({normalize_child_name((r.get("子会社名") or "").strip()) for r in rows if (r.get("子会社名") or "").strip()})
    child_corps = sorted(
        {
            first_present(r, ["子会社法人番号", "child_corporate_number", "childCorporateNumber"])
            for r in rows
            if first_present(r, ["子会社法人番号", "child_corporate_number", "childCorporateNumber"])
        }
    )

    cfg = db_config()
    print(f"接続先DB: host={cfg['host']} dbname={cfg['dbname']} user={cfg['user']}")
    if cfg["dbname"] != "postgres":
        raise RuntimeError(f"安全停止: dbname={cfg['dbname']} (postgres のみ許可)")

    conn = psycopg2.connect(**cfg)
    try:
        with conn.cursor() as cur:
            parent_map = load_parent_map(cur, parent_numbers, chunk_size)
            parent_industry_map = load_parent_industry_map(cur, sorted(set(parent_map.values())), chunk_size)
            name_to_id, corp_to_id = load_child_maps(cur, child_names, child_corps, chunk_size)
            existing = load_existing_relations(cur, sorted(set(parent_map.values())), chunk_size)

            dedup: dict[tuple[str, str], Candidate] = {}
            stats = {"input": 0, "parent_linked": 0, "child_linked": 0}

            for row in rows:
                stats["input"] += 1
                pcorp = (row.get("親会社法人番号") or "").strip()
                cname_raw = (row.get("子会社名") or "").strip()
                cname = normalize_child_name(cname_raw)
                if not pcorp or not cname:
                    continue
                parent_company_id = parent_map.get(pcorp)
                if not parent_company_id:
                    continue
                stats["parent_linked"] += 1

                child_corp = first_present(row, ["子会社法人番号", "child_corporate_number", "childCorporateNumber"]) or None
                child_company_id = corp_to_id.get(child_corp or "") if child_corp else None
                if child_company_id is None:
                    child_company_id = name_to_id.get(cname)
                if child_company_id:
                    stats["child_linked"] += 1

                cand = Candidate(
                    parent_company_id=parent_company_id,
                    child_company_id=child_company_id,
                    child_company_name=cname,
                    child_name_normalized=normalize_key(cname),
                    relation_type=((row.get("関係区分") or "").strip() or None),
                    voting_rights_ratio=normalize_ratio(row.get("議決権所有比率") or ""),
                    source_doc_id=((row.get("出典docID") or "").strip() or None),
                    child_corporate_number=child_corp,
                )
                k = (cand.parent_company_id, cand.child_name_normalized)
                dedup[k] = pick_better(dedup[k], cand) if k in dedup else cand

            insert_records: list[tuple] = []
            update_records: list[tuple] = []
            master_inserted = 0

            # corporate_number で未登録の子会社を、条件を満たす場合のみ companies に自動登録
            master_candidates: dict[str, tuple[str, IndustryTriple]] = {}
            for cand in dedup.values():
                child_corp = (cand.child_corporate_number or "").strip()
                if not child_corp:
                    continue
                if child_corp in corp_to_id:
                    continue
                if not should_register_master(cand.relation_type, cand.voting_rights_ratio):
                    continue
                if child_corp not in master_candidates:
                    parent_industry = parent_industry_map.get(cand.parent_company_id)
                    guessed = guess_industry(cand.child_company_name, parent_industry, industry_master)
                    master_candidates[child_corp] = (cand.child_company_name, guessed)

            if master_candidates:
                execute_values(
                    cur,
                    """
                    INSERT INTO companies (
                        name,
                        corporate_number,
                        primary_industry,
                        secondary_industry,
                        tertiary_industry
                    )
                    VALUES %s
                    ON CONFLICT (corporate_number) DO NOTHING
                    """,
                    [
                        (name, corp, inds.primary, inds.secondary, inds.tertiary)
                        for corp, (name, inds) in master_candidates.items()
                    ],
                    page_size=1000,
                )
                master_inserted = cur.rowcount if cur.rowcount > 0 else 0

                all_target_corps = sorted(set(child_corps) | set(master_candidates.keys()))
                _, refreshed_corp_to_id = load_child_maps(cur, [], all_target_corps, chunk_size)
                corp_to_id.update(refreshed_corp_to_id)

            # corporate_number があるものは、最新マップで child_company_id を確定させる
            for cand in dedup.values():
                child_corp = (cand.child_corporate_number or "").strip()
                if child_corp and child_corp in corp_to_id:
                    cand.child_company_id = corp_to_id[child_corp]

            for key, cand in dedup.items():
                ex = existing.get(key)
                if ex is None:
                    insert_records.append(
                        (
                            cand.parent_company_id,
                            cand.child_company_id,
                            cand.child_company_name,
                            cand.relation_type,
                            cand.voting_rights_ratio,
                            cand.source_doc_id,
                            cand.child_corporate_number,
                        )
                    )
                    continue

                new_relation_type = ex["relation_type"] if ex["relation_type"] is not None else cand.relation_type
                new_voting_ratio = ex["voting_rights_ratio"] if ex["voting_rights_ratio"] is not None else cand.voting_rights_ratio
                new_source_doc = ex["source_doc_id"]
                new_child_company_id = ex["child_company_id"]
                new_child_corp = ex["child_corporate_number"]
                updated = False

                if ex["relation_type"] is None and cand.relation_type is not None:
                    updated = True
                if ex["voting_rights_ratio"] is None and cand.voting_rights_ratio is not None:
                    updated = True
                if ex["child_company_id"] is None and cand.child_company_id is not None:
                    updated = True

                if not updated:
                    continue

                # 補完が発生した場合のみ、付随情報もNULLなら埋める
                if new_source_doc is None and cand.source_doc_id is not None:
                    new_source_doc = cand.source_doc_id
                if new_child_company_id is None and cand.child_company_id is not None:
                    new_child_company_id = cand.child_company_id
                if new_child_corp is None and cand.child_corporate_number is not None:
                    new_child_corp = cand.child_corporate_number

                update_records.append(
                    (
                        new_relation_type,
                        new_voting_ratio,
                        new_source_doc,
                        new_child_company_id,
                        new_child_corp,
                        cand.parent_company_id,
                        ex["child_company_name"],
                    )
                )

            if insert_records:
                execute_values(
                    cur,
                    """
                    INSERT INTO company_relations (
                        parent_company_id,
                        child_company_id,
                        child_company_name,
                        relation_type,
                        voting_rights_ratio,
                        source_doc_id,
                        child_corporate_number
                    ) VALUES %s
                    """,
                    insert_records,
                    page_size=1000,
                )

            if update_records:
                execute_values(
                    cur,
                    """
                    UPDATE company_relations AS cr
                    SET
                        relation_type = v.relation_type,
                        voting_rights_ratio = v.voting_rights_ratio,
                        source_doc_id = v.source_doc_id,
                        child_company_id = v.child_company_id,
                        child_corporate_number = v.child_corporate_number
                    FROM (
                        VALUES %s
                    ) AS v(
                        relation_type,
                        voting_rights_ratio,
                        source_doc_id,
                        child_company_id,
                        child_corporate_number,
                        parent_company_id,
                        child_company_name
                    )
                    WHERE cr.parent_company_id = v.parent_company_id
                      AND cr.child_company_name = v.child_company_name
                    """,
                    update_records,
                    page_size=1000,
                )

            conn.commit()
            print(
                f"完了 input={stats['input']} parent_linked={stats['parent_linked']} child_linked={stats['child_linked']} "
                f"inserted={len(insert_records)} updated={len(update_records)} master_inserted={master_inserted}"
            )
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="EDINET抽出CSVを既存 company_relations にマージ")
    parser.add_argument("--input-csv", required=True, help="例: data/edinet_relations_xbrl_structured_full.csv")
    parser.add_argument("--chunk-size", type=int, default=1000, help="IN句の分割サイズ")
    parser.add_argument("--industries-csv", default=None, help="業種マスターCSV。未指定時は scripts/industries.csv を優先")
    args = parser.parse_args()
    run(args.input_csv, chunk_size=args.chunk_size, industries_csv=args.industries_csv)


if __name__ == "__main__":
    main()
