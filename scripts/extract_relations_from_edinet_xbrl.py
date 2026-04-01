#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import io
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from xml.etree import ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EDINET_MASTER = ROOT / "edinet" / "EdinetcodeDlInfo 2.csv"
DEFAULT_ZIP_DIR = ROOT / "edinet_data"
DEFAULT_OUTPUT = ROOT / "data" / "edinet_relations_xbrl_structured.csv"

EDINET_CODE_RE = re.compile(r"(E\d{5})-\d{3}", re.IGNORECASE)
RATIO_RE = re.compile(r"(\d{1,3}(?:\.\d+)?)\s*[%％]?")

NAME_HINTS = (
    "subsidiary",
    "affiliate",
    "associatedcompany",
    "relatedcompany",
    "name",
)
RATIO_HINTS = ("voting", "ownership", "ratio", "percent", "equitymethod")
RELATION_HINTS = (
    "consolidated",
    "nonconsolidated",
    "equitymethod",
    "affiliate",
    "subsidiary",
    "relatedcompany",
    "associatedcompany",
)


@dataclass
class RawRelation:
    parent_edinet_code: str
    parent_corporate_number: str
    child_company_name: str
    relation_category: str
    voting_rights_ratio: str
    source_doc_id: str


def normalize_company_name(name: str) -> str:
    s = (name or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"（注[^）]*）|\(注[^)]*\)", "", s)
    return s.strip(" \t\r\n,，。")


def is_valid_child_name(name: str) -> bool:
    s = (name or "").strip()
    if not s:
        return False
    if len(s) < 2 or len(s) > 120:
        return False
    if any(x in s for x in ("<", ">", "style=", "src=", "http://", "https://")):
        return False
    ng_keywords = ("ファンドの仕組み", "投資対象", "運用方針", "信託", "提出会社", "注記")
    if any(k in s for k in ng_keywords):
        return False
    company_markers = ("株式会社", "有限会社", "合同会社", "Inc", "Ltd", "Co.", "Company", "GmbH", "B.V.", "Corp")
    return any(m in s for m in company_markers)


def normalize_ratio(text: str) -> str:
    m = RATIO_RE.search((text or "").strip())
    return m.group(1) if m else ""


def classify_relation(text: str, concept: str = "") -> str:
    t = f"{text} {concept}".lower()
    if "equitymethod" in t or "持分法" in t:
        return "持分法適用関連会社"
    if "nonconsolidated" in t or "非連結" in t:
        return "非連結子会社"
    if "consolidated" in t or "連結子会社" in t:
        return "連結子会社"
    if "affiliate" in t or "関連会社" in t:
        return "関連会社"
    if "subsidiary" in t or "子会社" in t:
        return "子会社"
    return ""


def load_edinet_master(path: Path) -> dict[str, str]:
    for enc in ("utf-8-sig", "cp932", "shift_jis", "utf-8"):
        try:
            with path.open("r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                rows = list(reader)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"エンコーディング判定失敗: {path}")

    if len(rows) < 3:
        return {}
    header = rows[1]
    body = rows[2:]
    idx_edinet = next((i for i, h in enumerate(header) if "ＥＤＩＮＥＴコード" in h or "EDINETコード" in h), None)
    idx_corp = next((i for i, h in enumerate(header) if "法人番号" in h), None)
    if idx_edinet is None or idx_corp is None:
        raise ValueError("EDINETマスタの必要列が見つかりません")

    out: dict[str, str] = {}
    for row in body:
        if len(row) <= max(idx_edinet, idx_corp):
            continue
        edinet = row[idx_edinet].strip()
        corp = row[idx_corp].strip().split(".")[0]
        if edinet and corp and corp.isdigit():
            out[edinet] = corp
    return out


def extract_text_from_xmlish(content: bytes) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp932", "shift_jis"):
        try:
            return content.decode(enc, errors="ignore")
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="ignore")


def strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", " ", s)


def parse_ixbrl_regex(text: str) -> list[tuple[str, str, str]]:
    pattern = re.compile(
        r"<ix:(?:nonNumeric|nonFraction)\b[^>]*\bname=\"([^\"]+)\"[^>]*\bcontextRef=\"([^\"]+)\"[^>]*>(.*?)</ix:(?:nonNumeric|nonFraction)>",
        re.IGNORECASE | re.DOTALL,
    )
    facts: list[tuple[str, str, str]] = []
    for m in pattern.finditer(text):
        name = m.group(1).split(":")[-1]
        context = m.group(2)
        value = normalize_company_name(strip_tags(m.group(3)))
        if value:
            facts.append((name, context, value))
    return facts


def parse_xbrl_xml(content: bytes) -> list[tuple[str, str, str]]:
    facts: list[tuple[str, str, str]] = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return facts
    for elem in root.iter():
        context_ref = elem.attrib.get("contextRef", "")
        if not context_ref:
            continue
        tag = elem.tag.split("}")[-1]
        text = normalize_company_name(elem.text or "")
        if not text:
            continue
        facts.append((tag, context_ref, text))
    return facts


def looks_like_name(concept: str, value: str) -> bool:
    c = concept.lower()
    if any(h in c for h in ("voting", "ratio", "percent", "ownership", "number", "amount", "date")):
        return False
    if any(h in c for h in NAME_HINTS):
        return True
    return any(k in value for k in ("株式会社", "有限会社", "合同会社", "Inc", "Ltd", "Co.", "Company"))


def group_relations_from_facts(
    facts: Iterable[tuple[str, str, str]],
    parent_edinet_code: str,
    parent_corp: str,
    doc_id: str,
) -> list[RawRelation]:
    by_ctx: dict[str, list[tuple[str, str]]] = {}
    for concept, ctx, val in facts:
        by_ctx.setdefault(ctx, []).append((concept, val))

    out: list[RawRelation] = []
    for items in by_ctx.values():
        names: list[str] = []
        relation_tokens: list[str] = []
        ratio = ""
        for concept, val in items:
            c = concept.lower()
            if any(h in c for h in RELATION_HINTS) or any(k in val for k in ("子会社", "関連会社", "持分法", "連結")):
                relation_tokens.append(f"{concept}:{val}")
            if any(h in c for h in RATIO_HINTS):
                ratio = ratio or normalize_ratio(val)
            if looks_like_name(concept, val):
                if len(val) >= 2:
                    names.append(val)
        relation_category = classify_relation(" ".join(relation_tokens))
        for n in names:
            nn = normalize_company_name(n)
            if not is_valid_child_name(nn):
                continue
            out.append(
                RawRelation(
                    parent_edinet_code=parent_edinet_code,
                    parent_corporate_number=parent_corp,
                    child_company_name=nn,
                    relation_category=relation_category,
                    voting_rights_ratio=ratio,
                    source_doc_id=doc_id,
                )
            )
    return out


def infer_edinet_code_from_members(members: Iterable[str]) -> str:
    for m in members:
        hit = EDINET_CODE_RE.search(m)
        if hit:
            return hit.group(1).upper()
    return ""


def extract_from_zip(zip_path: Path, edinet_to_corp: dict[str, str]) -> list[RawRelation]:
    doc_id = zip_path.stem
    rows: list[RawRelation] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        parent_edinet_code = infer_edinet_code_from_members(members)
        if not parent_edinet_code:
            return rows
        parent_corp = edinet_to_corp.get(parent_edinet_code, "")
        if not parent_corp:
            return rows

        all_facts: list[tuple[str, str, str]] = []
        for m in members:
            lower = m.lower()
            if lower.endswith(".xbrl") or lower.endswith(".xml"):
                data = zf.read(m)
                all_facts.extend(parse_xbrl_xml(data))
            elif lower.endswith(".htm") or lower.endswith(".html"):
                # iXBRL本文からfactsを拾う（XMLとして壊れているケースを想定して正規表現）
                text = extract_text_from_xmlish(zf.read(m))
                all_facts.extend(parse_ixbrl_regex(text))

        rows.extend(group_relations_from_facts(all_facts, parent_edinet_code, parent_corp, doc_id))
    return rows


def dedupe_rows(rows: list[RawRelation]) -> list[RawRelation]:
    uniq: dict[tuple[str, str, str, str, str], RawRelation] = {}
    for r in rows:
        key = (
            r.parent_corporate_number,
            r.child_company_name,
            r.relation_category,
            r.voting_rights_ratio,
            r.source_doc_id,
        )
        if key not in uniq:
            uniq[key] = r
    return list(uniq.values())


def write_csv(rows: list[RawRelation], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "親会社法人番号",
                "子会社名",
                "関係区分",
                "議決権所有比率",
                "出典docID",
                "親会社EDINETコード",
            ]
        )
        for r in rows:
            writer.writerow(
                [
                    r.parent_corporate_number,
                    r.child_company_name,
                    r.relation_category,
                    r.voting_rights_ratio,
                    r.source_doc_id,
                    r.parent_edinet_code,
                ]
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="edinet_data ZIP/XBRLから関係会社データを構造抽出")
    parser.add_argument("--zip-dir", default=str(DEFAULT_ZIP_DIR), help="対象ZIPディレクトリ")
    parser.add_argument("--edinet-master", default=str(DEFAULT_EDINET_MASTER), help="EDINETマスタCSV")
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT), help="抽出結果CSV")
    parser.add_argument("--limit", type=int, default=0, help="テスト用: 先頭Nファイルのみ処理（0=全件）")
    args = parser.parse_args()

    zip_dir = Path(args.zip_dir)
    master = Path(args.edinet_master)
    output = Path(args.output_csv)

    edinet_to_corp = load_edinet_master(master)
    zip_paths = sorted(zip_dir.glob("*.zip"))
    if args.limit > 0:
        zip_paths = zip_paths[: args.limit]

    all_rows: list[RawRelation] = []
    total = len(zip_paths)
    print(f"ZIP解析開始: {total} files")
    for i, zp in enumerate(zip_paths, start=1):
        try:
            rows = extract_from_zip(zp, edinet_to_corp)
            all_rows.extend(rows)
        except Exception as e:
            print(f"[WARN] {zp.name}: {e}")
        if i % 25 == 0 or i == total:
            print(f"progress {i}/{total} extracted={len(all_rows)}")

    deduped = dedupe_rows(all_rows)
    write_csv(deduped, output)
    print(f"done: rows={len(deduped)} output={output}")


if __name__ == "__main__":
    main()
