#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
companies 全件をチャンク単位で走査し、クレンジングを行う（省メモリ・高スループット向け）。

  1. CSV 照合: 多重マッチ（repair と同じ）でヒット → 空欄のみ CSV で補完（社名は 13 桁法人番号のときのみ差し替え）
  2. Layer 4: CSV 未ヒット時のみ — 法人番号 1 桁差、社名レーベンシュタイン（都道府県＋頭文字バケットで候補絞込）
  3. Layer 5: 代表者=郵便番号（厳密形式＋ハイフン混入の数字主体）、住所異常、社名=法人番号プレースホルダ
  4. Industry Rescue: 業種列から電話・代表者・住所を救出（--apply 時は industries.csv 非該当値を NULL 化）
  6. 法人番号衝突: np~ が正規行と同じ法人番号になる場合は正規行を補完し np~ を DELETE

  --chunk-size / --batch-size で fetchmany 件数（既定 10000）。--apply 時は約 1 万件ごとにコミット。
  速度: CSV 照合済みで空欄補充も Layer5/業種シグナルも無い行はスキップ。DB 反映は一括 UPDATE（マージのみ個別）。
  接続先（DATABASE_URL 等）を print するデバッグ出力は行いません。
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import unicodedata
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import repair_companies_column_shift_from_fixed_csv3 as rep
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import repair_companies_column_shift_from_fixed_csv3 as rep

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError as e:
    raise SystemExit("psycopg2 が必要です: pip install psycopg2-binary") from e

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

NAME_PLACEHOLDER = "（社名未確定・法人番号参照）"
DEFAULT_FUZZY_NAME_MAX = 1
DEFAULT_CHUNK_SIZE = 10_000

# 全件 SELECT の列（順序固定）。名前付きカーソルでは execute 直後 description が None になり得るため、ここを正とする。
_COMPANY_COL_NAMES: Tuple[str, ...] = (
    "id",
    "name",
    "corporate_number",
    "representative_name",
    "postal_code",
    "email",
    "address",
    "phone_number",
    "industry_large",
    "industry_middle",
    "industry_small",
    "industry_detail",
    "industries",
    "prefecture",
    "overview",
    "business_descriptions",
)
_COMPANY_SELECT_SQL = "SELECT " + ", ".join(_COMPANY_COL_NAMES) + " FROM companies"

RE_PHONE = re.compile(r"^0[0-9-]{9,13}$")
RE_EMAIL = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
# （有限代表）（職務執行）で始まる役職名、および従来の代表・役員表記
RE_REP = re.compile(
    r"^(\(|（)(有限代表|職務執行)"
    r"|^(\(|（)?(代表|取締役|会長|社長|理事|監事)"
)
# 「…道」一文字誤爆を避け、先頭が都道府県名であることのみ（○○県は漢字2〜3字＋県）
RE_ADDR = re.compile(r"^(東京都|北海道|京都府|大阪府|[一-龠]{2,3}県)")
# 概要文内の代表表現から氏名候補を抽出
RE_OVERVIEW_REP = re.compile(
    r"(?:代表(?:者|取締役)?\s*[:：は]\s*|代表の)\s*"
    r"([一-龠々〆ヵヶぁ-んァ-ヶーA-Za-z]{1,10})"
)
INVALID_INDUSTRY_VALUES = {"0", "なし", "unknown", "未分類", "-", "None", "null", "‐", "ー", "調査中"}
INDUSTRY_ALIAS_MAP = {
    "労働者派遣業": "人材派遣業",
    "人材派遣": "人材派遣業",
    "その他 その他": "その他",
    "各種学校": "その他サービス業",
    "語学学校": "その他サービス業",
    "システム開発": "情報サービス業",
    "ソフトウェア開発": "情報サービス業",
    "コンサルティング": "その他専門サービス業",
    "経営コンサル": "その他専門サービス業",
}


@dataclass(frozen=True)
class IndustryTriple:
    large: str
    middle: str
    small: str


@dataclass
class IndustryMasterContext:
    triples: List[IndustryTriple]
    valid_combos: Set[Tuple[str, str, str]]
    token_to_triple: Dict[str, IndustryTriple]
    small_token_to_triple: Dict[str, IndustryTriple]
    middle_token_to_triple: Dict[str, IndustryTriple]
    default_triple: IndustryTriple


def _find_industries_csv() -> Path:
    """業種マスタはリポジトリ正本の data/industries.csv を最優先する。"""
    here = Path(__file__).resolve().parent
    for p in (
        here.parent / "data" / "industries.csv",
        here / "industries.csv",
        here.parent / "industries.csv",
    ):
        if p.is_file():
            return p
    raise FileNotFoundError(
        "industries.csv が見つかりません（data/industries.csv を配置してください）"
    )


def load_industry_whitelist_from_csv(path: Path) -> Set[str]:
    """industries.csv の large/middle/small 列に現れる値の集合（完全一致用）。"""
    out: Set[str] = set()
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            for key in ("industryLarge", "industryMiddle", "industrySmall"):
                v = str(row.get(key) or "").strip()
                if v:
                    out.add(v)
    return out


def _norm_industry_token(s: str) -> str:
    n = unicodedata.normalize("NFKC", s).strip().lower()
    n = n.replace(" ", "").replace("　", "")
    n = n.replace("・", "").replace("／", "/")
    return n


def load_industry_master_context(path: Path) -> IndustryMasterContext:
    triples: List[IndustryTriple] = []
    token_to_triple: Dict[str, IndustryTriple] = {}
    small_token_to_triple: Dict[str, IndustryTriple] = {}
    middle_token_to_triple: Dict[str, IndustryTriple] = {}
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            large = str(row.get("industryLarge") or "").strip()
            middle = str(row.get("industryMiddle") or "").strip()
            small = str(row.get("industrySmall") or "").strip()
            if not (large and middle and small):
                continue
            t = IndustryTriple(large=large, middle=middle, small=small)
            triples.append(t)
            for v in (large, middle, small):
                token_to_triple.setdefault(_norm_industry_token(v), t)
            small_token_to_triple.setdefault(_norm_industry_token(small), t)
            middle_token_to_triple.setdefault(_norm_industry_token(middle), t)
    if not triples:
        raise RuntimeError("industries.csv に有効な業種階層がありません")

    default_triple = next(
        (t for t in triples if (t.large, t.middle, t.small) == ("サービス業", "他サービス業", "その他サービス業")),
        triples[0],
    )
    valid_combos = {(t.large, t.middle, t.small) for t in triples}
    return IndustryMasterContext(
        triples=triples,
        valid_combos=valid_combos,
        token_to_triple=token_to_triple,
        small_token_to_triple=small_token_to_triple,
        middle_token_to_triple=middle_token_to_triple,
        default_triple=default_triple,
    )


def _row_has_unknown_industry(db: Dict[str, Any], allowed: Set[str]) -> bool:
    """いずれかの業種列に、空でなくマスタに無い値があるか。"""
    for col in ("industry_large", "industry_middle", "industry_small", "industry_detail"):
        v = str(db.get(col) or "").strip()
        if v and v not in allowed:
            return True
    return False


def _null_industry_not_in_whitelist(db: Dict[str, Any], p: CleanupPatch, allowed: Set[str]) -> None:
    """業種4列のうち、マスタに無い値は NULL 化（パッチに反映）。"""
    hit = False
    for col in ("industry_large", "industry_middle", "industry_small"):
        cur = getattr(p, col)
        if cur is not None:
            v = str(cur).strip()
            if v == "":
                continue
            if v not in allowed:
                setattr(p, col, "")
                hit = True
        else:
            v = str(db.get(col) or "").strip()
            if not v:
                continue
            if v not in allowed:
                setattr(p, col, "")
                hit = True
    if hit:
        p.layers.append("industry:whitelist_null")


def _parse_industries_tokens(raw: Any) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    s = str(raw).strip()
    if not s:
        return []
    # JSON array
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if str(x).strip()]
        except Exception:
            pass
    # PostgreSQL text[] literal
    if s.startswith("{") and s.endswith("}"):
        body = s[1:-1].strip()
        if body:
            return [x.strip().strip('"') for x in body.split(",") if x.strip().strip('"')]
    parts = re.split(r"[、,;/\|]", s)
    return [x.strip() for x in parts if x.strip()]


def _pick_industry_triple_from_tokens(tokens: List[str], ctx: IndustryMasterContext) -> Optional[IndustryTriple]:
    # 1) exact/alias match（小分類完全一致を最優先）
    for tok in tokens:
        alias = INDUSTRY_ALIAS_MAP.get(tok, tok)
        key = _norm_industry_token(alias)
        t = ctx.small_token_to_triple.get(key)
        if t:
            return t
    # 2) exact/alias match（中分類）
    for tok in tokens:
        alias = INDUSTRY_ALIAS_MAP.get(tok, tok)
        key = _norm_industry_token(alias)
        t = ctx.middle_token_to_triple.get(key)
        if t:
            return t
    # 3) exact/alias match（全体）
    for tok in tokens:
        alias = INDUSTRY_ALIAS_MAP.get(tok, tok)
        key = _norm_industry_token(alias)
        t = ctx.token_to_triple.get(key)
        if t:
            return t
    # 4) partial contains（小分類優先）
    for tok in tokens:
        alias = INDUSTRY_ALIAS_MAP.get(tok, tok)
        key = _norm_industry_token(alias)
        if not key:
            continue
        for k, t in ctx.small_token_to_triple.items():
            if key in k or k in key:
                return t
    # 5) partial contains（中分類）
    for tok in tokens:
        alias = INDUSTRY_ALIAS_MAP.get(tok, tok)
        key = _norm_industry_token(alias)
        if not key:
            continue
        for k, t in ctx.middle_token_to_triple.items():
            if key in k or k in key:
                return t
    # 6) partial contains（全体）
    for tok in tokens:
        alias = INDUSTRY_ALIAS_MAP.get(tok, tok)
        key = _norm_industry_token(alias)
        if not key:
            continue
        for k, t in ctx.token_to_triple.items():
            if key in k or k in key:
                return t
    # 7) fuzzy best (high confidence only)
    best_t: Optional[IndustryTriple] = None
    best_score = 0.0
    for tok in tokens:
        alias = INDUSTRY_ALIAS_MAP.get(tok, tok)
        key = _norm_industry_token(alias)
        if not key:
            continue
        for k, t in ctx.token_to_triple.items():
            score = SequenceMatcher(None, key, k).ratio()
            if score > best_score:
                best_score = score
                best_t = t
    return best_t if best_score >= 0.82 else None


def _pick_default_triple_for_large(large: str, ctx: IndustryMasterContext) -> Optional[IndustryTriple]:
    cands = [t for t in ctx.triples if t.large == large]
    if not cands:
        return None
    for t in cands:
        if "その他" in t.small:
            return t
    for t in cands:
        if "その他" in t.middle:
            return t
    return cands[0]


def _merge_detail_values(existing: str, extras: List[str]) -> str:
    out: List[str] = []
    seen: Set[str] = set()
    for src in [existing] + extras:
        if not src:
            continue
        for part in re.split(r"[、,;/\|]", src):
            v = part.strip()
            if not v:
                continue
            key = _norm_industry_token(v)
            if key in seen:
                continue
            seen.add(key)
            out.append(v)
    return "、".join(out)


def _current_industry_value(db: Dict[str, Any], p: CleanupPatch, col: str) -> str:
    v = getattr(p, col)
    if v is None:
        return str(db.get(col) or "").strip()
    return str(v).strip()


def enforce_industry_master_consistency(db: Dict[str, Any], p: CleanupPatch, ctx: IndustryMasterContext) -> None:
    large = _current_industry_value(db, p, "industry_large")
    middle = _current_industry_value(db, p, "industry_middle")
    small = _current_industry_value(db, p, "industry_small")
    detail = _current_industry_value(db, p, "industry_detail")

    is_valid_combo = bool(large and middle and small and (large, middle, small) in ctx.valid_combos)
    need_fix = not is_valid_combo
    if not need_fix:
        return

    tokens: List[str] = []
    for x in (large, middle, small):
        if x:
            tokens.append(x)
    detail_tokens = _parse_industries_tokens(detail)
    tokens.extend(detail_tokens)
    industry_tags = _parse_industries_tokens(db.get("industries"))
    tokens.extend(industry_tags)
    triple: Optional[IndustryTriple] = None
    if not industry_tags and large and (not middle or not small):
        triple = _pick_default_triple_for_large(large, ctx)
    if triple is None:
        triple = _pick_industry_triple_from_tokens(tokens, ctx)

    detail_candidates: List[str] = []
    candidate_specific_tokens = detail_tokens + industry_tags
    if triple is not None:
        for tok in tokens:
            alias = INDUSTRY_ALIAS_MAP.get(tok, tok)
            n = _norm_industry_token(alias)
            if not n:
                continue
            # 採用トリプル（大中小）そのものは detail へ入れない
            if n in {
                _norm_industry_token(triple.large),
                _norm_industry_token(triple.middle),
                _norm_industry_token(triple.small),
            }:
                continue
            # マスター外（具体的タグ）を detail 退避対象とする
            if n not in ctx.token_to_triple:
                detail_candidates.append(tok)
        # タグ/detail の具体語は、マスター採用語でない限り残す（情報欠落防止）
        adopted = {
            _norm_industry_token(triple.large),
            _norm_industry_token(triple.middle),
            _norm_industry_token(triple.small),
        }
        for tok in candidate_specific_tokens:
            if _norm_industry_token(tok) not in adopted:
                detail_candidates.append(tok)
    else:
        triple = ctx.default_triple
        for tok in tokens:
            if _norm_industry_token(tok) not in ctx.token_to_triple:
                detail_candidates.append(tok)
        if detail_candidates:
            p.layers.append("industry:tag_to_detail_fallback")

    merged_detail = _merge_detail_values(detail, detail_candidates)
    if merged_detail and merged_detail != detail:
        p.industry_detail = merged_detail
        p.layers.append("industry:detail_preserved")
    p.industry_large = triple.large
    p.industry_middle = triple.middle
    p.industry_small = triple.small
    p.layers.append("industry:master_consistent")


# 代表者欄の「郵便番号残骸」（ハイフンあり・数字主体）
_RE_POSTAL_IN_TEXT = re.compile(r"(\d{3})[-－]?(\d{4})")


@dataclass
class CleanupPatch:
    id: str
    name: Optional[str] = None
    corporate_number: Optional[str] = None
    representative_name: Optional[str] = None
    postal_code: Optional[str] = None
    email: Optional[str] = None
    address: Optional[str] = None
    phone_number: Optional[str] = None
    overview: Optional[str] = None
    business_descriptions: Optional[str] = None
    industry_large: Optional[str] = None
    industry_middle: Optional[str] = None
    industry_small: Optional[str] = None
    industry_detail: Optional[str] = None
    layers: List[str] = field(default_factory=list)


def has_sql_changes(p: CleanupPatch) -> bool:
    cols = (
        "name",
        "corporate_number",
        "representative_name",
        "postal_code",
        "email",
        "address",
        "phone_number",
        "overview",
        "business_descriptions",
        "industry_large",
        "industry_middle",
        "industry_small",
        "industry_detail",
    )
    return any(getattr(p, f) is not None for f in cols)


# --- Levenshtein（早期打ち切り）---

def levenshtein_distance(a: str, b: str, max_d: int) -> int:
    la, lb = len(a), len(b)
    if abs(la - lb) > max_d:
        return max_d + 1
    if max_d == 0:
        return 0 if a == b else max_d + 1
    dp = list(range(lb + 1))
    for i in range(1, la + 1):
        ndp = [i]
        row_min = i
        ai = a[i - 1]
        for j in range(1, lb + 1):
            cost = 0 if ai == b[j - 1] else 1
            v = min(dp[j] + 1, ndp[j - 1] + 1, dp[j - 1] + cost)
            ndp.append(v)
            if v < row_min:
                row_min = v
        dp = ndp
        if row_min > max_d:
            return max_d + 1
    return dp[-1] if dp[-1] <= max_d else max_d + 1


def corp_one_substitution_variants(thirteen: str) -> List[str]:
    if len(thirteen) != 13 or not thirteen.isdigit():
        return []
    out: List[str] = []
    for i in range(13):
        for d in "0123456789":
            if d == thirteen[i]:
                continue
            out.append(thirteen[:i] + d + thirteen[i + 1 :])
    return out


# pref -> 正規化社名の先頭文字 -> [(norm_name, CsvRow), ...]
def build_pref_firstchar_index(lookup: rep.CsvLookup) -> Dict[str, Dict[str, List[Tuple[str, rep.CsvRow]]]]:
    idx: Dict[str, Dict[str, List[Tuple[str, rep.CsvRow]]]] = {}
    seen: Set[int] = set()
    for row in lookup.by_simple_id.values():
        oid = id(row)
        if oid in seen:
            continue
        seen.add(oid)
        p = rep.normalize_prefecture_for_hash(row.prefecture)
        nn = rep.normalize_company_name_for_hash(row.name)
        if not nn:
            continue
        ch0 = nn[0]
        idx.setdefault(p, {}).setdefault(ch0, []).append((nn, row))
    return idx


def _subfilter_by_second_char(
    candidates: List[Tuple[str, rep.CsvRow]], db_nn: str, max_bucket: int
) -> List[Tuple[str, rep.CsvRow]]:
    if len(candidates) <= max_bucket or len(db_nn) < 2:
        return candidates
    p2 = db_nn[1]
    sub = [t for t in candidates if len(t[0]) >= 2 and t[0][1] == p2]
    return sub if sub else candidates


def find_fuzzy_name_match(
    db_row: Dict[str, Any],
    pref_firstchar_index: Dict[str, Dict[str, List[Tuple[str, rep.CsvRow]]]],
    max_d: int,
    max_bucket_scan: int = 400,
) -> Optional[rep.CsvRow]:
    db_nn = rep.normalize_company_name_for_hash(str(db_row.get("name") or ""))
    if len(db_nn) < 2:
        return None
    db_p = rep.normalize_prefecture_for_hash(str(db_row.get("prefecture") or ""))
    bucket = pref_firstchar_index.get(db_p)
    if not bucket:
        return None
    ch0 = db_nn[0]
    candidates = bucket.get(ch0)
    if not candidates:
        return None
    candidates = _subfilter_by_second_char(candidates, db_nn, max_bucket_scan)

    best: Optional[rep.CsvRow] = None
    best_d = max_d + 1
    ln = len(db_nn)
    for nn, crow in candidates:
        if abs(ln - len(nn)) > max_d:
            continue
        d = levenshtein_distance(db_nn, nn, max_d)
        if d < best_d:
            best_d = d
            best = crow
            if d == 0:
                break
    return best if best_d <= max_d else None


def patch_csv_match_fill_only(db: Dict[str, Any], c: rep.CsvRow, match_tag: str) -> CleanupPatch:
    """CSV 照合済み: 空欄補完と 13 桁社名の救済のみ（不要な社名差し替えを避ける）。"""
    p = CleanupPatch(id=db["id"], layers=[])
    if rep.is_name_spilled_corporate_number(db.get("name")):
        p.name = c.name
        p.layers.append(f"csv:{match_tag}:name_corp_spill")
        if rep.empty_db_value(db.get("corporate_number")):
            p.corporate_number = str(db["name"]).strip()
    for f in (
        "corporate_number",
        "representative_name",
        "postal_code",
        "address",
        "business_descriptions",
    ):
        csv_val = getattr(c, f)
        if f == "representative_name" and csv_val and is_email_value(csv_val):
            if rep.empty_db_value(db.get("email")):
                p.email = str(csv_val).strip()
                p.layers.append(f"csv:{match_tag}:fill_email_from_representative")
            continue
        if csv_val and rep.empty_db_value(db.get(f)):
            setattr(p, f, csv_val)
            p.layers.append(f"csv:{match_tag}:fill_{f}")
    return p


def patch_from_csv_row(db: Dict[str, Any], c: rep.CsvRow, layer_tag: str) -> CleanupPatch:
    """Layer 4 ヒット時: 従来どおり CSV を正とするパッチ。"""
    p = CleanupPatch(id=db["id"], layers=[layer_tag])
    if rep.is_name_spilled_corporate_number(db.get("name")):
        p.name = c.name
        if rep.empty_db_value(db.get("corporate_number")):
            p.corporate_number = str(db["name"]).strip()
    elif (db.get("name") or "").strip() != c.name:
        p.name = c.name
    for f in (
        "corporate_number",
        "representative_name",
        "postal_code",
        "address",
        "industry_middle",
        "overview",
        "business_descriptions",
    ):
        csv_val = getattr(c, f)
        if f == "representative_name" and csv_val and is_email_value(csv_val):
            if rep.empty_db_value(db.get("email")):
                p.email = str(csv_val).strip()
            continue
        if csv_val and rep.empty_db_value(db.get(f)):
            setattr(p, f, csv_val)
    return p


def merge_patch(base: CleanupPatch, extra: CleanupPatch) -> None:
    cols = (
        "name",
        "corporate_number",
        "representative_name",
        "postal_code",
        "email",
        "address",
        "phone_number",
        "overview",
        "business_descriptions",
        "industry_large",
        "industry_middle",
        "industry_small",
        "industry_detail",
    )
    for f in cols:
        v = getattr(extra, f)
        if v is not None:
            setattr(base, f, v)
    base.layers.extend(extra.layers)


def representative_name_is_hyphen_postal_spill(raw: Any) -> bool:
    """
    代表者名に `-` があり、数字とハイフンが主体で、それ以外は最大 3 文字まで（前後のノイズ想定）のとき True。
    和字が含まれる場合は人名とみなして False。
    """
    if rep.empty_db_value(raw):
        return False
    s = str(raw).strip()
    if "-" not in s:
        return False
    n = unicodedata.normalize("NFKC", s).replace("－", "-")
    if "-" not in n:
        return False
    if len(n) > 48:
        return False
    digits = sum(c.isdigit() for c in n)
    if digits < 7:
        return False
    noise = 0
    for c in n:
        if c.isdigit() or c in "-‐ \t　":
            continue
        if c in "〒・.":
            noise += 1
        elif c.isalpha() and ord(c) < 128:
            noise += 1
        elif "\u3040" <= c <= "\u30ff" or "\u4e00" <= c <= "\u9fff" or "\u3400" <= c <= "\u4dbf":
            return False
        else:
            noise += 1
        if noise > 3:
            return False
    return True


def is_email_value(raw: Any) -> bool:
    if rep.empty_db_value(raw):
        return False
    s = unicodedata.normalize("NFKC", str(raw).strip())
    return bool(RE_EMAIL.match(s))


def extract_representative_from_overview(raw: Any) -> Optional[str]:
    if rep.empty_db_value(raw):
        return None
    s = unicodedata.normalize("NFKC", str(raw))
    m = RE_OVERVIEW_REP.search(s)
    if not m:
        return None
    name = m.group(1).strip(" 　:：,，。．()（）[]【】「」『』")
    name = re.sub(r"[はがの]+$", "", name).strip()
    if not name or is_email_value(name):
        return None
    return name


def extract_representative_from_texts(*texts: Any) -> Optional[str]:
    for t in texts:
        name = extract_representative_from_overview(t)
        if name:
            return name
    return None


def layer5_heuristic_patch(db: Dict[str, Any]) -> CleanupPatch:
    p = CleanupPatch(id=db["id"], layers=[])
    rep_raw = db.get("representative_name")
    if is_email_value(rep_raw):
        if rep.empty_db_value(db.get("email")):
            p.email = str(rep_raw).strip()
        p.representative_name = ""
        p.layers.append("heuristic:rep_email_to_email")
    elif rep.is_representative_spilled_postal(rep_raw):
        pc = str(rep_raw).strip()
        if rep.RE_POSTAL_DIGITS7.match(pc):
            pc = f"{pc[:3]}-{pc[3:]}"
        p.postal_code = pc
        p.representative_name = ""
        p.layers.append("heuristic:rep_postal_to_postal_code")
    elif representative_name_is_hyphen_postal_spill(rep_raw):
        pc = extract_postal_from_hyphen_residual(str(rep_raw).strip())
        if pc is not None and rep.empty_db_value(db.get("postal_code")):
            p.postal_code = pc
        p.representative_name = ""
        p.layers.append("heuristic:rep_hyphen_postal_spill_to_postal_code")
    if rep.is_address_anomalous(db.get("address")):
        addr = str(db.get("address") or "").strip()
        p.layers.append("heuristic:address_anomaly_clear")
        if rep.empty_db_value(db.get("overview")) and len(addr) >= 80:
            p.overview = addr
            p.layers.append("heuristic:address_moved_to_overview")
        p.address = ""
    if rep.is_name_spilled_corporate_number(db.get("name")):
        if rep.empty_db_value(db.get("corporate_number")):
            p.corporate_number = str(db["name"]).strip()
            p.name = NAME_PLACEHOLDER
            p.layers.append("heuristic:name_digits_to_placeholder")
    if rep.empty_db_value(db.get("representative_name")) and p.representative_name is None:
        ov_rep = extract_representative_from_texts(
            db.get("overview"),
            db.get("business_descriptions"),
        )
        if ov_rep:
            p.representative_name = ov_rep
            p.layers.append("heuristic:text_rep_to_representative_name")
    return p


def extract_postal_from_hyphen_residual(s: str) -> Optional[str]:
    n = unicodedata.normalize("NFKC", s.strip())
    m = _RE_POSTAL_IN_TEXT.search(n)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    d = re.sub(r"\D", "", n)
    if len(d) == 7:
        return f"{d[:3]}-{d[3:]}"
    return None


def _csv_might_fill_empty(db: Dict[str, Any], c: rep.CsvRow) -> bool:
    if rep.is_name_spilled_corporate_number(db.get("name")):
        return True
    if c.representative_name and is_email_value(c.representative_name) and rep.empty_db_value(db.get("email")):
        return True
    for f in (
        "corporate_number",
        "representative_name",
        "postal_code",
        "address",
        "industry_middle",
        "business_descriptions",
    ):
        if getattr(c, f) and rep.empty_db_value(db.get(f)):
            return True
    return False


def _row_has_cleanup_signals(db: Dict[str, Any]) -> bool:
    """Layer5 / 業種救出が走り得る行か（軽い判定のみ）。"""
    if rep.is_name_spilled_corporate_number(db.get("name")):
        return True
    r = db.get("representative_name")
    if r and isinstance(r, str):
        if rep.is_representative_spilled_postal(r):
            return True
        if "-" in r and representative_name_is_hyphen_postal_spill(r):
            return True
    if rep.is_address_anomalous(db.get("address")):
        return True
    if rep.empty_db_value(db.get("representative_name")) and extract_representative_from_texts(
        db.get("overview"),
        db.get("business_descriptions"),
    ):
        return True
    for col in ("industry_large", "industry_middle", "industry_small", "industry_detail"):
        val = str(db.get(col) or "").strip()
        if not val:
            continue
        low = val.lower()
        if low in INVALID_INDUSTRY_VALUES:
            return True
        if RE_PHONE.match(val) or RE_EMAIL.match(val) or RE_REP.match(val) or RE_ADDR.match(val):
            return True
    if (
        rep.empty_db_value(db.get("industry_large"))
        or rep.empty_db_value(db.get("industry_middle"))
        or rep.empty_db_value(db.get("industry_small"))
    ):
        return True
    if is_email_value(db.get("representative_name")):
        return True
    return False


_BULK_UPDATE_COLS = (
    "name",
    "corporate_number",
    "representative_name",
    "postal_code",
    "email",
    "address",
    "phone_number",
    "industry_large",
    "industry_middle",
    "industry_small",
    "industry_detail",
    "overview",
    "business_descriptions",
)


def _patch_to_bulk_tuple(p: CleanupPatch, skip_corporate_number: bool) -> Tuple[Any, ...]:
    row: List[Any] = [p.id]
    for col in _BULK_UPDATE_COLS:
        if skip_corporate_number and col == "corporate_number":
            row.append(None)
            continue
        v = getattr(p, col)
        row.append(v)
    return tuple(row)


def _corp_key(c: Any) -> Optional[str]:
    if c is None:
        return None
    s = str(c).strip()
    if not s:
        return None
    return rep.normalize_corporate_number(s) or s


def _fetch_canonical_ids_by_corps(cur, corps: List[str]) -> Dict[str, str]:
    if not corps:
        return {}
    cur.execute(
        """
        SELECT corporate_number, id FROM companies
        WHERE corporate_number = ANY(%s)
          AND NOT (id ~ '^np[0-9a-f]{32}$')
        """,
        (corps,),
    )
    out: Dict[str, str] = {}
    for corp, rid in cur.fetchall():
        if not corp:
            continue
        k = _corp_key(corp)
        if k and k not in out:
            out[k] = str(rid)
    return out


def _fetch_np_ids_by_corps(cur, corps: List[str]) -> Dict[str, Set[str]]:
    if not corps:
        return {}
    cur.execute(
        """
        SELECT corporate_number, id FROM companies
        WHERE corporate_number = ANY(%s)
          AND id ~ '^np[0-9a-f]{32}$'
        """,
        (corps,),
    )
    by_corp: Dict[str, Set[str]] = {}
    for corp, rid in cur.fetchall():
        if not corp:
            continue
        k = _corp_key(corp)
        if not k:
            continue
        by_corp.setdefault(k, set()).add(str(rid))
    return by_corp


def _fetch_all_ids_by_corporate_numbers(cur, corps: List[str]) -> Dict[str, Set[str]]:
    """各法人番号（正規化キー）を保持する全 id（np / 非 np 問わず）。"""
    if not corps:
        return {}
    cur.execute(
        "SELECT corporate_number, id FROM companies WHERE corporate_number = ANY(%s)",
        (corps,),
    )
    m: Dict[str, Set[str]] = {}
    for cn, rid in cur.fetchall():
        if not cn:
            continue
        k = _corp_key(cn)
        if not k:
            continue
        m.setdefault(k, set()).add(str(rid))
    return m


def _apply_merge_patch(cur, p: CleanupPatch, canonical_id: str) -> int:
    """先に np~ を DELETE し、正規行へ補完。法人番号はテーブル上に残件が無いときだけ付与。"""
    n = 0
    cur.execute("DELETE FROM companies WHERE id = %s", (p.id,))
    n += cur.rowcount

    sets, vals = [], []
    mapping = {
        "name": p.name,
        "representative_name": p.representative_name,
        "postal_code": p.postal_code,
        "email": p.email,
        "address": p.address,
        "phone_number": p.phone_number,
        "overview": p.overview,
        "industry_large": p.industry_large,
        "industry_middle": p.industry_middle,
        "industry_small": p.industry_small,
        "industry_detail": p.industry_detail,
    }
    for col, val in mapping.items():
        if val is None or val == "":
            continue
        sets.append(
            f"{col} = CASE WHEN ({col} IS NULL OR BTRIM({col}::text) = '') THEN %s ELSE {col} END"
        )
        vals.append(val)

    ck = _corp_key(p.corporate_number)
    if ck:
        cur.execute("SELECT COUNT(*) FROM companies WHERE corporate_number = %s", (ck,))
        if int(cur.fetchone()[0] or 0) == 0:
            sets.append(
                "corporate_number = CASE WHEN (corporate_number IS NULL OR "
                "BTRIM(corporate_number::text) = '') THEN %s ELSE corporate_number END"
            )
            vals.append(ck)

    if sets:
        vals.append(canonical_id)
        cur.execute(f"UPDATE companies SET {', '.join(sets)} WHERE id = %s", vals)
        n += cur.rowcount
    return n


def _apply_bulk_update(cur, rows: List[Tuple[Any, ...]]) -> int:
    if not rows:
        return 0
    cur.execute(
        """
        CREATE TEMP TABLE _cleanup_bulk (
            id TEXT PRIMARY KEY,
            name TEXT, corporate_number TEXT, representative_name TEXT, postal_code TEXT,
            email TEXT,
            address TEXT, phone_number TEXT,
            industry_large TEXT, industry_middle TEXT, industry_small TEXT, industry_detail TEXT,
            overview TEXT, business_descriptions TEXT
        ) ON COMMIT DROP
        """
    )
    execute_values(
        cur,
        "INSERT INTO _cleanup_bulk (id, name, corporate_number, representative_name, postal_code, "
        "email, address, phone_number, industry_large, industry_middle, industry_small, industry_detail, "
        "overview, business_descriptions) VALUES %s",
        rows,
    )
    set_parts = []
    for col in _BULK_UPDATE_COLS:
        if col == "name":
            set_parts.append(
                f"{col} = CASE WHEN f.{col} IS NULL THEN c.{col} ELSE f.{col} END"
            )
        else:
            set_parts.append(
                f"{col} = CASE WHEN f.{col} IS NULL THEN c.{col} "
                f"WHEN f.{col} = '' THEN NULL ELSE f.{col} END"
            )
    sql = (
        "UPDATE companies AS c SET "
        + ", ".join(set_parts)
        + " FROM _cleanup_bulk AS f WHERE c.id = f.id"
    )
    cur.execute(sql)
    return cur.rowcount


def rescue_industry_fields(
    db: Dict[str, Any],
    p: CleanupPatch,
    csv_hit: Optional[rep.CsvRow],
    industry_master_ctx: Optional[IndustryMasterContext] = None,
) -> None:
    ind_cols = ["industry_large", "industry_middle", "industry_small", "industry_detail"]
    for col in ind_cols:
        val = str(db.get(col) or "").strip()
        if not val:
            continue
        if val.lower() in INVALID_INDUSTRY_VALUES:
            setattr(p, col, "")
            continue
        if RE_PHONE.match(val):
            if rep.empty_db_value(db.get("phone_number")) and p.phone_number is None:
                p.phone_number = val
                p.layers.append(f"rescue:{col}_to_phone")
            setattr(p, col, "")
        elif is_email_value(val):
            if rep.empty_db_value(db.get("email")) and p.email is None:
                p.email = val
                p.layers.append(f"rescue:{col}_to_email")
            setattr(p, col, "")
        elif RE_REP.match(val):
            if rep.empty_db_value(db.get("representative_name")) and p.representative_name is None:
                p.representative_name = val
                p.layers.append(f"rescue:{col}_to_rep")
            setattr(p, col, "")
        elif RE_ADDR.match(val):
            if rep.empty_db_value(db.get("address")) and p.address is None:
                p.address = val
                p.layers.append(f"rescue:{col}_to_address")
            setattr(p, col, "")
    if csv_hit and csv_hit.industry_middle:
        p.industry_middle = csv_hit.industry_middle
    if industry_master_ctx is not None:
        enforce_industry_master_consistency(db, p, industry_master_ctx)


def _apply_patches_to_db(conn, patches: List[CleanupPatch], do_commit: bool) -> int:
    """マージは 1 件ずつ、その他は一時テーブルへ一括 INSERT 後に 1 回の UPDATE。"""
    if not patches:
        return 0
    cur = conn.cursor()
    n = 0
    try:
        np_with_corp = [p for p in patches if p.corporate_number and str(p.id).startswith("np")]
        corps = list({str(p.corporate_number) for p in np_with_corp if p.corporate_number})
        canon_map = _fetch_canonical_ids_by_corps(cur, corps)
        np_by_corp = _fetch_np_ids_by_corps(cur, corps)

        skip_corp_ids: Set[str] = set()
        for p in np_with_corp:
            ck = _corp_key(p.corporate_number)
            if not ck or ck in canon_map:
                continue
            others = np_by_corp.get(ck, set()) - {str(p.id)}
            if others:
                skip_corp_ids.add(str(p.id))

        if skip_corp_ids:
            LOG.warning(
                "同一法人番号を別 np~ が保持中のため corporate_number を除外する行: %d 件（例: %s）",
                len(skip_corp_ids),
                next(iter(skip_corp_ids)),
            )

        bulk_patches: List[CleanupPatch] = []
        for p in patches:
            ck = _corp_key(p.corporate_number)
            if ck and str(p.id).startswith("np") and ck in canon_map:
                n += _apply_merge_patch(cur, p, canon_map[ck])
                continue
            bulk_patches.append(p)

        corps_bulk = list({str(p.corporate_number) for p in bulk_patches if p.corporate_number})
        holders = _fetch_all_ids_by_corporate_numbers(cur, corps_bulk)

        # 同一バッチ内で複数行が同じ法人番号へ更新すると 1 文の UPDATE でも Unique 違反になる。
        # id 昇順で先着 1 行だけ付与し、他は corporate_number をスキップ。
        inner_skip: Set[str] = set()
        claimed_corp: Dict[str, str] = {}
        for p in sorted(bulk_patches, key=lambda x: str(x.id)):
            pid = str(p.id)
            if pid in skip_corp_ids:
                continue
            ck2 = _corp_key(p.corporate_number)
            if not p.corporate_number or not ck2:
                continue
            if holders.get(ck2, set()) - {pid}:
                continue
            prev = claimed_corp.get(ck2)
            if prev is None:
                claimed_corp[ck2] = pid
            elif prev != pid:
                inner_skip.add(pid)

        bulk_rows: List[Tuple[Any, ...]] = []
        skipped_ext = 0
        skipped_inner = 0
        for p in bulk_patches:
            pid = str(p.id)
            skip_np_peer = pid in skip_corp_ids
            skip_ext = False
            ck2 = _corp_key(p.corporate_number)
            if p.corporate_number and ck2 is not None:
                if holders.get(ck2, set()) - {pid}:
                    skip_ext = True
                    skipped_ext += 1
            skip_in = pid in inner_skip
            if skip_in:
                skipped_inner += 1
            bulk_rows.append(_patch_to_bulk_tuple(p, skip_np_peer or skip_ext or skip_in))

        if skipped_ext or skipped_inner:
            LOG.warning(
                "corporate_number を更新対象外: 他行保持=%d 件, 同一バッチ内競合=%d 件",
                skipped_ext,
                skipped_inner,
            )

        n += _apply_bulk_update(cur, bulk_rows)
        if do_commit:
            conn.commit()
    except Exception:
        if do_commit:
            try:
                if not getattr(conn, "closed", 1):
                    conn.rollback()
                else:
                    LOG.warning("rollback をスキップ: DB 接続は既にクローズ済みです")
            except Exception as rb_err:
                LOG.warning("rollback に失敗（接続断の可能性）: %s", rb_err)
        raise
    finally:
        cur.close()
    return n


def _count_companies(conn) -> int:
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM companies")
    n = c.fetchone()[0]
    c.close()
    return int(n or 0)


def build_patch_for_row(
    db: Dict[str, Any],
    lookup: rep.CsvLookup,
    pref_index: Dict[str, Dict[str, List[Tuple[str, rep.CsvRow]]]],
    fuzzy_max: int,
    industry_allowed: Optional[Set[str]] = None,
    industry_master_ctx: Optional[IndustryMasterContext] = None,
) -> Optional[CleanupPatch]:
    pid = db["id"]
    patch = CleanupPatch(id=str(pid))
    csv_res = rep.resolve_csv_for_db_row(db, lookup)
    csv_hit: Optional[rep.CsvRow] = None
    tag: Optional[str] = None

    if csv_res:
        csv_hit, match_tag = csv_res
        need_whitelist = industry_allowed is not None and _row_has_unknown_industry(
            db, industry_allowed
        )
        if (
            not _csv_might_fill_empty(db, csv_hit)
            and not _row_has_cleanup_signals(db)
            and not need_whitelist
        ):
            return None
        cp = patch_csv_match_fill_only(db, csv_hit, match_tag)
        if has_sql_changes(cp) or cp.layers:
            merge_patch(patch, cp)
    else:
        nm = str(db.get("name") or "").strip()
        if rep.RE_CORP_13.match(nm):
            for v in corp_one_substitution_variants(nm):
                if v in lookup.by_corp:
                    csv_hit, tag = lookup.by_corp[v], "csv:layer4_fuzzy_corp_1edit"
                    break
        if csv_hit is None:
            csv_hit = find_fuzzy_name_match(db, pref_index, fuzzy_max)
            if csv_hit:
                tag = "csv:layer4_fuzzy_name"
        if csv_hit and tag:
            cp = patch_from_csv_row(db, csv_hit, tag)
            merge_patch(patch, cp)

    h5 = layer5_heuristic_patch(db)
    if h5.layers:
        merge_patch(patch, h5)

    rescue_industry_fields(db, patch, csv_hit, industry_master_ctx)

    if industry_allowed is not None:
        _null_industry_not_in_whitelist(db, patch, industry_allowed)

    if not has_sql_changes(patch):
        return None
    return patch


def main() -> None:
    ap = argparse.ArgumentParser(description="companies 全件チャンク・クレンジング")
    ap.add_argument("--apply", action="store_true", help="DB に反映（チャンクごとにコミット）")
    ap.add_argument("--report", metavar="PATH", help="JSON Lines を追記")
    ap.add_argument("--fuzzy-name-max", type=int, default=DEFAULT_FUZZY_NAME_MAX)
    ap.add_argument(
        "--chunk-size",
        "--batch-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        dest="chunk_size",
        help="fetchmany の件数・解析コミット単位（既定 10000）",
    )
    ap.add_argument(
        "--offset",
        type=int,
        default=0,
        help="先頭からスキップする件数。途中再開に使用（例: --offset 4100000）",
    )
    args = ap.parse_args()

    lookup, _ = rep.build_csv_lookup_tables()
    pref_index = build_pref_firstchar_index(lookup)

    # ドライランでも業種補完パッチ（industry:master_consistent）を生成できるよう常時ロード。
    ind_path = _find_industries_csv()
    industry_allowed: Optional[Set[str]] = load_industry_whitelist_from_csv(ind_path)
    industry_master_ctx: Optional[IndustryMasterContext] = load_industry_master_context(ind_path)
    LOG.info(
        "業種ホワイトリスト: %s（%d 語）",
        ind_path,
        len(industry_allowed),
    )

    read_conn = rep.get_connection()
    write_conn = rep.get_connection()
    report_fp = open(args.report, "a", encoding="utf-8") if args.report else None

    stats: Counter = Counter()
    processed = 0
    patch_count = 0
    apply_ops = 0
    read_cur = None

    try:
        total_all = _count_companies(read_conn)
        if total_all <= 0:
            total_all = 1
        if args.offset < 0:
            raise ValueError("--offset は 0 以上を指定してください")
        total = max(total_all - args.offset, 1)
        LOG.info(
            "companies 総件数: %d（fetchmany=%d, offset=%d, 対象推定=%d）",
            total_all,
            args.chunk_size,
            args.offset,
            total,
        )

        try:
            read_cur = read_conn.cursor(name="cleanup_companies_fullscan")
            read_cur.itersize = args.chunk_size
            read_cur.execute(
                _COMPANY_SELECT_SQL + " ORDER BY id OFFSET %s",
                (args.offset,),
            )
            cols = list(_COMPANY_COL_NAMES)

            chunk_buf: List[CleanupPatch] = []
            rows_since_commit = 0

            while True:
                batch = read_cur.fetchmany(args.chunk_size)
                if not batch:
                    break
                for row in batch:
                    db = dict(zip(cols, row))
                    p = build_patch_for_row(
                        db,
                        lookup,
                        pref_index,
                        args.fuzzy_name_max,
                        industry_allowed=industry_allowed,
                        industry_master_ctx=industry_master_ctx,
                    )
                    processed += 1
                    rows_since_commit += 1
                    if p:
                        chunk_buf.append(p)
                        patch_count += 1
                        for layer in p.layers:
                            stats[layer] += 1
                        if report_fp:
                            report_fp.write(
                                json.dumps(
                                    {
                                        "id": p.id,
                                        "layers": list(dict.fromkeys(p.layers)),
                                        "patch": {
                                            k: getattr(p, k)
                                            for k in (
                                                "name",
                                                "corporate_number",
                                                "representative_name",
                                                "postal_code",
                                                "email",
                                                "phone_number",
                                                "address",
                                                "industry_middle",
                                            )
                                            if getattr(p, k) is not None
                                        },
                                    },
                                    ensure_ascii=False,
                                )
                                + "\n"
                            )

                    if args.apply and rows_since_commit >= args.chunk_size:
                        apply_ops += _apply_patches_to_db(write_conn, chunk_buf, do_commit=True)
                        chunk_buf.clear()
                        rows_since_commit = 0

                if processed % args.chunk_size == 0:
                    pct = 100.0 * processed / total
                    LOG.info(
                        "%.1f万件処理完了（進捗%.1f%%, 累計走査 %.1f万件）",
                        processed / 10_000.0,
                        pct,
                        (args.offset + processed) / 10_000.0,
                    )

            if args.apply and chunk_buf:
                apply_ops += _apply_patches_to_db(write_conn, chunk_buf, do_commit=True)
                chunk_buf.clear()
        finally:
            if read_cur is not None:
                read_cur.close()
            read_conn.commit()

        print(f"\n--- 最終統計 ---\n走査件数: {processed}\n生成パッチ数: {patch_count}")
        if args.apply:
            print(f"DB 操作計（更新・削除の合計 rowcount 近似）: {apply_ops}")
        for k, v in stats.most_common(40):
            print(f"  {k}: {v}")
        if not args.apply:
            print("\n（ドライラン）--apply でチャンクごとにコミットして反映します。")
    finally:
        if report_fp:
            report_fp.close()
        read_conn.close()
        write_conn.close()


if __name__ == "__main__":
    main()
