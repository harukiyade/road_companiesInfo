#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
csv_missing（CSV 逆引き不能）の np~ 企業に対し、以下の複合修復を行う。

  1. Layer 4: 曖昧一致（法人番号1桁差、または社名編集距離2以内）による特定
  2. Layer 5: ヒューリスティック修復（代表者=郵便番号、住所=日付等の移動）
  3. Industry Rescue: 業種4カラムに混入した電話番号・代表者・住所を正規のカラムへ救出
  4. 衝突回避: 法人番号が既存レコードと衝突する場合、既存行を補完して np~ 行を削除（統合）

  --apply が無い場合は DB を更新しない（統計のみ）。
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# 同ディレクトリの修復スクリプトを再利用
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
except ImportError as e:
    raise SystemExit("psycopg2 が必要です: pip install psycopg2-binary") from e

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

NAME_PLACEHOLDER = "（社名未確定・法人番号参照）"
DEFAULT_FUZZY_NAME_MAX = 2

# --- 救出用正規表現 ---
RE_PHONE = re.compile(r'^0[0-9-]{9,13}$')
RE_REP = re.compile(r'^(\(|（)?(代表|取締役|会長|社長|理事|監事)')
RE_ADDR = re.compile(r'^(東京都|北海道|京都府|大阪府|.{2,3}[県])')
INVALID_INDUSTRY_VALUES = {'0', 'なし', 'unknown', '未分類', '-', 'None', 'null', '‐', 'ー', '調査中'}

# --- データ構造 ---

@dataclass
class CleanupPatch:
    id: str
    name: Optional[str] = None
    corporate_number: Optional[str] = None
    representative_name: Optional[str] = None
    postal_code: Optional[str] = None
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
    cols = ("name", "corporate_number", "representative_name", "postal_code", "address", 
            "phone_number", "overview", "business_descriptions", 
            "industry_large", "industry_middle", "industry_small", "industry_detail")
    # 空文字（""）は NULL化マーカーとして扱う
    return any(getattr(p, f) is not None for f in cols)

# --- Layer 4/5（repair モジュールに無い処理はここで定義）---

def levenshtein_distance(a: str, b: str, max_d: int) -> int:
    la, lb = len(a), len(b)
    if abs(la - lb) > max_d:
        return max_d + 1
    dp = list(range(lb + 1))
    for i in range(1, la + 1):
        ndp = [i]
        for j in range(1, lb + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            ndp.append(min(dp[j] + 1, ndp[j - 1] + 1, dp[j - 1] + cost))
        dp = ndp
        if min(dp) > max_d:
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

def build_pref_norm_name_index(lookup: rep.CsvLookup) -> Dict[str, List[Tuple[str, rep.CsvRow]]]:
    idx: Dict[str, List[Tuple[str, rep.CsvRow]]] = {}
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
        idx.setdefault(p, []).append((nn, row))
    return idx

def find_fuzzy_name_match(
    db_row: Dict[str, Any],
    pref_index: Dict[str, List[Tuple[str, rep.CsvRow]]],
    max_d: int,
) -> Optional[rep.CsvRow]:
    db_nn = rep.normalize_company_name_for_hash(str(db_row.get("name") or ""))
    if len(db_nn) < 2:
        return None
    db_p = rep.normalize_prefecture_for_hash(str(db_row.get("prefecture") or ""))
    candidates = pref_index.get(db_p, [])
    best: Optional[rep.CsvRow] = None
    best_d = max_d + 1
    for nn, crow in candidates:
        if abs(len(db_nn) - len(nn)) > max_d:
            continue
        d = levenshtein_distance(db_nn, nn, max_d)
        if d < best_d:
            best_d = d
            best = crow
            if d == 0:
                break
    return best if best_d <= max_d else None

def patch_from_csv_row(db: Dict[str, Any], c: rep.CsvRow, layer_tag: str) -> CleanupPatch:
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
        if csv_val and rep.empty_db_value(db.get(f)):
            setattr(p, f, csv_val)
    return p

def merge_patch(base: CleanupPatch, extra: CleanupPatch) -> None:
    cols = (
        "name",
        "corporate_number",
        "representative_name",
        "postal_code",
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

def layer5_heuristic_patch(db: Dict[str, Any]) -> CleanupPatch:
    p = CleanupPatch(id=db["id"], layers=[])
    if rep.is_representative_spilled_postal(db.get("representative_name")):
        pc = str(db["representative_name"]).strip()
        if rep.RE_POSTAL_DIGITS7.match(pc):
            pc = f"{pc[:3]}-{pc[3:]}"
        p.postal_code = pc
        p.representative_name = ""
        p.layers.append("heuristic:rep_postal_to_postal_code")
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
    return p

def _find_canonical_id_by_corporate_number(cur, corporate_number: str) -> Optional[str]:
    cur.execute(
        """
        SELECT id FROM companies
        WHERE corporate_number = %s
          AND NOT (id ~ '^np[0-9a-f]{32}$')
        LIMIT 1
        """,
        (corporate_number,),
    )
    row = cur.fetchone()
    return str(row[0]) if row else None

# --- 救出ロジック ---

def rescue_industry_fields(db: Dict[str, Any], p: CleanupPatch, csv_hit: Optional[rep.CsvRow]) -> None:
    """業種カラムを走査し、他カラムへの救出とクレンジングを行う"""
    ind_cols = ["industry_large", "industry_middle", "industry_small", "industry_detail"]
    
    for col in ind_cols:
        val = str(db.get(col) or '').strip()
        if not val: continue

        # 無効値のクレンジング
        if val.lower() in INVALID_INDUSTRY_VALUES:
            setattr(p, col, "") # NULL化マーカー
            continue

        # 1. 電話番号の救出
        if RE_PHONE.match(val):
            if rep.empty_db_value(db.get('phone_number')) and p.phone_number is None:
                p.phone_number = val
                p.layers.append(f"rescue:{col}_to_phone")
            setattr(p, col, "") # 元のカラムはクリア

        # 2. 代表者名・役職の救出
        elif RE_REP.match(val):
            if rep.empty_db_value(db.get('representative_name')) and p.representative_name is None:
                p.representative_name = val
                p.layers.append(f"rescue:{col}_to_rep")
            setattr(p, col, "")

        # 3. 住所の救出
        elif RE_ADDR.match(val):
            if rep.empty_db_value(db.get('address')) and p.address is None:
                p.address = val
                p.layers.append(f"rescue:{col}_to_address")
            setattr(p, col, "")

    # 4. CSVマスターとの照合（最終復元）
    if csv_hit:
        # 現在 CsvRow は industry_middle のみを保持しているため、それのみ復元
        if csv_hit.industry_middle:
            p.industry_middle = csv_hit.industry_middle

# --- DB 更新ロジック ---

def _apply_patches_to_db(conn, patches: List[CleanupPatch]) -> int:
    if not patches: return 0
    cur = conn.cursor()
    n = 0
    try:
        for p in patches:
            # 1. 法人番号衝突による統合 (Merge & Delete)
            corp = p.corporate_number
            if corp and p.id.startswith('np'):
                canonical_id = _find_canonical_id_by_corporate_number(cur, corp)
                if canonical_id:
                    # 既存の正規行(canonical)を、今回のパッチで補完更新
                    # ※業種カラムなども含めて統合
                    sets, vals = [], []
                    mapping = {
                        "name": p.name, "representative_name": p.representative_name, "postal_code": p.postal_code,
                        "address": p.address, "phone_number": p.phone_number, "overview": p.overview,
                        "industry_large": p.industry_large, "industry_middle": p.industry_middle,
                        "industry_small": p.industry_small, "industry_detail": p.industry_detail
                    }
                    for col, val in mapping.items():
                        if val is None or val == "": continue
                        sets.append(f"{col} = CASE WHEN ({col} IS NULL OR BTRIM({col}::text) = '') THEN %s ELSE {col} END")
                        vals.append(val)
                    
                    if sets:
                        vals.append(canonical_id)
                        cur.execute(f"UPDATE companies SET {', '.join(sets)} WHERE id = %s", vals)
                    
                    # np~ 行を削除
                    cur.execute("DELETE FROM companies WHERE id = %s", (p.id,))
                    n += cur.rowcount
                    LOG.info("Merged and Deleted np id=%s -> canonical id=%s", p.id, canonical_id)
                    continue

            # 2. 通常の更新
            sets, vals = [], []
            mapping = {
                "name": p.name, "corporate_number": p.corporate_number,
                "representative_name": p.representative_name, "postal_code": p.postal_code,
                "address": p.address, "phone_number": p.phone_number,
                "industry_large": p.industry_large, "industry_middle": p.industry_middle,
                "industry_small": p.industry_small, "industry_detail": p.industry_detail,
                "overview": p.overview, "business_descriptions": p.business_descriptions
            }
            for col, val in mapping.items():
                if val is not None:
                    sets.append(f"{col} = %s")
                    vals.append(None if val == "" else val) # マーカーをNULLへ
            
            if not sets: continue
            vals.append(p.id)
            cur.execute(f"UPDATE companies SET {', '.join(sets)} WHERE id = %s", vals)
            n += cur.rowcount

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
    return n

# --- 補助関数 ---

def fetch_companies_extended(cur) -> List[Dict[str, Any]]:
    """レスキューに必要なカラムを含めてフェッチ"""
    cur.execute("""
        SELECT id, name, corporate_number, representative_name, postal_code,
               address, phone_number, industry_large, industry_middle, 
               industry_small, industry_detail, prefecture, overview, business_descriptions
        FROM companies WHERE id ~ '^np[0-9a-f]{32}$'
    """)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def main() -> None:
    ap = argparse.ArgumentParser(description="業種救出統合版クリーンアップ")
    ap.add_argument("--apply", action="store_true", help="DB に反映する")
    ap.add_argument("--report", metavar="PATH", help="レポート出力")
    ap.add_argument("--fuzzy-name-max", type=int, default=DEFAULT_FUZZY_NAME_MAX)
    args = ap.parse_args()

    lookup, _ = rep.build_csv_lookup_tables()
    pref_index = build_pref_norm_name_index(lookup)

    conn = rep.get_connection()
    try:
        cur = conn.cursor()
        db_rows = fetch_companies_extended(cur)
        cur.close()

        unmatched = [r for r in db_rows if rep.resolve_csv_for_db_row(r, lookup) is None]
        patches, report_events, stats = [], [], Counter()

        LOG.info("未マッチ対象 %d 件の解析および業種救出を開始...", len(unmatched))

        for db in unmatched:
            pid, csv_hit, tag = db["id"], None, None
            
            # Layer 4: 曖昧一致判定
            nm = str(db.get("name") or "").strip()
            if rep.RE_CORP_13.match(nm):
                for v in corp_one_substitution_variants(nm):
                    if v in lookup.by_corp:
                        csv_hit, tag = lookup.by_corp[v], "csv:layer4_fuzzy_corp_1edit"
                        break
            if csv_hit is None:
                csv_hit = find_fuzzy_name_match(db, pref_index, args.fuzzy_name_max)
                if csv_hit:
                    tag = "csv:layer4_fuzzy_name"

            patch = CleanupPatch(id=pid)
            
            # Layer 4/5 由来のパッチ
            if csv_hit and tag:
                cp = patch_from_csv_row(db, csv_hit, tag)
                merge_patch(patch, cp)
            
            h5 = layer5_heuristic_patch(db)
            if h5.layers:
                merge_patch(patch, h5)

            # ★ 業種レスキューの実行
            rescue_industry_fields(db, patch, csv_hit)

            if has_sql_changes(patch):
                patches.append(patch)
                for layer in patch.layers: stats[layer] += 1
                report_events.append({
                    "id": pid, "layers": list(set(patch.layers)),
                    "patch": {k: getattr(patch, k) for k in ("name", "corporate_number", "representative_name", "phone_number", "address", "industry_middle") if getattr(patch, k) is not None}
                })

        # 統計と反映
        if args.report:
            with open(args.report, "w", encoding="utf-8") as f:
                for ev in report_events: f.write(json.dumps(ev, ensure_ascii=False) + "\n")
        
        if args.apply:
            n = _apply_patches_to_db(conn, patches)
            LOG.info("適用完了: %d 行を処理しました（マージ/削除/更新含む）", n)
        
        print(f"\n--- 最終統計 ---\n生成パッチ数: {len(patches)}")
        for k, v in stats.most_common(): print(f"  {k}: {v}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()