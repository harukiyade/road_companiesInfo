#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
重複企業レコードの統合（丸まり法人番号 → 正規法人番号）と、財務の常識的補正。

1) 山都酒造株式会社: 4330001020456 を親に、他法人番号行をマージして削除
2) 一般化: 同一（社名+都道府県）で、下位7桁以上ゼロの「丸まり」法人番号を正規13桁へ寄せる（先頭6桁一致）
3) 売上が 4000億円超かつ中小規模（従業員数 < 300 または NULL）→ 1000分の1（単位ミス想定）
4) 山都酒造: 111.fixed.csv の列ズレで電話列(15)にある 5000 を「5000万円」→ 資本金 50,000,000 円

FK を参照するテーブルは information_schema から動的に列挙して ID を付け替え後、重複行を DELETE。

メモリ: 全件は「軽量4列」のみ読み込み。`SELECT *` 全件はメモリ不足で zsh: killed になり得るため行わない。

使い方:
  export POSTGRES_HOST=... POSTGRES_PASSWORD=...
  python scripts/merge_duplicate_companies_and_fix_financials.py          # DRY RUN
  python scripts/merge_duplicate_companies_and_fix_financials.py --execute

オプション:
  --csv-capital PATH   山都酒造の資本金再取得に使う CSV（デフォルト: fixed_csv_3/unit_million/111.fixed.csv）
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor, Json, execute_batch

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD") or os.getenv("PGPASSWORD") or ""
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

# 明示マージ: 社名 → 正規法人番号（親）
NAME_CANONICAL_CORPORATE: dict[str, str] = {
    "山都酒造株式会社": "4330001020456",
}

TRAILING_ZERO_MIN = 7
PREFIX_LEN = 6
REVENUE_SME_THRESHOLD_YEN = 400_000_000_000  # 4000億円
SME_MAX_EMPLOYEES = 300
PROFIT_SME_THRESHOLD_YEN = 200_000_000_000  # 利益も異常に大きい場合に同様に /1000

# 山都酒造: CSV のズレ列にある 5000 = 5000万円
YAMATO_CAPITAL_COL_SHIFTED = 15
YAMATO_CAPITAL_MAN = 5000  # 万円


def is_rounded_corp(cn: str | None) -> bool:
    if not cn or len(cn) != 13 or not cn.isdigit():
        return False
    return cn.endswith("0" * TRAILING_ZERO_MIN)


def same_prefix(a: str, b: str, n: int = PREFIX_LEN) -> bool:
    if not a or not b or len(a) < n or len(b) < n:
        return False
    return a[:n] == b[:n]


def fmt_oku(yen: int | None) -> str:
    if yen is None:
        return "NULL"
    y = abs(int(yen))
    oku = y / 1e8
    return f"{oku:.2f}億円"


def fetch_fk_columns(conn) -> list[tuple[str, str, str]]:
    """(table_schema, table_name, column_name) referencing companies(id)"""
    q = """
    SELECT tc.table_schema, tc.table_name, kcu.column_name
    FROM information_schema.table_constraints AS tc
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_catalog = kcu.constraint_catalog
      AND tc.constraint_schema = kcu.constraint_schema
      AND tc.constraint_name = kcu.constraint_name
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_catalog = tc.constraint_catalog
      AND ccu.constraint_schema = tc.constraint_schema
      AND ccu.constraint_name = tc.constraint_name
    WHERE tc.constraint_type = 'FOREIGN KEY'
      AND ccu.table_schema NOT IN ('pg_catalog', 'information_schema')
      AND ccu.table_name = 'companies'
      AND ccu.column_name = 'id'
    ORDER BY tc.table_schema, tc.table_name, kcu.column_name
    """
    with conn.cursor() as cur:
        cur.execute(q)
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def fetch_company_columns(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'companies'
            ORDER BY ordinal_position
            """
        )
        return [r[0] for r in cur.fetchall()]


def fetch_jsonb_columns(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'companies'
              AND udt_name = 'jsonb'
            """
        )
        return {r[0] for r in cur.fetchall()}


def load_merge_plan_rows(conn) -> list[dict]:
    """
    マージ判定に必要な列のみ取得（SELECT * は JSONB/TEXT が多くメモリを食い、
    環境によっては OOM でプロセスが killed になるため避ける）。
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id::text AS id, name, prefecture, corporate_number
            FROM companies
            """
        )
        return [dict(r) for r in cur.fetchall()]


def fetch_companies_full_by_ids(conn, ids: set[str], chunk_size: int = 400) -> dict[str, dict]:
    """マージ実行時のみ、対象 id の全列を取得。"""
    out: dict[str, dict] = {}
    id_list = [x for x in ids if x]
    for i in range(0, len(id_list), chunk_size):
        chunk = id_list[i : i + chunk_size]
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM companies WHERE id::text = ANY(%s)",
                (chunk,),
            )
            for r in cur.fetchall():
                out[str(r["id"])] = dict(r)
    return out


def merge_row_values(winner: dict, loser: dict, columns: list[str]) -> dict:
    out = dict(winner)
    for c in columns:
        if c == "id":
            continue
        wv = out.get(c)
        lv = loser.get(c)
        empty_w = wv is None or (isinstance(wv, str) and not wv.strip())
        if empty_w and lv is not None:
            if isinstance(lv, str) and not lv.strip():
                continue
            out[c] = lv
    return out


def yamato_capital_from_csv(csv_path: Path) -> int | None:
    if not csv_path.is_file():
        return None
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            with open(csv_path, "r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                headers = next(reader)
                name_i = next(
                    (
                        i
                        for i, h in enumerate(headers)
                        if h.strip() in ("会社名", "企業名")
                    ),
                    None,
                )
                if name_i is None:
                    continue
                for row in reader:
                    if len(row) <= YAMATO_CAPITAL_COL_SHIFTED:
                        continue
                    if name_i >= len(row):
                        continue
                    if (row[name_i] or "").strip() != "山都酒造株式会社":
                        continue
                    raw = (row[YAMATO_CAPITAL_COL_SHIFTED] or "").strip().replace(",", "")
                    if raw.replace(".", "").isdigit():
                        v = int(float(raw))
                        if v == YAMATO_CAPITAL_MAN:
                            return YAMATO_CAPITAL_MAN * 10_000  # 5000万円
                    return None
        except (UnicodeDecodeError, OSError):
            continue
    return None


def build_merge_plans(rows: list[dict]) -> list[tuple[str, str, list[str], str]]:
    """
    (winner_id, canonical_corporate_number, loser_ids, reason)
    """
    by_key: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        name = (r.get("name") or "").strip()
        if not name:
            continue
        pref = (r.get("prefecture") or "").strip()
        by_key[(name, pref)].append(r)

    plans: list[tuple[str, str, list[str], str]] = []

    for (name, pref), group in by_key.items():
        if len(group) < 2:
            continue

        # --- 明示ルール（山都酒造など）---
        if name in NAME_CANONICAL_CORPORATE:
            canon = NAME_CANONICAL_CORPORATE[name]
            winners = [
                r
                for r in group
                if (r.get("corporate_number") or "").strip() == canon
                or str(r["id"]) == canon
            ]
            if not winners:
                print(
                    f"警告: 明示マージ先 {name} の正規法人番号 {canon} に一致する行がありません。スキップします。"
                )
                continue
            winner = winners[0]
            wid = str(winner["id"])
            losers = [str(r["id"]) for r in group if str(r["id"]) != wid]
            if losers:
                plans.append((wid, canon, losers, f"explicit_name:{name}"))
            continue

        # --- 丸まり ID → 正規 ID（同一6桁プレフィックス）---
        good = [
            r
            for r in group
            if (r.get("corporate_number") or "").strip()
            and len((r.get("corporate_number") or "").strip()) == 13
            and not is_rounded_corp((r.get("corporate_number") or "").strip())
        ]
        rounded = [
            r
            for r in group
            if is_rounded_corp((r.get("corporate_number") or "").strip())
        ]

        used_rounded_losers: set[str] = set()
        for r_bad in rounded:
            cn_bad = (r_bad.get("corporate_number") or "").strip()
            bid = str(r_bad["id"])
            if bid in used_rounded_losers:
                continue
            matches = [
                g
                for g in good
                if same_prefix(cn_bad, (g.get("corporate_number") or "").strip(), PREFIX_LEN)
            ]
            if len(matches) != 1:
                continue
            g = matches[0]
            wid = str(g["id"])
            canon = (g.get("corporate_number") or "").strip()
            if bid == wid:
                continue
            plans.append((wid, canon, [bid], f"rounded_merge:{name}:{cn_bad}->{canon}"))
            used_rounded_losers.add(bid)

    # --- フェーズ2: np* 仮ID（明示・丸まりマージの loser 以外）---
    planned_losers = {lid for *_, ls, _ in plans for lid in ls}

    for (name, pref), group in by_key.items():
        if len(group) < 2:
            continue
        if name in NAME_CANONICAL_CORPORATE:
            continue

        np_rows = [r for r in group if str(r.get("id") or "").startswith("np")]
        corp_rows = [
            r
            for r in group
            if (r.get("corporate_number") or "").strip()
            and len((r.get("corporate_number") or "").strip()) == 13
            and not str(r["id"]).startswith("np")
        ]
        for r_np in np_rows:
            nid = str(r_np["id"])
            if nid in planned_losers:
                continue
            cand = [
                r
                for r in corp_rows
                if not is_rounded_corp((r.get("corporate_number") or "").strip())
            ]
            if not cand:
                cand = corp_rows
            if not cand:
                continue
            winner = cand[0]
            wid = str(winner["id"])
            if nid == wid:
                continue
            canon = (winner.get("corporate_number") or "").strip() or wid
            plans.append((wid, canon, [nid], f"np_merge:{name}:{nid}->{wid}"))
            planned_losers.add(nid)

    return plans


def rewire_fk(
    conn,
    schema: str,
    table: str,
    col: str,
    winner_id: str,
    loser_id: str,
    dry_run: bool,
) -> int:
    fq = f'"{schema}"."{table}"' if schema != "public" else f'"{table}"'
    sql = f"UPDATE {fq} SET \"{col}\" = %s WHERE \"{col}\"::text = %s"
    if dry_run:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT COUNT(*) FROM {fq} WHERE "{col}"::text = %s', (loser_id,)
            )
            return cur.fetchone()[0]
    with conn.cursor() as cur:
        cur.execute(sql, (winner_id, loser_id))
        return cur.rowcount


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="未指定時は DRY RUN")
    ap.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="1バッチで処理する loser 件数（目安）。この件数ごとに commit します。",
    )
    ap.add_argument(
        "--reconnect-once",
        action="store_true",
        help="接続切れ（timeout 等）で失敗した場合に、1回だけ再接続して続行を試みます。",
    )
    ap.add_argument(
        "--csv-capital",
        type=Path,
        default=PROJECT_ROOT / "fixed_csv_3" / "unit_million" / "111.fixed.csv",
        help="山都酒造の資本金（列ズレ）読み取り元 CSV",
    )
    args = ap.parse_args()
    dry_run = not args.execute

    if not DB_PASSWORD:
        print("エラー: POSTGRES_PASSWORD または PGPASSWORD が未設定です。", file=sys.stderr)
        sys.exit(2)

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )
    conn.autocommit = False

    stats: dict[str, int] = defaultdict(int)

    try:
        fks = fetch_fk_columns(conn)
        cols = [c for c in fetch_company_columns(conn) if c != "id"]
        jsonb_cols = fetch_jsonb_columns(conn)
        has_capital = "capital" in cols
        has_capital_stock = "capital_stock" in cols

        print(f"参照FK: {len(fks)} 列")
        for s, t, c in fks:
            print(f"  - {s}.{t}.{c}")

        print("マージ判定用データを読み込み中（id・社名・都道府県・法人番号のみ）…")
        sys.stdout.flush()
        rows = load_merge_plan_rows(conn)
        print(f"  → {len(rows):,} 件")
        plans = build_merge_plans(rows)
        # 同一 winner に複数 loser をまとめる
        merged: dict[str, tuple[str, list[str], list[str]]] = {}
        for wid, canon, losers, reason in plans:
            if wid not in merged:
                merged[wid] = (canon, [], [])
            merged[wid][1].extend(losers)
            merged[wid][2].append(reason)

        for wid in list(merged.keys()):
            canon, losers, reasons = merged[wid]
            merged[wid] = (canon, list(dict.fromkeys(losers)), reasons)

        print(f"\nマージプラン: {len(merged)} 親、合計 loser {sum(len(v[1]) for v in merged.values())} 件")
        for wid, (canon, losers, reasons) in merged.items():
            print(f"  親 id={wid} corp={canon} losers={losers}")
            for rs in reasons:
                print(f"    ({rs})")

        yamato_cap = yamato_capital_from_csv(args.csv_capital)
        if yamato_cap:
            print(f"\n山都酒造 CSV 資本金（列{YAMATO_CAPITAL_COL_SHIFTED}）→ {yamato_cap:,} 円（{YAMATO_CAPITAL_MAN}万円）")
        else:
            print("\n山都酒造 CSV から資本金セルを特定できませんでした（スキップ）")

        id_to_row_lite = {str(r["id"]): r for r in rows}

        id_to_row_full: dict[str, dict] = {}
        if not dry_run and merged:
            need_ids: set[str] = set(merged.keys())
            for _w, (_c, loser_ids, _) in merged.items():
                need_ids.update(loser_ids)
            print(
                f"マージ対象 {len(need_ids):,} 件の全列を取得中…",
                flush=True,
            )
            id_to_row_full = fetch_companies_full_by_ids(conn, need_ids)

        if not dry_run:
            winners_list = list(merged.items())
            total_winners = len(winners_list)
            start_idx = 0
            reconnect_used = False
            losers_since_commit = 0
            processed_winners = 0

            def reconnect_conn():
                nonlocal conn
                try:
                    conn.close()
                except Exception:
                    pass
                conn = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    dbname=DB_NAME,
                    sslmode=DB_SSLMODE,
                )
                conn.autocommit = False

            while start_idx < total_winners:
                try:
                    for i in range(start_idx, total_winners):
                        wid, (canon, loser_ids, _) = winners_list[i]
                        winner = id_to_row_full.get(wid)
                        if not winner:
                            stats["winner_missing"] += 1
                            start_idx = i + 1
                            continue

                        merged_data = dict(winner)
                        for lid in loser_ids:
                            lo = id_to_row_full.get(lid)
                            if not lo:
                                stats["loser_missing"] += 1
                                continue
                            merged_data = merge_row_values(merged_data, lo, cols)

                        merged_data["corporate_number"] = canon
                        set_parts = []
                        vals = []
                        for c in cols:
                            if c not in merged_data:
                                continue
                            set_parts.append(f'"{c}" = %s')
                            v = merged_data[c]
                            # psycopg2 は dict/list をそのまま JSONB に適応できない場合がある
                            if c in jsonb_cols and v is not None and isinstance(v, (dict, list)):
                                vals.append(Json(v))
                            else:
                                vals.append(v)
                        sql_u = f'UPDATE companies SET {", ".join(set_parts)} WHERE id::text = %s'
                        vals.append(wid)
                        with conn.cursor() as cur:
                            cur.execute(sql_u, vals)

                        for lid in loser_ids:
                            for sch, tab, fkcol in fks:
                                n = rewire_fk(
                                    conn, sch, tab, fkcol, wid, lid, dry_run=False
                                )
                                if n:
                                    stats["fk_updates"] += n
                            with conn.cursor() as cur:
                                cur.execute(
                                    "DELETE FROM companies WHERE id::text = %s",
                                    (lid,),
                                )
                                stats["deleted"] += cur.rowcount

                        processed_winners += 1
                        start_idx = i + 1
                        losers_since_commit += len(loser_ids)

                        if losers_since_commit >= args.batch_size:
                            conn.commit()
                            print(
                                f"[進捗] winners={processed_winners}/{total_winners}, "
                                f"deleted={stats['deleted']}, commit(batch={args.batch_size})"
                            )
                            losers_since_commit = 0

                    break
                except psycopg2.OperationalError as e:
                    if args.reconnect_once and not reconnect_used:
                        reconnect_used = True
                        print(f"[警告] OperationalError: {e}。再接続して続行します。")
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        reconnect_conn()
                        continue
                    raise

            # 山都酒造 資本金・売上補正（正規法人番号行）
            canon_yamato = NAME_CANONICAL_CORPORATE.get("山都酒造株式会社")
            if canon_yamato and yamato_cap:
                set_parts = []
                params: list[object] = []
                if has_capital:
                    set_parts.append("capital = %s")
                    params.append(int(yamato_cap))
                if has_capital_stock:
                    set_parts.append("capital_stock = %s")
                    params.append(int(yamato_cap))
                if not set_parts:
                    print("[警告] capital/capital_stock 列が見つからないため資本金更新をスキップします。")
                else:
                    sql_cap = (
                        f"UPDATE companies SET {', '.join(set_parts)} "
                        "WHERE corporate_number = %s AND name = %s"
                    )
                    params.extend([canon_yamato, "山都酒造株式会社"])
                    with conn.cursor() as cur:
                        cur.execute(sql_cap, tuple(params))
                        stats["yamato_capital_set"] += cur.rowcount

            # 全社: 異常売上・利益の /1000（必ず純粋な数値で更新）
            batch = 500
            updated_rows = 0
            upd_sql = """
                UPDATE companies
                SET latest_revenue = %s,
                    latest_profit = COALESCE(%s, latest_profit)
                WHERE id::text = %s
            """
            with conn.cursor() as cur_w:
                with conn.cursor(name="rescale_cursor", cursor_factory=RealDictCursor) as cscan:
                    cscan.itersize = batch
                    cscan.execute(
                        """
                        SELECT id::text AS id, name, latest_revenue, latest_profit, employee_count
                        FROM companies
                        WHERE latest_revenue IS NOT NULL
                          AND latest_revenue > %s
                          AND (employee_count IS NULL OR employee_count < %s)
                        """,
                        (REVENUE_SME_THRESHOLD_YEN, SME_MAX_EMPLOYEES),
                    )
                    pending: list[tuple[int, int | None, str]] = []
                    while True:
                        rows_batch = cscan.fetchmany(batch)
                        if not rows_batch:
                            break
                        for r in rows_batch:
                            old_r = int(r["latest_revenue"])
                            new_r = old_r // 1000
                            new_p = None
                            if r["latest_profit"] is not None:
                                old_p = int(r["latest_profit"])
                                if old_p > PROFIT_SME_THRESHOLD_YEN:
                                    new_p = old_p // 1000
                            pending.append(
                                (
                                    int(new_r),
                                    int(new_p) if new_p is not None else None,
                                    str(r["id"]),
                                )
                            )
                            if len(pending) >= batch:
                                execute_batch(cur_w, upd_sql, pending, page_size=batch)
                                conn.commit()
                                updated_rows += len(pending)
                                stats["revenue_rescaled"] += len(pending)
                                print(f"[進捗] 売上/利益補正: {updated_rows}件更新完了...")
                                pending = []
                    if pending:
                        execute_batch(cur_w, upd_sql, pending, page_size=batch)
                        conn.commit()
                        updated_rows += len(pending)
                        stats["revenue_rescaled"] += len(pending)
                        print(f"[進捗] 売上/利益補正: {updated_rows}件更新完了...")

            print("\nコミット完了。")
        else:
            # DRY RUN
            for wid, (canon, loser_ids, _) in merged.items():
                winner = id_to_row_lite.get(wid)
                if not winner:
                    stats["winner_missing"] += 1
                    continue
                for lid in loser_ids:
                    for sch, tab, fkcol in fks:
                        n = rewire_fk(conn, sch, tab, fkcol, wid, lid, dry_run=True)
                        stats["fk_would_update"] += n
                stats["would_delete"] += len(loser_ids)

            canon_yamato = NAME_CANONICAL_CORPORATE.get("山都酒造株式会社")
            if canon_yamato and yamato_cap:
                cap_select_col = "capital" if has_capital else "capital_stock"
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(
                        f"""
                        SELECT id, name, {cap_select_col} AS cap_current FROM companies
                        WHERE corporate_number = %s AND name = %s
                        """,
                        (canon_yamato, "山都酒造株式会社"),
                    )
                    for rr in cur.fetchall():
                        print(
                            f"[予定] 山都酒造 資本金 {rr['cap_current']} -> {yamato_cap} "
                            f"(id={rr['id']})"
                        )
                        stats["yamato_capital_would_set"] += 1

            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, name, latest_revenue, latest_profit, employee_count
                    FROM companies
                    WHERE latest_revenue IS NOT NULL
                      AND latest_revenue > %s
                      AND (employee_count IS NULL OR employee_count < %s)
                    """,
                    (REVENUE_SME_THRESHOLD_YEN, SME_MAX_EMPLOYEES),
                )
                for r in cur.fetchall():
                    old_r = int(r["latest_revenue"])
                    new_r = old_r // 1000
                    print(
                        f"[予定] 売上補正 {r['name']}: {fmt_oku(old_r)} -> {fmt_oku(new_r)} "
                        f"(id={r['id']})"
                    )
                    stats["revenue_would_rescale"] += 1
                    if r["latest_profit"] is not None:
                        op = int(r["latest_profit"])
                        if op > PROFIT_SME_THRESHOLD_YEN:
                            print(
                                f"[予定] 利益補正 {r['name']}: {fmt_oku(op)} -> {fmt_oku(op // 1000)}"
                            )

            print("\n（DRY RUN）--execute でマージ・DELETE・補正を実行します。")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print("\n=== 統計 ===")
    for k, v in sorted(stats.items()):
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
