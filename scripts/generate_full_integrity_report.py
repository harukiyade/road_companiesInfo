#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3/unit_million・unit_yen 配下の全CSVをスキャンし、
UIテストで判明したデータズレ・フォーマット異常・マッピング漏れ等をカテゴリ別に洗い出す。

出力先: reports/full_integrity_YYYYMMDD_HHMMSS/

  python scripts/generate_full_integrity_report.py           # CSV + DB突合（要 POSTGRES_PASSWORD）
  python scripts/generate_full_integrity_report.py --csv-only
  python scripts/generate_full_integrity_report.py --max-rows-per-file 5000  # 巨大ファイルの試行用
  python scripts/generate_full_integrity_report.py --no-tqdm  # プログレスバーなし（ファイルごと print のみ）

進捗表示:
  - フェーズ開始・終了を print（flush）
  - `tqdm` が入っている場合はファイル単位のプログレスバー（`pip install tqdm`）

カテゴリ:
  1. 列ズレ・行のカラム数異常・強シグナル
  2. 日付フォーマット異常
  3. マッピング・ロジック（重複ヘッダ、住所、NDA、import_firstTime、財務単位メモ、株主列）
  4. UI/DB 切り分け（コード参照の静的調査結果を Markdown で出力）
  5. 法人名などの重複
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

try:
    from tqdm import tqdm as _tqdm_impl  # type: ignore[import-untyped]

    _HAS_TQDM = True
except ImportError:
    _tqdm_impl = None  # type: ignore[misc, assignment]
    _HAS_TQDM = False

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
FIXED_CSV_3 = PROJECT_ROOT / "fixed_csv_3"
REPORTS_BASE = PROJECT_ROOT / "reports"

LISTED_OK = {"上場", "非上場", "未上場", "未確認", "不明", ""}


def log_phase(title: str, detail: str = "") -> None:
    """ユーザーが待っている間、どの処理か分かるようにする"""
    line = f"\n{'='*60}\n[進捗] {title}"
    if detail:
        line += f"\n       {detail}"
    print(line, flush=True)


def log_done(title: str, extra: str = "") -> None:
    msg = f"[完了] {title}"
    if extra:
        msg += f"  {extra}"
    print(msg, flush=True)


def iter_csv_files_progress(
    files: list[Path],
    desc: str,
    *,
    use_tqdm: bool,
) :
    """CSV ファイル一覧を順に返す。tqdm 利用時はプログレスバー、未使用時は 1 ファイルごと print。"""
    files = sorted(files)
    n = len(files)
    if not files:
        print(f"[スキップ] {desc} — 対象ファイルなし", flush=True)
        return
    if use_tqdm and _HAS_TQDM:
        bar = _tqdm_impl(
            files,
            desc=desc,
            unit="file",
            dynamic_ncols=True,
            leave=True,
        )
        for fp in bar:
            short = fp.name[:42] + ("…" if len(fp.name) > 42 else "")
            bar.set_postfix_str(short, refresh=True)
            yield fp
    else:
        print(f"{desc} — 全 {n} ファイルを順に処理します", flush=True)
        for i, fp in enumerate(files, 1):
            print(f"  ({i}/{n}) {fp.parent.name}/{fp.name}", flush=True)
            yield fp


POSTAL_JP_RE = re.compile(r"^\d{3}-\d{4}$")


def postal_looks_valid(p: str) -> bool:
    if POSTAL_JP_RE.match(p):
        return True
    digits = re.sub(r"\D", "", p)
    return len(digits) == 7

# 日付: 日本語の「月○日○年」「4月1日1980年」等
RE_DATE_MONTH_DAY_YEAR = re.compile(r"月\s*\d{1,2}\s*日.*年|月\d{1,2}日\d{4}年")
RE_SLASH_SHORT_YEAR = re.compile(r"^\d{1,2}/\d{1,2}/\d{2}$")
RE_HAS_4DIGIT_YEAR = re.compile(r"(19|20)\d{2}")


def open_csv_text(path: Path):
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            return open(path, "r", encoding=enc, newline="")
        except Exception:
            continue
    return open(path, "r", encoding="utf-8", errors="replace", newline="")


def norm(s) -> str:
    return (s or "").strip()


def normalize_corp_cell(val: str) -> str | None:
    s = re.sub(r"[^\d]", "", norm(val))
    if len(s) == 12:
        s = "0" + s
    if len(s) == 13 and s.isdigit():
        return s
    return None


def header_key_counts(headers: list[str]) -> dict[str, int]:
    c: dict[str, int] = Counter()
    for h in headers:
        c[norm(h)] += 1
    return dict(c)


def is_import_firsttime_file(name: str) -> bool:
    return "import_firsttime" in name.lower()


def shift_flags_from_row(row: list[str], headers: list[str]) -> list[str]:
    """列名と値のリストから列ズレ疑いフラグを付与（標準系向け）。行長とヘッダ長は一致している前提。"""
    d = {headers[i]: row[i] for i in range(len(headers))}
    flags = []
    rz = norm(d.get("代表者郵便番号", ""))
    if rz and rz in LISTED_OK:
        flags.append("rep_zip_is_listed_status")
    if rz and "年" in rz and "月" in rz:
        flags.append("rep_zip_looks_like_date")
    ph = norm(d.get("電話番号(窓口)", ""))
    if ph and ph not in ("0",) and re.fullmatch(r"\d{1,7}", ph):
        flags.append("phone_pure_small_int")
    lst = norm(d.get("上場", ""))
    if lst and lst not in LISTED_OK and len(lst) > 10:
        flags.append("listed_long_text")
    cap = norm(d.get("資本金", ""))
    if cap and len(cap) > 12 and not re.fullmatch(r"[\d,]+", cap.replace("，", ",")):
        flags.append("capital_not_numeric")
    ra = norm(d.get("代表者住所", ""))
    if ra and re.match(r"^\d{4}年\d{1,2}月", ra) and len(ra) < 25:
        flags.append("rep_addr_looks_like_close_date")
    # 法人番号の形が崩れている
    cn = norm(d.get("法人番号", ""))
    if cn and not normalize_corp_cell(cn):
        flags.append("corporate_number_invalid_shape")
    # 本社郵便（あれば）
    p = norm(d.get("郵便番号", ""))
    if p and not postal_looks_valid(p) and re.search(r"\d", p):
        flags.append("hq_postal_malformed")
    if rz and not postal_looks_valid(rz) and re.search(r"\d", rz):
        flags.append("rep_postal_malformed")
    return flags


def classify_date_value(val: str) -> list[str]:
    """1セルの日付として疑わしいパターンのラベル一覧（空なら []）"""
    v = norm(val)
    if not v:
        return []
    tags = []
    if RE_DATE_MONTH_DAY_YEAR.search(v):
        tags.append("jp_month_day_year_style")
    if RE_SLASH_SHORT_YEAR.match(v):
        tags.append("slash_mdy_2digit_year")
    if "月" in v and "日" in v and "年" in v and not RE_HAS_4DIGIT_YEAR.search(v):
        tags.append("jp_date_without_19xx_20xx")
    m4 = re.search(r"(\d{4})", v)
    if m4:
        y = int(m4.group(1))
        if y < 1800 or y > 2100:
            tags.append(f"year_out_of_range:{y}")
    return tags


DATE_HEADER_CANDIDATES = frozenset(
    {
        "設立",
        "代表者誕生日",
        "直近決算年月",
        "創業",
        "設立年月日",
        "設立年月日(西暦)",
    }
)


def scan_category1(
    out_rows_len: list,
    out_shift: list,
    summary: defaultdict,
    max_rows: int,
    use_tqdm: bool,
):
    """列数不一致 + 標準スキーマ向け列ズレ強シグナル"""
    log_phase(
        "カテゴリ1: 列ズレ・列数不一致チェック",
        "各CSVの全行を走査（列数不一致 / 郵便・法人番号・電話などの強シグナル）",
    )
    for sub in ("unit_million", "unit_yen"):
        root = FIXED_CSV_3 / sub
        if not root.is_dir():
            continue
        file_list = list(root.glob("*.csv"))
        for fp in iter_csv_files_progress(
            file_list,
            desc=f"Cat1[{sub}]",
            use_tqdm=use_tqdm,
        ):
            rel = f"{sub}/{fp.name}"
            with open_csv_text(fp) as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers:
                    continue
                n_h = len(headers)
                std = "電話番号(窓口)" in headers and "法人番号" in headers
                ift = is_import_firsttime_file(fp.name)
                for lineno, row in enumerate(reader, start=2):
                    if max_rows and lineno - 2 >= max_rows:
                        break
                    if len(row) != n_h:
                        out_rows_len.append(
                            (sub, fp.name, lineno, n_h, len(row), row[0][:60] if row else "")
                        )
                        summary[rel]["cat1_len"] += 1
                        continue
                    if std and not ift:
                        fl = shift_flags_from_row(row, headers)
                        if fl:
                            corp = ""
                            nm = ""
                            try:
                                ix = headers.index("法人番号")
                                corp = norm(row[ix]) if ix < len(row) else ""
                            except ValueError:
                                pass
                            try:
                                ix = headers.index("会社名")
                                nm = norm(row[ix])[:80] if ix < len(row) else ""
                            except ValueError:
                                pass
                            out_shift.append(
                                (sub, fp.name, lineno, ";".join(fl), corp, nm)
                            )
                            summary[rel]["cat1_shift"] += 1
    log_done("カテゴリ1")


def scan_category2(out: list, summary: defaultdict, max_rows: int, use_tqdm: bool):
    """全ファイル・日付っぽい列を走査"""
    log_phase(
        "カテゴリ2: 日付フォーマット異常チェック",
        "設立・代表者誕生日・直近決算年月・創業 等の列を走査",
    )
    for sub in ("unit_million", "unit_yen"):
        root = FIXED_CSV_3 / sub
        if not root.is_dir():
            continue
        file_list = list(root.glob("*.csv"))
        for fp in iter_csv_files_progress(
            file_list,
            desc=f"Cat2[{sub}]",
            use_tqdm=use_tqdm,
        ):
            rel = f"{sub}/{fp.name}"
            with open_csv_text(fp) as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers:
                    continue
                col_indexes = [
                    i for i, h in enumerate(headers) if norm(h) in DATE_HEADER_CANDIDATES
                ]
                if not col_indexes:
                    continue
                for lineno, row in enumerate(reader, start=2):
                    if max_rows and lineno - 2 >= max_rows:
                        break
                    for i in col_indexes:
                        if i >= len(row):
                            continue
                        tags = classify_date_value(row[i])
                        if not tags:
                            continue
                        hname = headers[i]
                        corp_guess = ""
                        name_guess = ""
                        if "法人番号" in headers:
                            j = headers.index("法人番号")
                            if j < len(row):
                                corp_guess = norm(row[j])
                        if "会社名" in headers:
                            j = headers.index("会社名")
                            if j < len(row):
                                name_guess = norm(row[j])[:60]
                        out.append(
                            (
                                sub,
                                fp.name,
                                lineno,
                                hname,
                                ";".join(tags),
                                row[i][:120],
                                corp_guess,
                                name_guess,
                            )
                        )
                        summary[rel]["cat2"] += 1
    log_done("カテゴリ2")


def scan_category3(
    out_dup_headers: list,
    out_addr: list,
    out_ift: list,
    out_fin: list,
    out_sh: list,
    summary: defaultdict,
    max_rows: int,
    use_tqdm: bool,
):
    log_phase(
        "カテゴリ3: マッピング・重複ヘッダ・住所・財務・株主チェック",
        "3a重複ヘッダ / 3b import_firstTime J列 / 3c 本社・代表住所 / 3d unit_yen売上 / 3e 株主-仕入れ先",
    )
    # 3a 重複ヘッダ
    for sub in ("unit_million", "unit_yen"):
        root = FIXED_CSV_3 / sub
        if not root.is_dir():
            continue
        file_list = list(root.glob("*.csv"))
        for fp in iter_csv_files_progress(
            file_list,
            desc=f"Cat3a-b[{sub}]",
            use_tqdm=use_tqdm,
        ):
            rel = f"{sub}/{fp.name}"
            with open_csv_text(fp) as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers:
                    continue
                hc = header_key_counts(headers)
                dups = [(k, v) for k, v in hc.items() if v > 1 and k]
                if dups:
                    out_dup_headers.append(
                        (
                            sub,
                            fp.name,
                            ";".join(f"{k}×{v}" for k, v in sorted(dups)),
                            "import_firstTime列固定パターン対象" if is_import_firsttime_file(fp.name) else "要:列位置マップまたはヘッダ改名",
                        )
                    )
                    summary[rel]["cat3_dup_hdr"] = 1

            if not is_import_firsttime_file(fp.name):
                continue
            # 3b import_firstTime: J列(インデックス9)が埋まっているが概要にマージされない旧ロジック向けメモ
            with open_csv_text(fp) as f:
                reader = csv.reader(f)
                h = next(reader, None)
                if not h or len(h) < 11:
                    continue
                j_non_empty = 0
                lines_sample = []
                for lineno, row in enumerate(reader, start=2):
                    if max_rows and lineno - 2 >= max_rows:
                        break
                    if len(row) > 9 and norm(row[9]):
                        j_non_empty += 1
                        if len(lines_sample) < 5:
                            lines_sample.append(str(lineno))
                if j_non_empty:
                    out_ift.append(
                        (
                            sub,
                            fp.name,
                            j_non_empty,
                            ",".join(lines_sample),
                            "import_full_update_fast の import_firsttime パターンで overview に【事業構成】追記",
                        )
                    )
                    summary[rel]["cat3_ift_j"] += j_non_empty

    # 3c 本社/代表 同一・逆転疑い（標準スキーマ）
    print("\n[進捗] カテゴリ3c: 本社/代表 住所・郵便番号の同一・乖離疑い", flush=True)
    for sub in ("unit_million", "unit_yen"):
        root = FIXED_CSV_3 / sub
        if not root.is_dir():
            continue
        file_list = list(root.glob("*.csv"))
        for fp in iter_csv_files_progress(
            file_list,
            desc=f"Cat3c[{sub}]",
            use_tqdm=use_tqdm,
        ):
            rel = f"{sub}/{fp.name}"
            if is_import_firsttime_file(fp.name):
                continue
            with open_csv_text(fp) as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers:
                    continue
                need = {"郵便番号", "住所", "代表者郵便番号", "代表者住所"}
                if not need.issubset(set(norm(h) for h in headers)):
                    continue
                # 同名ヘッダがある場合は先頭の列のみ見る
                def first_idx(name: str) -> int:
                    for i, h in enumerate(headers):
                        if norm(h) == name:
                            return i
                    return -1

                i_hp = first_idx("郵便番号")
                i_ha = first_idx("住所")
                i_rp = first_idx("代表者郵便番号")
                i_ra = first_idx("代表者住所")
                if min(i_hp, i_ha, i_rp, i_ra) < 0:
                    continue
                for lineno, row in enumerate(reader, start=2):
                    if max_rows and lineno - 2 >= max_rows:
                        break
                    if len(row) != len(headers):
                        continue
                    def g(i):
                        return norm(row[i]) if i < len(row) else ""

                    hp, ha, rp, ra = g(i_hp), g(i_ha), g(i_rp), g(i_ra)
                    if not (hp or ha or rp or ra):
                        continue
                    reasons = []
                    if hp and rp and hp == rp and ha and ra and ha != ra:
                        reasons.append("hq_rep_postal_same_addr_diff")
                    if ha and ra and ha == ra and hp and rp and hp != rp:
                        reasons.append("hq_rep_addr_same_postal_diff")
                    if ha and ra and ha == ra and hp and rp and hp == rp and len(ha) > 15:
                        reasons.append("hq_rep_all_identical_long")
                    if reasons:
                        cn = (
                            g(first_idx("法人番号"))
                            if any(norm(x) == "法人番号" for x in headers)
                            else ""
                        )
                        nm = (
                            g(first_idx("会社名"))[:60]
                            if any(norm(x) == "会社名" for x in headers)
                            else ""
                        )
                        out_addr.append(
                            (sub, fp.name, lineno, ";".join(reasons), hp, rp, ha[:40], ra[:40], cn, nm)
                        )
                        summary[rel]["cat3_addr"] += 1

    # 3d 財務: 単位語なし数値 → import で円換算（unit_yen×1000 / unit_million×1e6）のメモ用フラグ
    print("\n[進捗] カテゴリ3d: 直近売上（単位語なし数値メモ・フォルダ別スケール）", flush=True)
    for sub, scale_note in (
        ("unit_yen", "無印→×1,000（千円→円）"),
        ("unit_million", "無印→×1,000,000（百万円→円）"),
    ):
        root = FIXED_CSV_3 / sub
        if not root.is_dir():
            continue
        file_list_sub = list(root.glob("*.csv"))
        for fp in iter_csv_files_progress(
            file_list_sub,
            desc=f"Cat3d[{sub}]",
            use_tqdm=use_tqdm,
        ):
            rel = f"{sub}/{fp.name}"
            if is_import_firsttime_file(fp.name):
                continue
            with open_csv_text(fp) as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers or "直近売上" not in headers:
                    continue
                i_rev = headers.index("直近売上")
                for lineno, row in enumerate(reader, start=2):
                    if max_rows and lineno - 2 >= max_rows:
                        break
                    if len(row) <= i_rev:
                        continue
                    raw = norm(row[i_rev])
                    if not raw or raw in ("0", "nan"):
                        continue
                    s = raw.replace(",", "").replace("　", "")
                    if re.search(r"[万亿百万円]", s):
                        continue
                    if re.fullmatch(r"-?\d+(\.\d+)?", s):
                        out_fin.append(
                            (
                                sub,
                                fp.name,
                                lineno,
                                raw[:40],
                                f"import_full_update_fast: {scale_note}（セルに単位語ありなら別係数）",
                                norm(row[headers.index("法人番号")])
                                if "法人番号" in headers
                                else "",
                            )
                        )
                        summary[rel]["cat3_fin"] += 1

    # 3e 株主: 標準で「株主」空だが「仕入れ先」に株主っぽい（％を含む）文言 — 軽い列ずれ疑い
    print("\n[進捗] カテゴリ3e: 株主空・仕入れ先に％（列ずれ疑い）", flush=True)
    for sub in ("unit_million", "unit_yen"):
        root = FIXED_CSV_3 / sub
        if not root.is_dir():
            continue
        file_list = list(root.glob("*.csv"))
        for fp in iter_csv_files_progress(
            file_list,
            desc=f"Cat3e[{sub}]",
            use_tqdm=use_tqdm,
        ):
            rel = f"{sub}/{fp.name}"
            if is_import_firsttime_file(fp.name):
                continue
            with open_csv_text(fp) as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers:
                    continue
                if "株主" not in headers or "仕入れ先" not in headers:
                    continue
                is_h = headers.index("株主")
                is_s = headers.index("仕入れ先")
                for lineno, row in enumerate(reader, start=2):
                    if max_rows and lineno - 2 >= max_rows:
                        break
                    if len(row) != len(headers):
                        continue
                    sh = norm(row[is_h])
                    su = norm(row[is_s])
                    if not sh and su and ("％" in su or "%" in su):
                        out_sh.append(
                            (
                                sub,
                                fp.name,
                                lineno,
                                su[:100],
                                norm(row[headers.index("法人番号")])
                                if "法人番号" in headers
                                else "",
                            )
                        )
                        summary[rel]["cat3_sh_gap"] += 1
    log_done("カテゴリ3")


def scan_category5_names(out: list, summary: defaultdict, max_rows: int, use_tqdm: bool):
    """会社名の完全一致重複（ファイル横断で集計）"""
    log_phase(
        "カテゴリ5: 会社名の重複（全ファイル横断で集計）",
        "1 pass で名前→出現箇所を蓄積し、2 pass でグループ化",
    )
    key_locs: dict[str, list[tuple[str, str, int, str]]] = defaultdict(list)
    for sub in ("unit_million", "unit_yen"):
        root = FIXED_CSV_3 / sub
        if not root.is_dir():
            continue
        file_list = list(root.glob("*.csv"))
        for fp in iter_csv_files_progress(
            file_list,
            desc=f"Cat5-pass1[{sub}]",
            use_tqdm=use_tqdm,
        ):
            with open_csv_text(fp) as f:
                reader = csv.reader(f)
                headers = next(reader, None)
                if not headers or "会社名" not in headers:
                    continue
                ix = headers.index("会社名")
                for lineno, row in enumerate(reader, start=2):
                    if max_rows and lineno - 2 >= max_rows:
                        break
                    if ix >= len(row):
                        continue
                    name = norm(row[ix])
                    if len(name) < 2:
                        continue
                    cn = ""
                    if "法人番号" in headers:
                        ci = headers.index("法人番号")
                        if ci < len(row):
                            cn = norm(row[ci])
                    key_locs[name].append((sub, fp.name, lineno, cn))

    dup_candidates = [(n, locs) for n, locs in key_locs.items() if len(locs) >= 2]
    dup_candidates.sort(key=lambda x: (-len(x[1]), x[0]))
    print(
        f"\n[進捗] カテゴリ5 pass2: 重複グループ {len(dup_candidates)} 件を出力用に整形 …",
        flush=True,
    )
    wrap = (
        _tqdm_impl(dup_candidates, desc="Cat5-pass2", unit="grp", dynamic_ncols=True)
        if (use_tqdm and _HAS_TQDM)
        else dup_candidates
    )
    for name, locs in wrap:
        files = Counter(f"{a}/{b}" for a, b, _, _ in locs)
        corp_set = {c for *_, c in locs if c}
        out.append(
            (
                name[:200],
                len(locs),
                len(files),
                len(corp_set),
                ";".join(f"{k}:{v}" for k, v in files.most_common(8)),
            )
        )
        for sub, fn, ln, cn in locs[:20]:
            summary[f"{sub}/{fn}"]["cat5_dup_name"] += 1
    log_done("カテゴリ5")


def run_db_audit(out_dir: Path, csv_only: bool) -> tuple[str, Path | None]:
    audit_script = SCRIPT_DIR / "audit_csv_vs_db_integrity.py"
    report = out_dir / "01_csv_vs_db_audit.csv"
    null_only = out_dir / "02_csv_val_but_db_null.csv"
    if csv_only or not os.getenv("POSTGRES_PASSWORD"):
        return "skipped_no_password", None
    env = os.environ.copy()
    if env.get("POSTGRES_HOST") in ("127.0.0.1", "localhost") and not env.get("POSTGRES_SSLMODE"):
        env["POSTGRES_SSLMODE"] = "disable"
    cmd = [
        sys.executable,
        str(audit_script),
        "--report",
        str(report),
        "--null-only",
        str(null_only),
        "--quiet",
    ]
    log_phase(
        "DB突合サブプロセス",
        f"`{audit_script.name}` を実行中（完了まで無出力の場合があります）…",
    )
    r = subprocess.run(cmd, cwd=str(PROJECT_ROOT), env=env, capture_output=True, text=True)
    log_done("DB突合サブプロセス", f"exit={r.returncode}")
    log_path = out_dir / "00_db_audit_subprocess.log"
    log_path.write_text(
        f"exit={r.returncode}\n--- stdout ---\n{r.stdout}\n--- stderr ---\n{r.stderr}",
        encoding="utf-8",
    )
    if r.returncode != 0:
        return f"failed_exit_{r.returncode}", log_path
    return "ok", log_path


def write_category4_static_md(path: Path):
    """DB に無い / UI が見ていない の切り分け用（コードベース静的調査）"""
    body = """# カテゴリ4: UI 表示漏れ vs DB 保存漏れ（静的調査メモ）

このファイルは **リポジトリ内の実装を読んだ推定**です。確定には DB の実値確認と API レスポンスの確認が必要です。

## 直近決算年月

- **CSV 列名**: `直近決算年月` → `import_full_update_fast.py` の `HEADER_MAP` で `latest_fiscal_year_month` にマップ **（実装済み）**。
- **DB**: `companies.latest_fiscal_year_month`（欠ける場合は `backend/sql/migration_20260323_import_fast_columns.sql`）。
- **API**: `backend/scripts/search_ui_optimized.py` の検索結果に `latestFiscalYearMonth` を含む **（実装済み）**。
- 値があるのに UI に出ない場合は **別 API 経路**のフィールド定義を確認。

## 代表者誕生日

- DB: `representative_birth_date DATE`
- CSV が空、または日付パース不能なら **DB に入らない**のは正常。
- 値があるのに UI に出ない場合は API が `representativeBirthDate` を返しているか確認。

## 仕入れ先（suppliers）

- `仕入れ先` → `suppliers`。UPSERT は **`COALESCE(EXCLUDED, companies)`（CSV 非 NULL 優先）**（`import_full_update_fast.py`）。
- UI/API で `suppliers` を返しているか確認（検索 API では `suppliers` キーを追加済みの場合あり）。

## 株主（shareholders）

- `株主` / `株式保有率` / `主要株主` → `shareholders`。同上 **CSV 非 NULL 優先**。
- **import_firstTime** は `IMPORT_FIRSTTIME_INDEX_MAP` で列固定。

## NDA（締結済）

- CSV の `NDA` が `締結済` のとき、**旧 `parse_bool` は完全一致 `済` のみ True** であり `締結済` が False になる不具合があった。
- **`import_full_update_fast.parse_bool` を修正済み**（`締結済` / `契約済` / `済み` を含む場合 True）。
- それでも UI で無効なら `nda_flag` の API 露出と UI のバインドを確認。

---
*自動生成: `generate_full_integrity_report.py`*
"""
    path.write_text(body, encoding="utf-8")


def write_code_recommendations_md(path: Path):
    rec = """# インポート・移行コード修正案（サマリ）

## 1. NDA「締結済」が有効にならない

- **原因例**: `parse_bool` が `済` の完全一致のみ True としていた。
- **対応**: `scripts/import_full_update_fast.py` の `parse_bool` に `締結済` 等を追加 **（適用済み）**。

## 2. import_firstTime 系の本社/代表住所の取り違え・株主マッピング

- **原因**: ヘッダ重複により `norm_to_idx` が最後の列だけ残る。
- **対応**: ファイル名に `import_firsttime` を含む CSV は `IMPORT_FIRSTTIME_INDEX_MAP` で列固定 **（`import_full_update_fast.py` に実装済み）**。
- **運用**: 該当 CSV を再インポート。

## 3. 直近決算年月・設立日・事業詳細

- `直近決算年月` → `latest_fiscal_year_month`、`設立` → `founding`（DATE）+ `founding_year`、import_firstTime の J列 → `business_summary` 追記など **実装済み**（`import_full_update_fast.py`）。
- 列欠けは `backend/sql/migration_20260323_import_fast_columns.sql` を参照。

## 4. 財務スケール（円ベース）

- **unit_yen**: 単位語なし **×1,000**（千円→円）。**unit_million**: **×1,000,000**（百万円→円）。売上・利益・資本金が対象。
- セルに「億」「百万円」「万円」等がある場合は **その単位係数のみ**。

## 5. 列ズレ・日付・同名名寄せ

- 列数不一致等は **スキップ**＋`reports/import_skip_log.jsonl`。
- 日付は `backend.api.csv_founding_date`（設立はカレンダーのみ・シリアル禁止、誕生日は年範囲内シリアルのみ）。法人番号なしは **会社名+都道府県** で安定 ID で UPSERT。

---
*自動生成: `generate_full_integrity_report.py`*
"""
    path.write_text(rec, encoding="utf-8")


def scan_nda_summary_rows(out_dir: Path, use_tqdm: bool) -> tuple[int, Path]:
    """NDA=締結済 の件数（ファイル別）"""
    path_out = out_dir / "15_NDA締結済_件数サマリ.tsv"
    log_phase("NDA=締結済 件数サマリ", "全CSVを走査して TSV を出力")
    rows = []
    total = 0
    for sub in ("unit_million", "unit_yen"):
        root = FIXED_CSV_3 / sub
        if not root.is_dir():
            continue
        file_list = list(root.glob("*.csv"))
        for fp in iter_csv_files_progress(
            file_list,
            desc=f"NDA[{sub}]",
            use_tqdm=use_tqdm,
        ):
            n = 0
            with open_csv_text(fp) as f:
                dr = csv.DictReader(f)
                if not dr.fieldnames or "NDA" not in dr.fieldnames:
                    continue
                for row in dr:
                    if norm(row.get("NDA", "")) == "締結済":
                        n += 1
            if n:
                total += n
                rows.append((sub, fp.name, n))
    with open(path_out, "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(["folder", "file", "nda_seijyun_rows"])
        w.writerows(sorted(rows))
    log_done("NDAサマリ", f"合計 {total} 行（締結済）")
    return total, path_out


def scan_csv_inventory(out_dir: Path, use_tqdm: bool) -> tuple[int, Path]:
    path_out = out_dir / "16_csv_inventory.tsv"
    log_phase("CSVインベントリ", "行数・エンコーディング推定（全ファイル）")
    rows = []
    for sub in ("unit_million", "unit_yen"):
        root = FIXED_CSV_3 / sub
        if not root.is_dir():
            continue
        file_list = list(root.glob("*.csv"))
        for fp in iter_csv_files_progress(
            file_list,
            desc=f"Inventory[{sub}]",
            use_tqdm=use_tqdm,
        ):
            enc = "utf-8"
            sample = fp.read_bytes()[:65536]
            for e in ("utf-8-sig", "utf-8", "cp932"):
                try:
                    sample.decode(e)
                    enc = e
                    break
                except Exception:
                    continue
            nrows = 0
            hdr = ""
            with open_csv_text(fp) as f:
                dr = csv.DictReader(f)
                hdr = "|".join(dr.fieldnames or [])
                nrows = sum(1 for _ in dr)
            rows.append((sub, fp.name, nrows, len(hdr), enc))
    with open(path_out, "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(["folder", "file", "data_rows", "header_chars", "encoding_guess"])
        w.writerows(rows)
    log_done("CSVインベントリ", f"{len(rows)} ファイル")
    return len(rows), path_out


def write_summary_tsv(path: Path, summary: defaultdict):
    rows = []
    for rel in sorted(summary.keys()):
        d = summary[rel]
        rows.append(
            (
                rel,
                d.get("cat1_len", 0),
                d.get("cat1_shift", 0),
                d.get("cat2", 0),
                d.get("cat3_dup_hdr", 0),
                d.get("cat3_ift_j", 0),
                d.get("cat3_addr", 0),
                d.get("cat3_fin", 0),
                d.get("cat3_sh_gap", 0),
                d.get("cat5_dup_name", 0),
            )
        )
    with open(path, "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(
            [
                "file_rel",
                "cat1_column_count_mismatch",
                "cat1_shift_heuristic",
                "cat2_date_anomaly",
                "cat3_dup_header_file",
                "cat3_ift_j_nonempty_rows",
                "cat3_addr_anomaly",
                "cat3_fin_unit_note",
                "cat3_shareholder_gap",
                "cat5_dup_name_hits",
            ]
        )
        w.writerows(rows)


def write_readme(
    out_dir: Path,
    counts: dict,
    db_status: str,
    csv_only: bool,
    max_rows: int,
):
    readme = out_dir / "README.md"
    mr_note = f"\n> 試行モード: `--max-rows-per-file {max_rows}`（ファイルあたりの最大走査行数）\n" if max_rows else ""
    lines = [
        f"# 整合性レポート一式 ({out_dir.name})",
        "",
        f"- 生成日時: {datetime.now().isoformat(timespec='seconds')}",
        f"- 対象: `{FIXED_CSV_3.relative_to(PROJECT_ROOT)}` の `unit_million` / `unit_yen` 配下の全 `*.csv`",
        mr_note,
        "",
        "## 出力ファイル（カテゴリ別）",
        "",
        "| ファイル | 内容 |",
        "|----------|------|",
        "| `00_db_audit_subprocess.log` | DB突合サブプロセスログ |",
        "| `01_csv_vs_db_audit.csv` | CSV vs DB 突合 |",
        "| `02_csv_val_but_db_null.csv` | CSVに値あり・DB NULL |",
        "| `03_cat1_row_column_count_mismatch.tsv` | **カテゴリ1**: ヘッダ列数と行の列数が不一致 |",
        "| `04_cat1_shift_heuristics.tsv` | **カテゴリ1**: 郵便・法人番号・電話などの強シグナル |",
        "| `05_cat2_date_anomalies.tsv` | **カテゴリ2**: 日付フォーマット疑い |",
        "| `06_cat3_duplicate_headers.tsv` | **カテゴリ3**: 重複ヘッダ（マッピング衝突リスク） |",
        "| `07_cat3_import_firsttime_j_column.tsv` | **カテゴリ3**: import_firstTime の J 列（事業構成）非空行数 |",
        "| `08_cat3_hq_rep_address_anomalies.tsv` | **カテゴリ3**: 本社/代表の同一・乖離疑い |",
        "| `09_cat3_unit_yen_revenue_unit_notes.tsv` | **カテゴリ3**: 単位語なし売上（yen/million フォルダ別スケールのメモ） |",
        "| `10_cat3_shareholder_vs_supplier_gap.tsv` | **カテゴリ3**: 株主空・仕入れ先に％（列ずれ疑い） |",
        "| `11_cat4_ui_db_static_analysis.md` | **カテゴリ4**: UI/DB 切り分けの静的メモ |",
        "| `12_cat5_duplicate_company_names.tsv` | **カテゴリ5**: 同一会社名の重複一覧 |",
        "| `13_summary_by_file.tsv` | ファイル別ヒット数サマリ |",
        "| `14_code_fix_recommendations.md` | コード修正案 |",
        "| `15_NDA締結済_件数サマリ.tsv` | NDA=締結済 件数（ファイル別） |",
        "| `16_csv_inventory.tsv` | 全CSV行数・エンコーディング推定 |",
        "",
        "## サマリ（件数）",
        "",
        f"- カテゴリ1 列数不一致行: **{counts['c1_len']}**",
        f"- カテゴリ1 列ズレ強シグナル行: **{counts['c1_shift']}**",
        f"- カテゴリ2 日付異常セル: **{counts['c2']}**",
        f"- カテゴリ3 重複ヘッダファイル: **{counts['c3_dh']}**",
        f"- カテゴリ3 import_firstTime J列非空（行集計）: **{counts['c3_j']}**",
        f"- カテゴリ3 住所異常行: **{counts['c3_addr']}**",
        f"- カテゴリ3 財務単位メモ行: **{counts['c3_fin']}**",
        f"- カテゴリ3 株主/仕入れ先ギャップ行: **{counts['c3_sh']}**",
        f"- カテゴリ5 重複会社名グループ: **{counts['c5_groups']}**",
        f"- NDA締結済（合算）: **{counts['nda']}**",
        f"- CSVファイル数: **{counts['inv']}**",
        f"- DB突合: **{db_status}**",
        "",
        "## DB突合",
        "",
        "```bash",
        "export POSTGRES_HOST=127.0.0.1",
        "export POSTGRES_PORT=5434",
        "export POSTGRES_SSLMODE=disable",
        "export POSTGRES_PASSWORD='...'",
        "python scripts/generate_full_integrity_report.py",
        "```",
        "",
    ]
    if csv_only:
        lines.append("> `--csv-only` または `POSTGRES_PASSWORD` 未設定のため DB 突合はスキップしました。")
        lines.append("")
    readme.write_text("\n".join(lines), encoding="utf-8")


def parse_args():
    p = argparse.ArgumentParser(description="fixed_csv_3 全CSV 整合性レポート")
    p.add_argument("--csv-only", action="store_true", help="DB突合をスキップ")
    p.add_argument(
        "--max-rows-per-file",
        type=int,
        default=0,
        help="ファイルあたりの最大走査行数（0で無制限。巨大CSVの試行用）",
    )
    p.add_argument(
        "--no-tqdm",
        action="store_true",
        help="tqdm を使わず、ファイルごとの print のみで進捗表示",
    )
    return p.parse_args()


def main():
    args = parse_args()
    csv_only = args.csv_only
    max_rows = max(0, args.max_rows_per_file)
    use_tqdm = not args.no_tqdm and _HAS_TQDM

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = REPORTS_BASE / f"full_integrity_{ts}"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not FIXED_CSV_3.is_dir():
        print(f"エラー: {FIXED_CSV_3} がありません", file=sys.stderr)
        sys.exit(1)

    print(
        f"\n{'#'*60}\n# 整合性レポート開始\n"
        f"# 出力: {out_dir}\n"
        f"# max_rows_per_file: {max_rows or '無制限'}\n"
        f"# プログレスバー(tqdm): {'ON' if use_tqdm else 'OFF'}"
        + ("" if use_tqdm or args.no_tqdm else "（pip install tqdm で有効化）")
        + f"\n{'#'*60}\n",
        flush=True,
    )

    summary: defaultdict = defaultdict(lambda: defaultdict(int))

    c1_len: list = []
    c1_shift: list = []
    scan_category1(c1_len, c1_shift, summary, max_rows, use_tqdm)
    print("\n[進捗] カテゴリ1のTSVを書き込み中 …", flush=True)
    with open(out_dir / "03_cat1_row_column_count_mismatch.tsv", "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(["folder", "file", "line", "header_cols", "row_cols", "first_cell"])
        w.writerows(c1_len)
    with open(out_dir / "04_cat1_shift_heuristics.tsv", "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(["folder", "file", "line", "flags", "法人番号", "会社名"])
        w.writerows(c1_shift)

    c2: list = []
    scan_category2(c2, summary, max_rows, use_tqdm)
    print("[進捗] カテゴリ2のTSVを書き込み中 …", flush=True)
    with open(out_dir / "05_cat2_date_anomalies.tsv", "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(["folder", "file", "line", "column", "tags", "value", "法人番号", "会社名"])
        w.writerows(c2)

    c3_dh: list = []
    c3_ift: list = []
    c3_addr: list = []
    c3_fin: list = []
    c3_sh: list = []
    scan_category3(c3_dh, c3_addr, c3_ift, c3_fin, c3_sh, summary, max_rows, use_tqdm)
    print("[進捗] カテゴリ3のTSVを書き込み中 …", flush=True)
    with open(out_dir / "06_cat3_duplicate_headers.tsv", "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(["folder", "file", "duplicate_headers", "note"])
        w.writerows(c3_dh)
    with open(out_dir / "07_cat3_import_firsttime_j_column.tsv", "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(["folder", "file", "j_nonempty_rows", "sample_lines", "note"])
        w.writerows(c3_ift)
    with open(out_dir / "08_cat3_hq_rep_address_anomalies.tsv", "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(
            [
                "folder",
                "file",
                "line",
                "reasons",
                "hq_postal",
                "rep_postal",
                "hq_addr_snip",
                "rep_addr_snip",
                "法人番号",
                "会社名",
            ]
        )
        w.writerows(c3_addr)
    with open(out_dir / "09_cat3_unit_yen_revenue_unit_notes.tsv", "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(["folder", "file", "line", "直近売上_raw", "note", "法人番号"])
        w.writerows(c3_fin)
    with open(out_dir / "10_cat3_shareholder_vs_supplier_gap.tsv", "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(["folder", "file", "line", "仕入れ先_snip", "法人番号"])
        w.writerows(c3_sh)

    log_phase(
        "カテゴリ4: 静的 Markdown 出力",
        "UI/DB 切り分けメモ・コード修正案（ファイル書き込みのみ）",
    )
    write_category4_static_md(out_dir / "11_cat4_ui_db_static_analysis.md")
    write_code_recommendations_md(out_dir / "14_code_fix_recommendations.md")
    log_done("カテゴリ4")

    c5: list = []
    scan_category5_names(c5, summary, max_rows, use_tqdm)
    print("[進捗] カテゴリ5のTSV・サマリを書き込み中 …", flush=True)
    with open(out_dir / "12_cat5_duplicate_company_names.tsv", "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf, delimiter="\t")
        w.writerow(["会社名", "出現行数_全ファイル", "ファイル種類数", "法人番号の種類数", "ファイル内訳_top"])
        w.writerows(c5)

    write_summary_tsv(out_dir / "13_summary_by_file.tsv", summary)

    nda_total, _ = scan_nda_summary_rows(out_dir, use_tqdm)
    inv_n, _ = scan_csv_inventory(out_dir, use_tqdm)

    db_status = "skipped (--csv-only)" if csv_only else "skipped (no POSTGRES_PASSWORD)"
    if not csv_only and os.getenv("POSTGRES_PASSWORD"):
        status, _ = run_db_audit(out_dir, csv_only=False)
        db_status = status
    else:
        (out_dir / "01_csv_vs_db_audit.csv").write_text(
            "ファイル名,ID,カラム名,CSVの値,DBの値,不一致の種類\n"
            "# DB突合未実行。POSTGRES_PASSWORD を設定して再実行してください。\n",
            encoding="utf-8",
        )
        (out_dir / "02_csv_val_but_db_null.csv").write_text(
            "ファイル名,ID,カラム名,CSVの値,DBの値,不一致の種類\n", encoding="utf-8"
        )

    c5_groups = sum(1 for _ in c5)
    counts = {
        "c1_len": len(c1_len),
        "c1_shift": len(c1_shift),
        "c2": len(c2),
        "c3_dh": len(c3_dh),
        "c3_j": sum(r[2] for r in c3_ift),
        "c3_addr": len(c3_addr),
        "c3_fin": len(c3_fin),
        "c3_sh": len(c3_sh),
        "c5_groups": c5_groups,
        "nda": nda_total,
        "inv": inv_n,
    }
    write_readme(out_dir, counts, db_status, csv_only, max_rows)

    print(f"レポート出力先: {out_dir}")
    for k, v in counts.items():
        print(f"  {k}: {v}")
    print(f"  DB突合: {db_status}")
    print("  詳細: README.md / 14_code_fix_recommendations.md")


if __name__ == "__main__":
    main()
