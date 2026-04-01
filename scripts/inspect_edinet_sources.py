#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
CSV_PATHS = [
    ROOT / "data" / "EdinetcodeDlInfo.csv",
    ROOT / "edinet" / "EdinetcodeDlInfo 2.csv",
]
ZIP_DIR = ROOT / "edinet_data"
TARGET_COLUMNS = ["法人番号", "上場区分", "提出者種別"]
DATE_PATTERNS = (
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),
    re.compile(r"^\d{4}/\d{2}/\d{2}$"),
    re.compile(r"^\d{8}$"),
)
RELATION_KEYWORDS = ("関係会社", "子会社", "親会社", "related party", "subsidiary", "affiliate")


@dataclass
class CsvInfo:
    path: Path
    encoding: str
    header: list[str]
    sample_rows: list[list[str]]
    row_count_without_header: int
    date_column_candidates: list[str]
    latest_date_guess: str | None
    present_target_columns: dict[str, bool]


def open_text_guess(path: Path):
    for enc in ("utf-8-sig", "cp932", "utf-8", "shift_jis"):
        try:
            f = path.open("r", encoding=enc, newline="")
            f.read(2048)
            f.seek(0)
            return f, enc
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"Unsupported encoding: {path}")


def normalize_date_for_sort(value: str) -> str | None:
    value = value.strip()
    if not value:
        return None
    if DATE_PATTERNS[0].match(value):
        return value
    if DATE_PATTERNS[1].match(value):
        return value.replace("/", "-")
    if DATE_PATTERNS[2].match(value):
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    return None


def collect_csv_info(path: Path) -> CsvInfo:
    f, enc = open_text_guess(path)
    try:
        reader = csv.reader(f)
        rows = list(reader)
    finally:
        f.close()
    header = rows[0] if rows else []
    body = rows[1:] if len(rows) >= 2 else []
    sample_rows = rows[:5]
    present_target_columns = {c: c in header for c in TARGET_COLUMNS}

    # Date candidate columns are selected by column names first.
    named_candidates = [h for h in header if any(k in h for k in ("日", "date", "Date", "更新", "提出"))]
    # Fallback: detect columns with date-like values in first 200 rows.
    detected_candidates = []
    if header:
        check_rows = body[:200]
        for idx, name in enumerate(header):
            values = [r[idx] for r in check_rows if len(r) > idx]
            valid = [normalize_date_for_sort(v) for v in values]
            valid_ratio = sum(v is not None for v in valid) / len(values) if values else 0.0
            if valid_ratio >= 0.8:
                detected_candidates.append(name)
    date_column_candidates = list(dict.fromkeys(named_candidates + detected_candidates))

    latest_date_guess = None
    if date_column_candidates and header and body:
        idx = header.index(date_column_candidates[0])
        normalized_dates = []
        for r in body:
            if len(r) <= idx:
                continue
            nd = normalize_date_for_sort(r[idx])
            if nd:
                normalized_dates.append(nd)
        if normalized_dates:
            latest_date_guess = max(normalized_dates)

    return CsvInfo(
        path=path,
        encoding=enc,
        header=header,
        sample_rows=sample_rows,
        row_count_without_header=max(len(rows) - 1, 0),
        date_column_candidates=date_column_candidates,
        latest_date_guess=latest_date_guess,
        present_target_columns=present_target_columns,
    )


def choose_zip_samples(zip_paths: Iterable[Path], count: int = 2) -> list[Path]:
    sorted_paths = sorted(zip_paths, key=lambda p: p.stat().st_mtime, reverse=True)
    return sorted_paths[:count]


def has_relation_keyword_text(data: str) -> bool:
    lowered = data.lower()
    return any(k.lower() in lowered for k in RELATION_KEYWORDS)


def inspect_zip(path: Path) -> dict:
    result = {
        "path": path,
        "members": [],
        "csv_headers": [],
        "csv_relation_hint": False,
        "text_relation_hint_from_xbrl": False,
    }
    with zipfile.ZipFile(path) as zf:
        members = zf.namelist()
        result["members"] = members
        csv_members = [m for m in members if m.lower().endswith(".csv")]
        for m in csv_members:
            with zf.open(m, "r") as raw:
                content = raw.read(100_000)
            header_line = None
            for enc in ("utf-8-sig", "cp932", "utf-8", "shift_jis"):
                try:
                    txt = content.decode(enc, errors="ignore")
                    header_line = txt.splitlines()[0] if txt.splitlines() else ""
                    if has_relation_keyword_text(txt):
                        result["csv_relation_hint"] = True
                    break
                except UnicodeDecodeError:
                    continue
            result["csv_headers"].append({"member": m, "header": header_line or ""})

        # CSVがない場合でも、XBRL/HTMLの先頭を軽く確認して関係会社キーワード有無だけ参考情報として出す
        if not csv_members:
            for m in members:
                if not m.lower().endswith((".xbrl", ".xml", ".htm", ".html", ".xsd")):
                    continue
                with zf.open(m, "r") as raw:
                    sample = raw.read(500_000)
                for enc in ("utf-8-sig", "cp932", "utf-8", "shift_jis"):
                    text = sample.decode(enc, errors="ignore")
                    if has_relation_keyword_text(text):
                        result["text_relation_hint_from_xbrl"] = True
                        break
                if result["text_relation_hint_from_xbrl"]:
                    break
    return result


def print_csv_report(infos: list[CsvInfo]) -> None:
    print("=== CSV解析: EdinetcodeDlInfo ===")
    for info in infos:
        print(f"\n[FILE] {info.path}")
        print(f"- encoding_guess: {info.encoding}")
        print(f"- row_count_without_header: {info.row_count_without_header}")
        print(f"- header_columns: {len(info.header)}")
        print("- target_columns_presence:")
        for col, present in info.present_target_columns.items():
            print(f"  - {col}: {'YES' if present else 'NO'}")
        print(f"- date_column_candidates: {info.date_column_candidates}")
        print(f"- latest_date_guess: {info.latest_date_guess}")
        print("- first_5_rows (header含む):")
        for i, row in enumerate(info.sample_rows, start=1):
            print(f"  {i}: {row}")

    if len(infos) == 2:
        a, b = infos
        print("\n--- CSV比較結果 ---")
        print(f"- row_count_diff ({a.path.name} - {b.path.name}): {a.row_count_without_header - b.row_count_without_header}")
        print(f"- latest_date_guess_diff: {a.latest_date_guess} vs {b.latest_date_guess}")
        only_a = [c for c in a.header if c not in b.header]
        only_b = [c for c in b.header if c not in a.header]
        print(f"- header_only_in_{a.path.name}: {only_a[:10]}{' ...' if len(only_a) > 10 else ''}")
        print(f"- header_only_in_{b.path.name}: {only_b[:10]}{' ...' if len(only_b) > 10 else ''}")


def print_zip_report(results: list[dict]) -> None:
    print("\n=== ZIP解析: edinet_data ===")
    if not results:
        print("ZIPファイルが見つかりませんでした。")
        return
    for r in results:
        print(f"\n[ZIP] {r['path']}")
        print(f"- member_count: {len(r['members'])}")
        print("- members:")
        for m in r["members"]:
            print(f"  - {m}")
        if r["csv_headers"]:
            print("- csv_headers:")
            for h in r["csv_headers"]:
                print(f"  - {h['member']}: {h['header']}")
            print(f"- csv_relation_hint: {'YES' if r['csv_relation_hint'] else 'NO'}")
        else:
            print("- csv_headers: (none)")
            print(f"- xbrl_or_xml_relation_hint: {'YES' if r['text_relation_hint_from_xbrl'] else 'NO'}")


def print_judgement(infos: list[CsvInfo], zip_results: list[dict]) -> None:
    print("\n=== 判定レポート ===")
    if len(infos) >= 2:
        infos_sorted = sorted(infos, key=lambda x: x.row_count_without_header, reverse=True)
        parent_candidate = infos_sorted[0]
        print(
            "- 親会社リスト候補: "
            f"{parent_candidate.path} "
            "(理由: Edinetcode一覧として行数が多く、法人番号/上場区分/提出者種別の管理向き)"
        )
    else:
        print("- 親会社リスト候補: 判定不可 (CSV不足)")

    child_candidates = []
    for r in zip_results:
        if r["csv_headers"] and r["csv_relation_hint"]:
            child_candidates.append(str(r["path"]))
        elif (not r["csv_headers"]) and r["text_relation_hint_from_xbrl"]:
            child_candidates.append(f"{r['path']} (XBRL/XML内キーワード検出)")

    if child_candidates:
        print("- 子会社リスト含有候補:")
        for c in child_candidates:
            print(f"  - {c}")
        print("- 技術判断: 有価証券報告書由来の詳細データはZIP内(XBRL/XML/CSV)を優先確認するのが妥当。")
    else:
        print("- 子会社リスト含有候補: 今回抽出したZIPではCSVベースで明確に確認できず。")
        print("- 技術判断: 子会社一覧はEdinetcodeDlInfoではなく、有報ZIP内のXBRL本文に含まれる可能性が高い。")


def main() -> None:
    infos = []
    for p in CSV_PATHS:
        if p.exists():
            infos.append(collect_csv_info(p))
        else:
            print(f"[WARN] CSV not found: {p}")

    zips = list(ZIP_DIR.glob("*.zip"))
    selected = choose_zip_samples(zips, count=2)
    zip_results = [inspect_zip(zp) for zp in selected]

    print_csv_report(infos)
    print_zip_report(zip_results)
    print_judgement(infos, zip_results)


if __name__ == "__main__":
    main()
