#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3/unit_million と fixed_csv_3/unit_yen 配下の CSV を走査し、
財務に関連しそうな列（ヘッダーに特定キーワードを含む列）とサンプル値をコンソールに出力する。

キーワード: 売上, 利益, 資産, 資本, 決算, 期, 月, 年
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

PROJECT_ROOT = Path(__file__).resolve().parents[1]

FINANCIAL_KEYWORDS = ("売上", "利益", "資産", "資本", "決算", "期", "月", "年")

DEFAULT_SUBDIRS = (
    PROJECT_ROOT / "fixed_csv_3" / "unit_million",
    PROJECT_ROOT / "fixed_csv_3" / "unit_yen",
)


def detect_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "cp932", "utf-8"):
        try:
            with path.open("r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "utf-8"


def header_matches_financial(header: str) -> bool:
    h = (header or "").strip()
    if not h:
        return False
    return any(kw in h for kw in FINANCIAL_KEYWORDS)


def collect_csv_files(roots: list[Path]) -> list[Path]:
    out: list[Path] = []
    for root in roots:
        if not root.is_dir():
            continue
        for fp in sorted(root.rglob("*.csv")):
            if fp.is_file():
                out.append(fp)
    return sorted(set(out))


def collect_samples_for_columns(
    path: Path,
    col_indices: list[int],
    max_samples: int,
) -> dict[int, list[str]]:
    enc = detect_encoding(path)
    buckets: dict[int, list[str]] = {i: [] for i in col_indices}

    def all_full() -> bool:
        return all(len(buckets[i]) >= max_samples for i in col_indices)

    with path.open("r", encoding=enc, newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)  # skip header
        except StopIteration:
            return buckets

        for row in reader:
            if all_full():
                break
            for i in col_indices:
                if len(buckets[i]) >= max_samples:
                    continue
                if i >= len(row):
                    continue
                val = (row[i] or "").strip()
                if val:
                    buckets[i].append(val)
            if all_full():
                break

    return buckets


def run_survey(roots: list[Path], max_samples: int) -> None:
    files = collect_csv_files(roots)
    if not files:
        print("対象 CSV が見つかりません。ディレクトリの存在とパスを確認してください。")
        for r in roots:
            print(f"  - {r}")
        return

    rel_root = PROJECT_ROOT

    for fp in files:
        try:
            enc = detect_encoding(fp)
            with fp.open("r", encoding=enc, newline="") as f:
                reader = csv.reader(f)
                try:
                    header_row = next(reader)
                except StopIteration:
                    print(f"\n■ ファイル: {fp.relative_to(rel_root)}")
                    print("  （空ファイル）")
                    continue
        except OSError as e:
            print(f"\n■ ファイル: {fp}")
            print(f"  読み込みエラー: {e}")
            continue

        headers = [((h or "").strip()) for h in header_row]
        picked: list[tuple[int, str]] = [
            (idx, name) for idx, name in enumerate(headers) if header_matches_financial(name)
        ]

        try:
            rel = fp.relative_to(rel_root)
        except ValueError:
            rel = fp
        print(f"\n■ ファイル: {rel}")

        if not picked:
            print("  （キーワードに該当する列はありません）")
            continue

        indices = [i for i, _ in picked]
        samples = collect_samples_for_columns(fp, indices, max_samples)

        for idx, name in picked:
            vals = samples.get(idx, [])
            if not vals:
                sample_str = "（データ行が空、または該当なし）"
            else:
                quoted = ", ".join(f'"{v}"' for v in vals)
                sample_str = quoted
            print(f"- [{name}]: {sample_str}")


def main() -> None:
    p = argparse.ArgumentParser(
        description="財務関連キーワードを含む CSV 列とサンプル値を調査して表示する。",
    )
    p.add_argument(
        "roots",
        nargs="*",
        type=Path,
        default=[],
        help="走査するディレクトリ（省略時は fixed_csv_3/unit_million と unit_yen）",
    )
    p.add_argument(
        "--samples",
        type=int,
        default=5,
        metavar="N",
        help="列ごとの最大サンプル件数（既定: 5、3〜5件程度を想定）",
    )
    args = p.parse_args()
    roots = [Path(r).resolve() for r in args.roots] if args.roots else list(DEFAULT_SUBDIRS)
    n = max(1, min(args.samples, 50))
    run_survey(roots, n)


if __name__ == "__main__":
    main()
