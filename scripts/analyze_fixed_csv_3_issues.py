#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fixed_csv_3/ 配下の全 CSV をスキャンし、法人番号破損・列ズレ・unit_million の単位誤認疑いを
Type A / B / C に分類して Markdown + JSON を出力する。

使い方:
  python scripts/analyze_fixed_csv_3_issues.py
  python scripts/analyze_fixed_csv_3_issues.py --out-dir reports/my_scan
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

# 巨大フィールド対応
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SCAN_ROOT = PROJECT_ROOT / "fixed_csv_3"

# --- Type C: unit_million では cell * 1_000_000 が DB 円。異常とみなす閾値（円） ---
IMPLIED_CAPITAL_SUSPICIOUS = 500_000_000_000  # 5000億円超の資本金
IMPLIED_REVENUE_SUSPICIOUS = 1_000_000_000_000  # 1兆円超の売上
IMPLIED_PROFIT_SUSPICIOUS = 500_000_000_000  # 5000億円超の利益
# 「すでに円で書かれている」疑い: セル値が大きい（百万円として読むと桁が飛ぶ）
RAW_YEN_LIKE_MIN = 50_000_000  # 5000万以上の素の数値（カンマ除去後）→ 百万円換算で 5e13 円超

# --- Type A: 法人番号末尾ゼロ（丸め）---
# ユーザー記載の「末尾0000000」相当: 下位7桁以上が0（13桁正規化後）
TRAILING_ZERO_MIN = 7

RE_JP_POSTAL_OK = re.compile(r"^(\d{3}-\d{4}|\d{7}|\d{3}－\d{4})$")
RE_HAS_HYPHEN_DATEISH = re.compile(r"[-/年月]")


def normalize_header(raw: str) -> str:
    if not raw:
        return ""
    s = str(raw).strip().strip("\ufeff").replace("\n", " ").replace("\r", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)


def sniff_encoding(path: Path) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp932"):
        try:
            with open(path, "r", encoding=enc) as f:
                f.readline()
            return enc
        except (UnicodeDecodeError, UnicodeError):
            continue
    return "utf-8"


def find_col(headers: list[str], *candidates: str) -> int | None:
    nh = [normalize_header(h) for h in headers]
    for c in candidates:
        nc = normalize_header(c)
        if nc in nh:
            return nh.index(nc)
    return None


def parse_number_jp(s: str) -> float | None:
    if not s or not str(s).strip():
        return None
    t = str(s).replace(",", "").replace("，", "").replace("　", "").strip()
    if not t or t.lower() in ("nan", "none", "null", "-"):
        return None
    if re.search(r"[億万千兆円]", t):
        return None
    try:
        if "e" in t.lower():
            return float(Decimal(t))
        return float(t)
    except (ValueError, InvalidOperation):
        return None


def normalize_corp_digits(s: str) -> tuple[str | None, list[str]]:
    """戻り値: (13桁数字 or None, 検出した issue タグ)"""
    issues: list[str] = []
    if not s or not str(s).strip():
        return None, issues
    t = str(s).strip()
    if re.search(r"[eE][+-]?\d", t):
        issues.append("scientific_notation")
    try:
        if "e" in t.lower():
            t = str(int(Decimal(t)))
        elif "." in t and t.replace(".", "").isdigit():
            t = str(int(float(t)))
    except (ValueError, InvalidOperation, OverflowError):
        pass
    digits = re.sub(r"[^\d]", "", t)
    if len(digits) == 12:
        digits = "0" + digits
    if len(digits) != 13 or not digits.isdigit():
        issues.append("not_13_digits")
        return None, issues
    if digits.endswith("0" * TRAILING_ZERO_MIN):
        issues.append(f"trailing_{TRAILING_ZERO_MIN}+_zeros")
    return digits, issues


def postal_suspicious(val: str) -> str | None:
    v = (val or "").strip()
    if not v:
        return None
    if "http" in v.lower() or "@" in v:
        return "郵便番号列にURL・メール様の文字列"
    if RE_JP_POSTAL_OK.match(v):
        return None
    if v.isdigit():
        n = len(v)
        if n <= 5:
            return f"郵便番号が短すぎる純数字({n}桁)"
        if n == 6:
            return "郵便番号が6桁のみ（日本の郵便番号として不自然）"
        if n == 7:
            return None
        if n >= 8:
            return f"郵便番号列に8桁以上の数字（列ズレの可能性）"
    return None


def phone_suspicious(val: str) -> str | None:
    v = (val or "").strip()
    if not v:
        return None
    if "http" in v.lower():
        return "電話列にURL"
    digits = re.sub(r"[^\d]", "", v)
    if len(digits) < 6:
        return None
    if re.match(r"^[\d\-－‐ー−・\s()（）]+$", v) and "-" in v:
        return None
    # ハイフンなし純数字
    if v.replace("-", "").replace("－", "").isdigit():
        L = len(re.sub(r"\D", "", v))
        # 9〜11桁はハイフン無し電話であり得るため 6〜8 桁に限定（列ズレで売上等が入ったパターン）
        if 6 <= L <= 8:
            return f"電話列が{L}桁の数字のみ（売上・郵便番号誤混入の可能性）"
    return None


def founding_suspicious(val: str) -> str | None:
    v = (val or "").strip()
    if not v:
        return None
    if RE_HAS_HYPHEN_DATEISH.search(v):
        return None
    if v.isdigit():
        if len(v) in (5, 6, 7) and int(v) > 10000:
            return "設立列が5〜7桁の純数字（Excelシリアル誤認の可能性）"
    return None


def capital_text_suspicious(val: str) -> str | None:
    v = (val or "").strip()
    if not v:
        return None
    if len(v) > 25 and re.search(r"[\u3040-\u30ff\u4e00-\u9fff]", v):
        return "資本金列に長い日本語（列ズレの可能性）"
    if "http" in v.lower():
        return "資本金列にURL"
    return None


def revenue_text_suspicious(val: str) -> str | None:
    v = (val or "").strip()
    if not v:
        return None
    if "http" in v.lower() or re.search(r"[\u3040-\u9fff]{8,}", v):
        return "売上/利益列にURLまたは長いテキスト（列ズレの可能性）"
    return None


def analyze_file(path: Path, scan_root: Path) -> dict:
    rel = str(path.relative_to(scan_root)) if path.is_relative_to(scan_root) else str(path)
    is_million = "unit_million" in path.parts
    enc = sniff_encoding(path)
    type_a_rows: list[dict] = []
    type_b_rows: list[dict] = []
    type_c_rows: list[dict] = []
    MAX_SAMPLES_A = 40
    MAX_SAMPLES_B = 300
    MAX_SAMPLES_C = 200
    corp_scientific = 0
    corp_trailing_zero = 0
    corp_bad_digits = 0
    type_a_line_count = 0
    type_b_line_count = 0
    type_c_line_count = 0
    row_count = 0

    try:
        with open(path, "r", encoding=enc, errors="replace") as f:
            reader = csv.reader(f)
            headers = next(reader, None)
            if not headers:
                return {
                    "relative_path": rel,
                    "error": "empty",
                    "type_a_rows": [],
                    "type_b_rows": [],
                    "type_c_rows": [],
                }
            ic = find_col(headers, "法人番号", "法人番号(先頭)")
            ipost = find_col(headers, "郵便番号", "会社郵便番号")
            irpost = find_col(headers, "代表者郵便番号")
            iphone = find_col(headers, "電話番号(窓口)", "電話番号", "営業所電話番号")
            ifound = find_col(headers, "設立", "設立年月日", "設立年月日(西暦)")
            icap = find_col(headers, "資本金", "法人＿資本金")
            irev = find_col(headers, "直近売上", "売上高", "売上規模（百万円）", "売上高1")
            iprof = find_col(headers, "直近利益", "直近純利益", "純利益")

            for lineno, row in enumerate(reader, start=2):
                row_count += 1
                def cell(i: int | None) -> str:
                    if i is None or i >= len(row):
                        return ""
                    return row[i] if row[i] is not None else ""

                # Type A
                if ic is not None:
                    raw_c = cell(ic)
                    digits, issues = normalize_corp_digits(raw_c)
                    a_row = False
                    if "scientific_notation" in issues:
                        corp_scientific += 1
                        a_row = True
                        if len(type_a_rows) < MAX_SAMPLES_A:
                            type_a_rows.append(
                                {"line": lineno, "cause": "法人番号が科学表記", "sample": raw_c[:40]}
                            )
                    if any(x.startswith("trailing_") for x in issues):
                        corp_trailing_zero += 1
                        a_row = True
                        if len(type_a_rows) < MAX_SAMPLES_A:
                            type_a_rows.append(
                                {
                                    "line": lineno,
                                    "cause": "法人番号が丸め疑い（下位に連続0）",
                                    "sample": (digits or raw_c)[:20],
                                }
                            )
                    if "not_13_digits" in issues and raw_c.strip():
                        corp_bad_digits += 1
                        a_row = True
                    if a_row:
                        type_a_line_count += 1

                # Type B
                reasons: list[str] = []
                r = postal_suspicious(cell(ipost))
                if r:
                    reasons.append(f"会社:{r}")
                r = postal_suspicious(cell(irpost))
                if r:
                    reasons.append(f"代表者:{r}")
                r = phone_suspicious(cell(iphone))
                if r:
                    reasons.append(r)
                r = founding_suspicious(cell(ifound))
                if r:
                    reasons.append(r)
                r = capital_text_suspicious(cell(icap))
                if r:
                    reasons.append(r)
                r = revenue_text_suspicious(cell(irev))
                if r:
                    reasons.append(f"売上:{r}")
                r = revenue_text_suspicious(cell(iprof))
                if r:
                    reasons.append(f"利益:{r}")

                if reasons:
                    type_b_line_count += 1
                    if len(type_b_rows) < MAX_SAMPLES_B:
                        type_b_rows.append(
                            {
                                "line": lineno,
                                "cause": "; ".join(reasons),
                                "name_preview": cell(0)[:30] if row else "",
                            }
                        )

                # Type C (unit_million のみ)
                if is_million:
                    cap = parse_number_jp(cell(icap))
                    rev = parse_number_jp(cell(irev))
                    prof = parse_number_jp(cell(iprof))
                    for label, num in (("資本金", cap), ("直近売上", rev), ("直近利益", prof)):
                        if num is None or num < 0:
                            continue
                        implied = int(num * 1_000_000)
                        cause_parts = []
                        if label == "資本金" and implied > IMPLIED_CAPITAL_SUSPICIOUS:
                            cause_parts.append(f"換算{implied:,}円（資本金として異常に大きい）")
                        if label == "直近売上" and implied > IMPLIED_REVENUE_SUSPICIOUS:
                            cause_parts.append(f"換算{implied:,}円（売上として要確認・円単位混入の可能性）")
                        if label == "直近利益" and implied > IMPLIED_PROFIT_SUSPICIOUS:
                            cause_parts.append(f"換算{implied:,}円（利益として異常）")
                        if num >= RAW_YEN_LIKE_MIN:
                            cause_parts.append(
                                f"セル値{num:,.0f}が大きい（百万円ではなく円で入っている可能性）"
                            )
                        if cause_parts:
                            type_c_line_count += 1
                            if len(type_c_rows) < MAX_SAMPLES_C:
                                type_c_rows.append(
                                    {
                                        "line": lineno,
                                        "cause": f"{label}: " + " / ".join(cause_parts),
                                        "cell_value": num,
                                        "implied_yen": implied,
                                    }
                                )
    except Exception as e:
        return {
            "relative_path": rel,
            "error": str(e),
            "type_a_rows": type_a_rows,
            "type_b_rows": type_b_rows,
            "type_c_rows": type_c_rows,
        }

    return {
        "relative_path": rel,
        "encoding": enc,
        "is_unit_million": is_million,
        "data_rows": row_count,
        "corp_scientific_count": corp_scientific,
        "corp_trailing_zero_count": corp_trailing_zero,
        "corp_bad_digit_count": corp_bad_digits,
        "type_a_rows": type_a_rows,
        "type_a_row_total": type_a_line_count,
        "type_b_rows": type_b_rows,
        "type_b_row_total": type_b_line_count,
        "type_c_rows": type_c_rows,
        "type_c_row_total": type_c_line_count,
    }


def main():
    ap = argparse.ArgumentParser(description="fixed_csv_3 配下 CSV の問題行を Type A/B/C で分類")
    ap.add_argument("--root", type=Path, default=DEFAULT_SCAN_ROOT, help="スキャンルート")
    ap.add_argument("--out-dir", type=Path, default=None, help="出力ディレクトリ（既定: reports/fixed_csv_3_analysis_<UTC時刻>）")
    ap.add_argument("--exclude-later", action="store_true", default=True)
    ap.add_argument(
        "--no-per-file-json",
        action="store_true",
        help="analysis_report.json からファイル別の全行結果(per_file)を省略（要約のみ）",
    )
    args = ap.parse_args()

    root: Path = args.root
    if not root.is_dir():
        print(f"スキャンルートが存在しません: {root}", file=sys.stderr)
        sys.exit(1)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_dir = args.out_dir or (PROJECT_ROOT / "reports" / f"fixed_csv_3_analysis_{ts}")
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(root.rglob("*.csv"))
    if args.exclude_later:
        files = [p for p in files if "/later/" not in str(p).replace("\\", "/")]

    results: list[dict] = []
    type_a_files: list[dict] = []
    type_b_by_file: list[dict] = []
    type_c_by_file: list[dict] = []

    for fp in files:
        r = analyze_file(fp, root)
        results.append(r)
        rel = r["relative_path"]
        if r.get("error") and r["error"] != "empty":
            continue
        sci = r.get("corp_scientific_count", 0)
        tz = r.get("corp_trailing_zero_count", 0)
        bd = r.get("corp_bad_digit_count", 0)
        if sci > 0 or tz >= max(5, int(0.05 * max(1, r.get("data_rows", 1)))) or bd >= 10:
            type_a_files.append(
                {
                    "file": rel,
                    "cause": " / ".join(
                        filter(
                            None,
                            [
                                f"科学表記{sci}行" if sci else None,
                                f"丸め疑い末尾ゼロ{tz}行" if tz else None,
                                f"13桁非適合{bd}行" if bd else None,
                            ],
                        )
                    ),
                    "samples": (r.get("type_a_rows") or [])[:8],
                }
            )
        if r.get("type_b_row_total", 0) > 0:
            type_b_by_file.append(
                {
                    "file": rel,
                    "row_count": r["type_b_row_total"],
                    "rows": r.get("type_b_rows") or [],
                }
            )
        if r.get("type_c_row_total", 0) > 0:
            type_c_by_file.append(
                {
                    "file": rel,
                    "row_count": r["type_c_row_total"],
                    "rows": r.get("type_c_rows") or [],
                }
            )

    payload = {
        "generated_at_utc": ts,
        "scan_root": str(root),
        "files_scanned": len(files),
        "type_a_files": type_a_files,
        "type_b_files": type_b_by_file,
        "type_c_files": type_c_by_file,
        "legend": {
            "type_a": "法人番号の科学表記・13桁以外・下位に連続0が多い丸め疑い",
            "type_b": "郵便番号/電話/設立/資本金/売上の形式が列として不自然",
            "type_c": "unit_million で cell×1e6 が異常に大きい、またはセル値が円単位混入疑い",
        },
    }
    if not args.no_per_file_json:
        payload["per_file"] = results
    json_path = out_dir / "analysis_report.json"
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Markdown
    lines: list[str] = []
    lines.append(f"# fixed_csv_3 整合性スキャン ({ts} UTC)")
    lines.append("")
    lines.append(f"- スキャンルート: `{root}`")
    lines.append(f"- ファイル数: {len(files)}")
    lines.append("")
    lines.append("## Type A（法人番号破損・丸め疑い）")
    lines.append("")
    lines.append("| ファイル | 原因の要約 |")
    lines.append("|----------|------------|")
    for x in type_a_files:
        lines.append(f"| `{x['file']}` | {x['cause']} |")
    lines.append("")
    lines.append("## Type B（列ズレ・値混入疑い）")
    lines.append("")
    for block in type_b_by_file[:80]:
        lines.append(f"### `{block['file']}`（{block['row_count']} 行ヒット、先頭のみ表示）")
        lines.append("")
        lines.append("| 行番号 | 原因 | 会社名抜粋 |")
        lines.append("|--------|------|------------|")
        for row in block["rows"][:40]:
            lines.append(
                f"| {row['line']} | {row['cause'][:120]} | {row.get('name_preview', '')[:40]} |"
            )
        lines.append("")
    if len(type_b_by_file) > 80:
        lines.append(f"_… 他 {len(type_b_by_file) - 80} ファイルは JSON を参照_")
        lines.append("")

    lines.append("## Type C（unit_million での単位誤認・異常換算疑い）")
    lines.append("")
    lines.append(
        f"閾値（参考）: 資本金換算>{IMPLIED_CAPITAL_SUSPICIOUS:,}円 / "
        f"売上換算>{IMPLIED_REVENUE_SUSPICIOUS:,}円 / "
        f"利益換算>{IMPLIED_PROFIT_SUSPICIOUS:,}円 / "
        f"またはセル値≥{RAW_YEN_LIKE_MIN:,}（円混入疑い）"
    )
    lines.append("")
    for block in type_c_by_file[:80]:
        lines.append(f"### `{block['file']}`（{block['row_count']} 行）")
        lines.append("")
        lines.append("| 行番号 | 換算円（概算） | 原因 |")
        lines.append("|--------|----------------|------|")
        for row in block["rows"][:35]:
            iy = row.get("implied_yen", 0)
            lines.append(f"| {row['line']} | {iy:,} | {row['cause'][:100]} |")
        lines.append("")

    lines.append("## 出力ファイル")
    lines.append("")
    lines.append(f"- JSON: `{json_path.relative_to(PROJECT_ROOT)}`")
    lines.append("")

    md_path = out_dir / "analysis_report.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Wrote {md_path}")
    print(f"Wrote {json_path}")


if __name__ == "__main__":
    main()
