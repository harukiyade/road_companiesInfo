#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
タイトル: 数値IDレコードとnpレコードの品質一貫性検証（A/B比較テスト）

PostgreSQL companies テーブルについて以下を実施する。
  - Group A (Normal): id NOT LIKE 'np%'
  - Group B (Target): id LIKE 'np%'
  - 各群を ORDER BY RANDOM() LIMIT n でサンプリングし、充填率・形式一致率・文字長を比較
  - Group B 専用の残存異常（name=13桁、代表者にハイフン、address が19xx/20xx開始）をカウント

接続: DATABASE_URL（postgresql+psycopg2:// は postgresql:// に正規化）または POSTGRES_*（.env 対応）

終了コード: 0=合格, 1=不合格
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple

try:
    import psycopg2
except ImportError as e:
    raise SystemExit("psycopg2 が必要です: pip install psycopg2-binary") from e

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# 都道府県名（先頭一致・長い名称を優先）
PREFECTURES: Tuple[str, ...] = tuple(
    sorted(
        (
            "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
            "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
            "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
            "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
            "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
            "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
            "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
        ),
        key=len,
        reverse=True,
    )
)

RE_POSTAL_OK = re.compile(r"^\d{3}-\d{4}$")
RE_DATE_ONLY_SLASH = re.compile(r"^\d{4}/\d{1,2}/\d{1,2}$")
RE_DATE_ONLY_HYPHEN = re.compile(r"^\d{4}-\d{1,2}-\d{1,2}$")
RE_ADDR_YEAR_HEAD = re.compile(r"^(19\d{2}|20\d{2})")
RE_CORP_13 = re.compile(r"^\d{13}$")

FILL_COLS = (
    "name",
    "postal_code",
    "address",
    "representative_name",
    "industry_middle",
    "overview",
    "corporate_number",
)

DIST_COLS = ("name", "address", "overview")

FMT_LABELS = {
    "postal_code": "postal_code（NNN-NNNN）",
    "corporate_number": "corporate_number（非数字除去→13桁）",
    "representative_name": "representative_name（数字なし）",
    "address": "address（県で始まる or ≥12文字、日付のみ/5未満はNG）",
}

# --- 表罫線（UTF-8 コンソール向け）---
def _hline(widths: List[int], left: str = "├", mid: str = "┼", right: str = "┤") -> str:
    segs = ["─" * (w + 2) for w in widths]
    return left + mid.join(segs) + right

def _top(widths: List[int]) -> str:
    return _hline(widths, "┌", "┬", "┐")

def _sep(widths: List[int]) -> str:
    return _hline(widths, "├", "┼", "┤")

def _bot(widths: List[int]) -> str:
    return _hline(widths, "└", "┴", "┘")

def _row(cells: List[str], widths: List[int]) -> str:
    parts = []
    for text, w in zip(cells, widths):
        # 全角文字が含まれる場合の簡易的な位置調整
        # 厳密な alignment が必要な場合は wcwidth 等を検討
        t = text
        parts.append(" " + t.ljust(w) + " ")
    return "│" + "│".join(parts) + "│"

def _banner(title: str, total_width: int = 80) -> None:
    inner = max(4, total_width - 2)
    print("╔" + "═" * inner + "╗")
    print("║ " + title.ljust(inner - 2) + " ║")
    print("╚" + "═" * inner + "╝")

def _database_url_for_psycopg2(url: str) -> str:
    u = url.strip()
    for prefix in (
        "postgresql+psycopg2://",
        "postgres+psycopg2://",
        "postgresql+psycopg://",
        "postgres+psycopg://",
    ):
        if u.startswith(prefix):
            return "postgresql://" + u[len(prefix) :]
    return u

def get_connection():
    url = os.getenv("DATABASE_URL")
    if url:
        return psycopg2.connect(_database_url_for_psycopg2(url))
    password = os.getenv("POSTGRES_PASSWORD", "")
    if not password:
        raise SystemExit("DATABASE_URL または POSTGRES_PASSWORD を設定してください。")
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=password,
        dbname=os.getenv("POSTGRES_DB", "postgres"),
    )

def _nonempty(v: Any) -> bool:
    if v is None: return False
    if isinstance(v, str): return bool(v.strip())
    return True

def _is_date_only_address(s: str) -> bool:
    t = s.strip()
    return bool(RE_DATE_ONLY_SLASH.match(t) or RE_DATE_ONLY_HYPHEN.match(t))

def _corp_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def postal_format_ok(v: Any) -> Optional[bool]:
    if not _nonempty(v): return None
    return bool(RE_POSTAL_OK.match(str(v).strip()))

def corporate_format_ok(v: Any) -> Optional[bool]:
    if not _nonempty(v): return None
    d = _corp_digits(str(v))
    # 12桁の場合は先頭0埋めを考慮
    if len(d) == 12: d = "0" + d
    return len(d) == 13 and d.isdigit()

def representative_format_ok(v: Any) -> Optional[bool]:
    if not _nonempty(v): return None
    return not re.search(r"\d", str(v))

def address_format_ok(v: Any) -> Optional[bool]:
    if not _nonempty(v): return None
    t = str(v).strip()
    if _is_date_only_address(t): return False
    if len(t) < 5: return False
    if any(t.startswith(p) for p in PREFECTURES): return True
    return len(t) >= 12

def fetch_sample(cur, where_np: bool, limit: int) -> List[dict]:
    # psycopg2 はクエリ内の % をプレースホルダと解釈するため、'np%' を文字列リテラルに
    # 書くと誤解釈される。パターンはバインド引数で渡す。
    cols = ", ".join(FILL_COLS)
    op = "LIKE" if where_np else "NOT LIKE"
    cur.execute(
        f"SELECT {cols} FROM companies WHERE id {op} %s ORDER BY RANDOM() LIMIT %s",
        ("np%", limit),
    )
    names = [d[0] for d in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]

def fill_rate(rows: Sequence[dict], col: str) -> float:
    if not rows: return 0.0
    n = sum(1 for r in rows if _nonempty(r.get(col)))
    return 100.0 * n / len(rows)

def format_rate_for_col(rows: Sequence[dict], col: str, checker) -> Tuple[Optional[float], int]:
    ok = 0
    filled = 0
    for r in rows:
        v = r.get(col)
        res = checker(v)
        if res is None: continue
        filled += 1
        if res: ok += 1
    if filled == 0: return None, 0
    return 100.0 * ok / filled, filled

def length_stats(rows: Sequence[dict], col: str) -> Tuple[float, int, int]:
    lens = [len(str(r[col]).strip()) for r in rows if _nonempty(r.get(col))]
    if not lens: return 0.0, 0, 0
    return sum(lens) / len(lens), min(lens), max(lens)

@dataclass
class TableRow:
    label: str
    a_val: float
    b_val: float
    def diff(self) -> float: return self.b_val - self.a_val
    def ok(self, threshold: float) -> bool: return abs(self.diff()) <= threshold

def main() -> None:
    ap = argparse.ArgumentParser(description="数値ID（正常）と np~ ID の品質一貫性 A/B 検証")
    ap.add_argument("--sample-size", type=int, default=2500)
    ap.add_argument("--fill-threshold", type=float, default=3.0)
    ap.add_argument("--format-threshold", type=float, default=5.0)
    ap.add_argument("--allow-b-name-corp-spill", action="store_true")
    args = ap.parse_args()
    n = max(1000, min(5000, args.sample_size))

    _banner("数値ID vs np~ ID 品質一貫性検証（A/B）")
    print()

    conn = get_connection()
    try:
        cur = conn.cursor()
        print(f"  サンプルサイズ: 各群 {n} 件")
        rows_a = fetch_sample(cur, where_np=False, limit=n)
        rows_b = fetch_sample(cur, where_np=True, limit=n)
        cur.close()
    finally:
        conn.close()

    print(f"  取得結果: Group A={len(rows_a)}件 / Group B={len(rows_b)}件")
    print()

    # --- [1] 充填率 ---
    w_fill = [20, 14, 14, 10, 6]
    print("【1】充填率（Fill Rate %）")
    print(_top(w_fill))
    print(_row(["カラム名", "正常(A)", "修復(B)", "B-A", "判定"], w_fill))
    print(_sep(w_fill))
    fill_ok = True
    for col in FILL_COLS:
        ra, rb = fill_rate(rows_a, col), fill_rate(rows_b, col)
        d = rb - ra
        ok = "OK" if abs(d) <= args.fill_threshold else "NG"
        if ok == "NG": fill_ok = False
        print(_row([col, f"{ra:.1f}", f"{rb:.1f}", f"{d:+.1f}", ok], w_fill))
    print(_bot(w_fill))

    # --- [2] 形式一致率 ---
    checks = [
        ("postal_code", postal_format_ok),
        ("corporate_number", corporate_format_ok),
        ("representative_name", representative_format_ok),
        ("address", address_format_ok),
    ]
    w_fmt = [40, 12, 12, 10, 6]
    print()
    print("【2】形式一致率（Format Validity %）— 非NULLのみ評価")
    print(_top(w_fmt))
    print(_row(["項目", "正常(A)", "修復(B)", "B-A", "判定"], w_fmt))
    print(_sep(w_fmt))
    fmt_ok = True
    for col, fn in checks:
        ra, fa = format_rate_for_col(rows_a, col, fn)
        rb, fb = format_rate_for_col(rows_b, col, fn)
        if ra is None and rb is None:
            sa, sb, sd, res = "N/A", "N/A", "—", "OK"
        elif ra is None or rb is None:
            sa = f"{ra:.1f}" if ra is not None else "N/A"
            sb = f"{rb:.1f}" if rb is not None else "N/A"
            sd, res = "—", "OK"
        else:
            d = rb - ra
            sa, sb, sd = f"{ra:.1f}", f"{rb:.1f}", f"{d:+.1f}"
            res = "OK" if abs(d) <= args.format_threshold else "NG"
        if res == "NG": fmt_ok = False
        print(_row([FMT_LABELS[col], sa, sb, sd, res], w_fmt))
    print(_bot(w_fmt))

    # --- [3] 文字長 ---
    w_len = [14, 10, 10, 8, 8]
    print()
    print("【3】文字長分布（Text Length）— 非NULLのみ")
    print(_top(w_len))
    print(_row(["カラム", "群", "平均", "最小", "最大"], w_len))
    print(_sep(w_len))
    for col in DIST_COLS:
        ma, mina, maxa = length_stats(rows_a, col)
        mb, minb, maxb = length_stats(rows_b, col)
        print(_row([col, "Normal(A)", f"{ma:.1f}", str(mina), str(maxa)], w_len))
        print(_row([col, "Target(B)", f"{mb:.1f}", str(minb), str(maxb)], w_len))
    print(_bot(w_len))

    # --- [4] Group B 残存異常 ---
    name_13 = sum(1 for r in rows_b if _nonempty(r.get("name")) and RE_CORP_13.match(str(r["name"]).strip()))
    rep_hyphen = sum(1 for r in rows_b if _nonempty(r.get("representative_name")) and "-" in str(r["representative_name"]))
    addr_year = sum(1 for r in rows_b if _nonempty(r.get("address")) and RE_ADDR_YEAR_HEAD.match(str(r["address"]).strip()))
    
    w_b = [40, 10]
    print()
    print("【4】Group B 専用 — 残存異常スキャン")
    print(_top(w_b))
    print(_row(["検査内容", "件数"], w_b))
    print(_sep(w_b))
    print(_row(["name が 13桁数字のみ（法人番号混入）", str(name_13)], w_b))
    print(_row(["representative_name に '-' 含有（郵便番号混入）", str(rep_hyphen)], w_b))
    print(_row(["address が 19xx / 20xx で開始（設立日混入）", str(addr_year)], w_b))
    print(_bot(w_b))

    # --- 総合判定 ---
    critical = name_13 > 0 and not args.allow_b_name_corp_spill
    passed = fill_ok and fmt_ok and not critical

    print()
    _banner("総合判定: " + ("合格 (exit 0)" if passed else "不合格 (exit 1)"))
    if critical: print("  × ERROR: Group B に社名=法人番号のレコードが残存しています。")
    if not fill_ok: print(f"  × 充填率の差異が閾値 ({args.fill_threshold}pt) を超えています。")
    if not fmt_ok: print(f"  × 形式一致率の差異が閾値 ({args.format_threshold}pt) を超えています。")
    if passed: print("  数値ID群との統計的な品質一貫性が確認されました。")
    print()

    sys.exit(0 if passed else 1)

if __name__ == "__main__":
    main()