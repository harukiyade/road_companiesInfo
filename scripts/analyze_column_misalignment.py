#!/usr/bin/env python3
"""
CSV「動的カラムズレ」解析スクリプト
- ヘッダー列数とデータ行の列数差分を集計
- パターン（郵便番号・法人番号・URL等）で各列の期待値を推定
- ズレの法則をレポート出力
"""
import csv
import re
from pathlib import Path
from collections import defaultdict

TARGETS = [
    "csv_2/csv/107.csv", "csv_2/csv/109.csv", "csv_2/csv/110.csv",
    "csv_2/csv/111.csv", "csv_2/csv/112.csv", "csv_2/csv/113.csv",
    "csv_2/csv/114.csv", "csv_2/csv/115.csv", "csv_2/csv/116.csv",
    "csv_2/csv/117.csv", "csv_2/csv/122.csv",
    "csv_2/fixed/107.fixed.csv", "csv_2/fixed/109.fixed.csv",
    "csv_2/fixed/110.fixed.csv", "csv_2/fixed/111.fixed.csv",
    "csv_2/fixed/112.fixed.csv", "csv_2/fixed/113.fixed.csv",
    "csv_2/fixed/114.fixed.csv", "csv_2/fixed/115.fixed.csv",
    "csv_2/fixed/116.fixed.csv", "csv_2/fixed/117.fixed.csv",
    "csv_2/fixed/122.fixed.csv",
    "csv_2/import_firstTime/105.csv", "csv_2/import_firstTime/106.csv",
    "csv_2/import_firstTime/107.csv", "csv_2/import_firstTime/110.csv",
    "csv_2/import_firstTime/111.csv", "csv_2/import_firstTime/112.csv",
    "csv_2/import_firstTime/113.csv", "csv_2/import_firstTime/114.csv",
    "csv_2/import_firstTime/115.csv", "csv_2/import_firstTime/116.csv",
    "csv_2/import_firstTime/117.csv", "csv_2/import_firstTime/119.csv",
    "csv_2/import_firstTime/122.csv",
]

# 判定用パターン
P_POSTAL = re.compile(r'^\d{3}-?\d{4}$')
P_CORP = re.compile(r'^\d{13}$')
P_URL = re.compile(r'^https?://')
PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県"
)

def is_dummy_corp(v):
    return bool(v and P_CORP.match(str(v).strip()) and str(v).startswith("918"))

def guess_type(val):
    if not val or not str(val).strip():
        return "空"
    v = str(val).strip()
    if P_POSTAL.match(v):
        return "郵便番号"
    if P_CORP.match(v) and not v.startswith("918"):
        return "法人番号"
    if is_dummy_corp(v):
        return "法人番号(ダミー)"
    if P_URL.match(v):
        return "URL"
    if v in ("非上場", "上場", "プライム", "スタンダード", "グロース"):
        return "上場"
    if v in ("未設定", "未送信", "未締結", "未契約") or "NDA" in v or "AD" in v:
        return "フラグ"
    for p in PREFECTURES:
        if v.startswith(p):
            return "住所または都道府県"
    if re.match(r'^\d{5,}$', v):
        return "数値(日付シリアル?)"
    if re.match(r'^\d{4}年\d{1,2}月\d{1,2}日', v) or re.match(r'^\d{4}-\d{2}-\d{2}', v):
        return "日付"
    if re.match(r'^0\d{1,4}-\d{1,4}-\d{4}$', v):
        return "電話番号"
    return "その他"

def load_csv(path):
    base = Path(__file__).parent.parent
    fp = base / path
    for enc in ['utf-8', 'utf-8-sig', 'cp932']:
        try:
            with open(fp, 'r', encoding=enc, errors='replace') as f:
                rows = list(csv.reader(f))
                return rows, enc
        except Exception:
            continue
    return None, None

def analyze_file(path):
    rows, enc = load_csv(path)
    if not rows:
        return None
    header = rows[0]
    hlen = len(header)
    diff_counts = defaultdict(int)  # diff -> count
    sample_rows = []  # (diff, row_idx, row_len, row_preview)
    for i, row in enumerate(rows[1:], 2):
        diff = len(row) - hlen
        diff_counts[diff] += 1
        if diff != 0 and len(sample_rows) < 3:
            sample_rows.append((diff, i, len(row), row[:15]))
    return {
        "path": path,
        "header": header,
        "hlen": hlen,
        "total_rows": len(rows) - 1,
        "diff_counts": dict(diff_counts),
        "sample_rows": sample_rows,
        "encoding": enc,
    }

def main():
    print("=" * 80)
    print("CSV動的カラムズレ 解析レポート")
    print("=" * 80)

    for path in TARGETS:
        r = analyze_file(path)
        if not r:
            print(f"\n[SKIP] {path}: 読取失敗")
            continue
        print(f"\n--- {path} ---")
        print(f"ヘッダー列数: {r['hlen']}")
        print(f"データ行数: {r['total_rows']}")
        print(f"列数差分の内訳: {r['diff_counts']}")

        if r['sample_rows']:
            print("サンプル（ズレあり行）:")
            for diff, row_idx, rlen, preview in r['sample_rows']:
                print(f"  行{row_idx}: 列数={rlen} (差{diff:+d}), 先頭15列={preview[:5]}...")
                # ヘッダーとの対応を推定
                if r['hlen'] <= 15:
                    print(f"    ヘッダー: {r['header']}")
                else:
                    print(f"    ヘッダー先頭10: {r['header'][:10]}")

        # ヘッダー内容（Unnamed除去）
        main_headers = [h for h in r['header'] if h and not str(h).startswith('Unnamed')]
        print(f"実質ヘッダー列数: {len(main_headers)}")
        print(f"ヘッダー一覧: {main_headers}")

    print("\n" + "=" * 80)
    print("ズレ法則サマリ")
    print("=" * 80)
    for path in TARGETS:
        r = analyze_file(path)
        if r and r['diff_counts']:
            diffs = [d for d in r['diff_counts'] if d != 0]
            if diffs:
                print(f"{path}: 差分 {diffs}")

if __name__ == '__main__':
    main()
