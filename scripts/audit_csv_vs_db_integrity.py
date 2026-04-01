#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
データ整合性検査スクリプト

fixed_csv_3/ 配下のCSV（later/ 除外）と companies テーブルを突合し、
・データ移行漏れ
・カラム値の完全一致（CSVに値があるのにDBがNULL/空）
・論理フラグ変換（NDA/AD/SB）の再検証
を実施し、不一致をレポートします。

使い方（Cloud SQL Auth Proxy を 127.0.0.1:5434 で起動した例）:
  export POSTGRES_HOST=127.0.0.1
  export POSTGRES_PORT=5434
  export POSTGRES_SSLMODE=disable
  export POSTGRES_DB=postgres   # 実インスタンスのDB名に合わせる
  export POSTGRES_PASSWORD='...'
  python scripts/audit_csv_vs_db_integrity.py --report reports/csv_db_audit.csv --quiet
"""

import csv
import os
import re
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import RealDictCursor

# 巨大CSV対応
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

# --- 設定 ---
ROOT_DIR = "fixed_csv_3"
EXCLUDE_PATH_FRAGMENT = "/later/"
DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

# CSVヘッダー → DBカラム（import_full_update_fast と同等の主要マッピング）
HEADER_MAP = {
    "法人番号": "corporate_number",
    "ID": "corporate_number",
    # Legatus系CSV: 会社IDはDBの company_id（法人番号と別）。corporate_number に載せると上書きで突合が壊れる。
    "会社ID": "company_id",
    # リストIDに相当するDB列が無い場合は比較対象外（マッピングしない）
    "会社名": "name",
    "企業名": "name",
    "商号又は名称": "name",
    "都道府県": "prefecture",
    "取引種別": "transaction_type",
    "SBフラグ": "sb_flag",
    "NDA": "nda_flag",
    "AD": "ad_flag",
    "郵便番号": "postal_code",
    "住所": "address",
    "所在地": "address",
    "電話番号": "phone_number",
    "URL": "company_url",
    "直近売上": "latest_revenue",
    "売上高": "latest_revenue",
    "直近利益": "latest_profit",
    "資本金": "capital_stock",
    "従業員数": "employee_count",
    "社員数": "employee_count",
    "代表者名": "representative_name",
    "代表者": "representative_name",
    "業種（大）": "industry_large",
    "業種（中）": "industry_middle",
    "業種（小）": "industry_small",
    "概要": "overview",
    "説明": "overview",
    "設立": "founding_year",
    "創業": "founding_year",
    "上場": "listing",
    "上場区分": "listing",
}

# 論理フラグの「元CSV列名」と期待変換ルール（仕様）
# DBカラム名 -> (CSV列の正規化名の候補, 期待値計算関数)
FLAG_SPEC = {
    "nda_flag": (
        ["nda", "nda締結", "NDA"],
        lambda raw: _expect_nda(raw),
    ),
    "ad_flag": (
        ["ad", "ad締結", "AD"],
        lambda raw: _expect_ad(raw),
    ),
    "sb_flag": (
        ["sbフラグ", "sb", "ストロングバイヤー"],
        lambda raw: _expect_sb(raw),
    ),
}


def _expect_nda(raw):
    """NDA: 元が「締結済」なら True、それ以外は False"""
    s = _norm_val(raw)
    if s is None:
        return None
    return "締結済" in s


def _expect_ad(raw):
    """AD: 元が「契約済」なら True、それ以外は False"""
    s = _norm_val(raw)
    if s is None:
        return None
    return "契約済" in s


def _expect_sb(raw):
    """SB: 元が「true」または「ストロングバイヤー」なら True、それ以外は False"""
    s = _norm_val(raw)
    if s is None:
        return None
    if s.lower() == "true":
        return True
    if "ストロングバイヤー" in s and "以外" not in s:
        return True
    return False


def _norm_val(val):
    if val is None:
        return None
    s = str(val).strip()
    return s if s.lower() not in ("", "nan", "none", "null") else None


def get_encoding(file_path):
    for enc in ["utf-8-sig", "cp932", "utf-8"]:
        try:
            with open(file_path, "r", encoding=enc) as f:
                f.readline()
                return enc
        except Exception:
            continue
    return "utf-8"


def normalize_header(raw):
    if not raw:
        return ""
    s = str(raw).strip().strip("\ufeff").replace("\n", " ").replace("\r", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)


def collect_csv_files(project_root):
    root = project_root / ROOT_DIR
    if not root.exists():
        return []
    files = []
    for fp in root.rglob("*.csv"):
        path_str = str(fp.resolve()).replace("\\", "/")
        if EXCLUDE_PATH_FRAGMENT in path_str:
            continue
        files.append(fp)
    return sorted(files)


def build_header_to_db(headers):
    """ヘッダー行から (列インデックス -> DBカラム名) を構築"""
    norm_to_idx = {normalize_header(h): i for i, h in enumerate(headers)}
    result = {}
    for csv_name, db_col in HEADER_MAP.items():
        n = normalize_header(csv_name)
        if n in norm_to_idx:
            result[norm_to_idx[n]] = db_col
    for i, h in enumerate(headers):
        if i not in result and h.strip() in HEADER_MAP:
            result[i] = HEADER_MAP[h.strip()]
    return result


def resolve_company_id(row_data, cache_corp, cache_name_pref, cache_legatus_company_id, prefectures):
    """1行分のデータから companies.id を解決（法人番号 → Legatusの会社ID → 名前+都道府県）"""
    corp = row_data.get("corporate_number")
    if corp and corp in cache_corp:
        return cache_corp[corp]
    leg_id = row_data.get("company_id")
    if leg_id and cache_legatus_company_id and leg_id in cache_legatus_company_id:
        return cache_legatus_company_id[leg_id]
    name = row_data.get("name")
    pref = row_data.get("prefecture") or _extract_prefecture(row_data.get("address"), prefectures)
    if name and pref and (name, pref) in cache_name_pref:
        return cache_name_pref[(name, pref)]
    if name and (name, pref or "") in cache_name_pref:
        return cache_name_pref.get((name, pref or ""))
    return None


def _extract_prefecture(address, prefectures):
    if not address:
        return None
    s = str(address).strip()
    for p in prefectures:
        if p in s:
            return p
    return None


PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)


def get_companies_columns(conn):
    """companies テーブルに存在するカラム名のセットを返す"""
    cur = conn.cursor()
    cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'companies'"
    )
    out = {r[0] for r in cur.fetchall()}
    cur.close()
    return out


def load_id_caches(conn):
    cur = conn.cursor()
    cur.execute("SELECT corporate_number, id FROM companies WHERE corporate_number IS NOT NULL")
    cache_corp = dict(cur.fetchall())
    cache_legatus_company_id = {}
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'companies' AND column_name = 'company_id'
        """
    )
    if cur.fetchone():
        cur.execute(
            "SELECT company_id, id FROM companies WHERE company_id IS NOT NULL AND TRIM(company_id) <> ''"
        )
        for cid, pk in cur.fetchall():
            # 先勝ち（重複company_idがある場合は最初の1件）
            cache_legatus_company_id.setdefault(str(cid).strip(), pk)
    cur.execute(
        "SELECT name, prefecture, id FROM companies WHERE name IS NOT NULL AND prefecture IS NOT NULL"
    )
    cache_name_pref = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    cur.execute(
        "SELECT name, id FROM companies WHERE name IS NOT NULL AND prefecture IS NULL"
    )
    for r in cur.fetchall():
        cache_name_pref[(r[0], "")] = r[1]
    cur.close()
    return cache_corp, cache_name_pref, cache_legatus_company_id


def _db_val_to_compare(val):
    """DB取得値を比較用に正規化"""
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip()
    return s if s.lower() not in ("", "nan", "none", "null") else None


def _csv_val_to_compare(val):
    if val is None:
        return None
    s = str(val).strip()
    return s if s.lower() not in ("", "nan", "none", "null") else None


def run_audit(conn, csv_files, report_rows, cache_corp, cache_name_pref, cache_legatus_company_id, db_columns_set):
    cur = conn.cursor(cursor_factory=RealDictCursor)
    db_columns_to_fetch = set(HEADER_MAP.values()) | {"id"}
    db_columns_to_fetch |= {"nda_flag", "ad_flag", "sb_flag"} & db_columns_set
    db_columns_to_fetch.discard("status_for_active")
    db_columns_to_fetch &= db_columns_set
    cols_list = list(db_columns_to_fetch)

    # columns_list が空だと SELECT エラーになるので防ぐ
    if not cols_list:
        cols_list = ["id"]

    for fp in csv_files:
        enc = get_encoding(fp)
        try:
            with open(fp, "r", encoding=enc, errors="replace") as f:
                reader = csv.reader(f)
                try:
                    headers = next(reader)
                except StopIteration:
                    continue # 空のファイル

                col_map = build_header_to_db(headers)
                if not col_map:
                    continue

                # フラグ用: CSV列名（正規化）→ 列インデックス
                norm_headers = {normalize_header(h): i for i, h in enumerate(headers)}
                flag_col_idx = {}
                for db_col, (aliases, _) in FLAG_SPEC.items():
                    for a in aliases:
                        if a in norm_headers:
                            flag_col_idx[db_col] = norm_headers[a]
                            break

                row_num = 0
                for row in reader:
                    row_num += 1
                    if len(row) < 2:
                        continue

                    row_data = {}
                    for idx, db_col in col_map.items():
                        if idx < len(row):
                            row_data[db_col] = _norm_val(row[idx]) if row[idx] is not None else None
                    if not row_data.get("name"):
                        continue

                    company_id = resolve_company_id(
                        row_data, cache_corp, cache_name_pref, cache_legatus_company_id, PREFECTURES
                    )

                    # 1) 移行漏れチェック
                    if not company_id:
                        report_rows.append({
                            "ファイル名": str(fp.name),
                            "ID": row_data.get("corporate_number") or row_data.get("name", ""),
                            "カラム名": "(全体)",
                            "CSVの値": "",
                            "DBの値": "",
                            "不一致の種類": "移行漏れ（DBに該当レコードなし）",
                        })
                        continue

                    cur.execute(
                        f"SELECT {', '.join(cols_list)} FROM companies WHERE id = %s",
                        (company_id,),
                    )
                    db_row = cur.fetchone()
                    if not db_row:
                        report_rows.append({
                            "ファイル名": str(fp.name),
                            "ID": company_id,
                            "カラム名": "(全体)",
                            "CSVの値": "",
                            "DBの値": "",
                            "不一致の種類": "移行漏れ（IDで取得できず）",
                        })
                        continue

                    # 2) カラム値一致チェック（CSVに値があるのにDBがNULL/空）
                    for idx, db_col in col_map.items():
                        if db_col in ("nda_flag", "ad_flag", "sb_flag"):
                            continue
                        if idx >= len(row):
                            continue
                        csv_val = _csv_val_to_compare(row[idx])
                        db_val = _db_val_to_compare(db_row.get(db_col))
                        if (csv_val is not None and csv_val != "") and (db_val is None or db_val == ""):
                            report_rows.append({
                                "ファイル名": str(fp.name),
                                "ID": company_id,
                                "カラム名": db_col,
                                "CSVの値": str(csv_val)[:200],
                                "DBの値": str(db_val) if db_val is not None else "(NULL/空)",
                                "不一致の種類": "CSVに値あり・DBがNULL/空",
                            })
                        elif csv_val != db_val and (csv_val or db_val):
                            # 値が異なる（両方に何かある場合）
                            report_rows.append({
                                "ファイル名": str(fp.name),
                                "ID": company_id,
                                "カラム名": db_col,
                                "CSVの値": str(csv_val)[:200] if csv_val else "",
                                "DBの値": str(db_val)[:200] if db_val else "",
                                "不一致の種類": "値の不一致",
                            })

                    # 3) 論理フラグ変換の再検証（DBに該当カラムがある場合のみ）
                    for db_col, (_, expect_fn) in FLAG_SPEC.items():
                        if db_col not in db_columns_set or db_col not in flag_col_idx:
                            continue
                        idx = flag_col_idx[db_col]
                        if idx >= len(row):
                            continue
                        raw_csv = row[idx]
                        expected_bool = expect_fn(raw_csv)
                        if expected_bool is None:
                            continue
                        db_bool = db_row.get(db_col)
                        if db_bool is None:
                            db_bool = False
                        if expected_bool != db_bool:
                            report_rows.append({
                                "ファイル名": str(fp.name),
                                "ID": company_id,
                                "カラム名": db_col,
                                "CSVの値": str(raw_csv)[:100] if raw_csv else "",
                                "DBの値": str(db_bool),
                                "不一致の種類": "論理フラグ変換の不一致（期待: " + str(expected_bool) + "）",
                            })
        except Exception as e:
            report_rows.append({
                "ファイル名": str(fp.name),
                "ID": "",
                "カラム名": "",
                "CSVの値": "",
                "DBの値": "",
                "不一致の種類": f"ファイル読み込みエラー: {e}",
            })
            continue

    cur.close()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="fixed_csv_3 と companies の整合性検査")
    parser.add_argument(
        "--report",
        default="",
        help="不一致レポートを出力するCSVファイルパス（未指定時はコンソールのみ）",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="サマリーのみ表示し、各行はレポートファイルのみに出力",
    )
    parser.add_argument(
        "--null-only",
        default="",
        metavar="FILE",
        help="「CSVに値あり・DBがNULL/空」の行のみをこのファイルに出力（fill_null_from_integrity_report.py の入力用）",
    )
    args = parser.parse_args()

    if not DB_PASSWORD:
        print("エラー: POSTGRES_PASSWORD 環境変数を設定してください。", file=sys.stderr)
        sys.exit(1)

    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent
    csv_files = collect_csv_files(project_root)
    print(f"対象CSV: {len(csv_files)} 件（{ROOT_DIR}/ 配下、{EXCLUDE_PATH_FRAGMENT} 除外）")

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )

    try:
        db_columns_set = get_companies_columns(conn)
        cache_corp, cache_name_pref, cache_legatus_company_id = load_id_caches(conn)
        print(
            f"DBキャッシュ: 法人番号 {len(cache_corp)} 件, 会社ID {len(cache_legatus_company_id)} 件, "
            f"名前+都道府県 {len(cache_name_pref)} 件"
        )

        report_rows = []
        run_audit(
            conn, csv_files, report_rows, cache_corp, cache_name_pref, cache_legatus_company_id, db_columns_set
        )

        if args.report:
            with open(args.report, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(
                    f,
                    fieldnames=["ファイル名", "ID", "カラム名", "CSVの値", "DBの値", "不一致の種類"],
                )
                w.writeheader()
                w.writerows(report_rows)
            print(f"レポート出力: {args.report} ({len(report_rows)} 件)")

        null_fill_kind = "CSVに値あり・DBがNULL/空"
        if args.null_only:
            null_only_rows = [r for r in report_rows if r.get("不一致の種類") == null_fill_kind]
            with open(args.null_only, "w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(
                    f,
                    fieldnames=["ファイル名", "ID", "カラム名", "CSVの値", "DBの値", "不一致の種類"],
                )
                w.writeheader()
                w.writerows(null_only_rows)
            print(f"NULL補完用出力: {args.null_only} ({len(null_only_rows)} 件)")

        if not args.quiet:
            # コンソールへの出力は見やすさのため制限
            print("--- 不一致サンプル (最初の20件) ---")
            for r in report_rows[:20]:
                cv = str(r.get("CSVの値", ""))[:30].replace("\n", " ")
                dv = str(r.get("DBの値", ""))[:30].replace("\n", " ")
                print(f"{r['ファイル名']} | ID:{r['ID']} | {r['カラム名']} | CSV: {cv} | DB: {dv} | {r['不一致の種類']}")
            if len(report_rows) > 20:
                print("... (省略されました。詳細はレポートファイルを確認してください)")

        print(f"\n不一致合計: {len(report_rows)} 件")
    finally:
        conn.close()


if __name__ == "__main__":
    main()