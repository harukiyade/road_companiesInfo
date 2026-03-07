#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/import_csv_exclude_later.py
ディレクトリ除外条件付き・複雑なマッピングによるCSV一括インポート

fixed_csv_3/ 配下を再帰探索。パスに /later/ を含むディレクトリは完全スキップ。
overview / business_descriptions をファイル別・列マッピングに従い更新。

★修正版: 途中再開機能付き & Cloud SQL Proxy (5433) 対応
"""

import json
import logging
import os
import re
import getpass
from pathlib import Path

import pandas as pd
import psycopg2

# --- 設定 (Cloud SQL Proxy 5433番ポート経由) ---
DB_HOST = "127.0.0.1"
DB_PORT = "5433"
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
# Proxy経由の場合、SSLモードは通常 disable または prefer で接続します
DB_SSLMODE = "disable"

# プロジェクトルートからの相対パス
ROOT_DIR_NAME = "fixed_csv_3"
EXCLUDE_PATH_FRAGMENT = "/later/"
MOJIBAKE_CHAR = "\ufffd"

# ★ここを設定して再開します（ファイル名の一部を指定）
START_FROM_FILE = "import_firstTime_119.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("import_resume")

# --- 共通定義 ---
OVERVIEW_HEADERS = ("説明", "企業概要", "概況", "会社情報・備考", "担当者コメント")
BUSINESS_HEADERS = ("概要", "事業内容")

PREFECTURES = (
    "北海道", "青森県", "岩手県", "宮城県", "秋田県", "山形県", "福島県",
    "茨城県", "栃木県", "群馬県", "埼玉県", "千葉県", "東京都", "神奈川県",
    "新潟県", "富山県", "石川県", "福井県", "山梨県", "長野県", "岐阜県",
    "静岡県", "愛知県", "三重県", "滋賀県", "京都府", "大阪府", "兵庫県",
    "奈良県", "和歌山県", "鳥取県", "島根県", "岡山県", "広島県", "山口県",
    "徳島県", "香川県", "愛媛県", "高知県", "福岡県", "佐賀県", "長崎県",
    "熊本県", "大分県", "宮崎県", "鹿児島県", "沖縄県",
)

cache_corp = {}
cache_name_pref = {}

# --- 補助関数群 ---
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
    if not raw: return ""
    s = str(raw).strip().strip("\ufeff").replace("\n", " ").replace("\r", " ")
    s = s.replace("　", " ").replace("（", "(").replace("）", ")")
    return re.sub(r"\s+", "", s)

def normalize_val(val):
    if val is None: return None
    s = str(val).strip()
    return s if s and str(s).lower() not in ("nan", "none", "null") else None

def normalize_corp_num(val):
    if val is None: return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"): return None
    if "e" in s.lower():
        try: s = f"{float(s):.0f}"
        except Exception: return None
    s = re.sub(r"[^\d]", "", s)
    if len(s) == 12: s = "0" + s
    return s if len(s) == 13 and s.isdigit() else None

def extract_prefecture(address):
    if not address: return None
    s = str(address).strip()
    for pref in PREFECTURES:
        if pref in s: return pref
    return None

def has_mojibake(val):
    if val is None: return False
    return MOJIBAKE_CHAR in str(val)

def detect_file_pattern(file_path, headers):
    name = file_path.name.lower()
    if "129.csv" == name: return "129"
    if "108.csv" == name: return "108"
    if "51.csv" == name: return "51"
    if "yuzuri" in name: return "yuzuri"
    return "standard"

def _extract_129_gyomu(row_dict):
    def get_col(i): return row_dict.get(i) or row_dict.get(str(i))
    for idx in (7, 6, "7", "6"):
        v = get_col(idx)
        if v is not None:
            s = str(v).strip()
            if s and (s.startswith("◆") or len(s) > 20): return normalize_val(s)
    for idx in (10, 11, 9, "10", "11", "9"):
        v = get_col(idx)
        if v and "事業内容" in str(v):
            try:
                j = json.loads(str(v))
                kv = j.get("企業サマリ", {}).get("kv", {})
                gyomu = kv.get("事業内容")
                if gyomu: return normalize_val(gyomu)
            except: pass
    return None

def get_overview_and_business_sources(file_path, headers, row_dict):
    def get_first(keys):
        for k in keys:
            v = row_dict.get(k)
            if v is not None and str(v).strip(): return normalize_val(v)
        return None
    def get_all_joined(keys, sep="\n"):
        vals = []
        for k in keys:
            v = row_dict.get(k)
            if v is not None and str(v).strip(): vals.append(normalize_val(v))
        return sep.join(vals) if vals else None

    pattern = detect_file_pattern(file_path, list(row_dict.keys()))
    overview_val = None
    business_val = None

    if pattern == "129":
        v = get_first(["事業内容"]) or _extract_129_gyomu(row_dict)
        if v: overview_val = v
    elif pattern == "51":
        business_val = get_all_joined(["事業内容", "会社情報・備考"], sep="\n")
    elif pattern == "108":
        overview_val = get_first(["説明"])
        business_val = get_first(["概要"])
    elif pattern == "yuzuri":
        parts = [get_first(["企業概要"]), get_first(["担当者コメント"])]
        overview_val = " / ".join(p for p in parts if p) if any(parts) else None
        business_val = get_first(["概要"])
    else:
        parts = [get_first([h]) for h in OVERVIEW_HEADERS]
        overview_val = " / ".join(p for p in parts if p) if any(parts) else None
        business_val = get_first(BUSINESS_HEADERS)

    return overview_val, business_val

def get_company_identifiers(file_path, headers, row_dict):
    def col(k):
        v = row_dict.get(k)
        return normalize_val(v) if v is not None else None
    
    name = None
    for k in ("会社名", "企業名", "name"):
        name = col(k)
        if name: break
            
    prefecture = col("都道府県") or col("prefecture")
    if not prefecture and name:
        addr = col("住所") or col("所在地") or col("address")
        prefecture = extract_prefecture(addr)
        
    corporate_number = None
    for k in ("法人番号", "ID", "会社ID", "corporate_number"):
        corporate_number = normalize_corp_num(row_dict.get(k))
        if corporate_number: break

    if "129.csv" == file_path.name:
        keys = sorted(row_dict.keys(), key=lambda x: int(x) if str(x).isdigit() else 999)
        if len(keys) >= 4:
            name = name or col(keys[2])
            corporate_number = corporate_number or normalize_corp_num(row_dict.get(keys[3]))

    return corporate_number, name, prefecture

def load_id_caches(conn):
    logger.info("キャッシュをロード中...")
    cur = conn.cursor()
    cur.execute("SELECT corporate_number, id FROM companies WHERE corporate_number IS NOT NULL")
    global cache_corp
    cache_corp = dict(cur.fetchall())
    cur.execute("SELECT name, prefecture, id FROM companies WHERE name IS NOT NULL AND prefecture IS NOT NULL")
    global cache_name_pref
    cache_name_pref = {(r[0], r[1]): r[2] for r in cur.fetchall()}
    cur.close()
    logger.info(f"キャッシュ完了: 法人番号 {len(cache_corp)}件, 名前+都道府県 {len(cache_name_pref)}件")

def resolve_company_id(corporate_number, name, prefecture):
    if corporate_number and corporate_number in cache_corp: return cache_corp[corporate_number]
    if name and prefecture:
        pid = cache_name_pref.get((name, prefecture))
        if pid: return pid
    return None

def collect_csv_files(project_root):
    root = project_root / ROOT_DIR_NAME
    if not root.exists():
        logger.error(f"ディレクトリが見つかりません: {root}")
        return [], 0

    all_files = []
    excluded_dirs = set()
    file_list = sorted(list(root.rglob("*.csv")))
    
    skipping = True if START_FROM_FILE else False
        
    for fp in file_list:
        path_str = str(fp.resolve()).replace("\\", "/")
        if EXCLUDE_PATH_FRAGMENT in path_str:
            excluded_dirs.add(str(fp.parent))
            continue
            
        if skipping:
            if fp.name == START_FROM_FILE:
                skipping = False
                logger.info(f"===> ここから再開します: {fp.name}")
            else:
                continue
        all_files.append(fp)

    return all_files, len(excluded_dirs)

def has_target_headers(headers, file_path):
    if "129.csv" == file_path.name: return True
    norm = {normalize_header(h) for h in headers}
    target = set(OVERVIEW_HEADERS) | set(BUSINESS_HEADERS) | {"事業内容", "説明", "概要", "企業概要", "概況", "会社情報・備考", "担当者コメント"}
    for t in target:
        for n in norm:
            if t in n or n == t: return True
    return False

def process_file(conn, file_path, stats):
    enc = get_encoding(file_path)
    header_opt = None if "129.csv" == file_path.name else 0
    try:
        df = pd.read_csv(file_path, encoding=enc, dtype=str, low_memory=False, header=header_opt)
    except Exception as e:
        logger.warning(f"読み込み失敗: {file_path.name} - {e}")
        return

    if df.empty or len(df.columns) < 2: return
    headers = [str(c) for c in df.columns]
    if not has_target_headers(headers, file_path): return

    batch_updates = []
    for _, row in df.iterrows():
        row_dict = {str(h): row.get(h) for h in headers}
        overview_val, business_val = get_overview_and_business_sources(file_path, headers, row_dict)
        if not overview_val and not business_val: continue
        if has_mojibake(overview_val) or has_mojibake(business_val): continue

        corporate_number, name, prefecture = get_company_identifiers(file_path, headers, row_dict)
        company_id = resolve_company_id(corporate_number, name, prefecture)
        if not company_id: continue
        batch_updates.append((company_id, overview_val, business_val))

    if not batch_updates: return

    cur = conn.cursor()
    updated_count = 0
    try:
        for cid, nw_ov, nw_biz in batch_updates:
            cur.execute("SELECT overview, business_descriptions FROM companies WHERE id = %s", (cid,))
            row_db = cur.fetchone()
            if not row_db: continue
            ex_ov, ex_biz = row_db
            
            final_ov = None
            final_biz = None
            
            if nw_ov:
                if ex_ov:
                    if nw_ov not in ex_ov: final_ov = f"{ex_ov} / {nw_ov}"
                else: final_ov = nw_ov
            
            if nw_biz:
                if ex_biz is None: final_biz = nw_biz
                elif len(str(nw_biz)) > len(str(ex_biz)): final_biz = nw_biz
            
            if final_ov is not None or final_biz is not None:
                sets, vals = [], []
                if final_ov is not None:
                    sets.append("overview = %s"); vals.append(final_ov)
                if final_biz is not None:
                    sets.append("business_descriptions = %s"); vals.append(final_biz)
                if sets:
                    vals.append(cid)
                    sql = f"UPDATE companies SET {', '.join(sets)}, updated_at = NOW() WHERE id = %s"
                    cur.execute(sql, vals)
                    updated_count += cur.rowcount
        conn.commit()
        stats["updated"] += updated_count
        if updated_count > 0:
            logger.info(f"  -> {file_path.name}: {updated_count} 件更新")
    finally:
        cur.close()

def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    # パスワードの取得（環境変数になければ対話入力）
    db_pass = os.getenv("POSTGRES_PASSWORD")
    if not db_pass:
        db_pass = getpass.getpass(f"Password for {DB_USER} (at 127.0.0.1:5433): ")

    csv_files, excluded_count = collect_csv_files(project_root)
    
    logger.info("=== CSVインポート処理開始（再開モード） ===")
    logger.info(f"対象ディレクトリ: {ROOT_DIR_NAME}/")
    logger.info(f"除外ディレクトリ数: {excluded_count}")
    logger.info(f"処理予定ファイル数: {len(csv_files)} (スキップ済みを除く)")
    logger.info(f"再開位置: {START_FROM_FILE}")

    if not csv_files:
        logger.warning("対象となるCSVファイルが見つかりませんでした。")
        return

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=db_pass,
            dbname=DB_NAME,
            sslmode=DB_SSLMODE,
        )
    except Exception as e:
        logger.error(f"データベース接続失敗: {e}")
        return

    stats = {"updated": 0}
    try:
        load_id_caches(conn)
        for i, fp in enumerate(csv_files, 1):
            logger.info(f"処理 [{i}/{len(csv_files)}]: {fp.parent.name}/{fp.name}")
            process_file(conn, fp, stats)
    except Exception as e:
        logger.error(f"エラーが発生しました: {e}")
    finally:
        conn.close()

    logger.info("=== 処理完了 ===")
    logger.info(f"総更新レコード数: {stats['updated']}")

if __name__ == "__main__":
    main()