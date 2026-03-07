#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/import_representatives_safe.py

fixed_csv_3配下（later除く）のCSVから、代表者情報をDBにインポートする。
★重要: DB側のカラムが NULL または空文字の場合のみ更新する（上書き禁止）。
"""

import os
import re
import logging
from pathlib import Path

import pandas as pd
import psycopg2

# --- 設定 ---
DB_HOST = os.getenv("POSTGRES_HOST", "34.84.189.233")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")
DB_NAME = os.getenv("POSTGRES_DB", "postgres")
DB_SSLMODE = os.getenv("POSTGRES_SSLMODE", "require")

ROOT_DIR_NAME = "fixed_csv_3"
EXCLUDE_PATH_FRAGMENT = "/later/"
MOJIBAKE_CHAR = "\ufffd"

# マッピング定義 {DBカラム名: [CSVヘッダー候補リスト]}
# 候補リストの先頭から順に探し、最初に見つかった列の値を使用する
MAPPING = {
    "representative_name": ["代表者", "代表者名", "代表取締役", "社長", "氏名"],
    "representative_home_address": ["代表者住所", "住所"],
    "representative_birth_date": ["代表者誕生日", "生年月日"],
    "representative_postal_code": ["代表者郵便番号", "郵便番号"],
    "representative_kana": ["代表者カナ", "代表者名カナ", "フリガナ"],
    "representative_title": ["役職", "肩書"]
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("import_rep_safe")

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

def get_encoding(file_path):
    for enc in ["utf-8-sig", "cp932", "utf-8"]:
        try:
            with open(file_path, "r", encoding=enc) as f:
                f.readline()
            return enc
        except Exception:
            continue
    return "utf-8"

def normalize_val(val):
    if val is None:
        return None
    s = str(val).strip()
    return s if s and str(s).lower() not in ("nan", "none", "null") else None

def normalize_corp_num(val):
    if val is None: return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none", "null"): return None
    if "e" in s.lower():
        try:
            s = f"{float(s):.0f}"
        except: return None
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
    return MOJIBAKE_CHAR in str(val) if val else False

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

    # 129.csv 特例 (列位置)
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
    logger.info(f"キャッシュ完了: {len(cache_corp)}件 (法人番号), {len(cache_name_pref)}件 (名前+都道府県)")

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
        return []

    all_files = []
    for fp in root.rglob("*.csv"):
        path_str = str(fp.resolve()).replace("\\", "/")
        if EXCLUDE_PATH_FRAGMENT in path_str:
            continue
        all_files.append(fp)
    return sorted(all_files)

def process_file(conn, file_path, stats):
    enc = get_encoding(file_path)
    header_opt = None if "129.csv" == file_path.name else 0
    
    try:
        df = pd.read_csv(file_path, encoding=enc, dtype=str, low_memory=False, header=header_opt)
    except Exception as e:
        return

    if df.empty or len(df.columns) < 2: return
    headers = [str(c) for c in df.columns]

    batch_updates = []

    for _, row in df.iterrows():
        row_dict = {str(h): row.get(h) for h in headers}
        
        # 更新データの抽出
        update_data = {}
        has_data = False
        
        for db_col, csv_candidates in MAPPING.items():
            val = None
            for cand in csv_candidates:
                # 完全一致または部分一致で列を探す
                target_keys = [h for h in headers if cand == h] # 厳密一致優先
                if not target_keys:
                    # 部分一致 (例: "代表者" -> "代表者名")
                    target_keys = [h for h in headers if cand in h]
                
                if target_keys:
                    # 最初に見つかった列を採用
                    raw = row_dict.get(target_keys[0])
                    val = normalize_val(raw)
                    if val: break
            
            if val:
                if has_mojibake(val): continue
                update_data[db_col] = val
                has_data = True
        
        if not has_data: continue

        corporate_number, name, prefecture = get_company_identifiers(file_path, headers, row_dict)
        company_id = resolve_company_id(corporate_number, name, prefecture)
        
        if not company_id: continue
        batch_updates.append((company_id, update_data))

    if not batch_updates: return

    # DB更新 (NULLの場合のみ)
    cur = conn.cursor()
    updated_count = 0
    
    try:
        for cid, data in batch_updates:
            # 更新クエリの構築
            # "UPDATE companies SET col = val WHERE id = cid AND (col IS NULL OR col = '')"
            
            for col, val in data.items():
                sql = f"""
                    UPDATE companies 
                    SET {col} = %s, updated_at = NOW()
                    WHERE id = %s 
                      AND ({col} IS NULL OR {col} = '')
                """
                cur.execute(sql, (val, cid))
                if cur.rowcount > 0:
                    updated_count += 1
        
        conn.commit()
        stats["updated_fields"] += updated_count
        if updated_count > 0:
            logger.info(f"  -> {file_path.name}: {updated_count} 項目を新規登録")
            
    finally:
        cur.close()

def main():
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    csv_files = collect_csv_files(project_root)
    logger.info(f"=== 代表者情報の安全インポート開始 ===")
    logger.info(f"対象ファイル数: {len(csv_files)}")

    if not csv_files:
        return

    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        dbname=DB_NAME,
        sslmode=DB_SSLMODE,
    )

    stats = {"updated_fields": 0}
    try:
        load_id_caches(conn)
        for i, fp in enumerate(csv_files, 1):
            if i % 10 == 0:
                logger.info(f"進捗: {i}/{len(csv_files)}...")
            process_file(conn, fp, stats)
            
    except Exception as e:
        logger.error(f"エラー: {e}")
    finally:
        conn.close()

    logger.info("=== 処理完了 ===")
    logger.info(f"総更新(新規登録)項目数: {stats['updated_fields']}")

if __name__ == "__main__":
    main()