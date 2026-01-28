#!/usr/bin/env python3
"""
Firestore → Cloud SQL (PostgreSQL) 完全データ移管スクリプト
- Firestore取得はページネーションでメモリ一定
- multiprocessing の producer/consumer で並列処理（CPUコア活用）
- psycopg2.extras.execute_values によるバルクUPDATEでDB往復を削減
- 変換ロジック（to_int/to_pg_array）は精度維持
- Cloud SQL への接続は Private IP / Unix Domain Socket を環境変数で切替
"""

import os
import sys
import re
import logging
import json
import time
import multiprocessing as mp
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Iterable, Tuple
import firebase_admin
from firebase_admin import credentials, firestore
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import connection as PGConnection

# ==========================================
# 設定
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

FIREBASE_CREDENTIALS_PATH = os.getenv('FIREBASE_CREDENTIALS_PATH', 'serviceAccountKey.json')
POSTGRES_DSN = os.getenv('POSTGRES_DSN', '').strip()
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost').strip()
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432').strip()
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres').strip()
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres').strip()
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', '').strip()
POSTGRES_SSLMODE = os.getenv('POSTGRES_SSLMODE', '').strip()

# Cloud SQL Unix Domain Socket を使う場合:
# POSTGRES_HOST=/cloudsql/<INSTANCE_CONNECTION_NAME>
# （psycopg2のhostにディレクトリを渡すとUnix socket接続になります）

# Firestoreから一度に取得する件数
PAGE_SIZE = int(os.getenv('FIRESTORE_PAGE_SIZE', '1000'))

# producer → worker の1メッセージあたりの件数
WORK_ITEM_SIZE = int(os.getenv('WORK_ITEM_SIZE', '500'))

# worker がDBへ投げるexecute_valuesの1回あたり件数（大きすぎるとメモリ/タイムアウト増）
DB_BATCH_SIZE = int(os.getenv('DB_BATCH_SIZE', '1000'))

# multiprocessing
WORKER_COUNT = int(os.getenv('WORKER_COUNT', str(max(1, (os.cpu_count() or 4) - 1))))
QUEUE_MAXSIZE = int(os.getenv('QUEUE_MAXSIZE', '20'))

# Firestoreの読み取りを少しだけ間引いて負荷を平準化したい時（秒）
PRODUCER_SLEEP_SEC = float(os.getenv('PRODUCER_SLEEP_SEC', '0'))

# ==========================================
# マッピング定義 (DBカラム名: Firestoreフィールド名候補)
# ==========================================
MAPPING_CONFIG = {
    # --- 基本情報 ---
    'name': ['name', 'companyName'],
    'name_en': ['nameEn'],
    'kana': ['kana'],
    'corporate_number': ['corporateNumber', 'cNumber'],
    'corporation_type': ['corporationType'],
    'company_url': ['companyUrl', 'url', 'website_url', 'homepage_url'],
    'contact_form_url': ['contactFormUrl'],
    'phone_number': ['phoneNumber', 'tel'],
    'contact_phone_number': ['contactPhoneNumber'],
    'fax': ['fax'],
    'email': ['email'],
    'prefecture': ['prefecture'],
    'headquarters_address': ['headquartersAddress', 'address'],
    'address': ['address', 'location'],
    'postal_code': ['postalCode', 'zip_code', 'postcode'],

    # --- 代表者情報 ---
    'representative_name': ['representativeName', 'representative'],
    'representative_kana': ['representativeKana'],
    'representative_title': ['representativeTitle'],
    'representative_phone': ['representativePhone'],
    'representative_postal_code': ['representativePostalCode', 'representative_postcode'],
    'representative_home_address': ['representativeHomeAddress'],
    'representative_birth_date': ['representativeBirthDate', 'representative_birthday'],
    'representative_registered_address': ['representativeRegisteredAddress'],

    # --- 設立・沿革 ---
    'established': ['established', 'dateOfEstablishment', 'establishment_date'],
    'date_of_establishment': ['dateOfEstablishment'],
    'founding': ['founding', 'foundingDate'],
    'founding_year': ['foundingYear', 'founding', 'established'],

    # --- 財務・規模 ---
    'capital_stock': ['capitalStock', 'capital'],
    'employee_count': ['employeeCount', 'employeeNumber', 'employees'],
    'revenue': ['revenue'],
    'latest_revenue': ['latestRevenue', 'latest_sales'],
    'latest_profit': ['latestProfit'],
    'operating_income': ['operatingIncome'],
    'total_assets': ['totalAssets'],
    'net_assets': ['netAssets'],
    'total_liabilities': ['totalLiabilities'],
    'market_segment': ['marketSegment'],

    # --- 業種・事業 ---
    'industry': ['industry'],
    'industry_large': ['industryLarge'],
    'industry_middle': ['industryMiddle'],
    'industry_small': ['industrySmall'],
    'industry_detail': ['industryDetail'],
    'summary': ['businessSummary', 'summary', 'overview'],
    'company_description': ['companyDescription', 'businessDescriptions'],
    'business_descriptions': ['businessDescriptions'],
    'business_items': ['businessItems'],
    'specialties': ['specialties'],
    'overview': ['overview', 'businessSummary'],

    # --- 配列型カラム ---
    'tags': ['tags'],
    'badges': ['badges'],
    'industries': ['industries'],
    'business_items': ['businessItems'],
    'specialties': ['specialties'],

    # --- SNS・その他 ---
    'facebook': ['facebook'],
    'linkedin': ['linkedin'],
    'wantedly': ['wantedly'],
    'youtrust': ['youtrust'],
    'average_age': ['averageAge', 'representativeAge'],
    'average_overtime_hours': ['averageOvertimeHours'],
}

# PostgreSQLでARRAY型として定義されているカラム
ARRAY_COLUMNS = ('tags', 'badges', 'industries', 'business_items', 'specialties')

# バルクUPDATE対象の固定カラム（NULLはCOALESCEで上書きしない）
UPDATE_COLUMNS: Tuple[str, ...] = tuple(sorted(set(MAPPING_CONFIG.keys()) | {'listing'}))

# ==========================================
# 変換ヘルパー関数
# ==========================================
def to_int(val: Any) -> Optional[int]:
    if val is None: return None
    if isinstance(val, int): return val
    s = str(val).strip()
    # 西暦（4桁の数字）を優先抽出
    match = re.search(r'(\d{4})', s)
    if match: return int(match.group(1))
    digits = re.sub(r'[^\d-]', '', s)
    try:
        return int(digits) if digits else None
    except: return None

def to_pg_array(val: Any) -> Optional[List[str]]:
    """PostgreSQLの配列型(TEXT[])に適合する形式に変換"""
    if val is None or val == "" or val == "[]": return None
    if isinstance(val, list):
        return [str(i) for i in val] if len(val) > 0 else None
    if isinstance(val, str):
        # 文字列として "[a, b]" のように入っている場合の処理
        if val.startswith('[') and val.endswith(']'):
            try:
                l = json.loads(val)
                return [str(i) for i in l] if isinstance(l, list) and len(l) > 0 else None
            except: pass
        return [val.strip()]
    return None

def to_str(val: Any) -> Optional[str]:
    if val is None: return None
    if isinstance(val, (list, dict)): return json.dumps(val, ensure_ascii=False)
    return str(val).strip()

def determine_listing(doc_data: Dict) -> str:
    sec = doc_data.get('securitiesCode') or doc_data.get('shokenCode')
    if sec: return '上場'
    l = doc_data.get('listing') or doc_data.get('isListed')
    if l is True or str(l).lower() in ['true', '1', '上場']:
        return '上場'
    return '非上場'

# ==========================================
# コアロジック
# ==========================================
def prepare_record(doc_id: str, doc_data: Dict) -> Dict[str, Any]:
    record = {'id': doc_id}
    for sql_col, fs_fields in MAPPING_CONFIG.items():
        val = None
        for f in fs_fields:
            if doc_data.get(f) is not None and doc_data.get(f) != "":
                val = doc_data.get(f)
                break
        
        if val is not None:
            if sql_col in ARRAY_COLUMNS:
                record[sql_col] = to_pg_array(val)
            elif sql_col in (
                'capital_stock', 'employee_count', 'founding_year',
                'revenue', 'latest_revenue', 'latest_profit', 'operating_income',
                'total_assets', 'total_liabilities', 'net_assets',
            ):
                record[sql_col] = to_int(val)
            else:
                record[sql_col] = to_str(val)

    record['listing'] = determine_listing(doc_data)
    return {k: v for k, v in record.items() if v is not None}

def _build_bulk_update_sql() -> str:
    """
    execute_values 用のバルクUPDATE。
    NULLは既存値を維持したいので COALESCE(v.col, c.col) を使う。
    """
    cols = ['id', *UPDATE_COLUMNS]
    set_clause = ", ".join([f"{c} = COALESCE(v.{c}, c.{c})" for c in UPDATE_COLUMNS])
    cols_list = ", ".join(cols)
    return (
        f"UPDATE companies AS c "
        f"SET {set_clause} "
        f"FROM (VALUES %s) AS v({cols_list}) "
        f"WHERE c.id = v.id"
    )


BULK_UPDATE_SQL = _build_bulk_update_sql()


def bulk_update_execute_values(conn: PGConnection, records: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    records: prepare_recordの戻り値のリスト（可変キー）
    戻り値: (成功件数, スキップ件数)
    """
    if not records:
        return (0, 0)

    rows: List[Tuple[Any, ...]] = []
    skipped = 0
    for r in records:
        doc_id = r.get('id')
        if not doc_id:
            skipped += 1
            continue
        row = [doc_id]
        for c in UPDATE_COLUMNS:
            row.append(r.get(c))
        rows.append(tuple(row))

    if not rows:
        return (0, skipped)

    with conn.cursor() as cur:
        # page_size は psycopg2 側でVALUESを分割する単位
        execute_values(cur, BULK_UPDATE_SQL, rows, page_size=DB_BATCH_SIZE)

    return (len(rows), skipped)

def init_firebase():
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)
    return firestore.client()

def init_postgres():
    if POSTGRES_DSN:
        return psycopg2.connect(POSTGRES_DSN)

    kwargs: Dict[str, Any] = {
        'dbname': POSTGRES_DB,
        'user': POSTGRES_USER,
        'password': POSTGRES_PASSWORD,
    }

    # host がディレクトリ（/cloudsql/...）ならUnix socket接続
    if POSTGRES_HOST:
        kwargs['host'] = POSTGRES_HOST
    if POSTGRES_PORT and not POSTGRES_HOST.startswith('/'):
        kwargs['port'] = POSTGRES_PORT
    if POSTGRES_SSLMODE:
        kwargs['sslmode'] = POSTGRES_SSLMODE

    return psycopg2.connect(**kwargs)


@dataclass(frozen=True)
class WorkItem:
    docs: List[Tuple[str, Dict[str, Any]]]  # (doc_id, doc_data)


def producer(queue: "mp.Queue", stats_queue: "mp.Queue") -> None:
    """
    Firestoreからページネーションで取得してqueueに投入。
    取得は単一プロセスにして、変換/DB書込みを並列化する。
    """
    db = init_firebase()
    coll_ref = db.collection('companies_new')
    query = coll_ref.order_by("__name__").limit(PAGE_SIZE)

    last_doc = None
    buffered: List[Tuple[str, Dict[str, Any]]] = []
    produced = 0

    while True:
        current_query = query.start_after(last_doc) if last_doc else query
        docs = list(current_query.stream())
        if not docs:
            break

        for doc in docs:
            buffered.append((doc.id, doc.to_dict()))
            last_doc = doc

            if len(buffered) >= WORK_ITEM_SIZE:
                queue.put(WorkItem(docs=buffered))
                produced += len(buffered)
                buffered = []
                stats_queue.put(('produced', produced))

        if PRODUCER_SLEEP_SEC > 0:
            time.sleep(PRODUCER_SLEEP_SEC)

    if buffered:
        queue.put(WorkItem(docs=buffered))
        produced += len(buffered)
        stats_queue.put(('produced', produced))

    # sentinel
    for _ in range(WORKER_COUNT):
        queue.put(None)


def worker(worker_id: int, queue: "mp.Queue", stats_queue: "mp.Queue") -> None:
    """
    WorkItemを受け取り、prepare_record→execute_valuesでバルクUPDATE。
    """
    conn = init_postgres()
    conn.autocommit = False

    processed = 0
    skipped = 0

    try:
        while True:
            item = queue.get()
            if item is None:
                queue.task_done()
                break

            assert isinstance(item, WorkItem)

            batch_records: List[Dict[str, Any]] = []
            for doc_id, doc_data in item.docs:
                try:
                    batch_records.append(prepare_record(doc_id, doc_data))
                except Exception as e:
                    skipped += 1
                    logger.warning(f"[worker {worker_id}] 変換スキップ (ID: {doc_id}): {e}")

            try:
                ok, sk = bulk_update_execute_values(conn, batch_records)
                conn.commit()
                processed += ok
                skipped += sk
            except Exception as e:
                conn.rollback()
                skipped += len(batch_records)
                logger.warning(f"[worker {worker_id}] DB更新スキップ ({len(batch_records)}件): {e}")

            stats_queue.put(('progress', worker_id, processed, skipped))
            queue.task_done()
    finally:
        try:
            conn.close()
        except Exception:
            pass

def main():
    logger.info("--- Firestore -> Cloud SQL 並列同期開始 (multiprocessing + execute_values) ---")
    if not POSTGRES_PASSWORD and not POSTGRES_DSN:
        logger.error("POSTGRES_PASSWORD または POSTGRES_DSN が未設定です。")
        sys.exit(1)

    logger.info(
        f"設定: WORKER_COUNT={WORKER_COUNT}, FIRESTORE_PAGE_SIZE={PAGE_SIZE}, "
        f"WORK_ITEM_SIZE={WORK_ITEM_SIZE}, DB_BATCH_SIZE={DB_BATCH_SIZE}"
    )

    try:
        mp.set_start_method('spawn', force=True)
        work_queue: "mp.JoinableQueue" = mp.JoinableQueue(maxsize=QUEUE_MAXSIZE)
        stats_queue: "mp.Queue" = mp.Queue()

        # workers
        workers: List[mp.Process] = []
        for i in range(WORKER_COUNT):
            p = mp.Process(target=worker, args=(i, work_queue, stats_queue), daemon=True)
            p.start()
            workers.append(p)

        # producer
        prod = mp.Process(target=producer, args=(work_queue, stats_queue), daemon=True)
        prod.start()

        # progress aggregation
        started_at = time.time()
        produced = 0
        per_worker: Dict[int, Tuple[int, int]] = {i: (0, 0) for i in range(WORKER_COUNT)}

        while True:
            try:
                msg = stats_queue.get(timeout=1.0)
            except Exception:
                # 終了条件：producer終了 & queue空 & 全worker終了
                if not prod.is_alive() and work_queue.empty():
                    break
                continue

            if not msg:
                continue

            if msg[0] == 'produced':
                produced = int(msg[1])
            elif msg[0] == 'progress':
                _, wid, ok, sk = msg
                per_worker[int(wid)] = (int(ok), int(sk))

            ok_total = sum(v[0] for v in per_worker.values())
            sk_total = sum(v[1] for v in per_worker.values())
            elapsed = max(1e-6, time.time() - started_at)
            rate = ok_total / elapsed * 3600.0
            print(
                f"\r投入 {produced} | 成功 {ok_total} | スキップ {sk_total} | "
                f"{rate:,.0f} 件/時",
                end="",
            )

        # queueの残作業を待つ
        work_queue.join()

        # 終了待ち
        prod.join(timeout=10)
        for p in workers:
            p.join(timeout=10)

        ok_total = sum(v[0] for v in per_worker.values())
        sk_total = sum(v[1] for v in per_worker.values())
        elapsed = max(1e-6, time.time() - started_at)
        rate = ok_total / elapsed * 3600.0
        print(f"\n全処理完了。成功 {ok_total} 件、スキップ {sk_total} 件。平均 {rate:,.0f} 件/時")
    except KeyboardInterrupt:
        logger.info("ユーザーにより中断しました。プロセスを終了します。")
        try:
            # 強制終了（中途半端なトランザクションは各workerでrollback済み）
            for p in mp.active_children():
                p.terminate()
        finally:
            sys.exit(130)
    except Exception as e:
        logger.error(f"致命的エラー: {e}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()