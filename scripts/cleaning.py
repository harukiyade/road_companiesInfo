import pandas as pd
import re
import logging
import os
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

# ==========================================
# 1. データベース接続設定
# 環境変数から取得（設定されていない場合はデフォルト値を使用）
# 
# 【接続方法の選択】
# 方法1: Cloud SQL Auth Proxyを使用（推奨）
#   - POSTGRES_HOST='127.0.0.1' に設定
#   - Cloud SQL Auth Proxyを起動: 
#     cloud-sql-proxy albert-ma:asia-northeast1:companies-db
# 
# 方法2: 直接接続（パブリックIP）
#   - POSTGRES_HOST='34.84.189.233' に設定
#   - Google Cloud Consoleでファイアウォールルールに現在のIPアドレスを追加
# ==========================================
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")  # Cloud SQL Auth Proxy使用時は127.0.0.1
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "companies-db")

# パスワードが設定されていない場合はエラー
if not DB_PASSWORD:
    print("エラー: POSTGRES_PASSWORD 環境変数が設定されていません。")
    print("例: export POSTGRES_PASSWORD='your_password'")
    exit(1)

# 接続文字列の作成（パスワードをURLエンコード）
# 特殊文字（/など）が含まれる場合に備えてエンコード
encoded_password = quote_plus(DB_PASSWORD)
DATABASE_URL = f'postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# 正解データ（gBizINFO）のCSVフォルダ/ファイルパス
# フォルダ内の全CSVを読み込むか、統合されたファイルを指定してください
GBIZ_CSV_PATH = "./gBizINFO/master_data.csv"

# ログ設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# SSL接続を有効化（Google Cloud SQLはSSL接続のみ許可）
# Cloud SQL Auth Proxy経由（127.0.0.1）の場合はSSL不要
# 直接接続（パブリックIP）の場合はSSL必須
connect_args = {}
if DB_HOST != "127.0.0.1":  # 直接接続の場合（パブリックIP経由）
    connect_args = {
        "sslmode": "require"
    }
    logger.warning("⚠️  パブリックIP経由で接続します。ファイアウォールルールを確認してください。")
else:
    logger.info("ℹ️  Cloud SQL Auth Proxy経由で接続します（127.0.0.1）。")

# エンジン作成
engine = create_engine(
    DATABASE_URL,
    connect_args=connect_args
)

# --- 判定・クリーニング用関数 ---

def clean_text_symbols(val):
    """記号 [] 「」などを削除"""
    if not val:
        return None
    val = str(val)
    # [] や「」などの記号を削除
    val = re.sub(r'[\[\]「」]', '', val)
    return val.strip()

def normalize_corporate_number(val):
    """法人番号の正規化 (13桁数値以外はNoneを返す＝削除)"""
    if not val:
        return None
    val = str(val).strip()
    # 全角数字を半角へなど、基本的な正規化
    val = val.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
    # 数値以外が含まれていないか、桁数が13か
    if val.isdigit() and len(val) == 13:
        return val
    return None

def is_phone_number(val):
    """電話番号っぽいか判定（代表者名に入っている場合の検知）"""
    if not val:
        return False
    val_str = str(val)
    # 数字とハイフン、＋などを除去して空になる、または数字の割合が高い場合
    clean = re.sub(r'[0-9\-\+\(\)\s]', '', val_str)
    # 文字列の長さに対して記号・数字の割合が多い、かつ数字が一定以上
    digits = re.sub(r'\D', '', val_str)
    return len(clean) == 0 and len(digits) >= 9

def is_address(val):
    """住所っぽいか判定（会社名に入っている場合の検知）"""
    if not val:
        return False
    val_str = str(val)
    keywords = ['都', '道', '府', '県', '市', '区', '町', '村', '丁目', '番地']
    match_count = sum(1 for k in keywords if k in val_str)
    return match_count >= 2  # 複数のキーワードが含まれる場合住所とみなす

def fix_money_value(val):
    """資本金・売上の桁数補正（少なすぎる場合は百万円単位とみなして補正）"""
    if not val:
        return None
    try:
        # 数値のみ抽出
        num_str = re.sub(r'[^\d]', '', str(val))
        if not num_str:
            return None
        num = int(num_str)
        
        # 4桁以下（9999円以下）などの場合、明らかに資本金として異常なので100万倍する
        if 0 < num < 10000:
            return num * 1_000_000
        return num
    except:
        return None

def normalize_fax(val):
    """FAX番号を数値のみにする"""
    if not val:
        return None
    # 数字のみ抽出
    nums = re.sub(r'\D', '', str(val))
    return nums if nums else None

# --- メイン処理 ---

def process_deduplication(conn):
    """【要件①】重複削除・統合処理"""
    logger.info("重複排除処理を開始します...")

    # CSVデータのロード（正解データ）
    master_data = {}
    try:
        df = pd.read_csv(GBIZ_CSV_PATH, dtype=str)
        for _, row in df.iterrows():
            # キーは (会社名, 法人番号)
            key = (row.get('name'), row.get('corporate_number'))
            if key[0] and key[1]:
                master_data[key] = row.to_dict()
        logger.info(f"正解データ(CSV)を {len(master_data)} 件読み込みました。")
    except Exception as e:
        logger.warning(f"CSV読み込みエラー: {e}。DB内の情報のみでマージします。")

    # 重複の検出
    sql_find_dupes = """
    SELECT name, corporate_number, array_agg(id) as ids
    FROM companies
    WHERE name IS NOT NULL AND corporate_number IS NOT NULL
    GROUP BY name, corporate_number
    HAVING COUNT(*) > 1
    """
    duplicates = conn.execute(text(sql_find_dupes)).fetchall()
    
    logger.info(f"{len(duplicates)} 組の重複セットが見つかりました。")

    for name, corp_num, ids in duplicates:
        # 重複レコードを全取得
        # Cloud SQL(PostgreSQL)ではANYを使う
        ids_tuple = tuple(ids)
        sql_fetch = text("SELECT * FROM companies WHERE id = ANY(:ids)")
        records = conn.execute(sql_fetch, {"ids": ids_tuple}).fetchall()
        
        if not records:
            continue

        # カラムリスト取得
        columns = records[0]._mapping.keys()
        
        # 統合データの作成（ベースは最初の1件）
        merged_row = dict(records[0]._mapping)
        ids_to_delete = [r.id for r in records[1:]] # 先頭以外は削除候補
        base_id = records[0].id

        # 1. DB内マージ（NULL補完）
        for rec in records:
            r_dict = dict(rec._mapping)
            for col in columns:
                # ベースがNoneで、他レコードに値があれば採用
                if merged_row.get(col) is None and r_dict.get(col) is not None:
                    merged_row[col] = r_dict[col]

        # 2. CSV正解データによる上書き
        csv_row = master_data.get((name, corp_num))
        if csv_row:
            for col in columns:
                if col in csv_row and pd.notna(csv_row[col]):
                    merged_row[col] = csv_row[col]

        # 3. DB更新 (UPDATE)
        update_cols = [col for col in columns if col != 'id']
        if update_cols:
            set_clause = ", ".join([f"{col} = :{col}" for col in update_cols])
            stmt = text(f"UPDATE companies SET {set_clause} WHERE id = :id")
            # パラメータにidを追加
            merged_row['id'] = base_id
            conn.execute(stmt, merged_row)

        # 4. 不要レコード削除 (DELETE)
        if ids_to_delete:
            del_stmt = text("DELETE FROM companies WHERE id = ANY(:ids)")
            conn.execute(del_stmt, {"ids": ids_to_delete})

    logger.info("重複排除処理完了。")

def process_cleaning(conn):
    """【要件②】データクリーニング"""
    logger.info("データクリーニング処理を開始します...")

    # 全レコード取得（メモリ圧迫する場合はバッチ処理推奨）
    result = conn.execute(text("SELECT * FROM companies"))
    rows = result.fetchall()
    columns = result.keys()

    for row in rows:
        rec = dict(row._mapping)
        original_rec = rec.copy()
        update_needed = False
        delete_record = False

        # 1. 会社名チェック (住所が入っている、記号など)
        if is_address(rec.get('name')):
            delete_record = True
        
        # 2. 法人番号 (13桁以外は削除＝Noneにする)
        cn = normalize_corporate_number(rec.get('corporate_number'))
        if rec.get('corporate_number') != cn:
            rec['corporate_number'] = cn
            update_needed = True
            # ※ここで「13桁でない場合は削除」が「レコード削除」を意図する場合、
            # delete_record = True に変更してください。現在はフィールドクリアとしています。

        # 3. 代表者名 (電話番号なら削除)
        if is_phone_number(rec.get('representative_name')):
            rec['representative_name'] = None
            update_needed = True

        # 4. 取引先 (銀行なら削除)
        client = str(rec.get('client', ''))
        if '銀行' in client or 'Bank' in client:
            rec['client'] = None
            update_needed = True

        # 5. タグ (全削除)
        if rec.get('tags'):
            rec['tags'] = None
            update_needed = True

        # 6. FAX (数値のみ)
        fax_clean = normalize_fax(rec.get('fax'))
        if str(rec.get('fax')) != str(fax_clean): # 型変換比較
            rec['fax'] = fax_clean
            update_needed = True

        # 7. 資本金・売上 (桁数補正)
        for money_col in ['capital', 'sales']:
            if money_col in rec:
                fixed = fix_money_value(rec[money_col])
                if rec[money_col] != fixed:
                    rec[money_col] = fixed
                    update_needed = True

        # 8. SNSフィールド整理 (X/Twitter対応含む)
        sns_cols = ['facebook', 'linkedin', 'wantedly', 'youtrust']
        # external_detail_url へ移動するロジック
        for col in sns_cols:
            val = str(rec.get(col) or '')
            if not val: continue

            val_lower = val.lower()
            # X / Twitter の場合
            if 'twitter.com' in val_lower or 'x.com' in val_lower:
                # 既に値があっても上書き、または追記等の仕様に合わせて調整
                rec['external_detail_url'] = val
                rec[col] = None
                update_needed = True
            
            # 正しいカラムに入っていない場合（例: facebookカラムにlinkedin）
            elif 'linkedin.com' in val_lower and col != 'linkedin':
                rec['linkedin'] = val
                rec[col] = None
                update_needed = True
            # facebook, wantedly, youtrust も同様にクロスチェック可能
            
        # 9. 記号 [] の削除 (全テキストフィールド対象)
        for k, v in rec.items():
            if isinstance(v, str):
                cleaned = clean_text_symbols(v)
                if v != cleaned:
                    rec[k] = cleaned
                    update_needed = True

        # DB操作実行
        if delete_record:
            conn.execute(text("DELETE FROM companies WHERE id = :id"), {"id": rec['id']})
        elif update_needed:
            # 変更点のみ更新
            set_clauses = []
            params = {"id": rec['id']}
            for k in columns:
                if k == 'id': continue
                # 値が変わったものだけUPDATE文に含める
                if rec[k] != original_rec[k]:
                    set_clauses.append(f"{k} = :{k}")
                    params[k] = rec[k]
            
            if set_clauses:
                sql = text(f"UPDATE companies SET {', '.join(set_clauses)} WHERE id = :id")
                conn.execute(sql, params)

    logger.info("データクリーニング処理完了。")

# --- 実行 ---
if __name__ == "__main__":
    logger.info(f"データベース接続: {DB_HOST}:{DB_PORT}/{DB_NAME} (ユーザー: {DB_USER})")
    
    # 接続テスト
    try:
        with engine.connect() as test_conn:
            result = test_conn.execute(text("SELECT current_database();"))
            db_name = result.scalar()
            logger.info(f"✅ データベース接続成功: {db_name}")
    except Exception as e:
        error_msg = str(e)
        if "does not exist" in error_msg:
            logger.error(f"❌ エラー: データベース '{DB_NAME}' が存在しません。")
            logger.info("\n利用可能なデータベースを確認するには:")
            logger.info("  1. 'postgres'データベースに接続して確認")
            logger.info("  2. または、Google Cloud Consoleでデータベース名を確認")
            logger.info(f"\n現在の設定: POSTGRES_DB='{DB_NAME}'")
            logger.info("別のデータベース名を試す場合:")
            logger.info("  export POSTGRES_DB='postgres'  # デフォルトデータベース")
            logger.info("  または")
            logger.info("  export POSTGRES_DB='companies-db'  # ハイフン版")
            logger.info("  export POSTGRES_DB='companies_db'  # アンダースコア版")
        else:
            logger.error(f"❌ データベース接続エラー: {e}")
            logger.info("\n接続方法を確認してください:")
            logger.info("  1. Cloud SQL Auth Proxyを使用する場合:")
            logger.info("     export POSTGRES_HOST='127.0.0.1'")
            logger.info("     cloud-sql-proxy albert-ma:asia-northeast1:companies-db")
            logger.info("  2. 直接接続する場合:")
            logger.info("     export POSTGRES_HOST='34.84.189.233'")
            logger.info("     Google Cloud Consoleでファイアウォールルールを確認")
        exit(1)
    
    # メイン処理
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            # 1. 重複排除・統合
            process_deduplication(conn)
            
            # 2. データクリーニング
            process_cleaning(conn)
            
            trans.commit()
            logger.info("全ての処理が正常に完了しました。")
        except Exception as e:
            trans.rollback()
            logger.error(f"エラーが発生したためロールバックしました: {e}")
            raise