import pandas as pd
import csv
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
DB_NAME = os.getenv("POSTGRES_DB", "companies-db")  # デフォルトをハイフンに変更（Google Cloud SQLの接続名に合わせる）

# パスワードが設定されていない場合はエラー
if not DB_PASSWORD:
    print("エラー: POSTGRES_PASSWORD 環境変数が設定されていません。")
    print("例: export POSTGRES_PASSWORD='your_password'")
    exit(1)

# 接続文字列の作成（パスワードをURLエンコード）
# 特殊文字（/など）が含まれる場合に備えてエンコード
encoded_password = quote_plus(DB_PASSWORD)
DB_CONNECTION_STRING = f'postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# ==========================================
# 2. ファイルパス設定
# ==========================================
FILE_OWNED = 'csv/hoyu_corporateNunber.csv'       # 手持ちのリスト
FILE_MASTER = 'csv/00_zenkoku_all_20251226.csv'   # 国税庁の全量データ

# バッチサイズ（一度にINSERTする件数）
BATCH_SIZE = 2000

def main():
    print("--- 処理開始 ---")

    # ファイルの存在確認
    if not os.path.exists(FILE_OWNED) or not os.path.exists(FILE_MASTER):
        print("エラー: CSVファイルが見つかりません。パスを確認してください。")
        return

    # 1. 保有している法人番号をメモリにロード
    print(f"保有リスト読み込み中: {FILE_OWNED}")
    try:
        # ヘッダーがある場合
        df_owned = pd.read_csv(FILE_OWNED, dtype=str)
        # カラム名が 'corporate_number' であると想定
        owned_ids = set(df_owned['corporate_number'])
    except KeyError:
        # ヘッダーがない場合やカラム名が違う場合の保険（1列目を使用）
        df_owned = pd.read_csv(FILE_OWNED, header=None, dtype=str)
        owned_ids = set(df_owned.iloc[:, 0])
    
    print(f"保有ID数: {len(owned_ids)} 件")

    # 2. DB接続（Google Cloud SQLの場合はSSL接続が必要）
    print(f"データベース接続: {DB_HOST}:{DB_PORT}/{DB_NAME} (ユーザー: {DB_USER})")
    
    # SSL接続を有効化（Google Cloud SQLはSSL接続のみ許可）
    # Cloud SQL Auth Proxy経由（127.0.0.1）の場合はSSL不要
    # 直接接続（パブリックIP）の場合はSSL必須
    connect_args = {}
    if DB_HOST != "127.0.0.1":  # 直接接続の場合（パブリックIP経由）
        connect_args = {
            "sslmode": "require"
        }
        print("⚠️  パブリックIP経由で接続します。ファイアウォールルールを確認してください。")
    else:
        print("ℹ️  Cloud SQL Auth Proxy経由で接続します（127.0.0.1）。")
    
    engine = create_engine(
        DB_CONNECTION_STRING,
        connect_args=connect_args
    )
    
    # 接続テスト
    try:
        with engine.connect() as test_conn:
            result = test_conn.execute(text("SELECT current_database();"))
            db_name = result.scalar()
            print(f"✅ データベース接続成功: {db_name}")
    except Exception as e:
        error_msg = str(e)
        if "does not exist" in error_msg:
            print(f"❌ エラー: データベース '{DB_NAME}' が存在しません。")
            print("\n利用可能なデータベースを確認するには:")
            print("  1. 'postgres'データベースに接続して確認")
            print("  2. または、Google Cloud Consoleでデータベース名を確認")
            print(f"\n現在の設定: POSTGRES_DB='{DB_NAME}'")
            print("別のデータベース名を試す場合:")
            print("  export POSTGRES_DB='postgres'  # デフォルトデータベース")
            print("  または")
            print("  export POSTGRES_DB='companies-db'  # ハイフン版")
            print("  export POSTGRES_DB='companies_db'  # アンダースコア版")
        raise
    
    # 3. マスターデータ読み込み & 差分INSERT
    print(f"マスターデータ読み込み＆DB登録開始: {FILE_MASTER}")
    
    insert_buffer = []
    processed_count = 0
    inserted_count = 0

    # 国税庁CSV (Shift_JIS) を読み込み
    with open(FILE_MASTER, 'r', encoding='cp932', errors='replace') as f:
        reader = csv.reader(f)
        
        with engine.connect() as conn:
            for row in reader:
                processed_count += 1
                
                # 国税庁データの仕様チェック（行が壊れていないか）
                if len(row) < 12:
                    continue

                corp_id = row[1] # 法人番号

                # ★ 判定: 保有リストになければ処理対象
                if corp_id not in owned_ids:
                    
                    # DBのカラム名に合わせて辞書を作成
                    # addressには市区町村と番地を結合して保存
                    city = row[10] if len(row) > 10 else ""  # 市区町村
                    street = row[11] if len(row) > 11 else ""  # 番地など
                    full_address = f"{city} {street}".strip() if city or street else ""
                    
                    data = {
                        "corporate_number": corp_id,
                        "name": row[6],       # 商号
                        "prefecture": row[9] if len(row) > 9 else "", # 都道府県
                        "address": full_address  # 市区町村 + 番地
                    }
                    insert_buffer.append(data)

                # バッファが溜まったら一括登録
                if len(insert_buffer) >= BATCH_SIZE:
                    execute_bulk_insert(conn, insert_buffer)
                    inserted_count += len(insert_buffer)
                    insert_buffer = []
                    print(f"進捗: {processed_count}行 ... {inserted_count}件 追加済")

            # 残りのデータを登録
            if insert_buffer:
                execute_bulk_insert(conn, insert_buffer)
                inserted_count += len(insert_buffer)

    print(f"--- 完了 ---")
    print(f"最終結果: {inserted_count} 件の新規企業を登録しました。")

def execute_bulk_insert(conn, data_list):
    if not data_list:
        return

    # companies テーブルへINSERT
    # corporate_numberが既に存在する場合はスキップ
    # idはcorporate_numberを使用（FirestoreのドキュメントIDの代わり）
    # バルクINSERTを効率化するため、VALUES句を複数行で構築
    values_list = []
    params = {}
    
    for idx, data in enumerate(data_list):
        values_list.append(
            f"(:corp_num_{idx}, :corp_num_{idx}, :name_{idx}, :pref_{idx}, :addr_{idx})"
        )
        params[f"corp_num_{idx}"] = data["corporate_number"]
        params[f"name_{idx}"] = data["name"]
        params[f"pref_{idx}"] = data["prefecture"]
        params[f"addr_{idx}"] = data["address"]
    
    sql = text(f"""
        INSERT INTO companies (id, corporate_number, name, prefecture, address)
        SELECT * FROM (VALUES {', '.join(values_list)}) AS v(id, corporate_number, name, prefecture, address)
        WHERE NOT EXISTS (
            SELECT 1 FROM companies WHERE companies.corporate_number = v.corporate_number
        );
    """)
    
    conn.execute(sql, params)
    conn.commit()

if __name__ == '__main__':
    main()