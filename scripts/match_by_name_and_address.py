import csv
import re
import unicodedata
import os
from typing import Optional
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from difflib import SequenceMatcher

# ==========================================
# 設定（環境変数から取得）
# ==========================================
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
DB_CONNECTION_STRING = f'postgresql+psycopg2://{DB_USER}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# 国税庁のマスターデータ
FILE_MASTER = 'csv/00_zenkoku_all_20251226.csv'

# バッチサイズ
BATCH_SIZE = 1000

# 住所類似度の閾値（複数候補時のみ適用）。未設定なら 0.3（ログのコメントに合わせる）
MIN_ADDRESS_SIMILARITY = float(os.getenv("MIN_ADDRESS_SIMILARITY", "0.3"))
# 「続きから」用: URL/住所が空欄の企業だけを対象にする（デフォルトOFF）
ONLY_EMPTY_URL_OR_ADDRESS = os.getenv("ONLY_EMPTY_URL_OR_ADDRESS", "0") in ("1", "true", "TRUE", "yes", "YES")

# ==========================================
# 正規化・類似度判定関数
# ==========================================
def normalize_text(text):
    """社名・住所の正規化（全角半角統一、スペース削除、法人格削除）"""
    if not text: return ""
    s = unicodedata.normalize('NFKC', str(text))
    s = re.sub(r'\s+', '', s)
    s = s.upper()
    # 社名用: 法人格の削除（株式会社など）
    s = re.sub(r'(株式会社|有限会社|合同会社|一般社団法人)', '', s)
    return s

def calculate_similarity(addr1, addr2):
    """住所文字列の類似度を計算 (0.0 ~ 1.0)"""
    if not addr1 or not addr2: return 0.0
    return SequenceMatcher(None, addr1, addr2).ratio()

# ==========================================
# 取得データのクリーニング（保存直前に適用）
# ==========================================
HTTP_URL_EXTRACT_REGEX = re.compile(r"https?://[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+")

def clean_url(raw: Optional[str]) -> Optional[str]:
    """URLは http(s) から始まるURL部分のみ抽出する（混入テキストを除外）"""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    m = HTTP_URL_EXTRACT_REGEX.search(s)
    return m.group(0) if m else s

def clean_address(raw: Optional[str]) -> Optional[str]:
    """住所から /地図 や Googleマップで表示 などの不要文言を除去する"""
    if raw is None:
        return None
    s = re.sub(r"\s+", " ", str(raw)).strip()
    if not s:
        return None
    # 末尾に繋がる案内文をカット
    s = re.sub(r"/地図.*$", "", s)
    s = re.sub(r"Google\s*マップで表示.*$", "", s)
    s = re.sub(r"Google\s*マップ.*$", "", s)
    # 単語として混入するものも除去
    s = s.replace("/地図", "")
    s = re.sub(r"Google\s*マップで表示", "", s)
    s = re.sub(r"Google\s*マップ", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None

# ==========================================
# メイン処理
# ==========================================
def main():
    print("--- 処理開始 ---")
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
    except OperationalError as e:
        # SQLAlchemy/psycopg2 のエラー文面で原因を分岐（誤判定しないように優先順位を付ける）
        error_msg = str(getattr(e, "orig", e))
        lower = error_msg.lower()

        # 0) DBが存在しない
        if ("does not exist" in lower) and ("database" in lower):
            print(f"❌ エラー: データベース '{DB_NAME}' が存在しません。")
            print(f"  接続先: {DB_HOST}:{DB_PORT}/{DB_NAME} (ユーザー: {DB_USER})")
            print("\n対処:")
            print("  - POSTGRES_DB を実在するDB名に変更してください（例: postgres / companies_db など）")
            print("  - Cloud SQL側のDB一覧を確認して合わせてください")
            raise

        # 1) 認証エラー（接続はできている）
        if "password authentication failed" in lower:
            print("❌ エラー: PostgreSQLの認証に失敗しました（パスワードが違う可能性が高いです）。")
            print(f"  接続先: {DB_HOST}:{DB_PORT}/{DB_NAME} (ユーザー: {DB_USER})")
            print("\n対処:")
            print("  - 正しいパスワードを設定してください（例: export POSTGRES_PASSWORD='...')  ")
            print("  - Cloud SQLのpostgresパスワードを再確認（またはリセット）してください")
            raise

        # 2) 接続拒否/到達不可（Proxy未起動など）
        is_conn_refused = ("connection refused" in lower) or ("could not connect to server" in lower) or ("is the server running on that host" in lower)
        if is_conn_refused:
            print("❌ エラー: PostgreSQLに接続できません（Connection refused など）。")
            print(f"  接続先: {DB_HOST}:{DB_PORT}/{DB_NAME}")
            if DB_HOST == "127.0.0.1":
                print("\n考えられる原因:")
                print("  - Cloud SQL Auth Proxy が起動していない")
                print("  - Proxyの待受ポートが 5432 ではない（別ポートで起動している）")
                print("  - ローカルPostgreSQLも起動していない")
                print("\n対処:")
                print("  1) 別ターミナルで Cloud SQL Auth Proxy を起動（例）:")
                print("     cloud-sql-proxy albert-ma:asia-northeast1:companies-db --address 127.0.0.1 --port 5432")
                print("     （古いProxyの場合: cloud_sql_proxy -instances=albert-ma:asia-northeast1:companies-db=tcp:5432）")
                print("  2) Proxyが別ポートなら環境変数で合わせる:")
                print("     export POSTGRES_PORT='5432'  # 例: 5433 で起動しているなら 5433")
            raise

        raise
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

    # 1. DBからターゲットデータを取得（社名と住所）
    print("DBから未特定データを取得中...")
    
    # 住所カラムは address, headquarters_address, prefecture などを結合して利用
    # 「続きから」実行（任意）: ONLY_EMPTY_URL_OR_ADDRESS=1 の場合は URL/住所が空欄の企業のみ対象
    only_empty_clause = ""
    if ONLY_EMPTY_URL_OR_ADDRESS:
        only_empty_clause = """
          AND (
            company_url IS NULL OR company_url = '' OR
            address IS NULL OR address = '' OR
            headquarters_address IS NULL OR headquarters_address = ''
          )
        """

    sql_fetch = text(f"""
        SELECT ctid::text, name, prefecture, address, headquarters_address, company_url
        FROM companies
        WHERE (corporate_number IS NULL OR corporate_number = '')
        {only_empty_clause}
        ;
    """)
    
    # 検索用辞書: キー=正規化社名, 値=リスト[ (ctid, 正規化住所文字列) ]
    target_map = {}
    db_count = 0

    with engine.connect() as conn:
        rows = conn.execute(sql_fetch).fetchall()
        db_count = len(rows)
        
        for row in rows:
            ctid = row[0]
            name_raw = row[1]
            if not name_raw: continue
            
            # 住所の結合（prefecture + address + headquarters_address）
            # NULL除外して繋げる
            # 住所は混入テキストを事前にクリーニングしてから比較精度を上げる
            pref = str(row[2]) if row[2] else ""
            addr = clean_address(row[3]) if row[3] else ""
            hq_addr = clean_address(row[4]) if row[4] else ""
            addr_parts = [p for p in [pref, addr, hq_addr] if p]
            full_addr = "".join(addr_parts)
            
            name_norm = normalize_text(name_raw)
            addr_norm = normalize_text(full_addr)
            
            if name_norm not in target_map:
                target_map[name_norm] = []
            
            # 同じ社名の企業がDB内に複数ある場合に対応
            target_map[name_norm].append({
                'ctid': ctid,
                'db_addr_norm': addr_norm,
                'original_name': name_raw,
                'prefecture': pref.strip() if pref else "",
                # 保存直前に使うクリーニング値も保持（このスクリプトが触るついでに整形する）
                'clean_company_url': clean_url(row[5]) if len(row) > 5 else None,
                'clean_address': addr if addr else None,
                'clean_headquarters_address': hq_addr if hq_addr else None,
                'candidates': [] # ここにCSVからの候補を貯める
            })

    print(f"DBターゲット件数: {db_count} 件 (ユニーク社名数: {len(target_map)})")

    # 2. CSVを走査して候補を収集
    print(f"マスターデータ照合中: {FILE_MASTER}")
    
    processed_rows = 0
    
    with open(FILE_MASTER, 'r', encoding='cp932', errors='replace') as f:
        reader = csv.reader(f)
        
        for row in reader:
            processed_rows += 1
            if len(row) < 12: continue

            # CSVデータ
            csv_corp_id = row[1]
            csv_name = row[6]
            csv_pref = row[9]
            csv_city = row[10]
            csv_street = row[11]
            csv_addr_full = csv_pref + csv_city + csv_street
            
            csv_name_norm = normalize_text(csv_name)
            
            # DBに存在する社名か？
            if csv_name_norm in target_map:
                # 該当する全てのDBレコードに対して候補を追加
                csv_addr_norm = normalize_text(csv_addr_full)
                
                for db_record in target_map[csv_name_norm]:
                    db_record['candidates'].append({
                        'corp_id': csv_corp_id,
                        'csv_addr_norm': csv_addr_norm,
                        'csv_pref': csv_pref,
                        'csv_addr_original': csv_addr_full
                    })
            
            if processed_rows % 1000000 == 0:
                print(f"CSV進捗: {processed_rows} 行...")

    # 3. 候補の中からベストマッチを選定してUPDATEリスト作成
    print("候補の絞り込み中...")
    
    update_list = []
    matched_count = 0
    ambiguous_count = 0
    skipped_ambiguous_no_addr = 0
    skipped_low_score = 0
    
    for name_key, db_records in target_map.items():
        for record in db_records:
            candidates = record['candidates']
            
            if not candidates:
                continue # 候補なし
            
            best_match = None
            
            if len(candidates) == 1:
                # 候補が1つだけなら即採用
                best_match = candidates[0]
            else:
                # 候補が複数の場合、住所類似度で勝負
                # まず都道府県で絞り込めるなら絞る（DB住所が薄いケースの救済）
                pref_resolved = False
                pref = (record.get("prefecture") or "").strip()
                if pref:
                    pref_filtered = [c for c in candidates if (c.get("csv_pref") or "").strip() == pref]
                    if len(pref_filtered) == 1:
                        best_match = pref_filtered[0]
                        pref_resolved = True
                        ambiguous_count += 1
                    elif len(pref_filtered) >= 2:
                        candidates = pref_filtered  # 住所類似度での勝負を継続

                # 都道府県で一意に絞れた場合は、DB住所が空でも採用してよい
                if not pref_resolved:
                    # DB住所が空だと誤マッチしやすいのでスキップ（都道府県で一意にならない場合）
                    if not record.get('db_addr_norm'):
                        ambiguous_count += 1
                        skipped_ambiguous_no_addr += 1
                        continue

                best_score = -1.0
                
                for cand in candidates:
                    score = calculate_similarity(record['db_addr_norm'], cand['csv_addr_norm'])
                    if score > best_score:
                        best_score = score
                        best_match = cand
                
                ambiguous_count += 1
                # 閾値未満は誤マッチ防止のためスキップ（必要なら MIN_ADDRESS_SIMILARITY=0 で無効化）
                if best_match and best_score < MIN_ADDRESS_SIMILARITY:
                    skipped_low_score += 1
                    best_match = None
            
            if best_match:
                update_list.append({
                    "corporate_number": best_match['corp_id'],
                    "ctid": record['ctid'],
                    "company_url": record.get("clean_company_url"),
                    "address": record.get("clean_address"),
                    "headquarters_address": record.get("clean_headquarters_address"),
                })
                matched_count += 1

    print(f"特定完了: {matched_count} 件 (内、複数候補からの絞り込み: {ambiguous_count} 件)")
    if ambiguous_count > 0:
        print(f"  - 複数候補でDB住所なしによりスキップ: {skipped_ambiguous_no_addr} 件")
        print(f"  - 類似度<{MIN_ADDRESS_SIMILARITY}によりスキップ: {skipped_low_score} 件")
    
    # 4. DB更新
    if update_list:
        print(f"DB更新開始: {len(update_list)} 件")
        # バッチ処理で更新
        for i in range(0, len(update_list), BATCH_SIZE):
            batch = update_list[i : i + BATCH_SIZE]
            execute_update(engine, batch)
            print(f"更新進捗: {min(i + BATCH_SIZE, len(update_list))} / {len(update_list)}")
    else:
        print("更新対象が見つかりませんでした。")

    print("--- 完了 ---")

def execute_update(engine, data_list):
    if not data_list:
        return
    
    with engine.begin() as conn:
        # 各レコードを個別に実行（ctidの型キャストを正しく処理するため）
        # 保存直前にURL/住所のクリーニング結果も反映（NULLに上書きしないよう条件付きでSETする）
        for data in data_list:
            set_parts = ["corporate_number = :corporate_number"]
            params = {
                "corporate_number": data.get("corporate_number"),
                "ctid": data.get("ctid"),
            }
            if data.get("company_url"):
                set_parts.append("company_url = :company_url")
                params["company_url"] = data.get("company_url")
            if data.get("address"):
                set_parts.append("address = :address")
                params["address"] = data.get("address")
            if data.get("headquarters_address"):
                set_parts.append("headquarters_address = :headquarters_address")
                params["headquarters_address"] = data.get("headquarters_address")

            stmt = text(f"""
                UPDATE companies
                SET {', '.join(set_parts)}
                WHERE ctid::text = :ctid
            """)
            conn.execute(stmt, params)

if __name__ == '__main__':
    main()