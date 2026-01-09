#!/usr/bin/env python3
"""
hasAnyWebPresence フィールドの高速化バックフィルスクリプト

companies_newとcompanies_indexコレクションの全ドキュメントに対して、
hasAnyWebPresenceフィールドを計算して追加します。

Web関連フィールド（companyUrl, contactFormUrl, urls, profileUrl, 
externalDetailUrl, facebook, linkedin, wantedly, youtrust）のいずれかが
有効な値を持っている場合、hasAnyWebPresenceをtrueに設定します。
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    from google.cloud.firestore import Client
    from google.cloud.firestore_v1.field_path import FieldPath
    from google.api_core import retry
    from google.api_core.exceptions import DeadlineExceeded, ServiceUnavailable, ResourceExhausted
except ImportError:
    print("❌ エラー: 必要なパッケージがインストールされていません")
    print("   以下のコマンドでインストールしてください:")
    print("   pip install firebase-admin google-cloud-firestore")
    sys.exit(1)


# ==============================
# 定数定義
# ==============================

# Web関連フィールドのリスト
WEB_PRESENCE_FIELDS = [
    "companyUrl",
    "contactFormUrl",
    "urls",
    "profileUrl",
    "externalDetailUrl",
    "facebook",
    "linkedin",
    "wantedly",
    "youtrust",
]

# Firestoreのバッチ書き込み制限（500件）
MAX_BATCH_SIZE = 500

# デフォルト設定
DEFAULT_BATCH_SIZE = 500
DEFAULT_CONCURRENCY = 5

# リトライ設定
MAX_RETRIES = 3
RETRY_DELAY = 1.0  # 秒


# ==============================
# ヘルパー関数
# ==============================

def has_value(value) -> bool:
    """
    値が有効かどうかを判定します。
    null, undefined, 空文字列, 空配列は無効とみなします。
    """
    if value is None:
        return False
    if isinstance(value, str):
        normalized = value.strip()
        return normalized != "" and normalized.lower() not in ["null", "undefined"]
    if isinstance(value, (int, float, bool)):
        return True
    if isinstance(value, list):
        return len(value) > 0
    if isinstance(value, dict):
        return len(value) > 0
    return True


def calculate_has_any_web_presence(data: Dict) -> bool:
    """
    ドキュメントデータからhasAnyWebPresenceを計算します。
    
    Web関連フィールドのいずれかが有効な値を持っていればTrueを返します。
    """
    for field in WEB_PRESENCE_FIELDS:
        if field in data:
            value = data[field]
            if has_value(value):
                return True
    return False


def find_credentials_file(cred_path: Optional[str] = None) -> Optional[str]:
    """
    認証情報ファイルを検索します。
    """
    if cred_path:
        if os.path.exists(cred_path):
            return cred_path
        return None
    
    # 環境変数を確認
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.exists(env_path):
        return env_path
    
    # デフォルトパスを検索（TypeScriptスクリプトと同様のパス）
    project_root = Path.cwd()
    default_paths = [
        project_root / "serviceAccountKey.json",
        project_root / "service-account-key.json",
        project_root / "firebase-service-account.json",
        project_root / "albert-ma-firebase-adminsdk-iat1k-a64039899f.json",
        project_root / "backend" / "api" / "serviceAccountKey.json",
        project_root / "backend" / "serviceAccountKey.json",
        project_root / "scripts" / "serviceAccountKey.json",
    ]
    
    for path in default_paths:
        if path.exists():
            return str(path)
    
    return None


def initialize_firebase(cred_path: Optional[str] = None) -> Client:
    """
    Firebase Admin SDKを初期化します。
    """
    # 既に初期化されている場合はスキップ
    try:
        if firebase_admin._apps:
            return firestore.client()
    except:
        pass
    
    # 認証情報ファイルを検索
    cred_file = find_credentials_file(cred_path)
    if not cred_file:
        # 検索したパスを表示
        project_root = Path.cwd()
        searched_paths = [
            str(project_root / "serviceAccountKey.json"),
            str(project_root / "service-account-key.json"),
            str(project_root / "firebase-service-account.json"),
            str(project_root / "albert-ma-firebase-adminsdk-iat1k-a64039899f.json"),
            str(project_root / "backend" / "api" / "serviceAccountKey.json"),
            str(project_root / "backend" / "serviceAccountKey.json"),
            str(project_root / "scripts" / "serviceAccountKey.json"),
        ]
        
        error_msg = (
            "認証情報ファイルが見つかりません。\n\n"
            "以下のパスを検索しました:\n"
        )
        for path in searched_paths:
            error_msg += f"  - {path}\n"
        error_msg += (
            "\n解決方法:\n"
            "1. --cred-pathオプションで認証情報ファイルのパスを指定\n"
            "   例: --cred-path /path/to/serviceAccountKey.json\n"
            "2. 環境変数GOOGLE_APPLICATION_CREDENTIALSを設定\n"
            "   例: export GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json\n"
            "3. 認証情報ファイルをプロジェクトルートに配置\n"
            "   例: ./serviceAccountKey.json"
        )
        raise FileNotFoundError(error_msg)
    
    # 認証情報を読み込み
    try:
        with open(cred_file, "r", encoding="utf-8") as f:
            cred_data = json.load(f)
        
        project_id = cred_data.get("project_id")
        if not project_id:
            raise ValueError("認証情報ファイルにproject_idが含まれていません")
        
        cred = credentials.Certificate(cred_file)
        firebase_admin.initialize_app(cred, {"projectId": project_id})
        
        print(f"✅ Firebase 初期化完了 (Project ID: {project_id})")
        return firestore.client()
    
    except Exception as e:
        raise RuntimeError(f"Firebase初期化エラー: {str(e)}")


# ==============================
# バッチ処理関数
# ==============================

def process_batch_with_retry(
    db: Client,
    collection_name: str,
    batch_docs: List[Tuple[str, Dict]],
    dry_run: bool = False
) -> Tuple[int, int, int]:
    """
    リトライ機能付きでバッチを処理します。
    
    Returns:
        (更新件数, スキップ件数, エラー件数)
    """
    for attempt in range(MAX_RETRIES):
        try:
            return process_batch(db, collection_name, batch_docs, dry_run)
        except Exception as e:
            error_msg = str(e)
            # リトライ可能なエラーかチェック
            is_retryable = any(keyword in error_msg.lower() for keyword in [
                "deadline", "timeout", "unavailable", "resource exhausted", "429", "504"
            ])
            
            if is_retryable and attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)  # 指数バックオフ
                print(f"  ⚠️  リトライ ({attempt + 1}/{MAX_RETRIES}) - {wait_time:.1f}秒待機...")
                time.sleep(wait_time)
                continue
            else:
                # リトライ不可能または最大リトライ回数に達した
                print(f"  ❌ バッチ処理エラー: {error_msg}")
                # エラー件数を返す（全ドキュメントをエラーとしてカウント）
                return 0, 0, len(batch_docs)
    
    return 0, 0, len(batch_docs)


def process_batch(
    db: Client,
    collection_name: str,
    batch_docs: List[Tuple[str, Dict]],
    dry_run: bool = False
) -> Tuple[int, int, int]:
    """
    バッチを処理します。
    
    Returns:
        (更新件数, スキップ件数, エラー件数)
    """
    updated = 0
    skipped = 0
    errors = 0
    
    batch = db.batch()
    batch_count = 0
    
    for doc_id, doc_data in batch_docs:
        try:
            # hasAnyWebPresenceを計算
            has_web_presence = calculate_has_any_web_presence(doc_data)
            
            # 既にhasAnyWebPresenceフィールドが存在し、値が同じ場合はスキップ
            if "hasAnyWebPresence" in doc_data:
                existing_value = doc_data["hasAnyWebPresence"]
                if existing_value == has_web_presence:
                    skipped += 1
                    continue
            
            # バッチに追加
            doc_ref = db.collection(collection_name).document(doc_id)
            batch.update(doc_ref, {"hasAnyWebPresence": has_web_presence})
            batch_count += 1
            updated += 1
            
            # バッチサイズに達したらコミット
            if batch_count >= MAX_BATCH_SIZE:
                if not dry_run:
                    batch.commit()
                batch = db.batch()
                batch_count = 0
        
        except Exception as e:
            print(f"  ⚠️  エラー (doc_id: {doc_id}): {str(e)}")
            errors += 1
    
    # 残りのバッチをコミット
    if batch_count > 0:
        if not dry_run:
            batch.commit()
    
    return updated, skipped, errors


def collect_batches(
    db: Client,
    collection_name: str,
    batch_size: int
) -> List[List[Tuple[str, Dict]]]:
    """
    コレクションからバッチを収集します（ページネーション使用）。
    """
    batches = []
    current_batch = []
    
    print("バッチを収集中...")
    
    # ページネーション用の設定
    FETCH_BATCH_SIZE = 500  # Firestoreから一度に取得する件数（タイムアウトを避けるため小さく）
    last_doc = None
    total_fetched = 0
    
    while True:
        # リトライ処理付きでクエリを実行
        retry_count = 0
        max_retries = 5  # リトライ回数を増やす
        docs_list = None
        
        while retry_count <= max_retries:
            try:
                # クエリを構築（ドキュメントID順にソート）
                # Firestore Python SDKでは、ドキュメントIDでソートするためにFieldPath.document_id()を使用
                query = db.collection(collection_name).order_by(FieldPath.document_id()).limit(FETCH_BATCH_SIZE)
                
                if last_doc:
                    query = query.start_after(last_doc)
                
                # get()を使用（内部的にstream()が呼ばれるが、例外をキャッチしてリトライ）
                snapshot = query.get()
                docs_list = list(snapshot)
                
                # 成功したらループを抜ける
                break
                
            except (DeadlineExceeded, ServiceUnavailable, ResourceExhausted, AttributeError) as e:
                # AttributeErrorは、Firestore SDKの内部バグ（_retry属性エラー）をキャッチ
                error_msg = str(e).lower()
                retry_count += 1
                
                if retry_count > max_retries:
                    print(f"  ❌ エラー: 最大リトライ回数に達しました: {str(e)}")
                    raise
                
                # リトライ可能なエラーかチェック
                is_retryable = (
                    isinstance(e, (DeadlineExceeded, ServiceUnavailable, ResourceExhausted)) or
                    isinstance(e, AttributeError) and "_retry" in str(e) or
                    "timeout" in error_msg or 
                    "unavailable" in error_msg or 
                    "deadline" in error_msg or
                    "deadline exceeded" in error_msg
                )
                
                if is_retryable:
                    wait_time = 5 * retry_count  # 指数バックオフ（5秒、10秒、15秒...）
                    error_short = str(e)[:100] if len(str(e)) > 100 else str(e)
                    print(f"  ⚠️  エラー ({retry_count}/{max_retries}): {error_short}")
                    print(f"  ⏳ {wait_time}秒待機して再試行...")
                    time.sleep(wait_time)
                    continue
                else:
                    # リトライ不可能なエラー
                    raise
                    
            except Exception as e:
                # その他の予期しないエラー
                error_msg = str(e).lower()
                retry_count += 1
                
                if retry_count > max_retries:
                    print(f"  ❌ エラー: 最大リトライ回数に達しました: {str(e)}")
                    raise
                
                # リトライ可能かどうかをチェック
                if "timeout" in error_msg or "unavailable" in error_msg or "deadline" in error_msg:
                    wait_time = 5 * retry_count
                    error_short = str(e)[:100] if len(str(e)) > 100 else str(e)
                    print(f"  ⚠️  エラー ({retry_count}/{max_retries}): {error_short}")
                    print(f"  ⏳ {wait_time}秒待機して再試行...")
                    time.sleep(wait_time)
                    continue
                else:
                    # リトライ不可能なエラー
                    raise
        
        if not docs_list:
            break
        
        # 取得したドキュメントを処理
        for doc in docs_list:
            doc_data = doc.to_dict()
            current_batch.append((doc.id, doc_data))
            total_fetched += 1
            
            # バッチサイズに達したら追加
            if len(current_batch) >= batch_size:
                batches.append(current_batch)
                current_batch = []
                
                if len(batches) % 100 == 0:
                    print(f"  収集中: {len(batches)} バッチ収集済み ({total_fetched:,} 件)...")
        
        # 次のページネーション用に最後のドキュメントを保存
        if len(docs_list) < FETCH_BATCH_SIZE:
            # これ以上取得できない
            break
        
        last_doc = docs_list[-1]
    
    # 残りのバッチを追加
    if current_batch:
        batches.append(current_batch)
    
    print(f"  収集完了: {len(batches)} バッチ ({total_fetched:,} 件)")
    return batches


def process_collection(
    db: Client,
    collection_name: str,
    batch_size: int = DEFAULT_BATCH_SIZE,
    concurrency: int = DEFAULT_CONCURRENCY,
    dry_run: bool = False
) -> Dict:
    """
    コレクションを処理します。
    """
    print(f"\n{'=' * 60}")
    print(f"{collection_name} のバックフィルを開始")
    print(f"{'=' * 60}")
    print(f"  バッチサイズ: {batch_size}")
    print(f"  並列実行数: {concurrency}")
    print(f"  開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # バッチを収集
    batches = collect_batches(db, collection_name, batch_size)
    
    if not batches:
        print("  処理するドキュメントがありません。")
        return {
            "updated": 0,
            "skipped": 0,
            "errors": 0,
            "batches": 0,
            "duration": 0.0,
        }
    
    # バッチ処理を開始
    print("\nバッチ処理を開始...")
    start_time = datetime.now()
    
    total_updated = 0
    total_skipped = 0
    total_errors = 0
    completed_batches = 0
    
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        # タスクを送信
        future_to_batch = {
            executor.submit(process_batch_with_retry, db, collection_name, batch, dry_run): i
            for i, batch in enumerate(batches)
        }
        
        # 完了を待機
        for future in as_completed(future_to_batch):
            batch_idx = future_to_batch[future]
            try:
                updated, skipped, errors = future.result()
                total_updated += updated
                total_skipped += skipped
                total_errors += errors
                completed_batches += 1
                
                # 進捗を表示
                if completed_batches % 10 == 0 or completed_batches == len(batches):
                    elapsed = (datetime.now() - start_time).total_seconds()
                    rate = completed_batches / elapsed if elapsed > 0 else 0
                    remaining = (len(batches) - completed_batches) / rate if rate > 0 else 0
                    
                    print(
                        f"  進捗: {completed_batches}/{len(batches)} バッチ完了 "
                        f"(更新: {total_updated}, スキップ: {total_skipped}, エラー: {total_errors}) "
                        f"[{rate:.2f} バッチ/秒, 残り約{remaining/60:.1f}分]"
                    )
            
            except Exception as e:
                print(f"  ⚠️  バッチ {batch_idx} でエラー: {str(e)}")
                total_errors += 1
                completed_batches += 1
    
    # 結果をまとめる
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"\n{'=' * 60}")
    print(f"{collection_name} のバックフィル完了")
    print(f"{'=' * 60}")
    print(f"  合計更新: {total_updated:,} 件")
    print(f"  合計スキップ: {total_skipped:,} 件")
    print(f"  合計エラー: {total_errors:,} 件")
    print(f"  バッチ数: {len(batches)} バッチ")
    print(f"  処理時間: {duration/60:.1f} 分 ({duration:.1f} 秒)")
    if duration > 0:
        print(f"  平均速度: {len(batches)/duration:.2f} バッチ/秒")
    print(f"  終了時刻: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    return {
        "updated": total_updated,
        "skipped": total_skipped,
        "errors": total_errors,
        "batches": len(batches),
        "duration": duration,
    }


# ==============================
# メイン処理
# ==============================

def main():
    parser = argparse.ArgumentParser(
        description="hasAnyWebPresenceフィールドの高速化バックフィルスクリプト"
    )
    parser.add_argument(
        "--collection",
        type=str,
        choices=["companies_new", "companies_index", "both"],
        default="both",
        help="対象コレクション (companies_new, companies_index, both)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"バッチサイズ（デフォルト: {DEFAULT_BATCH_SIZE}）",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"並列実行数（デフォルト: {DEFAULT_CONCURRENCY}）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="実際には更新せず、統計のみ表示",
    )
    parser.add_argument(
        "--cred-path",
        type=str,
        default=None,
        help="Firebase認証情報ファイルのパス",
    )
    
    args = parser.parse_args()
    
    # バッチサイズの検証
    if args.batch_size > MAX_BATCH_SIZE:
        print(f"⚠️  警告: バッチサイズが{MAX_BATCH_SIZE}を超えています。{MAX_BATCH_SIZE}に制限します。")
        args.batch_size = MAX_BATCH_SIZE
    
    # ヘッダーを表示
    print("=" * 60)
    print("hasAnyWebPresence フィールドの高速化バックフィル")
    print("=" * 60)
    
    if args.dry_run:
        print("[DRY-RUN モード: 実際には更新しません]")
        print()
    else:
        print("本番環境で実行する場合は、必ずバックアップを取ってください。")
        print()
    
    # Firebaseを初期化
    try:
        db = initialize_firebase(args.cred_path)
    except Exception as e:
        print(f"❌ エラー: {str(e)}")
        sys.exit(1)
    
    # コレクションを処理
    results = {}
    
    if args.collection in ["companies_new", "both"]:
        results["companies_new"] = process_collection(
            db, "companies_new", args.batch_size, args.concurrency, args.dry_run
        )
    
    if args.collection in ["companies_index", "both"]:
        results["companies_index"] = process_collection(
            db, "companies_index", args.batch_size, args.concurrency, args.dry_run
        )
    
    # 総合結果を表示
    if len(results) > 1:
        print("=" * 60)
        print("総合結果")
        print("=" * 60)
        total_updated = sum(r["updated"] for r in results.values())
        total_skipped = sum(r["skipped"] for r in results.values())
        total_errors = sum(r["errors"] for r in results.values())
        total_duration = sum(r["duration"] for r in results.values())
        
        print(f"  合計更新: {total_updated:,} 件")
        print(f"  合計スキップ: {total_skipped:,} 件")
        print(f"  合計エラー: {total_errors:,} 件")
        print(f"  合計処理時間: {total_duration/60:.1f} 分 ({total_duration:.1f} 秒)")
        print()


if __name__ == "__main__":
    main()
