#!/usr/bin/env python3
"""
hasAnyWebPresenceフィールドの追加状況を確認するスクリプト

companies_newとcompanies_indexコレクションの全ドキュメントに対して、
hasAnyWebPresenceフィールドが存在するかどうかを確認します。
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, Optional

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    from google.cloud.firestore import Client
    from google.cloud.firestore_v1.field_path import FieldPath
    from google.api_core.exceptions import DeadlineExceeded, ServiceUnavailable, ResourceExhausted
except ImportError:
    print("❌ エラー: 必要なパッケージがインストールされていません")
    print("   以下のコマンドでインストールしてください:")
    print("   pip install firebase-admin google-cloud-firestore")
    sys.exit(1)


def find_credentials_file(cred_path: Optional[str] = None) -> Optional[str]:
    """認証情報ファイルを検索します。"""
    if cred_path:
        if os.path.exists(cred_path):
            return cred_path
        return None
    
    env_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if env_path and os.path.exists(env_path):
        return env_path
    
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
    """Firebase Admin SDKを初期化します。"""
    try:
        if firebase_admin._apps:
            return firestore.client()
    except:
        pass
    
    cred_file = find_credentials_file(cred_path)
    if not cred_file:
        raise FileNotFoundError(
            "認証情報ファイルが見つかりません。\n"
            "--cred-pathオプションでパスを指定するか、\n"
            "環境変数GOOGLE_APPLICATION_CREDENTIALSを設定してください。"
        )
    
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


def verify_collection(
    db: Client,
    collection_name: str,
    sample_size: Optional[int] = None
) -> Dict:
    """コレクションのhasAnyWebPresenceフィールドの存在を確認します。"""
    print(f"\n{'=' * 60}")
    print(f"{collection_name} の確認を開始")
    print(f"{'=' * 60}")
    
    total_count = 0
    has_field_count = 0
    missing_field_count = 0
    true_count = 0
    false_count = 0
    
    FETCH_BATCH_SIZE = 500
    last_doc = None
    
    print("ドキュメントを確認中...")
    
    while True:
        retry_count = 0
        max_retries = 5
        docs_list = None
        
        while retry_count <= max_retries:
            try:
                query = db.collection(collection_name).order_by(FieldPath.document_id()).limit(FETCH_BATCH_SIZE)
                
                if last_doc:
                    query = query.start_after(last_doc)
                
                snapshot = query.get()
                docs_list = list(snapshot)
                break
                
            except (DeadlineExceeded, ServiceUnavailable, ResourceExhausted, AttributeError) as e:
                error_msg = str(e).lower()
                retry_count += 1
                
                if retry_count > max_retries:
                    print(f"  ❌ エラー: 最大リトライ回数に達しました: {str(e)}")
                    raise
                
                is_retryable = (
                    isinstance(e, (DeadlineExceeded, ServiceUnavailable, ResourceExhausted)) or
                    isinstance(e, AttributeError) and "_retry" in str(e) or
                    "timeout" in error_msg or "unavailable" in error_msg or "deadline" in error_msg
                )
                
                if is_retryable:
                    import time
                    wait_time = 5 * retry_count
                    print(f"  ⚠️  エラー ({retry_count}/{max_retries}): {str(e)[:100]}")
                    print(f"  ⏳ {wait_time}秒待機して再試行...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
                    
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    raise
                import time
                time.sleep(5 * retry_count)
                continue
        
        if not docs_list:
            break
        
        for doc in docs_list:
            total_count += 1
            doc_data = doc.to_dict()
            
            if "hasAnyWebPresence" in doc_data:
                has_field_count += 1
                if doc_data["hasAnyWebPresence"] is True:
                    true_count += 1
                elif doc_data["hasAnyWebPresence"] is False:
                    false_count += 1
            else:
                missing_field_count += 1
            
            # サンプルサイズが指定されている場合は、その数に達したら終了
            if sample_size and total_count >= sample_size:
                break
            
            if total_count % 10000 == 0:
                print(f"  確認中: {total_count:,} 件 (フィールドあり: {has_field_count:,}, なし: {missing_field_count:,})")
        
        if sample_size and total_count >= sample_size:
            break
        
        if len(docs_list) < FETCH_BATCH_SIZE:
            break
        
        last_doc = docs_list[-1]
    
    print(f"\n{'=' * 60}")
    print(f"{collection_name} の確認完了")
    print(f"{'=' * 60}")
    print(f"  総ドキュメント数: {total_count:,} 件")
    print(f"  hasAnyWebPresenceフィールドあり: {has_field_count:,} 件 ({has_field_count/total_count*100:.2f}%)")
    print(f"  hasAnyWebPresenceフィールドなし: {missing_field_count:,} 件 ({missing_field_count/total_count*100:.2f}%)")
    print(f"  hasAnyWebPresence = true: {true_count:,} 件")
    print(f"  hasAnyWebPresence = false: {false_count:,} 件")
    print()
    
    return {
        "total": total_count,
        "has_field": has_field_count,
        "missing_field": missing_field_count,
        "true_count": true_count,
        "false_count": false_count,
    }


def main():
    parser = argparse.ArgumentParser(
        description="hasAnyWebPresenceフィールドの追加状況を確認するスクリプト"
    )
    parser.add_argument(
        "--collection",
        type=str,
        choices=["companies_new", "companies_index", "both"],
        default="both",
        help="対象コレクション (companies_new, companies_index, both)",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=None,
        help="サンプルサイズ（指定した数のドキュメントのみ確認）",
    )
    parser.add_argument(
        "--cred-path",
        type=str,
        default=None,
        help="Firebase認証情報ファイルのパス",
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("hasAnyWebPresence フィールドの確認")
    print("=" * 60)
    if args.sample_size:
        print(f"[サンプルモード: {args.sample_size:,} 件のみ確認]")
    print()
    
    try:
        db = initialize_firebase(args.cred_path)
    except Exception as e:
        print(f"❌ エラー: {str(e)}")
        sys.exit(1)
    
    results = {}
    
    if args.collection in ["companies_new", "both"]:
        results["companies_new"] = verify_collection(
            db, "companies_new", args.sample_size
        )
    
    if args.collection in ["companies_index", "both"]:
        results["companies_index"] = verify_collection(
            db, "companies_index", args.sample_size
        )
    
    if len(results) > 1:
        print("=" * 60)
        print("総合結果")
        print("=" * 60)
        total_docs = sum(r["total"] for r in results.values())
        total_has_field = sum(r["has_field"] for r in results.values())
        total_missing = sum(r["missing_field"] for r in results.values())
        total_true = sum(r["true_count"] for r in results.values())
        total_false = sum(r["false_count"] for r in results.values())
        
        print(f"  総ドキュメント数: {total_docs:,} 件")
        print(f"  hasAnyWebPresenceフィールドあり: {total_has_field:,} 件 ({total_has_field/total_docs*100:.2f}%)")
        print(f"  hasAnyWebPresenceフィールドなし: {total_missing:,} 件 ({total_missing/total_docs*100:.2f}%)")
        print(f"  hasAnyWebPresence = true: {total_true:,} 件")
        print(f"  hasAnyWebPresence = false: {total_false:,} 件")
        print()
        
        if total_missing == 0:
            print("✅ 全てのドキュメントにhasAnyWebPresenceフィールドが追加されています！")
        else:
            print(f"⚠️  {total_missing:,} 件のドキュメントにhasAnyWebPresenceフィールドがありません。")


if __name__ == "__main__":
    main()
