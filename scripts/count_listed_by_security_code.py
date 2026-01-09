#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
証券コードベースの上場企業数カウントスクリプト

目的:
- companies_newコレクションで、securityCodeまたはsecuritiesCodeフィールドに
  有効な値が入っているドキュメント数をカウント
- より信頼性の高い指標として実質的な上場企業数を算出

必要環境変数:
- FIREBASE_SERVICE_ACCOUNT_KEY: Firebaseサービスアカウントキーのパス（必須）

実行例:
  export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
  python3 scripts/count_listed_by_security_code.py
"""

import os
import sys
from typing import Optional, Dict, Any, List
from datetime import datetime

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
except ImportError:
    print("❌ エラー: firebase-admin がインストールされていません")
    print("   インストール方法: pip install firebase-admin")
    sys.exit(1)


def initialize_firebase() -> firestore.Client:
    """Firebase Admin SDKを初期化"""
    service_account_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY")
    
    if not service_account_path:
        print("❌ エラー: FIREBASE_SERVICE_ACCOUNT_KEY 環境変数が設定されていません")
        print("   実行例: export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'")
        sys.exit(1)
    
    if not os.path.exists(service_account_path):
        print(f"❌ エラー: サービスアカウントキーファイルが存在しません: {service_account_path}")
        sys.exit(1)
    
    try:
        # 既に初期化されている場合はスキップ
        if not firebase_admin._apps:
            cred = credentials.Certificate(service_account_path)
            firebase_admin.initialize_app(cred, {
                'projectId': 'albert-ma'
            })
        
        db = firestore.client()
        print("✅ Firebase初期化完了")
        return db
    except Exception as e:
        print(f"❌ Firebase初期化エラー: {e}")
        sys.exit(1)


def has_valid_value(value: Any) -> bool:
    """
    値が有効かどうかをチェック
    
    Args:
        value: チェックする値
        
    Returns:
        有効な値がある場合True
    """
    if value is None:
        return False
    
    if isinstance(value, str):
        return value.strip() != ""
    
    if isinstance(value, list):
        return len(value) > 0
    
    if isinstance(value, dict):
        return len(value) > 0
    
    # 数値やその他の型は有効とみなす
    return True


def has_security_code(data: Dict[str, Any]) -> bool:
    """
    ドキュメントに有効な証券コードがあるかチェック
    
    Args:
        data: ドキュメントのデータ
        
    Returns:
        securityCodeまたはsecuritiesCodeに有効な値がある場合True
    """
    # securityCodeをチェック
    if has_valid_value(data.get("securityCode")):
        return True
    
    # securitiesCodeをチェック
    if has_valid_value(data.get("securitiesCode")):
        return True
    
    return False


def count_listed_companies_by_security_code(db: firestore.Client) -> None:
    """
    証券コードベースで上場企業数をカウント
    
    Args:
        db: Firestoreクライアント
    """
    print("\n証券コードベースの上場企業数カウントを開始...\n")
    
    total_count = 0
    has_security_code_count = 0
    samples: List[Dict[str, Any]] = []
    
    BATCH_SIZE = 5000
    companies_ref = db.collection("companies_new")
    
    # 全ドキュメントをバッチ処理で取得
    last_doc = None
    
    try:
        while True:
            # クエリを構築（Python版ではdocument_id()は直接使用できないため、別の方法を使用）
            # まずは全件取得してからフィルタリングする方法に変更
            query = companies_ref.limit(BATCH_SIZE)
            
            if last_doc:
                query = query.start_after(last_doc)
            
            # ドキュメントを取得
            docs = query.stream()
            doc_list = list(docs)
            
            if not doc_list:
                break
            
            # 各ドキュメントを処理
            for doc in doc_list:
                total_count += 1
                data = doc.to_dict()
                doc_id = doc.id
                
                # 証券コードの有無をチェック
                if has_security_code(data):
                    has_security_code_count += 1
                    
                    # サンプルを収集（最大5件）
                    if len(samples) < 5:
                        samples.append({
                            "id": doc_id,
                            "name": data.get("name", "(名前なし)"),
                            "securityCode": data.get("securityCode", ""),
                            "securitiesCode": data.get("securitiesCode", "")
                        })
                
                # 進捗表示（10,000件ごと）
                if total_count % 10000 == 0:
                    print(f"処理中... 総数: {total_count:,} 件, 証券コードあり: {has_security_code_count:,} 件")
            
            # 次のバッチの開始位置を設定
            last_doc = doc_list[-1]
    
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # 結果を表示
    print("\n" + "=" * 60)
    print("📊 調査結果")
    print("=" * 60)
    print(f"スキャンした総数: {total_count:,} 件")
    print(f"証券コードあり（実質的な上場企業数）: {has_security_code_count:,} 件")
    
    if total_count > 0:
        percentage = (has_security_code_count / total_count) * 100
        print(f"割合: {percentage:.2f}%")
    
    # サンプルデータを表示
    if samples:
        print("\n" + "=" * 60)
        print("📋 証券コードがあるデータのサンプル（5件）")
        print("=" * 60)
        for i, sample in enumerate(samples, 1):
            print(f"\n{i}. ドキュメントID: {sample['id']}")
            print(f"   企業名: {sample['name']}")
            print(f"   securityCode: {sample['securityCode'] or '(なし)'}")
            print(f"   securitiesCode: {sample['securitiesCode'] or '(なし)'}")
    else:
        print("\n⚠️  サンプルデータが見つかりませんでした")
    
    print("\n" + "=" * 60)
    print("✅ 処理完了")
    print("=" * 60)


def main():
    """メイン処理"""
    print("=" * 60)
    print("証券コードベースの上場企業数カウント")
    print("=" * 60)
    print(f"開始時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Firebase初期化
    db = initialize_firebase()
    
    # カウント実行
    count_listed_companies_by_security_code(db)
    
    print(f"\n終了時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()
