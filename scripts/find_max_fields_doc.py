#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
companies_new コレクションでフィールド数が最大のドキュメントを洗い出し、
そのフィールド名を全て出力するスクリプト。

必要環境変数:
- FIREBASE_SERVICE_ACCOUNT_KEY: Firebase サービスアカウントキーのパス（必須）

オプション環境変数:
- LIMIT: スキャンする最大件数（未指定なら全件。動作確認用に 10000 等を指定可）

実行例:
  export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
  python3 scripts/find_max_fields_doc.py
"""

from __future__ import annotations

import os
import sys
from typing import Any

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    from google.cloud.firestore_v1.field_path import FieldPath
except ImportError:
    print("❌ エラー: firebase-admin 等がインストールされていません")
    print("   インストール: pip install firebase-admin")
    sys.exit(1)


def initialize_firebase() -> firestore.Client:
    """Firebase Admin SDK を初期化"""
    path = os.getenv("FIREBASE_SERVICE_ACCOUNT_KEY")
    if not path:
        print("❌ エラー: FIREBASE_SERVICE_ACCOUNT_KEY が未設定です")
        print("   例: export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'")
        sys.exit(1)
    if not os.path.exists(path):
        print(f"❌ エラー: キーファイルが存在しません: {path}")
        sys.exit(1)
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(path)
            firebase_admin.initialize_app(cred, {"projectId": "albert-ma"})
        print("✅ Firebase 初期化完了")
        return firestore.client()
    except Exception as e:
        print(f"❌ Firebase 初期化エラー: {e}")
        sys.exit(1)


def run(db: firestore.Client) -> None:
    BATCH_SIZE = 2000
    limit_raw = os.getenv("LIMIT")
    max_to_scan = int(limit_raw) if (limit_raw and limit_raw.isdigit()) else None

    companies_ref = db.collection("companies_new")
    query = (
        companies_ref.order_by(FieldPath.document_id())
        .limit(BATCH_SIZE)
    )

    max_count = -1
    max_docs: list[tuple[str, int, list[str]]] = []
    total = 0
    last_doc: Any = None

    print("companies_new をスキャン中...")
    try:
        while True:
            snapshot = list(query.get())
            if not snapshot:
                break

            for doc in snapshot:
                total += 1
                doc_id = doc.id
                data = doc.to_dict() or {}
                fields = sorted(data.keys())
                n = len(fields)

                if n > max_count:
                    max_count = n
                    max_docs = [(doc_id, n, fields)]
                elif n == max_count:
                    max_docs.append((doc_id, n, fields))

                if total % 50000 == 0 and total > 0:
                    print(f"  処理済み: {total:,} 件 (現在の最大フィールド数: {max_count})")

            if max_to_scan and total >= max_to_scan:
                print(f"  LIMIT={max_to_scan} に達したため打ち切り")
                break
            if len(snapshot) < BATCH_SIZE:
                break

            last_doc = snapshot[-1]
            query = (
                companies_ref.order_by(FieldPath.document_id())
                .limit(BATCH_SIZE)
                .start_after([last_doc.id])
            )

    except KeyboardInterrupt:
        print("\n⏸ 中断しました。ここまでの結果を出力します。")
    except Exception as e:
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # 結果出力
    print()
    print("=" * 60)
    print("📊 フィールド数が最大のドキュメント")
    print("=" * 60)
    print(f"スキャン件数: {total:,} 件")
    if max_to_scan:
        print("  ⚠️  LIMIT により打ち切っています。全件で確定するには LIMIT 未設定で再実行してください。")
    print(f"最大フィールド数: {max_count}")
    print(f"該当ドキュメント数: {len(max_docs)} 件")
    print()

    all_fields: set[str] = set()
    for i, (doc_id, n, fields) in enumerate(max_docs, 1):
        print(f"--- ドキュメント {i} ---")
        print(f"  ドキュメントID: {doc_id}")
        print(f"  フィールド数: {n}")
        print(f"  フィールド名一覧（{n} 件）:")
        for f in fields:
            print(f"    - {f}")
            all_fields.add(f)
        print()

    if len(max_docs) > 1:
        print("=" * 60)
        print("📋 全該当ドキュメントのユニークフィールド名（重複除く）")
        print("=" * 60)
        for f in sorted(all_fields):
            print(f"  - {f}")
        print(f"  合計: {len(all_fields)} 件")
    print()
    print("✅ 完了")


def main() -> None:
    db = initialize_firebase()
    run(db)


if __name__ == "__main__":
    main()
