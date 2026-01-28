# Firestore → Cloud SQL (PostgreSQL) 移行ガイド

このディレクトリには、Firestore `companies_new` コレクションから Cloud SQL (PostgreSQL) への移行に必要なファイルが含まれています。

## 📁 ファイル構成

- `sql/create_companies_table.sql` - PostgreSQL用テーブル定義 (DDL)
- `scripts/migrate_companies.py` - データ移行スクリプト
- `scripts/search_ui_optimized.py` - 検索API（PostgreSQL版）

## 🚀 セットアップ手順

### 1. PostgreSQLデータベースの準備

Cloud SQL (PostgreSQL) インスタンスを作成し、データベースを作成します。

```bash
# Cloud SQLに接続
gcloud sql connect <INSTANCE_NAME> --user=postgres

# データベース作成
CREATE DATABASE companies_db;
```

### 2. テーブル作成

```bash
# SQLファイルを実行
psql -h <HOST> -U postgres -d companies_db -f sql/create_companies_table.sql
```

または、Cloud SQLに直接接続して実行:

```bash
psql "postgresql://<USER>:<PASSWORD>@<HOST>:<PORT>/companies_db" -f sql/create_companies_table.sql
```

### 3. 環境変数の設定

移行スクリプトと検索APIで使用する環境変数を設定します。

```bash
# Firebase認証情報のパス
export FIREBASE_CREDENTIALS_PATH="path/to/serviceAccountKey.json"

# PostgreSQL接続情報
export POSTGRES_HOST="<CLOUD_SQL_HOST>"
export POSTGRES_PORT="5432"
export POSTGRES_DB="companies_db"
export POSTGRES_USER="postgres"
export POSTGRES_PASSWORD="<PASSWORD>"
```

### 4. Python依存パッケージのインストール

```bash
pip install firebase-admin psycopg2-binary sqlalchemy
```

## 📊 データ移行の実行

### 移行スクリプトの実行

```bash
python backend/scripts/migrate_companies.py
```

### 機能

- **ページネーション**: 1000件ずつFirestoreからデータを取得
- **再開機能**: 途中で停止した場合、`migrate_companies_resume.txt` に最後のIDを記録し、次回実行時にそこから再開
- **バッチINSERT**: `executemany` を使用して500件ずつINSERT（高速化）
- **UPSERT**: 既存データは更新、新規データは挿入

### ログ

移行の進捗は `migrate_companies.log` に記録されます。

### 再開方法

移行が途中で停止した場合、同じコマンドを再実行するだけで、最後の処理位置から自動的に再開されます。

```bash
# 再開ポイントが保存されている場合、自動的にそこから再開
python backend/scripts/migrate_companies.py
```

## 🔍 検索APIの使用

### 基本的な使用方法

```python
from backend.scripts.search_ui_optimized import SearchParams, search_companies

# 検索パラメータを設定
params = SearchParams(
    prefecture='東京都',
    revenue_min=100000000,
    industry_tags=['IT', '通信'],
    listing='プライム',
    limit=50,
    offset=0
)

# 検索実行
result = search_companies(params)

print(f"総件数: {result['total']}")
print(f"取得件数: {len(result['companies'])}")
```

### 検索パラメータ

- `prefecture` (str): 都道府県
- `revenue_min` (int): 売上高（最小値）
- `revenue_max` (int): 売上高（最大値）
- `industry_tags` (List[str]): 業種タグ（配列検索）
- `listing` (str): 上場区分
- `capital_stock_min` (int): 資本金（最小値）
- `employee_count_min` (int): 従業員数（最小値）
- `industry` (str): 業種
- `industry_large` (str): 業種（大分類）
- `industry_middle` (str): 業種（中分類）
- `name` (str): 企業名（部分一致）
- `corporate_number` (str): 法人番号
- `limit` (int): 取得件数（デフォルト: 50）
- `offset` (int): オフセット（デフォルト: 0）

### APIエンドポイントとして使用

FlaskやFastAPIなどのWebフレームワークで使用する場合:

```python
from flask import Flask, request, jsonify
from backend.scripts.search_ui_optimized import handle_search_request

app = Flask(__name__)

@app.route('/api/companies/search', methods=['POST'])
def search_companies_api():
    request_data = request.get_json()
    result = handle_search_request(request_data)
    return jsonify(result)
```

## 📋 テーブル構造

### 主要フィールド

- **基本情報**: `id` (PK), `name`, `corporate_number`, `prefecture`
- **財務情報**: `revenue`, `capital_stock`, `listing`
- **業種情報**: `industry`, `industries` (配列), `industry_large`, `industry_middle`
- **組織情報**: `employee_count`, `representative_name`
- **JSONBフィールド**: `executives`, `financials`, `subsidiaries`, `affiliations`

### インデックス

以下のインデックスが作成されています:

- `idx_companies_prefecture` - 都道府県検索用
- `idx_companies_revenue` - 売上高範囲検索用
- `idx_companies_listing` - 上場区分検索用
- `idx_companies_industries_gin` - 業種タグ配列検索用（GINインデックス）
- `idx_companies_name` - 企業名検索用
- `idx_companies_corporate_number` - 法人番号検索用
- その他、主要フィールドにインデックス

## ⚠️ 注意事項

1. **データ量**: 約400万件のデータを移行するため、時間がかかります（数時間〜1日程度）
2. **メモリ使用量**: バッチ処理によりメモリ使用量を抑制していますが、大量データ処理時は注意が必要です
3. **接続タイムアウト**: Cloud SQLへの接続タイムアウトが発生する場合は、接続プールの設定を調整してください
4. **再開機能**: 移行中にエラーが発生した場合、`migrate_companies_resume.txt` を確認して再開ポイントを確認できます

## 🔧 トラブルシューティング

### 移行が途中で停止した場合

1. `migrate_companies_resume.txt` を確認
2. 同じコマンドを再実行（自動的に再開ポイントから再開）

### 接続エラーが発生した場合

- Cloud SQLの接続設定を確認
- ファイアウォールルールを確認
- 環境変数（`POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`）を確認

### 検索が遅い場合

- インデックスが正しく作成されているか確認
- `EXPLAIN ANALYZE` を使用してクエリプランを確認
- 必要に応じて追加のインデックスを作成

## 📝 補足情報

- Firestoreのデータ構造はそのままPostgreSQLに移行されます
- フィールド名はキャメルケースからスネークケースに自動変換されます
- 配列フィールドは PostgreSQL の `TEXT[]` 型として保存されます
- 構造化データ（JSON）は `JSONB` 型として保存されます
