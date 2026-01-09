# hasAnyWebPresence フィールドバックフィルスクリプト 実行ガイド

## 概要

`companies_new`と`companies_index`コレクションの全ドキュメントに対して、`hasAnyWebPresence`フィールドを計算して追加する高速化バックフィルスクリプトです。

## 前提条件

### 1. 必要なPythonパッケージ

```bash
pip install firebase-admin google-cloud-firestore
```

### 2. Firebase認証情報の準備

以下のいずれかの方法で認証情報を準備してください：

#### 方法A: サービスアカウントキーファイル（推奨）

1. Firebase Consoleでサービスアカウントキーをダウンロード
2. ファイルをプロジェクト内に配置（例: `backend/api/serviceAccountKey.json`）

#### 方法B: 環境変数

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json
```

## スクリプトの配置

### 1. スクリプトファイルの作成

`backend/scripts/backfill_hasAnyWebPresence_optimized.py` をプロジェクトに配置します。

### 2. 実行権限の付与

```bash
chmod +x backend/scripts/backfill_hasAnyWebPresence_optimized.py
```

## 実行方法

### 1. ドライラン（推奨：まず実行して確認）

実際には更新せず、統計情報のみを表示します。

```bash
# 両方のコレクションを確認
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection both --dry-run

# 個別に確認
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection companies_index --dry-run
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection companies_new --dry-run
```

### 2. 実際の実行

```bash
# 基本実行（推奨設定）
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection both --concurrency 5 --batch-size 500

# 個別に実行
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection companies_index --concurrency 5 --batch-size 500
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection companies_new --concurrency 5 --batch-size 500
```

### 3. オプション

#### 認証情報ファイルを明示的に指定

```bash
python backend/scripts/backfill_hasAnyWebPresence_optimized.py \
  --collection both \
  --cred-path /path/to/serviceAccountKey.json \
  --concurrency 5 \
  --batch-size 500
```

#### より高速化したい場合（注意: Firestoreのレート制限に注意）

```bash
# 並列数を増やす（最大10程度まで推奨）
python backend/scripts/backfill_hasAnyWebPresence_optimized.py \
  --collection both \
  --concurrency 10 \
  --batch-size 500
```

## オプション一覧

| オプション | 説明 | デフォルト値 |
|-----------|------|------------|
| `--collection` | 対象コレクション (`companies_new`, `companies_index`, `both`) | `both` |
| `--batch-size` | バッチサイズ（Firestore制限: 500件/バッチ） | `500` |
| `--concurrency` | 並列実行数（推奨: 5-10） | `5` |
| `--dry-run` | 実際には更新せず、統計のみ表示 | `False` |
| `--cred-path` | Firebase認証情報ファイルのパス | 自動検出 |

## 実行例

### 例1: ドライランで確認

```bash
$ python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection both --dry-run

============================================================
hasAnyWebPresence フィールドの高速化バックフィル
============================================================
[DRY-RUN モード: 実際には更新しません]

============================================================
companies_new のバックフィルを開始
============================================================
  バッチサイズ: 500
  並列実行数: 5
  開始時刻: 2026-01-07 10:00:00

バッチを収集中...
  収集中: 100 バッチ収集済み...
  収集中: 200 バッチ収集済み...
  収集完了: 250 バッチ

バッチ処理を開始...
  進捗: 10/250 バッチ完了 (更新: 5000, スキップ: 2000, エラー: 0) [2.5 バッチ/秒, 残り約1.6分]
  ...
```

### 例2: 実際の実行

```bash
$ python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection both

============================================================
hasAnyWebPresence フィールドの高速化バックフィル
============================================================
本番環境で実行する場合は、必ずバックアップを取ってください。

============================================================
companies_new のバックフィルを開始
============================================================
  バッチサイズ: 500
  並列実行数: 5
  開始時刻: 2026-01-07 10:00:00

バッチを収集中...
  収集完了: 250 バッチ

バッチ処理を開始...
  進捗: 10/250 バッチ完了 (更新: 5000, スキップ: 2000, エラー: 0) [2.5 バッチ/秒, 残り約1.6分]
  ...

============================================================
companies_new のバックフィル完了
============================================================
  合計更新: 50,000 件
  合計スキップ: 75,000 件
  合計エラー: 0 件
  バッチ数: 250 バッチ
  処理時間: 100.0 分 (6000.0 秒)
  平均速度: 0.25 バッチ/秒
  終了時刻: 2026-01-07 11:40:00
```

## トラブルシューティング

### 1. 認証エラー

**エラー:**
```
google.auth.exceptions.DefaultCredentialsError: Your default credentials were not found.
```

**解決方法:**
- `--cred-path`オプションで認証情報ファイルのパスを指定
- 環境変数`GOOGLE_APPLICATION_CREDENTIALS`を設定

### 2. DeadlineExceededエラー

**エラー:**
```
google.api_core.exceptions.DeadlineExceeded: 504 Deadline Exceeded
```

**解決方法:**
- スクリプトは自動的にリトライします（最大3回）
- バッチサイズを小さくする: `--batch-size 300`
- 並列数を減らす: `--concurrency 3`

### 3. レート制限エラー

**エラー:**
```
google.api_core.exceptions.ResourceExhausted: 429 Too Many Requests
```

**解決方法:**
- 並列数を減らす: `--concurrency 3`
- バッチサイズを小さくする: `--batch-size 300`
- 実行を一時停止してから再開

### 4. メモリ不足

**症状:**
- スクリプトがクラッシュする
- システムが遅くなる

**解決方法:**
- バッチサイズを小さくする: `--batch-size 200`
- 並列数を減らす: `--concurrency 3`
- コレクションを個別に実行

## パフォーマンス最適化のヒント

### 1. バッチサイズの調整

- **小さいバッチ（200-300）**: メモリ使用量が少ない、エラー時の影響が小さい
- **標準バッチ（500）**: Firestoreの制限内で最大効率
- **注意**: 500を超えるとエラーになります

### 2. 並列数の調整

- **少ない並列数（3-5）**: 安定性重視、レート制限回避
- **標準並列数（5-7）**: バランスの取れた設定
- **多い並列数（8-10）**: 高速化重視、レート制限に注意

### 3. 実行時間の見積もり

- **1バッチあたり**: 約2-5秒（ネットワーク状況による）
- **10,000件**: 約20-50バッチ = 約2-5分
- **100,000件**: 約200-500バッチ = 約20-50分
- **1,000,000件**: 約2,000-5,000バッチ = 約3-8時間

## 実行前のチェックリスト

- [ ] バックアップを取得（本番環境の場合）
- [ ] ドライランで統計を確認
- [ ] 認証情報が正しく設定されている
- [ ] 十分な実行時間を確保
- [ ] ネットワーク接続が安定している

## 注意事項

1. **本番環境での実行**: 必ずバックアップを取得してから実行してください
2. **実行時間**: 大量のデータの場合、数時間かかる可能性があります
3. **レート制限**: Firestoreのレート制限に注意してください
4. **中断と再開**: スクリプトは中断しても再実行可能です（既に更新済みのドキュメントはスキップされます）

## 関連ファイル

- `backend/scripts/backfill_hasAnyWebPresence_optimized.py`: メインスクリプト
- `backend/scripts/backfill_hasAnyWebPresence.py`: 元のスクリプト（シーケンシャル処理）

## サポート

問題が発生した場合は、以下を確認してください：

1. エラーメッセージの全文
2. 実行時のオプション
3. データ量（ドキュメント数）
4. ネットワーク環境
