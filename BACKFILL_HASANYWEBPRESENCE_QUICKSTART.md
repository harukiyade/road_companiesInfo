# hasAnyWebPresence バックフィル クイックスタートガイド

## 最短手順（5分で開始）

### 1. 前提条件の確認

```bash
# Python 3.8以上がインストールされているか確認
python --version

# 必要なパッケージをインストール
pip install firebase-admin google-cloud-firestore
```

### 2. Firebase認証情報の設定

```bash
# 方法A: 環境変数で設定（推奨）
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json

# 方法B: スクリプト実行時に指定（後述）
```

### 3. スクリプトの配置確認

```bash
# スクリプトが存在するか確認
ls -la backend/scripts/backfill_hasAnyWebPresence_optimized.py

# 実行権限を付与
chmod +x backend/scripts/backfill_hasAnyWebPresence_optimized.py
```

### 4. ドライランで確認（必須）

```bash
cd /path/to/your/project
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection both --dry-run
```

### 5. 実際の実行

```bash
# 基本実行
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection both --concurrency 5 --batch-size 500

# 認証情報ファイルを指定する場合
python backend/scripts/backfill_hasAnyWebPresence_optimized.py \
  --collection both \
  --cred-path /path/to/serviceAccountKey.json \
  --concurrency 5 \
  --batch-size 500
```

## よく使うコマンド

### ドライラン（更新なし）

```bash
# 両方のコレクション
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection both --dry-run

# companies_index のみ
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection companies_index --dry-run

# companies_new のみ
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection companies_new --dry-run
```

### 実際の実行

```bash
# 標準設定（推奨）
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection both --concurrency 5 --batch-size 500

# 高速化（レート制限に注意）
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection both --concurrency 10 --batch-size 500

# 安定性重視（レート制限回避）
python backend/scripts/backfill_hasAnyWebPresence_optimized.py --collection both --concurrency 3 --batch-size 300
```

## トラブルシューティング（よくある問題）

### 認証エラー

```bash
# エラー: DefaultCredentialsError
# 解決: 認証情報ファイルを指定
python backend/scripts/backfill_hasAnyWebPresence_optimized.py \
  --collection both \
  --cred-path /path/to/serviceAccountKey.json
```

### タイムアウトエラー

```bash
# エラー: DeadlineExceeded
# 解決: バッチサイズを小さく、並列数を減らす
python backend/scripts/backfill_hasAnyWebPresence_optimized.py \
  --collection both \
  --concurrency 3 \
  --batch-size 300
```

### レート制限エラー

```bash
# エラー: ResourceExhausted
# 解決: 並列数を減らす
python backend/scripts/backfill_hasAnyWebPresence_optimized.py \
  --collection both \
  --concurrency 3 \
  --batch-size 500
```

## 実行時間の目安

| ドキュメント数 | バッチ数（500件/バッチ） | 推定時間（並列数5） |
|--------------|------------------------|-------------------|
| 10,000件 | 20バッチ | 約2-5分 |
| 100,000件 | 200バッチ | 約20-50分 |
| 500,000件 | 1,000バッチ | 約2-4時間 |
| 1,000,000件 | 2,000バッチ | 約4-8時間 |

## チェックリスト

実行前:
- [ ] バックアップ取得（本番環境）
- [ ] ドライランで確認
- [ ] 認証情報設定
- [ ] 十分な実行時間確保

実行中:
- [ ] 進捗を確認
- [ ] エラーログを監視
- [ ] ネットワーク接続を確認

実行後:
- [ ] 統計情報を確認
- [ ] エラー件数を確認
- [ ] 必要に応じて再実行

## 詳細情報

詳細な説明は `BACKFILL_HASANYWEBPRESENCE_README.md` を参照してください。
