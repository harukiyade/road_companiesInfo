# 全件取得実行コマンド（2台PC並行実行）

## 実行前の確認

1. `null_fields_detailed/null_fields_detailed_2025-12-19T*.csv` が存在することを確認
2. Firebase認証情報が正しく設定されていることを確認

## 重要：全件取得のため制限を設定しない

以下のコマンドでは、`LIMIT`と`SUCCESS_LIMIT`を**設定していません**。これにより、CSVファイルに記載されている全企業を処理します。

## PC1（順方向実行）

### 方法1: スクリプトファイルを使用（推奨）

```bash
./scripts/run_full_pc1.sh
```

### 方法2: 直接コマンドを実行

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail" && \
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' && \
export FAST_MODE=true && \
export PARALLEL_WORKERS=4 && \
export SLEEP_MS=350 && \
export SKIP_ON_ERROR=true && \
export REVERSE_ORDER=false && \
LOG_FILE="logs/full_all_forward_$(date +%Y%m%d_%H%M%S).log" && \
npx ts-node scripts/scrape_extended_fields.ts 2>&1 | tee "$LOG_FILE" && \
echo "" && \
echo "=== 取得結果サマリー ===" && \
echo "" && \
echo "【取得できたドキュメントIDとフィールド】" && \
grep -E "保存フィールド一覧:" "$LOG_FILE" | sed 's/.*\[\([0-9]*\)\].*保存フィールド一覧: \(.*\)/\1: \2/' && \
echo "" && \
echo "=== 詳細な取得結果 ===" && \
./scripts/show_results.sh "$LOG_FILE"
```

## PC2（逆方向実行）

### 方法1: スクリプトファイルを使用（推奨）

```bash
./scripts/run_full_pc2.sh
```

### 方法2: 直接コマンドを実行

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail" && \
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' && \
export FAST_MODE=true && \
export PARALLEL_WORKERS=4 && \
export SLEEP_MS=350 && \
export SKIP_ON_ERROR=true && \
export REVERSE_ORDER=true && \
LOG_FILE="logs/full_all_reverse_$(date +%Y%m%d_%H%M%S).log" && \
npx ts-node scripts/scrape_extended_fields.ts 2>&1 | tee "$LOG_FILE" && \
echo "" && \
echo "=== 取得結果サマリー ===" && \
echo "" && \
echo "【取得できたドキュメントIDとフィールド】" && \
grep -E "保存フィールド一覧:" "$LOG_FILE" | sed 's/.*\[\([0-9]*\)\].*保存フィールド一覧: \(.*\)/\1: \2/' && \
echo "" && \
echo "=== 詳細な取得結果 ===" && \
./scripts/show_results.sh "$LOG_FILE"
```

## 設定の説明

- **FAST_MODE=true**: 高速化モードを有効化
- **PARALLEL_WORKERS=4**: 4並列で処理（精度を維持しつつ高速化）
- **SLEEP_MS=350**: リクエスト間隔350ms（精度を維持しつつ高速化）
- **SKIP_ON_ERROR=true**: エラー時にスキップして続行
- **REVERSE_ORDER**: PC1は`false`（順方向）、PC2は`true`（逆方向）
- **LIMIT/SUCCESS_LIMIT**: 設定なし（全件処理）

## 対象CSVファイル

- 自動的に `null_fields_detailed_2025-12-19T02-50-47.csv`（最新のタイムスタンプファイル）が選択されます
- このファイルに記載されている全企業が処理対象となります

## 実行中の監視

```bash
# リアルタイムでログを監視
latest_log=$(ls -t logs/full_all_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  tail -f "$latest_log"
fi
```

## 進捗確認

```bash
# 処理済み企業数と成功数を確認
latest_log=$(ls -t logs/full_all_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  echo "=== 進捗状況 ==="
  grep -E "処理企業数|更新数|成功数|成功カウント|CSVから読み込んだ企業数" "$latest_log" | tail -10
fi
```

## 実行時間の目安

- CSVファイルに記載されている企業数による（通常は数万〜数十万件）
- 1企業あたりの処理時間: 約10-30秒
- 並列処理（4並列）により、実効速度は約4倍
- **2台で並行実行の場合**: 処理時間は約半分に短縮

## 実行結果の確認

実行完了後、自動的に以下の情報が表示されます：

1. **取得できたドキュメントIDとフィールドの一覧**
   - 例: `1000410: companyUrl`
   - 例: `1000412: contactFormUrl, clients`

2. **詳細な取得結果（各ドキュメントごと）**
   - 各ドキュメントで取得したフィールドと値の詳細

3. **統計情報**
   - 処理企業数
   - 更新数
   - スキップ数
   - フィールド別の取得数

## 注意事項

1. **全件処理のため、実行時間が長くなる可能性があります**
   - 必要に応じて、実行を中断して再開できます（`START_FROM_COMPANY_ID`を使用）

2. **2台のPCで同時実行する場合**
   - 同じFirestoreデータベースにアクセスしますが、各ドキュメントは個別に更新されるため安全です
   - 同じ企業を同時に処理する可能性がありますが、Firestoreのトランザクションにより安全に処理されます

3. **精度を維持するための設定**
   - `SLEEP_MS=350`: 350ms以上の間隔を確保
   - `PARALLEL_WORKERS=4`: 4並列まで（過度な並列化を避ける）
   - バリデーション関数により、不正なデータは除外されます

