# 全件取得実行コマンド（2台PC用）

## 概要

`null_fields_detailed/null_fields_detailed_2025-12-19T*.csv` を対象に、全件取得を実行します。

- **高速化**: `PARALLEL_WORKERS=4`, `SLEEP_MS=350`, `FAST_MODE=true`
- **精度維持**: タイムアウトや待機時間を適切に設定
- **2台PCで並列実行**: PC1は順方向、PC2は逆方向から処理

## 実行前の確認

1. CSVファイルが存在することを確認:
   ```bash
   ls -lh null_fields_detailed/null_fields_detailed_2025-12-19T*.csv
   ```

2. Firebase認証情報が正しく設定されていることを確認:
   ```bash
   echo $FIREBASE_SERVICE_ACCOUNT_KEY
   ```

## PC1（順方向実行）

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail" && ./scripts/run_full_pc1.sh
```

または、直接コマンドを実行:

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail" && \
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' && \
export FAST_MODE=true && \
export PARALLEL_WORKERS=4 && \
export SLEEP_MS=350 && \
export SKIP_ON_ERROR=true && \
export REVERSE_ORDER=false && \
unset LIMIT && \
unset SUCCESS_LIMIT && \
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

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail" && ./scripts/run_full_pc2.sh
```

または、直接コマンドを実行:

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail" && \
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' && \
export FAST_MODE=true && \
export PARALLEL_WORKERS=4 && \
export SLEEP_MS=350 && \
export SKIP_ON_ERROR=true && \
export REVERSE_ORDER=true && \
unset LIMIT && \
unset SUCCESS_LIMIT && \
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

## 実行中の確認方法

### 実行中のプロセスを確認

```bash
ps aux | grep "scrape_extended_fields" | grep -v grep
```

### 最新のログファイルを確認

```bash
# PC1のログ
latest_log=$(ls -t logs/full_all_forward_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  tail -50 "$latest_log"
fi

# PC2のログ
latest_log=$(ls -t logs/full_all_reverse_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  tail -50 "$latest_log"
fi
```

### リアルタイムでログを監視

```bash
# PC1のログ
latest_log=$(ls -t logs/full_all_forward_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  tail -f "$latest_log"
fi

# PC2のログ
latest_log=$(ls -t logs/full_all_reverse_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  tail -f "$latest_log"
fi
```

## 実行後の結果確認

実行完了後、自動的に以下が表示されます:

1. **取得できたドキュメントIDとフィールドの一覧**
   - 例: `[1000410]: companyUrl, phoneNumber`
   - 例: `[1000412]: contactFormUrl, clients, executives`

2. **詳細な取得結果**
   - 各ドキュメントで取得したフィールドと値の詳細
   - フィールド名と値が表示されます（長い値は切り詰められます）

3. **統計情報**
   - 処理企業数
   - 更新数
   - スキップ数
   - フィールド別の取得数

## 実行結果の手動確認

実行完了後、以下のコマンドで結果を再確認できます:

```bash
# PC1の結果を確認
latest_log=$(ls -t logs/full_all_forward_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  ./scripts/show_results.sh "$latest_log"
fi

# PC2の結果を確認
latest_log=$(ls -t logs/full_all_reverse_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  ./scripts/show_results.sh "$latest_log"
fi
```

## 注意事項

1. **実行時間**: CSVファイルのサイズによっては、処理に数時間から数日かかる場合があります
2. **ネットワーク**: 安定したネットワーク接続が必要です
3. **リソース**: メモリとCPUリソースを十分に確保してください
4. **ログファイル**: ログファイルは自動的に `logs/` ディレクトリに保存されます
5. **中断と再開**: `SKIP_ON_ERROR=true` が設定されているため、エラーが発生しても処理は続行されます

## トラブルシューティング

### 実行が途中で止まった場合

1. ログファイルを確認してエラーを特定
2. エラーが発生した企業IDを確認
3. 必要に応じて `START_FROM_COMPANY_ID` を設定して再開

```bash
export START_FROM_COMPANY_ID=1000500  # 最後に処理した企業IDの次のID
```

### ログファイルが大きくなりすぎた場合

ログファイルは自動的にローテーションされません。定期的に確認し、必要に応じてアーカイブしてください。

