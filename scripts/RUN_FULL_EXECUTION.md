# 全体実行コマンド（2台PC並行実行）

## 実行前の準備

1. 最新のCSVファイルが`null_fields_detailed`配下にあることを確認
2. Firebase認証情報が正しく設定されていることを確認

## PC1（順方向実行）

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail" && \
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' && \
export FAST_MODE=true && \
export PARALLEL_WORKERS=3 && \
export SLEEP_MS=400 && \
export SKIP_ON_ERROR=true && \
export REVERSE_ORDER=false && \
LOG_FILE="logs/full_execution_forward_$(date +%Y%m%d_%H%M%S).log" && \
npx ts-node scripts/scrape_extended_fields.ts 2>&1 | tee "$LOG_FILE" && \
echo "" && \
echo "=== 取得結果サマリー ===" && \
echo "" && \
echo "【取得できたドキュメントIDとフィールド】" && \
echo "" && \
grep -E "保存フィールド一覧:|✓.*:" "$LOG_FILE" | grep -B 2 "保存フィールド一覧" | grep -E "\[.*\]|保存フィールド一覧|✓" | head -100 && \
echo "" && \
echo "=== 詳細な取得結果 ===" && \
grep -E "保存フィールド一覧:" "$LOG_FILE" | while read line; do \
  doc_id=$(echo "$line" | grep -oE "\[[0-9]+\]" | head -1); \
  if [ -n "$doc_id" ]; then \
    echo ""; \
    echo "$doc_id で取得したフィールド:"; \
    sed -n "/$doc_id/,/保存フィールド一覧:/p" "$LOG_FILE" | grep -E "✓.*:" | sed 's/^/  /'; \
  fi; \
done
```

## PC2（逆方向実行）

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail" && \
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' && \
export FAST_MODE=true && \
export PARALLEL_WORKERS=3 && \
export SLEEP_MS=400 && \
export SKIP_ON_ERROR=true && \
export REVERSE_ORDER=true && \
LOG_FILE="logs/full_execution_reverse_$(date +%Y%m%d_%H%M%S).log" && \
npx ts-node scripts/scrape_extended_fields.ts 2>&1 | tee "$LOG_FILE" && \
echo "" && \
echo "=== 取得結果サマリー ===" && \
echo "" && \
echo "【取得できたドキュメントIDとフィールド】" && \
echo "" && \
grep -E "保存フィールド一覧:|✓.*:" "$LOG_FILE" | grep -B 2 "保存フィールド一覧" | grep -E "\[.*\]|保存フィールド一覧|✓" | head -100 && \
echo "" && \
echo "=== 詳細な取得結果 ===" && \
grep -E "保存フィールド一覧:" "$LOG_FILE" | while read line; do \
  doc_id=$(echo "$line" | grep -oE "\[[0-9]+\]" | head -1); \
  if [ -n "$doc_id" ]; then \
    echo ""; \
    echo "$doc_id で取得したフィールド:"; \
    sed -n "/$doc_id/,/保存フィールド一覧:/p" "$LOG_FILE" | grep -E "✓.*:" | sed 's/^/  /'; \
  fi; \
done
```

## 設定の説明

- **FAST_MODE=true**: 高速化モードを有効化（待機時間とタイムアウトを最適化）
- **PARALLEL_WORKERS=3**: 3並列で処理（精度を落とさない範囲で高速化）
- **SLEEP_MS=400**: リクエスト間隔400ms（精度を落とさない範囲で高速化）
- **SKIP_ON_ERROR=true**: エラー時にスキップして続行
- **REVERSE_ORDER**: PC1はfalse、PC2はtrue（逆順実行）

## 実行状況の確認

### ログファイルの確認

```bash
# 最新のログファイルを確認
latest_log=$(ls -t logs/scrape_extended_fields_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  echo "=== 最新ログ: $latest_log ==="
  tail -50 "$latest_log"
fi
```

### 進捗確認

```bash
# 処理済み企業数と成功数を確認
latest_log=$(ls -t logs/scrape_extended_fields_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  echo "=== 進捗状況 ==="
  grep -E "処理企業数|更新数|成功数|成功カウント" "$latest_log" | tail -10
fi
```

## 注意事項

1. **2台のPCで同時実行する場合**:
   - 同じFirestoreデータベースにアクセスするため、競合が発生する可能性があります
   - ただし、各ドキュメントは個別に更新されるため、基本的には問題ありません
   - 同じ企業を同時に処理する可能性がありますが、Firestoreのトランザクションにより安全に処理されます

2. **精度を維持するための設定**:
   - `SLEEP_MS=400`: 400ms以上の間隔を確保（サイトへの負荷を軽減）
   - `PARALLEL_WORKERS=3`: 3並列まで（過度な並列化を避ける）
   - バリデーション関数により、不正なデータは除外されます

3. **エラー時の動作**:
   - `SKIP_ON_ERROR=true`により、エラーが発生した企業はスキップして続行します
   - エラーログは詳細に記録されるため、後で確認できます

## 実行時間の目安

- 企業数: 約340件（CSVから読み込まれる企業数による）
- 1企業あたりの処理時間: 約10-30秒（サイトの応答速度による）
- 並列処理（3並列）により、実効速度は約3倍
- **推定実行時間**: 約2-4時間（2台で並行実行の場合、約1-2時間）

## 実行中の監視

```bash
# リアルタイムでログを監視
latest_log=$(ls -t logs/scrape_extended_fields_*.log logs/full_execution_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  tail -f "$latest_log"
fi
```

## 実行結果の確認

### 方法1: 自動表示（実行コマンドに含まれています）

実行コマンドを実行すると、完了後に自動的に取得結果が表示されます。

### 方法2: 手動で結果を表示

```bash
# 最新のログファイルから結果を表示
./scripts/show_results.sh

# または、特定のログファイルを指定
./scripts/show_results.sh logs/full_execution_forward_20251219_120000.log
```

### 方法3: 簡易確認

```bash
# 取得できたドキュメントIDとフィールドの一覧のみ表示
latest_log=$(ls -t logs/scrape_extended_fields_*.log logs/full_execution_*.log 2>/dev/null | head -1)
if [ -n "$latest_log" ]; then
  echo "=== 取得結果 ==="
  grep -E "保存フィールド一覧:" "$latest_log" | sed 's/.*\[\([0-9]*\)\].*保存フィールド一覧: \(.*\)/\1: \2/'
fi
```

