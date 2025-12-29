#!/bin/bash
cd "/Users/harukishiroyama/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail" && \
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harukishiroyama/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' && \
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
grep -E "保存フィールド一覧:" "$LOG_FILE" | sed 's/.*\[\([0-9]*\)\].*保存フィールド一覧: \(.*\)/\1: \2/' && \
echo "" && \
echo "=== 詳細な取得結果 ===" && \
./scripts/show_results.sh "$LOG_FILE"
