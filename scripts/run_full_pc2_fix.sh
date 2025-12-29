#!/bin/bash
# PC2（逆方向実行） - 全件取得（修正版）
# null_fields_detailed配下のCSVファイルを対象に全件取得

cd "/Users/harukishiroyama/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"

# 環境変数の設定
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harukishiroyama/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export FAST_MODE=true
export PARALLEL_WORKERS=8
export SLEEP_MS=200
export PAGE_TIMEOUT=10000
export NAVIGATION_TIMEOUT=12000
export SKIP_ON_ERROR=true
export REVERSE_ORDER=true
# 途中から再開する場合、最後に処理した企業IDを指定
export START_FROM_COMPANY_ID='1766040597448037783'
# Node.jsのヒープサイズを12GBに設定
export NODE_OPTIONS="--max-old-space-size=12288"
# 全件取得のため、LIMITとSUCCESS_LIMITは設定しない
unset LIMIT
unset SUCCESS_LIMIT

LOG_FILE="logs/full_all_reverse_$(date +%Y%m%d_%H%M%S).log"

echo "=========================================="
echo "PC2（逆方向）全件取得を開始します"
echo "対象CSV: null_fields_detailed配下の全CSVファイル"
echo "ログファイル: $LOG_FILE"
echo "実行モード: 逆順実行（企業IDを大きい順から処理）"
if [ -n "$START_FROM_COMPANY_ID" ]; then
  echo "開始企業ID: ${START_FROM_COMPANY_ID} (途中から再開)"
fi
echo "並列処理数: ${PARALLEL_WORKERS}並列"
echo "待機時間: ${SLEEP_MS}ms"
echo "高速化モード: 有効（精度を保ちながら最大限高速化）"
echo "メモリ設定: 12GB（メモリ不足対策）"
echo "=========================================="
echo ""

# ts-nodeの代わりに、TypeScriptをコンパイルしてから実行
# または、tsxをローカルにインストールして使用
if command -v tsx &> /dev/null; then
  echo "tsxを使用して実行します..."
  tsx scripts/scrape_extended_fields.ts 2>&1 | tee "$LOG_FILE"
elif [ -f "node_modules/.bin/tsx" ]; then
  echo "ローカルのtsxを使用して実行します..."
  ./node_modules/.bin/tsx scripts/scrape_extended_fields.ts 2>&1 | tee "$LOG_FILE"
else
  echo "tsxが見つかりません。ts-nodeを使用します..."
  npx ts-node scripts/scrape_extended_fields.ts 2>&1 | tee "$LOG_FILE"
fi

EXIT_CODE=${PIPESTATUS[0]}

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "=== 取得結果サマリー ==="
echo "═══════════════════════════════════════════════════════════════"
echo ""

if [ -f "$LOG_FILE" ]; then
  echo "【取得できたドキュメントIDとフィールド】"
  echo ""
  grep -E "保存フィールド一覧:" "$LOG_FILE" | sed 's/.*\[\([0-9]*\)\].*保存フィールド一覧: \(.*\)/\1: \2/'
  
  echo ""
  echo "═══════════════════════════════════════════════════════════════"
  echo "=== 詳細な取得結果 ==="
  echo "═══════════════════════════════════════════════════════════════"
  ./scripts/show_results.sh "$LOG_FILE"
else
  echo "⚠️  ログファイルが見つかりません: $LOG_FILE"
fi

exit $EXIT_CODE

