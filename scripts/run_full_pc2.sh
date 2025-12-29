#!/bin/bash
# PC2（逆方向実行） - 全件取得
# null_fields_detailed/null_fields_detailed_2025-12-19T*.csv を対象に全件取得

cd "/Users/harukishiroyama/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"

export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harukishiroyama/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export FAST_MODE=true
export PARALLEL_WORKERS=8
export SLEEP_MS=200
export PAGE_TIMEOUT=10000
export NAVIGATION_TIMEOUT=12000
export SKIP_ON_ERROR=true
export REVERSE_ORDER=true
# 途中から再開する場合、最後に処理した企業IDを指定
# ターミナル出力から最後に処理されていた企業ID: 3182916
# 逆順実行の場合、このID以下の最大のIDから処理を再開します
export START_FROM_COMPANY_ID='3182916'
# Node.jsのヒープサイズを12GBに設定（メモリ不足エラー対策：8GBでは不足していたため増加）
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

# tsxを使用（型チェックを緩和）
npx tsx scripts/scrape_extended_fields.ts 2>&1 | tee "$LOG_FILE"

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

