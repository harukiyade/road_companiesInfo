#!/bin/bash

# PC1用: foundValueをクリアして上から再実行

cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"

export FIREBASE_SERVICE_ACCOUNT_KEY="/Users/harumacmini/programming/ma-tool-albert/ma_tool/config/serviceAccountKey.json"
export START_FILE=${START_FILE:-1}
export END_FILE=${END_FILE:-10000}

# clear_found_values.shに環境変数を渡す
export START_FILE
export END_FILE

echo "🔄 PC1: foundValueをクリアして上から再実行します"
echo ""

# 1. foundValueをクリア
echo "📋 ステップ1: foundValue列をクリア中..."
./scripts/clear_found_values.sh

echo ""
echo "📋 ステップ2: 上から実行を開始します"
echo "   開始ファイル: $START_FILE"
echo "   終了ファイル: $END_FILE"
echo "   実行方向: 上から（順順）"
echo ""

export REVERSE=false
export CONCURRENT_REQUESTS=5
export CONCURRENT_FIELDS=3

npx tsx scripts/fill_null_fields_from_csv_enhanced.ts 2>&1 | tee fill_null_fields_forward.log

