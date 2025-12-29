#!/bin/bash

# PC1用: 上から実行（順順）

cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"

export FIREBASE_SERVICE_ACCOUNT_KEY="/Users/harumacmini/programming/ma-tool-albert/ma_tool/config/serviceAccountKey.json"
export START_FILE=1
export END_FILE=10000
export REVERSE=false
export CONCURRENT_REQUESTS=5
export CONCURRENT_FIELDS=3

echo "🚀 PC1: 上から実行を開始します"
echo "   開始ファイル: $START_FILE"
echo "   終了ファイル: $END_FILE"
echo "   実行方向: 上から（順順）"
echo "   並列リクエスト数: $CONCURRENT_REQUESTS"
echo "   並列フィールド処理数: $CONCURRENT_FIELDS"
echo ""

npx tsx scripts/fill_null_fields_from_csv_enhanced.ts 2>&1 | tee fill_null_fields_forward.log

