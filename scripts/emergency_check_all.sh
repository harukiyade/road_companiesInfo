#!/bin/bash
# 緊急確認: データの状態をすべてチェック

set -e

echo ""
echo "=========================================="
echo "🚨 緊急確認: データ状態チェック"
echo "=========================================="
echo ""

# 環境変数チェック
if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
  echo "❌ エラー: GOOGLE_APPLICATION_CREDENTIALS が設定されていません"
  echo ""
  echo "設定方法:"
  echo "  export GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json"
  exit 1
fi

# 1. 総件数確認
echo "【1】総件数確認"
echo "=========================================="
npx ts-node scripts/quick_query.ts count

# 2. 丹羽興業株式会社の確認
echo ""
echo "【2】丹羽興業株式会社の確認"
echo "=========================================="
npx ts-node scripts/emergency_check.ts

# 3. ランダムサンプル確認
echo ""
echo "【3】ランダム5社サンプル"
echo "=========================================="
npx ts-node scripts/quick_query.ts random 5

echo ""
echo "=========================================="
echo "✅ 緊急確認完了"
echo "=========================================="
echo ""

