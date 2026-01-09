#!/bin/bash
# すべてのデータ問題を一気に修正

set -e

echo ""
echo "=========================================="
echo "🚨 緊急データ修正スクリプト"
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

echo "発見された問題:"
echo "  ❌ データが420万件（予想32万件の13倍）"
echo "  ❌ 丹羽興業株式会社が11件存在（住所なし10件）"
echo "  ❌ 法人番号が数値型（9.18E+12）"
echo "  ❌ 空データが大量に残存"
echo ""
echo "修正内容:"
echo "  ✅ 空データを削除（主要フィールドがnullのもの）"
echo "  ✅ 法人番号を数値型→string型に修正"
echo "  ✅ 重複を統合"
echo ""
echo "⚠️  推定削除数: 350万〜400万件"
echo "⚠️  推定残存数: 30万〜50万件"
echo ""
echo "⏱️  推定時間: 2〜3時間"
echo ""

# フェーズ1: 空データ削除
echo "=========================================="
echo "【フェーズ1】空データの削除"
echo "=========================================="
echo ""
NODE_OPTIONS="--max-old-space-size=8192" npx ts-node scripts/emergency_cleanup.ts

echo ""
echo "✅ フェーズ1完了"
echo ""

# フェーズ2: 重複統合
echo "=========================================="
echo "【フェーズ2】重複統合"
echo "=========================================="
echo ""
NODE_OPTIONS="--max-old-space-size=8192" npx ts-node scripts/dedupe_type_b_companies_batch.ts

echo ""
echo "✅ フェーズ2完了"
echo ""

# 完了確認
echo "=========================================="
echo "【完了】最終確認"
echo "=========================================="
echo ""

echo "1. 総件数確認:"
npx ts-node scripts/quick_query.ts count

echo ""
echo "2. 丹羽興業株式会社の確認:"
npx ts-node scripts/emergency_check.ts

echo ""
echo "=========================================="
echo "🎉 すべての修正が完了しました！"
echo "=========================================="
echo ""

