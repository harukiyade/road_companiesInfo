#!/bin/bash
# ステップ4のみ実行: タイプB 重複統合

set -e

echo ""
echo "=========================================="
echo "【ステップ4】タイプB 重複統合"
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

echo "🔄 重複統合を実行中（バッチ処理版・メモリ効率的）..."
echo "   ※ 法人番号ごとに処理、67万件すべてをメモリにロードしません"
echo ""
echo "   処理内容:"
echo "   - 法人番号 + 住所が同じ企業を統合"
echo "   - 情報が最も充実しているドキュメントを正とする"
echo "   - 不足フィールドを他のドキュメントから補完"
echo "   - 統合後、重複ドキュメントを削除"
echo ""
echo "   推定時間: 30分〜1時間"
echo ""

NODE_OPTIONS="--max-old-space-size=8192" npx ts-node scripts/dedupe_type_b_companies_batch.ts

echo ""
echo "=========================================="
echo "✅ 重複統合完了"
echo "=========================================="
echo ""

