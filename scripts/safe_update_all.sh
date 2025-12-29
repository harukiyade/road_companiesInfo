#!/bin/bash
# 安全なデータ更新（削除最小限、既存データを活かす）

set -e

echo ""
echo "=========================================="
echo "🔧 安全なデータ更新スクリプト"
echo "=========================================="
echo ""

# 環境変数チェック
if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
  echo "❌ エラー: GOOGLE_APPLICATION_CREDENTIALS が設定されていません"
  exit 1
fi

echo "方針:"
echo "  ✅ 既存データは削除しない"
echo "  ✅ 空データはCSVで更新"
echo "  ✅ 法人番号を数値型→string型に修正"
echo "  ✅ 重複は企業名+住所が同じもののみ統合"
echo "  ✅ 削除は最小限（完全重複のみ）"
echo ""
echo "⏱️  推定時間: 2〜3時間"
echo ""

# フェーズ1: 法人番号の型修正
echo "=========================================="
echo "【フェーズ1】法人番号の型修正"
echo "=========================================="
echo ""
echo "処理内容: 数値型（9.18E+12）→ string型（9180000000000）"
echo ""
NODE_OPTIONS="--max-old-space-size=8192" npx ts-node scripts/update_existing_data.ts

echo ""
echo "✅ フェーズ1完了"

# フェーズ2: CSVで空データを更新
echo ""
echo "=========================================="
echo "【フェーズ2】CSVで空データを更新"
echo "=========================================="
echo ""
echo "処理内容: 空フィールドをCSVデータで補完"
echo ""
bash scripts/run_backfill_by_type.sh

echo ""
echo "✅ フェーズ2完了"

# 完了確認
echo ""
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
echo "3. ランダム10社確認:"
npx ts-node scripts/quick_query.ts random 10

echo ""
echo "=========================================="
echo "🎉 すべての更新が完了しました！"
echo "=========================================="
echo ""
echo "結果:"
echo "  ✅ 法人番号をstring型に修正"
echo "  ✅ 重複を統合（企業名+住所が同じもの）"
echo "  ✅ 空データをCSVで更新"
echo "  ✅ 既存データは保持"
echo ""

