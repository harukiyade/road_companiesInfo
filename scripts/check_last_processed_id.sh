#!/bin/bash
# 最後に処理された企業IDを確認するスクリプト

cd "/Users/harukishiroyama/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"

echo "=========================================="
echo "最後に処理された企業IDを確認中..."
echo "=========================================="
echo ""

# 最新のログファイルを取得
LATEST_LOG=$(ls -t logs/scrape_extended_fields_*.log logs/full_all_*.log 2>/dev/null | head -1)

if [ -z "$LATEST_LOG" ]; then
  echo "❌ ログファイルが見つかりません"
  exit 1
fi

echo "📁 確認中のログファイル: $LATEST_LOG"
echo ""

# 最後に処理された企業IDを取得（「保存フィールド一覧」が含まれる行から）
LAST_ID=$(grep -E "\[[0-9]+\].*保存フィールド一覧" "$LATEST_LOG" | tail -1 | grep -oE "\[[0-9]+\]" | head -1 | tr -d '[]')

if [ -z "$LAST_ID" ]; then
  echo "⚠️  処理済みの企業IDが見つかりませんでした"
  echo ""
  echo "ログファイルの最後の数行を表示します:"
  tail -20 "$LATEST_LOG"
else
  echo "✅ 最後に処理された企業ID: $LAST_ID"
  echo ""
  echo "この企業IDから再開する場合は、以下のコマンドを実行してください:"
  echo ""
  echo "export START_FROM_COMPANY_ID='$LAST_ID'"
  echo "./scripts/run_full_pc2.sh"
  echo ""
  echo "または、スクリプト内の START_FROM_COMPANY_ID を '$LAST_ID' に設定してください"
fi

echo ""
echo "=========================================="
echo "統計情報"
echo "=========================================="

# 処理企業数
TOTAL_PROCESSED=$(grep -E "処理企業数:" "$LATEST_LOG" | tail -1 | grep -oE "[0-9]+" | head -1)
echo "処理企業数: ${TOTAL_PROCESSED:-0}件"

# 更新数
TOTAL_UPDATED=$(grep -E "更新数:" "$LATEST_LOG" | tail -1 | grep -oE "[0-9]+" | head -1)
echo "更新数: ${TOTAL_UPDATED:-0}件"

# スキップ数
TOTAL_SKIPPED=$(grep -E "スキップ数:" "$LATEST_LOG" | tail -1 | grep -oE "[0-9]+" | head -1)
echo "スキップ数: ${TOTAL_SKIPPED:-0}件"

echo ""
echo "=========================================="
echo "最後に処理された10件の企業ID"
echo "=========================================="
grep -E "\[[0-9]+\].*保存フィールド一覧" "$LATEST_LOG" | tail -10 | grep -oE "\[[0-9]+\]" | tr -d '[]' | while read id; do
  echo "  - $id"
done

