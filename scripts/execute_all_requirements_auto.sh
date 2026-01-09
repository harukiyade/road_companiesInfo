#!/bin/bash
# 全8要件を自動実行（確認なし）

set -e  # エラーで停止

echo ""
echo "========================================"
echo "🚀 全8要件 自動実行スクリプト"
echo "========================================"
echo ""

# 環境変数チェック
if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
  echo "❌ エラー: GOOGLE_APPLICATION_CREDENTIALS が設定されていません"
  echo ""
  echo "設定方法:"
  echo "  export GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json"
  exit 1
fi

echo "📋 実行内容:"
echo "  ✅ 要件2: 法人番号バリデーション（実装済み）"
echo "  📝 要件3: タイプD, E の CSV修正"
echo "  📝 要件4: タイプG の CSV修正"
echo "  📝 要件5: タイプH（既に完了）"
echo "  📝 要件6: タイプI の CSV修正"
echo "  📝 要件7, 8: タイプJ の CSV修正"
echo "  🔄 要件1: タイプB の重複統合"
echo ""
echo "⚠️  自動実行モード: すべて自動で実行します"
echo ""

# ステップ1: CSV修正
echo "=========================================="
echo "【ステップ1】CSV修正"
echo "=========================================="
python3 scripts/fix_all_csv_requirements.py

# ステップ2: 修正ファイルを元のファイルと置き換え
echo ""
echo "=========================================="
echo "【ステップ2】修正ファイルの置き換え"
echo "=========================================="
echo ""

cd csv
for f in *_fixed.csv; do
  if [ -f "$f" ]; then
    original="${f/_fixed/}"
    echo "  ✅ $f → $original"
    mv "$f" "$original"
  fi
done
cd ..

echo ""
echo "✅ 置き換え完了"

# ステップ3: バックフィル実行（タイプ別）
echo ""
echo "=========================================="
echo "【ステップ3】バックフィル実行"
echo "=========================================="
echo ""

# タイプD
echo "📦 タイプD バックフィル中..."
npx ts-node scripts/backfill_companies_from_csv.ts \
  csv/111.csv csv/112.csv csv/113.csv csv/114.csv csv/115.csv

# タイプE
echo ""
echo "📦 タイプE バックフィル中..."
npx ts-node scripts/backfill_companies_from_csv.ts \
  csv/116.csv csv/117.csv

# タイプG
echo ""
echo "📦 タイプG バックフィル中..."
npx ts-node scripts/backfill_companies_from_csv.ts \
  csv/127.csv csv/128.csv

# タイプI
echo ""
echo "📦 タイプI バックフィル中..."
npx ts-node scripts/backfill_companies_from_csv.ts \
  csv/132.csv

# タイプJ
echo ""
echo "📦 タイプJ バックフィル中..."
npx ts-node scripts/backfill_companies_from_csv.ts \
  csv/133.csv csv/134.csv csv/135.csv csv/136.csv

echo ""
echo "✅ バックフィル完了"

# ステップ4: 重複統合（タイプB）
echo ""
echo "=========================================="
echo "【ステップ4】タイプB 重複統合"
echo "=========================================="
echo ""
echo "🔄 重複統合を実行中..."
npx ts-node scripts/dedupe_type_b_companies.ts

echo ""
echo "✅ 重複統合完了"

# 完了
echo ""
echo "=========================================="
echo "🎉 全要件の実行が完了しました！"
echo "=========================================="
echo ""
echo "📌 次のステップ:"
echo "  1. データ確認:"
echo "     npx ts-node scripts/verify_csv_import_by_type.ts"
echo ""
echo "  2. DBブラウザーで確認:"
echo "     npx ts-node scripts/db_browser.ts"
echo ""
echo "  3. 特定企業の確認:"
echo "     npx ts-node scripts/quick_query.ts name \"企業名\""
echo ""
echo "=========================================="
echo ""

