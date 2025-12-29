#!/bin/bash
# 
# 問題のあるCSVファイルを再インポートするスクリプト
#
# 使い方:
#   ./scripts/reimport_csv_files.sh <report.json>
#
# 例:
#   ./scripts/reimport_csv_files.sh invalid_company_names_report_1234567890.json

set -e

REPORT_FILE="$1"

if [ -z "$REPORT_FILE" ]; then
  echo "❌ エラー: レポートファイルのパスが指定されていません"
  echo ""
  echo "使用方法:"
  echo "  ./scripts/reimport_csv_files.sh <report.json>"
  exit 1
fi

if [ ! -f "$REPORT_FILE" ]; then
  echo "❌ エラー: レポートファイルが見つかりません: $REPORT_FILE"
  exit 1
fi

# レポートからCSVファイルリストを抽出
CSV_FILES=$(node -e "
  const report = require('./$REPORT_FILE');
  const files = Object.keys(report.summary.byFile)
    .filter(f => f !== '(不明)')
    .sort();
  console.log(files.join('\n'));
")

if [ -z "$CSV_FILES" ]; then
  echo "⚠️  再インポートが必要なCSVファイルが見つかりませんでした"
  exit 0
fi

echo "📋 再インポートが必要なCSVファイル:"
echo "$CSV_FILES" | while read file; do
  if [ -n "$file" ]; then
    count=$(node -e "const r = require('./$REPORT_FILE'); console.log(r.summary.byFile['$file'] || 0)")
    echo "  - $file ($count件)"
  fi
done

echo ""
read -p "これらのCSVファイルを再インポートしますか？ (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
  echo "キャンセルしました"
  exit 0
fi

# 各CSVファイルを再インポート
echo "$CSV_FILES" | while read file; do
  if [ -n "$file" ]; then
    csv_path="csv/$file"
    if [ -f "$csv_path" ]; then
      echo ""
      echo "📄 インポート中: $csv_path"
      GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS:-./serviceAccountKey.json} \
        npx ts-node scripts/import_companies_from_csv.ts "$csv_path"
    else
      echo "⚠️  ファイルが見つかりません: $csv_path"
    fi
  fi
done

echo ""
echo "✅ 再インポート完了"
