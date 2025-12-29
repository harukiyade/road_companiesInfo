#!/bin/bash
# CSVサンプル表示スクリプト

CSV_FILE="$1"

if [ -z "$CSV_FILE" ]; then
  echo "使い方: bash scripts/show_csv_sample.sh csv/107.csv"
  exit 1
fi

if [ ! -f "$CSV_FILE" ]; then
  echo "エラー: ファイルが見つかりません: $CSV_FILE"
  exit 1
fi

FILENAME=$(basename "$CSV_FILE")

echo ""
echo "========================================"
echo "📄 $FILENAME のサンプル"
echo "========================================"
echo ""

# 行数表示
LINES=$(wc -l < "$CSV_FILE" | tr -d ' ')
echo "総行数: $((LINES - 1))行（ヘッダー除く）"
echo ""

# ヘッダー表示
echo "📋 ヘッダー:"
head -1 "$CSV_FILE" | tr ',' '\n' | nl -v 1
echo ""

# 最初の3行のデータ表示
echo "📊 データサンプル（最初の3行）:"
echo ""
tail -n +2 "$CSV_FILE" | head -3 | while IFS= read -r line; do
  echo "---"
  echo "$line" | cut -d',' -f1-5
  echo ""
done

echo "========================================"
echo ""
