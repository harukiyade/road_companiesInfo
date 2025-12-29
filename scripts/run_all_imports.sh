#!/bin/bash
#
# 全CSVファイルをcompanies_newコレクションにインポートするスクリプト
#
# 使い方:
#   ./scripts/run_all_imports.sh [--dry-run] [--preprocess-only] [--skip-preprocess]
#
# オプション:
#   --dry-run         : 実際には書き込まず、何が行われるか確認
#   --preprocess-only : 前処理のみ実行（DBへのインポートはスキップ）
#   --skip-preprocess : 前処理をスキップ（既に前処理済みの場合）
#

set -e

cd "$(dirname "$0")/.."

DRY_RUN=""
PREPROCESS_ONLY=false
SKIP_PREPROCESS=false

for arg in "$@"; do
  case $arg in
    --dry-run)
      DRY_RUN="--dry-run"
      ;;
    --preprocess-only)
      PREPROCESS_ONLY=true
      ;;
    --skip-preprocess)
      SKIP_PREPROCESS=true
      ;;
  esac
done

echo "========================================"
echo "📊 CSV → companies_new インポートスクリプト"
echo "========================================"
echo ""

# ファイルグループ定義
TYPE_A="csv/7.csv csv/8.csv csv/9.csv csv/10.csv csv/11.csv csv/12.csv csv/13.csv csv/14.csv csv/15.csv csv/16.csv csv/17.csv csv/18.csv csv/19.csv csv/20.csv csv/21.csv csv/22.csv csv/25.csv csv/26.csv csv/27.csv csv/28.csv csv/29.csv csv/30.csv csv/31.csv csv/32.csv csv/33.csv csv/34.csv csv/35.csv csv/39.csv csv/52.csv csv/54.csv csv/55.csv csv/56.csv csv/57.csv csv/58.csv csv/59.csv csv/60.csv csv/61.csv csv/62.csv csv/63.csv csv/64.csv csv/65.csv csv/66.csv csv/67.csv csv/68.csv csv/69.csv csv/70.csv csv/71.csv csv/72.csv csv/73.csv csv/74.csv csv/75.csv csv/76.csv csv/77.csv csv/101.csv csv/104.csv"

TYPE_B="csv/1.csv csv/2.csv csv/53.csv csv/103.csv csv/106.csv csv/126.csv"

TYPE_C="csv/23.csv csv/78.csv csv/79.csv csv/80.csv csv/81.csv csv/82.csv csv/83.csv csv/84.csv csv/85.csv csv/86.csv csv/87.csv csv/88.csv csv/89.csv csv/90.csv csv/91.csv csv/92.csv csv/93.csv csv/94.csv csv/95.csv csv/96.csv csv/97.csv csv/98.csv csv/99.csv csv/100.csv csv/102.csv csv/105.csv"

TYPE_D="csv/36.csv csv/37.csv csv/38.csv csv/40.csv csv/41.csv csv/42.csv csv/43.csv csv/44.csv csv/45.csv csv/46.csv csv/47.csv csv/48.csv csv/49.csv csv/50.csv csv/107.csv csv/108.csv csv/109.csv csv/110.csv csv/111.csv csv/112.csv csv/113.csv csv/114.csv csv/115.csv csv/116.csv csv/117.csv csv/119.csv csv/24.csv csv/133.csv csv/134.csv"

TYPE_E="csv/3.csv csv/4.csv csv/5.csv csv/6.csv csv/118.csv csv/120.csv csv/121.csv csv/122.csv csv/123.csv csv/124.csv csv/125.csv"

TYPE_F_ENG="csv/130.csv csv/131.csv"
TYPE_F_JSON="csv/127.csv csv/128.csv"
TYPE_F_SPECIAL="csv/51.csv csv/129.csv csv/132.csv"

# ==============================
# Step 1: 前処理
# ==============================
if [ "$SKIP_PREPROCESS" = false ]; then
  echo "📝 Step 1: CSVファイルの前処理"
  echo "========================================"
  
  npx ts-node scripts/preprocess_csv.ts $DRY_RUN ./csv
  
  if [ "$PREPROCESS_ONLY" = true ]; then
    echo ""
    echo "✅ 前処理完了（--preprocess-only モード）"
    exit 0
  fi
  
  echo ""
fi

# ==============================
# Step 2: タイプ別インポート
# ==============================
echo "📥 Step 2: Firestoreへのインポート"
echo "========================================"
echo ""

# 前処理済みファイルがあれば優先して使用
CSV_DIR="./csv"
if [ -d "./csv_preprocessed" ] && [ "$(ls -A ./csv_preprocessed 2>/dev/null)" ]; then
  echo "ℹ️  前処理済みファイル (csv_preprocessed/) を使用します"
  CSV_DIR="./csv_preprocessed"
fi

run_import() {
  local TYPE_NAME=$1
  local FILES=$2
  
  # CSV_DIRに合わせてパスを調整
  if [ "$CSV_DIR" = "./csv_preprocessed" ]; then
    FILES=$(echo $FILES | sed 's|csv/|csv_preprocessed/|g')
  fi
  
  # 存在するファイルのみフィルタ
  local EXISTING_FILES=""
  for f in $FILES; do
    if [ -f "$f" ]; then
      EXISTING_FILES="$EXISTING_FILES $f"
    fi
  done
  
  if [ -z "$EXISTING_FILES" ]; then
    echo "⚠️  $TYPE_NAME: 対象ファイルなし（スキップ）"
    return
  fi
  
  echo ""
  echo "🔄 $TYPE_NAME のインポート..."
  npx ts-node scripts/backfill_companies_from_csv.ts $DRY_RUN $EXISTING_FILES
}

# タイプB: 法人番号あり（最も確実）
run_import "タイプB (法人番号あり)" "$TYPE_B"

# タイプA: 基本形式
run_import "タイプA (基本形式)" "$TYPE_A"

# タイプD: 都道府県・ID詳細形式（最多データ）
run_import "タイプD (都道府県・ID形式)" "$TYPE_D"

# タイプE: 都道府県形式（法人番号なし）
run_import "タイプE (都道府県形式)" "$TYPE_E"

# タイプC: 重複ヘッダー（前処理必要）
run_import "タイプC (重複ヘッダー)" "$TYPE_C"

# タイプF: 特殊形式
run_import "タイプF-ENG (英語ヘッダー)" "$TYPE_F_ENG"
# run_import "タイプF-JSON (JSON形式)" "$TYPE_F_JSON"  # JSON形式は個別対応が必要
# run_import "タイプF-SPECIAL (特殊形式)" "$TYPE_F_SPECIAL"  # 個別対応が必要

echo ""
echo "========================================"
echo "✅ インポート完了"
echo "========================================"

if [ -n "$DRY_RUN" ]; then
  echo ""
  echo "💡 実際にインポートするには、--dry-run フラグを外して実行してください。"
fi

