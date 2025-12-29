#!/bin/bash

# タイプB,C,DのCSVファイルをインポートするスクリプト
#
# 使い方:
#   # DRY RUN
#   GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
#   ./scripts/run_type_bcd_import.sh --dry-run
#
#   # 実際に実行
#   GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
#   ./scripts/run_type_bcd_import.sh

set -e

# カレントディレクトリをプロジェクトルートに設定
cd "$(dirname "$0")/.."

# サービスアカウントキーのパスをチェック
if [ -z "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
  if [ -f "./albert-ma-firebase-adminsdk-iat1k-a64039899f.json" ]; then
    export GOOGLE_APPLICATION_CREDENTIALS="./albert-ma-firebase-adminsdk-iat1k-a64039899f.json"
    echo "ℹ️  デフォルトのサービスアカウントキーを使用: $GOOGLE_APPLICATION_CREDENTIALS"
  else
    echo "❌ エラー: GOOGLE_APPLICATION_CREDENTIALS が設定されていません"
    exit 1
  fi
fi

# DRY RUNフラグのチェック
DRY_RUN=""
if [[ "$*" == *"--dry-run"* ]]; then
  DRY_RUN="--dry-run"
  echo "🔍 DRY RUN モード: 実際の書き込みは行いません"
  echo ""
fi

echo "======================================================================"
echo "  タイプB,C,DのCSVファイルをインポートします"
echo "======================================================================"
echo ""

# タイプBのファイル（6件）
TYPE_B=(
  csv/1.csv
  csv/2.csv
  csv/53.csv
  csv/103.csv
  csv/106.csv
  csv/126.csv
)

# タイプCのファイル（26件）
TYPE_C=(
  csv/23.csv
  csv/78.csv csv/79.csv csv/80.csv csv/81.csv csv/82.csv
  csv/83.csv csv/84.csv csv/85.csv csv/86.csv csv/87.csv
  csv/88.csv csv/89.csv csv/90.csv csv/91.csv csv/92.csv
  csv/93.csv csv/94.csv csv/95.csv csv/96.csv csv/97.csv
  csv/98.csv csv/99.csv csv/100.csv csv/102.csv csv/105.csv
)

# タイプDのファイル（29件）
TYPE_D=(
  csv/36.csv csv/37.csv csv/38.csv csv/40.csv csv/41.csv
  csv/42.csv csv/43.csv csv/44.csv csv/45.csv csv/46.csv
  csv/47.csv csv/48.csv csv/49.csv csv/50.csv csv/107.csv
  csv/108.csv csv/109.csv csv/110.csv csv/111.csv csv/112.csv
  csv/113.csv csv/114.csv csv/115.csv csv/116.csv csv/117.csv
  csv/119.csv csv/24.csv csv/133.csv csv/134.csv
)

# ログディレクトリの作成
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo "----------------------------------------------------------------------"
echo "  タイプB: 6件のCSVファイルをインポート"
echo "----------------------------------------------------------------------"
echo ""
npx ts-node scripts/backfill_companies_from_csv.ts $DRY_RUN "${TYPE_B[@]}" 2>&1 | tee "$LOG_DIR/type_b_import_$TIMESTAMP.log"
echo ""
echo "✅ タイプB完了"
echo ""

echo "----------------------------------------------------------------------"
echo "  タイプC: 26件のCSVファイルをインポート"
echo "----------------------------------------------------------------------"
echo ""
npx ts-node scripts/backfill_companies_from_csv.ts $DRY_RUN "${TYPE_C[@]}" 2>&1 | tee "$LOG_DIR/type_c_import_$TIMESTAMP.log"
echo ""
echo "✅ タイプC完了"
echo ""

echo "----------------------------------------------------------------------"
echo "  タイプD: 29件のCSVファイルをインポート"
echo "----------------------------------------------------------------------"
echo ""
npx ts-node scripts/backfill_companies_from_csv.ts $DRY_RUN "${TYPE_D[@]}" 2>&1 | tee "$LOG_DIR/type_d_import_$TIMESTAMP.log"
echo ""
echo "✅ タイプD完了"
echo ""

echo "======================================================================"
echo "  タイプB,C,Dのインポートが完了しました！"
echo "======================================================================"
echo ""
echo "📄 ログファイルは以下に保存されました:"
echo "  $LOG_DIR/type_b_import_$TIMESTAMP.log"
echo "  $LOG_DIR/type_c_import_$TIMESTAMP.log"
echo "  $LOG_DIR/type_d_import_$TIMESTAMP.log"
echo ""

if [ -n "$DRY_RUN" ]; then
  echo "💡 実際に変更を適用するには、--dry-run フラグを外して再実行してください。"
  echo ""
fi

echo "完了時刻: $(date)"

