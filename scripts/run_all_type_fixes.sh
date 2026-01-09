#!/bin/bash

# 全タイプの修正・統合処理を順番に実行するスクリプト
#
# 使い方:
#   # DRY RUN（書き込みなし）
#   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
#   ./scripts/run_all_type_fixes.sh --dry-run
#
#   # 実際に実行
#   GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
#   ./scripts/run_all_type_fixes.sh

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
echo "  全タイプの修正・統合処理を開始します"
echo "======================================================================"
echo ""

# ログディレクトリの作成
LOG_DIR="./logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

echo "----------------------------------------------------------------------"
echo "  ステップ1: タイプAの重複チェック"
echo "----------------------------------------------------------------------"
echo ""
npx ts-node scripts/check_duplicates_type_a.ts 2>&1 | tee "$LOG_DIR/type_a_check_$TIMESTAMP.log"
echo ""
echo "✅ タイプA完了"
echo ""

echo "----------------------------------------------------------------------"
echo "  ステップ2: タイプB, C, Dの統合処理"
echo "----------------------------------------------------------------------"
echo ""
NODE_OPTIONS="--max-old-space-size=8192" npx ts-node scripts/dedupe_and_merge_type_bcd.ts $DRY_RUN 2>&1 | tee "$LOG_DIR/type_bcd_$TIMESTAMP.log"
echo ""
echo "✅ タイプB, C, D完了"
echo ""

echo "----------------------------------------------------------------------"
echo "  ステップ3: タイプEの修正・統合処理"
echo "----------------------------------------------------------------------"
echo ""
npx ts-node scripts/fix_and_dedupe_type_e.ts $DRY_RUN 2>&1 | tee "$LOG_DIR/type_e_$TIMESTAMP.log"
echo ""
echo "✅ タイプE完了"
echo ""

echo "----------------------------------------------------------------------"
echo "  ステップ4: タイプGの修正処理"
echo "----------------------------------------------------------------------"
echo ""
npx ts-node scripts/fix_type_g.ts $DRY_RUN 2>&1 | tee "$LOG_DIR/type_g_$TIMESTAMP.log"
echo ""
echo "✅ タイプG完了"
echo ""

echo "----------------------------------------------------------------------"
echo "  ステップ5: タイプHの修正・統合処理"
echo "----------------------------------------------------------------------"
echo ""
npx ts-node scripts/fix_and_dedupe_type_h.ts $DRY_RUN 2>&1 | tee "$LOG_DIR/type_h_$TIMESTAMP.log"
echo ""
echo "✅ タイプH完了"
echo ""

echo "----------------------------------------------------------------------"
echo "  ステップ6: タイプIの修正・統合処理"
echo "----------------------------------------------------------------------"
echo ""
npx ts-node scripts/fix_and_dedupe_type_i.ts $DRY_RUN 2>&1 | tee "$LOG_DIR/type_i_$TIMESTAMP.log"
echo ""
echo "✅ タイプI完了"
echo ""

echo "----------------------------------------------------------------------"
echo "  ステップ7: タイプJの修正・統合処理"
echo "----------------------------------------------------------------------"
echo ""
npx ts-node scripts/fix_and_dedupe_type_j.ts $DRY_RUN 2>&1 | tee "$LOG_DIR/type_j_$TIMESTAMP.log"
echo ""
echo "✅ タイプJ完了"
echo ""

echo "======================================================================"
echo "  全タイプの処理が完了しました！"
echo "======================================================================"
echo ""
echo "📄 ログファイルは以下に保存されました:"
echo "  $LOG_DIR/"
echo ""

if [ -n "$DRY_RUN" ]; then
  echo "💡 実際に変更を適用するには、--dry-run フラグを外して再実行してください。"
  echo ""
fi

echo "完了時刻: $(date)"

