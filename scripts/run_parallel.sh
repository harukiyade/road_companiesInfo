#!/bin/bash
# 並列実行スクリプト - 全タイプを同時に実行して爆速化

echo "🚀 並列インポート開始"
echo ""

# 各タイプをバックグラウンドで並列実行
npx ts-node scripts/import_by_type.ts --type=B &
PID_B=$!

npx ts-node scripts/import_by_type.ts --type=A &
PID_A=$!

npx ts-node scripts/import_by_type.ts --type=C &
PID_C=$!

npx ts-node scripts/import_by_type.ts --type=D &
PID_D=$!

npx ts-node scripts/import_by_type.ts --type=E &
PID_E=$!

npx ts-node scripts/import_by_type.ts --type=F51 &
PID_F51=$!

npx ts-node scripts/import_by_type.ts --type=F130 &
PID_F130=$!

npx ts-node scripts/import_by_type.ts --type=F132 &
PID_F132=$!

# 全プロセスの完了を待機
echo "⏳ 全タイプ並列処理中..."
wait $PID_B $PID_A $PID_C $PID_D $PID_E $PID_F51 $PID_F130 $PID_F132

echo ""
echo "✅ 全タイプ完了"

