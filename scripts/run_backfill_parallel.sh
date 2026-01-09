#!/bin/bash
# backfill_companies_from_csv.ts を並列実行するスクリプト
# CSVファイルを8グループに分けて同時処理

set -e

cd "$(dirname "$0")/.."

echo "🚀 バックフィル並列実行開始"
echo ""

# CSVファイルを取得してグループ分け
CSV_FILES=(./csv/*.csv)
TOTAL=${#CSV_FILES[@]}
echo "📂 総CSVファイル数: $TOTAL"

# 8並列で処理
NUM_WORKERS=8
FILES_PER_WORKER=$(( (TOTAL + NUM_WORKERS - 1) / NUM_WORKERS ))

echo "👷 ワーカー数: $NUM_WORKERS"
echo "📄 1ワーカーあたり約 $FILES_PER_WORKER ファイル"
echo ""

# 各ワーカー用のファイルリストを作成して実行
PIDS=()

for ((i=0; i<NUM_WORKERS; i++)); do
    START=$((i * FILES_PER_WORKER))
    END=$((START + FILES_PER_WORKER))
    
    # このワーカーが担当するファイルを抽出
    WORKER_FILES=("${CSV_FILES[@]:$START:$FILES_PER_WORKER}")
    
    if [ ${#WORKER_FILES[@]} -eq 0 ]; then
        continue
    fi
    
    echo "🔧 ワーカー $((i+1)): ${#WORKER_FILES[@]} ファイル処理開始"
    
    # バックグラウンドで実行
    GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
        npx ts-node scripts/backfill_companies_from_csv.ts "${WORKER_FILES[@]}" \
        > "/tmp/backfill_worker_$((i+1)).log" 2>&1 &
    
    PIDS+=($!)
done

echo ""
echo "⏳ 全ワーカー処理中... (ログは /tmp/backfill_worker_*.log に出力)"
echo ""

# 進捗表示
while true; do
    RUNNING=0
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            ((RUNNING++))
        fi
    done
    
    if [ $RUNNING -eq 0 ]; then
        break
    fi
    
    # 各ワーカーの進捗を表示
    echo -n "  進捗: "
    for ((i=0; i<NUM_WORKERS; i++)); do
        LOG_FILE="/tmp/backfill_worker_$((i+1)).log"
        if [ -f "$LOG_FILE" ]; then
            COUNT=$(grep -c "✅ ここまでの更新件数" "$LOG_FILE" 2>/dev/null || echo "0")
            UPDATED=$((COUNT * 500))
            echo -n "W$((i+1)):${UPDATED}件 "
        fi
    done
    echo ""
    
    sleep 10
done

echo ""
echo "✅ 全ワーカー完了"
echo ""

# 結果サマリーを表示
echo "📊 結果サマリー:"
for ((i=0; i<NUM_WORKERS; i++)); do
    LOG_FILE="/tmp/backfill_worker_$((i+1)).log"
    if [ -f "$LOG_FILE" ]; then
        echo "--- ワーカー $((i+1)) ---"
        tail -10 "$LOG_FILE" | grep -E "(✅|📊|🆕|❓|⚠️)" || true
        echo ""
    fi
done

echo "🎉 バックフィル完了！"

