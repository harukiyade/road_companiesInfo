#!/bin/bash

# 実行中のプロセスを停止するスクリプト

echo "🛑 実行中のプロセスを停止中..."

# fill_null_fields_from_csv_enhanced.ts のプロセスを検索して停止
PIDS=$(ps aux | grep "fill_null_fields_from_csv_enhanced.ts" | grep -v grep | awk '{print $2}')

if [ -z "$PIDS" ]; then
    echo "✅ 実行中のプロセスはありません"
else
    for PID in $PIDS; do
        echo "  プロセス $PID を停止中..."
        kill $PID 2>/dev/null
        sleep 1
        # 強制終了が必要な場合
        if ps -p $PID > /dev/null 2>&1; then
            echo "  プロセス $PID を強制終了中..."
            kill -9 $PID 2>/dev/null
        fi
    done
    echo "✅ プロセスを停止しました"
fi

# ログファイルの確認
if [ -f "fill_null_fields_forward.log" ]; then
    echo "📄 fill_null_fields_forward.log の最終行:"
    tail -3 fill_null_fields_forward.log
fi

if [ -f "fill_null_fields_reverse.log" ]; then
    echo "📄 fill_null_fields_reverse.log の最終行:"
    tail -3 fill_null_fields_reverse.log
fi

