#!/bin/bash
# 削除スクリプトの完了を待つスクリプト

LOG_FILE="/Users/harumacmini/Downloads/road_companiesInfo/delete_duplicates.log"

echo "⏳ 削除スクリプトの完了を待機中..."
echo "   ログファイル: $LOG_FILE"
echo ""

while true; do
    if [ -f "$LOG_FILE" ]; then
        # ログファイルの最後の数行を確認
        LAST_LINES=$(tail -5 "$LOG_FILE" 2>/dev/null)
        
        # 完了メッセージをチェック
        if echo "$LAST_LINES" | grep -q "✅ 重複チェック完了"; then
            echo ""
            echo "✅ 削除スクリプトが完了しました！"
            echo ""
            tail -10 "$LOG_FILE"
            break
        fi
        
        # 進捗を表示
        if echo "$LAST_LINES" | grep -q "スキャン中"; then
            echo -n "."
        fi
    fi
    
    sleep 5
done
