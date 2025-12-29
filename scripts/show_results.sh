#!/bin/bash
# 実行結果を表示するスクリプト

LOG_FILE="$1"

if [ -z "$LOG_FILE" ]; then
  # 最新のログファイルを自動検出
  LOG_FILE=$(ls -t logs/scrape_extended_fields_*.log logs/full_execution_*.log 2>/dev/null | head -1)
fi

if [ -z "$LOG_FILE" ] || [ ! -f "$LOG_FILE" ]; then
  echo "❌ ログファイルが見つかりません"
  echo "使用方法: $0 [ログファイルパス]"
  exit 1
fi

echo "=== ログファイル: $LOG_FILE ==="
echo ""

# 取得できたドキュメントIDとフィールドのサマリー
echo "【取得できたドキュメントIDとフィールド】"
echo ""
grep -E "保存フィールド一覧:" "$LOG_FILE" | while read line; do
  doc_id=$(echo "$line" | grep -oE "\[[0-9]+\]" | head -1)
  fields=$(echo "$line" | sed 's/.*保存フィールド一覧: //')
  if [ -n "$doc_id" ] && [ -n "$fields" ]; then
    echo "$doc_id: $fields"
  fi
done

echo ""
echo "=== 詳細な取得結果（各ドキュメントごと） ==="
echo ""

# 各ドキュメントの詳細情報
grep -E "保存フィールド一覧:" "$LOG_FILE" | while read line; do
  doc_id=$(echo "$line" | grep -oE "\[[0-9]+\]" | head -1 | tr -d '[]')
  fields=$(echo "$line" | sed 's/.*保存フィールド一覧: //')
  
  if [ -n "$doc_id" ] && [ -n "$fields" ]; then
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "[$doc_id] で取得したフィールド:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    # 保存されるフィールドと値のセクションから取得
    # ドキュメントIDを含む行から「保存フィールド一覧」までの範囲を検索
    awk -v doc_id="$doc_id" '
      /\[.*\]/ && $0 ~ "\\[" doc_id "\\]" { 
        in_section=1; 
      }
      in_section && /保存されるフィールドと値:/ { 
        in_fields=1; 
        next;
      }
      in_fields && /保存フィールド一覧:/ { 
        in_fields=0; 
        in_section=0;
      }
      in_fields && /^\s+- [^:]+:/ { 
        print;
      }
    ' "$LOG_FILE" | while read field_line; do
      field_name=$(echo "$field_line" | sed 's/^\s+- \([^:]*\):.*/\1/')
      field_value=$(echo "$field_line" | sed 's/^\s+- [^:]*: //')
      
      if [ -n "$field_name" ] && [ -n "$field_value" ]; then
        # 値が長すぎる場合は切り詰め
        if [ ${#field_value} -gt 150 ]; then
          field_value="${field_value:0:150}... (長さ: ${#field_value}文字)"
        fi
        
        echo "  • $field_name: $field_value"
      fi
    done
    
    # フィールド一覧を表示（カンマ区切り）
    echo ""
    echo "  取得フィールド: $fields"
  fi
done

echo ""
echo "=== 統計情報 ==="
echo ""

# 処理企業数
total_processed=$(grep -E "処理企業数:" "$LOG_FILE" | tail -1 | grep -oE "[0-9]+" | head -1)
echo "処理企業数: ${total_processed:-0}件"

# 更新数
total_updated=$(grep -E "更新数:" "$LOG_FILE" | tail -1 | grep -oE "[0-9]+" | head -1)
echo "更新数: ${total_updated:-0}件"

# スキップ数
total_skipped=$(grep -E "スキップ数:" "$LOG_FILE" | tail -1 | grep -oE "[0-9]+" | head -1)
echo "スキップ数: ${total_skipped:-0}件"

# フィールド別の取得数
echo ""
echo "【フィールド別の取得数】"
grep -E "✓.*:" "$LOG_FILE" | sed 's/.*✓ \([^:]*\):.*/\1/' | sort | uniq -c | sort -rn | head -20 | while read count field; do
  echo "  $field: ${count}件"
done

