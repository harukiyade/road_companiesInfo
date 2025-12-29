#!/bin/bash
echo "=== プロセス確認 ==="
ps aux | grep "tsx.*fill_null_fields_from_csv_enhanced" | grep -v grep || echo "プロセスは実行されていません"

echo -e "\n=== ログファイルサイズ ==="
ls -lh "fill_null_fields_reverse.log" 2>/dev/null || echo "ログファイルが見つかりません"

echo -e "\n=== 最新のログ（最後の30行） ==="
tail -n 30 "fill_null_fields_reverse.log" 2>/dev/null || echo "ログファイルが見つかりません"

echo -e "\n=== エラーや警告 ==="
grep -i -E "(error|エラー|失敗|失敗)" "fill_null_fields_reverse.log" 2>/dev/null | tail -5 || echo "エラーは見つかりませんでした"
