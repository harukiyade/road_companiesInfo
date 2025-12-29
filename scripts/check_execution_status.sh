#!/bin/bash

# 実行状況確認スクリプト

echo "🔍 実行状況確認"
echo "=================="
echo ""

# プロセス確認
echo "📌 実行中のプロセス:"
ps aux | grep "fill_null_fields_from_csv_enhanced.ts" | grep -v grep || echo "  実行中のプロセスはありません"
echo ""

# ログファイル確認
echo "📌 ログファイル:"
if [ -f "fill_null_fields_forward.log" ]; then
    echo "  fill_null_fields_forward.log (上から実行):"
    echo "    最終更新: $(stat -f "%Sm" fill_null_fields_forward.log 2>/dev/null || stat -c "%y" fill_null_fields_forward.log 2>/dev/null)"
    echo "    最終10行:"
    tail -10 fill_null_fields_forward.log | sed 's/^/      /'
    echo ""
fi

if [ -f "fill_null_fields_reverse.log" ]; then
    echo "  fill_null_fields_reverse.log (下から実行):"
    echo "    最終更新: $(stat -f "%Sm" fill_null_fields_reverse.log 2>/dev/null || stat -c "%y" fill_null_fields_reverse.log 2>/dev/null)"
    echo "    最終10行:"
    tail -10 fill_null_fields_reverse.log | sed 's/^/      /'
    echo ""
fi

# CSVファイルの更新状況
echo "📌 CSVファイルの更新状況:"
if [ -d "null_fields_detailed" ]; then
    total_files=$(ls -1 null_fields_detailed/null_fields_detailed_*.csv 2>/dev/null | wc -l | tr -d ' ')
    files_with_foundvalue=$(grep -l "foundValue" null_fields_detailed/null_fields_detailed_*.csv 2>/dev/null | wc -l | tr -d ' ')
    echo "  総ファイル数: $total_files"
    echo "  foundValue列があるファイル数: $files_with_foundvalue"
    echo ""
    
    # 最近更新されたファイル
    echo "  最近更新されたファイル（上位5件）:"
    ls -lt null_fields_detailed/null_fields_detailed_*.csv 2>/dev/null | head -5 | awk '{print "    " $9 " (" $6 " " $7 " " $8 ")"}'
    echo ""
fi

# 更新されたドキュメントIDの確認
echo "📌 更新されたドキュメントID:"
if [ -f "updated_company_ids.txt" ]; then
    total_ids=$(wc -l < updated_company_ids.txt | tr -d ' ')
    echo "  総数: $total_ids 件"
    echo "  最初の10件:"
    head -10 updated_company_ids.txt | sed 's/^/    /'
    if [ "$total_ids" -gt 10 ]; then
        echo "  最後の10件:"
        tail -10 updated_company_ids.txt | sed 's/^/    /'
    fi
else
    echo "  updated_company_ids.txt が見つかりません"
    echo "  実行: npx tsx scripts/check_fill_progress.ts"
fi

echo ""
echo "✅ 確認完了"

