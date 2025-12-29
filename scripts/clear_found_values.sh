#!/bin/bash

# CSVファイルのfoundValue列をクリアするスクリプト

echo "🧹 CSVファイルのfoundValue列をクリア中..."

cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"

CSV_DIR="null_fields_detailed"

if [ ! -d "$CSV_DIR" ]; then
    echo "❌ ディレクトリが見つかりません: $CSV_DIR"
    exit 1
fi

# ファイル範囲の指定（オプション）
START_FILE=${START_FILE:-1}
END_FILE=${END_FILE:-10000}

echo "   開始ファイル: $START_FILE"
echo "   終了ファイル: $END_FILE"
echo ""

count=0
for file in "$CSV_DIR"/null_fields_detailed_*.csv; do
    if [ ! -f "$file" ]; then
        continue
    fi
    
    # ファイル番号を抽出
    filename=$(basename "$file")
    if [[ $filename =~ null_fields_detailed_([0-9]+)\.csv ]]; then
        file_num=${BASH_REMATCH[1]}
        
        # ファイル範囲でフィルタリング
        if [ $file_num -ge $START_FILE ] && [ $file_num -le $END_FILE ]; then
            # foundValue列をクリア（Pythonスクリプトを使用）
            python3 << EOF
import csv
import sys
import os

file_path = "$file"
file_basename = os.path.basename(file_path)
new_lines = []

try:
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader)
        
        # foundValue列のインデックスを取得
        if 'foundValue' in header:
            found_value_idx = header.index('foundValue')
        else:
            # foundValue列がない場合は追加
            header.append('foundValue')
            found_value_idx = len(header) - 1
        
        new_lines.append(header)
        
        for row in reader:
            if len(row) > found_value_idx:
                row[found_value_idx] = ''
            elif len(row) == found_value_idx:
                row.append('')
            new_lines.append(row)
    
    # ファイルに書き戻し
    with open(file_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(new_lines)
    
    print(f"  ✅ {file_basename}")
except Exception as e:
    print(f"  ❌ {file_basename}: {e}")
EOF
            count=$((count + 1))
            
            if [ $((count % 100)) -eq 0 ]; then
                echo "   処理中: $count ファイル..."
            fi
        fi
    fi
done

echo ""
echo "✅ 完了: $count ファイルのfoundValue列をクリアしました"

