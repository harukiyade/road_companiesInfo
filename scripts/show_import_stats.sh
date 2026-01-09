#!/bin/bash
# タイプ別CSV取り込み統計表示

echo ""
echo "=========================================="
echo "📊 タイプ別CSV取り込み統計"
echo "=========================================="
echo ""

# 各タイプのCSV行数を集計
echo "📁 CSVファイル行数:"
echo ""

count_type_lines() {
  local type_name="$1"
  shift
  local files=("$@")
  local total_lines=0
  local file_count=0
  
  for file in "${files[@]}"; do
    if [ -f "csv/$file" ]; then
      # ヘッダー除く行数を計算
      lines=$(tail -n +2 "csv/$file" 2>/dev/null | wc -l | tr -d ' ')
      total_lines=$((total_lines + lines))
      file_count=$((file_count + 1))
    fi
  done
  
  printf "%-10s: %7d行 (%2dファイル)\n" "$type_name" "$total_lines" "$file_count"
}

# タイプA
count_type_lines "タイプA" 10.csv 11.csv 100.csv 101.csv 102.csv 103.csv 104.csv 118.csv 119.csv 120.csv 121.csv

# タイプB
count_type_lines "タイプB" 12.csv 13.csv 14.csv 15.csv 16.csv 17.csv 18.csv 19.csv 20.csv 21.csv 22.csv 23.csv 24.csv 25.csv 26.csv 27.csv 28.csv 29.csv 30.csv 31.csv 32.csv 33.csv 34.csv 35.csv 36.csv 37.csv

# タイプC
count_type_lines "タイプC" 105.csv 106.csv 107.csv 109.csv 110.csv 122.csv

# タイプD
count_type_lines "タイプD" 111.csv 112.csv 113.csv 114.csv 115.csv

# タイプE
count_type_lines "タイプE" 116.csv 117.csv

# タイプF
count_type_lines "タイプF" 124.csv 125.csv 126.csv

# タイプG
count_type_lines "タイプG" 127.csv 128.csv

# タイプH
count_type_lines "タイプH" 130.csv 131.csv

# タイプI
count_type_lines "タイプI" 132.csv

# タイプJ
count_type_lines "タイプJ" 133.csv 134.csv 135.csv 136.csv

echo ""
echo "=========================================="
echo ""
echo "📌 確認方法:"
echo ""
echo "1. タイプ別サンプル確認（各タイプ3社ずつ）:"
echo "   GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \\"
echo "   npx ts-node scripts/verify_csv_import_by_type.ts"
echo ""
echo "2. 特定CSVの全行確認:"
echo "   GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \\"
echo "   npx ts-node scripts/verify_specific_csv.ts csv/107.csv"
echo ""
echo "3. 詳細確認（verbose）:"
echo "   GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \\"
echo "   npx ts-node scripts/verify_specific_csv.ts csv/130.csv --verbose"
echo ""
echo "4. 丹羽興業株式会社の統合確認:"
echo "   GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \\"
echo "   npx ts-node scripts/check_niwa_kogyo.ts"
echo ""
echo "=========================================="
echo ""
