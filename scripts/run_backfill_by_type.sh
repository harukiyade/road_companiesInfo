#!/bin/bash
# タイプ別バックフィル実行スクリプト
# 各タイプを並列で実行

set -e
cd "$(dirname "$0")/.."

export GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json

echo "🚀 タイプ別バックフィル開始"
echo ""リリ

# タイプA: 基本形式（法人番号なし）- 55ファイル
TYPE_A="csv/7.csv csv/8.csv csv/9.csv csv/10.csv csv/11.csv csv/12.csv csv/13.csv csv/14.csv csv/15.csv csv/16.csv csv/17.csv csv/18.csv csv/19.csv csv/20.csv csv/21.csv csv/22.csv csv/25.csv csv/26.csv csv/27.csv csv/28.csv csv/29.csv csv/30.csv csv/31.csv csv/32.csv csv/33.csv csv/34.csv csv/35.csv csv/39.csv csv/52.csv csv/54.csv csv/55.csv csv/56.csv csv/57.csv csv/58.csv csv/59.csv csv/60.csv csv/61.csv csv/62.csv csv/63.csv csv/64.csv csv/65.csv csv/66.csv csv/67.csv csv/68.csv csv/69.csv csv/70.csv csv/71.csv csv/72.csv csv/73.csv csv/74.csv csv/75.csv csv/76.csv csv/77.csv csv/101.csv csv/104.csv"

# タイプB: 創業あり形式 - 26ファイル
TYPE_B="csv/23.csv csv/78.csv csv/79.csv csv/80.csv csv/81.csv csv/82.csv csv/83.csv csv/84.csv csv/85.csv csv/86.csv csv/87.csv csv/88.csv csv/89.csv csv/90.csv csv/91.csv csv/92.csv csv/93.csv csv/94.csv csv/95.csv csv/96.csv csv/97.csv csv/98.csv csv/99.csv csv/100.csv csv/102.csv csv/105.csv"

# タイプC: 直近決算情報あり（法人番号あり）- 6ファイル
TYPE_C="csv/36.csv csv/37.csv csv/44.csv csv/49.csv csv/107.csv csv/109.csv"

# タイプD: 法人番号から始まる形式 - 6ファイル
TYPE_D="csv/1.csv csv/2.csv csv/53.csv csv/103.csv csv/106.csv csv/126.csv"

# タイプE: 法人番号・都道府県形式 - 4ファイル
TYPE_E="csv/3.csv csv/4.csv csv/5.csv csv/6.csv"

# タイプF: 決算5期分形式 - 1ファイル
TYPE_F="csv/132.csv"

# タイプG: 直近決算情報（法人番号あり、備考あり）- 4ファイル
TYPE_G="csv/108.csv csv/110.csv csv/111.csv csv/112.csv"

# タイプH: 直近決算情報（法人番号なし）- 4ファイル
TYPE_H="csv/118.csv csv/119.csv csv/120.csv csv/122.csv"

# タイプI: 英語ヘッダー形式 - 2ファイル
TYPE_I="csv/130.csv csv/131.csv"

# その他（129.csvはヘッダーがないため除外）
TYPE_OTHER="csv/24.csv csv/38.csv csv/40.csv csv/41.csv csv/42.csv csv/43.csv csv/45.csv csv/46.csv csv/47.csv csv/48.csv csv/50.csv csv/51.csv csv/113.csv csv/114.csv csv/115.csv csv/116.csv csv/117.csv csv/121.csv csv/123.csv csv/124.csv csv/125.csv csv/127.csv csv/128.csv csv/133.csv csv/134.csv"

PIDS=()

echo "📌 タイプA (55ファイル) 開始..."
npx ts-node scripts/backfill_companies_from_csv.ts $TYPE_A > /tmp/backfill_typeA.log 2>&1 &
PIDS+=($!)

echo "📌 タイプB (26ファイル) 開始..."
npx ts-node scripts/backfill_companies_from_csv.ts $TYPE_B > /tmp/backfill_typeB.log 2>&1 &
PIDS+=($!)

echo "📌 タイプC (6ファイル) 開始..."
npx ts-node scripts/backfill_companies_from_csv.ts $TYPE_C > /tmp/backfill_typeC.log 2>&1 &
PIDS+=($!)

echo "📌 タイプD (6ファイル) 開始..."
npx ts-node scripts/backfill_companies_from_csv.ts $TYPE_D > /tmp/backfill_typeD.log 2>&1 &
PIDS+=($!)

echo "📌 タイプE (4ファイル) 開始..."
npx ts-node scripts/backfill_companies_from_csv.ts $TYPE_E > /tmp/backfill_typeE.log 2>&1 &
PIDS+=($!)

echo "📌 タイプF (1ファイル) 開始..."
npx ts-node scripts/backfill_companies_from_csv.ts $TYPE_F > /tmp/backfill_typeF.log 2>&1 &
PIDS+=($!)

echo "📌 タイプG (4ファイル) 開始..."
npx ts-node scripts/backfill_companies_from_csv.ts $TYPE_G > /tmp/backfill_typeG.log 2>&1 &
PIDS+=($!)

echo "📌 タイプH (4ファイル) 開始..."
npx ts-node scripts/backfill_companies_from_csv.ts $TYPE_H > /tmp/backfill_typeH.log 2>&1 &
PIDS+=($!)

echo "📌 タイプI (2ファイル) 開始..."
npx ts-node scripts/backfill_companies_from_csv.ts $TYPE_I > /tmp/backfill_typeI.log 2>&1 &
PIDS+=($!)

echo "📌 その他 (26ファイル) 開始..."
npx ts-node scripts/backfill_companies_from_csv.ts $TYPE_OTHER > /tmp/backfill_typeOther.log 2>&1 &
PIDS+=($!)

echo ""
echo "⏳ 10タイプを並列処理中..."
echo "   ログは /tmp/backfill_type*.log に出力"
echo ""

# 全プロセスの完了を待機
wait "${PIDS[@]}"

echo ""
echo "✅ 全タイプ完了"
echo ""
echo "📊 結果サマリー:"
echo ""

for type in A B C D E F G H I Other; do
    LOG="/tmp/backfill_type${type}.log"
    if [ -f "$LOG" ]; then
        echo "=== タイプ${type} ==="
        grep -E "(📊 CSV 総行数|✨ 既存更新件数|🆕 新規作成件数|❌ エラー)" "$LOG" 2>/dev/null || echo "  処理中またはエラー"
        echo ""
    fi
done

echo "🎉 バックフィル完了！"

