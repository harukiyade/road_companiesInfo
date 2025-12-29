# 全要望実現のための実行手順

## 📋 前提確認

### 1. 修正済みCSVファイルの状態確認
```bash
cd /Users/harumacmini/Downloads/road_companiesInfo

# タイプC（107含む）- 業種（細）追加済み
head -1 csv/107.csv | grep -o "業種" | wc -l  # 4なら修正済み

# 130/131の状態
head -1 csv/130.csv | tr ',' '\n' | wc -l  # フィールド数確認

# 127/128の状態
head -1 csv/127.csv | tr ',' '\n' | wc -l  # フィールド数確認
```

---

## 🚀 実行手順

### ステップ1: 130.csv/131.csvを展開（必要な場合）
```bash
cd /Users/harumacmini/Downloads/road_companiesInfo

# 現在の状態を確認
CURRENT_COLS=$(head -1 csv/130.csv | tr ',' '\n' | wc -l)
echo "130.csvの現在のカラム数: $CURRENT_COLS"

# 61カラム未満の場合は展開実行
if [ "$CURRENT_COLS" -lt 61 ]; then
  echo "展開を実行します..."
  python3 scripts/expand_type_i_csv.py
  mv csv/130_expanded.csv csv/130.csv
  mv csv/131_expanded.csv csv/131.csv
  echo "✅ 130/131展開完了"
else
  echo "✅ 130/131は既に展開済み"
fi
```

### ステップ2: 127.csv/128.csvを統一（必要な場合）
```bash
cd /Users/harumacmini/Downloads/road_companiesInfo

# 統一版が存在する場合は更新
if [ -f csv/127_unified.csv ] && [ -f csv/128_unified.csv ]; then
  mv csv/127_unified.csv csv/127.csv
  mv csv/128_unified.csv csv/128.csv
  echo "✅ 127/128統一版に更新"
else
  echo "✅ 127/128は既に最新"
fi
```

### ステップ3: 全CSVをバックフィル実行
```bash
cd /Users/harumacmini/Downloads/road_companiesInfo

# 環境変数を設定してバックフィル実行
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
./scripts/run_backfill_by_type.sh
```

**このコマンドで：**
- 全134ファイル（129.csv除く）を10タイプ並列処理
- 各企業を法人番号または名前+住所で一意に特定
- 既存ドキュメントは情報を補完して更新
- 新規企業は新規作成

---

## 🎯 ワンライナー（全実行）

```bash
cd /Users/harumacmini/Downloads/road_companiesInfo && \

# 130/131展開（必要な場合）
if [ $(head -1 csv/130.csv | tr ',' '\n' | wc -l) -lt 61 ]; then \
  python3 scripts/expand_type_i_csv.py && \
  mv csv/130_expanded.csv csv/130.csv && \
  mv csv/131_expanded.csv csv/131.csv; \
fi && \

# 127/128統一版更新（存在する場合）
[ -f csv/127_unified.csv ] && mv csv/127_unified.csv csv/127.csv; \
[ -f csv/128_unified.csv ] && mv csv/128_unified.csv csv/128.csv; \

# バックフィル実行
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
./scripts/run_backfill_by_type.sh
```

---

## ⏱️ 推定実行時間

- **130/131展開**: 30秒
- **127/128更新**: 即座
- **バックフィル実行**: 15-30分（並列処理）

---

## 📊 実行後の確認

### データベース確認
```bash
# Firestoreコンソールで確認
# companies_newコレクションのドキュメント数
# ランダムに数社のフィールドを確認
```

### ログ確認
```bash
# 各タイプのログを確認
ls -lh /tmp/backfill_type*.log

# エラーがあれば確認
grep "エラー\|警告" /tmp/backfill_type*.log
```

---

## 🔧 トラブルシューティング

### 130/131が正しく展開されない場合
```bash
# バックアップから再生成
cd /Users/harumacmini/Downloads/road_companiesInfo
cp csv/130_backup.csv csv/130.csv
cp csv/131_backup.csv csv/131.csv
python3 scripts/expand_type_i_csv.py
mv csv/130_expanded.csv csv/130.csv
mv csv/131_expanded.csv csv/131.csv
```

### 127/128が正しく統一されない場合
```bash
# 元のCSVから再処理
cd /Users/harumacmini/Downloads/road_companiesInfo
python3 scripts/expand_type_j1_csv.py  # summaryJson展開
python3 scripts/cleanup_type_j1_csv.py  # JSONフィールド削除
python3 scripts/unify_127_128.py       # 最終統一
mv csv/127_unified.csv csv/127.csv
mv csv/128_unified.csv csv/128.csv
```

---

## ✨ 全実装済み機能

1. ✅ 取引先を4つに分離（clients, subsidiaries, suppliers, banks）
2. ✅ 決算5期分フィールド（fiscalMonth1-5, revenue1-5, profit1-5）
3. ✅ 役員10名個別フィールド（executiveName1-10, executivePosition1-10）
4. ✅ 部署7箇所個別フィールド（departmentName1-7, Address, Phone）
5. ✅ 財務詳細フィールド（totalAssets, totalLiabilities, netAssets, operatingIncome）
6. ✅ 51.csv固有フィールド（averageAge, specialties など8項目）
7. ✅ 127/128固有フィールド（nikkeiCode, issuedShares, affiliations）
8. ✅ 107タイプの業種（細）ヘッダー追加
9. ✅ registrantフィールド削除
10. ✅ 企業重複統合（法人番号または名前+住所で一意化）
11. ✅ 数値正規化（単位除去）
12. ✅ 変な記号除去（〒、◆、※など）

**companies_newコレクション**: 155フィールド

