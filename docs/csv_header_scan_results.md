# 大量のCSV群からの特定ヘッダー抽出 スキャン結果

## 1. サマリ

| 項目 | 値 |
|------|-----|
| 該当ファイル総数 | **356 ファイル** |
| スキップ条件 | 「備考」のみのヘッダー／列のファイル（該当なし） |

## 2. ヘッダー別 出現ファイル数

| ヘッダー名 | 出現ファイル数 | DBマッピング先 |
|-----------|----------------|----------------|
| 概要 | 239 | business_descriptions |
| 説明 | 159 | overview |
| 概況 | 114 | overview |
| 企業概要 | 28 | overview |
| 事業内容 | 3 | business_descriptions |
| 会社情報・備考 | 2 | overview |
| 担当者コメント | 2 | 備考用途（要判断） |

## 3. ヘッダー組み合わせ別 内訳

| 該当ヘッダー | ファイル数 | 備考（特記事項） |
|-------------|-----------|------------------|
| 説明, 概要 | 159 | 説明=短い業種説明、概要=事業内容・財務等の詳細（→ overview / business_descriptions に振り分け） |
| 概況 | 114 | 概況のみ（→ overview） |
| 概要 | 52 | 概要のみ（→ business_descriptions） |
| 企業概要, 概要 | 26 | 譲渡案件系（→ overview + business_descriptions） |
| 事業内容, 会社情報・備考 | 2 | import_firstTime_51（→ business_descriptions + overview） |
| 企業概要, 担当者コメント, 概要 | 2 | yuzuri_10（→ overview + 備考 + business_descriptions） |
| 事業内容 | 1 | import_firstTime_129（→ business_descriptions） |

## 4. DBマッピング方針（最終目的）

| カラム | 振り分け元ヘッダー |
|--------|-------------------|
| **overview** | 説明、企業概要、概況、会社情報・備考、担当者コメント |
| **business_descriptions** | 概要、事業内容 |

## 5. ファイル一覧（パス・該当ヘッダー・備考）

### fixed_csv_3/unit_million/

| ファイルパス | 該当ヘッダー | 備考（特記事項） |
|-------------|-------------|------------------|
| fixed_csv_3/unit_million/108.csv | 説明, 概要 | 説明(短) + 概要(詳細) |
| fixed_csv_3/unit_million/yuzuri_*.csv (26件) | 企業概要, 概要 | 譲渡案件 |
| fixed_csv_3/unit_million/yuzuri_10.csv | 企業概要, 担当者コメント, 概要 | 3列あり |

### fixed_csv_3/later/

| ファイルパス | 該当ヘッダー | 備考（特記事項） |
|-------------|-------------|------------------|
| fixed_csv_3/later/import_firstTime_51.csv | 事業内容, 会社情報・備考 | |

### fixed_csv_2/（直下）

| 該当ヘッダー | ファイル数 | 備考 |
|-------------|-----------|------|
| 説明, 概要 | 約45件 | 1.csv, 2.csv, ... 32.csv, *_20251224.csv など |

### fixed_csv_2/import_firstTime/

| 該当ヘッダー | 例 |
|-------------|-----|
| 概況 | 1.csv, 10.csv, 11.csv, 12.csv, 13.csv など多数 |
| 概要 | 100.csv, 102.csv, 105.csv など |
| 説明, 概要 | 107.csv, 108.csv, 110.csv〜125.csv, 132.csv, 133.csv など |
| 事業内容 | 129.csv |
| 事業内容, 会社情報・備考 | 51.csv |

### fixed_csv_2/yuzuri/

| 該当ヘッダー | 例 |
|-------------|-----|
| 企業概要, 概要 | 1.csv, 12.csv など26件 |
| 企業概要, 担当者コメント, 概要 | 10.csv |

### csv_final_exact_35/ などその他

| 該当ヘッダー | 例 |
|-------------|-----|
| 説明, 概要 | import_firstTime_119.csv など |

---

**作成日**: 2025-02-14  
**探索範囲**: fixed_csv_3/unit_million/, fixed_csv_2/, import_firstTime_*.csv, その他
