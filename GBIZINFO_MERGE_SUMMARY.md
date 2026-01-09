# gBizINFO CSV統合バッチ - 実装完了サマリ

## 実装内容

### 作成ファイル

1. **`scripts/merge_gbizinfo_csv.ts`**
   - メインの統合バッチスクリプト
   - TypeScript/Node.jsで実装
   - ストリーミング処理対応（巨大CSV対応）

2. **`GBIZINFO_MERGE_README.md`**
   - 実行手順とドキュメント

### 追加パッケージ

- `iconv-lite`: SJIS→UTF-8変換用（既にインストール済み）

## 機能概要

### 1. 入力ファイル処理

- **Kihonjoho_UTF-8.csv**: 基本情報（親テーブル）
  - 法人番号をキーに1社1行に集約
  - 重複がある場合は警告を出して後勝ちで集約

- **Zaimujoho_UTF-8.csv**: 財務情報
  - 法人番号ごとに最新1件を自動検出
  - 日付カラムを自動検出（事業年度、決算日、開示日など）
  - 資本金、売上高、従業員数などを取り込み

- **Chotatsujoho_UTF-8.csv**: 調達情報
  - 法人番号ごとにサマリを作成
  - `procurementCount`: 調達件数
  - `procurementLatestDate`: 最新の受注日
  - `procurementLatestAmount`: 最大金額

- **Shokubajoho_SJIS_20251227.csv**: 職場情報（SJIS）
  - SJIS→UTF-8変換後に処理
  - 法人番号ごとにサマリを作成
  - `workplaceRowCount`: 職場情報の行数
  - `workplaceLatestYear`: 最新年

### 2. 出力CSV

- **出力先**: `out/gBizINFO/companies_export.csv`
- **形式**: UTF-8 / ヘッダーあり / カンマ区切り
- **主キー**: `corporateNumber`（13桁の法人番号）
- **カラム**: companies_newの既存フィールドのみ（140+カラム）

### 3. フィールドマッピング

gBizINFOのカラムをcompanies_newのフィールドにマッピング：

| gBizINFOカラム | companies_newフィールド |
|---------------|----------------------|
| 法人番号 | corporateNumber |
| 法人名 | name |
| 法人名ふりがな | kana |
| 法人名英語 | nameEn |
| 本社所在地 | address, headquartersAddress |
| 郵便番号 | postalCode |
| 法人代表者名 | representativeName |
| 法人代表者役職 | representativeTitle |
| 資本金（Zaimujoho） | capitalStock |
| 売上高（Zaimujoho） | revenue |
| 従業員数（Zaimujoho） | employeeCount |
| 事業概要 | businessDescriptions |
| 営業品目リスト | businessItems（JSON配列） |
| 企業ホームページ | companyUrl |
| 創業年 | foundingYear |
| 最終更新日 | updatedAt |

その他のフィールドは空欄（null）で出力されます。

### 4. ログ出力

処理中に以下のログが出力されます：

- 各CSVファイルの読み込み行数（10万行ごとに進捗表示）
- 法人番号の欠損・重複の警告
- JOINでヒットしなかった件数
- 出力行数
- ファイルサイズ
- 先頭5行のサンプル

## 実行方法

```bash
# 1. 依存パッケージのインストール（既に完了）
npm install

# 2. スクリプトの実行
npx tsx scripts/merge_gbizinfo_csv.ts
```

## 技術的な特徴

### ストリーミング処理

- 巨大なCSVファイル（200MB以上）をメモリ効率的に処理
- `csv-parse`のストリーミングAPIを使用
- 10万行ごとに進捗ログを出力

### メモリ効率

- 入力ファイルはストリーミングで読み込み
- 出力ファイルはストリーミングで書き込み
- 中間データはMap構造で効率的に管理

### エラーハンドリング

- 入力ファイルの存在確認
- 法人番号の欠損チェック
- 重複データの警告
- 日付カラムの自動検出失敗時の警告

## 出力CSVのサンプル

```csv
"name","kana","nameEn","corporateNumber","corporationType","address","postalCode",...
"五洋建設株式会社","","","1010001000006","","東京都文京区後楽二丁目２番８号","","",...
```

## 注意事項

1. **処理時間**: 巨大なCSVファイルを処理するため、実行には数分〜数十分かかる場合があります
2. **メモリ使用量**: 大量のデータを処理するため、十分なメモリが必要です
3. **出力ファイル**: 既存の出力ファイルは上書きされます
4. **法人番号**: 法人番号が空の行は出力から除外されます

## トラブルシューティング

### メモリ不足エラー

```bash
node --max-old-space-size=4096 -r tsx/register scripts/merge_gbizinfo_csv.ts
```

### SJISファイルの文字化け

`iconv-lite`を使用してSJIS→UTF-8変換を行っています。文字化けが発生する場合は、ファイルのエンコーディングを確認してください。

### 日付カラムが検出されない

Zaimujohoで日付カラムが検出されない場合、警告ログが出力され、年度文字列から年を抽出して判定します。

## 次のステップ

1. 実行して出力CSVを確認
2. companies_newへのインポート
3. 必要に応じてフィールドマッピングの調整

