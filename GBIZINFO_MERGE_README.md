# gBizINFO CSV統合バッチ

gBizINFOの複数CSVファイルを結合し、companies_new用のCSVを出力するバッチスクリプトです。

## 概要

- **入力**: `csv/gBizINFO/` 配下の4つのCSVファイル
  - `Kihonjoho_UTF-8.csv`（基本情報：主テーブル）
  - `Zaimujoho_UTF-8.csv`（財務情報）
  - `Chotatsujoho_UTF-8.csv`（調達情報）
  - `Shokubajoho_SJIS_20251227.csv`（職場情報：SJIS）
- **出力**: `out/gBizINFO/companies_export.csv`
- **形式**: UTF-8 / ヘッダーあり / カンマ区切り
- **主キー**: `corporateNumber`（13桁の法人番号）

## 結合ロジック

1. **Kihonjoho（基本情報）**: 親テーブルとして使用。1法人番号 = 1行
2. **Zaimujoho（財務情報）**: 最新1件を採用して結合
   - 日付カラムを自動検出（事業年度、決算日、開示日、更新日など）
   - 最新のデータを選んで、資本金・売上高・従業員数などを取り込む
3. **Chotatsujoho（調達情報）**: サマリを作成して結合
   - `procurementCount`: 調達件数
   - `procurementLatestDate`: 最新の受注日
   - `procurementLatestAmount`: 最大金額
4. **Shokubajoho（職場情報）**: SJIS→UTF-8変換後、サマリを作成して結合
   - `workplaceRowCount`: 職場情報の行数
   - `workplaceLatestYear`: 最新年（更新日や年度から抽出）

## 出力カラム

companies_newの既存フィールドのみを出力します。主なカテゴリ：

- 基本情報: `name`, `kana`, `nameEn`, `corporateNumber`, `corporationType`
- 住所・連絡先: `address`, `postalCode`, `prefecture`, `headquartersAddress`, `phoneNumber`, `email`, `companyUrl` など
- 業種・事業: `industry`, `industries`, `businessDescriptions`, `businessItems` など
- 財務・経営: `capitalStock`, `revenue`, `employeeCount`, `foundingYear`, `fiscalMonth` など
- 代表者・役員: `representativeName`, `representativeTitle` など
- 追加サマリ: `procurementCount`, `procurementLatestDate`, `workplaceRowCount`, `workplaceLatestYear`

配列型フィールド（`industries`, `businessItems`, `banks`, `tags`, `urls` など）はJSON文字列形式で出力されます。

## 実行方法

### 1. 依存パッケージのインストール

```bash
npm install
```

### 2. スクリプトの実行

```bash
npx tsx scripts/merge_gbizinfo_csv.ts
```

**実行例:**

```bash
$ npx tsx scripts/merge_gbizinfo_csv.ts
[2025-12-26T22:04:36.975Z] 🚀 gBizINFO CSV統合バッチ開始
[2025-12-26T22:04:36.976Z] 📁 出力ディレクトリ作成: .../out/gBizINFO
[2025-12-26T22:04:36.976Z] 
============================================================
[2025-12-26T22:04:36.976Z] ステップ1: Kihonjoho処理
[2025-12-26T22:04:36.976Z] ============================================================
[2025-12-26T22:04:36.976Z] 📖 UTF-8 CSV読み込み開始: Kihonjoho_UTF-8.csv
[2025-12-26T22:04:37.123Z]   📊 読み込み中: 100,000 行
[2025-12-26T22:04:37.456Z]   📊 読み込み中: 200,000 行
...
[2025-12-26T22:05:12.789Z]   ✅ 読み込み完了: 1,234,567 行
[2025-12-26T22:05:12.790Z] 🔄 Kihonjoho処理開始
[2025-12-26T22:05:13.123Z]   ✅ 処理完了: 1,200,000 社
...
```

**注意**: 巨大なCSVファイルを処理するため、実行には数分〜数十分かかる場合があります。

### 3. 出力確認

出力ファイル: `out/gBizINFO/companies_export.csv`

実行中に以下のログが出力されます：
- 各CSVファイルの読み込み行数
- 法人番号の欠損・重複の警告
- JOINでヒットしなかった件数
- 出力行数
- ファイルサイズ
- 先頭5行のサンプル

## 処理の詳細

### 重複処理

- 同一法人番号が複数存在する場合、警告を出して1件に集約
- 優先順位: 欠損が少ない行を採用（同数の場合は後勝ち）

### 法人番号の検証

- `corporateNumber`が空の行は出力から除外
- 欠損行数はログに記録

### 日付判定

- Zaimujohoの最新データ判定では、以下の順で日付カラムを検出：
  1. カラム名による判定（事業年度、決算日、開示日、更新日など）
  2. 値の形式による判定（YYYY-MM-DD形式）
  3. 年度文字列からの年抽出

### メモリ効率

- 出力時はストリーミング処理を使用
- 10,000行ごとに進捗ログを出力

## トラブルシューティング

### メモリ不足エラー

巨大なCSVファイルを扱う場合、Node.jsのメモリ制限を増やす：

```bash
node --max-old-space-size=4096 -r tsx/register scripts/merge_gbizinfo_csv.ts
```

### SJISファイルの文字化け

`iconv-lite`を使用してSJIS→UTF-8変換を行っています。文字化けが発生する場合は、ファイルのエンコーディングを確認してください。

### 日付カラムが検出されない

Zaimujohoで日付カラムが検出されない場合、警告ログが出力され、年度文字列から年を抽出して判定します。

## ファイル構成

```
scripts/
  └── merge_gbizinfo_csv.ts  # メインスクリプト

csv/gBizINFO/
  ├── Kihonjoho_UTF-8.csv
  ├── Zaimujoho_UTF-8.csv
  ├── Chotatsujoho_UTF-8.csv
  └── Shokubajoho_SJIS_20251227.csv

out/gBizINFO/
  └── companies_export.csv  # 出力ファイル
```

## 注意事項

- 入力ファイルが存在しない場合、エラーで終了します
- 出力ディレクトリは自動的に作成されます
- 既存の出力ファイルは上書きされます
- 処理には時間がかかる場合があります（ファイルサイズによる）

