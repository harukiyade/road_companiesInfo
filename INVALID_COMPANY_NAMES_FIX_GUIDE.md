# 無効な会社名の修正ガイド

## 概要

`companies_new`コレクションの`name`フィールドに「株式会社」「有限会社」などの法人格が含まれないドキュメントを洗い出し、該当CSVから再インポートする手順です。

## 手順

### ステップ1: 問題のあるドキュメントを洗い出す

```bash
GOOGLE_APPLICATION_CREDENTIALS=./serviceAccountKey.json \
npx ts-node scripts/find_invalid_company_names.ts --output invalid_company_names_report.json
```

このコマンドで以下を実行します：
- `companies_new`コレクションの全ドキュメントをスキャン
- `name`フィールドに法人格が含まれないドキュメントを特定
- 各ドキュメントがどのCSVからインポートされたかを分析
- レポートファイル（JSON）を生成

**出力される情報:**
- 問題のあるドキュメントの総数
- CSVファイル別の内訳
- 各ドキュメントの詳細（ID、名前、法人番号、ソースファイルなど）

### ステップ2: レポートを確認

生成されたレポートファイルを確認して、問題の範囲を把握します。

```bash
# レポートの内容を確認（jqが必要）
cat invalid_company_names_report.json | jq '.summary'

# CSVファイル別の内訳を確認
cat invalid_company_names_report.json | jq '.summary.byFile'
```

### ステップ3: 問題のあるドキュメントを削除（DRY RUN）

まずはDRY RUNモードで削除対象を確認します：

```bash
GOOGLE_APPLICATION_CREDENTIALS=./serviceAccountKey.json \
npx ts-node scripts/delete_invalid_company_names.ts invalid_company_names_report.json --dry-run
```

このコマンドは実際には削除せず、削除対象のドキュメントIDと名前を表示します。

### ステップ4: 実際に削除

DRY RUNの結果を確認して問題がなければ、実際に削除します：

```bash
GOOGLE_APPLICATION_CREDENTIALS=./serviceAccountKey.json \
npx ts-node scripts/delete_invalid_company_names.ts invalid_company_names_report.json
```

**注意:** この操作は元に戻せません。必ず事前にバックアップを取ってください。

### ステップ5: CSVファイルを再インポート

削除後、該当するCSVファイルを再インポートします。

#### 方法A: 自動スクリプトを使用（推奨）

```bash
./scripts/reimport_csv_files.sh invalid_company_names_report.json
```

このスクリプトは：
- レポートから再インポートが必要なCSVファイルを抽出
- 各ファイルを順番に再インポート

#### 方法B: 手動で再インポート

レポートに記載されているCSVファイルを個別に再インポート：

```bash
# 例: 36.csvを再インポート
GOOGLE_APPLICATION_CREDENTIALS=./serviceAccountKey.json \
npx ts-node scripts/import_companies_from_csv.ts csv/36.csv

# 例: 複数ファイルを一度に
GOOGLE_APPLICATION_CREDENTIALS=./serviceAccountKey.json \
npx ts-node scripts/import_companies_from_csv.ts csv/36.csv csv/53.csv csv/54.csv
```

## 法人格の判定基準

以下の法人格が含まれている場合は有効とみなします：

- **一般的な法人**: 株式会社、有限会社、合資会社、合名会社、合同会社
- **社団・財団法人**: 一般社団法人、一般財団法人、公益社団法人、公益財団法人
- **特殊法人**: 学校法人、医療法人、社会福祉法人、宗教法人
- **NPO法人**: 特定非営利活動法人、NPO法人
- **組合**: 協同組合、農業協同組合、生活協同組合
- **金融機関**: 信用金庫、信用組合、労働金庫
- **専門法人**: 税理士法人、司法書士法人、弁理士法人、行政書士法人
- **大学法人**: 国立大学法人、公立大学法人、私立大学法人
- **その他**: 投資法人、商工会議所、森林組合、農業共済組合など

旧字体（「株式會社」など）も有効とみなします。

## トラブルシューティング

### ソースファイルが不明なドキュメント

レポートに「(不明)」として表示されるドキュメントは、`lastImportSource`フィールドが設定されていません。これらのドキュメントは：

1. 手動で作成された可能性
2. 古いインポートスクリプトで作成された可能性
3. 他の方法で作成された可能性

これらのドキュメントは削除スクリプトでは削除されません。必要に応じて手動で確認・削除してください。

### 大量のドキュメントがある場合

大量のドキュメントがある場合、スキャンに時間がかかります。進捗はコンソールに表示されます。

### 再インポート時のエラー

再インポート時にエラーが発生した場合：

1. CSVファイルの形式を確認
2. 法人番号が正しく設定されているか確認
3. インポートログを確認

## 注意事項

- **必ずバックアップを取る**: 削除操作は元に戻せません
- **DRY RUNを必ず実行**: 削除前に必ずDRY RUNで確認してください
- **段階的に実行**: 大量のデータがある場合は、CSVファイルごとに段階的に実行することを推奨します

## 関連ファイル

- `scripts/find_invalid_company_names.ts` - 問題のあるドキュメントを検索
- `scripts/delete_invalid_company_names.ts` - 問題のあるドキュメントを削除
- `scripts/reimport_csv_files.sh` - CSVファイルを再インポート
- `scripts/import_companies_from_csv.ts` - CSVから企業情報をインポート
