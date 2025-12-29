# スクレイピング実行手順

このドキュメントでは、nullフィールドの抽出とスクレイピングの実行方法を説明します。

## 📋 手順概要

1. **nullフィールドの抽出** - `export_null_fields.ts` を実行して、nullフィールドを持つ企業をCSVに出力
2. **スクレイピング実行** - `scrape_extended_fields.ts` を実行して、nullフィールドを補完

---

## ステップ1: nullフィールドの抽出

### 実行コマンド

```bash
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
npx ts-node scripts/export_null_fields.ts
```

### 出力先

`null_fields_detailed/null_fields_detailed_{timestamp}.csv`

### CSVフォーマット

```csv
companyId,companyName,nullFieldName
1234567890,株式会社サンプル,phoneNumber
1234567890,株式会社サンプル,fax
1234567891,株式会社テスト,email
...
```

---

## ステップ2: スクレイピング実行

### 実行方法（2通り）

#### 方法1: 通常順序（小さいIDから）

```bash
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
export FAST_MODE=true
npx ts-node scripts/scrape_extended_fields.ts
```

#### 方法2: 逆順序（大きいIDから）- もう一台のPCで使用

```bash
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
export FAST_MODE=true
export REVERSE_ORDER=true
npx ts-node scripts/scrape_extended_fields.ts
```

### オプション環境変数

| 環境変数 | 説明 | デフォルト値 | 推奨値（高速化） |
|---------|------|------------|----------------|
| `FAST_MODE` | 高速化モード | `false` | `true` |
| `REVERSE_ORDER` | 逆順実行モード | `false` | もう一台のPCで使用する場合: `true` |
| `PARALLEL_WORKERS` | 並列処理数 | `3`（FAST_MODE時は`6`） | `6`（高速化時） |
| `SLEEP_MS` | リクエスト間隔（ミリ秒） | `500`（FAST_MODE時は`300`） | `300`（高速化時） |
| `START_FROM_COMPANY_ID` | 指定した企業IDから開始 | - | 途中から再開する場合に使用 |
| `SKIP_ON_ERROR` | エラー発生時にスキップ | `false` | `true`（エラー時に中断しない） |

---

## 🚀 高速実行の推奨設定

### 通常実行（PC1）

```bash
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
export FAST_MODE=true
export PARALLEL_WORKERS=6
export SLEEP_MS=300
export SKIP_ON_ERROR=true
npx ts-node scripts/scrape_extended_fields.ts
```

### 逆順実行（PC2）

```bash
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
export FAST_MODE=true
export REVERSE_ORDER=true
export PARALLEL_WORKERS=6
export SLEEP_MS=300
export SKIP_ON_ERROR=true
npx ts-node scripts/scrape_extended_fields.ts
```

---

## 📊 対象フィールド（全44フィールド）

### 基本情報（7フィールド）
- `corporateNumber` - 法人番号
- `prefecture` - 都道府県
- `address` - 住所
- `phoneNumber` - 電話番号
- `fax` - FAX番号
- `email` - メールアドレス
- `companyUrl` - 企業URL
- `contactFormUrl` - 問い合わせフォームURL

### 代表者情報（8フィールド）
- `representativeName` - 代表者名
- `representativeKana` - 代表者名（カナ）
- `representativeTitle` - 代表者役職
- `representativeBirthDate` - 代表者生年月日
- `representativePhone` - 代表者電話番号
- `representativePostalCode` - 代表者郵便番号
- `representativeHomeAddress` - 代表者自宅住所
- `representativeRegisteredAddress` - 代表者登録住所
- `representativeAlmaMater` - 代表者出身校

### 役員・組織情報（1フィールド）
- `executives` - 役員一覧

### 業種情報（5フィールド）
- `industry` - 業種
- `industryLarge` - 業種（大分類）
- `industryMiddle` - 業種（中分類）
- `industrySmall` - 業種（小分類）
- `industryDetail` - 業種（詳細）

### 財務情報（6フィールド）
- `capitalStock` - 資本金
- `revenue` - 売上高
- `operatingIncome` - 営業利益
- `totalAssets` - 総資産
- `totalLiabilities` - 総負債
- `netAssets` - 純資産

### 上場情報（4フィールド）
- `listing` - 上場区分
- `marketSegment` - 市場区分
- `latestFiscalYearMonth` - 最新決算年月
- `fiscalMonth` - 決算月

### 規模情報（5フィールド）
- `employeeCount` - 従業員数
- `factoryCount` - 工場数
- `officeCount` - オフィス数
- `storeCount` - 店舗数
- `established` - 設立日

### 取引先情報（4フィールド）
- `clients` - 取引先
- `suppliers` - 仕入先
- `shareholders` - 株主
- `banks` - 取引銀行

---

## 🔍 実行状況の確認

### ログファイルの確認

```bash
# 最新のログファイルを確認
tail -f logs/scrape_extended_fields_*.log
```

### CSV結果ファイルの確認

```bash
# 最新のCSV結果ファイルを確認
cat logs/scrape_extended_fields_*.csv | tail -20
```

---

## ⚠️ 注意事項

1. **精度を保つため**: 高速化モードでも、レート制限を適切に設定しています
2. **並列処理**: デフォルトで3並列、高速化モードで6並列実行します
3. **エラーハンドリング**: `SKIP_ON_ERROR=true`を設定すると、エラー発生時にその企業をスキップして次の企業に進みます
4. **再開機能**: 処理済みの企業は自動的にスキップされます。途中から再開する場合は`START_FROM_COMPANY_ID`を使用してください

---

## 📝 実行例

### 完全な実行例（PC1）

```bash
# 1. nullフィールドの抽出
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
npx ts-node scripts/export_null_fields.ts

# 2. スクレイピング実行（通常順序）
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export FAST_MODE=true
export PARALLEL_WORKERS=6
export SLEEP_MS=300
export SKIP_ON_ERROR=true
npx ts-node scripts/scrape_extended_fields.ts
```

### 完全な実行例（PC2 - 逆順）

```bash
# 1. nullフィールドの抽出（PC1と同じ）
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
npx ts-node scripts/export_null_fields.ts

# 2. スクレイピング実行（逆順）
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
export FAST_MODE=true
export REVERSE_ORDER=true
export PARALLEL_WORKERS=6
export SLEEP_MS=300
export SKIP_ON_ERROR=true
npx ts-node scripts/scrape_extended_fields.ts
```

---

## 🛠️ トラブルシューティング

### エラーが発生した場合

1. **ログファイルを確認**: `logs/scrape_extended_fields_*.log`を確認
2. **エラー発生企業をスキップ**: `SKIP_ON_ERROR=true`を設定して再実行
3. **途中から再開**: `START_FROM_COMPANY_ID`を使用してエラー発生企業のIDを指定

### 処理速度が遅い場合

1. **高速化モードを有効化**: `FAST_MODE=true`
2. **並列数を増やす**: `PARALLEL_WORKERS=6`（またはそれ以上）
3. **待機時間を短縮**: `SLEEP_MS=300`（またはそれ以下、ただし精度に注意）

### 精度が低下した場合

1. **待機時間を延長**: `SLEEP_MS=500`（またはそれ以上）
2. **並列数を減らす**: `PARALLEL_WORKERS=3`
3. **高速化モードを無効化**: `FAST_MODE=false`（または環境変数を削除）

