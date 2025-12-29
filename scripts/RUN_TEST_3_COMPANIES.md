# テスト実行コマンド（3件のデータ取得成功まで）

## 🎯 実行内容

- 最新のCSVファイル（`null_fields_detailed_2025-12-19T02-50-47.csv`）から企業を読み込み
- 実際にデータが取得できた（保存された）企業が**3件**になるまで処理を続けます
- 3件取得できたら自動的に終了します

## 📋 実行コマンド

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export FAST_MODE=true
export PARALLEL_WORKERS=1
export SLEEP_MS=500
export SKIP_ON_ERROR=true
export LIMIT=3
npx ts-node scripts/scrape_extended_fields.ts
```

## 📊 ログで確認できる情報

実行中に以下のログが出力されます：

1. **取得対象のドキュメント**: 各企業の処理開始時に企業IDと企業名が表示されます
   ```
   [Worker 1] [1000031] 株式会社北海道ダイケン
   ```

2. **取得できたフィールド**: データが取得できた場合、以下のログが表示されます
   ```
   📋 [1000031] スクレイピングで取得したデータ（保存前）:
     - phoneNumber: 03-1234-5678
     - email: info@example.com
     ...
   ```

3. **保存されるフィールド**: Firestoreに保存される前に、保存されるフィールドと値が表示されます
   ```
   📝 [1000031] 保存されるフィールドと値:
     - phoneNumber: 03-1234-5678
     - email: info@example.com
     ...
   ```

4. **保存後の確認**: 実際に保存されたフィールドが表示されます
   ```
   ✅ [1000031] 保存フィールド一覧: phoneNumber, email, ... - 処理済みフラグ設定
   ```

5. **成功カウント**: データが取得できた企業数が表示されます
   ```
   ✅ [Worker 1] [1000031] データ取得成功！ 現在の成功数: 1件
   ```

6. **終了条件**: 3件取得できた場合、以下のログが表示されます
   ```
   ✅ 成功カウント制限（3件）に達しました。処理を終了します。
   ```

## ⚙️ 環境変数の説明

| 環境変数 | 値 | 説明 |
|---------|---|------|
| `FIREBASE_SERVICE_ACCOUNT_KEY` | 必須 | Firebase認証キーのパス |
| `FAST_MODE` | `true` | 高速化モード（精度を保ちつつ高速化） |
| `PARALLEL_WORKERS` | `1` | 並列処理数（テストのため1並列） |
| `SLEEP_MS` | `500` | リクエスト間隔（ミリ秒） |
| `SKIP_ON_ERROR` | `true` | エラー発生時にスキップして続行 |
| `LIMIT` | `3` | **実際にデータが取得できた企業数が3件になるまで続ける** |

## 🔍 重要なポイント

- **LIMIT=3** は「処理する企業数を3件に制限」するのではなく、「**実際にデータが取得できた企業が3件になるまで処理を続ける**」という意味です
- データが取得できなかった企業はスキップされ、次の企業を処理します
- 3件取得できたら自動的に処理が終了します

## 📝 実行例

```bash
# 1. ディレクトリに移動
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"

# 2. 環境変数を設定
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export FAST_MODE=true
export PARALLEL_WORKERS=1
export SLEEP_MS=500
export SKIP_ON_ERROR=true
export LIMIT=3

# 3. 実行
npx ts-node scripts/scrape_extended_fields.ts
```

## 📄 ログファイルの場所

- ログファイル: `logs/scrape_extended_fields_YYYY-MM-DDTHH-MM-SS.log`
- CSVファイル: `logs/scrape_extended_fields_YYYY-MM-DDTHH-MM-SS.csv`

処理完了後、ログファイルを確認して取得できた企業とフィールドを確認できます。

