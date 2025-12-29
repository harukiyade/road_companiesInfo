# 途中から再開する方法

## 方法1: 指定した企業IDから再開（推奨）

### 手順

1. **ログファイルから最後に処理した企業IDを確認**
   ```bash
   # 最新のログファイルから最後に成功した企業IDを取得
   grep -E "\[Worker.*\] \[.*\] データ取得成功" logs/scrape_extended_fields_*.log logs/full_all_*.log 2>/dev/null | tail -1 | grep -oE '\[[0-9]+\]' | tail -1 | tr -d '[]'
   ```

2. **取得した企業IDを使って再開コマンドを実行**

   **PC1（順方向）の場合:**
   ```bash
   cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
   
   export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
   export FAST_MODE=true
   export PARALLEL_WORKERS=8
   export SLEEP_MS=200
   export PAGE_TIMEOUT=10000
   export NAVIGATION_TIMEOUT=12000
   export SKIP_ON_ERROR=true
   export REVERSE_ORDER=false
   export START_FROM_COMPANY_ID='1766039998556026547'  # ここに取得した企業IDを指定
   unset LIMIT
   unset SUCCESS_LIMIT
   
   npx tsx scripts/scrape_extended_fields.ts
   ```

   **PC2（逆方向）の場合:**
   ```bash
   cd "/Users/harukishiroyama/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
   
   export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harukishiroyama/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
   export FAST_MODE=true
   export PARALLEL_WORKERS=8
   export SLEEP_MS=200
   export PAGE_TIMEOUT=10000
   export NAVIGATION_TIMEOUT=12000
   export SKIP_ON_ERROR=true
   export REVERSE_ORDER=true
   export START_FROM_COMPANY_ID='1766039998556026547'  # ここに取得した企業IDを指定
   export NODE_OPTIONS="--max-old-space-size=8192"
   unset LIMIT
   unset SUCCESS_LIMIT
   
   npx tsx scripts/scrape_extended_fields.ts
   ```

### 注意点

- `START_FROM_COMPANY_ID` は、**その企業IDから処理を開始**します（その企業IDを含む）
- 順方向実行の場合: 指定ID以上の企業IDから開始
- 逆方向実行の場合: 指定ID以下の企業IDから開始

## 方法2: そのまま再実行（自動スキップ）

実は、**そのまま再実行しても問題ありません**。既に取得済みのフィールドは自動的にスキップされます。

### 手順

```bash
# PC1（順方向）の場合
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
./scripts/run_full_pc1.sh

# PC2（逆方向）の場合
cd "/Users/harukishiroyama/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
./scripts/run_full_pc2.sh
```

### 動作

- Firestoreから企業データを取得
- CSVのnullフィールドリストと照合
- **既に値が入っているフィールドは自動的にスキップ**
- 値がないフィールドのみを取得対象とする

### メリット

- 手動で企業IDを確認する必要がない
- 確実に全ての企業を処理（漏れがない）
- 既に取得済みのフィールドは処理しないため、効率的

## 推奨方法

**方法2（そのまま再実行）を推奨**します。理由：
- 既に取得済みのフィールドは自動的にスキップされる
- 手動で企業IDを確認する必要がない
- 処理漏れがない

ただし、特定の企業IDから開始したい場合は、**方法1**を使用してください。

## ログファイルの確認方法

最新のログファイルを確認して、処理状況を把握できます：

```bash
# 最新のログファイルを確認
tail -f logs/scrape_extended_fields_*.log

# または、最新のログファイルを表示
ls -t logs/scrape_extended_fields_*.log logs/full_all_*.log 2>/dev/null | head -1 | xargs tail -f
```

## 処理済み企業数の確認

```bash
# 最新のログファイルから処理済み企業数を確認
grep "✅.*データ取得成功" logs/scrape_extended_fields_*.log logs/full_all_*.log 2>/dev/null | wc -l

# 最後に処理した企業IDと成功数を確認
grep -E "\[Worker.*\] \[.*\] データ取得成功" logs/scrape_extended_fields_*.log logs/full_all_*.log 2>/dev/null | tail -1
```

