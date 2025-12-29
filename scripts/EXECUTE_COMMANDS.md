# スクレイピング実行コマンド

## 📊 抽出結果
- 総企業数: **2,801,231件**
- nullフィールドを持つ企業数: **2,801,231件**
- nullフィールド総数: **100,329,431件**
- 出力CSV: `null_fields_detailed/null_fields_detailed_2025-12-18T18-42-03.csv`

---

## 🖥️ 2台のPCで実行する方法

### PC1（通常順序: 小さいIDから）

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export FAST_MODE=true
export PARALLEL_WORKERS=6
export SLEEP_MS=300
export SKIP_ON_ERROR=true
npx ts-node scripts/scrape_extended_fields.ts
```

### PC2（逆順序: 大きいIDから）

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export FAST_MODE=true
export REVERSE_ORDER=true
export PARALLEL_WORKERS=6
export SLEEP_MS=300
export SKIP_ON_ERROR=true
npx ts-node scripts/scrape_extended_fields.ts
```

---

## 📝 ログの確認方法

実行中は以下のログが出力されます：

### 1. スクレイピング取得データ（保存前）
```
📋 [companyId] スクレイピングで取得したデータ（保存前）:
  - phoneNumber: 03-1234-5678
  - email: info@example.com
  - executives: [3件] 山田太郎, 佐藤花子, 鈴木一郎
  ...
```

### 2. 保存されるフィールドと値（保存前）
```
💾 [companyId] Firestore保存開始: 5 フィールド
📝 [companyId] 保存されるフィールドと値:
  - phoneNumber: 03-1234-5678
  - email: info@example.com
  - executives: [3件] 山田太郎, 佐藤花子, 鈴木一郎
  ...
```

### 3. Firestore保存後の確認
```
✅ [companyId] Firestore保存完了: 5 フィールド
📋 [companyId] Firestore保存後の確認（保存されたフィールドと値）:
  ✓ phoneNumber: 03-1234-5678
  ✓ email: info@example.com
  ✓ executives: [3件] 山田太郎, 佐藤花子, 鈴木一郎
  ...
✅ [companyId] 保存フィールド一覧: phoneNumber, email, executives, ... - 処理済みフラグ設定
```

### ログファイルの確認

```bash
# リアルタイムでログを確認
tail -f logs/scrape_extended_fields_*.log

# 特定の企業IDで検索
grep "\[企業ID\]" logs/scrape_extended_fields_*.log

# 保存されたフィールドを確認
grep "保存フィールド一覧" logs/scrape_extended_fields_*.log
```

---

## 🚀 実行を開始する

上記のコマンドを2台のPCで同時に実行してください。処理は自動的に通常順序と逆順序で分割されます。

