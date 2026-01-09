# メモリ不足エラー対策

## 問題

約537万行のCSVを処理中に、JavaScript heap out of memoryエラーが発生しています。

## 対策

### 1. Node.jsのメモリ制限を増やす（推奨）

```bash
NODE_OPTIONS="--max-old-space-size=8192" \
GOOGLE_APPLICATION_CREDENTIALS=/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
PREF_ZIP_PATH=pref/00_zenkoku_all_20251226.zip \
DRY_RUN=1 \
npx tsx scripts/backfill_corporate_number_from_pref.ts
```

**メモリサイズの目安:**
- `4096` (4GB) - 最小推奨
- `8192` (8GB) - 推奨
- `16384` (16GB) - 余裕がある場合

### 2. 索引の最適化（実装済み）

以下の最適化を実装しました：

- **複数候補の制限**: 各索引キーに対して最大2件まで保存（3件目以降は無視）
  - これにより、メモリ使用量を大幅に削減
  - ユニーク一致の判定には影響なし（1件の場合はそのまま、2件以上の場合は「複数候補」として扱う）

### 3. ログ出力の最適化

- 進捗ログの出力間隔を10,000行から50,000行に変更
- メモリ使用量の推定値を表示

## 実行例

### メモリ制限を増やして実行

```bash
NODE_OPTIONS="--max-old-space-size=8192" \
GOOGLE_APPLICATION_CREDENTIALS=/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
PREF_ZIP_PATH=pref/00_zenkoku_all_20251226.zip \
DRY_RUN=1 \
npx tsx scripts/backfill_corporate_number_from_pref.ts
```

### テスト実行（CSV_LIMITで制限）

```bash
NODE_OPTIONS="--max-old-space-size=4096" \
GOOGLE_APPLICATION_CREDENTIALS=/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
PREF_ZIP_PATH=pref/00_zenkoku_all_20251226.zip \
DRY_RUN=1 \
CSV_LIMIT=1000000 \
LIMIT=1000 \
npx tsx scripts/backfill_corporate_number_from_pref.ts
```

## 注意点

- 索引の最適化により、3件以上の候補がある場合は最初の2件のみが保存されます
- これは「複数候補」として正しく分類されるため、精度への影響はありません
- メモリが不足する場合は、`CSV_LIMIT`で処理する行数を制限してください
