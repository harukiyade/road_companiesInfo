# 2台のPCでの並列実行方法

## 概要

2台のPCで同時に実行することで、処理時間を約半分に短縮できます。
- **PC1**: 順方向実行（企業IDを小さい順から処理）
- **PC2**: 逆方向実行（企業IDを大きい順から処理）

## 実行前の準備

### 1. 両方のPCで必要なファイルを確認
- `null_fields_detailed/` 配下のCSVファイルが同じであることを確認
- Firebaseサービスアカウントキーファイルのパスを確認

### 2. スクリプトのパスを確認・修正
- **PC1**: `scripts/run_full_pc1.sh` のパスを確認
- **PC2**: `scripts/run_full_pc2.sh` のパスを確認（特に `cd` と `FIREBASE_SERVICE_ACCOUNT_KEY` のパス）

## 実行方法

### PC1（順方向実行）

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
chmod +x scripts/run_full_pc1.sh
./scripts/run_full_pc1.sh
```

または、直接コマンドで実行：

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
unset LIMIT
unset SUCCESS_LIMIT

npx tsx scripts/scrape_extended_fields.ts
```

### PC2（逆方向実行）

```bash
cd "/Users/harukishiroyama/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
chmod +x scripts/run_full_pc2.sh
./scripts/run_full_pc2.sh
```

または、直接コマンドで実行：

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
export NODE_OPTIONS="--max-old-space-size=8192"
unset LIMIT
unset SUCCESS_LIMIT

npx tsx scripts/scrape_extended_fields.ts
```

## 実行設定の詳細

### 高速化設定（精度を保ちながら最大限高速化）

| 設定項目 | 値 | 説明 |
|---------|-----|------|
| `FAST_MODE` | `true` | 高速化モードを有効化 |
| `PARALLEL_WORKERS` | `8` | 並列処理数（精度を保つ最大値） |
| `SLEEP_MS` | `200` | リクエスト間隔（精度を保つ最小値） |
| `PAGE_TIMEOUT` | `10000` | ページタイムアウト（10秒） |
| `NAVIGATION_TIMEOUT` | `12000` | ナビゲーションタイムアウト（12秒） |
| `SKIP_ON_ERROR` | `true` | エラー時にスキップして続行 |
| `REVERSE_ORDER` | PC1: `false`<br>PC2: `true` | 実行順序（PC1: 順方向、PC2: 逆方向） |

### 実行順序の違い

- **PC1（順方向）**: 企業IDを小さい順から処理（例: 1000001 → 1000002 → ...）
- **PC2（逆方向）**: 企業IDを大きい順から処理（例: 9999999 → 9999998 → ...）

これにより、2台のPCが中央で合流する形になり、効率的に処理できます。

## 実行中の確認方法

### ログファイルの確認

- **PC1**: `logs/full_all_forward_YYYYMMDD_HHMMSS.log`
- **PC2**: `logs/full_all_reverse_YYYYMMDD_HHMMSS.log`

### 進捗の確認

ログファイルから以下の情報を確認できます：
- 処理済み企業数
- 取得できたフィールド数
- エラー発生状況
- スキップされた企業数（既に取得済み）

### 実行結果の確認

スクリプト実行後、自動的に結果サマリーが表示されます：
- 取得できたドキュメントIDとフィールド
- 詳細な取得結果

## 注意事項

### 1. 既に取得済みのフィールドは自動的にスキップ
- Firestoreに既に値があるフィールドは自動的にスキップされます
- ログに「既に取得済み: X件」と表示されます

### 2. 2台で同時実行しても問題ありません
- Firestoreの更新は競合しません（各企業ごとに独立して更新）
- 同じ企業を2台で処理しても、最後に更新した値が保存されます

### 3. メモリ使用量
- PC2では `NODE_OPTIONS="--max-old-space-size=8192"` を設定して8GBのメモリを確保
- PC1でもメモリ不足が発生する場合は、同様の設定を追加してください

### 4. ネットワーク負荷
- 2台で同時実行すると、ネットワーク負荷が増加します
- 問題が発生する場合は、`PARALLEL_WORKERS` を減らすか、`SLEEP_MS` を増やしてください

## トラブルシューティング

### エラーが頻発する場合
- `SLEEP_MS` を `250` や `300` に増やす
- `PARALLEL_WORKERS` を `6` に減らす

### メモリ不足が発生する場合
- PC1にも `NODE_OPTIONS="--max-old-space-size=8192"` を追加
- `PARALLEL_WORKERS` を減らす

### 処理が遅い場合
- ネットワーク接続を確認
- 他のアプリケーションのメモリ使用量を確認
- `FAST_MODE=true` が設定されているか確認

## 実行時間の目安

- **1台で実行**: 約X日（企業数と取得成功率による）
- **2台で実行**: 約X/2日（理論上は半分、実際は若干のオーバーヘッドあり）

処理速度は以下の要因に依存します：
- 取得成功率（既に取得済みのフィールドが多いほど速い）
- ネットワーク速度
- PCの性能
- 並列処理数

