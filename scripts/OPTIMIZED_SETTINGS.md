# 高速化かつ精度を保つ最適化設定

## 🎯 最適化の考え方

精度を保ちつつ高速化するため、以下のバランスを考慮しています：

### 精度を保つための設定
1. **タイムアウト**: 十分な時間を確保（PAGE_TIMEOUT: 12秒、NAVIGATION_TIMEOUT: 15秒）
2. **待機時間**: 最小250msを確保（リクエスト間隔）
3. **ページ読み込み**: `domcontentloaded`を使用（`networkidle`より高速だが精度も十分）
4. **並列処理数**: 6並列（多すぎると精度が落ちる可能性があるため適度に制限）

### 高速化のための設定
1. **並列処理**: 6並列で同時処理
2. **待機時間**: 最小限（250ms）
3. **ページ読み込み**: `domcontentloaded`（DOM構築完了で次へ）
4. **タイムアウト**: 必要最小限（ただし精度を保つため12-15秒を確保）

---

## ⚙️ 推奨設定（高速化 + 精度重視）

### PC1（通常順序: 小さいIDから）

```bash
cd "/Users/harumacmini/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export FAST_MODE=true
export PARALLEL_WORKERS=6
export SLEEP_MS=250
export PAGE_WAIT_MODE=domcontentloaded
export PAGE_TIMEOUT=12000
export NAVIGATION_TIMEOUT=15000
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
export SLEEP_MS=250
export PAGE_WAIT_MODE=domcontentloaded
export PAGE_TIMEOUT=12000
export NAVIGATION_TIMEOUT=15000
export SKIP_ON_ERROR=true
npx ts-node scripts/scrape_extended_fields.ts
```

---

## 📊 設定値の説明

| 環境変数 | 値 | 説明 |
|---------|---|------|
| `FAST_MODE` | `true` | 高速化モードを有効化 |
| `PARALLEL_WORKERS` | `6` | 6並列処理（精度と速度のバランス） |
| `SLEEP_MS` | `250` | リクエスト間隔250ms（精度を保つ最小値） |
| `PAGE_WAIT_MODE` | `domcontentloaded` | DOM構築完了を待つ（高速で精度も十分） |
| `PAGE_TIMEOUT` | `12000` | ページタイムアウト12秒（精度を保つため） |
| `NAVIGATION_TIMEOUT` | `15000` | ナビゲーションタイムアウト15秒（精度を保つため） |
| `SKIP_ON_ERROR` | `true` | エラー時にスキップして続行 |

---

## 🔍 精度を保つための仕組み

### 1. 適切なタイムアウト設定
- **PAGE_TIMEOUT: 12秒**: ページ読み込みに十分な時間を確保
- **NAVIGATION_TIMEOUT: 15秒**: ナビゲーション完了に十分な時間を確保

### 2. 適切な待機時間
- **SLEEP_MS: 250ms**: リクエスト間隔を確保（IPブロック防止）
- **MIN_SLEEP_MS: 250ms**: 最小待機時間を確保
- **MIN_SLEEP_MS_LONG: 600ms**: 重要な操作後の待機時間

### 3. 適切なページ読み込み待機
- **domcontentloaded**: DOM構築完了を待つ（高速で精度も十分）
- `networkidle`より高速だが、必要な要素は読み込まれている

### 4. 適度な並列処理数
- **6並列**: 速度と精度のバランスが良い
- 多すぎると精度が落ちる可能性があるため制限

---

## 📈 パフォーマンス比較

### 設定例1: 高速重視（精度がやや落ちる可能性）
```bash
PARALLEL_WORKERS=8
SLEEP_MS=200
PAGE_TIMEOUT=8000
NAVIGATION_TIMEOUT=10000
```
- 速度: ⭐⭐⭐⭐⭐
- 精度: ⭐⭐⭐

### 設定例2: 精度重視（推奨）
```bash
PARALLEL_WORKERS=6
SLEEP_MS=250
PAGE_TIMEOUT=12000
NAVIGATION_TIMEOUT=15000
```
- 速度: ⭐⭐⭐⭐
- 精度: ⭐⭐⭐⭐⭐

### 設定例3: 最高精度（速度は遅い）
```bash
PARALLEL_WORKERS=3
SLEEP_MS=500
PAGE_TIMEOUT=15000
NAVIGATION_TIMEOUT=20000
PAGE_WAIT_MODE=networkidle
```
- 速度: ⭐⭐
- 精度: ⭐⭐⭐⭐⭐

---

## ⚠️ 注意事項

1. **精度と速度のトレードオフ**: より高速化したい場合は精度がやや落ちる可能性があります
2. **IPブロック**: `SLEEP_MS`を250ms以下に下げると、IPブロックのリスクが高まります
3. **タイムアウト**: `PAGE_TIMEOUT`や`NAVIGATION_TIMEOUT`を短くしすぎると、読み込みが完了する前にタイムアウトする可能性があります
4. **並列数**: `PARALLEL_WORKERS`を増やしすぎると、メモリ使用量が増加し、精度が落ちる可能性があります

---

## 🔧 カスタマイズ

必要に応じて環境変数を調整してください：

```bash
# さらに高速化したい場合（精度がやや落ちる可能性あり）
export PARALLEL_WORKERS=8
export SLEEP_MS=200

# さらに精度を重視したい場合（速度がやや遅くなる）
export PARALLEL_WORKERS=4
export SLEEP_MS=300
export PAGE_TIMEOUT=15000
export NAVIGATION_TIMEOUT=20000
```

