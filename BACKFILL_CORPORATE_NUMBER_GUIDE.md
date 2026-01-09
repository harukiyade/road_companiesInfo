# 法人番号補完バッチ実行ガイド

## 概要

`scripts/backfill_corporate_number_from_pref.ts` は、国税庁の法人番号公表サイトの全件データ（ZIPファイル）を使用して、Firestoreの `companies_new` コレクション内で **`corporateNumber == null` のドキュメントに法人番号を補完する**バッチスクリプトです。

**目的**: 法人番号がnullなものをなくすこと

## 前提条件

- Node.js 18以上
- Firebase Admin SDKのサービスアカウントキー
- 国税庁の法人番号公表サイトの全件データZIPファイル（`pref/00_zenkoku_all_20251226.zip`）

## 実行手順

### 1. DRY_RUN（試行実行）

まず、DRY_RUNモードで実行し、更新予定の件数と候補を確認します。

```bash
cd /Users/harumacmini/programming/road_companiesInfo

GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
PREF_ZIP_PATH=pref/00_zenkoku_all_20251226.zip \
DRY_RUN=1 \
LIMIT=1000 \
npx tsx scripts/backfill_corporate_number_from_pref.ts
```

**オプション説明:**
- `GOOGLE_APPLICATION_CREDENTIALS`: Firebase Admin SDKのサービスアカウントキーのパス
- `PREF_ZIP_PATH`: 国税庁の法人番号公表サイトの全件データZIPファイルのパス（デフォルト: `pref/00_zenkoku_all_20251226.zip`）
- `DRY_RUN=1`: 実際には更新せず、更新予定の内容を表示（必須: 最初は必ず1で実行）
- `LIMIT=1000`: 処理するnullドキュメント数を制限（テスト用、省略可。省略すると全件処理）
- `CSV_LIMIT=10000`: CSV読み込み時の行数制限（テスト用、省略可。省略すると全件読み込み）

### 2. 結果確認

DRY_RUN実行後、以下のファイルが `out/` ディレクトリに生成されます：

- `corporate_number_candidates.csv`: 候補複数・候補なしの一覧
- `corporate_number_update_plan.csv`: 更新予定の一覧（DRY_RUN時のみ）

### 3. 本番実行（全件処理）

DRY_RUNの結果を確認し、問題がなければ本番実行します。

```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/serviceAccountKey.json \
PREF_ZIP_PATH=pref/00_zenkoku_all_20251226.zip \
DRY_RUN=0 \
npx tsx scripts/backfill_corporate_number_from_pref.ts
```

**注意:** 
- `DRY_RUN=0` で実行すると、実際にFirestoreが更新されます
- `LIMIT` を指定しない場合、**すべての `corporateNumber == null` のドキュメント**を処理します（約13,377件）

## 出力ファイル

### corporate_number_candidates.csv

候補複数・候補なしのドキュメント一覧

| 列名 | 説明 |
|------|------|
| docId | FirestoreのドキュメントID |
| name | 企業名 |
| address | 住所 |
| postalCode | 郵便番号 |
| candidates | 法人番号の候補（`|`で区切られた複数の場合あり） |
| matchType | `multiple`（複数候補）または `none`（候補なし） |

### corporate_number_update_plan.csv

更新予定のドキュメント一覧（DRY_RUN時のみ生成）

| 列名 | 説明 |
|------|------|
| docId | FirestoreのドキュメントID |
| name | 企業名 |
| address | 住所 |
| postalCode | 郵便番号 |
| corporateNumber | 更新予定の法人番号 |

## 処理フロー

1. **ZIP展開**: 指定されたZIPファイルを展開し、CSVファイルを取得
2. **CSV読み込み**: CSVファイルをストリーミング処理で読み込み、索引を構築
   - 索引1: 正規化した社名 + 正規化した住所 → 法人番号のSet
   - 索引2: 正規化した社名 + 正規化した郵便番号 → 法人番号のSet
   - 索引3: 正規化した社名のみ → 法人番号のSet（フォールバック用、ユニークな場合のみ使用）
3. **Firestore取得**: `corporateNumber == null` のドキュメントを取得
4. **突合**: 各ドキュメントを索引と照合（優先順位: 社名+郵便番号 > 社名+住所 > 社名のみ）
5. **分類**:
   - ユニーク一致（候補1件） → 自動更新対象
   - 複数候補 → CSV出力（レビュー対象）
   - 候補なし → CSV出力（レビュー対象）
6. **更新**: DRY_RUN=0の場合、ユニーク一致のドキュメントをFirestoreにバッチ更新

## 正規化ルール

### 社名の正規化

- 法人格表記の統一: `(株)` → `株式会社`、`（株）` → `株式会社` など
- 括弧類の除去: `（）()【】「」『』［］` を除去
- 空白除去: 全角/半角スペースを除去
- 全角半角統一: 全角英数字を半角に変換
- ハイフン類の統一: 全角ハイフン、長音符などを半角ハイフンに統一
- カナ統一: 半角カナを全角カナに統一

### 住所の正規化

- 都道府県表記の統一: `ほっかいどう` → `北海道` など
- 市区町村+町域名まで抽出: 丁目/番地/建物名は除去、ただし町域名は含める
- 空白除去: 全角/半角スペースを除去
- ハイフン類の統一: 全角ハイフン、長音符などを半角ハイフンに統一

### 郵便番号の正規化

- ハイフン除去: `123-4567` → `1234567`

## 想定される落とし穴と対策

### 1. 文字コード問題

**問題**: CSVファイルの文字コードがUTF-8以外（UTF-16LE、Shift_JISなど）の場合

**対策**: 
- 自動文字コード判定機能を実装済み
- BOMをチェックしてUTF-16LEを検出
- UTF-8として読めるかチェック

### 2. 巨大ファイルでメモリ不足

**問題**: 約1.2GBのCSVファイルを一度に読み込むとメモリ不足になる可能性

**対策**:
- ストリーミング処理でCSVを読み込み
- `csv-parse`のストリーミング機能を使用
- 索引はMapで構築（メモリ効率的）

### 3. CSVの引用符/カンマ問題

**問題**: フィールド内にカンマや引用符が含まれる場合

**対策**:
- `csv-parse`の`relax_quotes: true`オプションを使用
- `relax_column_count: true`で列数の不一致を許容

### 4. Firestore readコスト

**問題**: 約13,377件のnullドキュメントを取得する際のreadコスト

**対策**:
- ページネーションで取得（1000件ずつ）
- `LIMIT`オプションでテスト時に件数を制限可能

### 5. 複数候補の判定

**問題**: 同じ社名+住所で複数の法人番号が存在する場合

**対策**:
- 複数候補は自動更新せず、CSV出力してレビュー対象とする
- ユニーク一致のみ自動更新

### 6. マッチングが0件になる問題

**問題**: FirestoreのデータとCSVのデータが一致しない可能性

**対策**:
- 社名のみの索引を追加（フォールバック）
- より詳細なデバッグログを追加
- CSV_LIMITを外して全件読み込む（デフォルト）

## 次の精度向上ステップ案

### 1. 候補多発時の改善

- **スコアリング機能**: 住所の一致度、郵便番号の一致度などでスコアを計算
- **追加の照合キー**: 電話番号、URLなども照合キーに追加
- **手動レビュー用UI**: 複数候補を効率的にレビューできるUI

### 2. 正規化の改善

- **住所の正規化強化**: 丁目/番地の表記揺れに対応（例: `1-2-3` vs `1丁目2番地3号`）
- **社名の正規化強化**: より多くの法人格表記の揺れに対応
- **都道府県/市区町村の正規化**: より詳細な表記揺れに対応

### 3. パフォーマンス改善

- **並列処理**: 複数のnullドキュメントを並列で突合
- **インデックス最適化**: 索引の構築を最適化
- **キャッシュ**: よく使う正規化結果をキャッシュ

## トラブルシューティング

### ZIP展開エラー

```bash
# unzipコマンドがインストールされているか確認
which unzip

# 手動で展開してCSVパスを直接指定することも可能（コード修正が必要）
```

### メモリ不足エラー

```bash
# Node.jsのメモリ制限を増やす
NODE_OPTIONS="--max-old-space-size=8192" npx tsx scripts/backfill_corporate_number_from_pref.ts
```

### CSVパースエラー

- CSVファイルの形式を確認
- 文字コードを手動で指定（コード修正が必要）

### マッチングが0件になる

- CSV_LIMITを外して全件読み込む（デフォルトで全件読み込み）
- デバッグログを確認して、正規化が正しく動作しているか確認
- FirestoreのデータとCSVのデータが一致しているか確認

## 実行例

### テスト実行（100件のみ）

```bash
GOOGLE_APPLICATION_CREDENTIALS=/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
PREF_ZIP_PATH=pref/00_zenkoku_all_20251226.zip \
DRY_RUN=1 \
LIMIT=100 \
CSV_LIMIT=100000 \
npx tsx scripts/backfill_corporate_number_from_pref.ts
```

### 本番実行（全件）

```bash
GOOGLE_APPLICATION_CREDENTIALS=/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
PREF_ZIP_PATH=pref/00_zenkoku_all_20251226.zip \
DRY_RUN=0 \
npx tsx scripts/backfill_corporate_number_from_pref.ts
```

**注意**: `LIMIT` と `CSV_LIMIT` を指定しない場合、すべてのnullドキュメントとCSV全件を処理します。
