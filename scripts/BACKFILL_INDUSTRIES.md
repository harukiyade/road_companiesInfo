# 業種バックフィル処理ガイド

## 概要

`scripts/backfill_industries.ts` は、`scripts/industries.csv`（正規マスタ）を基に、Firestoreの`companies_new`コレクションの業種4階層フィールドを更新するスクリプトです。

## マッピング戦略

### 1. 既存データの優先順位
- 既存の`industryDetail`があれば最優先で採用
- 既存の`industryLarge/Middle/Small`を参照
- `industry`, `industries`, `industryName`などを補助情報として使用

### 2. 正規化ルール
- 全角/半角統一（全角→半角に変換）
- スペース・タブ除去
- 括弧内の文字を除去（補助情報として活用）
- 「業」の有無を正規化

### 3. マッチング方法（優先順位）
1. **完全一致 (exact)**: 既存の階層がindustries.csvに完全に存在する
2. **正規化一致 (normalized)**: industryDetailから小分類を逆引き、industrySmallから中分類・大分類を逆引き
3. **部分一致 (fuzzy)**: テキストマッチングで一意に特定できる場合
4. **要確認 (manual-needed)**: 複数候補がある場合

### 4. 安全性
- industries.csvに存在しない組み合わせは設定しない
- 複数候補がある場合は「要確認」として出力
- マッチしない場合は「未確定」として出力

## 実行コマンド例

### DRY_RUN（確認用）

```bash
cd "/Users/harukishiroyama/Library/Mobile Documents/com~apple~CloudDocs/info_companyDetail"

export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harukishiroyama/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export DRY_RUN=1
export LIMIT=100

npx ts-node scripts/backfill_industries.ts
```

### 本番実行（少量テスト）

```bash
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harukishiroyama/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export DRY_RUN=0
export LIMIT=1000

npx ts-node scripts/backfill_industries.ts
```

### 本番実行（全件）

```bash
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harukishiroyama/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export DRY_RUN=0
# LIMITは指定しない（全件処理）

npx ts-node scripts/backfill_industries.ts
```

### 途中から再開

```bash
export FIREBASE_SERVICE_ACCOUNT_KEY='/Users/harukishiroyama/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json'
export DRY_RUN=0
export START_AFTER_ID='1234567890'  # 最後に処理したdocIdを指定

npx ts-node scripts/backfill_industries.ts
```

## 環境変数

| 変数名 | 必須 | 説明 | デフォルト |
|--------|------|------|-----------|
| `FIREBASE_SERVICE_ACCOUNT_KEY` | ✅ | Firebaseサービスアカウントキーのパス | - |
| `DRY_RUN` | ❌ | `1`の場合はFirestoreを更新せずレポートのみ出力 | `0` |
| `LIMIT` | ❌ | 処理件数上限 | なし（全件） |
| `START_AFTER_ID` | ❌ | 途中から再開する場合の開始docId | なし |

## 出力ファイル

処理完了後、`out/`ディレクトリに以下のファイルが出力されます。

### 1. `out/industry_backfill_report.csv`

全ドキュメントの更新前後の状態を記録したレポート。

**カラム:**
- `docId`: ドキュメントID
- `corporateNumber`: 法人番号
- `name`: 企業名
- `beforeLarge/Middle/Small/Detail`: 更新前の値
- `afterLarge/Middle/Small/Detail`: 更新後の値
- `method`: 判定方法 (`exact`, `normalized`, `fuzzy`, `manual-needed`, `unresolved`)
- `confidence`: 信頼度 (`high`, `medium`, `low`)
- `unresolved`: 未解決の理由（該当する場合）
- `candidates`: 候補一覧（複数候補がある場合）

### 2. `out/industry_unresolved.csv`

未確定または要確認のドキュメント一覧。`industry_backfill_report.csv`のサブセット。

## 確認手順

### 1. DRY_RUNで確認

まず、`DRY_RUN=1`で実行してレポートを確認：

```bash
export DRY_RUN=1
export LIMIT=100
npx ts-node scripts/backfill_industries.ts
```

### 2. レポートの確認ポイント

#### `industry_backfill_report.csv`を確認

1. **更新される件数**: `afterLarge/Middle/Small/Detail`が「未確定」でない件数
2. **判定方法の分布**: `method`カラムで分布を確認
   - `exact`: 完全一致（最も信頼できる）
   - `normalized`: 正規化一致（信頼できる）
   - `fuzzy`: 部分一致（確認推奨）
   - `manual-needed`: 要確認（手動確認が必要）
   - `unresolved`: 未解決（手動対応が必要）
3. **信頼度**: `confidence`カラムで`high`の割合を確認

#### `industry_unresolved.csv`を確認

1. **未確定の件数**: 全体に占める割合
2. **未確定の理由**: `unresolved`カラムの内容を確認
3. **候補の有無**: `candidates`カラムに候補がある場合は手動で確認可能

### 3. 本番実行

DRY_RUNの結果を確認し、問題がなければ本番実行：

```bash
export DRY_RUN=0
export LIMIT=1000  # まずは少量でテスト
npx ts-node scripts/backfill_industries.ts
```

### 4. 実行後の確認

1. **更新件数**: コンソール出力の「更新数」を確認
2. **未確定件数**: コンソール出力の「未確定数」を確認
3. **レポート再確認**: 更新後のレポートを確認し、期待通りの更新が行われているか検証
4. **Firestore確認**: サンプルドキュメントをFirestoreで直接確認

## 注意事項

1. **安全性**: 
   - `DRY_RUN=1`で必ず事前確認を行う
   - 初回は`LIMIT`を小さく設定してテスト実行

2. **未確定の扱い**:
   - `industry_unresolved.csv`に出力されたドキュメントは手動で確認・更新が必要
   - 候補がある場合は`candidates`カラムを参照

3. **既存データの保持**:
   - 既存の`industryDetail`は可能な限り保持される
   - industries.csvに存在しない組み合わせは設定されない

4. **バッチ処理**:
   - 大量データの場合は`LIMIT`と`START_AFTER_ID`を使って分割実行
   - エラー時は最後に処理したdocIdを記録して再開可能

## トラブルシューティング

### エラー: industries.csv が見つかりません
- `scripts/industries.csv`が存在するか確認
- ファイルパスが正しいか確認

### エラー: Firebase初期化エラー
- `FIREBASE_SERVICE_ACCOUNT_KEY`環境変数が正しく設定されているか確認
- サービスアカウントキーファイルが存在するか確認

### 更新件数が0件
- `DRY_RUN=1`になっていないか確認
- 既存データが既に正しい形式になっている可能性
- レポートで`before`と`after`を比較

### 未確定件数が多い
- `industry_unresolved.csv`を確認し、理由を分析
- industries.csvに該当する業種が存在するか確認
- 必要に応じてindustries.csvを拡充

