# スクレイピングロジックの説明

## 📋 全体の処理フロー

```
1. CSVファイルを読み込み
   ↓
2. 企業ごとにグループ化（同じ企業の複数フィールドをまとめる）
   ↓
3. Firestoreで存在確認（一括取得、100件ずつ）
   ↓
4. 存在するドキュメントのみをフィルタリング
   ↓
5. 各企業に対して並列処理（最大3企業同時）
   ├─ 検索結果ページから詳細ページURLを抽出
   ├─ 詳細ページから情報を抽出
   └─ 取得した情報をCSVとFirestoreに書き込み
   ↓
6. CSVファイルに保存
```

## 🔍 詳細な処理ステップ

### ステップ1: CSVファイルの読み込み
- `null_fields_detailed/null_fields_detailed_XXXX.csv` を読み込み
- ヘッダー: `companyId,companyName,nullFieldName,fieldCategory,fieldType,foundValue`
- 既に`foundValue`がある行はスキップ

### ステップ2: 企業ごとのグループ化
- 同じ`companyId`のフィールドをまとめる
- 例: `companyId=10000`の`phoneNumber`, `email`, `companyUrl`をまとめて処理

### ステップ3: Firestoreで存在確認
- バッチ処理（100件ずつ）でFirestoreから取得
- 存在するドキュメントIDを`existingCompanyIds`セットに保存
- 存在しないドキュメントはログに出力してスキップ

### ステップ4: 存在するドキュメントのみフィルタリング
- `filteredCompanyFields`に存在するドキュメントのみを保存
- 存在しないドキュメントはスクレイピング処理をスキップ

### ステップ5: スクレイピング処理（並列処理）
各企業に対して以下を実行：

#### 5-1. 検索URLの生成
以下のサービスから検索URLを生成：
- 企業INDEXナビ (`cnavi-app.g-search.or.jp`)
- バフェットコード (`buffett-code.com`)
- マイナビ転職 (`tenshoku.mynavi.jp`)
- マイナビ2026 (`job.mynavi.jp`)
- 全国法人リスト (`houjin.jp`)
- 官報決算データベース (`catr.jp`)
- Alarmbox (`alarmbox.jp`)

#### 5-2. 検索結果ページから詳細ページURLを抽出
- 各検索結果ページのHTMLを解析
- 企業詳細ページのリンクを抽出（最大10件）

#### 5-3. 詳細ページから情報を抽出
- 抽出された詳細ページのHTMLを解析
- サイト別の抽出ロジックを適用
- テキストから正規表現で情報を抽出

#### 5-4. 取得した情報をCSVとFirestoreに書き込み
- CSVファイルの`foundValue`列を更新
- Firestoreの`companies_new`コレクションを更新（存在確認を再度実施）

## 🛡️ companyUrlとcontactFormUrlの保護ロジック

### companyUrlの抽出ロジック

#### 除外ドメインリスト
以下のドメインを含むURLは除外：
- Google関連: `googletagmanager.com`, `google-analytics.com`, `googleapis.com`, `gstatic.com`
- SNS: `facebook.com`, `twitter.com`, `linkedin.com`, `youtube.com`, `instagram.com`
- CDN: `cdnjs.cloudflare.com`, `cdn.jsdelivr.net`, `unpkg.com`, `bootstrapcdn.com`
- その他: `amazonaws.com`, `cloudfront.net`, `azureedge.net`
- 採用サイト: `mynavi.jp`, `job.mynavi.jp`, `wantedly.com`, `green-japan.com`
- 広告関連: `doubleclick.net`, `googlesyndication.com`, `googleadservices.com`

#### 抽出パターン
1. テキストから抽出: `公式サイト: https://...` のようなパターン
2. HTMLの`<a>`タグから抽出: リンクテキストに「公式」「ホームページ」などが含まれる
3. 日本企業のドメイン（`.co.jp`, `.com.jp`）を優先

#### 検証チェック
- ✅ 除外ドメインに含まれていないか
- ✅ JSON文字（`{`, `}`, `[`, `]`）を含んでいないか
- ✅ 有効なURL形式か（`https?://`で始まり、10-200文字）
- ✅ スクリプトタグ内のリンクではないか

### contactFormUrlの抽出ロジック

#### 除外ドメインリスト
- Google関連、SNS、CDNを除外

#### 抽出パターン
1. テキストから抽出: `お問い合わせ: https://...` のようなパターン
2. HTMLの`<a>`タグから抽出: リンクテキストに「お問い合わせ」「問い合わせ」「contact」などが含まれる
3. URLに問い合わせを示すキーワードが含まれる（`contact`, `inquiry`, `form`など）

#### 検証チェック
- ✅ 除外ドメインに含まれていないか
- ✅ JSON文字（`{`, `}`, `[`, `]`）を含んでいないか
- ✅ 周辺テキストにJSONパターンがないか（`"{"`のようなパターン）
- ✅ 有効なURL形式か
- ✅ 問い合わせフォームを示すキーワードを含むか（必須）

## ⚠️ 誤った値が入らないようにする保護機能

### 1. 除外ドメインリスト
Google Tag Manager、CDN、SNSなどのURLを確実に除外

### 2. JSON文字の検出
- URL内に`{`, `}`, `[`, `]`が含まれている場合は除外
- 周辺テキストにJSONパターンがある場合も除外

### 3. URL形式の検証
- 正規表現で有効なURL形式かチェック
- 長さ制限（10-200文字）

### 4. キーワードチェック
- `companyUrl`: 「公式」「ホームページ」などのキーワードを含む
- `contactFormUrl`: 「お問い合わせ」「contact」などのキーワードを含む（必須）

### 5. HTML構造の考慮
- スクリプトタグ内のリンクは除外（`.not('script a, style a')`）
- 親要素のテキストもチェック

## 🔄 Firestore更新のロジック

### 更新対象
- ✅ 既存ドキュメントのみ更新（`.update()`を使用）
- ❌ 新規ドキュメントは作成しない

### 更新フロー
```
1. Firestoreでドキュメントの存在確認（`.get()`）
   ↓
2. 存在する場合のみ`.update()`を実行
   ↓
3. 存在しない場合はスキップ（ログに出力）
```

## 📊 並列処理の設定

- **並列リクエスト数**: 5件（`CONCURRENT_REQUESTS=5`）
- **並列フィールド処理数**: 3企業（`CONCURRENT_FIELDS=3`）
- **レート制限**: リクエスト間に300-500msの待機時間

