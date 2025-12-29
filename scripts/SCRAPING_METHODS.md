# スクレイピング方法まとめ

このドキュメントでは、`scripts/scrape_extended_fields.ts` で実装されている各Webサイトからのスクレイピング方法をまとめます。

## 📋 目次

1. [全体の流れ](#全体の流れ)
2. [ログインが必要なサイト](#ログインが必要なサイト)
3. [各サイトのスクレイピング方法](#各サイトのスクレイピング方法)
4. [取得フィールド一覧](#取得フィールド一覧)
5. [技術的な詳細](#技術的な詳細)

---

## 全体の流れ

### 1. データソース
- `null_fields_detailed/` 配下のCSVファイルから、nullフィールドを持つ企業IDと対象フィールドを読み込む
- CSVが見つからない場合は、Firestoreの`companies_new`コレクションから全企業を取得

### 2. 処理順序
各企業に対して、以下の順序で情報を取得します：

1. **企業ホームページ** → 基本情報（電話番号、FAX、メール、問い合わせフォームURLなど）
2. **キャリタス就活 / マイナビ転職** → 電話番号
3. **マイナビ転職** → メール、株主、業種、取引先
4. **マイナビ2026** → 業種、取引先（より詳細）
5. **日本食糧新聞** → 役員、代表者生年月日（食品関連企業のみ）
6. **uSonar YELLOWPAGE** → 電話番号、FAX、代表者名、本社住所
7. **日経コンパス** → 役員情報
8. **就活会議 / バフェットコード / 官報決算DB** → 営業利益
9. **ニュース記事（Google検索）** → 代表者生年月日

### 3. 並列処理
- デフォルト: 3並列処理（`PARALLEL_WORKERS=3`）
- 高速化モード: 6並列処理（`FAST_MODE=true`時）
- ログインが必要なサイト（企業INDEXナビ、バフェットコード）は共有ブラウザインスタンスを使用
- 各ワーカーは独立したページを使用して競合を回避

---

## ログインが必要なサイト

### 企業INDEXナビ（Cnavi）
- **URL**: `https://cnavi-app.g-search.or.jp/`
- **ログイン情報**:
  - メールアドレス: `h.shiroyama@legatuscorp.com`
  - パスワード: `Furapote0403/`
- **ログイン方法**:
  1. ベースURLにアクセス（ログインページにリダイレクトされる）
  2. iframe内のログインフォームを検索
  3. メールアドレス入力フィールド（`input[name="username"]`）に入力
  4. パスワード入力フィールド（`input[name="password"]`）に入力
  5. ログインボタン（`button:has-text("ログイン")`）をクリック
  6. ログイン後のURLが `cnavi-app.g-search.or.jp` で始まることを確認

### バフェットコード
- **URL**: `https://www.buffett-code.com/global_screening`
- **ログイン情報**:
  - メールアドレス: `h.shiroyama@legatuscorp.com`
  - パスワード: `furapote0403`
- **ログイン方法**:
  1. 検索ページにアクセス
  2. ログインフォームを検索（`input[type="email"]` または `input[name*="email"]`）
  3. メールアドレスとパスワードを入力
  4. ログインボタンをクリック
  5. ログイン成功を確認

---

## 各サイトのスクレイピング方法

### 1. 企業ホームページ（`scrapeFromHomepage`）

**対象URL**: 企業の公式ホームページ（`companyUrl`フィールドから取得）

**取得フィールド**:
- `phoneNumber`（電話番号）
- `fax`（FAX番号）
- `email`（メールアドレス）
- `contactFormUrl`（問い合わせフォームURL）
- `sns`（SNSアカウント情報）

**スクレイピング手順**:
1. 企業のホームページURLに直接アクセス
2. HTMLをCheerioで解析
3. 正規表現で各種情報を抽出

---

### 2. マイナビ転職（`scrapeFromMynavi`）

**URL**: `https://tenshoku.mynavi.jp/company/`

**取得フィールド**:
- `phoneNumber`（電話番号）
- `email`（メールアドレス）
- `shareholders`（株主）
- `industryLarge`, `industryMiddle`, `industrySmall`, `industryDetail`（業種）
- `clients`（取引先）

**スクレイピング手順**:
1. 検索ページにアクセス
2. 検索テキストボックス（`input[placeholder*="企業名"]`）に企業名を入力
3. 「企業を検索する」ボタンをクリック
4. 検索結果ページで企業名を含むリンク（`a:has-text("企業名")`）をクリック
5. 詳細ページのHTMLから正規表現で情報を抽出

**注意事項**:
- 「該当する企業はありませんでした」のメッセージが表示された場合はスキップ

---

### 3. マイナビ2026（`scrapeFromMynavi2026`）

**URL**: `https://job.mynavi.jp/26/pc/search/corp.html?tab=corp`

**取得フィールド**:
- `industryLarge`, `industryMiddle`, `industrySmall`, `industryDetail`（業種）
- `clients`（取引先、より詳細）

**スクレイピング手順**:
1. 検索ページにアクセス
2. 検索テキストボックス（`input[placeholder*="企業名"]`）に企業名を入力
3. 「検索」ボタンをクリック
4. 検索結果ページで企業名を含むリンク（`a[href*="/corp/"]`）をクリック
5. 詳細ページのHTMLから正規表現で情報を抽出

---

### 4. キャリタス就活（`scrapeFromCareeritas`）

**URL**: `https://job.careerconnection.jp/`

**取得フィールド**:
- `phoneNumber`（電話番号）

**スクレイピング手順**:
1. 検索ページにアクセス
2. 検索テキストボックスに企業名を入力
3. 検索ボタンをクリック
4. 検索結果ページで企業名を含むリンクをクリック
5. 詳細ページから電話番号を抽出

**注意事項**:
- 現在このサイトは利用できない可能性があるため、関数は無効化されている可能性があります

---

### 5. uSonar YELLOWPAGE（`scrapeFromUsonarYellowpage`）

**URL**: `https://yellowpage.usonar.co.jp/`

**取得フィールド**:
- `phoneNumber`（電話番号）
- `fax`（FAX番号）
- `representativeName`（代表者名、`executives`に追加）
- `headquartersAddress`（本社住所）

**スクレイピング手順**:
1. トップページにアクセス
2. 検索テキストボックス（`input[type="text"]` または `input[placeholder*="企業名"]`）に企業名を入力
3. 虫眼鏡マーク（検索ボタン）をクリック、またはEnterキーで検索実行
4. 検索結果ページで企業名を含むリンク（`a:has-text("企業名")` または `a[href*="/company/"]`）をクリック
5. 詳細ページの「本社情報」セクションから情報を抽出
   - 電話番号: `(?:電話番号|電話|TEL|Tel)[：:\s]*([0-9-()]{10,15})`
   - FAX: `(?:FAX|Fax|fax|ファックス)[：:\s]*([0-9-()]{10,15})`
   - 代表者名: `(?:代表者名|代表者|代表取締役|社長)[：:\s]*([^\n\r]{1,50})`
   - 所在地: `(?:所在地|住所|本社)[：:\s]*([^\n\r]{10,100})`

**注意事項**:
- 「検索結果がありません」のメッセージが表示された場合はスキップ

---

### 6. バフェットコード（`scrapeOperatingIncomeFromBuffett`）

**URL**: `https://www.buffett-code.com/global_screening`

**取得フィールド**:
- `operatingIncome`（営業利益、千円単位）

**スクレイピング手順**:
1. **ログインが必要**（事前に`loginToBuffett`でログイン済みである必要がある）
2. 検索ページにアクセス
3. 検索ボックス（`input[placeholder*="会社名"]`）に企業名または日経コードを入力
4. Enterキーで検索実行
5. 検索結果ページで企業名を含むリンク（`a:has-text("企業名")`）をクリック
6. 詳細ページで「業績」タブをクリック
7. 営業利益を抽出（複数のパターンに対応）
   - パターン: `(?:営業利益|営業損益)[：:\s]*([^\n\r]{0,50})`
   - N/A、レンジ表示（-、～、〜）、非公開などの非数値表現を除外
   - 単位（億円、百万円、万円、千円）を千円に統一

**注意事項**:
- 「検索結果はありませんでした」のメッセージが表示された場合はスキップ
- マイナス値や異常に大きな値は除外

---

### 7. 官報決算データベース（`scrapeOperatingIncomeFromCatr`）

**URL**: `https://catr.jp/`

**取得フィールド**:
- `operatingIncome`（営業利益、千円単位）

**スクレイピング手順**:
1. トップページにアクセス
2. 画面上部の検索テキストボックス（`input[placeholder*="社名"]`）に企業名または法人番号を入力
3. 検索ボタン（`button:has-text("検索")`）をクリック
4. 広告を閉じる（右上の×ボタン）※あれば
5. 検索結果ページで企業名を含むリンク（`a:has-text("企業名")` または `a[href*="/company/"]`）をクリック
6. 詳細ページから営業利益を抽出

**注意事項**:
- 検索結果が表示されない場合はスキップ

---

### 8. 就活会議（`scrapeOperatingIncomeFromShukatsu`）

**URL**: `https://shukatsu-kigyo.jp/`

**取得フィールド**:
- `operatingIncome`（営業利益、千円単位）

**スクレイピング手順**:
1. 検索ページにアクセス
2. 検索テキストボックスに企業名を入力
3. 検索ボタンをクリック
4. 検索結果ページで企業名を含むリンクをクリック
5. 詳細ページから営業利益を抽出

---

### 9. 企業INDEXナビ（Cnavi）（`scrapeFromCnavi`）

**URL**: `https://cnavi-app.g-search.or.jp/`

**取得フィールド**:
- 電話番号、FAX、メール、業種、取引先、取引先銀行など

**スクレイピング手順**:
1. **ログインが必要**（事前に`loginToCnavi`でログイン済みである必要がある）
2. ベースURLにアクセス
3. 「企業名で探す」のテキストボックス（`input[placeholder*="企業名"]`）に企業名を入力
4. 「検索する」ボタン（`button:has-text("検索する")`）をクリック
5. 検索結果の企業リスト（`tr` または `.company-row`）から企業名と住所が一致する行を探す
6. 企業名のリンク（`a[href*="/company/"]` または `a[href*="/detail/"]`）をクリック
7. 詳細ページから情報を抽出

**注意事項**:
- 「指定された検索条件に一致する結果がありませんでした」のメッセージが表示された場合はスキップ

---

### 10. 日本食糧新聞（`scrapeFromNihonShokuryo`）

**URL**: `https://www.nissyoku.co.jp/search?q={企業名}`

**取得フィールド**:
- `executives`（役員情報）
- `representativeBirthDate`（代表者生年月日）

**スクレイピング手順**:
1. 検索URLに直接アクセス（企業名をURLエンコード）
2. HTMLをCheerioで解析
3. 役員情報と代表者生年月日を正規表現で抽出

**注意事項**:
- 食品関連企業（業種に「食品」「飲食」「フード」を含む）の場合のみ実行

---

### 11. 日経コンパス（`scrapeOfficersFromNikkeiCompass`）

**URL**: `https://compass.nikkei.com/search?q={企業名}`

**取得フィールド**:
- `executives`（役員情報）

**スクレイピング手順**:
1. 検索URLに直接アクセス（企業名をURLエンコード）
2. HTMLをCheerioで解析
3. 役員情報を正規表現で抽出
   - パターン: `(?:役員|取締役|監査役|執行役員)[：:\s]*([^\n\r]+)`

---

### 12. ニュース記事（Google検索）（`scrapeRepresentativeBirthDateFromNews`）

**URL**: `https://www.google.com/search?q={代表者名} {企業名} 生年月日`

**取得フィールド**:
- `representativeBirthDate`（代表者生年月日）

**スクレイピング手順**:
1. Google検索で「代表者名 + 企業名 + 生年月日」を検索
2. 検索結果のHTMLをCheerioで解析
3. 生年月日を正規表現で抽出
   - パターン1: `(?:生年月日|誕生日)[：:\s]*(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)`
   - パターン2: `(\d{4}[年/-]\d{1,2}[月/-]\d{1,2}[日]?)\s*(?:生まれ|誕生)`
   - パターン3: `(?:昭和|平成|令和)\s*(\d{1,2})年\s*(\d{1,2})月\s*(\d{1,2})日`
4. 和暦を西暦に変換（必要に応じて）

---

## 取得フィールド一覧

| フィールド名 | 取得元サイト | データ型 |
|------------|------------|---------|
| `phoneNumber` | 企業HP、キャリタス就活、マイナビ転職、uSonar YELLOWPAGE | string |
| `fax` | 企業HP、uSonar YELLOWPAGE | string |
| `email` | 企業HP、マイナビ転職 | string |
| `contactFormUrl` | 企業HP | string |
| `sns` | 企業HP | string[] |
| `industryLarge` | マイナビ転職、マイナビ2026 | string |
| `industryMiddle` | マイナビ転職、マイナビ2026 | string |
| `industrySmall` | マイナビ転職、マイナビ2026 | string |
| `industryDetail` | マイナビ転職、マイナビ2026 | string |
| `shareholders` | マイナビ転職 | string[] |
| `clients` | マイナビ転職、マイナビ2026 | string[] |
| `executives` | 日本食糧新聞、uSonar YELLOWPAGE、日経コンパス | string[] |
| `representativeName` | uSonar YELLOWPAGE（`executives`に追加） | string |
| `representativeBirthDate` | 日本食糧新聞、ニュース記事（Google検索） | string |
| `representativeHomeAddress` | - | string |
| `suppliers` | - | string[] |
| `banks` | - | string[] |
| `operatingIncome` | 就活会議、バフェットコード、官報決算DB | number（千円単位） |
| `headquartersAddress` | uSonar YELLOWPAGE | string |

---

## 技術的な詳細

### 使用技術
- **Playwright**: ヘッドレスブラウザでの自動操作
- **Cheerio**: HTML解析
- **Firebase Admin SDK**: Firestoreへのデータ保存
- **CSV Parser**: CSVファイルの読み込み

### レート制限
- デフォルト: 500ms待機（`SLEEP_MS=500`）
- 高速化モード: 300ms待機（`FAST_MODE=true`時）
- ページ読み込み待機: `domcontentloaded`（高速化モード）または`networkidle`（通常モード）
- タイムアウト: 8秒（高速化モード）または10秒（通常モード）

### エラーハンドリング
- 各スクレイピング関数は`try-catch`でエラーをキャッチ
- エラーが発生しても処理は続行（次のサイトから取得を試みる）
- `SKIP_ON_ERROR=true`を設定すると、エラー発生時にその企業をスキップ

### データ検証
取得したデータは以下の検証を通過した場合のみ保存されます：

- **電話番号**: `isValidPhoneNumber()` - 10-15桁の数字とハイフン
- **FAX**: `isValidFax()` - 10-15桁の数字とハイフン
- **メールアドレス**: `isValidEmail()` - 標準的なメールアドレス形式
- **URL**: `isValidUrl()` - 有効なURL形式
- **業種**: `isValidIndustry()` - 業種として正常な値（改行、特殊文字などを除外）
- **生年月日**: `isValidBirthDate()` - 有効な日付形式（YYYY-MM-DDなど）
- **数値**: `isValidNumber()` - 有効な数値範囲

### 並列処理の実装
```typescript
// 企業IDをチャンクに分割
const companyChunks = [];
for (let i = 0; i < companyIds.length; i += PARALLEL_WORKERS) {
  companyChunks.push(companyIds.slice(i, i + PARALLEL_WORKERS));
}

// 各チャンクを並列処理
for (const chunk of companyChunks) {
  const promises = chunk.map(async (companyId, index) => {
    const workerId = index + 1;
    return processCompany(companyId, workerId);
  });
  await Promise.allSettled(promises);
}
```

### ログ出力
- 処理ログ: `logs/scrape_extended_fields_{timestamp}.log`
- 結果CSV: `logs/scrape_extended_fields_{timestamp}.csv`
- 各企業の処理状況、取得フィールド、エラー情報を記録

---

## 環境変数

### 必須
- `FIREBASE_SERVICE_ACCOUNT_KEY`: Firebaseサービスアカウントキーのパス

### オプション
- `START_FROM_COMPANY_ID`: 指定した企業IDから処理を開始
- `REVERSE_ORDER`: `true`で企業IDを大きい順から処理
- `FAST_MODE`: `true`で高速化モード（並列数6、待機時間短縮）
- `SKIP_ON_ERROR`: `true`でエラー発生時にその企業をスキップ
- `PARALLEL_WORKERS`: 並列処理数（デフォルト: 3、FAST_MODE時は6）
- `SLEEP_MS`: リクエスト間隔（ミリ秒、デフォルト: 500ms、FAST_MODE時は300ms）
- `PAGE_WAIT_MODE`: ページ読み込み待機方法（`domcontentloaded`または`networkidle`）
- `PAGE_TIMEOUT`: ページタイムアウト（ミリ秒、デフォルト: 10000ms、FAST_MODE時は8000ms）
- `NAVIGATION_TIMEOUT`: ナビゲーションタイムアウト（ミリ秒、デフォルト: 10000ms、FAST_MODE時は8000ms）

---

## 実行コマンド例

```bash
# 基本的な実行
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
npx ts-node scripts/scrape_extended_fields.ts

# 高速化モード
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
export FAST_MODE=true
npx ts-node scripts/scrape_extended_fields.ts

# 特定の企業IDから開始
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
export START_FROM_COMPANY_ID="12345"
npx ts-node scripts/scrape_extended_fields.ts

# 逆順実行
export FIREBASE_SERVICE_ACCOUNT_KEY='/path/to/serviceAccount.json'
export REVERSE_ORDER=true
npx ts-node scripts/scrape_extended_fields.ts
```

---

## 注意事項

1. **利用規約遵守**: 各サイトの利用規約（ToS）とrobots.txtを遵守してください
2. **レート制限**: 適切な待機時間を設定してIPブロックを回避してください
3. **ログイン情報**: ログイン情報は環境変数で管理することを推奨します（現在はコード内にハードコードされています）
4. **エラーハンドリング**: エラーが発生しても処理は続行されるため、ログファイルを定期的に確認してください
5. **データ検証**: 取得したデータは検証を通過した場合のみ保存されますが、誤検出の可能性もあるため、必要に応じて手動で確認してください

