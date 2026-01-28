# 企業URL補完スクリプト

Firestoreの `companies_new` コレクションに保存されている企業データの `companyUrl` と `contactFormUrl` を、Web検索（ブラウザ操作）で補完するスクリプトです。

## 概要

- **スクリプト名**: `scripts/updateCompanyUrls.ts`
- **目的**: `companyUrl` が null の企業データを検索して、企業HPと問い合わせフォームのURLを取得・更新
- **検索エンジン**: DuckDuckGo（デフォルト）または Bing
- **ブラウザ自動化**: Playwright

## 機能

1. Firestoreから `companyUrl` が null のドキュメントを取得
2. ブラウザで検索エンジンを使用して企業HPを検索
3. 企業HPにアクセスして問い合わせフォームのURLを特定
4. 取得した情報をFirestoreに更新

## 必要な環境

- Node.js (v14以上推奨)
- TypeScript
- Firebase Admin SDK のサービスアカウントキー

## インストール

### 1. 必要なパッケージのインストール

```bash
npm install
```

### 2. Playwrightブラウザのインストール

```bash
npx playwright install chromium
```

### 3. 環境変数の設定

`.env` ファイルを作成するか、環境変数を設定します：

```bash
export FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json
```

または、実行時に指定：

```bash
FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json npx ts-node scripts/updateCompanyUrls.ts
```

## 使用方法

### 基本的な実行

```bash
FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json npx ts-node scripts/updateCompanyUrls.ts
```

### 途中から再開する

処理が途中で中断した場合、`--offset` オプションを使用して途中から再開できます。

```bash
# 26件目から再開する場合（オフセットは0から開始）
FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json npx ts-node scripts/updateCompanyUrls.ts --offset 25
```

**注意**: オフセットは0から開始します。例えば、26件目から再開する場合は `--offset 25` を指定します。

### ヘルプの表示

```bash
npx ts-node scripts/updateCompanyUrls.ts --help
```

### 実行例

```bash
# macOS/Linux - 最初から実行
FIREBASE_SERVICE_ACCOUNT_KEY=/Users/username/path/to/serviceAccount.json npx ts-node scripts/updateCompanyUrls.ts

# macOS/Linux - 26件目から再開
FIREBASE_SERVICE_ACCOUNT_KEY=/Users/username/path/to/serviceAccount.json npx ts-node scripts/updateCompanyUrls.ts --offset 25

# Windows (PowerShell) - 最初から実行
$env:FIREBASE_SERVICE_ACCOUNT_KEY="C:\path\to\serviceAccount.json"; npx ts-node scripts/updateCompanyUrls.ts

# Windows (PowerShell) - 26件目から再開
$env:FIREBASE_SERVICE_ACCOUNT_KEY="C:\path\to\serviceAccount.json"; npx ts-node scripts/updateCompanyUrls.ts --offset 25
```

## 動作の流れ

1. **Firestoreからデータ取得**
   - `companies_new` コレクションから `companyUrl` が null のドキュメントを最大50件取得
   - `--offset` オプションが指定されている場合、指定された位置から処理を開始

2. **Web検索の実行**
   - 各企業について、以下の検索キーワードで検索：
     - `企業名 + 法人番号`
     - `企業名 + コーポレートサイト`
     - `企業名 + 公式サイト`
   - 検索結果のトップ（広告を除く）から企業HPのURLを取得

3. **問い合わせフォームの検索**
   - 取得した企業HPにアクセス
   - ページ内のリンクから「お問い合わせ」「Contact」「Inquiry」などのキーワードを含むリンクを検索
   - 問い合わせフォームのURLを特定

4. **Firestoreへの更新**
   - 取得した `companyUrl` と `contactFormUrl` を該当ドキュメントに更新

5. **待機処理**
   - 各検索の間に3秒〜10秒のランダムな待機時間を設ける（スクレイピング対策）

## 設定のカスタマイズ

スクリプト内の以下の定数を変更することで動作を調整できます：

```typescript
const MIN_DELAY_MS = 3000; // 最小待機時間（3秒）
const MAX_DELAY_MS = 10000; // 最大待機時間（10秒）
const PAGE_TIMEOUT_MS = 30000; // ページ読み込みタイムアウト（30秒）
const SEARCH_ENGINE = "duckduckgo"; // "duckduckgo" または "bing"
const BATCH_SIZE = 50; // バッチ処理サイズ
```

## 注意事項

### スクレイピング対策

- 各検索の間にランダムな待機時間（3〜10秒）を設けています
- 大量のデータを処理する場合は、時間がかかる可能性があります
- IPブロックを避けるため、適度な間隔で実行することを推奨します

### エラーハンドリング

- 検索で企業HPが見つからない場合、その企業はスキップされます
- 問い合わせフォームが見つからなくても、企業HPが見つかれば `companyUrl` は更新されます
- エラーが発生した場合でも、次の企業の処理は継続されます

### 検索エンジンの選択

- **DuckDuckGo（推奨）**: IPブロックのリスクが低い
- **Bing**: より多くの検索結果が得られる可能性があるが、ブロックのリスクがやや高い

検索エンジンを変更する場合は、スクリプト内の `SEARCH_ENGINE` 定数を変更してください。

## 実行結果の確認

スクリプト実行後、以下のような結果が表示されます：

```
📊 処理結果
============================================================
✅ 成功: 15 件
❌ エラー: 3 件
⚠️  スキップ: 2 件
📋 合計: 20 件
============================================================
```

## トラブルシューティング

### Playwrightのブラウザがインストールされていない

```bash
npx playwright install chromium
```

### Firebase初期化エラー

- `FIREBASE_SERVICE_ACCOUNT_KEY` 環境変数が正しく設定されているか確認
- サービスアカウントキーファイルのパスが正しいか確認
- ファイルの読み取り権限があるか確認

### 検索結果が取得できない

- ネットワーク接続を確認
- 検索エンジンのサイトがアクセス可能か確認
- 待機時間を長くする（`MIN_DELAY_MS` と `MAX_DELAY_MS` を増やす）

### タイムアウトエラー

- `PAGE_TIMEOUT_MS` の値を増やす
- ネットワーク速度を確認

## 関連ファイル

- `scripts/updateCompanyUrls.ts` - メインスクリプト
- `scripts/scrape_fumadata.ts` - 参考実装（Playwright使用例）
