# 企業URL補完スクリプト - インストールガイド

## 必要なパッケージのインストール

### 1. npmパッケージのインストール

プロジェクトのルートディレクトリで以下を実行：

```bash
npm install
```

これにより、以下のパッケージがインストールされます：
- `playwright` - ブラウザ自動化
- `cheerio` - HTMLパース
- `firebase-admin` - Firebase Admin SDK
- `dotenv` - 環境変数管理（既に含まれている場合）
- `typescript` - TypeScriptコンパイラ
- `ts-node` - TypeScript実行環境

### 2. Playwrightブラウザのインストール

Playwrightを使用するには、ブラウザバイナリをインストールする必要があります：

```bash
npx playwright install chromium
```

または、すべてのブラウザをインストール：

```bash
npx playwright install
```

### 3. 環境変数の設定

Firebase Admin SDKのサービスアカウントキーファイルのパスを環境変数に設定します。

#### macOS/Linux

```bash
export FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json
```

#### Windows (PowerShell)

```powershell
$env:FIREBASE_SERVICE_ACCOUNT_KEY="C:\path\to\serviceAccount.json"
```

#### Windows (Command Prompt)

```cmd
set FIREBASE_SERVICE_ACCOUNT_KEY=C:\path\to\serviceAccount.json
```

#### .envファイルを使用する場合

プロジェクトルートに `.env` ファイルを作成：

```env
FIREBASE_SERVICE_ACCOUNT_KEY=/absolute/path/to/serviceAccount.json
```

`.env` ファイルを使用する場合は、`dotenv` パッケージが必要です（既にインストールされている場合）。

## インストールの確認

### パッケージの確認

```bash
npm list playwright cheerio firebase-admin
```

### Playwrightブラウザの確認

```bash
npx playwright --version
```

### TypeScriptの確認

```bash
npx tsc --version
```

## トラブルシューティング

### Playwrightのインストールエラー

ネットワークエラーが発生する場合：

```bash
# プロキシ設定がある場合
export HTTP_PROXY=http://proxy.example.com:8080
export HTTPS_PROXY=http://proxy.example.com:8080

# 再インストール
npx playwright install chromium
```

### 権限エラー

macOS/Linuxで権限エラーが発生する場合：

```bash
sudo npx playwright install chromium
```

### ディスク容量不足

Playwrightのブラウザバイナリは約200MB必要です。ディスク容量を確認してください。

## 次のステップ

インストールが完了したら、[UPDATE_COMPANY_URLS_README.md](./UPDATE_COMPANY_URLS_README.md) を参照してスクリプトを実行してください。
