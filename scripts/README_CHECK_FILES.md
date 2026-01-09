# 各タイプのデータ確認ファイル一覧

## 📋 確認スクリプト一覧

### 1. **全タイプ確認スクリプト（新規作成）**
```bash
# 全タイプを確認
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_all_types.ts

# 特定タイプのみ確認
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_all_types.ts A
```

**出力:**
- 各タイプのCSVレコード数
- Firestoreとのマッチング結果
- 重複の詳細
- ログファイル: `logs/type_*_check_*.log`

---

### 2. **タイプA: 重複チェック**
```bash
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_duplicates_type_a.ts
```

**出力:**
- 重複グループの詳細
- レポートファイル: `TYPE_A_DUPLICATES_REPORT.txt`

---

### 3. **タイプB: 存在確認**
```bash
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_type_b_exists.ts
```

**確認内容:**
- 特定ドキュメントIDの存在確認
- 企業名・法人番号での検索

---

### 4. **タイプE: docId確認**
```bash
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_type_e_doc_ids.ts
```

**確認内容:**
- 特定企業のdocId検索

---

### 5. **汎用クエリツール**
```bash
# 企業名で検索
npx ts-node scripts/quick_query.ts name "丹羽興業株式会社"

# 法人番号で検索
npx ts-node scripts/quick_query.ts corp 1234567890123

# 総件数
npx ts-node scripts/quick_query.ts count

# ランダム表示
npx ts-node scripts/quick_query.ts random 5
```

---

### 6. **特定企業データ確認**
```bash
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_company_data.ts
```

**注意:** スクリプト内の `TARGET_CORPORATE_NUMBER` と `TARGET_DOC_ID` を編集して使用

---

### 7. **緊急確認（全タイプ）**
```bash
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
bash scripts/emergency_check_all.sh
```

**確認内容:**
- 総件数
- 特定企業の確認
- ランダムサンプル

---

## 📊 タイプ別CSVファイル数

| タイプ | CSVファイル数 | 説明 |
|--------|--------------|------|
| **A** | 55ファイル | 基本形式（法人番号なし） |
| **B** | 26ファイル | 創業あり形式 |
| **C** | 6ファイル | 直近決算情報あり（法人番号あり） |
| **D** | 6ファイル | 法人番号から始まる形式 |
| **E** | 4ファイル | 法人番号・都道府県形式 |
| **F** | 1ファイル | 決算5期分形式 |
| **G** | 4ファイル | 直近決算情報（法人番号あり、備考あり） |
| **H** | 4ファイル | 直近決算情報（法人番号なし） |
| **I** | 2ファイル | 英語ヘッダー形式 |
| **Other** | 26ファイル | その他 |

---

## 🚀 推奨確認手順

### 1. 全タイプの概要確認
```bash
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_all_types.ts
```

### 2. 特定タイプの詳細確認
```bash
# タイプAの重複チェック
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_duplicates_type_a.ts

# タイプBの存在確認
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_type_b_exists.ts
```

### 3. 個別企業の確認
```bash
# 企業名で検索
npx ts-node scripts/quick_query.ts name "企業名"

# 法人番号で検索
npx ts-node scripts/quick_query.ts corp 1234567890123
```

---

## 📁 ログファイルの場所

- `logs/type_*_check_*.log` - 各タイプの確認ログ
- `TYPE_A_DUPLICATES_REPORT.txt` - タイプAの重複レポート

---

## ⚠️ 注意事項

1. **環境変数の設定**
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json
   ```

2. **大量データの処理**
   - 全タイプ確認は時間がかかる場合があります
   - 特定タイプのみ確認することを推奨

3. **ログファイル**
   - 確認結果は自動的にログファイルに保存されます
   - `logs/` ディレクトリが自動的に作成されます

