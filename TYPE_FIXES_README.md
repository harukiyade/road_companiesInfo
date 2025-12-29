# 各タイプ別修正・統合処理ガイド

各CSVタイプに対する修正・統合処理のスクリプトと実行方法をまとめています。

---

## 📋 概要

各タイプのCSVファイルについて、以下の要件に対応した処理を実施します：

- **タイプA**: 重複企業の確認
- **タイプB, C, D**: 企業名+住所で同じ企業を特定して統合
- **タイプE**: 統合 + 特定フィールド無視 + フィールド修正
- **タイプG**: フィールド修正 + URL削除 + フィールド名統一
- **タイプH**: CSV正として統合 + フィールド修正
- **タイプI**: 統合 + 特定フィールド無視 + フィールド修正
- **タイプJ**: 統合 + フィールド修正

---

## 🚀 クイックスタート

### 1. 全タイプを一括実行（推奨）

```bash
# DRY RUN（変更なし、確認のみ）
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
./scripts/run_all_type_fixes.sh --dry-run

# 実際に実行
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
./scripts/run_all_type_fixes.sh
```

---

## 📂 各タイプ別の詳細

### タイプA: 重複チェック

**対象CSV**: `7.csv`, `8.csv`, `9.csv`, `10.csv`, `11.csv`, `12-35.csv`, `39.csv`, `52-77.csv`, `101.csv`, `104.csv`

**スクリプト**: `scripts/check_duplicates_type_a.ts`

**処理内容**:
- 企業名+住所で重複を検出
- 重複レポートを生成（`TYPE_A_DUPLICATES_REPORT.txt`）

**実行方法**:
```bash
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_duplicates_type_a.ts
```

---

### タイプB, C, D: 統合処理

**対象CSV**:
- **タイプB**: `1.csv`, `2.csv`, `53.csv`, `103.csv`, `106.csv`, `126.csv`
- **タイプC**: `23.csv`, `78-99.csv`, `100.csv`, `102.csv`, `105.csv`
- **タイプD**: `36-50.csv`, `107-117.csv`, `119.csv`, `24.csv`, `133.csv`, `134.csv`

**スクリプト**: `scripts/dedupe_and_merge_type_bcd.ts`

**処理内容**:
- 企業名+住所で同じ企業を特定して統合
- 最も情報が充実しているドキュメントをマスターとして選択
- 重複ドキュメントを削除

**実行方法**:
```bash
# DRY RUN
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/dedupe_and_merge_type_bcd.ts --dry-run

# 実行
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/dedupe_and_merge_type_bcd.ts
```

---

### タイプE: 統合 + フィールド修正

**対象CSV**: `3.csv`, `4.csv`, `5.csv`, `6.csv`, `118.csv`, `120-125.csv`

**スクリプト**: `scripts/fix_and_dedupe_type_e.ts`

**処理内容**:
- 企業名+住所で統合
- **以下のフィールドは無視**: 取引種別、SBフラグ、NDA、AD、ステータス、備考
- フィールドマッピングの修正

**実行方法**:
```bash
# DRY RUN
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_and_dedupe_type_e.ts --dry-run

# 実行
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_and_dedupe_type_e.ts
```

---

### タイプG: フィールド修正 + URL削除

**対象CSV**: `127.csv`, `128.csv`

**スクリプト**: `scripts/fix_type_g.ts`

**処理内容**:
- 英語ヘッダーを正しいフィールド名にマッピング
- **`https://valuesearch.nikkei.com`で始まるURLを削除**
- 銀行名のクリーニング（借入金額情報を削除）

**実行方法**:
```bash
# DRY RUN
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_type_g.ts --dry-run

# 実行
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_type_g.ts
```

---

### タイプH: CSV正として統合

**対象CSV**: `130.csv`, `131.csv`

**スクリプト**: `scripts/fix_and_dedupe_type_h.ts`

**処理内容**:
- **CSVの内容を正として既存データを上書き**
- 役員情報（executiveName1-10, executivePosition1-10）の処理
- 部署情報（departmentName1-7, departmentAddress1-7, departmentPhone1-7）の処理
- `executiveTitle` → `executivePosition` にマッピング

**実行方法**:
```bash
# DRY RUN
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_and_dedupe_type_h.ts --dry-run

# 実行
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_and_dedupe_type_h.ts
```

---

### タイプI: 決算情報の統合

**対象CSV**: `132.csv`

**スクリプト**: `scripts/fix_and_dedupe_type_i.ts`

**処理内容**:
- 企業名+住所で統合
- **以下のフィールドは無視**: 種別、状態、NDA締結、AD締結、担当者
- 決算月1-5、売上1-5、利益1-5の処理

**実行方法**:
```bash
# DRY RUN
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_and_dedupe_type_i.ts --dry-run

# 実行
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_and_dedupe_type_i.ts
```

---

### タイプJ: 部署情報の統合

**対象CSV**: `133.csv`, `134.csv`, `speeda/135-139.csv`

**スクリプト**: `scripts/fix_and_dedupe_type_j.ts`

**処理内容**:
- 企業名+住所で統合
- 部署・拠点情報の処理
- **会社IDフィールドは無視**

**実行方法**:
```bash
# DRY RUN
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_and_dedupe_type_j.ts --dry-run

# 実行
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_and_dedupe_type_j.ts
```

---

## 📊 処理フロー

```
1. タイプAの重複チェック
   ↓
2. タイプB, C, Dの統合処理
   ↓
3. タイプEの修正・統合処理
   ↓
4. タイプGの修正処理
   ↓
5. タイプHの修正・統合処理（CSV優先）
   ↓
6. タイプIの修正・統合処理
   ↓
7. タイプJの修正・統合処理
```

---

## 🔍 重複検出ロジック

各スクリプトは以下のロジックで重複を検出します：

1. **企業名の正規化**:
   - 空白削除
   - 小文字化
   - 「株式会社」「有限会社」などの除去

2. **住所の正規化**:
   - 空白削除
   - 小文字化

3. **キー生成**:
   ```
   キー = 正規化した企業名 + "|" + 正規化した住所（最初の30文字）
   ```

4. **追加判定**:
   - 郵便番号の一致
   - 電話番号の一致
   - URLホストの一致

---

## 📝 ログファイル

実行ログは `logs/` ディレクトリに保存されます：

```
logs/
├── type_a_check_20231215_143000.log
├── type_bcd_20231215_143500.log
├── type_e_20231215_144000.log
├── type_g_20231215_144500.log
├── type_h_20231215_145000.log
├── type_i_20231215_145500.log
└── type_j_20231215_150000.log
```

---

## ⚠️ 注意事項

### DRY RUNの推奨

初回実行時は必ず `--dry-run` フラグを使用して、変更内容を確認してください：

```bash
./scripts/run_all_type_fixes.sh --dry-run
```

### バックアップ

重要なデータベース操作を行う前に、必ずFirestoreのバックアップを取得してください。

### タイプHの特殊処理

タイプH（130.csv, 131.csv）は**CSVを正として既存データを上書き**します。
既存データが失われる可能性があるため、特に注意してください。

---

## 🛠️ トラブルシューティング

### エラー: サービスアカウントキーが見つからない

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/serviceAccountKey.json
```

### エラー: CSVファイルが見つからない

CSVファイルが以下のディレクトリに配置されていることを確認してください：
- `csv/*.csv`
- `csv/speeda/*.csv`

### メモリエラー

大量のデータを処理する場合、Node.jsのメモリ制限を増やしてください：

```bash
NODE_OPTIONS="--max-old-space-size=4096" npx ts-node scripts/...
```

---

## 📞 サポート

各スクリプトの詳細については、以下のドキュメントを参照してください：

- `CSV_TYPE_REFERENCE.md` - CSVタイプ別リファレンス
- `FIELD_MAPPING_REFERENCE.md` - フィールドマッピング一覧
- `VERIFICATION_GUIDE.md` - 検証ガイド

---

## ✅ チェックリスト

処理実行前の確認事項：

- [ ] サービスアカウントキーのパスが正しく設定されている
- [ ] 全てのCSVファイルが配置されている
- [ ] Firestoreのバックアップを取得している
- [ ] DRY RUNで動作確認を行った
- [ ] ログディレクトリ（`logs/`）が存在する

---

## 📅 実行履歴

| 日付 | タイプ | 実行者 | 結果 | 備考 |
|------|--------|--------|------|------|
| YYYY-MM-DD | 全タイプ | - | - | 初回実行 |

---

**最終更新**: 2024年12月

