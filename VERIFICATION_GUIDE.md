# CSV→Firestore取り込み確認ガイド

## 📊 タイプ別CSV統計

| タイプ | CSV行数 | ファイル数 | 主な内容 |
|--------|---------|-----------|----------|
| タイプA | 68,340行 | 11 | 基本情報 + 営業種目 |
| タイプB | 94,060行 | 26 | 創業・設立あり |
| タイプC | 95,308行 | 6 | 詳細情報（業種・資本金・売上） |
| タイプD | 25,140行 | 5 | 取引先情報 |
| タイプE | 10,033行 | 2 | メールアドレスあり |
| タイプF | 5,890行 | 3 | 説明・概要あり |
| タイプG | 2,386行 | 2 | 銀行・決算情報 |
| タイプH | 14,506行 | 2 | 業種展開・役員情報 |
| タイプI | 1,405行 | 1 | 決算月・売上・利益（複数年） |
| タイプJ | 7,503行 | 2 | 部署・拠点情報 |
| **合計** | **324,571行** | **60** | |

---

## 🔍 確認方法

### 1️⃣ タイプ別サンプル確認（推奨）

各タイプから3社ずつサンプリングして、Firestoreに正しくデータが入っているか確認します。

```bash
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/verify_csv_import_by_type.ts
```

**確認内容:**
- Firestoreにドキュメントが存在するか
- 各タイプの主要フィールドがnullでないか
- フィールド充足率

**実行時間:** 約2-3分

---

### 2️⃣ 特定CSVの全行確認

特定のCSVファイルの全行がFirestoreに入っているか確認します。

```bash
# 107.csvの全行確認
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/verify_specific_csv.ts csv/107.csv
```

**確認内容:**
- CSV全行の取り込み率
- Firestoreに見つからなかった企業リスト

**実行時間:** CSVサイズによる（107.csv: 約10-15分）

---

### 3️⃣ 詳細確認（verbose）

全行の詳細ログを出力します。

```bash
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/verify_specific_csv.ts csv/130.csv --verbose
```

**実行時間:** CSVサイズによる

---

### 4️⃣ 特定企業の統合確認

丹羽興業株式会社の重複統合を確認します（11件→1件の統合確認）。

```bash
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/check_niwa_kogyo.ts
```

**期待結果:** 1件に統合されている

---

## 📋 確認すべきタイプの優先順位

### 高優先度
1. **タイプC（107.csv含む）** ← 今回修正したタイプ
   ```bash
   npx ts-node scripts/verify_specific_csv.ts csv/107.csv
   ```

2. **タイプH（130.csv, 131.csv）** ← 役員情報の展開
   ```bash
   npx ts-node scripts/verify_specific_csv.ts csv/130.csv
   ```

3. **タイプA（10.csv, 11.csv）** ← businessDescriptions確認
   ```bash
   npx ts-node scripts/verify_specific_csv.ts csv/10.csv
   ```

### 中優先度
4. **タイプG（127.csv, 128.csv）** ← 銀行情報
5. **タイプJ（133.csv-136.csv）** ← 部署情報
6. **タイプI（132.csv）** ← 複数年決算

### 低優先度
7. タイプB, D, E, F ← 基本的なマッピング

---

## 🎯 確認のポイント

### ✅ 正常な状態
- Firestore存在率: **95%以上**
- フィールド充足率: **70%以上**（タイプによる）
- 重複企業: **統合されている**

### ⚠️ 要確認
- Firestore存在率: **90%未満**
  → CSVとFirestoreの不一致を調査

- フィールド充足率: **50%未満**
  → マッピングが正しいか確認

- 重複企業: **複数存在**
  → スコアリングロジックの調整が必要

---

## 🛠️ トラブルシューティング

### npm権限エラーが出る場合

ターミナルで直接実行してください（サンドボックスの制約）。

### Firestore存在率が低い場合

1. バックフィルが完了しているか確認
2. CSV内の企業名・法人番号が正しいか確認
3. ダミー法人番号（末尾9桁が0）が多い場合は名前ベース統合を確認

### フィールド充足率が低い場合

1. CSVヘッダーとFirestoreフィールドのマッピングを確認
2. `scripts/backfill_companies_from_csv.ts`の`HEADER_HINT`を確認

---

## 📌 次のステップ

確認が完了したら：

1. ✅ タイプC完了 → 他のタイプもバックフィル
   ```bash
   bash scripts/run_backfill_by_type.sh
   ```

2. ✅ 全タイプ完了 → 最終確認
   ```bash
   npx ts-node scripts/verify_csv_import_by_type.ts
   ```

3. ✅ データ確認完了 → フロントエンド実装へ

---

## 📞 サポート

問題が発生した場合は、以下の情報を確認してください：

- `scripts/backfill_companies_from_csv.ts`: バックフィルロジック
- `scripts/fix_and_dedupe_type_c.py`: タイプCのデータ修正
- `VERIFICATION_GUIDE.md`: このファイル

