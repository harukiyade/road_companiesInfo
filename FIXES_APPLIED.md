# 修正内容サマリー

実行ログの分析結果に基づいて、以下の修正を実施しました。

---

## 🔧 実施した修正

### 1. タイプJ: 配列ソートエラーの修正

**問題**: 
```
TypeError: existingItems.sort is not a function
```

**原因**: 
Firestoreから取得した`banks`や`suppliers`フィールドが配列でない場合があり、`.sort()`メソッドを呼び出すとエラーになる。

**修正内容**:
```typescript
// 修正前
const existingItems = currentData[field] || [];

// 修正後
const existingItems = Array.isArray(currentData[field]) ? currentData[field] : [];
```

また、ソート時に元の配列を変更しないようにスプレッド演算子を使用：
```typescript
// 修正前
if (JSON.stringify(existingItems.sort()) !== JSON.stringify(newItems.sort())) {

// 修正後
if (JSON.stringify([...existingItems].sort()) !== JSON.stringify([...newItems].sort())) {
```

**修正ファイル**:
- `scripts/fix_and_dedupe_type_j.ts`
- `scripts/fix_and_dedupe_type_i.ts`
- `scripts/fix_type_g.ts`

---

### 2. タイプB,C,D: メモリ不足エラーの修正

**問題**:
```
FATAL ERROR: Reached heap limit Allocation failed - JavaScript heap out of memory
```

**原因**:
Firestoreから全ドキュメント（数万件）を一度に読み込もうとしたため、メモリが不足。

**修正内容**:

#### (1) バッチ処理の実装

全ドキュメントを一度に取得するのではなく、1000件ずつバッチで取得：

```typescript
// 修正前
const companiesSnap = await db.collection(COLLECTION_NAME).get();
const allDocs: CompanyDoc[] = companiesSnap.docs.map(doc => { ... });

// 修正後
const allDocs: CompanyDoc[] = [];
const BATCH_SIZE = 1000;
let lastDoc: any = null;

while (true) {
  let query = db.collection(COLLECTION_NAME).orderBy("__name__").limit(BATCH_SIZE);
  
  if (lastDoc) {
    query = query.startAfter(lastDoc);
  }
  
  const snapshot = await query.get();
  if (snapshot.empty) break;
  
  // 処理...
  
  lastDoc = snapshot.docs[snapshot.docs.length - 1];
  if (snapshot.docs.length < BATCH_SIZE) break;
}
```

#### (2) Node.jsメモリ制限の増加

実行スクリプトにメモリオプションを追加：

```bash
# 修正前
npx ts-node scripts/dedupe_and_merge_type_bcd.ts $DRY_RUN

# 修正後
NODE_OPTIONS="--max-old-space-size=8192" npx ts-node scripts/dedupe_and_merge_type_bcd.ts $DRY_RUN
```

**修正ファイル**:
- `scripts/dedupe_and_merge_type_bcd.ts`
- `scripts/run_all_type_fixes.sh`

---

## ✅ 修正済みの問題

| 問題 | タイプ | 状態 | 修正内容 |
|------|--------|------|----------|
| 配列ソートエラー | J, I, G | ✅ 修正完了 | 配列チェック追加 |
| メモリ不足 | B,C,D | ✅ 修正完了 | バッチ処理 + メモリ増加 |

---

## 🧪 テスト実行

修正後のスクリプトをテストするには：

### 個別テスト

```bash
# タイプJのテスト（DRY RUN）
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/fix_and_dedupe_type_j.ts --dry-run

# タイプB,C,Dのテスト（DRY RUN）
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
NODE_OPTIONS="--max-old-space-size=8192" \
npx ts-node scripts/dedupe_and_merge_type_bcd.ts --dry-run
```

### 全タイプ一括テスト

```bash
# DRY RUN
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
./scripts/run_all_type_fixes.sh --dry-run

# 実際に実行
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
./scripts/run_all_type_fixes.sh
```

---

## 📊 期待される結果

### タイプJ
- ✅ 配列ソートエラーが発生しない
- ✅ 7,505件のレコードを正常に処理
- ✅ 7グループの重複を統合

### タイプB,C,D
- ✅ メモリ不足エラーが発生しない
- ✅ バッチ処理で段階的にデータ取得
- ✅ 重複企業を正常に統合

---

## 🔍 前回の実行結果（修正前）

| タイプ | 状態 | レコード数 | 重複検出 |
|--------|------|------------|----------|
| A | ✅ 成功 | 27,020行 | - |
| B,C,D | ❌ エラー | - | メモリ不足 |
| E | ✅ 成功 | 17,271件 | 853グループ |
| G | ✅ 成功 | 1,350件 | - |
| H | ✅ 成功 | 14,506件 | 1グループ |
| I | ✅ 成功 | 1,406件 | 0グループ |
| J | ❌ エラー | 7,505件 | 配列ソートエラー |

---

## 📝 次のステップ

1. **DRY RUNで動作確認**
   ```bash
   ./scripts/run_all_type_fixes.sh --dry-run
   ```

2. **ログを確認**
   ```bash
   tail -f logs/type_*_$(date +%Y%m%d)*.log
   ```

3. **問題なければ本番実行**
   ```bash
   ./scripts/run_all_type_fixes.sh
   ```

---

**修正日時**: 2024年12月4日  
**修正者**: AI Assistant

