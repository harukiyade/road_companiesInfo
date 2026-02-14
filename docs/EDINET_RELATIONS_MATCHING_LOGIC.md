# EDINET関係会社とcompaniesテーブル突合ロジック案

## 目的
`edinet_relations.csv` の「子会社名」を、既存の `companies` テーブルの `name`（または `company_name`）と照合し、子会社の `id` を特定する。

## 前提

- **companies テーブル**: `id` (PK), `name`, `corporate_number` 等
- **edinet_relations.csv**: 親会社名, 子会社名, 住所, 議決権所有割合

## 突合ロジック案（優先順）

### 1. 企業名の正規化（共通前処理）

両者を同じ形式にそろえる。

```python
def normalize_company_name(name: str) -> str:
    return (
        name.replace("（株）", "株式会社")
        .replace("(株)", "株式会社")
        .replace("㈱", "株式会社")
        .replace("（有）", "有限会社")
        .replace("(有)", "有限会社")
        .replace("（合）", "合資会社")
        .replace("(合)", "合資会社")
        .replace("（名）", "合名会社")
        .replace("(名)", "合名会社")
        .strip()
    )
```

### 2. 照合ステップ（多段階）

| 段階 | 方法 | 説明 |
|------|------|------|
| 1 | **完全一致** | 正規化後の `子会社名` と `companies.name` を比較 |
| 2 | **株式会社の有無を無視** | `株式会社ABC` と `ABC` を同一扱い（前後のみ） |
| 3 | **部分一致（LIKE）** | 子会社名が companies.name の部分列として含まれる、または逆 |
| 4 | **住所との組み合わせ** | 名前のみで複数候補がある場合、住所（都道府県など）で絞り込み |
| 5 | **編集距離（Levenshtein）** | 表記揺れ（スペース、略称）に対応 |

### 3. 推奨SQL例（PostgreSQL）

```sql
-- ステップ1: 完全一致
SELECT id, name
FROM companies
WHERE TRIM(name) = :normalized_subsidiary_name
LIMIT 1;

-- ステップ2: 株式会社の有無を無視（正規化関数使用）
SELECT id, name
FROM companies
WHERE REGEXP_REPLACE(REGEXP_REPLACE(TRIM(name), '^株式会社', ''), '株式会社$', '')
    = REGEXP_REPLACE(REGEXP_REPLACE(:normalized_subsidiary_name, '^株式会社', ''), '株式会社$', '')
LIMIT 1;

-- ステップ3: 部分一致（完全一致でない場合）
SELECT id, name
FROM companies
WHERE name LIKE '%' || :subsidiary_core || '%'
   OR :subsidiary_core LIKE '%' || REGEXP_REPLACE(REGEXP_REPLACE(name, '^株式会社', ''), '株式会社$', '') || '%'
LIMIT 5;
```

### 4. 住所補助ロジック

子会社名から複数候補が出る場合、住所で絞る。

- `edinet_relations.csv` の住所から都道府県を抽出
- `companies.prefecture` と比較
- さらに `companies.address` に都道府県や市区町村が含まれるか確認

### 5. 突合結果の扱い

| ケース | アクション |
|--------|------------|
| 1件に特定 | `company_id` にその `id` をマッピング |
| 0件 | 「未突合」としてフラグを立て、手動確認用に出力 |
| 複数件 | 住所や法人番号で絞り込めなければ「要確認」フラグ |

### 6. 突合用マッピングテーブル案

```sql
CREATE TABLE IF NOT EXISTS edinet_relation_mappings (
    id SERIAL PRIMARY KEY,
    parent_company_name VARCHAR(500),
    subsidiary_name_raw VARCHAR(500),
    subsidiary_name_normalized VARCHAR(500),
    address VARCHAR(500),
    voting_rights_pct VARCHAR(20),
    company_id VARCHAR(255) REFERENCES companies(id),
    match_status VARCHAR(20),  -- 'matched', 'unmatched', 'multiple_candidates'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 7. パフォーマンス考慮（580万件）

- `companies.name` にインデックス: `CREATE INDEX idx_companies_name ON companies(name);`
- 正規化名用の検索用カラムを追加し、正規化結果を事前に格納
- バッチ処理で `edinet_relations.csv` を分割し、並列突合

### 8. 実装例（Python）

```python
def find_company_id(
    cursor,
    subsidiary_name: str,
    address: str = "",
) -> tuple[str | None, str]:
    """companies から id を検索。戻り値: (id or None, match_status)"""
    norm = normalize_company_name(subsidiary_name)
    if not norm:
        return None, "unmatched"

    # 1. 完全一致
    cursor.execute(
        "SELECT id FROM companies WHERE TRIM(name) = %s LIMIT 1",
        (norm,)
    )
    row = cursor.fetchone()
    if row:
        return row[0], "matched"

    # 2. 株式会社の有無を無視
    core = re.sub(r"^株式会社|株式会社$", "", norm)
    cursor.execute("""
        SELECT id FROM companies
        WHERE REGEXP_REPLACE(REGEXP_REPLACE(TRIM(name), '^株式会社', ''), '株式会社$', '')
            = %s
        LIMIT 1
    """, (core,))
    row = cursor.fetchone()
    if row:
        return row[0], "matched"

    # 3. 部分一致（候補が複数なら住所で絞り込み）
    cursor.execute(
        "SELECT id, prefecture, address FROM companies WHERE name LIKE %s LIMIT 10",
        (f"%{core}%",)
    )
    rows = cursor.fetchall()
    if len(rows) == 1:
        return rows[0][0], "matched"
    if len(rows) > 1 and address:
        # 住所から都道府県抽出して絞り込み
        prefs = ["北海道", "東京都", "大阪府", ...]  # または正規表現で抽出
        for r in rows:
            if any(p in (r[2] or "") for p in prefs):
                return r[0], "matched"
    if len(rows) > 1:
        return None, "multiple_candidates"

    return None, "unmatched"
```

---

## まとめ

1. **正規化**を必ず行う（(株)→株式会社 など）
2. **完全一致 → 株式会社除外 → 部分一致 → 住所補助**の順で照合
3. 未突合・複数候補は別テーブルやCSVに出力し、手動確認
4. 580万件規模では `name` インデックスと正規化名の事前計算を推奨
