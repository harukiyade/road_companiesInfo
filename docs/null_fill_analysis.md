# NULL補完レポートの分析とデータ移行時の注意点

## 概要

`audit_csv_vs_db_integrity.py` により「DB側がNULLでCSV側に値がある」レコードを抽出した結果を元に、カラム別の傾向と移行時の注意点をまとめる。

---

## 1. 集計結果から読み取れる傾向

### 1.1 件数が多くなりやすいカラム（想定）

整合性レポートの「CSVに値あり・DBがNULL/空」に該当しやすい主なカラムは以下のとおり。

| カテゴリ | カラム例 | 傾向 |
|----------|----------|------|
| **テキスト（長文）** | `overview`, `address`, `company_description` | 複数CSVで同じ企業が登場し、概要・説明がファイルごとに異なることが多い。補完余地が最も多い。 |
| **設立年・日付** | `founding_year`, `established` | CSVは「1989-06-01」形式、DBは `founding_year` が整数（年のみ）のため型変換が必要。 |
| **財務数値** | `latest_revenue`, `latest_profit`, `capital_stock` | 単位（円・万円・百万円・億円）や1000倍ルールの有無で解釈が分かれる。NULL補完時は既存インポートと同じ変換ルールを踏襲すること。 |
| **連絡先・所在地** | `postal_code`, `phone_number`, `company_url` | 文字列のためそのまま補完しやすい。桁数・形式の正規化は任意。 |
| **業種** | `industry_large`, `industry_middle`, `industry_small` | 文字列。そのまま補完可能。 |
| **代表者** | `representative_name`, `representative_home_address` | 文字列。そのまま補完可能。 |

### 1.2 サンプルから見る典型的な不一致パターン

- **overview**  
  - CSV: 長い説明文、DB: 別ソースの短い説明、または逆。  
  - **注意**: 「値の不一致」と「CSVに値あり・DBがNULL」の両方が出る。補完対象は「DBがNULL/空のときのみ」。

- **founding_year**  
  - CSV: `1989-06-01`, `1970-05-01` など日付文字列。  
  - DB: `founding_year` は **INTEGER**（西暦4桁）。  
  - **対応**: 日付から年を抽出して整数で格納する変換が必須。

- **latest_revenue / latest_profit**  
  - CSV: `2288848`（百万円単位など）、DB: `2288848000000`（円単位で1000倍済みなど）。  
  - **注意**: 既存データは「百万円×1000」等のルールで投入されているため、NULL補完時も **同じ単位・同じ倍率** で変換する必要がある。単位なし数値は既存インポートと同様「1000倍」を適用するか、仕様を確認すること。

- **capital_stock**  
  - 同上。BIGINTで単位・倍率を既存ロジックに合わせる。

---

## 2. データ移行時の注意点（型変換・破壊防止）

### 2.1 型変換が必要なカラム

| DB型 | カラム例 | 変換ルール |
|------|----------|------------|
| **INTEGER** | `founding_year`, `employee_count`, `office_count`, `factory_count`, `store_count` | 文字列から数値部分を抽出。日付形式（YYYY-MM-DD）の場合は先頭4桁を年として使用。 |
| **BIGINT** | `latest_revenue`, `latest_profit`, `capital_stock` | 「億円」「百万円」「万円」等の単位を検出し、既存インポート（`import_full_update_fast.py` の `parse_revenue_profit`）と同一ロジックで円単位に変換。単位なしは1000倍を適用する仕様に合わせる。 |
| **BOOLEAN** | `nda_flag`, `ad_flag`, `sb_flag` | 「締結済」「契約済」「true」「ストロングバイヤー」等のキーワードで True/False に変換。 |
| **VARCHAR(n)/TEXT** | `overview`, `address`, `name`, `postal_code` 等 | 前後の空白除去。VARCHAR の場合は桁数制限（n）を超えないよう truncate。 |

### 2.2 上書き事故を防ぐための原則

1. **UPDATE 条件を厳格にする**  
   - 必ず `WHERE id = %s AND (col IS NULL OR col = '')`（文字列系）または `AND col IS NULL`（数値・日付・Boolean）を付与する。  
   - 既に値が入っているレコードは **一切更新しない**。

2. **トランザクション**  
   - 補完処理は1トランザクションで実行し、エラー時はロールバックする。

3. **ドライラン**  
   - 実行前に `--dry-run` で「どの (id, カラム, 値) が更新対象か」を確認する。

4. **同一 (ID, カラム) の重複**  
   - 複数CSVから同じ企業・同じカラムがレポートに複数行ある場合、1つの値に集約する（例: 最初の非NULL、または最長の文字列）。補完スクリプト側で集約ルールを明示する。

5. **財務数値の単位**  
   - 既存DBの値が「円」「百万円×1000」等で統一されているか確認し、NULL補完時も同じルールで変換する。

---

## 3. 補完スクリプトの前提と使い方

- **入力**: `integrity_report.csv` または「CSVに値あり・DBがNULL/空」のみの `report_null_only.csv`。  
  - フルレポートを渡した場合は、スクリプト内で `不一致の種類 == "CSVに値あり・DBがNULL/空"` の行だけを対象とする。
- **NULL補完用ファイルの作成**: 整合性検査時に `--null-only` でまとめて出力できる。  
  `python scripts/audit_csv_vs_db_integrity.py --report integrity_report.csv --null-only report_null_only.csv`
- **対象**: レポートの **(ID, カラム名, CSVの値)** のうち、DB側が NULL（または空文字）のものだけを UPDATE。既存の非NULLは更新しない。
- **出力**: 更新件数・スキップ件数・エラー件数。`--dry-run` 時は実行せず一覧のみ表示。

スクリプト: `scripts/fill_null_from_integrity_report.py`
