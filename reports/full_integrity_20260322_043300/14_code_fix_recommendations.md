# インポート・移行コード修正案（サマリ）

## 1. NDA「締結済」が有効にならない

- **原因例**: `parse_bool` が `済` の完全一致のみ True としていた。
- **対応**: `scripts/import_full_update_fast.py` の `parse_bool` に `締結済` 等を追加 **（適用済み）**。

## 2. import_firstTime 系の本社/代表住所の取り違え・株主マッピング

- **原因**: ヘッダ重複により `norm_to_idx` が最後の列だけ残る。
- **対応**: ファイル名に `import_firsttime` を含む CSV は `IMPORT_FIRSTTIME_INDEX_MAP` で列固定 **（`import_full_update_fast.py` に実装済み）**。
- **運用**: 該当 CSV を再インポート。

## 3. 直近決算年月が UI に出ない

- **原因候補**: `import_full_update_fast.py` に `直近決算年月` → `latest_fiscal_year_month` のマップがない。
- **対応案**:
  1. `HEADER_MAP` に `"直近決算年月": "latest_fiscal_year_month"` を追加。
  2. `DB_COLS` と `upsert_batch` の列リストに `latest_fiscal_year_month` を追加（テーブルに列が無ければ DDL 実行）。
  3. 文字列のまま保存するか、DATE 型に正規化するかを決める。

## 4. unit_yen の財務 1000 倍と表示の齟齬

- `parse_revenue_profit` は単位語が無い数値に **1000 倍**する。UI が千円・円を混同すると「倍されていない」ように見える。
- **対応**: UI の表示単位を DB の保存単位（円ベースの BIGINT）に合わせるか、API で変換して返す。

## 5. 列ズレ・日付フォーマット

- 本レポートの `cat1_*` / `cat2_*` TSV を元に **CSV 修正**または **列固定インデックスマップ追加**。
- 日付は `YYYY-MM-DD` または `YYYY年M月D日` 等、パーサが解釈できる形に正規化するバッチを推奨。

---
*自動生成: `generate_full_integrity_report.py`*
