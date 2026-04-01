# カテゴリ4: UI 表示漏れ vs DB 保存漏れ（静的調査メモ）

このファイルは **リポジトリ内の実装を読んだ推定**です。確定には DB の実値確認と API レスポンスの確認が必要です。

## 直近決算年月

- **CSV 列名**: `直近決算年月`
- **`scripts/import_full_update_fast.py`**: `HEADER_MAP` に **`直近決算年月` のマッピングが存在しない**ため、
  同スクリプト経由の取り込みでは **`latest_fiscal_year_month`（PostgreSQL `companies.latest_fiscal_year_month`）に保存されない**可能性が高いです。
- **DB スキーマ**: `backend/sql/create_companies_table.sql` に `latest_fiscal_year_month VARCHAR(10)` あり。
- **切り分け**:
  1. 対象法人の DB で `latest_fiscal_year_month IS NULL` か確認。
  2. NULL なら **保存漏れ（インポートマッピング）**が先に疑う。
  3. 非 NULL ならフロントの GraphQL/REST フィールドと表示コンポーネントを追う。

参考: `scripts/import_all_csv.ts` では `latestFiscalYearMonth` をセットしている例あり（別経路）。

## 代表者誕生日

- DB: `representative_birth_date DATE`
- CSV が空、または日付パース不能なら **DB に入らない**のは正常。
- 値があるのに UI に出ない場合は API が `representativeBirthDate` を返しているか確認。

## 仕入れ先（suppliers）

- `import_full_update_fast.py` は `仕入れ先` → `suppliers`（配列）にマップ。
- **`upsert_batch` のルール**: `FORCE_OVERWRITE_COLS` に含まれないため、**既存レコードは `COALESCE(companies.col, EXCLUDED.col)` で NULL のときだけ補完**。
  CSV に値があっても **DB に既に空でない別値があると上書きされない**場合がある（表示と期待がずれる原因）。
- UI が `suppliers` / `仕入れ先` を参照しているかをフロントで確認。

## 株主（shareholders）

- `株主` / `株式保有率` / `主要株主` → `shareholders`（配列）。
- **import_firstTime 形式**（`import_firstTime_*.csv`）は **列位置固定パターン**で `株式保有率` を読む（`import_full_update_fast.py` の `IMPORT_FIRSTTIME_INDEX_MAP`）。
- 重複ヘッダのみの旧ロジックでは **本社/代表の列が壊れる**ため、株主以外も連鎖的に誤る可能性あり。
- 上記と同様 **COALESCE 補完のみ**のカラムは、既存データによっては更新されない。

## NDA（締結済）

- CSV の `NDA` が `締結済` のとき、**旧 `parse_bool` は完全一致 `済` のみ True** であり `締結済` が False になる不具合があった。
- **`import_full_update_fast.parse_bool` を修正済み**（`締結済` / `契約済` / `済み` を含む場合 True）。
- それでも UI で無効なら `nda_flag` の API 露出と UI のバインドを確認。

---
*自動生成: `generate_full_integrity_report.py`*
