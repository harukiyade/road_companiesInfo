# 整合性レポート一式 (full_integrity_20260322_044303)

- 生成日時: 2026-03-22T04:43:16
- 対象: `fixed_csv_3` の `unit_million` / `unit_yen` 配下の全 `*.csv`


## 出力ファイル（カテゴリ別）

| ファイル | 内容 |
|----------|------|
| `00_db_audit_subprocess.log` | DB突合サブプロセスログ |
| `01_csv_vs_db_audit.csv` | CSV vs DB 突合 |
| `02_csv_val_but_db_null.csv` | CSVに値あり・DB NULL |
| `03_cat1_row_column_count_mismatch.tsv` | **カテゴリ1**: ヘッダ列数と行の列数が不一致 |
| `04_cat1_shift_heuristics.tsv` | **カテゴリ1**: 郵便・法人番号・電話などの強シグナル |
| `05_cat2_date_anomalies.tsv` | **カテゴリ2**: 日付フォーマット疑い |
| `06_cat3_duplicate_headers.tsv` | **カテゴリ3**: 重複ヘッダ（マッピング衝突リスク） |
| `07_cat3_import_firsttime_j_column.tsv` | **カテゴリ3**: import_firstTime の J 列（事業構成）非空行数 |
| `08_cat3_hq_rep_address_anomalies.tsv` | **カテゴリ3**: 本社/代表の同一・乖離疑い |
| `09_cat3_unit_yen_revenue_unit_notes.tsv` | **カテゴリ3**: 単位語なし売上（1000倍ロジック注意） |
| `10_cat3_shareholder_vs_supplier_gap.tsv` | **カテゴリ3**: 株主空・仕入れ先に％（列ずれ疑い） |
| `11_cat4_ui_db_static_analysis.md` | **カテゴリ4**: UI/DB 切り分けの静的メモ |
| `12_cat5_duplicate_company_names.tsv` | **カテゴリ5**: 同一会社名の重複一覧 |
| `13_summary_by_file.tsv` | ファイル別ヒット数サマリ |
| `14_code_fix_recommendations.md` | コード修正案 |
| `15_NDA締結済_件数サマリ.tsv` | NDA=締結済 件数（ファイル別） |
| `16_csv_inventory.tsv` | 全CSV行数・エンコーディング推定 |

## サマリ（件数）

- カテゴリ1 列数不一致行: **0**
- カテゴリ1 列ズレ強シグナル行: **5017**
- カテゴリ2 日付異常セル: **272626**
- カテゴリ3 重複ヘッダファイル: **26**
- カテゴリ3 import_firstTime J列非空（行集計）: **308927**
- カテゴリ3 住所異常行: **8996**
- カテゴリ3 財務単位メモ行: **11894**
- カテゴリ3 株主/仕入れ先ギャップ行: **76**
- カテゴリ5 重複会社名グループ: **68154**
- NDA締結済（合算）: **38815**
- CSVファイル数: **182**
- DB突合: **skipped (--csv-only)**

## DB突合

```bash
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5434
export POSTGRES_SSLMODE=disable
export POSTGRES_PASSWORD='...'
python scripts/generate_full_integrity_report.py
```

> `--csv-only` または `POSTGRES_PASSWORD` 未設定のため DB 突合はスキップしました。
