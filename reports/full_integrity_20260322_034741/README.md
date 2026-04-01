# 整合性レポート一式 (full_integrity_20260322_034741)

- 生成日時: 2026-03-22T03:47:46
- 対象: `fixed_csv_3`（`later/` は audit スクリプト側で除外）

## 含まれるファイル

| ファイル | 内容 |
|----------|------|
| `00_db_audit_subprocess.log` | DB突合実行ログ（実行時） |
| `01_csv_vs_db_audit.csv` | CSV vs `companies` 全不一致 |
| `02_csv_val_but_db_null.csv` | CSVに値あり・DBがNULL/空 のみ |
| `03_column_shift_strong_signals.tsv` | 列ズレ疑い（電話/上場/代表者郵便等の異常） |
| `04_import_firstTime_1_date_patterns.tsv` | `import_firstTime_1.csv` の日付表記要修正行 |
| `05_NDA締結済_件数サマリ.tsv` | ファイル別・NDA=締結済の行数 |
| `06_csv_inventory.tsv` | 全CSVの行数・エンコーディング推定 |

## サマリ（数値）

- 列ズレ強シグナル: **15** 行
- import_firstTime_1 日付パターン対象: **1969** 行
- NDA締結済（全CSV合算）: **38815** 行（ファイル内訳はTSV参照）
- CSVファイル数: **182**
- DB突合: **skipped (--csv-only)**

## DB突合のやり方

```bash
export POSTGRES_HOST=127.0.0.1
export POSTGRES_PORT=5434
export POSTGRES_SSLMODE=disable
export POSTGRES_DB=postgres
export POSTGRES_PASSWORD='...'
python scripts/generate_full_integrity_report.py
```

`--csv-only` のときは `01`/`02` は生成されません。

## 確認のすすめ方

1. `01_csv_vs_db_audit.csv` を開き、種類別にフィルタ（移行漏れ / 値不一致 / NDAフラグ等）。
2. `02_...` で「CSVにはあるが画面やDBが空」の候補を優先確認。
3. `03_...` でインポート前に直すべき行を特定。
4. `04_...` で日付パース修正が必要な行を一括処理。

> 今回は `--csv-only` または `POSTGRES_PASSWORD` 未設定のためDB突合はスキップしました。
