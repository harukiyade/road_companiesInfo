# gBizINFO（基本情報）と自社DBのカラムマッピングおよびアクティブ企業特定ロジック

## 前提

- **ソース**: 経済産業省 gBizINFO「法人基本情報」CSV（`Kihonjoho_UTF-8.csv` 等）
- **実ヘッダー**（プロジェクト内 `gBizINFO/Kihonjoho_UTF-8.csv` の1行目）:
  - `法人番号`, `法人名`, `法人名ふりがな`, `法人名英語`, `郵便番号`, `本社所在地`, `ステータス`, `登記記録の閉鎖等年月日`, `登記記録の閉鎖等の事由`, `法人代表者名`, `法人代表者役職`, `資本金`, `従業員数`, `企業規模詳細(男性)`, `企業規模詳細(女性)`, `営業品目リスト`, `事業概要`, `企業ホームページ`, `設立年月日`, `創業年`, `最終更新日`, `資格等級`
- **自社DB**: `companies` テーブル（LegatusONE）

---

## 1. カラムマッピング表

| 自社DBカラム | 対応可否 | gBizINFO CSV列（または補足） | 備考 |
|-------------|----------|------------------------------|------|
| **id** | △ | なし | **要加工**: 法人番号をそのまま `id` にすることが多い（13桁・一意）。別ID方針の場合は採番ロジックが必要。 |
| **name** | ○ | 法人名 | そのままマッピング可能。 |
| **company_url / contact_form_url** | △ | 企業ホームページ | **要加工**: `company_url` にそのままマッピング可能。`contact_form_url` はCSVにないため、HPを入れるか「×」扱い。法人番号公表サイトURLは別API/国税庁データで取得可能。 |
| **corporate_number** | ○ | 法人番号 | 13桁。そのままマッピング。 |
| **prefecture** | △ | 本社所在地 | **要加工**: 住所文字列から都道府県を抽出（先頭の「〇〇県」「東京都」等でパース、または都道府県リストとの突合）。 |
| **legal_form** | × | なし | **取得不可**: 基本情報CSVに「法人種別」列は含まれない。国税庁の法人番号公表データ等で補完可能。 |
| **transaction_type** | × | なし | **取得不可**: 取引種別は本CSVにはない。他CSV・リストで付与する必要あり。 |
| **latest_revenue** | × | なし | **取得不可**: 基本情報には売上なし。gBizINFO「財務情報」CSVで補完可能。 |
| **latest_profit** | × | なし | **取得不可**: 同上。財務情報CSVで補完可能。 |
| **total_assets** | × | なし | **取得不可**: 同上。 |
| **net_assets** | × | なし | **取得不可**: 同上。 |
| **capital_stock** | ○ | 資本金 | 単位（円/千円等）の表記に注意。数値化・正規化が必要な場合は△扱い。 |
| **employee_count** | ○ | 従業員数 | 数値化して投入。 |
| **founded_year** | △ | 設立年月日 / 創業年 | **要加工**: 設立年月日から年を抽出→`founding_year` 等。創業年は別列。 |
| **listing** | × | なし | **取得不可**: 上場有無は本CSVにない。東証・EDINET等のリストで補完。 |
| **related_companies** | × | なし | **取得不可**: 関連会社は本CSVにない。EDINET・スクレイピング等で補完。 |
| **securities_code** | × | なし | **取得不可**: 証券コードは本CSVにない。上場会社リストで補完。 |
| **industry_large / middle / small / detail** | △ | 営業品目リスト, 事業概要 | **要加工**: 営業品目はコード（例: 115,116,117）。コード→業種名のマスタで大/中/小に変換可能。事業概要はテキストで `industry_detail` や `business_descriptions` に格納可能。 |
| **phone_number** | × | なし | **取得不可**: 基本情報に電話番号は含まれない。法人番号公表サイト・HPスクレイピング等で補完。 |
| **fiscal_month** | × | なし | **取得不可**: 決算月は本CSVにない。有価証券報告書・EDINET等で補完。 |
| **representative_name** | ○ | 法人代表者名 | そのままマッピング。 |
| **representative_birth_date** | × | なし | **取得不可**: 代表者生年月日は本CSVにない。 |
| **representative_home_address** | × | なし | **取得不可**: 代表者住所は本CSVにない。 |
| **executives** | △ | 法人代表者名, 法人代表者役職 | **要加工**: 代表者のみ。役員一覧はないため、代表者を1名分のJSON等で格納する程度。 |

### 補足（自社DBとの対応）

- 自社の `corporation_type` → 本CSVでは **legal_form（法人種別）に相当する列なし** → ×。
- 自社の `founding_year` / `established` → CSVの「設立年月日」「創業年」から設定可能（△）。
- `related_companies` は自社では `subsidiaries` / `affiliations` 等のJSONBで保持する想定で、本CSVでは取得不可。

---

## 2. アクティブ企業の特定ロジック

約500万件の基本情報CSVから、**登記上閉鎖されていない「アクティブ企業」**（目安: 約150万件）を抽出する条件です。

### 2.1 根拠

- **ステータス**: 閉鎖時のみ「閉鎖」が入る。空欄または「閉鎖」以外＝稼働中。
- **登記記録の閉鎖等の事由**: 01=清算の結了等、11=合併による解散等、21=登記官による閉鎖、31=その他の清算の結了等。これらが入っている場合は閉鎖法人とみなす（ステータスと整合）。
- その他: 法人名が空の行は実務上除外することが多い。

### 2.2 Pandas によるフィルタリング例

```python
import pandas as pd

# 読み込み（必要に応じて chunksize で分割読み込み）
df = pd.read_csv("Kihonjoho_UTF-8.csv", dtype=str, encoding="utf-8")

# カラム名は実際のヘッダーに合わせる（ダブルクォートで囲まれた場合の例）
df.columns = df.columns.str.strip('"').str.strip()

# アクティブ企業の条件
# 1) ステータスが空（閉鎖されていない）
# 2) 登記記録の閉鎖等の事由が空（閉鎖事由がない）
# 3) 法人名が空でない（任意だが推奨）
status_col = "ステータス"
reason_col = "登記記録の閉鎖等の事由"
name_col = "法人名"

df_active = df[
    (df[status_col].isna() | (df[status_col].astype(str).str.strip() == "")) &
    (df[reason_col].isna() | (df[reason_col].astype(str).str.strip() == "")) &
    (df[name_col].notna() & (df[name_col].astype(str).str.strip() != ""))
].copy()

print(f"全件: {len(df)}, アクティブ: {len(df_active)}")
# 必要なら df_active をCSV出力やDB投入に回す
```

### 2.3 SQL で扱う場合（CSVを一時テーブルに取込済みと仮定）

```sql
-- 一時テーブル kihonjoho のカラム例:
-- corporate_number, name, name_kana, name_en, postal_code, address,
-- status, closure_date, closure_reason, representative_name, representative_title,
-- capital, employee_count, ..., established_date, updated_at

CREATE TABLE active_companies AS
SELECT *
FROM kihonjoho
WHERE (status IS NULL OR TRIM(status) = '')
  AND (closure_reason IS NULL OR TRIM(closure_reason) = '')
  AND name IS NOT NULL AND TRIM(name) <> '';
```

### 2.4 注意点

- 「法人種別」は本CSVにはないため、**株式会社のみ**など法人種別で絞る場合は、国税庁の法人番号公表データなど別ソースと結合する必要があります。
- 件数が約150万件になるかは、政府の公表件数・更新日次で変動するため、上記条件で件数を確認し、必要に応じて「創業年が古い」「資本金・従業員数が非零」などの追加条件を検討してください。

---

## 3. 不足情報の取得戦略案

マッピングで **×（取得不可）** となった項目のうち、重要なものについて、低コスト・現実的な補完案をまとめます。

| 不足項目 | 取得戦略案 | コスト・注意 |
|----------|------------|--------------|
| **財務情報**（売上・利益・総資産・純資産） | 1) **gBizINFO「法人活動情報・財務情報」CSV**を法人番号で基本情報と結合。<br>2) **EDINET**（有価証券報告書・決算短信）から上場・提出法人の数値を取得。<br>3) 非上場はgBizINFO財務情報があればそれを、なければ「なし」のまま運用。 | 無料。gBizINFO財務は届出分に限られる。EDINETはAPI/スクレイピング。 |
| **電話番号** | 1) **法人番号公表サイト**（国税庁API/ダウンロード）に電話番号が含まれる場合があるので確認し、あれば法人番号でマージ。<br>2) **企業HP**（企業ホームページ）をクロールして問い合わせページから電話番号を抽出（利用規約・robots.txt遵守）。 | 公表サイトは無料。スクレイピングは対象数に応じて負荷・規約確認が必要。 |
| **代表者生年月日・代表者住所** | 1) 原則 **取得しない**（個人情報の取り扱いが重い）。<br>2) 必要な場合のみ、**登記情報**（登記所の履歴事項証明書等）を有償で取得する運用。 | 大量取得は現実的でない。必要最小限に留める。 |
| **取引種別（transaction_type）** | 1) 既存の **取引種別CSV**（官公需等のリスト）を法人番号でマージ（`scripts/update_companies_transaction.py` と同様）。<br>2) 調達情報・補助金情報などgBizINFO他CSVから「取引実績あり」をフラグとして付与する方法も可。 | 既存スクリプト流用。無料。 |
| **上場有無・証券コード** | 1) **東証・マザーズ等の上場会社リスト**（PDF/Excel）を定期的に取得し、法人番号または会社名でマッチング。<br>2) **EDINET** の提出者一覧と証券コードの対応を利用。 | 無料。リストの更新頻度に依存。 |
| **業種（大/中/小）** | 1) **営業品目リスト**のコードを、gBizINFOまたは独自の **業種マスタ** で大・中・小に変換（△で記載した部分の具体化）。<br>2) **事業概要** をキーワードで簡易分類する方法も可。 | マスタ整備の手間のみ。無料。 |
| **決算月（fiscal_month）** | 1) **EDINET** の提出書類から決算期を取得（上場・提出法人）。<br>2) 非上場はgBizINFO「財務情報」に事業年度があればそこから算出。 | 無料。対象は提出法人に限られる。 |
| **関連会社（related_companies）** | 1) **EDINET** の「関係会社の状況」等から取得（上場・提出法人）。<br>2) 既存の `scripts/fetch_edinet_relations.py` 等を活用。 | 無料。EDINET対象法人に限る。 |
| **contact_form_url** | 1) **企業ホームページ** から「お問い合わせ」「Contact」リンクを抽出して `contact_form_url` に格納。 | スクレイピング。規約・負荷に注意。 |

### 優先度の目安

1. **まず実施**: アクティブ企業の抽出（本ドキュメント §2）＋ 基本情報で取れる項目のマッピング（id, name, corporate_number, 代表者名・役職, 資本金, 従業員数, 設立年月日, 企業ホームページ, 本社所在地→都道府県）。
2. **次に**: gBizINFO「財務情報」CSVの結合（売上・利益・総資産・純資産・決算関連）、営業品目→業種マスタの整備。
3. **必要に応じて**: 上場・証券コードのリスト結合、取引種別CSVのマージ、EDINET関係会社・決算期の取得。電話番号・問い合わせURLは範囲を限定してスクレイピング検討。

---

## 参照

- gBizINFO 法人基本情報: [データダウンロード](https://info.gbiz.go.jp/hojin/DownloadTop)、[リソース定義（PDF）](https://info.gbiz.go.jp/hojin/common/data/resourceinfo.pdf)
- 自社DDL: `backend/sql/create_companies_table.sql`、`backend/sql/add_transaction_type_column.sql`
- 取引種別更新: `scripts/update_companies_transaction.py`
- EDINET関係会社: `scripts/fetch_edinet_relations.py`、`docs/EDINET_RELATIONS_README.md`
