# csv_2 配下 CSV 全ヘッダー一覧

## 概要

- **ユニーク列名数**: 309（ノイズ除く実質約280）
- **csv0115**: Shift-JISのため文字化け列名あり（実体は add_20251224 と同構造）
- **import_firstTime/129.csv**: 1行目がヘッダーでなくデータの可能性（数値・URL・JSON等が列名として混入）

---

## 【1】全ユニーク列名一覧（出現ファイル数順）

※(空), Unnamed系, 文字化け, 明らかなデータ値は下位にまとめ

### 基本情報系（高頻度）

| # | 列名 | 出現ファイル数 | 想定DBカラム候補 |
|---|------|----------------|------------------|
| 1 | URL | 211 | url |
| 2 | 会社名 | 185 | name |
| 3 | 設立 | 184 | established / established_at |
| 4 | 代表者名 | 181 | representative_name |
| 5 | 株主 | 157 | shareholders |
| 6 | 代表者郵便番号 | 155 | representative_postal_code |
| 7 | 代表者住所 | 155 | representative_address |
| 8 | 代表者誕生日 | 155 | representative_birthday |
| 9 | 取締役 | 155 | directors |
| 10 | 住所 | 150 | address |
| 11 | 郵便番号 | 148 | postal_code |
| 12 | 概要 | 122 | overview |
| 13 | 都道府県 | 98 | prefecture |
| 14 | 資本金 | 98 | capital |
| 15 | 業種1 | 97 | industry_1 / industry_large |
| 16 | 上場 | 97 | listed |
| 17 | 直近利益 | 97 | recent_profit |
| 18 | 業種2 | 96 | industry_2 / industry_middle |
| 19 | 業種3 | 96 | industry_3 / industry_small |
| 20 | 説明 | 96 | description |
| 21 | 社員数 | 96 | employee_count |
| 22 | 電話番号(窓口) | 95 | phone |
| 23 | 直近売上 | 95 | recent_revenue |
| 24 | オフィス数 | 95 | office_count |
| 25 | 工場数 | 95 | factory_count |
| 26 | 店舗数 | 95 | store_count |
| 27 | 仕入れ先 | 94 | suppliers |
| 28 | 取引先 | 94 | clients |
| 29 | 取引先銀行 | 94 | banks |
| 30 | 直近決算年月 | 93 | fiscal_period_end |
| 31 | 電話番号 | 88 | phone |
| 32 | 法人番号 | 87 | corporate_number |

### 管理・取引系

| # | 列名 | 出現ファイル数 | 想定DBカラム候補 |
|---|------|----------------|------------------|
| 33 | NDA | 66 | nda_status |
| 34 | AD | 66 | ad_status |
| 35 | 営業種目 | 62 | business_items |
| 36 | 会社郵便番号 | 61 | postal_code |
| 37 | 会社住所 | 61 | address |
| 38 | 概況 | 61 | overview |
| 39 | 業種-大 | 61 | industry_large |
| 40 | 業種-中 | 61 | industry_middle |
| 41 | 業種-小 | 61 | industry_small |
| 42 | 業種-細 | 61 | industry_detail |
| 43 | 取引種別 | 39 | transaction_type |
| 44 | 備考 | 39 | notes |
| 45 | 業種（細） | 39 | industry_detail |
| 46 | 創業 | 27 | established (創業日) |
| 47 | 代表者 | 26 | representative_name |
| 48 | 株式保有率 | 26 | shareholders |
| 49 | 役員 | 26 | directors |
| 50 | 業種（大） | 26 | industry_large |
| 51 | 業種（中） | 26 | industry_middle |
| 52 | 業種（小） | 26 | industry_small |
| 53 | 業種4 | 25 | industry_4 |
| 54 | 業種 | 23 | industry |
| 55 | 企業名 | 22 | name |
| 56 | SBフラグ | 21 | sb_flag |
| 57 | ステータス | 21 | status |
| 58 | 所在地 | 21 | address / prefecture+address |
| 59 | 区分 | 21 | category |
| 60 | 売上規模（百万円） | 21 | revenue_scale |
| 61 | 企業概要 | 21 | overview |
| 62 | 状態 | 19 | status |
| 63 | 会社ID | 18 | company_id (外部) |
| 64 | リストID | 18 | list_id (外部) |
| 65 | ID | 9 | id (外部) |

### 英語ヘッダー系（import_firstTime/old127, old128, 130, 131）

| 列名 | 対応日本語 |
|------|------------|
| name | 会社名 |
| corporateNumber | 法人番号 |
| representativeName | 代表者名 |
| url / companyUrl | URL |
| address | 住所 |
| prefecture | 都道府県 |
| phoneNumber | 電話番号 |
| established | 設立 |
| fiscalMonth | 決算月 |
| revenue | 売上 |
| capitalStock | 資本金 |
| listing | 上場 |
| employeeCount | 従業員数 |
| industryLarge | 業種-大 |
| industryMiddle | 業種-中 |
| industrySmall | 業種-小 |
| industryDetail | 業種-細 |
| overview | 概要 |
| history | 沿革 |
| banks | 取引先銀行 |
| contactUrl | お問い合わせURL |
| detailUrl | 詳細URL |

### 英語JSON系（old127, old128）

topTabsJson, leftNavJson, summaryJson, overviewTabJson, orgJson, basicJson, financeJson, compareMAJson, shareholdersJson, shareholdersMeetingJson, esgJson, statementsJson, notesJson, analysisJson, segmentsJson, bankBorrowingsJson, forecastJson

### import_firstTime/127, 128 固有

会社名（英語）, 売上, 発行株式数, 決算月, businessDescriptions, 銀行, affiliations, totalAssets, totalLiabilities, netAssets, revenueFromStatements, operatingIncome, 従業員数

### import_firstTime/132 固有

種別, NDA締結, AD締結, 担当者, 決算月1〜5, 売上1〜5, 利益1〜5

### import_firstTime/51 固有

ジャンル, 業種（分類１〜３）, FAX番号, メールアドレス, 企業ホームページURL, お問い合わせURL, 部署・拠点名, 会社情報・備考, 得意分野, 設立年月日, 上場区分, 子会社・関連会社, 主要株主, 決算期, 売上高, 経常利益, 事業内容, [募集人数][実績][主な取引銀行] 等

### csv0108/5 固有（財務・決算）

商号又は名称かな, 商号又は名称, 自己資本, 営業所名, 営業所郵便番号, 営業所電話番号, 営業所所在地, 事業年度＿自年月, 事業年度＿至年月, 合計＿官公庁, 合計＿民間, 合計＿うち下請, 合計＿海外, 合計＿計, 法人＿決算年月日, 法人＿会計税率, 法人＿流動資産合計〜法人＿当期純利益(損失) 等

### import_firstTime/130 固有（部署・役員）

departmentName1〜7, departmentAddress1〜7, departmentPhone1〜7, executiveName1〜10, executivePosition1〜10, bankCorporateNumber

### yuzuri 固有

担当者コメント, コメント

### 業種の重複・表記ゆれまとめ

| CSV列名 | 想定DBカラム | 備考 |
|---------|--------------|------|
| 業種1 | industry_large | |
| 業種2 | industry_middle | |
| 業種3 | industry_small | |
| 業種4 | industry_detail | 一部ファイルのみ |
| 業種（細） | industry_detail | 業種4と同義・位置が異なる |
| 業種-大 | industry_large | |
| 業種-中 | industry_middle | |
| 業種-小 | industry_small | |
| 業種-細 | industry_detail | |
| 業種（大） | industry_large | |
| 業種（中） | industry_middle | |
| 業種（小） | industry_small | |
| 業種 | industry | 単一列 |
| 業種（分類１〜３） | industry_* | import_firstTime/51 |
| 業種5, 6, 7 | industry_* | 111.fixed, 111.normalized |

### 住所・郵便番号の重複・表記ゆれ

| CSV列名 | 想定DBカラム |
|---------|--------------|
| 郵便番号 | postal_code |
| 会社郵便番号 | postal_code |
| 住所 | address |
| 会社住所 | address |
| 代表者郵便番号 | representative_postal_code |
| 代表者住所 | representative_address |
| 所在地 | address (都道府県含む場合あり) |

### 代表者・役員の重複・表記ゆれ

| CSV列名 | 想定DBカラム |
|---------|--------------|
| 代表者名 | representative_name |
| 代表者 | representative_name |
| 取締役 | directors |
| 役員 | directors |
| 株主 | shareholders |
| 株式保有率 | shareholders |

### 日付・数値系の表記ゆれ

| CSV列名 | 想定DBカラム | 備考 |
|---------|--------------|------|
| 設立 | established | 形式混在: "1980年4月1日", "1980-04-01", "20852" |
| 創業 | established | 同上 |
| 設立年月日 | established | import_firstTime/51 |
| 直近決算年月 | fiscal_period_end | "2022年9月1日", "45139" 等 |
| 直近売上 | recent_revenue | |
| 直近利益 | recent_profit | |

---

## 【2】ノイズ・除外候補

- **(空)**: 693件 — 空列・無名列
- **Unnamed: 38〜46**: ヘッダー列数超過分
- **文字化け** (Ж 等): csv0115（Shift-JIS）— 実体は会社名, 都道府県, 代表者名 等
- **{}**: import_firstTime/129
- **1, 0, （株）エコＥＲＣ, 7460101004178 等**: import_firstTime/129 の1行目がヘッダーでない

---

## 【3】分析用：正規化列名カンマ区切り

```
URL,会社名,設立,代表者名,株主,代表者郵便番号,代表者住所,代表者誕生日,取締役,住所,郵便番号,概要,都道府県,資本金,業種1,上場,直近利益,業種2,業種3,説明,社員数,電話番号(窓口),直近売上,オフィス数,工場数,店舗数,仕入れ先,取引先,取引先銀行,直近決算年月,電話番号,法人番号,NDA,AD,営業種目,会社郵便番号,会社住所,概況,業種-大,業種-中,業種-小,業種-細,取引種別,備考,業種（細）,創業,代表者,株式保有率,役員,業種（大）,業種（中）,業種（小）,業種4,業種,企業名,SBフラグ,ステータス,所在地,区分,売上規模（百万円）,企業概要,状態,会社ID,リストID,ID,従業員数,業種5,業種6,会社名（英語）,売上,発行株式数,決算月,businessDescriptions,銀行,affiliations,担当者コメント,コメント
```
