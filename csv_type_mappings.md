# CSV タイプ別ヘッダーマッピング

## タイプA: 基本形式（法人番号なし）- 55件, ~61,500行
**ファイル:** 7-22, 25-35, 39, 52, 54-77, 101, 104

| CSVヘッダー | companies_new フィールド |
|------------|------------------------|
| 会社名 | name |
| 電話番号 | phoneNumber |
| 会社郵便番号 | postalCode |
| 会社住所 | address |
| URL | companyUrl |
| 代表者名 | representativeName |
| 代表者郵便番号 | representativeRegisteredAddress (郵便番号部分) |
| 代表者住所 | representativeHomeAddress |
| 代表者誕生日 | representativeBirthDate |
| 営業種目 | businessDescriptions |
| 設立 | established |
| 株主 | shareholders |
| 取締役 | executives |
| 概況 | overview |
| 業種-大 | industryLarge |
| 業種-中 | industryMiddle |
| 業種-小 | industrySmall |
| 業種-細 | industryDetail |

---

## タイプB: 基本形式（法人番号あり）- 6件, ~19,300行
**ファイル:** 1, 2, 53, 103, 106, 126

| CSVヘッダー | companies_new フィールド |
|------------|------------------------|
| 法人番号 | corporateNumber |
| (その他はタイプAと同じ) | |

---

## タイプC: 別形式（重複ヘッダー）- 26件, ~21,700行
**ファイル:** 23, 78-99, 100, 102, 105

⚠️ **注意**: 「郵便番号」「住所」が2回出現（会社/代表者）

| CSVヘッダー | companies_new フィールド |
|------------|------------------------|
| 会社名 | name |
| 電話番号 | phoneNumber |
| 郵便番号 (1番目) | postalCode |
| 住所 (1番目) | address |
| URL | companyUrl |
| 代表者 | representativeName |
| 郵便番号 (2番目) | representativeRegisteredAddress |
| 住所 (2番目) | representativeHomeAddress |
| 創業 | foundingYear |
| 設立 | established |
| 株式保有率 | (shareholders に含める) |
| 役員 | executives |
| 概要 | overview |
| 業種（大） | industryLarge |
| 業種（中） | industryMiddle |
| 業種（小） | industrySmall |
| 業種（細） | industryDetail |

---

## タイプD: 都道府県・ID詳細形式 - 29件, ~357,500行 ⭐最多データ
**ファイル:** 24, 36-50, 107-117, 119, 133, 134

| CSVヘッダー | companies_new フィールド |
|------------|------------------------|
| 会社名 | name |
| 都道府県 | prefecture |
| 代表者名 | representativeName |
| 法人番号 | corporateNumber |
| ID | (内部ID、無視) |
| 取引種別 | (無視) |
| SBフラグ | (無視) |
| NDA | (無視) |
| AD | (無視) |
| ステータス | (無視) |
| 備考 | salesNotes |
| URL | companyUrl |
| 業種1 | industry |
| 業種2 | industries[0] |
| 業種3 | industries[1] |
| 郵便番号 | postalCode |
| 住所 | address |
| 設立 | established |
| 電話番号(窓口) | phoneNumber |
| 代表者郵便番号 | representativeRegisteredAddress |
| 代表者住所 | representativeHomeAddress |
| 代表者誕生日 | representativeBirthDate |
| 資本金 | capitalStock |
| 上場 | listing |
| 直近決算年月 | fiscalMonth |
| 直近売上 | revenue |
| 直近利益 | financials |
| 説明 | companyDescription |
| 概要 | overview |
| 仕入れ先 | suppliers |
| 取引先 | clients |
| 取引先銀行 | (suppliers に追加) |
| 取締役 | executives |
| 株主 | shareholders |
| 社員数 | employeeCount |
| オフィス数 | officeCount |
| 工場数 | factoryCount |
| 店舗数 | storeCount |

---

## タイプE: 都道府県形式（法人番号なし）- 11件, ~17,400行
**ファイル:** 3, 4, 5, 6, 118, 120, 121, 122, 123, 124, 125

(タイプDから法人番号を除いた形式)

---

## タイプF: 特殊形式 - 7件, ~23,300行

### F-1: JSON形式 (127, 128)
上場企業の詳細情報がJSON列に入っている。個別パース必要。

### F-2: 英語ヘッダー (130, 131)
| CSVヘッダー | companies_new フィールド |
|------------|------------------------|
| name | name |
| corporateNumber | corporateNumber |
| representative | representativeName |
| sales | revenue |
| capital | capitalStock |
| listing | listing |
| address | address |
| employees | employeeCount |
| founded | established |
| fiscalMonth | fiscalMonth |
| industries | industries |
| tel | phoneNumber |
| url | companyUrl |

### F-3: 特殊 (51)
求人情報形式。個別対応必要。

### F-4: 特殊 (129)
ヘッダーがない？1行目がデータ。確認必要。

### F-5: 特殊 (132)
DM送信履歴等の独自フィールドあり。

