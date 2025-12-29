# companies_new コレクション フィールド一覧

全160フィールドをカテゴリー別に整理しています。

---

## 📊 基本情報（14フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `name` | string | **企業名**（必須） | 株式会社〇〇 |
| `nameEn` | string | 企業名（英語） | ABC Corp. |
| `kana` | string | 企業名カナ | カブシキガイシャ〇〇 |
| `corporateNumber` | string | **法人番号**（13桁） | 1234567890123 |
| `corporationType` | string | 法人種別 | 株式会社、有限会社 |
| `nikkeiCode` | string | 日経コード | |
| `badges` | array | バッジ（タグ配列） | ["優良企業"] |
| `tags` | array | タグ | ["製造業"] |
| `createdAt` | timestamp | 作成日時 | |
| `updatedAt` | timestamp | 更新日時 | |
| `updateDate` | string | 更新日 | |
| `updateCount` | number | 更新回数 | |
| `changeCount` | number | 変更回数 | |
| `qualificationGrade` | string | 資格等級 | |

---

## 📍 所在地情報（6フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `prefecture` | string | **都道府県** | 東京都 |
| `address` | string | **住所**（本社） | 東京都千代田区〇〇 |
| `headquartersAddress` | string | 本社所在地 | |
| `postalCode` | string | **郵便番号** | 100-0001 |
| `location` | string | 立地 | |
| `departmentLocation` | string | 部署所在地 | |

---

## 📞 連絡先情報（6フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `phoneNumber` | string | **電話番号**（代表） | 03-1234-5678 |
| `contactPhoneNumber` | string | 窓口電話番号 | 03-9876-5432 |
| `fax` | string | FAX番号 | 03-1234-5679 |
| `email` | string | **メールアドレス** | info@example.com |
| `companyUrl` | string | **企業URL** | https://example.com |
| `contactFormUrl` | string | 問い合わせURL | https://example.com/contact |

---

## 👤 代表者情報（10フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `representativeName` | string | **代表者名** | 山田太郎 |
| `representativeKana` | string | 代表者名カナ | ヤマダタロウ |
| `representativeTitle` | string | 代表者役職 | 代表取締役社長 |
| `representativeBirthDate` | string | 代表者誕生日 | 1970-01-01 |
| `representativePhone` | string | 代表者電話番号 | |
| `representativePostalCode` | string | 代表者郵便番号 | |
| `representativeHomeAddress` | string | 代表者自宅住所 | |
| `representativeRegisteredAddress` | string | 代表者登録住所 | |
| `representativeAlmaMater` | string | 代表者出身校 | |
| `executives` | string | 取締役情報（テキスト） | |

---

## 👔 役員情報（20フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `executiveName1` | string | **役員名1** | 山田太郎 |
| `executivePosition1` | string | **役職1** | 代表取締役社長 |
| `executiveName2` | string | 役員名2 | 田中花子 |
| `executivePosition2` | string | 役職2 | 専務取締役 |
| `executiveName3` | string | 役員名3 | |
| `executivePosition3` | string | 役職3 | |
| `executiveName4` | string | 役員名4 | |
| `executivePosition4` | string | 役職4 | |
| `executiveName5` | string | 役員名5 | |
| `executivePosition5` | string | 役職5 | |
| `executiveName6` | string | 役員名6 | |
| `executivePosition6` | string | 役職6 | |
| `executiveName7` | string | 役員名7 | |
| `executivePosition7` | string | 役職7 | |
| `executiveName8` | string | 役員名8 | |
| `executivePosition8` | string | 役職8 | |
| `executiveName9` | string | 役員名9 | |
| `executivePosition9` | string | 役職9 | |
| `executiveName10` | string | 役員名10 | |
| `executivePosition10` | string | 役職10 | |

**出典**: タイプH（130.csv, 131.csv）の`people`フィールドから展開

---

## 🏢 業種情報（13フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `industry` | string | **業種**（主要） | 製造業 |
| `industryLarge` | string | 業種（大分類） | 製造業 |
| `industryMiddle` | string | 業種（中分類） | 機械製造業 |
| `industrySmall` | string | 業種（小分類） | 精密機械製造業 |
| `industryDetail` | string | 業種（細分類） | 半導体製造装置 |
| `industries` | array | 業種リスト | ["製造業", "卸売業"] |
| `industryCategories` | string | 業種カテゴリー | |
| `businessDescriptions` | string | **営業種目**（詳細） | 製造業、卸売業、... |
| `businessItems` | array | 事業品目 | |
| `businessSummary` | string | 事業概要 | |
| `specialties` | string | 専門分野 | |
| `demandProducts` | string | 需要製品 | |
| `specialNote` | string | 特記事項 | |

**出典**: 
- タイプA: `businessDescriptions`
- タイプH: `industry1-3` → `industryLarge/Middle/Small`

---

## 💰 財務情報（24フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `capitalStock` | number | **資本金**（円） | 100000000 |
| `revenue` | number | **売上高**（円） | 5000000000 |
| `latestRevenue` | number | **直近売上** | 5500000000 |
| `latestProfit` | number | **直近利益** | 500000000 |
| `revenueFromStatements` | number | 財務諸表からの売上 | |
| `operatingIncome` | number | 営業利益 | |
| `totalAssets` | number | 総資産 | |
| `totalLiabilities` | number | 総負債 | |
| `netAssets` | number | 純資産 | |
| `issuedShares` | number | 発行済株式数 | |
| `financials` | string | 財務情報 | |
| `listing` | string | **上場区分** | 東証プライム、非上場 |
| `marketSegment` | string | 市場区分 | |
| `latestFiscalYearMonth` | string | 直近決算年月 | 2024年3月 |
| `fiscalMonth` | string | 決算月 | 3月 |
| `fiscalMonth1` | string | **決算月1** | 2024年3月 |
| `fiscalMonth2` | string | **決算月2** | 2023年3月 |
| `fiscalMonth3` | string | 決算月3 | 2022年3月 |
| `fiscalMonth4` | string | 決算月4 | 2021年3月 |
| `fiscalMonth5` | string | 決算月5 | 2020年3月 |
| `revenue1` | number | **売上1** | 5500000000 |
| `revenue2` | number | **売上2** | 5000000000 |
| `revenue3` | number | 売上3 | 4800000000 |
| `revenue4` | number | 売上4 | 4500000000 |
| `revenue5` | number | 売上5 | 4000000000 |
| `profit1` | number | **利益1** | 500000000 |
| `profit2` | number | **利益2** | 450000000 |
| `profit3` | number | 利益3 | 400000000 |
| `profit4` | number | 利益4 | 350000000 |
| `profit5` | number | 利益5 | 300000000 |

**出典**: 
- タイプC: `latestRevenue`, `latestProfit`
- タイプG: `banks`, `latestRevenue`, `latestProfit`
- タイプI: `fiscalMonth1-5`, `revenue1-5`, `profit1-5`

---

## 🏭 企業規模・組織（10フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `employeeCount` | number | **従業員数** | 500 |
| `employeeNumber` | number | 従業員番号 | |
| `factoryCount` | number | 工場数 | 3 |
| `officeCount` | number | 事業所数 | 10 |
| `storeCount` | number | 店舗数 | 50 |
| `averageAge` | string | 平均年齢 | 38.5歳 |
| `averageYearsOfService` | string | 平均勤続年数 | 12.3年 |
| `averageOvertimeHours` | string | 平均残業時間 | 20時間/月 |
| `averagePaidLeave` | string | 平均有給取得日数 | 12日/年 |
| `femaleExecutiveRatio` | string | 女性役員比率 | 30% |

---

## 📅 設立・沿革（5フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `established` | string | **設立**（年月日または文字列） | 1950-01-15 |
| `dateOfEstablishment` | string | **設立日** | 1950-01-15 |
| `founding` | string | **創業** | 1945年 |
| `foundingYear` | string | 創業年 | 1945 |
| `acquisition` | string | 買収情報 | |

---

## 🤝 取引先・関係会社（7フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `clients` | string | **取引先**（販売先） | トヨタ自動車、ソニー |
| `suppliers` | array | **仕入れ先** | ["〇〇商事", "△△株式会社"] |
| `subsidiaries` | array | 子会社 | ["〇〇子会社"] |
| `affiliations` | string | 関連会社・提携先 | |
| `shareholders` | string | 株主情報 | |
| `banks` | array | **取引銀行** | ["三菱UFJ銀行", "みずほ銀行"] |
| `bankCorporateNumber` | string | 銀行法人番号 | |

**出典**:
- タイプD: `clients`, `suppliers`
- タイプG: `banks`

---

## 🏢 部署・拠点情報（21フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `departmentName1` | string | **部署名1** | 本社 |
| `departmentAddress1` | string | **部署住所1** | 東京都千代田区... |
| `departmentPhone1` | string | **部署電話1** | 03-1234-5678 |
| `departmentName2` | string | 部署名2 | 大阪支店 |
| `departmentAddress2` | string | 部署住所2 | 大阪府大阪市... |
| `departmentPhone2` | string | 部署電話2 | 06-1234-5678 |
| `departmentName3` | string | 部署名3 | |
| `departmentAddress3` | string | 部署住所3 | |
| `departmentPhone3` | string | 部署電話3 | |
| `departmentName4` | string | 部署名4 | |
| `departmentAddress4` | string | 部署住所4 | |
| `departmentPhone4` | string | 部署電話4 | |
| `departmentName5` | string | 部署名5 | |
| `departmentAddress5` | string | 部署住所5 | |
| `departmentPhone5` | string | 部署電話5 | |
| `departmentName6` | string | 部署名6 | |
| `departmentAddress6` | string | 部署住所6 | |
| `departmentPhone6` | string | 部署電話6 | |
| `departmentName7` | string | 部署名7 | |
| `departmentAddress7` | string | 部署住所7 | |
| `departmentPhone7` | string | 部署電話7 | |

**出典**: タイプJ（133-136.csv）の部署情報

---

## 📝 企業説明（4フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `overview` | string | **概要**（長文可） | 当社は製造業を中心に... |
| `companyDescription` | string | **企業説明**（長文可） | 1950年創業、精密機械... |
| `businessDescriptions` | string | **営業種目**（長文可） | 製造業、卸売業、建設業... |
| `salesNotes` | string | 営業メモ | |

**最大文字数制限**:
- `overview`: 200,000文字（約600KB）
- `companyDescription`: 200,000文字（約600KB）
- `businessDescriptions`: 50,000文字（約150KB）

**出典**:
- タイプA: `businessDescriptions`
- タイプF: `companyDescription`, `overview`

---

## 🌐 SNS・外部リンク（8フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `urls` | array | URL配列 | ["https://..."] |
| `profileUrl` | string | プロフィールURL | |
| `externalDetailUrl` | string | 外部詳細URL | |
| `facebook` | string | Facebook URL | |
| `linkedin` | string | LinkedIn URL | |
| `wantedly` | string | Wantedly URL | |
| `youtrust` | string | YOUTRUST URL | |
| `metaKeywords` | string | メタキーワード | |
| `metaDescription` | string | メタ説明 | |

---

## 📊 取引状態・内部管理（4フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `tradingStatus` | string | **取引可否** | 取引中、取引不可 |
| `adExpiration` | string | 広告有効期限 | |
| `numberOfActivity` | number | 活動回数 | |
| `transportation` | string | 交通手段 | |

---

## 🎯 その他情報（10フィールド）

| フィールド名 | 型 | 用途 | 例 |
|-------------|-----|------|-----|
| `executiveTitle1` | string | 役職1（別名） | 代表取締役 |
| `executiveTitle2` | string | 役職2（別名） | 専務取締役 |
| `executiveTitle3` | string | 役職3（別名） | |
| `executiveTitle4` | string | 役職4（別名） | |
| `executiveTitle5` | string | 役職5（別名） | |
| `executiveTitle6` | string | 役職6（別名） | |
| `executiveTitle7` | string | 役職7（別名） | |
| `executiveTitle8` | string | 役職8（別名） | |
| `executiveTitle9` | string | 役職9（別名） | |
| `executiveTitle10` | string | 役職10（別名） | |

**注**: 
- `executiveTitle1-10`は`executivePosition1-10`の別名として使用
- 130.csvの`業種1-3`は`industryLarge/Middle/Small`に統合（上記「業種情報」セクション参照）

---

## 📊 総フィールド数: **157フィールド**

### カテゴリー別集計

| カテゴリー | フィールド数 |
|-----------|-------------|
| 基本情報 | 14 |
| 所在地情報 | 6 |
| 連絡先情報 | 6 |
| 代表者情報 | 10 |
| 役員情報 | 20 |
| 業種情報 | 13 |
| 財務情報 | 29 |
| 企業規模・組織 | 10 |
| 設立・沿革 | 5 |
| 取引先・関係会社 | 7 |
| 部署・拠点情報 | 21 |
| 企業説明 | 4 |
| SNS・外部リンク | 9 |
| 取引状態・内部管理 | 4 |
| その他 | 10 |

**注**: `industry1-3`（130.csv専用）は`industryLarge/Middle/Small`に統合済み

---

## 🎯 必須フィールド（UI表示に重要）

### 最優先（必須）
- ✅ `name` - 企業名
- ✅ `corporateNumber` - 法人番号
- ✅ `address` - 住所
- ✅ `phoneNumber` - 電話番号

### 高優先度
- ✅ `companyUrl` - 企業URL
- ✅ `representativeName` - 代表者名
- ✅ `industry` - 業種
- ✅ `prefecture` - 都道府県
- ✅ `postalCode` - 郵便番号

### 中優先度
- ✅ `capitalStock` - 資本金
- ✅ `employeeCount` - 従業員数
- ✅ `latestRevenue` - 直近売上
- ✅ `latestProfit` - 直近利益
- ✅ `businessDescriptions` - 営業種目
- ✅ `email` - メールアドレス

---

## 📋 CSV タイプ別の主要フィールド

| タイプ | 主要フィールド |
|--------|---------------|
| **タイプA** | `businessDescriptions` (営業種目) |
| **タイプB** | `founding`, `dateOfEstablishment` (創業・設立) |
| **タイプC** | `industry`, `capitalStock`, `latestRevenue` (業種・資本金・売上) |
| **タイプD** | `suppliers`, `clients` (仕入れ先・取引先) |
| **タイプE** | `email` (メールアドレス) |
| **タイプF** | `companyDescription`, `overview` (説明・概要) |
| **タイプG** | `banks`, `latestRevenue`, `latestProfit` (銀行・決算) |
| **タイプH** | `industry1-3`, `executiveName1-10`, `executivePosition1-10` (業種・役員) |
| **タイプI** | `fiscalMonth1-5`, `revenue1-5`, `profit1-5` (複数年決算) |
| **タイプJ** | `departmentName1-7`, `departmentAddress1-7`, `departmentPhone1-7` (部署) |

---

## 🔢 数値フィールド（32個）

以下のフィールドは数値型として保存されます：

```
capitalStock, employeeCount, employeeNumber, numberOfActivity,
revenue, revenueFromStatements, revenue1, revenue2, revenue3, revenue4, revenue5,
latestRevenue, latestProfit, profit1, profit2, profit3, profit4, profit5,
issuedShares, totalAssets, totalLiabilities, netAssets, operatingIncome,
factoryCount, officeCount, storeCount, changeCount, updateCount
```

**単位**: すべて円建て（¥）で統一

---

## 📐 文字数制限

大量のテキストを格納するフィールドには制限があります（Firestore 1MB制限対策）：

| フィールド | 最大文字数 | 推定サイズ |
|-----------|-----------|-----------|
| `shareholders` | 100,000 | 約300KB |
| `executives` | 100,000 | 約300KB |
| `overview` | 200,000 | 約600KB |
| `companyDescription` | 200,000 | 約600KB |
| `businessDescriptions` | 50,000 | 約150KB |
| `address` | 5,000 | 約15KB |

---

## 🔑 ユニークキー

**ドキュメントID**: 基本的に法人番号（13桁）を使用

**重複判定**: 
1. 法人番号 + 住所が同じ → 統合
2. 企業名が同じでも法人番号・住所が違う → 別企業として保持

---

## 📚 関連ドキュメント

- `CSV_TYPE_REFERENCE.md` - CSVタイプ別詳細
- `VERIFICATION_GUIDE.md` - データ確認ガイド
- `DB_INSPECTION_GUIDE.md` - DB確認方法
- `CLEANUP_REQUIREMENTS.md` - 8つの修正要件
