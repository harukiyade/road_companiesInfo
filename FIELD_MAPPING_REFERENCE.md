# companies_new フィールドマッピング一覧

全160フィールドのマッピング元（CSVヘッダー）を記載しています。

---

## 📋 基本情報

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `name` | 会社名, 企業名, 商号 |
| `nameEn` | 会社名（英語）, nameEn |
| `kana` | カナ, フリガナ |
| `corporateNumber` | 法人番号, corporate_number |
| `corporationType` | 法人種別, 会社種別 |
| `nikkeiCode` | 日経コード |
| `badges` | バッジ（配列） |
| `tags` | タグ（配列） |
| `createdAt` | 作成日時 |
| `updatedAt` | 更新日時 |
| `updateDate` | 更新日 |
| `updateCount` | 更新回数 |
| `changeCount` | 変更回数 |
| `qualificationGrade` | 資格等級 |

---

## 📍 所在地情報

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `prefecture` | 都道府県, pref |
| `address` | 会社住所, 住所, 所在地, 本社所在地, 本社住所 |
| `headquartersAddress` | 本社所在地, 本社住所 |
| `postalCode` | 会社郵便番号, 郵便番号 |
| `location` | 立地, 所在地 |
| `departmentLocation` | 部署所在地 |

---

## 📞 連絡先情報

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `phoneNumber` | 電話番号, 代表電話, phone, tel |
| `contactPhoneNumber` | 電話番号(窓口), 窓口電話番号, 問い合わせ電話番号 |
| `fax` | FAX, FAX番号 |
| `email` | メールアドレス, E-mail, mail |
| `companyUrl` | URL, 企業ホームページURL, HP |
| `contactFormUrl` | お問い合わせURL, 問い合わせフォームURL |

---

## 👤 代表者情報

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `representativeName` | 代表者名, 代表者, 代表取締役名 |
| `representativeKana` | 代表者カナ, 代表者フリガナ |
| `representativeTitle` | 代表者役職, 役職, 肩書 |
| `representativeBirthDate` | 代表者誕生日, 生年月日 |
| `representativePhone` | 代表者電話番号, 代表電話 |
| `representativePostalCode` | 代表者郵便番号 |
| `representativeHomeAddress` | 代表者住所, 代表者自宅住所 |
| `representativeRegisteredAddress` | 代表者登録住所 |
| `representativeAlmaMater` | 代表者出身校, 出身大学 |
| `executives` | 取締役, 役員（テキスト形式） |

---

## 👔 役員情報（最大10名）

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `executiveName1` | 役員名1, executiveName1 |
| `executivePosition1` | 役職1, executivePosition1, executiveTitle1 |
| `executiveName2` | 役員名2, executiveName2 |
| `executivePosition2` | 役職2, executivePosition2, executiveTitle2 |
| `executiveName3` | 役員名3, executiveName3 |
| `executivePosition3` | 役職3, executivePosition3, executiveTitle3 |
| `executiveName4` | 役員名4, executiveName4 |
| `executivePosition4` | 役職4, executivePosition4, executiveTitle4 |
| `executiveName5` | 役員名5, executiveName5 |
| `executivePosition5` | 役職5, executivePosition5, executiveTitle5 |
| `executiveName6` | 役員名6, executiveName6 |
| `executivePosition6` | 役職6, executivePosition6, executiveTitle6 |
| `executiveName7` | 役員名7, executiveName7 |
| `executivePosition7` | 役職7, executivePosition7, executiveTitle7 |
| `executiveName8` | 役員名8, executiveName8 |
| `executivePosition8` | 役職8, executivePosition8, executiveTitle8 |
| `executiveName9` | 役員名9, executiveName9 |
| `executivePosition9` | 役職9, executivePosition9, executiveTitle9 |
| `executiveName10` | 役員名10, executiveName10 |
| `executivePosition10` | 役職10, executivePosition10, executiveTitle10 |

**出典**: タイプH（130.csv, 131.csv）の`people`フィールドから展開

---

## 🏢 業種情報

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `industry` | 業種 |
| `industryLarge` | 業種-大, 業種（大）, 業種1 |
| `industryMiddle` | 業種-中, 業種（中）, 業種2 |
| `industrySmall` | 業種-小, 業種（小）, 業種3 |
| `industryDetail` | 業種-細, 業種（細）, 業種4 |
| `industries` | 業種リスト（配列） |
| `industryCategories` | ジャンル, 業種カテゴリー |
| `businessDescriptions` | **営業種目**, 事業内容, 得意分野 |
| `businessItems` | 事業品目（配列） |
| `businessSummary` | 事業概要 |
| `specialties` | 専門分野 |
| `demandProducts` | 需要製品 |
| `specialNote` | 特記事項 |

**出典**:
- タイプA: `businessDescriptions`（営業種目フィールド）
- タイプH: `industries`フィールドから`industryLarge/Middle/Small`に展開

---

## 💰 財務情報

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `capitalStock` | 資本金 |
| `revenue` | 売上高, 売上 |
| `latestRevenue` | 直近売上 |
| `latestProfit` | 直近利益, 経常利益 |
| `revenueFromStatements` | 財務諸表売上 |
| `operatingIncome` | 営業利益 |
| `totalAssets` | 総資産 |
| `totalLiabilities` | 総負債 |
| `netAssets` | 純資産 |
| `issuedShares` | 発行済株式数 |
| `financials` | 財務情報 |
| `listing` | 上場, 上場区分 |
| `marketSegment` | 市場区分 |
| `latestFiscalYearMonth` | 直近決算年月 |
| `fiscalMonth` | 決算期, 決算月 |
| `fiscalMonth1` | **決算月1** |
| `fiscalMonth2` | **決算月2** |
| `fiscalMonth3` | 決算月3 |
| `fiscalMonth4` | 決算月4 |
| `fiscalMonth5` | 決算月5 |
| `revenue1` | **売上1** |
| `revenue2` | **売上2** |
| `revenue3` | 売上3 |
| `revenue4` | 売上4 |
| `revenue5` | 売上5 |
| `profit1` | **利益1** |
| `profit2` | **利益2** |
| `profit3` | 利益3 |
| `profit4` | 利益4 |
| `profit5` | 利益5 |

**出典**:
- タイプC: `capitalStock`, `latestRevenue`
- タイプG: `latestRevenue`, `latestProfit`
- タイプI: `fiscalMonth1-5`, `revenue1-5`, `profit1-5`

---

## 🏭 企業規模・組織

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `employeeCount` | 従業員数, 社員数, employees |
| `employeeNumber` | 従業員番号 |
| `factoryCount` | 工場数 |
| `officeCount` | オフィス数, 事業所数 |
| `storeCount` | 店舗数 |
| `averageAge` | 平均年齢 |
| `averageYearsOfService` | 平均勤続年数 |
| `averageOvertimeHours` | 平均残業時間 |
| `averagePaidLeave` | 平均有給取得日数 |
| `femaleExecutiveRatio` | 女性役員比率 |

---

## 📅 設立・沿革

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `established` | 設立, 設立年月日 |
| `dateOfEstablishment` | 設立日 |
| `founding` | 創業, 創業年 |
| `foundingYear` | 創業年 |
| `acquisition` | 買収情報 |

---

## 🤝 取引先・関係会社

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `clients` | 取引先, 主要取引先, 子会社・関連会社 |
| `suppliers` | 仕入れ先, 取引先銀行（配列） |
| `subsidiaries` | 子会社（配列） |
| `affiliations` | 関連会社, 提携先 |
| `shareholders` | 株主, 主要株主, 株式保有率 |
| `banks` | 銀行, 取引銀行（配列） |
| `bankCorporateNumber` | 銀行法人番号 |

**出典**:
- タイプD: `clients`, `suppliers`
- タイプG: `banks`

---

## 🏢 部署・拠点情報（最大7拠点）

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `departmentName1` | 部署名1, 事業所名1 |
| `departmentAddress1` | 部署住所1, 事業所住所1 |
| `departmentPhone1` | 部署電話番号1, 事業所電話1 |
| `departmentName2` | 部署名2 |
| `departmentAddress2` | 部署住所2 |
| `departmentPhone2` | 部署電話番号2 |
| `departmentName3` | 部署名3 |
| `departmentAddress3` | 部署住所3 |
| `departmentPhone3` | 部署電話番号3 |
| `departmentName4` | 部署名4 |
| `departmentAddress4` | 部署住所4 |
| `departmentPhone4` | 部署電話番号4 |
| `departmentName5` | 部署名5 |
| `departmentAddress5` | 部署住所5 |
| `departmentPhone5` | 部署電話番号5 |
| `departmentName6` | 部署名6 |
| `departmentAddress6` | 部署住所6 |
| `departmentPhone6` | 部署電話番号6 |
| `departmentName7` | 部署名7 |
| `departmentAddress7` | 部署住所7 |
| `departmentPhone7` | 部署電話番号7 |

**出典**: タイプJ（133-136.csv）

---

## 📝 企業説明

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `overview` | 概況, 概要 |
| `companyDescription` | 説明, 会社情報・備考 |
| `businessDescriptions` | **営業種目**, 事業内容, 得意分野 |
| `salesNotes` | 備考, 営業メモ |

**出典**:
- タイプA: `businessDescriptions`（営業種目）
- タイプF: `companyDescription`, `overview`

---

## 🌐 SNS・外部リンク

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `urls` | URL配列 |
| `profileUrl` | プロフィールURL |
| `externalDetailUrl` | 外部詳細URL |
| `facebook` | Facebook URL |
| `linkedin` | LinkedIn URL |
| `wantedly` | Wantedly URL |
| `youtrust` | YOUTRUST URL |
| `metaKeywords` | メタキーワード |
| `metaDescription` | メタ説明 |

---

## 📊 取引状態・内部管理

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `tradingStatus` | 取引可否, 取引状況, 取引ステータス |
| `adExpiration` | 広告有効期限 |
| `numberOfActivity` | 活動回数 |
| `transportation` | 交通手段 |

---

## 🎯 その他

| companies_new フィールド | マッピング元（CSVヘッダー例） |
|-------------------------|---------------------------|
| `executiveTitle1` | 役職1（executivePosition1の別名） |
| `executiveTitle2` | 役職2（executivePosition2の別名） |
| `executiveTitle3` | 役職3 |
| `executiveTitle4` | 役職4 |
| `executiveTitle5` | 役職5 |
| `executiveTitle6` | 役職6 |
| `executiveTitle7` | 役職7 |
| `executiveTitle8` | 役職8 |
| `executiveTitle9` | 役職9 |
| `executiveTitle10` | 役職10 |

**注**: 130.csvの`業種1`, `業種2`, `業種3`は`industryLarge`, `industryMiddle`, `industrySmall`に統合されています（上記「業種情報」セクション参照）

---

## ⚠️ 削除されたフィールド

以下のフィールドはCSVにあるが、DBには**保存しない**内部管理フィールド：

### タイプC, D, E, I, J で削除
```
ID, 取引種別, SBフラグ, NDA, AD, ステータス, 備考
```

### タイプI（132.csv）で追加削除
```
売DM最終送信日時, 買DM最終送信日時, 売手紙最終送付日時,
買手最終荷電日時, 社長手紙最終送付日時,
SDS手紙最終送付日時, SDS社長手紙最終送付日時
```

### タイプJ（133-136.csv）で削除
```
会社ID
```

### 全タイプで削除
```
registrant（担当者）
```

---

## 📊 CSV タイプ別の主要マッピング

### タイプA（10-11, 100-104, 118-121.csv）
```
営業種目 → businessDescriptions ★重要
業種-大 → industryLarge
業種-中 → industryMiddle
業種-小 → industrySmall
株主 → shareholders
取締役 → executives
概況 → overview
```

### タイプB（12-37.csv）
```
創業 → founding
設立 → dateOfEstablishment
```

### タイプC（105-107, 109-110, 122.csv）
```
業種1 → industryLarge
業種2 → industryMiddle
業種3 → industrySmall
業種（細） → industryDetail
資本金 → capitalStock
直近売上 → latestRevenue
直近利益 → latestProfit
上場 → listing
```

### タイプD（111-115.csv）
```
仕入れ先 → suppliers
取引先 → clients
業種4 → industryDetail ★追加
```

### タイプE（116-117.csv）
```
メールアドレス → email
業種4 → industryDetail ★追加
```

### タイプF（124-126.csv）
```
説明 → companyDescription
概要 → overview
```

### タイプG（127-128.csv）
```
name → 会社名 → name
corporateNumber → 法人番号 → corporateNumber
banks → 銀行 → banks ★重要
latestRevenue → 直近売上
latestProfit → 直近利益
※ヘッダーを英語から日本語に変換
```

### タイプH（130-131.csv）
```
industryLarge → 業種（大）
industryMiddle → 業種（中）
industrySmall → 業種（小）
executiveName1-10 → 役員名1-10 ★重要
executivePosition1-10 → 役職1-10 ★重要
※peopleフィールドから展開
```

### タイプI（132.csv）
```
決算月1 → fiscalMonth1 ★重要
売上1 → revenue1 ★重要
利益1 → profit1 ★重要
決算月2-5 → fiscalMonth2-5
売上2-5 → revenue2-5
利益2-5 → profit2-5
```

### タイプJ（133-136.csv）
```
部署名1-7 → departmentName1-7 ★重要
部署住所1-7 → departmentAddress1-7 ★重要
部署電話番号1-7 → departmentPhone1-7 ★重要
代表名 → representativeName（134.csv）
```

---

## 🔢 数値フィールド（32個）

以下のフィールドは**数値型**として保存されます：

```
capitalStock           - 資本金
employeeCount          - 従業員数
employeeNumber         - 従業員番号
numberOfActivity       - 活動回数
revenue                - 売上高
revenueFromStatements  - 財務諸表売上
revenue1-5             - 売上1-5
latestRevenue          - 直近売上
latestProfit           - 直近利益
profit1-5              - 利益1-5
issuedShares           - 発行済株式数
totalAssets            - 総資産
totalLiabilities       - 総負債
netAssets              - 純資産
operatingIncome        - 営業利益
factoryCount           - 工場数
officeCount            - 事業所数
storeCount             - 店舗数
changeCount            - 変更回数
updateCount            - 更新回数
```

**単位**: すべて円建て（¥）で統一

---

## 📏 文字数制限

大量テキストフィールドには最大文字数制限があります（Firestore 1MB制限対策）：

| フィールド | 最大文字数 | 推定サイズ |
|-----------|-----------|-----------|
| `shareholders` | 100,000 | 約300KB |
| `executives` | 100,000 | 約300KB |
| `overview` | 200,000 | 約600KB |
| `companyDescription` | 200,000 | 約600KB |
| `businessDescriptions` | 50,000 | 約150KB |
| `address` | 5,000 | 約15KB |

---

## 🔑 特殊な処理

### 法人番号（corporateNumber）
- ✅ 13桁の数値のみ有効
- ❌ 文字列混在 → null
- ❌ ダミー番号（末尾9桁が0） → null
- ❌ 桁数不足・超過 → null

### URL関連
```
contactUrl (CSVヘッダー) → companyUrl (DBフィールド)
※ただし https://valuesearch.nikkei で始まるURLは削除
```

### 銀行（banks）
```
CSVの値: 三菱UFJ銀行（借入50億）・みずほ銀行（借入30億）
  ↓ クリーニング
DBの値: ["三菱UFJ銀行", "みずほ銀行"]
```

### 営業種目（businessDescriptions）
```
タイプAの「営業種目」列から取得
長文対応（最大50,000文字）
```

---

## 📚 関連ドキュメント

- `COMPANIES_NEW_FIELDS.md` - 全フィールド詳細（用途・例）
- `CSV_TYPE_REFERENCE.md` - CSVタイプ別構造
- `scripts/backfill_companies_from_csv.ts` - マッピングロジック実装

