# CSV タイプ別リファレンス

各タイプのCSVファイルの構造と特徴をまとめています。

---

## 📁 タイプA: 基本情報 + 営業種目

### 代表的なCSV
- `10.csv` (6,834行)
- `11.csv` (9,498行)
- `100.csv` (9,020行)

### ヘッダー構造（18列）
```
1. 会社名
2. 電話番号
3. 会社郵便番号
4. 会社住所
5. URL
6. 代表者名
7. 代表者郵便番号
8. 代表者住所
9. 代表者誕生日
10. 営業種目 ★重要
11. 設立
12. 株主
13. 取締役
14. 概況
15. 業種-大
16. 業種-中
17. 業種-小
18. 法人番号
```

### 重要フィールド
- **営業種目**: `businessDescriptions` にマッピング
- 複数の営業種目が記載されている

### サンプル
```
会社名: 株式会社環境設備設計
営業種目: 建設設備設計業務...
```

---

## 📁 タイプB: 創業・設立あり

### 代表的なCSV
- `12.csv` (3,620行)
- `13.csv` (3,614行)
- `14.csv` (3,614行)

### 特徴
- 創業年、設立年の情報が充実
- `founding`, `dateOfEstablishment` フィールド

---

## 📁 タイプC: 詳細情報（業種・資本金・売上）

### 代表的なCSV
- `105.csv` (299行)
- `106.csv` (5,921行)
- **`107.csv` (40,846行)** ← 今回修正したファイル

### ヘッダー構造（41列、修正後）
```
1. 会社名
2. 都道府県
3. 代表者名
4. 法人番号
5. URL
6. 業種1
7. 業種2
8. 業種3
9. 業種（細）
10. 郵便番号
11. 住所
12. 設立
13. 電話番号(窓口)
14. 代表者郵便番号
15. 代表者住所
16. 代表者誕生日
17. 資本金
18. 上場
19. 直近決算年月
20. 直近売上
21. 直近利益
...
```

### 修正内容
- ✅ データズレ修正: 19,781行
- ✅ 内部管理フィールド削除（ID, 取引種別, SBフラグ, NDA, AD, ステータス, 備考）
- ✅ ダミー法人番号検出
- ✅ 重複企業統合

### サンプル
```
会社名: 丹羽興業株式会社
法人番号: 9180000000000
都道府県: 愛知県
住所: 愛知県名古屋市西区木前町９８
```

---

## 📁 タイプD: 取引先情報

### 代表的なCSV
- `111.csv` (5,028行)
- `112.csv` (5,028行)
- `113.csv` (5,028行)

### 重要フィールド
- **仕入れ先**: `suppliers`
- **取引先**: `clients`

---

## 📁 タイプE: メールアドレスあり

### 代表的なCSV
- `116.csv` (5,017行)
- `117.csv` (5,016行)

### 重要フィールド
- **メールアドレス**: `email`

---

## 📁 タイプF: 説明・概要あり

### 代表的なCSV
- `124.csv` (1,961行)
- `125.csv` (1,961行)
- `126.csv` (1,968行)

### 重要フィールド
- **説明**: `companyDescription`
- **概要**: `overview`

---

## 📁 タイプG: 銀行・決算情報

### 代表的なCSV
- **`127.csv` (1,193行)**
- **`128.csv` (1,193行)**

### ヘッダー構造（27列）
```
1. name
2. nameEn
3. corporateNumber
4. prefecture
5. address
6. industry
7. capitalStock
8. revenue
9. latestProfit
10. employeeCount
11. issuedShares
12. established
13. fiscalMonth
14. listing
15. representativeName
16. representativeTitle
17. banks ★重要
18. phoneNumber
19. companyUrl
...
```

### 重要フィールド
- **銀行**: `banks` (取引銀行名)
- **売上**: `revenue`, `latestRevenue`
- **利益**: `latestProfit`

### サンプル
```
name: （株）かくまん
corporateNumber: 3440001000373
banks: 三菱UFJ銀行・みずほ銀行
```

---

## 📁 タイプH: 業種展開・役員情報

### 代表的なCSV
- **`130.csv` (7,253行)**
- **`131.csv` (7,253行)**

### ヘッダー構造（61列）
```
1. name
2. corporateNumber
3. representativeName
4. revenue
5. capitalStock
6. listing
7. address
8. employeeCount
9. established
10. fiscalMonth
11. industryLarge (業種-大)
12. industryMiddle (業種-中)
13. industrySmall (業種-小)
14. industryDetail (業種-細)
15. phoneNumber
16. companyUrl
17. bankCorporateNumber
...
28. executiveName1 ★重要
29. executiveTitle1 ★重要
30. executiveName2
31. executiveTitle2
...（最大10人まで）
```

### 修正内容
- ✅ 英語ヘッダーを日本語に変換済み
- ✅ `people` フィールドから役員情報を展開（最大10人）
- ✅ `industries` フィールドから業種1-3を分離

### 重要フィールド
- **業種1-3**: `industry1`, `industry2`, `industry3`
- **役員名1-10**: `executiveName1`-`executiveName10`
- **役職1-10**: `executiveTitle1`-`executiveTitle10`

### サンプル
```
name: 株式会社東邦銀行
corporateNumber: 9380001001018
executiveName1: 佐藤 稔
executiveTitle1: 代表取締役頭取
executiveName2: 田村 正博
executiveTitle2: 専務取締役
```

---

## 📁 タイプI: 決算月・売上・利益（複数年）

### 代表的なCSV
- **`132.csv` (1,405行)**

### ヘッダー構造（54列）
```
1. 会社名
2. 都道府県
3. 代表者名
4. 法人番号
...
15. 住所
16. 設立
...
21. 決算月1 ★重要
22. 売上1 ★重要
23. 利益1 ★重要
24. 決算月2
25. 売上2
26. 利益2
27. 決算月3
28. 売上3
29. 利益3
30. 決算月4
31. 売上4
32. 利益4
33. 決算月5
34. 売上5
35. 利益5
```

### 重要フィールド
- **決算月1-5**: `fiscalMonth1`-`fiscalMonth5`
- **売上1-5**: `revenue1`-`revenue5`
- **利益1-5**: `profit1`-`profit5`

### サンプル
```
会社名: Ecuitee Corporation
決算月1: 2023年3月
売上1: 1,200,000,000
利益1: 150,000,000
決算月2: 2022年3月
売上2: 1,000,000,000
利益2: 120,000,000
```

---

## 📁 タイプJ: 部署・拠点情報

### 代表的なCSV
- `133.csv` (3,752行)
- `134.csv` (1,875行)
- `135.csv` (938行)
- `136.csv` (938行)

### ヘッダー構造（32列）
```
1. 会社名
2. 都道府県
3. 代表者名
4. 法人番号
5. 会社ID
6. URL
7. 業種1
8. 業種2
9. 業種3
10. 郵便番号
11. 住所
12. 設立
13. 電話番号(窓口)
...
16. 部署名1 ★重要
17. 部署住所1 ★重要
18. 部署電話番号1 ★重要
19. 部署名2
20. 部署住所2
21. 部署電話番号2
...（最大7部署まで）
```

### 重要フィールド
- **部署名1-7**: `departmentName1`-`departmentName7`
- **部署住所1-7**: `departmentAddress1`-`departmentAddress7`
- **部署電話番号1-7**: `departmentPhone1`-`departmentPhone7`

### サンプル
```
会社名: カタリスト株式会社
departmentName1: 本社
departmentAddress1: 愛知県名古屋市...
departmentPhone1: 052-xxx-xxxx
departmentName2: 東京支店
departmentAddress2: 東京都千代田区...
departmentPhone2: 03-xxxx-xxxx
```

---

## 📋 各タイプの確認方法

### CSV内容の直接確認
```bash
# ヘッダーのみ表示
head -1 csv/107.csv

# 最初の5行表示
head -5 csv/130.csv

# 行数確認
wc -l csv/132.csv
```

### Firestoreでの確認
```bash
# タイプ別サンプル確認
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/verify_csv_import_by_type.ts

# 特定CSVの全行確認
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
npx ts-node scripts/verify_specific_csv.ts csv/107.csv
```

---

## 🎯 重点確認が必要なタイプ

### 最優先
1. **タイプC (107.csv)**: 今回修正、重複統合
2. **タイプH (130.csv, 131.csv)**: 役員情報の展開
3. **タイプA (10.csv, 11.csv)**: 営業種目の取り込み

### 中優先度
4. **タイプG (127.csv, 128.csv)**: 銀行情報
5. **タイプJ (133-136.csv)**: 部署情報
6. **タイプI (132.csv)**: 複数年決算

---

## 📞 サポート

各タイプの詳細については:
- `VERIFICATION_GUIDE.md`: CSV取り込み確認
- `DB_INSPECTION_GUIDE.md`: Firestore確認
- `scripts/verify_csv_import_by_type.ts`: タイプ別確認スクリプト

