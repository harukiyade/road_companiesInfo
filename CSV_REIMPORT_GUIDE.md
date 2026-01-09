# CSV再インポートガイド

見つからなかったCSVファイルをヘッダーの種類別にグループ化し、各グループごとのインポート方法を記載しています。

## グループ別インポート方法

### グループ1: 法人番号付き標準フォーマット（5ファイル）
**ヘッダー特徴**: 法人番号, 会社名, 電話番号, 会社郵便番号, 会社住所, URL, 代表者名, 代表者郵便番号, 代表者住所, 代表者誕生日, 営業種目, 設立, 株主, 取締役, 概況, 業種-大, 業種-中, 業種-小, 業種-細

**対象ファイル**: 1.csv, 103.csv, 126.csv, 2.csv, 53.csv

**インポートコマンド**:
```bash
GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/1.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/103.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/126.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/2.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/53.csv
```

---

### グループ2: 取引種別・SBフラグ付きフォーマット（4ファイル）
**ヘッダー特徴**: 会社名, 都道府県, 代表者名, 取引種別, SBフラグ, NDA, AD, ステータス, 備考, URL, 業種1, 業種2, 業種3, 郵便番号, 住所, 設立, 電話番号(窓口), 代表者郵便番号, 代表者住所, 代表者誕生日, 資本金, 上場, 直近決算年月, 直近売上, 直近利益, 説明, 概要, 仕入れ先, 取引先, 取引先銀行, 取締役, 株主, 社員数, オフィス数, 工場数, 店舗数

**対象ファイル**: 3.csv, 4.csv, 5.csv, 6.csv

**インポートコマンド**:
```bash
GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/3.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/4.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/5.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/6.csv
```

---

### グループ3: 標準フォーマット（54ファイル）⭐最大グループ
**ヘッダー特徴**: 会社名, 電話番号, 会社郵便番号, 会社住所, URL, 代表者名, 代表者郵便番号, 代表者住所, 代表者誕生日, 営業種目, 設立, 株主, 取締役, 概況, 業種-大, 業種-中, 業種-小, 業種-細

**対象ファイル**: 10.csv, 101.csv, 11.csv, 12.csv, 13.csv, 14.csv, 15.csv, 16.csv, 17.csv, 18.csv, 19.csv, 20.csv, 21.csv, 22.csv, 25.csv, 26.csv, 27.csv, 28.csv, 29.csv, 30.csv, 31.csv, 32.csv, 33.csv, 34.csv, 35.csv, 39.csv, 52.csv, 54.csv, 55.csv, 56.csv, 57.csv, 58.csv, 59.csv, 60.csv, 61.csv, 62.csv, 63.csv, 64.csv, 65.csv, 66.csv, 67.csv, 68.csv, 69.csv, 7.csv, 70.csv, 71.csv, 72.csv, 73.csv, 74.csv, 75.csv, 76.csv, 77.csv, 8.csv, 9.csv

**インポートコマンド（一括）**:
```bash
# グループ全体を一括でインポートする場合
GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv
```

**インポートコマンド（個別）**:
```bash
# 個別にインポートする場合（例）
GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/10.csv
# ... 他のファイルも同様
```

---

### グループ4: 創業・株式保有率付きフォーマット（24ファイル）
**ヘッダー特徴**: 会社名, 電話番号, 郵便番号, 住所, URL, 代表者, 郵便番号, 住所, 創業, (空), 設立, 株式保有率, 役員, 概要, 業種（大）, 業種（中）, 業種（小）, 業種（細）

**対象ファイル**: 102.csv, 23.csv, 78.csv, 79.csv, 80.csv, 81.csv, 82.csv, 83.csv, 84.csv, 85.csv, 86.csv, 87.csv, 88.csv, 89.csv, 90.csv, 91.csv, 92.csv, 93.csv, 94.csv, 95.csv, 96.csv, 97.csv, 98.csv, 99.csv

**インポートコマンド（例）**:
```bash
GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/102.csv

# 他のファイルも同様に実行
```

---

### グループ5: 法人番号・業種3つ付きフォーマット（5ファイル）
**ヘッダー特徴**: 会社名, 都道府県, 代表者名, 法人番号, URL, 業種1, 業種2, 業種3, 郵便番号, 住所, 設立, 電話番号(窓口), 代表者郵便番号, 代表者住所, 代表者誕生日, 資本金, 上場, 直近決算年月, 直近売上, 直近利益, 説明, 概要, 仕入れ先, 取引先, 取引先銀行, 取締役, 株主, 社員数, オフィス数, 工場数, 店舗数

**対象ファイル**: 133.csv, 134.csv, 24.csv, 40.csv, 41.csv

**インポートコマンド**:
```bash
GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/133.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/134.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/24.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/40.csv

GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/41.csv
```

---

### グループ6-30: その他の特殊フォーマット

詳細は `csv_header_groups_*.json` ファイルを参照してください。

**主な特殊フォーマット**:
- **グループ10**: 51.csv（ジャンル・業種分類付き）
- **グループ27**: 127.csv（英語名・businessDescriptions付き）
- **グループ28**: 130.csv（name, corporateNumber等の英語ヘッダー）
- **グループ29**: 131.csv（name, corporateNumber等の英語ヘッダー・短縮版）
- **グループ30**: 132.csv（決算月1-5、売上1-5、利益1-5付き）

各ファイルのインポートコマンドは以下の形式です：
```bash
GOOGLE_APPLICATION_CREDENTIALS='/Users/harumacmini/Downloads/albert-ma-firebase-adminsdk-iat1k-a64039899f.json' \
npx ts-node scripts/import_companies_from_csv.ts ./csv/[ファイル名].csv
```

---

## 注意事項

1. **128.csv と 129.csv は除外**: これらのファイルはCSV読み込みエラーが発生しているため、再インポート対象外です。

2. **重複削除について**: 重複で故意に削除した企業は再インポートされません。企業名＋住所で照合した結果、見つからなかった企業のみが再インポートされます。

3. **一括インポート**: グループ3（54ファイル）は同じヘッダーパターンのため、`./csv` ディレクトリを指定して一括インポートすることも可能です。

4. **インポート順序**: 特に指定がなければ、どの順序でインポートしても問題ありません。
