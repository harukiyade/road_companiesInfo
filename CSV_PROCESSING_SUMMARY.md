# CSV処理完了サマリー

## ✅ 完了したタスク

### 1. 100.csv（タイプB）の確認
- **タイプ**: B（創業あり形式）
- **設立と創業の間**: 営業種目（仕入れ先ではない）
- **状態**: 正常

### 2. 107.csvタイプ（タイプC）のヘッダー修正
- **対象ファイル**: 105.csv, 106.csv, 107.csv, 109.csv, 110.csv, 122.csv
- **修正内容**: 業種3の後に業種（細）ヘッダーを追加
- **状態**: ✅ 修正完了

### 3. registrantフィールドの削除
- **修正内容**: COMPANY_TEMPLATEからregistrantフィールドを削除
- **理由**: 担当者情報は後で設定する
- **状態**: ✅ 削除完了

### 4. 132.csvタイプ（タイプF）の決算5期分
- **確認結果**: 既に決算月1-5、売上1-5、利益1-5が存在
- **COMPANY_TEMPLATE**: 既に追加済み
- **状態**: ✅ 完了済み

### 5. 108.csvタイプ（タイプG）の確認
- **確認結果**: ヘッダーズレなし、業種4ヘッダー存在
- **状態**: ✅ 正常

### 6. 118.csvタイプ（タイプH）の確認
- **確認結果**: ヘッダーズレなし、業種4ヘッダー存在
- **状態**: ✅ 正常

### 7. 130.csv/131.csv（タイプI）の展開
- **展開フィールド**: 
  - departmentName1-7, departmentAddress1-7, departmentPhone1-7（21フィールド）
  - executiveName1-10, executivePosition1-10（20フィールド）
  - bankCorporateNumber
- **COMPANY_TEMPLATE**: ✅ 追加済み
- **CSV状態**: ✅ 展開済み（61フィールド）

### 8. 51.csv（タイプJ2）の新フィールド追加
- **追加フィールド**:
  - departmentLocation（部署・拠点名）
  - specialties（得意分野）
  - averageAge（平均年齢）
  - averageYearsOfService（平均勤続年数）
  - averageOvertimeHours（月平均所定外労働時間）
  - averagePaidLeave（平均有給休暇取得日数）
  - femaleExecutiveRatio（役員女性比率）
  - transportation（交通機関）
- **COMPANY_TEMPLATE**: ✅ 追加済み

### 9. 127.csv/128.csv（タイプJ1）の統一
- **処理内容**:
  - summaryJson展開（18項目）
  - statementsJson展開（5項目の財務情報）
  - banksフィールドを銀行名のみに（・で連結）
  - contactUrlの valuesearch.nikkei を削除
  - 不要なJSONフィールド削除
  - ヘッダー統一（27フィールド）
- **CSV状態**: ✅ 統一済み

---

## 📊 companies_new コレクション 最終仕様

**総フィールド数**: 155フィールド

### 新規追加フィールド（前回比）

1. **部署情報（21）**: departmentName1-7, departmentAddress1-7, departmentPhone1-7
2. **役員情報（20）**: executiveName1-10, executivePosition1-10
3. **決算5期分（15）**: fiscalMonth1-5, revenue1-5, profit1-5
4. **財務詳細（5）**: totalAssets, totalLiabilities, netAssets, revenueFromStatements, operatingIncome
5. **取引先分離（3）**: clients, subsidiaries, suppliers, banks（分離済み）
6. **51.csv固有（8）**: departmentLocation, specialties, averageAge, averageYearsOfService, averageOvertimeHours, averagePaidLeave, femaleExecutiveRatio, transportation
7. **127/128固有（3）**: nikkeiCode, issuedShares, affiliations
8. **その他（2）**: bankCorporateNumber, nameEn

---

## ⚠️ CSVでスキップまたは保留されたフィールド

### 1. 130.csv/131.csvの保留フィールド
- **departments**（元データ）: specialNoteに一部保存
- **people**（元データ）: overviewに一部保存
- **rawText**（元データ）: companyDescriptionに一部保存

### 2. 127.csv/128.csvの保留JSON
- **削除したJSONフィールド（17個）**: topTabsJson, leftNavJson, overviewTabJson, orgJson, basicJson, financeJson, compareMAJson, shareholdersJson, shareholdersMeetingJson, esgJson, notesJson, analysisJson, segmentsJson, bankBorrowingsJson, forecastJson など
- **理由**: 情報の大部分はsummaryJsonとstatementsJsonから抽出済み

### 3. 107タイプ（タイプC）のUnnamedカラム
- **Unnamed: 38-46**（9列）: 空列のため無視
- **理由**: データなし

### 4. タイプJ3（その他22ファイル）の特殊カラム
- **会社ID, リストID**: 内部管理用IDのためスキップ
- **SBフラグ, NDA, AD, ステータス**: ステータス管理用のためスキップ
- **売DM最終送信日時, 買DM最終送信日時** など: 営業活動記録のためスキップ

### 5. 132.csvの特殊フィールド
- **種別, 状態, NDA締結, AD締結**: 内部管理用
- **担当者**: registrantとして削除済み
- **各種最終送信日時**: 営業活動記録

---

## 📋 全CSVファイルの状態

| タイプ | ファイル数 | 状態 | 備考 |
|-------|----------|------|------|
| A | 55 | ✅ 正常 | 基本形式 |
| B | 26 | ✅ 正常 | 創業あり |
| C | 6 | ✅ 修正済み | 業種（細）追加 |
| D | 6 | ✅ 正常 | 法人番号先頭 |
| E | 4 | ✅ 正常 | 都道府県あり |
| F | 1 | ✅ 正常 | 決算5期分 |
| G | 4 | ✅ 正常 | 業種4あり |
| H | 4 | ✅ 正常 | 業種4あり |
| I | 2 | ✅ 展開済み | 130.csv, 131.csv（61フィールド） |
| J1 | 2 | ✅ 統一済み | 127.csv, 128.csv（27フィールド） |
| J2 | 1 | ✅ 正常 | 51.csv |
| J3 | 22 | ✅ 正常 | その他直近決算型 |

**合計**: 133ファイル（129.csvを除く134ファイル中）

---

## 🎯 次のステップ

### CSV更新が必要なファイル（確認）
```bash
# 130/131は既に更新済みか確認
head -1 csv/130.csv | tr ',' '\n' | wc -l  # 58なら展開済み

# 127/128は既に更新済みか確認
head -1 csv/127.csv | tr ',' '\n' | wc -l  # 27なら統一済み
```

### バックフィル実行
```bash
cd /Users/harumacmini/Downloads/road_companiesInfo

# 全CSVを一括バックフィル
GOOGLE_APPLICATION_CREDENTIALS=./albert-ma-firebase-adminsdk-iat1k-a64039899f.json \
./scripts/run_backfill_by_type.sh
```

---

## 📝 重要な変更点

1. **取引先を4つに分離**: clients, subsidiaries, suppliers, banks
2. **決算情報を拡張**: 5期分個別フィールド
3. **役員情報を個別化**: 10名まで個別フィールド
4. **部署情報を個別化**: 7部署まで個別フィールド  
5. **財務情報を詳細化**: 総資産、総負債、純資産、営業利益
6. **業種分類を統一**: industryLarge, industryMiddle, industrySmall, industryDetail
7. **registrantを削除**: 担当者情報は後で設定

---

## 🔍 データ品質チェックポイント

### 確認推奨ファイル（タイプ別代表）
- **タイプD**: 1.csv（法人番号先頭）
- **タイプB**: 100.csv（創業あり）
- **タイプC**: 107.csv（業種（細）追加済み）
- **タイプF**: 132.csv（決算5期分）
- **タイプI**: 130.csv（61フィールド展開）
- **タイプJ1**: 127.csv（27フィールド統一）
- **タイプJ2**: 51.csv（特殊ヘッダー）

