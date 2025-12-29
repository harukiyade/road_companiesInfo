# companies_new コレクション フィールド構成レポート

作成日: 2025年1月

## 概要

`companies_new`コレクションの現在のフィールド構成を調査した結果をまとめます。

## 新規追加フィールド

以下のフィールドが新たに追加されています：

### ✨ transactionType（取引種別）
- **型**: `string | null`
- **説明**: 取引種別を表すフィールド
- **取り得る値**:
  - `"譲受企業"`: 譲受企業（上場企業など）
  - `null`: 未設定
- **設定状況**:
  - `listing="上場"`の企業は自動的に`"譲受企業"`に設定される
  - その他の企業は`null`

### ✨ needs
- **型**: `null`（将来的に他の型になる可能性あり）
- **説明**: ニーズ情報を格納するフィールド（現在は未使用）
- **設定状況**: 全ドキュメントで`null`

### ✨ securityCode（証券コード）
- **型**: `string | null`
- **説明**: 上場企業の証券コード（4桁の数字）
- **設定状況**:
  - 上場企業の一部に設定されている
  - 例: `"2930"`, `"4088"`, `"9450"`, `"3802"`, `"3987"`, `"7091"`など

## 全フィールド一覧

### 基本情報フィールド

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `name` | `string` | 企業名 |
| `kana` | `string \| null` | 企業名（カナ） |
| `nameEn` | `string \| null` | 企業名（英語） |
| `corporateNumber` | `string` | 法人番号（13桁） |
| `corporationType` | `string \| null` | 法人種別 |

### 住所・連絡先フィールド

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `address` | `string \| null` | 住所（フルテキスト） |
| `postalCode` | `string \| null` | 郵便番号 |
| `prefecture` | `string \| null` | 都道府県 |
| `headquartersAddress` | `string \| null` | 本社住所 |
| `phoneNumber` | `string \| null` | 電話番号 |
| `contactPhoneNumber` | `string \| null` | 連絡先電話番号 |
| `fax` | `string \| null` | FAX番号 |
| `email` | `string \| null` | メールアドレス |
| `companyUrl` | `string \| null` | 企業URL |
| `contactFormUrl` | `string \| null` | お問い合わせフォームURL |

### 業種・事業フィールド

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `industry` | `string \| null` | 業種（主要） |
| `industries` | `string[]` | 業種（配列） |
| `industryLarge` | `string \| null` | 業種（大分類） |
| `industryMiddle` | `string \| null` | 業種（中分類） |
| `industrySmall` | `string \| null` | 業種（小分類） |
| `industryDetail` | `string \| null` | 業種（詳細） |
| `industryCategories` | `string \| null` | 業種カテゴリ |
| `businessDescriptions` | `string \| null` | 事業内容説明 |
| `businessItems` | `string[]` | 事業項目 |
| `businessSummary` | `string \| null` | 事業概要 |

### 財務・経営情報フィールド

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `capitalStock` | `number \| null` | 資本金 |
| `revenue` | `number \| null` | 売上高 |
| `revenueFromStatements` | `number \| null` | 売上高（決算書より） |
| `employeeCount` | `number \| null` | 従業員数 |
| `employeeNumber` | `number \| null` | 従業員数（別形式） |
| `foundingYear` | `number \| null` | 設立年 |
| `fiscalMonth` | `number \| null` | 決算月 |
| `financials` | `object \| null` | 財務情報 |
| `factoryCount` | `number \| null` | 工場数 |
| `officeCount` | `number \| null` | 事業所数 |
| `storeCount` | `number \| null` | 店舗数 |

### 上場関連フィールド

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `listing` | `string \| null` | 上場区分（例: `"上場"`） |
| `marketSegment` | `string \| null` | 市場区分 |
| `securityCode` | `string \| null` | 証券コード（4桁）✨新規 |
| `securitiesCode` | `string \| null` | 証券コード（別形式） |
| `nikkeiCode` | `string \| null` | 日経コード |
| `tradingStatus` | `string \| null` | 取引状況 |

### 取引種別フィールド

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `transactionType` | `string \| null` | 取引種別✨新規 |
| `needs` | `any \| null` | ニーズ情報✨新規 |

### 代表者・役員情報フィールド

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `representativeName` | `string \| null` | 代表者名 |
| `representativeKana` | `string \| null` | 代表者名（カナ） |
| `representativeTitle` | `string \| null` | 代表者役職 |
| `representativeBirthDate` | `string \| null` | 代表者生年月日 |
| `representativePhone` | `string \| null` | 代表者電話番号 |
| `representativeHomeAddress` | `string \| null` | 代表者自宅住所 |
| `representativeRegisteredAddress` | `string \| null` | 代表者登録住所 |
| `representativeAlmaMater` | `string \| null` | 代表者出身校 |
| `executives` | `string \| null` | 役員情報 |
| `executiveName1` ~ `executiveName10` | `string \| null` | 役員名（1-10） |
| `executivePosition1` ~ `executivePosition10` | `string \| null` | 役員役職（1-10） |

### 組織・関連企業フィールド

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `subsidiaries` | `string[]` | 子会社リスト |
| `shareholders` | `string[]` | 株主リスト |
| `suppliers` | `string[]` | 取引先（サプライヤー）リスト |
| `clients` | `string \| null` | 顧客情報 |
| `relatedCompanies` | `any \| null` | 関連企業情報 |
| `banks` | `string[]` | 取引銀行リスト |
| `bankCorporateNumber` | `string \| null` | 銀行法人番号 |

### その他のフィールド

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `tags` | `string[]` | タグ |
| `urls` | `string[]` | URLリスト |
| `overview` | `string \| null` | 概要 |
| `companyDescription` | `string \| null` | 企業説明 |
| `demandProducts` | `string \| null` | 需要製品 |
| `salesNotes` | `string \| null` | 営業ノート |
| `acquisition` | `any \| null` | 買収情報 |
| `facebook` | `string \| null` | Facebook URL |
| `linkedin` | `string \| null` | LinkedIn URL |
| `wantedly` | `string \| null` | Wantedly URL |
| `youtrust` | `string \| null` | YouTrust URL |
| `externalDetailUrl` | `string \| null` | 外部詳細URL |
| `profileUrl` | `string \| null` | プロフィールURL |
| `metaDescription` | `string \| null` | メタ説明 |
| `metaKeywords` | `string \| null` | メタキーワード |
| `adExpiration` | `any \| null` | 広告期限 |
| `registrant` | `string \| null` | 登録者 |
| `location` | `string \| null` | 所在地 |
| `departmentLocation` | `string \| null` | 部門所在地 |

### タイムスタンプ・メタデータフィールド

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `createdAt` | `Timestamp \| null` | 作成日時 |
| `updatedAt` | `Timestamp \| null` | 更新日時 |
| `extendedFieldsScrapedAt` | `Timestamp \| null` | 拡張フィールド取得日時 |
| `updateCount` | `number \| null` | 更新回数 |
| `changeCount` | `number \| null` | 変更回数 |

### 上場企業専用フィールド

上場企業には以下の追加フィールドが存在します：

| フィールド名 | 型 | 説明 |
|------------|----|----|
| `listedParentName` | `string \| null` | 親会社名 |
| `listedParentCorporateNumber` | `string \| null` | 親会社法人番号 |
| `listedParentEdinet` | `string \| null` | 親会社EDINETコード |
| `listedGroupAsOf` | `string \| null` | グループ情報取得日 |
| `listedGroupCached` | `any \| null` | グループ情報（キャッシュ） |
| `listedGroupConfidence` | `number \| null` | グループ情報信頼度 |
| `listedGroupConsolidation` | `any \| null` | 連結情報 |
| `listedGroupOwnership` | `any \| null` | 所有関係 |
| `listedGroupSource` | `string \| null` | グループ情報ソース |

## フィールド数の統計

- **通常の企業**: 約66フィールド
- **上場企業**: 約115フィールド（追加フィールド含む）

## 重要な変更点

1. **transactionTypeフィールドの追加**
   - 全ドキュメントに追加済み
   - 上場企業は自動的に`"譲受企業"`に設定

2. **needsフィールドの追加**
   - 全ドキュメントに追加済み（現在は全て`null`）

3. **securityCodeフィールドの追加**
   - 上場企業の一部に設定済み

## 注意事項

- フィールドの多くは`null`で初期化されている
- 配列型フィールドは空配列`[]`で初期化される
- 上場企業には追加のフィールドが存在する
- 一部のフィールドはCSVインポート時に動的に追加される可能性がある

