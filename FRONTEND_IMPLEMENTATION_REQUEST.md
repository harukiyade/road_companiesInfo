# フロントエンド実装依頼：関連会社タブ機能

## 概要

Firestoreの`companies_new`コレクションに保存されている関連会社情報を表示するUIタブを実装してください。

## データ構造

### Firestoreコレクション: `companies_new`

各企業ドキュメントに以下のフィールドが追加されています：

#### 1. 財務情報 (`financialData`)
```typescript
interface FinancialData {
  fiscalYear: string;           // 決算期（例: "2024年3月期"）
  revenue?: number;              // 売上高（千円）
  operatingIncome?: number;      // 営業利益（千円）
  ordinaryIncome?: number;       // 経常利益（千円）
  netIncome?: number;            // 当期純利益（千円）
  totalAssets?: number;          // 総資産（千円）
  totalEquity?: number;          // 純資産（千円）
  totalLiabilities?: number;     // 総負債（千円）
  capital?: number;              // 資本金（千円）
}
```

#### 2. 関連会社情報 (`relatedCompanies`)
```typescript
interface RelatedCompany {
  name: string;                  // 会社名
  relationship: "子会社" | "関連会社" | "その他" | "親会社";  // 関連性
  capital?: number;              // 資本金（千円）
  equityRatio?: number;          // 持株比率（%）
  address?: string;              // 所在地
}
```

## 実装要件

### 1. 関連会社タブの作成

企業詳細画面に「関連会社」タブを追加してください。

#### タブの表示条件
- `relatedCompanies`フィールドが存在し、配列の長さが0より大きい場合のみタブを表示
- データがない場合はタブを非表示にする

### 2. 関連会社一覧の表示

関連会社タブ内に以下の情報を表示するテーブルを作成してください：

#### 表示項目
1. **会社名** (`name`)
   - クリック可能なリンクとして表示
   - クリック時は該当企業の詳細ページに遷移

2. **関連性** (`relationship`)
   - バッジまたはラベルで表示
   - 色分け推奨：
     - 子会社: 青系
     - 関連会社: 緑系
     - 親会社: オレンジ系
     - その他: グレー系

3. **持株比率** (`equityRatio`)
   - パーセンテージ表示（例: "50.5%"）
   - データがない場合は "-" を表示

4. **資本金** (`capital`)
   - 千円単位で表示
   - 3桁区切りで表示（例: "1,000,000千円"）
   - データがない場合は "-" を表示

5. **所在地** (`address`)
   - データがない場合は "-" を表示

#### テーブルの機能
- ソート機能（会社名、関連性、持株比率でソート可能）
- フィルタ機能（関連性でフィルタリング可能）
- レスポンシブ対応（モバイル表示にも対応）

### 3. 関連性によるグループ化表示（オプション）

関連性（子会社、関連会社、親会社）ごとにセクションを分けて表示することも検討してください。

### 4. 財務情報の表示（オプション）

関連会社タブ内に、本体企業の財務情報（`financialData`）を表示するセクションを追加することも検討してください。

#### 財務情報の表示形式
- 決算期ごとにテーブルまたはカード形式で表示
- 最新の決算期を最初に表示
- 各項目は3桁区切りで表示（例: "1,000,000千円"）

## 技術要件

### データ取得
- Firestoreから`companies_new`コレクションの企業ドキュメントを取得
- `relatedCompanies`フィールドを配列として取得
- エラーハンドリングを実装（データがない場合、取得エラーの場合）

### パフォーマンス
- 関連会社が多数ある場合でもスムーズに表示できるよう、必要に応じてページネーションまたは仮想スクロールを実装

### UI/UX
- モダンで使いやすいUIデザイン
- ローディング状態の表示
- エラー状態の表示
- 空データ状態の表示（関連会社がない場合のメッセージ）

## 実装例（参考）

```typescript
// 型定義の例
interface CompanyDetail {
  id: string;
  name: string;
  relatedCompanies?: RelatedCompany[];
  financialData?: FinancialData[];
}

// コンポーネントの例（React/Next.jsの場合）
function RelatedCompaniesTab({ company }: { company: CompanyDetail }) {
  if (!company.relatedCompanies || company.relatedCompanies.length === 0) {
    return <div>関連会社情報がありません</div>;
  }

  return (
    <div>
      <h2>関連会社一覧</h2>
      <table>
        <thead>
          <tr>
            <th>会社名</th>
            <th>関連性</th>
            <th>持株比率</th>
            <th>資本金</th>
            <th>所在地</th>
          </tr>
        </thead>
        <tbody>
          {company.relatedCompanies.map((related, index) => (
            <tr key={index}>
              <td>
                <Link href={`/companies/${related.name}`}>
                  {related.name}
                </Link>
              </td>
              <td>
                <Badge type={related.relationship}>
                  {related.relationship}
                </Badge>
              </td>
              <td>
                {related.equityRatio !== undefined 
                  ? `${related.equityRatio}%` 
                  : "-"}
              </td>
              <td>
                {related.capital !== undefined 
                  ? `${formatNumber(related.capital)}千円` 
                  : "-"}
              </td>
              <td>{related.address || "-"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
```

## 注意事項

1. **企業名の正規化**
   - 企業名には「（株）」「株式会社」など様々な表記があるため、表示時は統一された形式で表示することを推奨

2. **双方向の関係**
   - 本体企業の関連会社として「子会社A」が登録されている場合、子会社Aの関連会社として「親会社（本体企業）」も登録されています
   - 両方の企業で関連会社タブが正しく表示されることを確認してください

3. **データの整合性**
   - `relatedCompanies`が`undefined`や`null`の場合のハンドリング
   - 配列が空の場合の表示

4. **パフォーマンス**
   - 関連会社が100社以上ある場合のパフォーマンスを考慮
   - 必要に応じてページネーションや仮想スクロールを実装

## テスト要件

以下のケースをテストしてください：

1. 関連会社がある場合の表示
2. 関連会社がない場合の表示（タブ非表示）
3. 関連会社が多数ある場合の表示（パフォーマンステスト）
4. データ取得エラー時の表示
5. 各関連性（子会社、関連会社、親会社）の表示
6. モバイル表示での動作確認

## 参考情報

- Firestoreコレクション: `companies_new`
- フィールド名: `relatedCompanies` (配列)
- フィールド名: `financialData` (配列、オプション)

