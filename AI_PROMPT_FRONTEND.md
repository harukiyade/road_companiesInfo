# AIへの依頼プロンプト（フロントエンド実装）

以下のプロンプトをコピーしてAIに依頼してください：

---

## プロンプト本文

企業詳細画面に「関連会社」タブを実装してください。

### データ構造

Firestoreの`companies_new`コレクションの各企業ドキュメントに、以下のフィールドが保存されています：

```typescript
// 関連会社情報（配列）
relatedCompanies?: Array<{
  name: string;                    // 会社名
  relationship: "子会社" | "関連会社" | "その他" | "親会社";  // 関連性
  capital?: number;                // 資本金（千円）
  equityRatio?: number;            // 持株比率（%）
  address?: string;                 // 所在地
}>

// 財務情報（配列、オプション）
financialData?: Array<{
  fiscalYear: string;               // 決算期
  revenue?: number;                // 売上高（千円）
  operatingIncome?: number;        // 営業利益（千円）
  ordinaryIncome?: number;         // 経常利益（千円）
  netIncome?: number;              // 当期純利益（千円）
  totalAssets?: number;            // 総資産（千円）
  totalEquity?: number;            // 純資産（千円）
  totalLiabilities?: number;       // 総負債（千円）
  capital?: number;                // 資本金（千円）
}>
```

### 実装要件

1. **関連会社タブの追加**
   - 企業詳細画面に「関連会社」タブを追加
   - `relatedCompanies`が存在し、配列の長さが0より大きい場合のみ表示

2. **関連会社一覧テーブル**
   - 以下の項目を表示：
     - 会社名（クリック可能なリンク）
     - 関連性（バッジ表示、色分け）
     - 持株比率（%表示、データなしは"-"）
     - 資本金（3桁区切り、千円単位、データなしは"-"）
     - 所在地（データなしは"-"）
   - ソート機能（会社名、関連性、持株比率）
   - フィルタ機能（関連性でフィルタリング）
   - レスポンシブ対応

3. **UI要件**
   - モダンで使いやすいデザイン
   - ローディング状態の表示
   - エラー状態の表示
   - 空データ状態の表示

4. **パフォーマンス**
   - 関連会社が多数ある場合でもスムーズに表示
   - 必要に応じてページネーションまたは仮想スクロールを実装

### 注意事項

- 企業名の表記揺れ（（株）と株式会社など）に対応
- `relatedCompanies`が`undefined`や`null`、空配列の場合のハンドリング
- 双方向の関係（本体企業と子会社の両方で関連会社情報が表示される）

### 技術スタック

- Next.js / React
- Firestore（`companies_new`コレクション）
- TypeScript

上記の要件に基づいて、関連会社タブ機能を実装してください。

---

## 使用例

このプロンプトをAI（ChatGPT、Claude、Cursor AIなど）に送信すると、実装コードを生成してもらえます。

必要に応じて、以下の情報も追加してください：
- 使用しているUIライブラリ（Material-UI、Tailwind CSS、Chakra UIなど）
- 既存のコードベースの構造
- 特定のデザイン要件

