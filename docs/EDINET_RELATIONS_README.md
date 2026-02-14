# EDINET「関係会社の状況」取得・統合プロジェクト

## 概要
上場企業の「関係会社の状況」をEDINETから取得し、既存の企業DBに「関連会社リンク」として統合する。

## 1. スクレイピングエンジン

### 実行方法
```bash
# 依存関係
pip install -r scripts/requirements-edinet.txt

# 環境変数（推奨）
export EDINET_API_KEY=your_api_key  # https://api.edinet-fsa.go.jp/api/auth/index.aspx?mode=1

# 実行（証券コードを指定）
python scripts/fetch_edinet_relations.py 4578 2181 7270

# カンマ区切りで指定
python scripts/fetch_edinet_relations.py --codes 4578,2181,7270

# 出力先指定・既存CSVを上書き
python scripts/fetch_edinet_relations.py 4578 --output data/edinet_relations.csv --overwrite
```

### 出力
- `data/edinet_relations.csv`
- カラム: 親会社名, 子会社名, 住所, 議決権所有割合
- 子会社名は正規化済み（(株)→株式会社 など）

## 2. クリーニング
`fetch_edinet_relations.py` 内で以下を実施:
- 企業名正規化: （株）→株式会社、（有）→有限会社 等
- 議決権所有割合の数値抽出
- 重複行の除去

## 3. 突合（マッピング）ロジック
[EDINET_RELATIONS_MATCHING_LOGIC.md](./EDINET_RELATIONS_MATCHING_LOGIC.md) を参照。
