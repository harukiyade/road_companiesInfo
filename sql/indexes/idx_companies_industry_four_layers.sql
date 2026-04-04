-- 業種4階層（large / middle / small / detail）のカスケード取得・検索向け複合インデックス
-- CREATE INDEX CONCURRENTLY はトランザクション内では実行できないため、単独で実行してください。

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_companies_industry_four_layers
ON public.companies (
  industry_large,
  industry_middle,
  industry_small,
  industry_detail
);
