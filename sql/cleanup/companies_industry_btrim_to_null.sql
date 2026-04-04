-- companies の業種4カラムで、空文字・空白のみを NULL に統一（PostgreSQL）
-- 580万件規模ではメンテナンスウィンドウで実行し、必要なら主キー範囲で分割してください。
-- 実行前に件数確認用:
--
-- SELECT count(*) FROM public.companies
-- WHERE (industry_large IS NOT NULL AND btrim(industry_large) = '')
--    OR (industry_middle IS NOT NULL AND btrim(industry_middle) = '')
--    OR (industry_small IS NOT NULL AND btrim(industry_small) = '')
--    OR (industry_detail IS NOT NULL AND btrim(industry_detail) = '');

BEGIN;

UPDATE public.companies
SET industry_large = NULL
WHERE industry_large IS NOT NULL
  AND btrim(industry_large) = '';

UPDATE public.companies
SET industry_middle = NULL
WHERE industry_middle IS NOT NULL
  AND btrim(industry_middle) = '';

UPDATE public.companies
SET industry_small = NULL
WHERE industry_small IS NOT NULL
  AND btrim(industry_small) = '';

UPDATE public.companies
SET industry_detail = NULL
WHERE industry_detail IS NOT NULL
  AND btrim(industry_detail) = '';

COMMIT;
