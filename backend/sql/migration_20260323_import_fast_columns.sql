-- =============================================================================
-- import_full_update_fast.py 用: 既存 DB で列が欠けている場合の補完
-- create_companies_table.sql と整合。実行は環境に応じて選択（IF NOT EXISTS）。
-- =============================================================================

-- --- 財務・決算 ---
ALTER TABLE companies ADD COLUMN IF NOT EXISTS latest_fiscal_year_month VARCHAR(10);

-- --- 設立（フル日付）---
ALTER TABLE companies ADD COLUMN IF NOT EXISTS founding DATE;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS founding_year INTEGER;

-- --- 事業説明（J列結合先・事業詳細）---
ALTER TABLE companies ADD COLUMN IF NOT EXISTS business_summary TEXT;

-- --- 代表者 ---
ALTER TABLE companies ADD COLUMN IF NOT EXISTS representative_birth_date DATE;

-- --- 取引・株主（JSONB。型が TEXT[] の環境は手動で型変更が必要な場合あり）---
ALTER TABLE companies ADD COLUMN IF NOT EXISTS shareholders JSONB;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS suppliers JSONB;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS clients JSONB;

-- --- 参照: よくある欠落列 ---
ALTER TABLE companies ADD COLUMN IF NOT EXISTS overview TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS company_description TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS latest_revenue BIGINT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS latest_profit BIGINT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS capital_stock BIGINT;

COMMENT ON COLUMN companies.latest_fiscal_year_month IS 'CSV 直近決算年月';
COMMENT ON COLUMN companies.founding IS '設立日（フル日付）— import_full_update_fast が設立列からパース';
COMMENT ON COLUMN companies.business_summary IS '事業詳細（import_firstTime J列等を結合）';
COMMENT ON COLUMN companies.shareholders IS '株主（CSV 株主・株式保有率）';
COMMENT ON COLUMN companies.suppliers IS '仕入れ先';
