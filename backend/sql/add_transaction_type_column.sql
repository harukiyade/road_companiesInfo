-- companies テーブルに transaction_type カラムを追加する（取引種別用）
-- update_companies_transaction.py 実行前に未定義の場合のみ実行してください。

ALTER TABLE companies
ADD COLUMN IF NOT EXISTS transaction_type VARCHAR(100);

COMMENT ON COLUMN companies.transaction_type IS '取引種別（CSV「取引種別」から投入）';
