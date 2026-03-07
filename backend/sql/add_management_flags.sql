-- companies テーブルに管理フラグ（SB・NDA・AD）を追加
-- import_full_update_fast.py 実行前に実行してください。

ALTER TABLE companies ADD COLUMN IF NOT EXISTS sb_flag BOOLEAN;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS nda_flag BOOLEAN;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS ad_flag BOOLEAN;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

COMMENT ON COLUMN companies.sb_flag IS 'SBフラグ（CSV「SBフラグ」から投入）';
COMMENT ON COLUMN companies.nda_flag IS 'NDA締結フラグ（CSV「NDA」から投入）';
COMMENT ON COLUMN companies.ad_flag IS 'AD締結フラグ（CSV「AD」から投入）';
COMMENT ON COLUMN companies.is_active IS 'アクティブ判定（状態/ステータスに「解散」等が含まれればfalse）';
