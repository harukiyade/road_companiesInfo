-- ============================================
-- companies テーブル定義 (DDL)
-- Firestore companies_new コレクションから移行
-- ============================================

-- テーブル作成
CREATE TABLE IF NOT EXISTS companies (
    -- Primary Key
    id VARCHAR(255) PRIMARY KEY,
    
    -- ============================================
    -- 基本情報（15フィールド）
    -- ============================================
    company_id VARCHAR(255),
    name VARCHAR(500) NOT NULL,
    name_en VARCHAR(500),
    kana VARCHAR(500),
    corporate_number VARCHAR(13),
    corporation_type VARCHAR(100),
    nikkei_code VARCHAR(10),
    badges TEXT[],
    tags TEXT[],
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    update_date DATE,
    update_count INTEGER DEFAULT 0,
    change_count INTEGER DEFAULT 0,
    qualification_grade VARCHAR(100),
    
    -- ============================================
    -- 所在地情報（6フィールド）
    -- ============================================
    prefecture VARCHAR(50),
    address TEXT,
    headquarters_address TEXT,
    postal_code VARCHAR(10),
    location TEXT,
    department_location TEXT,
    
    -- ============================================
    -- 連絡先情報（6フィールド）
    -- ============================================
    phone_number VARCHAR(50),
    contact_phone_number VARCHAR(50),
    fax VARCHAR(50),
    email VARCHAR(255),
    company_url TEXT,
    contact_form_url TEXT,
    
    -- ============================================
    -- 代表者情報（10フィールド）
    -- ============================================
    representative_name VARCHAR(200),
    representative_kana VARCHAR(200),
    representative_title VARCHAR(100),
    representative_birth_date DATE,
    representative_phone VARCHAR(50),
    representative_postal_code VARCHAR(10),
    representative_home_address TEXT,
    representative_registered_address TEXT,
    representative_alma_mater VARCHAR(200),
    executives JSONB,
    
    -- ============================================
    -- 役員情報（20フィールド）
    -- ============================================
    executive_name1 VARCHAR(200),
    executive_name2 VARCHAR(200),
    executive_name3 VARCHAR(200),
    executive_name4 VARCHAR(200),
    executive_name5 VARCHAR(200),
    executive_name6 VARCHAR(200),
    executive_name7 VARCHAR(200),
    executive_name8 VARCHAR(200),
    executive_name9 VARCHAR(200),
    executive_name10 VARCHAR(200),
    executive_position1 VARCHAR(100),
    executive_position2 VARCHAR(100),
    executive_position3 VARCHAR(100),
    executive_position4 VARCHAR(100),
    executive_position5 VARCHAR(100),
    executive_position6 VARCHAR(100),
    executive_position7 VARCHAR(100),
    executive_position8 VARCHAR(100),
    executive_position9 VARCHAR(100),
    executive_position10 VARCHAR(100),
    
    -- ============================================
    -- 業種情報（13フィールド）
    -- ============================================
    industry VARCHAR(200),
    industry_large VARCHAR(200),
    industry_middle VARCHAR(200),
    industry_small VARCHAR(200),
    industry_detail VARCHAR(200),
    industries TEXT[],
    industry_categories TEXT[],
    business_descriptions TEXT,
    business_items TEXT[],
    business_summary TEXT,
    specialties TEXT[],
    demand_products TEXT[],
    special_note TEXT,
    
    -- ============================================
    -- 財務情報（29フィールド）
    -- ============================================
    capital_stock BIGINT,
    revenue BIGINT,
    latest_revenue BIGINT,
    latest_profit BIGINT,
    revenue_from_statements BIGINT,
    operating_income BIGINT,
    total_assets BIGINT,
    total_liabilities BIGINT,
    net_assets BIGINT,
    issued_shares BIGINT,
    financials JSONB,
    listing VARCHAR(100),
    market_segment VARCHAR(100),
    latest_fiscal_year_month VARCHAR(10),
    fiscal_month INTEGER,
    fiscal_month1 INTEGER,
    fiscal_month2 INTEGER,
    fiscal_month3 INTEGER,
    fiscal_month4 INTEGER,
    fiscal_month5 INTEGER,
    revenue1 BIGINT,
    revenue2 BIGINT,
    revenue3 BIGINT,
    revenue4 BIGINT,
    revenue5 BIGINT,
    profit1 BIGINT,
    profit2 BIGINT,
    profit3 BIGINT,
    profit4 BIGINT,
    profit5 BIGINT,
    
    -- ============================================
    -- 企業規模・組織（10フィールド）
    -- ============================================
    employee_count INTEGER,
    employee_number VARCHAR(50),
    factory_count INTEGER,
    office_count INTEGER,
    store_count INTEGER,
    average_age NUMERIC(5, 2),
    average_years_of_service NUMERIC(5, 2),
    average_overtime_hours NUMERIC(5, 2),
    average_paid_leave NUMERIC(5, 2),
    female_executive_ratio NUMERIC(5, 2),
    
    -- ============================================
    -- 設立・沿革（5フィールド）
    -- ============================================
    established DATE,
    date_of_establishment DATE,
    founding DATE,
    founding_year INTEGER,
    acquisition TEXT,
    
    -- ============================================
    -- 取引先・関係会社（7フィールド）
    -- ============================================
    clients JSONB,
    suppliers JSONB,
    subsidiaries JSONB,
    affiliations JSONB,
    shareholders JSONB,
    banks JSONB,
    bank_corporate_number VARCHAR(13),
    
    -- ============================================
    -- 部署・拠点情報（21フィールド）
    -- ============================================
    department_name1 VARCHAR(200),
    department_name2 VARCHAR(200),
    department_name3 VARCHAR(200),
    department_name4 VARCHAR(200),
    department_name5 VARCHAR(200),
    department_name6 VARCHAR(200),
    department_name7 VARCHAR(200),
    department_address1 TEXT,
    department_address2 TEXT,
    department_address3 TEXT,
    department_address4 TEXT,
    department_address5 TEXT,
    department_address6 TEXT,
    department_address7 TEXT,
    department_phone1 VARCHAR(50),
    department_phone2 VARCHAR(50),
    department_phone3 VARCHAR(50),
    department_phone4 VARCHAR(50),
    department_phone5 VARCHAR(50),
    department_phone6 VARCHAR(50),
    department_phone7 VARCHAR(50),
    
    -- ============================================
    -- 企業説明（4フィールド）
    -- ============================================
    overview TEXT,
    company_description TEXT,
    business_descriptions_text TEXT,
    sales_notes TEXT,
    
    -- ============================================
    -- SNS・外部リンク（8フィールド）
    -- ============================================
    urls TEXT[],
    profile_url TEXT,
    external_detail_url TEXT,
    facebook TEXT,
    linkedin TEXT,
    wantedly TEXT,
    youtrust TEXT,
    meta_keywords TEXT[]
);

-- ============================================
-- インデックス作成
-- ============================================

-- 都道府県インデックス（検索で頻繁に使用）
CREATE INDEX IF NOT EXISTS idx_companies_prefecture ON companies(prefecture);

-- 売上高インデックス（範囲検索用）
CREATE INDEX IF NOT EXISTS idx_companies_revenue ON companies(revenue);

-- 上場区分インデックス
CREATE INDEX IF NOT EXISTS idx_companies_listing ON companies(listing);

-- 業種タグのGINインデックス（配列検索用）
CREATE INDEX IF NOT EXISTS idx_companies_industries_gin ON companies USING GIN(industries);

-- 業種カテゴリのGINインデックス
CREATE INDEX IF NOT EXISTS idx_companies_industry_categories_gin ON companies USING GIN(industry_categories);

-- 企業名検索用インデックス
CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name);

-- 法人番号インデックス
CREATE INDEX IF NOT EXISTS idx_companies_corporate_number ON companies(corporate_number);

-- 業種インデックス
CREATE INDEX IF NOT EXISTS idx_companies_industry ON companies(industry);

-- 業種大分類インデックス
CREATE INDEX IF NOT EXISTS idx_companies_industry_large ON companies(industry_large);

-- 業種中分類インデックス
CREATE INDEX IF NOT EXISTS idx_companies_industry_middle ON companies(industry_middle);

-- 業種小分類インデックス
CREATE INDEX IF NOT EXISTS idx_companies_industry_small ON companies(industry_small);

-- 資本金インデックス
CREATE INDEX IF NOT EXISTS idx_companies_capital_stock ON companies(capital_stock);

-- 従業員数インデックス
CREATE INDEX IF NOT EXISTS idx_companies_employee_count ON companies(employee_count);

-- 複合インデックス（都道府県 + 売上高）
CREATE INDEX IF NOT EXISTS idx_companies_prefecture_revenue ON companies(prefecture, revenue);

-- JSONBフィールドのGINインデックス（related_companies等の検索用）
CREATE INDEX IF NOT EXISTS idx_companies_executives_gin ON companies USING GIN(executives);
CREATE INDEX IF NOT EXISTS idx_companies_financials_gin ON companies USING GIN(financials);
CREATE INDEX IF NOT EXISTS idx_companies_clients_gin ON companies USING GIN(clients);
CREATE INDEX IF NOT EXISTS idx_companies_subsidiaries_gin ON companies USING GIN(subsidiaries);
CREATE INDEX IF NOT EXISTS idx_companies_affiliations_gin ON companies USING GIN(affiliations);

-- コメント追加
COMMENT ON TABLE companies IS '企業情報テーブル - Firestore companies_new コレクションから移行';
COMMENT ON COLUMN companies.id IS 'FirestoreのドキュメントID（Primary Key）';
COMMENT ON COLUMN companies.industries IS '業種タグ配列（検索用GINインデックス付き）';
COMMENT ON COLUMN companies.executives IS '役員一覧（JSONB形式）';
COMMENT ON COLUMN companies.financials IS '財務情報（JSONB形式）';
COMMENT ON COLUMN companies.revenue IS '売上高（BIGINT型、桁あふれ防止）';
