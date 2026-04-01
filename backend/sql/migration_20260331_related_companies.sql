-- Related companies extracted from EDINET (XBRL)
CREATE TABLE IF NOT EXISTS related_companies (
    id BIGSERIAL PRIMARY KEY,
    parent_company_id VARCHAR(255) NOT NULL,
    parent_corporate_number VARCHAR(13) NOT NULL,
    child_company_id VARCHAR(255),
    child_company_name TEXT NOT NULL,
    child_name_normalized TEXT NOT NULL,
    relation_type VARCHAR(64),
    voting_rights_ratio NUMERIC(7,3),
    source_doc_id VARCHAR(32),
    parent_edinet_code VARCHAR(16),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_related_parent_company FOREIGN KEY (parent_company_id) REFERENCES companies(id),
    CONSTRAINT fk_related_child_company FOREIGN KEY (child_company_id) REFERENCES companies(id),
    CONSTRAINT uq_related_parent_child_name UNIQUE (parent_company_id, child_name_normalized)
);

CREATE INDEX IF NOT EXISTS idx_related_parent_company_id ON related_companies(parent_company_id);
CREATE INDEX IF NOT EXISTS idx_related_child_company_id ON related_companies(child_company_id);
CREATE INDEX IF NOT EXISTS idx_related_parent_corporate_number ON related_companies(parent_corporate_number);
CREATE INDEX IF NOT EXISTS idx_related_relation_type ON related_companies(relation_type);
