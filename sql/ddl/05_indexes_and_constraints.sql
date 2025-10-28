-- Indexes and constraints for performance optimization

-- Staging table indexes
CREATE INDEX idx_common_crawl_url ON staging.common_crawl_raw(website_url);
CREATE INDEX idx_common_crawl_extraction_date ON staging.common_crawl_raw(extraction_date);

CREATE INDEX idx_abr_abn ON staging.abr_raw(abn);
CREATE INDEX idx_abr_postcode ON staging.abr_raw(address_postcode);
CREATE INDEX idx_abr_state ON staging.abr_raw(address_state_code);
CREATE INDEX idx_abr_status ON staging.abr_raw(entity_status_code);

-- Entity matching indexes
CREATE INDEX idx_entity_matching_similarity ON staging.entity_matching_candidates(similarity_score DESC);
CREATE INDEX idx_entity_matching_method ON staging.entity_matching_candidates(matching_method);
CREATE INDEX idx_entity_matching_review ON staging.entity_matching_candidates(manual_review_required);

-- Core table indexes
CREATE INDEX idx_companies_abn ON core.companies(abn);
CREATE INDEX idx_companies_postcode ON core.companies(address_postcode);
CREATE INDEX idx_companies_state ON core.companies(address_state);
CREATE INDEX idx_companies_industry ON core.companies(industry);
CREATE INDEX idx_companies_status ON core.companies(is_active);
CREATE INDEX idx_companies_quality ON core.companies(data_quality_score DESC);
CREATE INDEX idx_companies_updated ON core.companies(updated_at);

-- Alternative names indexes
CREATE INDEX idx_company_names_company_id ON core.company_names(company_id);
CREATE INDEX idx_company_names_type ON core.company_names(name_type);
CREATE INDEX idx_company_names_primary ON core.company_names(company_id, is_primary);

-- Contact information indexes
CREATE INDEX idx_company_contacts_company_id ON core.company_contacts(company_id);
CREATE INDEX idx_company_contacts_type ON core.company_contacts(contact_type);
CREATE INDEX idx_company_contacts_value ON core.company_contacts(contact_value);

-- Industry classification indexes
CREATE INDEX idx_industry_class_company_id ON core.industry_classifications(company_id);
CREATE INDEX idx_industry_class_code ON core.industry_classifications(code);
CREATE INDEX idx_industry_class_system ON core.industry_classifications(classification_system);

-- Data lineage indexes
CREATE INDEX idx_data_lineage_company_id ON core.data_lineage(company_id);
CREATE INDEX idx_data_lineage_source ON core.data_lineage(source_system);
CREATE INDEX idx_data_lineage_extraction_date ON core.data_lineage(extraction_date);

-- Check constraints for data quality
ALTER TABLE core.companies ADD CONSTRAINT valid_abn_format 
    CHECK (REGEXP_LIKE(abn, '^[0-9]{11}$') OR abn IS NULL);

ALTER TABLE core.companies ADD CONSTRAINT valid_postcode_format 
    CHECK (REGEXP_LIKE(address_postcode, '^[0-9]{4}$') OR address_postcode IS NULL);

ALTER TABLE core.companies ADD CONSTRAINT valid_data_quality_score 
    CHECK (data_quality_score >= 0.0 AND data_quality_score <= 1.0);

-- Note: Snowflake doesn't support triggers like PostgreSQL
-- Instead, use MERGE statements or stored procedures to update timestamps