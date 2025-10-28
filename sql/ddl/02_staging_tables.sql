-- Staging tables for raw data extraction

-- Common Crawl staging table
CREATE TABLE staging.common_crawl_raw (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    extraction_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    website_url VARCHAR(16777216) NOT NULL,
    company_name VARCHAR(16777216),
    industry VARCHAR(16777216),
    raw_html_content VARCHAR(16777216),
    meta_description VARCHAR(16777216),
    title VARCHAR(16777216),
    contact_info VARIANT,
    social_links VARIANT,
    extraction_confidence NUMBER(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id)
);

-- ABR staging table  
CREATE TABLE staging.abr_raw (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    extraction_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    abn VARCHAR(11) NOT NULL,
    entity_name VARCHAR(16777216) NOT NULL,
    entity_type VARCHAR(16777216),
    entity_status VARCHAR(16777216),
    entity_type_code VARCHAR(10),
    entity_status_code VARCHAR(10),
    address_state_code VARCHAR(3),
    address_postcode VARCHAR(10),
    address_line_1 VARCHAR(16777216),
    address_line_2 VARCHAR(16777216),
    address_suburb VARCHAR(16777216),
    address_state VARCHAR(16777216),
    start_date DATE,
    registration_date DATE,
    last_updated_date DATE,
    gst_status VARCHAR(16777216),
    dgr_status VARCHAR(16777216),
    acn VARCHAR(9),
    trading_names ARRAY,
    business_names ARRAY,
    raw_xml VARCHAR(16777216),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id)
);

-- Entity matching staging table for LLM processing
CREATE TABLE staging.entity_matching_candidates (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    common_crawl_id NUMBER,
    abr_id NUMBER,
    similarity_score NUMBER(7,4),
    matching_method VARCHAR(50),
    llm_confidence NUMBER(5,2),
    llm_reasoning VARCHAR(16777216),
    manual_review_required BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id),
    FOREIGN KEY (common_crawl_id) REFERENCES staging.common_crawl_raw(id),
    FOREIGN KEY (abr_id) REFERENCES staging.abr_raw(id)
);