-- Core tables for unified company data

-- Main companies table - the golden record
CREATE TABLE core.companies (
    company_id VARCHAR(36) PRIMARY KEY DEFAULT UUID_STRING(),
    abn VARCHAR(11) UNIQUE,
    company_name VARCHAR(16777216) NOT NULL,
    normalized_name VARCHAR(16777216) NOT NULL,
    website_url VARCHAR(16777216),
    entity_type VARCHAR(16777216),
    entity_status VARCHAR(16777216),
    industry VARCHAR(16777216),
    industry_code VARCHAR(10),
    
    -- Address information
    address_line_1 VARCHAR(16777216),
    address_line_2 VARCHAR(16777216),
    address_suburb VARCHAR(16777216),
    address_state VARCHAR(50),
    address_postcode VARCHAR(10),
    
    -- Important dates
    start_date DATE,
    registration_date DATE,
    
    -- Status and metadata
    gst_registered BOOLEAN,
    dgr_endorsed BOOLEAN,
    is_active BOOLEAN DEFAULT TRUE,
    data_quality_score NUMBER(5,2),
    
    -- Audit fields
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    data_source ARRAY -- Track which sources contributed to this record
);

-- Alternative names and trading names
CREATE TABLE core.company_names (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    company_id VARCHAR(36),
    name VARCHAR(16777216) NOT NULL,
    name_type VARCHAR(20) CHECK (name_type IN ('trading', 'business', 'legal', 'dba')),
    is_primary BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id),
    FOREIGN KEY (company_id) REFERENCES core.companies(company_id) ON DELETE CASCADE
);

-- Company contact information
CREATE TABLE core.company_contacts (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    company_id VARCHAR(36),
    contact_type VARCHAR(20) CHECK (contact_type IN ('email', 'phone', 'fax', 'linkedin', 'twitter', 'facebook')),
    contact_value VARCHAR(16777216) NOT NULL,
    is_verified BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id),
    FOREIGN KEY (company_id) REFERENCES core.companies(company_id) ON DELETE CASCADE
);

-- Industry classifications (ANZSIC codes)
CREATE TABLE core.industry_classifications (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    company_id VARCHAR(36),
    classification_system VARCHAR(20) DEFAULT 'ANZSIC',
    code VARCHAR(10),
    description VARCHAR(16777216),
    level NUMBER, -- 1=Division, 2=Subdivision, 3=Group, 4=Class
    confidence_score NUMBER(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id),
    FOREIGN KEY (company_id) REFERENCES core.companies(company_id) ON DELETE CASCADE
);

-- Data lineage table to track source contributions
CREATE TABLE core.data_lineage (
    id NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    company_id VARCHAR(36),
    source_system VARCHAR(50) NOT NULL,
    source_record_id VARCHAR(16777216),
    contribution_fields ARRAY,
    extraction_date TIMESTAMP_NTZ,
    confidence_score NUMBER(5,2),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (id),
    FOREIGN KEY (company_id) REFERENCES core.companies(company_id) ON DELETE CASCADE
);