-- Analytics and reporting tables

-- Company statistics aggregated by state
CREATE TABLE analytics.companies_by_state (
    state VARCHAR(50) PRIMARY KEY,
    total_companies NUMBER,
    active_companies NUMBER,
    gst_registered_companies NUMBER,
    avg_data_quality_score NUMBER(7,2),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Industry distribution
CREATE TABLE analytics.industry_distribution (
    industry_code VARCHAR(10) PRIMARY KEY,
    industry_description VARCHAR(16777216),
    company_count NUMBER,
    percentage_of_total NUMBER(7,2),
    avg_data_quality_score NUMBER(7,2),
    last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Data quality metrics
CREATE TABLE analytics.data_quality_summary (
    metric_name VARCHAR(100),
    metric_value NUMBER(12,2),
    metric_description VARCHAR(16777216),
    calculation_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (metric_name, calculation_date)
);

-- Entity matching performance metrics
CREATE TABLE analytics.entity_matching_metrics (
    matching_run_id VARCHAR(36) DEFAULT UUID_STRING(),
    total_common_crawl_records NUMBER,
    total_abr_records NUMBER,
    successful_matches NUMBER,
    manual_review_required NUMBER,
    avg_confidence_score NUMBER(5,2),
    processing_duration_seconds NUMBER,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);