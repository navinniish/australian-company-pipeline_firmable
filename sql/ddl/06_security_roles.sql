-- Security roles and permissions for Snowflake

-- Create roles for different access levels
CREATE ROLE pipeline_admin;
CREATE ROLE pipeline_etl;
CREATE ROLE pipeline_analyst;
CREATE ROLE pipeline_readonly;

-- Grant database and schema usage permissions
GRANT USAGE ON DATABASE australian_companies TO pipeline_admin, pipeline_etl, pipeline_analyst, pipeline_readonly;
GRANT USAGE ON SCHEMA staging TO pipeline_etl, pipeline_admin;
GRANT USAGE ON SCHEMA core TO pipeline_etl, pipeline_analyst, pipeline_admin, pipeline_readonly;
GRANT USAGE ON SCHEMA analytics TO pipeline_analyst, pipeline_admin, pipeline_readonly;
GRANT USAGE ON SCHEMA logs TO pipeline_admin;

-- Admin role - full access
GRANT ALL ON SCHEMA staging TO pipeline_admin;
GRANT ALL ON SCHEMA core TO pipeline_admin;
GRANT ALL ON SCHEMA analytics TO pipeline_admin;
GRANT ALL ON SCHEMA logs TO pipeline_admin;

-- ETL role - read/write to staging and core, read analytics
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA staging TO pipeline_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA core TO pipeline_etl;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO pipeline_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA staging TO pipeline_etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA core TO pipeline_etl;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics TO pipeline_etl;

-- Analyst role - read core and analytics, limited staging access
GRANT SELECT ON ALL TABLES IN SCHEMA core TO pipeline_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO pipeline_analyst;
GRANT SELECT ON staging.common_crawl_raw, staging.abr_raw TO pipeline_analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA core TO pipeline_analyst;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics TO pipeline_analyst;

-- Read-only role - read access to core and analytics only
GRANT SELECT ON ALL TABLES IN SCHEMA core TO pipeline_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO pipeline_readonly;
GRANT SELECT ON FUTURE TABLES IN SCHEMA core TO pipeline_readonly;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics TO pipeline_readonly;

-- Note: Snowflake uses masking policies instead of row-level security
-- Example masking policy (uncomment to use):
-- CREATE MASKING POLICY company_data_mask AS (val string) RETURNS string ->
--   CASE 
--     WHEN current_role() IN ('PIPELINE_ADMIN', 'PIPELINE_ETL') THEN val
--     ELSE '*MASKED*'
--   END;

-- Create users (examples - replace with actual usernames)
-- CREATE USER etl_service PASSWORD='secure_password_here';
-- CREATE USER analyst_user PASSWORD='secure_password_here'; 
-- CREATE USER readonly_user PASSWORD='secure_password_here';

-- Grant roles to users
-- GRANT ROLE pipeline_etl TO USER etl_service;
-- GRANT ROLE pipeline_analyst TO USER analyst_user;
-- GRANT ROLE pipeline_readonly TO USER readonly_user;