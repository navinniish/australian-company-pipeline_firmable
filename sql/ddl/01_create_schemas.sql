-- Create schemas for the Australian company data pipeline
-- Staging schema for raw extracted data
CREATE SCHEMA IF NOT EXISTS staging;

-- Core schema for cleaned and processed data
CREATE SCHEMA IF NOT EXISTS core;

-- Analytics schema for aggregated views
CREATE SCHEMA IF NOT EXISTS analytics;

-- Logging schema for pipeline monitoring
CREATE SCHEMA IF NOT EXISTS logs;