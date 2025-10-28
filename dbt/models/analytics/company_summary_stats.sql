{{ config(
    materialized='table',
    post_hook="CREATE INDEX IF NOT EXISTS idx_company_summary_calculation_date ON {{ this }} (calculation_date)"
) }}

/*
    Company summary statistics for monitoring and reporting.
    Provides key metrics about the company dataset.
*/

with base_stats as (
    select
        count(*) as total_companies,
        count(case when is_active = true then 1 end) as active_companies,
        count(case when abn is not null then 1 end) as companies_with_abn,
        count(case when website_url is not null then 1 end) as companies_with_website,
        count(case when has_email_contact = true then 1 end) as companies_with_email,
        count(case when has_phone_contact = true then 1 end) as companies_with_phone,
        count(case when gst_registered = true then 1 end) as gst_registered_companies,
        count(case when dgr_endorsed = true then 1 end) as dgr_endorsed_companies
    from {{ ref('dim_companies') }}
),

quality_stats as (
    select
        count(case when quality_tier = 'high' then 1 end) as high_quality_companies,
        count(case when quality_tier = 'medium' then 1 end) as medium_quality_companies,
        count(case when quality_tier = 'low' then 1 end) as low_quality_companies,
        avg(data_quality_score) as avg_data_quality_score,
        min(data_quality_score) as min_data_quality_score,
        max(data_quality_score) as max_data_quality_score,
        avg(address_completeness_score) as avg_address_completeness
    from {{ ref('dim_companies') }}
),

industry_stats as (
    select
        count(distinct industry_category) as unique_industries,
        count(case when industry is not null then 1 end) as companies_with_industry_info,
        count(case when industry_category = 'Other' then 1 end) as companies_uncategorized_industry
    from {{ ref('dim_companies') }}
),

geographic_stats as (
    select
        count(distinct address_state) as unique_states,
        count(distinct address_postcode) as unique_postcodes,
        count(case when address_state is not null then 1 end) as companies_with_state_info
    from {{ ref('dim_companies') }}
),

digital_presence_stats as (
    select
        count(case when digital_presence_level = 'high' then 1 end) as high_digital_presence,
        count(case when digital_presence_level = 'medium' then 1 end) as medium_digital_presence,
        count(case when digital_presence_level = 'low' then 1 end) as low_digital_presence,
        count(case when digital_presence_level = 'none' then 1 end) as no_digital_presence
    from {{ ref('dim_companies') }}
),

compliance_stats as (
    select
        count(case when compliance_status = 'fully_compliant' then 1 end) as fully_compliant_companies,
        count(case when compliance_status = 'basic_compliant' then 1 end) as basic_compliant_companies,
        count(case when compliance_status = 'registered_inactive' then 1 end) as registered_inactive_companies,
        count(case when compliance_status = 'unregistered' then 1 end) as unregistered_companies
    from {{ ref('dim_companies') }}
),

data_source_stats as (
    select
        count(case when 'common_crawl' = any(data_source) then 1 end) as companies_from_common_crawl,
        count(case when 'abr' = any(data_source) then 1 end) as companies_from_abr,
        count(case when array_length(data_source, 1) > 1 then 1 end) as companies_from_multiple_sources
    from {{ ref('dim_companies') }}
)

select
    -- Timestamp
    current_timestamp as calculation_date,
    
    -- Total counts
    bs.total_companies,
    bs.active_companies,
    round(bs.active_companies::decimal / bs.total_companies * 100, 2) as active_company_percentage,
    
    -- Data completeness
    bs.companies_with_abn,
    round(bs.companies_with_abn::decimal / bs.total_companies * 100, 2) as abn_completeness_percentage,
    bs.companies_with_website,
    round(bs.companies_with_website::decimal / bs.total_companies * 100, 2) as website_completeness_percentage,
    bs.companies_with_email,
    round(bs.companies_with_email::decimal / bs.total_companies * 100, 2) as email_completeness_percentage,
    bs.companies_with_phone,
    round(bs.companies_with_phone::decimal / bs.total_companies * 100, 2) as phone_completeness_percentage,
    
    -- Compliance metrics
    bs.gst_registered_companies,
    round(bs.gst_registered_companies::decimal / bs.total_companies * 100, 2) as gst_registration_percentage,
    bs.dgr_endorsed_companies,
    round(bs.dgr_endorsed_companies::decimal / bs.total_companies * 100, 2) as dgr_endorsement_percentage,
    
    -- Quality metrics
    qs.high_quality_companies,
    qs.medium_quality_companies,
    qs.low_quality_companies,
    round(qs.high_quality_companies::decimal / bs.total_companies * 100, 2) as high_quality_percentage,
    round(qs.avg_data_quality_score, 3) as avg_data_quality_score,
    round(qs.min_data_quality_score, 3) as min_data_quality_score,
    round(qs.max_data_quality_score, 3) as max_data_quality_score,
    round(qs.avg_address_completeness, 3) as avg_address_completeness,
    
    -- Industry diversity
    is_.unique_industries,
    is_.companies_with_industry_info,
    round(is_.companies_with_industry_info::decimal / bs.total_companies * 100, 2) as industry_completeness_percentage,
    is_.companies_uncategorized_industry,
    round(is_.companies_uncategorized_industry::decimal / is_.companies_with_industry_info * 100, 2) as uncategorized_industry_percentage,
    
    -- Geographic coverage
    gs.unique_states,
    gs.unique_postcodes,
    gs.companies_with_state_info,
    round(gs.companies_with_state_info::decimal / bs.total_companies * 100, 2) as state_completeness_percentage,
    
    -- Digital presence
    dps.high_digital_presence,
    dps.medium_digital_presence,
    dps.low_digital_presence,
    dps.no_digital_presence,
    round(dps.high_digital_presence::decimal / bs.total_companies * 100, 2) as high_digital_presence_percentage,
    
    -- Compliance distribution
    cs.fully_compliant_companies,
    cs.basic_compliant_companies,
    cs.registered_inactive_companies,
    cs.unregistered_companies,
    round(cs.fully_compliant_companies::decimal / bs.total_companies * 100, 2) as fully_compliant_percentage,
    
    -- Data source coverage
    dss.companies_from_common_crawl,
    dss.companies_from_abr,
    dss.companies_from_multiple_sources,
    round(dss.companies_from_multiple_sources::decimal / bs.total_companies * 100, 2) as multi_source_percentage
    
from base_stats bs
cross join quality_stats qs
cross join industry_stats is_
cross join geographic_stats gs
cross join digital_presence_stats dps
cross join compliance_stats cs
cross join data_source_stats dss