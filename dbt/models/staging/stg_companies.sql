{{ config(materialized='view') }}

/*
    Staging model for companies data with basic cleaning and standardization
*/

with companies_cleaned as (
    select
        company_id,
        abn,
        company_name,
        normalized_name,
        website_url,
        entity_type,
        entity_status,
        industry,
        
        -- Address standardization
        trim(address_line_1) as address_line_1,
        trim(address_line_2) as address_line_2,
        trim(address_suburb) as address_suburb,
        upper(trim(address_state)) as address_state,
        address_postcode,
        
        -- Date fields
        start_date,
        registration_date,
        
        -- Status fields
        gst_registered,
        dgr_endorsed,
        is_active,
        
        -- Quality metrics
        data_quality_score,
        data_source,
        
        -- Timestamps
        created_at,
        updated_at,
        
        -- Calculated fields
        case 
            when website_url is not null then true 
            else false 
        end as has_website,
        
        case 
            when abn is not null and length(abn) = 11 then true 
            else false 
        end as has_valid_abn,
        
        case 
            when data_quality_score >= {{ var('high_quality_threshold') }} then 'high'
            when data_quality_score >= {{ var('min_data_quality_score') }} then 'medium'
            else 'low'
        end as quality_tier,
        
        -- Industry categorization
        case 
            when industry ilike '%manufacturing%' then 'Manufacturing'
            when industry ilike '%construction%' then 'Construction'
            when industry ilike '%professional%' or industry ilike '%services%' then 'Professional Services'
            when industry ilike '%technology%' or industry ilike '%IT%' or industry ilike '%software%' then 'Technology'
            when industry ilike '%retail%' or industry ilike '%trade%' then 'Retail'
            when industry ilike '%financ%' or industry ilike '%banking%' then 'Financial Services'
            when industry ilike '%health%' or industry ilike '%medical%' then 'Healthcare'
            when industry ilike '%education%' or industry ilike '%training%' then 'Education'
            when industry ilike '%transport%' or industry ilike '%logistics%' then 'Transport & Logistics'
            when industry ilike '%agricult%' or industry ilike '%farming%' then 'Agriculture'
            else 'Other'
        end as industry_category
        
    from {{ source('core', 'companies') }}
    where 
        company_name is not null
        and length(trim(company_name)) >= 2
        and (data_quality_score is null or data_quality_score >= {{ var('min_data_quality_score') }})
),

companies_with_metrics as (
    select 
        *,
        
        -- Address completeness score (0-1)
        (
            case when address_line_1 is not null then 0.25 else 0 end +
            case when address_suburb is not null then 0.25 else 0 end +
            case when address_state is not null then 0.25 else 0 end +
            case when address_postcode is not null then 0.25 else 0 end
        ) as address_completeness_score,
        
        -- Contact availability flags
        case 
            when exists (
                select 1 from {{ source('core', 'company_contacts') }} cc 
                where cc.company_id = companies_cleaned.company_id 
                and cc.contact_type = 'email'
            ) then true 
            else false 
        end as has_email_contact,
        
        case 
            when exists (
                select 1 from {{ source('core', 'company_contacts') }} cc 
                where cc.company_id = companies_cleaned.company_id 
                and cc.contact_type = 'phone'
            ) then true 
            else false 
        end as has_phone_contact
        
    from companies_cleaned
)

select * from companies_with_metrics